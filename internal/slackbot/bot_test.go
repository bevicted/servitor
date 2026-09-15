package slackbot

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/controller"
	"github.com/bevicted/servitor/internal/inventory"
	"github.com/bevicted/servitor/internal/state"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type testAllocationClient struct {
	client.Client
	reader client.Reader
}

func (c testAllocationClient) AllocationReader() client.Reader { return c.reader }

type memoryResponder struct {
	mu        sync.Mutex
	responses []Response
	err       error
}

func (r *memoryResponder) Reply(_ context.Context, response Response) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.responses = append(r.responses, response)
	return nil
}
func botForTest(t *testing.T, objects ...runtime.Object) (Bot, *memoryResponder) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	responses := &memoryResponder{}
	return Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Responder: responses, Lease: 4 * time.Hour, RetryIntervals: []time.Duration{time.Minute}, Clock: func() time.Time { return time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC) }}, responses
}
func TestCreateAcknowledgesBeforeCreatingOneDeterministicCluster(t *testing.T) {
	bot, responses := botForTest(t)
	if err := bot.Handle(context.Background(), Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || responses.responses[0].Text != "Planning..." {
		t.Fatalf("responses=%+v", responses.responses)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "123")}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Slack.OwnerID != "U1" || cluster.Spec.Slack.ThreadTimestamp != "123" || cluster.Spec.UserOptions.Version != "4.22" || cluster.Spec.UserOptions.Provider != "" || cluster.Spec.Lifecycle.InitialLeaseSeconds != int64(bot.Lease/time.Second) || cluster.Spec.Lifecycle.RetrySeconds[0] != int64(time.Minute/time.Second) {
		t.Fatalf("cluster=%+v", cluster.Spec)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	var clusters servitorv1alpha1.ServitorClusterList
	if err := bot.Client.List(context.Background(), &clusters); err != nil {
		t.Fatal(err)
	}
	if len(clusters.Items) != 1 {
		t.Fatalf("clusters=%d", len(clusters.Items))
	}
}
func TestCreateAllocationLimitsAndCompletedCapacity(t *testing.T) {
	bot, responses := botForTest(t)
	bot.MaxAllocationsPerUser = 1
	first := Envelope{ID: "first", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22", Timestamp: "100.000001"}}
	if err := bot.Handle(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "over-cap", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22", Timestamp: "100.000002"}}); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[len(responses.responses)-1].Text; !containsText(got, "limit of 1") || containsText(got, "Planning...") {
		t.Fatalf("one-slot quota response = %q", got)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "replay", Message: first.Message}); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[len(responses.responses)-1].Text; !containsText(got, "already have an allocation") {
		t.Fatalf("replay at quota = %q", got)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "100.000001")}, stored); err != nil {
		t.Fatal(err)
	}
	stored.Status.Phase = servitorv1alpha1.PhaseCleanupComplete
	if err := bot.Client.Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "capacity-restored", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22", Timestamp: "100.000003"}}); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[len(responses.responses)-1].Text; got != "Planning..." {
		t.Fatalf("completed allocation did not free capacity: %q", got)
	}

	more, moreResponses := botForTest(t)
	more.MaxAllocationsPerUser = 5
	for index := range 4 {
		if err := more.Handle(context.Background(), Envelope{ID: fmt.Sprintf("more-%d", index), Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22", Timestamp: fmt.Sprintf("200.00000%d", index)}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(moreResponses.responses) != 4 || moreResponses.responses[3].Text != "Planning..." {
		t.Fatalf("greater-than-three cap responses = %+v", moreResponses.responses)
	}
	helpBot, helpResponses := botForTest(t)
	helpBot.MaxAllocationsPerUser = 5
	if err := helpBot.Handle(context.Background(), Envelope{ID: "help", Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "help"}}); err != nil {
		t.Fatal(err)
	}
	if got := helpResponses.responses[0].Text; !containsText(got, "up to 5") {
		t.Fatalf("configured help = %q", got)
	}
	if err := helpBot.Handle(context.Background(), Envelope{ID: "unknown", Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "unknown"}}); err != nil {
		t.Fatal(err)
	}
	if got := helpResponses.responses[1].Text; !containsText(got, "up to 5") {
		t.Fatalf("configured unknown-command help = %q", got)
	}
}

func TestCreateUsesDirectReaderForExistingAllocationAndQuota(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cached := fake.NewClientBuilder().WithScheme(scheme).Build()
	existing := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "300.000001"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "300.000001"}}}
	direct := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(existing).Build()
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: testAllocationClient{Client: cached, reader: direct}, Events: state.NewEventStore(cached, "servitor"), Responder: responses, MaxAllocationsPerUser: 1}
	if err := bot.Handle(context.Background(), Envelope{ID: "replay", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create invalid=option", Timestamp: "300.000001"}}); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[len(responses.responses)-1].Text; !containsText(got, "already have an allocation") {
		t.Fatalf("direct existing lookup = %q", got)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "quota", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22", Timestamp: "300.000002"}}); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[len(responses.responses)-1].Text; !containsText(got, "limit of 1") {
		t.Fatalf("direct quota lookup = %q", got)
	}
}

func TestReviewDecisionUsesDirectReader(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ReviewDeadline: &deadline}}
	direct := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	cached := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, underlying client.WithWatch, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
			if _, ok := object.(*servitorv1alpha1.ServitorCluster); ok {
				return apierrors.NewNotFound(schema.GroupResource{Group: "servitor.bevicted.github.io", Resource: "servitorclusters"}, key.Name)
			}
			return underlying.Get(ctx, key, object, options...)
		},
		Update: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			return direct.Update(ctx, object, options...)
		},
	}).Build()
	bot := Bot{Namespace: "servitor", Client: testAllocationClient{Client: cached, reader: direct}, Clock: func() time.Time { return now }}
	outcome, err := bot.recordReviewDecision(context.Background(), cluster.Name, Message{Channel: "C1", User: "U1"}, "root", "approved")
	if err != nil || outcome != reviewDecisionRecorded {
		t.Fatalf("recordReviewDecision() = (%q, %v), want (%q, nil)", outcome, err, reviewDecisionRecorded)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := direct.Get(context.Background(), client.ObjectKey{Namespace: "servitor", Name: cluster.Name}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Lifecycle.Approval != "approved" {
		t.Fatalf("direct allocation review intent = %q", stored.Spec.Lifecycle.Approval)
	}
}

func TestCountedAllocationsExcludesOnlyCleanupComplete(t *testing.T) {
	phases := []string{"", servitorv1alpha1.PhasePending, servitorv1alpha1.PhasePlanning, servitorv1alpha1.PhaseAwaitingApproval, servitorv1alpha1.PhaseApplying, servitorv1alpha1.PhaseReady, servitorv1alpha1.PhaseCleanupPending, servitorv1alpha1.PhaseUnresolved, servitorv1alpha1.PhaseCleanupComplete}
	allocations := make([]servitorv1alpha1.ServitorCluster, len(phases))
	for index, phase := range phases {
		allocations[index].Status.Phase = phase
	}
	if got := (Bot{}).countedAllocations(allocations); got != len(phases)-1 {
		t.Fatalf("counted allocations = %d, want %d", got, len(phases)-1)
	}
}

func TestCreateAuthOptInQueuesOnlyEligiblePublicRequests(t *testing.T) {
	for _, test := range []struct {
		name, text, target, provider string
		public                       bool
		wantIntent                   bool
		unsupported                  bool
	}{
		{name: "public bare", text: "auth", target: "public", provider: "vpc-gen2", public: true, wantIntent: true},
		{name: "public assignment", text: "auth=true", target: "public", provider: "vpc-gen2", public: true, wantIntent: true},
		{name: "public disabled", text: "auth=false", target: "public", provider: "vpc-gen2", public: true},
		{name: "public ordinary", text: "version=4.22", target: "public", provider: "vpc-gen2", public: true},
		{name: "private", text: "auth", target: "private", provider: "vpc-gen2", unsupported: true},
		{name: "satellite", text: "auth=true", target: "public", provider: "satellite", public: true, unsupported: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			bot, responses := botForTest(t)
			bot.Defaults = command.CreateDefaults{Target: test.target, Provider: test.provider}
			if test.public {
				bot.PublicAuthTargets = []string{"public"}
			}
			event := Envelope{ID: "create-" + test.name, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create " + test.text, Timestamp: "1710000000.000100"}}
			if err := bot.Handle(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			key := types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", event.Message.Timestamp)}
			if err := bot.Client.Get(context.Background(), key, stored); err != nil {
				t.Fatal(err)
			}
			wantTimestamp := ""
			if test.wantIntent {
				wantTimestamp = event.Message.Timestamp
			}
			if stored.Spec.Lifecycle.AuthRequestTimestamp != wantTimestamp {
				t.Fatalf("auth intent = %q, want %q", stored.Spec.Lifecycle.AuthRequestTimestamp, wantTimestamp)
			}
			if len(responses.responses) == 0 || responses.responses[0].Text != "Planning..." {
				t.Fatalf("create response = %+v", responses.responses)
			}
			if test.unsupported {
				if len(responses.responses) != 2 || responses.responses[1].Text != "VPN-backed authentication is not implemented yet" {
					t.Fatalf("private or Satellite auth response = %+v", responses.responses)
				}
			} else if len(responses.responses) != 1 {
				t.Fatalf("unexpected auth create response = %+v", responses.responses)
			}
			if err := bot.Handle(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			if err := bot.Handle(context.Background(), Envelope{ID: "repeat-" + test.name, Message: event.Message}); err != nil {
				t.Fatal(err)
			}
			if err := bot.Client.Get(context.Background(), key, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Spec.Lifecycle.AuthRequestTimestamp != wantTimestamp {
				t.Fatalf("repeat create changed auth intent to %q", stored.Spec.Lifecycle.AuthRequestTimestamp)
			}
		})
	}
}

func TestHandleCreateNormalizesAssignmentsWithoutDefaults(t *testing.T) {
	want := servitorv1alpha1.UserOptions{
		Target: "synthetic-target", Version: "4.22", ResourceGroup: "Platform Team=Core", WorkerCount: 3,
		SubnetIDs: []string{"subnet-one", "subnet-two"},
	}
	bot, responses := botForTest(t)
	bot.Defaults = command.CreateDefaults{Provider: "vpc-gen2", Zone: "operator-zone"}
	event := Envelope{ID: "assignments", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: `<@BOT> create target=synthetic-target version=4.22 resource-group="Platform Team=Core" worker-count=3 subnet-id=subnet-one subnet-id=subnet-two`, Timestamp: "123"}}
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || responses.responses[0].Text != "Planning..." {
		t.Fatalf("responses=%+v", responses.responses)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "123")}, cluster); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cluster.Spec.UserOptions, want) {
		t.Fatalf("userOptions = %+v\nwant %+v", cluster.Spec.UserOptions, want)
	}
}

func TestHandleCreateRejectsInvalidWorkerCountWithoutAllocation(t *testing.T) {
	for _, workerCount := range []string{"not-a-number", "999999999999999999999999999999", "0", "101"} {
		t.Run(workerCount, func(t *testing.T) {
			bot, responses := botForTest(t)
			event := Envelope{ID: "worker-" + workerCount, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create worker-count=" + workerCount, Timestamp: "123"}}
			if err := bot.Handle(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, "worker-count must be an integer from 1 through 100") {
				t.Fatalf("responses=%+v", responses.responses)
			}
			cluster := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, cluster); err == nil {
				t.Fatal("invalid worker count recorded an allocation")
			}
		})
	}
}

func TestCreateRejectsInvalidAuthOptionsWithoutAllocation(t *testing.T) {
	for _, text := range []string{"auth=maybe", "auth auth=true", "auth=false auth"} {
		t.Run(text, func(t *testing.T) {
			bot, responses := botForTest(t)
			event := Envelope{ID: "invalid-" + text, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create " + text, Timestamp: "1710000000.000100"}}
			if err := bot.Handle(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, "auth") {
				t.Fatalf("responses = %+v", responses.responses)
			}
			cluster := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, cluster); !apierrors.IsNotFound(err) {
				t.Fatalf("invalid auth create recorded allocation: %v", err)
			}
		})
	}
}

func TestCreateRejectsCallerSelectedName(t *testing.T) {
	bot, responses := botForTest(t)
	if err := bot.Handle(context.Background(), Envelope{ID: "Ev-name", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22 name=caller-selected", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, `unknown create option "name"`) {
		t.Fatalf("responses=%+v", responses.responses)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, cluster); err == nil {
		t.Fatal("Slack create with name recorded an allocation")
	}
}

func TestCreateDoesNotProceedWhenAcceptanceDeliveryFails(t *testing.T) {
	bot, responses := botForTest(t)
	event := Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22", Timestamp: "123"}}
	responses.err = errors.New("Slack unavailable")
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	name := types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "123")}
	if err := bot.Client.Get(context.Background(), name, cluster); err == nil {
		t.Fatal("create proceeded after Slack acceptance delivery failed")
	}
	seen, err := bot.Events.(*state.EventStore).Seen(context.Background(), event.ID)
	if err != nil || seen {
		t.Fatalf("failed create receipt Seen() = (%v, %v), want (false, nil)", seen, err)
	}
	responses.err = nil
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := bot.Client.Get(context.Background(), name, cluster); err != nil {
		t.Fatalf("redelivered create was suppressed: %v", err)
	}
}

type progressRecorder struct {
	events    []string
	responses []Response
	err       error
}

func (r *progressRecorder) Reply(_ context.Context, response Response) error {
	r.events = append(r.events, "reply: "+response.Text)
	if r.err != nil {
		return r.err
	}
	r.responses = append(r.responses, response)
	return nil
}

func progressBotForTest(t *testing.T, recorder *progressRecorder, failCreate, failUpdate bool, objects ...runtime.Object) Bot {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.CreateOption) error {
			if _, ok := object.(*servitorv1alpha1.ServitorCluster); ok {
				recorder.events = append(recorder.events, "kubernetes create")
				if failCreate {
					return errors.New("persistence failure")
				}
			}
			return underlying.Create(ctx, object, options...)
		},
		Update: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			if _, ok := object.(*servitorv1alpha1.ServitorCluster); ok {
				recorder.events = append(recorder.events, "kubernetes update")
				if failUpdate {
					return errors.New("persistence failure")
				}
			}
			return underlying.Update(ctx, object, options...)
		},
	}).Build()
	return Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Responder: recorder, Lease: 4 * time.Hour, RetryIntervals: []time.Duration{time.Minute}, Clock: func() time.Time { return time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC) }}
}

func TestHandleCommandProgressAndPersistenceOrdering(t *testing.T) {
	message := func(text, timestamp string) Message {
		return Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: text, Timestamp: timestamp}
	}
	cluster := func() *servitorv1alpha1.ServitorCluster {
		return &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}}
	}

	t.Run("create delivers planning before Kubernetes creation", func(t *testing.T) {
		recorder := &progressRecorder{}
		bot := progressBotForTest(t, recorder, false, false)
		if err := bot.Handle(context.Background(), Envelope{Message: message("<@BOT> create version=4.22", "create")}); err != nil {
			t.Fatal(err)
		}
		if want := []string{"reply: Planning...", "kubernetes create"}; !reflect.DeepEqual(recorder.events, want) {
			t.Fatalf("events=%q, want %q", recorder.events, want)
		}
		if len(recorder.responses) != 1 || recorder.responses[0].Text != "Planning..." {
			t.Fatalf("responses=%+v", recorder.responses)
		}
	})

	t.Run("done persists cleanup before acknowledging it", func(t *testing.T) {
		recorder := &progressRecorder{}
		bot := progressBotForTest(t, recorder, false, false, cluster())
		if err := bot.Handle(context.Background(), Envelope{Message: message("<@BOT> done", "done")}); err != nil {
			t.Fatal(err)
		}
		if want := []string{"kubernetes update", "reply: Cleaning up...", "reply: Cleanup requested for 1 allocation(s)."}; !reflect.DeepEqual(recorder.events, want) {
			t.Fatalf("events=%q, want %q", recorder.events, want)
		}
		current := &servitorv1alpha1.ServitorCluster{}
		if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, current); err != nil || !current.Spec.Lifecycle.CleanupRequested {
			t.Fatalf("cleanup request = (%t, %v), want (true, nil)", current.Spec.Lifecycle.CleanupRequested, err)
		}
	})

	t.Run("destroy persists cleanup silently", func(t *testing.T) {
		recorder := &progressRecorder{}
		bot := progressBotForTest(t, recorder, false, false, cluster())
		if err := bot.Handle(context.Background(), Envelope{Message: message("<@BOT> destroy", "destroy")}); err != nil {
			t.Fatal(err)
		}
		if want := []string{"kubernetes update"}; !reflect.DeepEqual(recorder.events, want) {
			t.Fatalf("events=%q, want %q", recorder.events, want)
		}
		if len(recorder.responses) != 0 {
			t.Fatalf("destroy responses=%+v", recorder.responses)
		}
	})

	t.Run("failed create reply does not create an allocation", func(t *testing.T) {
		recorder := &progressRecorder{err: errors.New("Slack unavailable")}
		bot := progressBotForTest(t, recorder, false, false)
		if err := bot.Handle(context.Background(), Envelope{Message: message("<@BOT> create version=4.22", "create")}); err != nil {
			t.Fatal(err)
		}
		if want := []string{"reply: Planning..."}; !reflect.DeepEqual(recorder.events, want) {
			t.Fatalf("events=%q, want %q", recorder.events, want)
		}
		current := &servitorv1alpha1.ServitorCluster{}
		if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, current); err == nil {
			t.Fatal("failed reply created an allocation")
		}
	})

	t.Run("persistence failures do not claim progress", func(t *testing.T) {
		recorder := &progressRecorder{}
		bot := progressBotForTest(t, recorder, true, false)
		if err := bot.Handle(context.Background(), Envelope{Message: message("<@BOT> create version=4.22", "create")}); err != nil {
			t.Fatal(err)
		}
		if want := []string{"reply: Planning...", "kubernetes create", "reply: Command rejected.\n\nUnable to record the create request. No operation was started."}; !reflect.DeepEqual(recorder.events, want) {
			t.Fatalf("create events=%q, want %q", recorder.events, want)
		}
		current := &servitorv1alpha1.ServitorCluster{}
		if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, current); err == nil {
			t.Fatal("failed persistence created an allocation")
		}

		recorder = &progressRecorder{}
		bot = progressBotForTest(t, recorder, false, true, cluster())
		if err := bot.Handle(context.Background(), Envelope{Message: message("<@BOT> done", "done")}); err != nil {
			t.Fatal(err)
		}
		if want := []string{"kubernetes update", "reply: Cleanup requested for 0 allocation(s).\nFailed to record cleanup for 1 allocation(s)."}; !reflect.DeepEqual(recorder.events, want) {
			t.Fatalf("cleanup events=%q, want %q", recorder.events, want)
		}
		current = &servitorv1alpha1.ServitorCluster{}
		if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, current); err != nil || current.Spec.Lifecycle.CleanupRequested {
			t.Fatalf("cleanup request = (%t, %v), want (false, nil)", current.Spec.Lifecycle.CleanupRequested, err)
		}
	})
}

func TestChannelCleanupAllUsesDirectReaderAndContinuesAfterFailures(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	allocation := func(name, owner, channel, thread, phase string) *servitorv1alpha1.ServitorCluster {
		return &servitorv1alpha1.ServitorCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "servitor", UID: types.UID(name)},
			Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: owner, ChannelID: channel, ThreadTimestamp: thread}},
			Status:     servitorv1alpha1.ServitorClusterStatus{Phase: phase},
		}
	}
	requestedOne := allocation("requested-one", "U1", "C1", "thread-one", servitorv1alpha1.PhasePlanning)
	requestedTwo := allocation("requested-two", "U1", "C1", "thread-two", servitorv1alpha1.PhaseReady)
	failed := allocation("failed", "U1", "C1", "thread-failed", servitorv1alpha1.PhasePlanning)
	vanished := allocation("vanished", "U1", "C1", "thread-vanished", servitorv1alpha1.PhasePlanning)
	alreadyCleaning := allocation("already-cleaning", "U1", "C1", "thread-cleaning", servitorv1alpha1.PhaseCleanupPending)
	alreadyCleaning.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit}
	completed := allocation("completed", "U1", "C1", "thread-completed", servitorv1alpha1.PhaseCleanupComplete)
	completed.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit}
	unresolved := allocation("unresolved", "U1", "C1", "thread-unresolved", servitorv1alpha1.PhaseUnresolved)
	unresolved.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit}
	otherOwner := allocation("other-owner", "U2", "C1", "thread-other", servitorv1alpha1.PhasePlanning)
	otherChannel := allocation("other-channel", "U1", "C2", "thread-channel", servitorv1alpha1.PhasePlanning)
	otherNamespace := allocation("other-namespace", "U1", "C1", "thread-namespace", servitorv1alpha1.PhasePlanning)
	otherNamespace.Namespace = "other"
	direct := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(requestedOne, requestedTwo, failed, vanished, alreadyCleaning, completed, unresolved, otherOwner, otherChannel, otherNamespace).Build()
	cached := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Update: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			cluster, ok := object.(*servitorv1alpha1.ServitorCluster)
			if !ok {
				return underlying.Update(ctx, object, options...)
			}
			if cluster.Name == "failed" {
				return errors.New("persistence failure")
			}
			if cluster.Name == "vanished" {
				if err := direct.Delete(ctx, cluster); err != nil {
					return err
				}
				return apierrors.NewNotFound(schema.GroupResource{Group: "servitor.bevicted.github.io", Resource: "servitorclusters"}, cluster.Name)
			}
			return direct.Update(ctx, cluster, options...)
		},
	}).Build()
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: testAllocationClient{Client: cached, reader: direct}, Events: state.NewEventStore(cached, "servitor"), Responder: responses}
	if err := bot.Handle(context.Background(), Envelope{ID: "cleanup-all", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> done", Timestamp: "command-thread"}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"requested-one", "requested-two"} {
		current := &servitorv1alpha1.ServitorCluster{}
		if err := direct.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: name}, current); err != nil || !current.Spec.Lifecycle.CleanupRequested {
			t.Fatalf("cleanup intent for %s = (%t, %v), want (true, nil)", name, current.Spec.Lifecycle.CleanupRequested, err)
		}
	}
	for _, name := range []string{"failed", "already-cleaning", "completed", "unresolved", "other-owner", "other-channel"} {
		current := &servitorv1alpha1.ServitorCluster{}
		if err := direct.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: name}, current); err != nil {
			t.Fatal(err)
		}
		if current.Spec.Lifecycle.CleanupRequested {
			t.Fatalf("unexpected cleanup intent for %s", name)
		}
	}
	if len(responses.responses) != 3 {
		t.Fatalf("responses=%+v", responses.responses)
	}
	if responses.responses[0].ThreadTimestamp != "thread-one" || responses.responses[1].ThreadTimestamp != "thread-two" || responses.responses[0].Text != "Cleaning up..." || responses.responses[1].Text != "Cleaning up..." {
		t.Fatalf("cleanup notices=%+v", responses.responses[:2])
	}
	summary := responses.responses[2]
	if summary.ThreadTimestamp != "command-thread" {
		t.Fatalf("summary routing=%+v", summary)
	}
	for _, want := range []string{"Cleanup requested for 2 allocation(s).", "Already cleaning: 1 allocation(s).", "Cleanup complete: 1 allocation(s).", "Cleanup unresolved: 1 allocation(s).", "Failed to record cleanup for 2 allocation(s)."} {
		if !strings.Contains(summary.Text, want) {
			t.Fatalf("summary missing %q: %s", want, summary.Text)
		}
	}
}

func TestChannelDestroyCleansAllAllocationsSilently(t *testing.T) {
	allocation := func(name, owner string) *servitorv1alpha1.ServitorCluster {
		return &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: owner, ChannelID: "C1", ThreadTimestamp: name}}}
	}
	first, second, other := allocation("first", "U1"), allocation("second", "U1"), allocation("other", "U2")
	bot, responses := botForTest(t, first, second, other)
	if err := bot.Handle(context.Background(), Envelope{ID: "destroy-all", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> destroy", Timestamp: "command-thread"}}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		want bool
	}{{first.Name, true}, {second.Name, true}, {other.Name, false}} {
		current := &servitorv1alpha1.ServitorCluster{}
		if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: test.name}, current); err != nil || current.Spec.Lifecycle.CleanupRequested != test.want {
			t.Fatalf("cleanup intent for %s = (%t, %v), want (%t, nil)", test.name, current.Spec.Lifecycle.CleanupRequested, err, test.want)
		}
	}
	if len(responses.responses) != 0 {
		t.Fatalf("destroy responses=%+v", responses.responses)
	}
}

func TestChannelCleanupReportsNoMatchesAndListFailure(t *testing.T) {
	t.Run("no matches", func(t *testing.T) {
		bot, responses := botForTest(t, &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U2", ChannelID: "C1"}}})
		if err := bot.Handle(context.Background(), Envelope{ID: "none", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> done", Timestamp: "command-thread"}}); err != nil {
			t.Fatal(err)
		}
		if len(responses.responses) != 1 || responses.responses[0].Text != "You have no allocations in this channel. Use @servitor create to start one." {
			t.Fatalf("responses=%+v", responses.responses)
		}
	})
	t.Run("list failure", func(t *testing.T) {
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		kube := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("list failure")
		}}).Build()
		responses := &memoryResponder{}
		bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Responder: responses}
		if err := bot.Handle(context.Background(), Envelope{ID: "list-failure", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> done", Timestamp: "command-thread"}}); err != nil {
			t.Fatal(err)
		}
		if len(responses.responses) != 1 || responses.responses[0].Text != rejectedText("Unable to list your allocations. No cleanup was started.") {
			t.Fatalf("responses=%+v", responses.responses)
		}
	})
}

func TestThreadCleanupTargetsOnlyItsLifecycleThread(t *testing.T) {
	primary := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "primary"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "primary"}}}
	sibling := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "sibling"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "sibling"}}}
	bot, responses := botForTest(t, primary, sibling)
	if err := bot.Handle(context.Background(), Envelope{ID: "thread-cleanup", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "done", Timestamp: "reply", ThreadTimestamp: "primary"}}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		want bool
	}{{primary.Name, true}, {sibling.Name, false}} {
		current := &servitorv1alpha1.ServitorCluster{}
		if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: test.name}, current); err != nil || current.Spec.Lifecycle.CleanupRequested != test.want {
			t.Fatalf("cleanup intent for %s = (%t, %v), want (%t, nil)", test.name, current.Spec.Lifecycle.CleanupRequested, err, test.want)
		}
	}
	if len(responses.responses) != 1 || responses.responses[0].Text != "Cleaning up..." {
		t.Fatalf("responses=%+v", responses.responses)
	}
}

func TestReviewDecisionRecordsIntentAndAcknowledges(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(10 * time.Minute))
	for _, test := range []struct{ command, approval, response string }{
		{"yes", "approved", "Plan approved, creating...\nThis may take 30m-90m."},
		{"no", "rejected", "Plan rejected.\nCleaning up..."},
	} {
		t.Run(test.command, func(t *testing.T) {
			cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 14400}, ReviewDeadline: &deadline}}
			bot, responses := botForTest(t, cluster)
			if err := bot.Handle(context.Background(), Envelope{ID: test.command, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: test.command, Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 1 || responses.responses[0].Text != test.response {
				t.Fatalf("responses=%+v", responses.responses)
			}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, cluster); err != nil {
				t.Fatal(err)
			}
			if cluster.Spec.Lifecycle.Approval != test.approval {
				t.Fatalf("approval=%q, want %q", cluster.Spec.Lifecycle.Approval, test.approval)
			}
		})
	}
}

func TestReviewDecisionRejectsExpiredAndInvalidReviews(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name     string
		deadline time.Time
		phase    string
		response string
	}{
		{name: "at deadline", deadline: now, phase: servitorv1alpha1.PhaseAwaitingApproval, response: "The review deadline has passed. No decision was recorded; cleanup will begin."},
		{name: "after review", deadline: now.Add(time.Hour), phase: servitorv1alpha1.PhaseCleanupPending, response: "This plan is no longer awaiting review. No decision was recorded."},
	} {
		t.Run(test.name, func(t *testing.T) {
			deadline := metav1.NewTime(test.deadline)
			cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: test.phase, ReviewDeadline: &deadline}}
			bot, responses := botForTest(t, cluster)
			if err := bot.Handle(context.Background(), Envelope{ID: test.name, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 1 || responses.responses[0].Text != test.response || strings.Contains(responses.responses[0].Text, "Creating...") {
				t.Fatalf("responses=%+v", responses.responses)
			}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, cluster); err != nil {
				t.Fatal(err)
			}
			if cluster.Spec.Lifecycle.Approval != "" {
				t.Fatalf("invalid review recorded approval=%q", cluster.Spec.Lifecycle.Approval)
			}
		})
	}
}

func TestReviewDecisionRechecksStateAfterConflict(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ReviewDeadline: &deadline}}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	conflicted := false
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).WithInterceptorFuncs(interceptor.Funcs{Update: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.UpdateOption) error {
		if !conflicted {
			conflicted = true
			current := &servitorv1alpha1.ServitorCluster{}
			if err := underlying.Get(ctx, types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, current); err != nil {
				return err
			}
			current.Status.Phase = servitorv1alpha1.PhaseCleanupPending
			if err := underlying.Update(ctx, current); err != nil {
				return err
			}
			return apierrors.NewConflict(schema.GroupResource{Group: "servitor.bevicted.github.io", Resource: "servitorclusters"}, current.Name, errors.New("review expired"))
		}
		return underlying.Update(ctx, object, options...)
	}}).Build()
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Responder: responses, Clock: func() time.Time { return now }}
	if err := bot.Handle(context.Background(), Envelope{ID: "conflict", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || responses.responses[0].Text != "This plan is no longer awaiting review. No decision was recorded." || strings.Contains(responses.responses[0].Text, "Creating...") {
		t.Fatalf("responses=%+v", responses.responses)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Phase != servitorv1alpha1.PhaseCleanupPending || cluster.Spec.Lifecycle.Approval != "" {
		t.Fatalf("conflict retry recorded invalid intent: %+v", cluster)
	}
}

func TestUnauthorizedAndUnrelatedReviewThreadMessagesRemainSilent(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ReviewDeadline: &deadline}}
	bot, responses := botForTest(t, cluster)
	for _, message := range []Message{
		{Channel: "C1", ChannelType: "channel", User: "U2", Text: "yes", Timestamp: "reply", ThreadTimestamp: "root"},
		{Channel: "C1", ChannelType: "channel", User: "U1", Text: "sounds good", Timestamp: "reply", ThreadTimestamp: "root"},
	} {
		if err := bot.Handle(context.Background(), Envelope{ID: message.User + message.Text, Message: message}); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 0 {
		t.Fatalf("unauthorized or unrelated review responses=%+v", responses.responses)
	}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Lifecycle.Approval != "" {
		t.Fatalf("unauthorized decision recorded approval=%q", cluster.Spec.Lifecycle.Approval)
	}
}

func TestReviewDecisionControllerInterleavings(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name         string
		deadline     time.Time
		complete     bool
		wantPhase    string
		wantResponse []string
	}{
		{name: "timely approval reaches controller apply", deadline: now.Add(time.Minute), wantPhase: servitorv1alpha1.PhaseApplying, wantResponse: []string{"Plan approved, creating...\nThis may take 30m-90m."}},
		{name: "expired approval reaches terminal cleanup", deadline: now, complete: true, wantPhase: servitorv1alpha1.PhaseCleanupComplete, wantResponse: []string{"The review deadline has passed. No decision was recorded; cleanup will begin.", "Plan auto rejected due to missed approval deadline.\nCleaning up...", "Cleanup complete."}},
	} {
		t.Run(test.name, func(t *testing.T) {
			deadline := metav1.NewTime(test.deadline)
			cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor", UID: "uid", Generation: 1, Finalizers: []string{servitorv1alpha1.CleanupFinalizer}}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400, RetrySeconds: []int64{60}}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{}, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 14400, RetrySeconds: []int64{60}}, ReviewDeadline: &deadline, ReviewGeneration: 1}}
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := tektonv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithRuntimeObjects(cluster).Build()
			responses := &memoryResponder{}
			bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Responder: responses, Clock: func() time.Time { return now }}
			if err := bot.Handle(context.Background(), Envelope{ID: test.name, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, stored); err != nil {
				t.Fatal(err)
			}
			// The Kubernetes fake does not advance generation for spec updates.
			// Model the API-server generation that the controller authorization uses.
			if stored.Spec.Lifecycle.Approval != "" {
				stored.Generation++
				if err := bot.Client.Update(context.Background(), stored); err != nil {
					t.Fatal(err)
				}
			}
			reconciler := &controller.Reconciler{Client: bot.Client, Config: controller.Config{Namespace: "servitor"}, Now: func() time.Time { return now }}
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "servitor", Name: cluster.Name}}
			if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if test.complete {
				// Deliberately skip notifier polling while cleanup advances to its
				// observable terminal phase.
				if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
					t.Fatal(err)
				}
			}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Phase != test.wantPhase {
				t.Fatalf("phase=%q, want %q", stored.Status.Phase, test.wantPhase)
			}
			notifier := &StatusNotifier{Client: bot.Client, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(bot.Client, "servitor"), Clock: func() time.Time { return now }}
			if err := notifier.notify(context.Background()); err != nil {
				t.Fatal(err)
			}
			var transcript []string
			for _, response := range responses.responses {
				transcript = append(transcript, response.Text)
			}
			if !reflect.DeepEqual(transcript, test.wantResponse) {
				t.Fatalf("transcript=%q, want %q", transcript, test.wantResponse)
			}
		})
	}
}

func TestRejectedCommandReceiptSuppressesNotifierCauseDuplicate(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ReviewDeadline: &deadline}}
	bot, responses := botForTest(t, cluster)
	if err := bot.Handle(context.Background(), Envelope{ID: "reject", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "no", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	current := &servitorv1alpha1.ServitorCluster{}
	name := types.NamespacedName{Namespace: "servitor", Name: cluster.Name}
	if err := bot.Client.Get(context.Background(), name, current); err != nil {
		t.Fatal(err)
	}
	completed := metav1.NewTime(now)
	current.Status.Phase = servitorv1alpha1.PhaseCleanupComplete
	current.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonRejected, CompletedAt: &completed}
	if err := bot.Client.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	notifier := &StatusNotifier{Client: bot.Client, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(bot.Client, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	var transcript []string
	for _, response := range responses.responses {
		transcript = append(transcript, response.Text)
	}
	if want := []string{"Plan rejected.\nCleaning up...", "Cleanup complete."}; !reflect.DeepEqual(transcript, want) {
		t.Fatalf("transcript=%q, want %q", transcript, want)
	}
}

func TestFailedCommandCauseReplyRemainsRetryable(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ReviewDeadline: &deadline}}
	bot, responses := botForTest(t, cluster)
	responses.err = errors.New("Slack unavailable")
	if err := bot.Handle(context.Background(), Envelope{ID: "reject", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "no", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	causeID := cleanupCauseNoticeID(clusterNoticeUID(cluster), servitorv1alpha1.CleanupReasonRejected)
	if seen, err := bot.Events.Seen(context.Background(), causeID); err != nil || seen {
		t.Fatalf("failed command cause receipt = (%v, %v), want (false, nil)", seen, err)
	}
	current := &servitorv1alpha1.ServitorCluster{}
	name := types.NamespacedName{Namespace: "servitor", Name: cluster.Name}
	if err := bot.Client.Get(context.Background(), name, current); err != nil {
		t.Fatal(err)
	}
	completed := metav1.NewTime(now)
	current.Status.Phase = servitorv1alpha1.PhaseCleanupComplete
	current.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonRejected, CompletedAt: &completed}
	if err := bot.Client.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	responses.err = nil
	notifier := &StatusNotifier{Client: bot.Client, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(bot.Client, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 2 {
		t.Fatalf("retry responses = %+v", responses.responses)
	}
	if got := []string{responses.responses[0].Text, responses.responses[1].Text}; !reflect.DeepEqual(got, []string{"Plan rejected.\nCleaning up...", "Cleanup complete."}) {
		t.Fatalf("retry transcript = %q", got)
	}
}

func TestThreadOwnerMutatesOnlySpecIntent(t *testing.T) {
	expiry := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	deadline := metav1.NewTime(time.Date(2026, 9, 8, 1, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 14400}, ReviewDeadline: &deadline}}
	bot, _ := botForTest(t, cluster)
	if err := bot.Handle(context.Background(), Envelope{ID: "yes", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Lifecycle.Approval != "approved" || cluster.Status.Phase != servitorv1alpha1.PhaseAwaitingApproval {
		t.Fatalf("cluster=%+v", cluster)
	}
	cluster.Status.Phase = servitorv1alpha1.PhaseReady
	cluster.Status.LeaseExpiresAt = &expiry
	if err := bot.Client.Update(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "extend", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "extend 1h", Timestamp: "200.000001", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Lifecycle.RequestedExpiry == nil || !cluster.Spec.Lifecycle.RequestedExpiry.Time.Equal(expiry.Add(time.Hour)) || cluster.Spec.Lifecycle.ExtensionEventTimestamp != "200.000001" {
		t.Fatalf("extension=%+v", cluster.Spec.Lifecycle)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "foreign", Message: Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: "done", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Lifecycle.CleanupRequested {
		t.Fatal("foreign user requested cleanup")
	}
}
func TestChannelAndDMExtensionsDoNotMutateIntent(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	expiry := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &expiry, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600}}}
	bot, responses := botForTest(t, cluster)
	for _, event := range []Envelope{
		{ID: "channel-bare", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> extend", Timestamp: "100.000001"}},
		{ID: "channel-duration", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> extend 1h", Timestamp: "100.000002"}},
		{ID: "dm", Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "extend 1h", Timestamp: "100.000003"}},
	} {
		if err := bot.Handle(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 3 {
		t.Fatalf("extension responses = %+v", responses.responses)
	}
	if got := []string{responses.responses[0].Text, responses.responses[1].Text}; !reflect.DeepEqual(got, []string{bot.unknownText(), bot.unknownText()}) {
		t.Fatalf("channel extension responses = %q", got)
	}
	if got := responses.responses[2].Text; got != rejectedText("`extend [N[h]]` is available only in your lifecycle thread.") {
		t.Fatalf("DM extension response = %q", got)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Lifecycle.RequestedExpiry != nil || stored.Spec.Lifecycle.ExtensionEventTimestamp != "" {
		t.Fatalf("non-thread extension changed intent: %+v", stored.Spec.Lifecycle)
	}
}

func TestStaleExtensionDoesNotRecomputeTargetAfterReceiptEviction(t *testing.T) {
	expiry := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &expiry, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 14400}}}
	bot, _ := botForTest(t, cluster)
	store := bot.Events.(*state.EventStore)
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	store.Now = func() time.Time { now = now.Add(time.Nanosecond); return now }
	event := Envelope{ID: "extend-event", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "extend 1h", Timestamp: "100.000001", ThreadTimestamp: "root"}}
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	current := &servitorv1alpha1.ServitorCluster{}
	name := types.NamespacedName{Namespace: "servitor", Name: cluster.Name}
	if err := bot.Client.Get(context.Background(), name, current); err != nil {
		t.Fatal(err)
	}
	firstTarget := current.Spec.Lifecycle.RequestedExpiry.DeepCopy()
	current.Status.LeaseExpiresAt = firstTarget.DeepCopy()
	if err := bot.Client.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	for index := 0; index <= 1100; index++ {
		if _, err := store.Claim(context.Background(), fmt.Sprintf("other-event-%d", index)); err != nil {
			t.Fatal(err)
		}
	}
	if seen, err := store.Seen(context.Background(), event.ID); err != nil || seen {
		t.Fatalf("evicted event receipt Seen() = (%v, %v), want (false, nil)", seen, err)
	}
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := bot.Client.Get(context.Background(), name, current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Lifecycle.RequestedExpiry == nil || !current.Spec.Lifecycle.RequestedExpiry.Equal(firstTarget) {
		t.Fatalf("stale event changed target to %v, want %v", current.Spec.Lifecycle.RequestedExpiry, firstTarget)
	}
}

func TestCreateRejectsPlatformAndHelpDoesNotAdvertiseIt(t *testing.T) {
	bot, responses := botForTest(t)
	if err := bot.Handle(context.Background(), Envelope{ID: "platform", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create version=4.22 platform=kubernetes", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, `unknown create option "platform"`) {
		t.Fatalf("responses=%+v", responses.responses)
	}
	for _, page := range createHelp(command.CreateDefaults{}, 3) {
		if containsText(page, "platform=") {
			t.Fatalf("create help advertises platform: %s", page)
		}
	}
}

func TestCreateHelpDistinguishesDefaultsAliasesStreamsAndProvider(t *testing.T) {
	help := strings.Join(createHelp(command.CreateDefaults{}, 3), "\n")
	for _, wanted := range []string{"Configured defaults", "provider=", "key=value", "target=synthetic-target", "resource-group=\"Platform Team\"", "auth=true", "auth=false", "Public `auth`", "Private-only and Satellite", "roks", "iks", "k8s", "default_openshift", "default_kubernetes", "4.17"} {
		if !containsText(help, wanted) {
			t.Fatalf("create help missing %q: %s", wanted, help)
		}
	}
	for _, forbidden := range []string{"--key=value", "--key value", "--provider", "config="} {
		if containsText(help, forbidden) {
			t.Fatalf("create help advertises unsupported form %q: %s", forbidden, help)
		}
	}
}

func TestCreateRejectsIncompatibleAliasVersionWithoutAllocation(t *testing.T) {
	bot, responses := botForTest(t)
	event := Envelope{ID: "alias-conflict", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create roks 1.34", Timestamp: "123"}}
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, "incompatible") {
		t.Fatalf("responses=%+v", responses.responses)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, cluster); err == nil {
		t.Fatal("incompatible alias/version recorded an allocation")
	}
}

func TestHelpDoesNotExposeMaintainerControls(t *testing.T) {
	bot, responses := botForTest(t)
	if err := bot.Handle(context.Background(), Envelope{ID: "help", Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "help"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 {
		t.Fatal("missing help")
	}
	for _, forbidden := range []string{"status", "pause", "unpause", "stop", "Maintainer", "@servitor extend"} {
		if containsText(responses.responses[0].Text, forbidden) {
			t.Fatalf("help exposed %q: %s", forbidden, responses.responses[0].Text)
		}
	}
}
func TestHandleListReportsCallerAllocationsInDMAndChannel(t *testing.T) {
	run := func(t *testing.T, bot Bot, responses *memoryResponder, expected, absent []string) {
		t.Helper()
		for _, test := range []struct {
			name, channel, channelType, text, timestamp, thread string
		}{
			{name: "DM", channel: "D1", channelType: "im", text: "list", timestamp: "dm-list"},
			{name: "mentioned channel", channel: "C1", channelType: "channel", text: "<@BOT> list", timestamp: "channel-list", thread: "channel-list"},
		} {
			t.Run(test.name, func(t *testing.T) {
				before := len(responses.responses)
				event := Envelope{ID: "list-" + test.name, Message: Message{Channel: test.channel, ChannelType: test.channelType, User: "Ucaller", Text: test.text, Timestamp: test.timestamp}}
				if err := bot.Handle(context.Background(), event); err != nil {
					t.Fatal(err)
				}
				if len(responses.responses) != before+1 {
					t.Fatalf("responses = %+v", responses.responses[before:])
				}
				response := responses.responses[before]
				if response.Channel != test.channel || response.ThreadTimestamp != test.thread {
					t.Fatalf("response routing = %+v", response)
				}
				if !strings.HasPrefix(response.Text, "Your allocations:\n```\n") {
					t.Fatalf("response has no list heading and table: %s", response.Text)
				}
				header := strings.Split(response.Text, "\n")[2]
				if fields := strings.Fields(header); len(fields) != 4 || strings.Join(fields, ",") != "cluster,state,location,expires" {
					t.Fatalf("table header = %q", header)
				}
				for _, want := range expected {
					if !strings.Contains(response.Text, want) {
						t.Fatalf("response missing %q: %s", want, response.Text)
					}
				}
				for _, value := range absent {
					if strings.Contains(response.Text, value) {
						t.Fatalf("response rendered another owner's value %q: %s", value, response.Text)
					}
				}
			})
		}
	}

	t.Run("only other owners", func(t *testing.T) {
		expires := metav1.NewTime(time.Date(2031, 1, 2, 3, 4, 5, 0, time.UTC))
		other := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Uother"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseApplying, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "other-owner-sentinel", Region: "eu-de"}, LeaseExpiresAt: &expires}}
		bot, responses := botForTest(t, other)
		run(t, bot, responses, nil, []string{"other-owner-sentinel", "applying", "eu-de", "2031-01-02 03:04:05 UTC", "Uother"})
	})

	t.Run("redacts other persisted owner IDs", func(t *testing.T) {
		caller := &servitorv1alpha1.ServitorCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "caller", Namespace: "servitor"},
			Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}},
			Status: servitorv1alpha1.ServitorClusterStatus{
				Phase:           servitorv1alpha1.PhaseReady,
				ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "servitor-private-owner-id", Region: "us-south/private-owner-id"},
			},
		}
		other := &servitorv1alpha1.ServitorCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "servitor"},
			Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "private-owner-id"}},
		}
		bot, responses := botForTest(t, caller, other)
		run(t, bot, responses, []string{"ready"}, []string{"private-owner-id"})
	})

	t.Run("all caller lifecycle phases", func(t *testing.T) {
		phases := []string{
			servitorv1alpha1.PhasePending,
			servitorv1alpha1.PhasePlanning,
			servitorv1alpha1.PhaseAwaitingApproval,
			servitorv1alpha1.PhaseApplying,
			servitorv1alpha1.PhaseReady,
			servitorv1alpha1.PhaseCleanupPending,
			servitorv1alpha1.PhaseCleanupComplete,
			servitorv1alpha1.PhaseUnresolved,
		}
		objects := make([]runtime.Object, 0, len(phases)+1)
		for index, phase := range phases {
			name, location := fmt.Sprintf("cluster-%d", index), "us-south/us-south-1"
			if phase == servitorv1alpha1.PhaseAwaitingApproval {
				name, location = "servitor-260914043417-26841e2f", "us-south"
			}
			objects = append(objects, &servitorv1alpha1.ServitorCluster{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("allocation-%d", index), Namespace: "servitor"},
				Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}},
				Status:     servitorv1alpha1.ServitorClusterStatus{Phase: phase, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: name, Region: location}},
			})
		}
		objects = append(objects, &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Uother"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "other-owner-sentinel", Region: "eu-de"}}})
		bot, responses := botForTest(t, objects...)
		run(t, bot, responses, []string{"servitor-260914043417-26841e2f", "review", "us-south", "never", "planning", "applying", "ready", "cleanup in progress", "cleanup complete", "unresolved"}, []string{"other-owner-sentinel", "eu-de", "Ucaller", "Uother", "*"})
	})
}

func TestListHelpExplainsCallerOnlyAllocationsAndCleanupStates(t *testing.T) {
	bot, responses := botForTest(t)
	if err := bot.Handle(context.Background(), Envelope{ID: "list-help", Message: Message{Channel: "D1", ChannelType: "im", User: "Ucaller", Text: "help list"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 {
		t.Fatalf("responses = %+v", responses.responses)
	}
	for _, want := range []string{"only your allocations", "cluster, state, location, and expires", "Cleanup in progress", "cleanup complete"} {
		if !strings.Contains(responses.responses[0].Text, want) {
			t.Fatalf("list help missing %q: %s", want, responses.responses[0].Text)
		}
	}
	if strings.Contains(responses.responses[0].Text, "marks your allocation") {
		t.Fatalf("list help retained marker legend: %s", responses.responses[0].Text)
	}
}

func TestLifecycleHelpExplainsAvailableStates(t *testing.T) {
	bot, responses := botForTest(t)
	for _, topic := range []struct {
		command string
		wanted  []string
	}{
		{command: "help done", wanted: []string{"lifecycle thread", "all of your allocations in that channel", "cleanup completes", "cleanup in progress"}},
		{command: "help extend", wanted: []string{"only in your lifecycle thread", "only while the lease is ready", "planning, cleanup, or after expiry"}},
	} {
		responses.responses = nil
		if err := bot.Handle(context.Background(), Envelope{ID: topic.command, Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: topic.command}}); err != nil {
			t.Fatal(err)
		}
		if len(responses.responses) != 1 {
			t.Fatalf("%s responses=%+v", topic.command, responses.responses)
		}
		for _, wanted := range topic.wanted {
			if !containsText(responses.responses[0].Text, wanted) {
				t.Fatalf("%s help missing %q: %s", topic.command, wanted, responses.responses[0].Text)
			}
		}
		if topic.command == "help extend" && containsText(responses.responses[0].Text, "@servitor extend") {
			t.Fatalf("%s help advertised channel extension: %s", topic.command, responses.responses[0].Text)
		}
	}
}

func TestCreateMatchesPublishedInventoryBareValues(t *testing.T) {
	bot, responses := botWithPublishedInventory(t, false)
	event := Envelope{ID: "mixed-bare", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create bx2.4x16 Platform\\ Team target-a vpc-gen2 us-south-1 version=4.22", Timestamp: "123"}}
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "123")}, cluster); err != nil {
		t.Fatal(err)
	}
	want := servitorv1alpha1.UserOptions{Target: "target-a", Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Platform Team", Zone: "us-south-1", Flavor: "bx2.4x16"}
	if !reflect.DeepEqual(cluster.Spec.UserOptions, want) || len(responses.responses) != 1 {
		t.Fatalf("bare create = %+v, responses=%+v", cluster.Spec.UserOptions, responses.responses)
	}

	explicit, _ := botWithPublishedInventory(t, false)
	if err := explicit.Handle(context.Background(), Envelope{ID: "explicit", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create target=target-a provider=vpc-gen2 version=4.22 resource-group=Platform\\ Team zone=us-south-1 flavor=bx2.4x16", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	explicitCluster := &servitorv1alpha1.ServitorCluster{}
	if err := explicit.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "123")}, explicitCluster); err != nil || !reflect.DeepEqual(explicitCluster.Spec.UserOptions, want) {
		t.Fatalf("explicit create = %+v, %v", explicitCluster.Spec.UserOptions, err)
	}

	defaults, _ := botWithPublishedInventory(t, false)
	if err := defaults.Handle(context.Background(), Envelope{ID: "defaults", Message: Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: "<@BOT> create Platform\\ Team", Timestamp: "124"}}); err != nil {
		t.Fatal(err)
	}
	defaultCluster := &servitorv1alpha1.ServitorCluster{}
	if err := defaults.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "124")}, defaultCluster); err != nil || !reflect.DeepEqual(defaultCluster.Spec.UserOptions, servitorv1alpha1.UserOptions{ResourceGroup: "Platform Team"}) {
		t.Fatalf("default create = %+v, %v", defaultCluster.Spec.UserOptions, err)
	}
}

func TestCreateBareValuesRequireCurrentInventoryButKeysProceed(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot expired=%t", expired), func(t *testing.T) {
			bot, responses := botWithPublishedInventory(t, expired)
			if !expired {
				if err := state.NewInventoryStore(bot.Client, bot.Namespace).Delete(context.Background(), "target-a"); err != nil {
					t.Fatal(err)
				}
			}
			if err := bot.Handle(context.Background(), Envelope{ID: "bare", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create Platform\\ Team", Timestamp: "123"}}); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, "inventory is") {
				t.Fatalf("bare response=%+v", responses.responses)
			}
			cluster := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "123")}, cluster); err == nil {
				t.Fatal("bare value created an allocation without a usable snapshot")
			}
		})
	}

	bot, _ := botWithPublishedInventory(t, false)
	store := state.NewInventoryStore(bot.Client, bot.Namespace)
	if err := store.Delete(context.Background(), "target-a"); err != nil {
		t.Fatal(err)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "keyed", Message: Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: "<@BOT> create resource-group=Uncatalogued", Timestamp: "124"}}); err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "124")}, cluster); err != nil || cluster.Spec.UserOptions.ResourceGroup != "Uncatalogued" {
		t.Fatalf("keyed create = %+v, %v", cluster.Spec.UserOptions, err)
	}
	for _, text := range []string{"<@BOT> create target=unknown resource-group=value", "<@BOT> create provider=classic resource-group=value"} {
		invalid, responses := botWithPublishedInventory(t, false)
		if err := invalid.Handle(context.Background(), Envelope{ID: text, Message: Message{Channel: "C1", ChannelType: "channel", User: "U3", Text: text, Timestamp: "125"}}); err != nil {
			t.Fatal(err)
		}
		if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, "not configured") {
			t.Fatalf("invalid selector response=%+v", responses.responses)
		}
	}
}

func botWithPublishedInventory(t *testing.T, expired bool) (Bot, *memoryResponder) {
	t.Helper()
	configData := "version: 1\ntargets:\n  target-a:\n    providers: [vpc-gen2]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://resource-manager.example.invalid\n      resource_controller: https://resource-controller.example.invalid\n      vpc: https://vpc.{region}.example.invalid\n"
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "targets", Namespace: "servitor"}, Data: map[string]string{"config.yaml": configData}}
	bot, responses := botForTest(t, configMap)
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	bot.Clock = func() time.Time { return now }
	bot.Defaults = command.CreateDefaults{Target: "target-a", Provider: "vpc-gen2", Zone: "us-south-1"}
	bot.InventoryConfigMap, bot.InventoryConfigKey, bot.InventoryMaximumAge = "targets", "config.yaml", time.Hour
	target := inventory.TargetConfig{Providers: []string{"vpc-gen2"}, DefaultRegion: "us-south", Endpoints: inventory.Endpoints{IAM: "https://iam.example.invalid", ContainerService: "https://containers.example.invalid", GlobalTagging: "https://tagging.example.invalid", ResourceManagement: "https://resource-manager.example.invalid", ResourceController: "https://resource-controller.example.invalid", VPC: "https://vpc.{region}.example.invalid"}}
	revision, err := inventory.Revision(target)
	if err != nil {
		t.Fatal(err)
	}
	catalog := inventory.Catalog{Version: inventory.CatalogVersion, Target: "target-a", Providers: []string{"vpc-gen2"}, Versions: []inventory.Version{{Name: "4.22_openshift", Platform: "openshift", Default: true, Supported: true}}, ResourceGroups: []string{"Platform Team"}, VPCLocations: []inventory.Location{{Name: "us-south-1", Flavors: []string{"bx2.4x16"}}}}
	published := now
	if expired {
		published = now.Add(-2 * time.Hour)
	}
	if _, err := state.NewInventoryStore(bot.Client, bot.Namespace).Update(context.Background(), "target-a", func(current *state.InventoryState) error {
		current.Revision, current.Catalog, current.Disposition = revision, &catalog, state.InventorySucceeded
		current.PublishedAt = ptrTime(published)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return bot, responses
}

func ptrTime(value time.Time) *metav1.Time { result := metav1.NewTime(value); return &result }

func TestLifecycleCommandsExplainStateAndRecordOnlyAcceptedIntent(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	expiry := metav1.NewTime(now.Add(time.Hour))
	cluster := func(phase string) *servitorv1alpha1.ServitorCluster {
		return &servitorv1alpha1.ServitorCluster{
			ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"},
			Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600}},
			Status:     servitorv1alpha1.ServitorClusterStatus{Phase: phase, LeaseExpiresAt: expiry.DeepCopy(), LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600}},
		}
	}
	message := func(text string) Envelope {
		event := Envelope{ID: text, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> " + text, Timestamp: "100.000001"}}
		if firstToken(text) == "extend" {
			event.Message.Text = text
			event.Message.ThreadTimestamp = "root"
		}
		return event
	}

	for _, test := range []struct {
		name     string
		phase    string
		command  string
		prepare  func(*servitorv1alpha1.ServitorCluster)
		response string
		mutated  bool
	}{
		{name: "planning extension", phase: servitorv1alpha1.PhasePlanning, command: "extend 1h", response: "Planning is in progress. Extend is available when your cluster is ready."},
		{name: "cleanup extension", phase: servitorv1alpha1.PhaseCleanupPending, command: "extend 1h", response: "Cleanup is in progress. The lease cannot be extended."},
		{name: "repeated cleanup", phase: servitorv1alpha1.PhaseCleanupPending, command: "done", response: "Cleanup requested for 0 allocation(s).\nAlready cleaning: 1 allocation(s)."},
		{name: "completed cleanup", phase: servitorv1alpha1.PhaseCleanupComplete, command: "done", response: "Cleanup requested for 0 allocation(s).\nCleanup complete: 1 allocation(s)."},
		{name: "expired extension", phase: servitorv1alpha1.PhaseReady, command: "extend 1h", prepare: func(cluster *servitorv1alpha1.ServitorCluster) {
			expired := metav1.NewTime(now)
			cluster.Status.LeaseExpiresAt = &expired
		}, response: "Your lease has expired. Cleanup will begin; use @servitor create after cleanup completes."},
		{name: "ready extension", phase: servitorv1alpha1.PhaseReady, command: "extend 1h", mutated: true},
		{name: "planning cleanup", phase: servitorv1alpha1.PhasePlanning, command: "done", response: "Cleanup requested for 1 allocation(s).", mutated: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			allocation := cluster(test.phase)
			if test.prepare != nil {
				test.prepare(allocation)
			}
			bot, responses := botForTest(t, allocation)
			bot.Clock = func() time.Time { return now }
			if err := bot.Handle(context.Background(), message(test.command)); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 0 {
				if responses.responses[len(responses.responses)-1].Text != test.response {
					t.Fatalf("responses=%+v, want final response %q", responses.responses, test.response)
				}
			} else if test.response != "" {
				t.Fatalf("responses=%+v, want %q", responses.responses, test.response)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocation.Name}, stored); err != nil {
				t.Fatal(err)
			}
			if test.mutated != (stored.Spec.Lifecycle.CleanupRequested || stored.Spec.Lifecycle.RequestedExpiry != nil) {
				t.Fatalf("intent=%+v, mutated=%t", stored.Spec.Lifecycle, test.mutated)
			}
		})
	}

	t.Run("no allocation", func(t *testing.T) {
		bot, responses := botForTest(t)
		if err := bot.Handle(context.Background(), message("done")); err != nil {
			t.Fatal(err)
		}
		if len(responses.responses) != 1 || responses.responses[0].Text != "You have no allocations in this channel. Use @servitor create to start one." {
			t.Fatalf("responses=%+v", responses.responses)
		}
	})
}

func TestLifecycleCommandsKeepForeignAndWrongThreadRequestsSilent(t *testing.T) {
	expiry := metav1.NewTime(time.Date(2026, 9, 8, 1, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &expiry, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600}}}
	bot, responses := botForTest(t, cluster)
	for _, event := range []Envelope{
		{ID: "foreign", Message: Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: "extend nonsense", Timestamp: "100.000001", ThreadTimestamp: "root"}},
		{ID: "wrong-thread", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "extend 1h", Timestamp: "100.000002", ThreadTimestamp: "other"}},
	} {
		if err := bot.Handle(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 0 {
		t.Fatalf("responses=%+v", responses.responses)
	}
	stored := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Lifecycle.CleanupRequested || stored.Spec.Lifecycle.RequestedExpiry != nil {
		t.Fatalf("foreign or wrong-thread request changed intent: %+v", stored.Spec.Lifecycle)
	}
}

func TestExtensionFeedbackAfterPersistenceFailureAndConflict(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	expiry := metav1.NewTime(now.Add(time.Hour))
	cluster := func() *servitorv1alpha1.ServitorCluster {
		return &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: expiry.DeepCopy(), LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 3600}}}
	}
	event := Envelope{ID: "extend", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "extend 1h", Timestamp: "100.000001", ThreadTimestamp: "root"}}
	t.Run("persistence failure", func(t *testing.T) {
		recorder := &progressRecorder{}
		bot := progressBotForTest(t, recorder, false, true, cluster())
		bot.Clock = func() time.Time { return now }
		if err := bot.Handle(context.Background(), event); err != nil {
			t.Fatal(err)
		}
		want := []string{"kubernetes update", "reply: Command rejected.\n\nUnable to record the lease extension."}
		if !reflect.DeepEqual(recorder.events, want) {
			t.Fatalf("events=%q, want %q", recorder.events, want)
		}
		stored := &servitorv1alpha1.ServitorCluster{}
		if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocationClusterName("C1", "root")}, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Spec.Lifecycle.RequestedExpiry != nil {
			t.Fatalf("failed persistence recorded extension: %+v", stored.Spec.Lifecycle)
		}
	})
	t.Run("conflict changes phase", func(t *testing.T) {
		allocation := cluster()
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		conflicted := false
		kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(allocation).WithInterceptorFuncs(interceptor.Funcs{Update: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.UpdateOption) error {
			if !conflicted {
				conflicted = true
				current := &servitorv1alpha1.ServitorCluster{}
				if err := underlying.Get(ctx, types.NamespacedName{Namespace: "servitor", Name: allocation.Name}, current); err != nil {
					return err
				}
				current.Status.Phase = servitorv1alpha1.PhaseCleanupPending
				if err := underlying.Update(ctx, current); err != nil {
					return err
				}
				return apierrors.NewConflict(schema.GroupResource{Group: "servitor.bevicted.github.io", Resource: "servitorclusters"}, current.Name, errors.New("lease expired"))
			}
			return underlying.Update(ctx, object, options...)
		}}).Build()
		responses := &memoryResponder{}
		bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Responder: responses, Clock: func() time.Time { return now }}
		if err := bot.Handle(context.Background(), event); err != nil {
			t.Fatal(err)
		}
		if len(responses.responses) != 1 || responses.responses[0].Text != "Cleanup is in progress. The lease cannot be extended." {
			t.Fatalf("responses=%+v", responses.responses)
		}
		stored := &servitorv1alpha1.ServitorCluster{}
		if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: allocation.Name}, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Spec.Lifecycle.RequestedExpiry != nil {
			t.Fatalf("conflict retry recorded extension after cleanup won: %+v", stored.Spec.Lifecycle)
		}
	})
}

func containsText(text, part string) bool {
	for i := 0; i+len(part) <= len(text); i++ {
		if text[i:i+len(part)] == part {
			return true
		}
	}
	return false
}

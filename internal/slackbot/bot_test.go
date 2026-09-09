package slackbot

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type memoryResponder struct {
	responses []Response
	err       error
}

func (r *memoryResponder) Reply(_ context.Context, response Response) error {
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
	if err := bot.Handle(context.Background(), Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create --version 4.22", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || responses.responses[0].Text != "Command accepted.\nPlanning..." {
		t.Fatalf("responses=%+v", responses.responses)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: ownerClusterName("U1")}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Slack.OwnerID != "U1" || cluster.Spec.Slack.ThreadTimestamp != "123" || cluster.Spec.UserOptions.Version != "4.22" || cluster.Spec.UserOptions.Provider != "" || cluster.Spec.Lifecycle.InitialLeaseSeconds != int64(bot.Lease/time.Second) || cluster.Spec.Lifecycle.RetrySeconds[0] != int64(time.Minute/time.Second) {
		t.Fatalf("cluster=%+v", cluster.Spec)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create --version 4.22", Timestamp: "123"}}); err != nil {
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
func TestHandleCreateNormalizesMixedAssignmentsWithoutDefaults(t *testing.T) {
	want := servitorv1alpha1.UserOptions{
		Target: "synthetic-target", Version: "4.22", ResourceGroup: "Platform Team=Core", WorkerCount: 3,
		SubnetIDs: []string{"subnet-one", "subnet-two"},
	}
	for _, text := range []string{
		`<@BOT> create --target synthetic-target --version 4.22 --resource-group "Platform Team=Core" --worker-count 3 --subnet-id subnet-one --subnet-id subnet-two`,
		`<@BOT> create target=synthetic-target --version=4.22 resource-group="Platform Team=Core" --worker-count 3 subnet-id=subnet-one --subnet-id subnet-two`,
	} {
		t.Run(text, func(t *testing.T) {
			bot, responses := botForTest(t)
			bot.Defaults = command.CreateDefaults{Provider: "vpc-gen2", Zone: "operator-zone"}
			event := Envelope{ID: "mixed", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: text, Timestamp: "123"}}
			if err := bot.Handle(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 1 || responses.responses[0].Text != "Command accepted.\nPlanning..." {
				t.Fatalf("responses=%+v", responses.responses)
			}
			cluster := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: ownerClusterName("U1")}, cluster); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cluster.Spec.UserOptions, want) {
				t.Fatalf("userOptions = %+v\nwant %+v", cluster.Spec.UserOptions, want)
			}
		})
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
			if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, "--worker-count must be an integer from 1 through 100") {
				t.Fatalf("responses=%+v", responses.responses)
			}
			cluster := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: ownerClusterName("U1")}, cluster); err == nil {
				t.Fatal("invalid worker count recorded an allocation")
			}
		})
	}
}

func TestCreateRejectsCallerSelectedName(t *testing.T) {
	bot, responses := botForTest(t)
	if err := bot.Handle(context.Background(), Envelope{ID: "Ev-name", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create --version 4.22 --name caller-selected", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, `unknown create flag "--name"`) {
		t.Fatalf("responses=%+v", responses.responses)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: ownerClusterName("U1")}, cluster); err == nil {
		t.Fatal("Slack create with name recorded an allocation")
	}
}

func TestCreateDoesNotProceedWhenAcceptanceDeliveryFails(t *testing.T) {
	bot, responses := botForTest(t)
	event := Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create --version 4.22", Timestamp: "123"}}
	responses.err = errors.New("Slack unavailable")
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	name := types.NamespacedName{Namespace: "servitor", Name: ownerClusterName("U1")}
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

func TestReviewDecisionIsAcknowledgedImmediately(t *testing.T) {
	for _, test := range []struct{ command, approval, response string }{
		{"yes", "approved", "Plan approved.\nCreating... This may take 30m-90m."},
		{"no", "rejected", "Plan rejected.\nCleaning up..."},
	} {
		t.Run(test.command, func(t *testing.T) {
			cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: ownerClusterName("U1"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 14400}}}
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

func TestThreadOwnerMutatesOnlySpecIntent(t *testing.T) {
	expiry := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: ownerClusterName("U1"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 14400}}}
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
	if err := bot.Handle(context.Background(), Envelope{ID: "extend", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "extend 1h", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Lifecycle.RequestedExpiry == nil || !cluster.Spec.Lifecycle.RequestedExpiry.Time.Equal(expiry.Add(time.Hour)) {
		t.Fatalf("extension=%+v", cluster.Spec.Lifecycle.RequestedExpiry)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "foreign", Message: Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: "done", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Lifecycle.CleanupRequested {
		t.Fatal("foreign user requested cleanup")
	}
}
func TestStaleExtensionDoesNotRecomputeTargetAfterReceiptEviction(t *testing.T) {
	expiry := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: ownerClusterName("U1"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 14400}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &expiry, LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{InitialLeaseSeconds: 14400}}}
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
	if err := bot.Handle(context.Background(), Envelope{ID: "platform", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create --version 4.22 --platform kubernetes", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || !containsText(responses.responses[0].Text, `unknown create flag "--platform"`) {
		t.Fatalf("responses=%+v", responses.responses)
	}
	for _, page := range createHelp(command.CreateDefaults{}) {
		if containsText(page, "--platform") {
			t.Fatalf("create help advertises platform: %s", page)
		}
	}
}

func TestCreateHelpDistinguishesDefaultsAliasesStreamsAndProvider(t *testing.T) {
	help := strings.Join(createHelp(command.CreateDefaults{}), "\n")
	for _, wanted := range []string{"Configured defaults", "--provider", "key=value", "--key=value", "--key value", "target=synthetic-target", "resource-group \"Platform Team\"", "roks", "iks", "k8s", "default_openshift", "default_kubernetes", "4.17"} {
		if !containsText(help, wanted) {
			t.Fatalf("create help missing %q: %s", wanted, help)
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
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: ownerClusterName("U1")}, cluster); err == nil {
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
	for _, forbidden := range []string{"status", "pause", "unpause", "stop", "Maintainer"} {
		if containsText(responses.responses[0].Text, forbidden) {
			t.Fatalf("help exposed %q: %s", forbidden, responses.responses[0].Text)
		}
	}
}
func containsText(text, part string) bool {
	for i := 0; i+len(part) <= len(text); i++ {
		if text[i:i+len(part)] == part {
			return true
		}
	}
	return false
}

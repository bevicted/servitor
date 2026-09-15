package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/controller"
	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestMultipleThreadAllocationsAdmissionAndCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)
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
	created := 0
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}).WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.CreateOption) error {
		if cluster, ok := object.(*servitorv1alpha1.ServitorCluster); ok {
			created++
			cluster.UID = types.UID(fmt.Sprintf("allocation-uid-%d", created))
		}
		return underlying.Create(ctx, object, options...)
	}}).Build()
	responses := &replies{}
	bot := slackbot.Bot{
		ChannelID: "C1", SelfUserID: "BOT", Namespace: "ns", Client: kube,
		Events: state.NewEventStore(kube, "ns"), Responder: responses,
		Defaults: command.CreateDefaults{Target: "target", Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Default"},
		Lease:    4 * time.Hour, RetryIntervals: []time.Duration{time.Minute}, Clock: func() time.Time { return now },
	}
	create := func(id, owner, timestamp string) {
		t.Helper()
		if err := bot.Handle(ctx, slackbot.Envelope{ID: id, Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: owner, Text: "<@BOT> create version=4.22", Timestamp: timestamp}}); err != nil {
			t.Fatal(err)
		}
	}
	threads := []string{"1710000000.000100", "1710000001.000100", "1710000002.000100"}
	for index, thread := range threads {
		create(fmt.Sprintf("create-%d", index), "U1", thread)
	}
	create("over-cap", "U1", "1710000003.000100")
	if got := responses.values[len(responses.values)-1].Text; !strings.Contains(got, "limit of 3") || strings.Contains(got, "Planning...") {
		t.Fatalf("over-cap response = %q", got)
	}
	create("replay-with-new-receipt", "U1", threads[0])
	if got := responses.values[len(responses.values)-1].Text; !strings.Contains(got, "already have an allocation") {
		t.Fatalf("same-thread replay = %q", got)
	}
	create("other-owner", "U2", "1710000004.000100")

	var allocations servitorv1alpha1.ServitorClusterList
	if err := kube.List(ctx, &allocations, client.InNamespace("ns")); err != nil {
		t.Fatal(err)
	}
	if len(allocations.Items) != 4 {
		t.Fatalf("stored allocations = %d, want 4", len(allocations.Items))
	}
	byThread := make(map[string]*servitorv1alpha1.ServitorCluster, len(allocations.Items))
	for index := range allocations.Items {
		allocation := allocations.Items[index].DeepCopy()
		byThread[allocation.Spec.Slack.ThreadTimestamp] = allocation
	}
	for _, thread := range threads {
		if byThread[thread] == nil {
			t.Fatalf("missing allocation for thread %q", thread)
		}
	}

	reconciler := &controller.Reconciler{Client: kube, DirectReader: kube, Scheme: scheme, Config: controller.Config{
		Namespace: "ns", Defaults: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Default"}, Platform: "openshift"},
		Backend: servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Region: "us-south", Endpoint: "https://s3.us-south.example.invalid"}, BackendPrefix: "servitor",
		ExecutionImage: "registry.example/ict@sha256:deadbeef", ReviewTimeout: time.Minute,
	}, Now: func() time.Time { return now }}
	for _, thread := range threads {
		request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: byThread[thread].Name}}
		for range 4 {
			if _, err := reconciler.Reconcile(ctx, request); err != nil {
				t.Fatal(err)
			}
		}
	}
	backendKeys := make([]string, 0, len(threads))
	for _, thread := range threads {
		stored := &servitorv1alpha1.ServitorCluster{}
		if err := kube.Get(ctx, types.NamespacedName{Namespace: "ns", Name: byThread[thread].Name}, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Status.Backend == nil || stored.Status.Operation == nil || !strings.HasSuffix(stored.Status.Backend.Key, string(stored.UID)+".tfstate") {
			t.Fatalf("backend for %s = %#v", thread, stored.Status.Backend)
		}
		run := &tektonv1.PipelineRun{}
		if err := kube.Get(ctx, types.NamespacedName{Namespace: "ns", Name: stored.Status.Operation.PipelineRunName}, run); err != nil {
			t.Fatal(err)
		}
		backendParameter := ""
		for _, parameter := range run.Spec.Params {
			if parameter.Name == "backend" {
				backendParameter = parameter.Value.StringVal
			}
		}
		if !strings.Contains(backendParameter, stored.Status.Backend.Key) {
			t.Fatalf("PipelineRun backend for %s = %q, want %q", thread, backendParameter, stored.Status.Backend.Key)
		}
		backendKeys = append(backendKeys, stored.Status.Backend.Key)
	}
	sort.Strings(backendKeys)
	if backendKeys[0] == backendKeys[1] || backendKeys[1] == backendKeys[2] {
		t.Fatalf("thread allocations share backend keys: %v", backendKeys)
	}

	first := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "ns", Name: byThread[threads[0]].Name}, first); err != nil {
		t.Fatal(err)
	}
	first.Status.Phase = servitorv1alpha1.PhaseAwaitingApproval
	first.Status.ReviewDeadline = &metav1.Time{Time: now.Add(time.Hour)}
	if err := kube.Status().Update(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := bot.Handle(ctx, slackbot.Envelope{ID: "approve-first", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "1710000010.000100", ThreadTimestamp: threads[0]}}); err != nil {
		t.Fatal(err)
	}
	for _, thread := range threads {
		stored := &servitorv1alpha1.ServitorCluster{}
		if err := kube.Get(ctx, types.NamespacedName{Namespace: "ns", Name: byThread[thread].Name}, stored); err != nil {
			t.Fatal(err)
		}
		if (thread == threads[0]) != (stored.Spec.Lifecycle.Approval == "approved") {
			t.Fatalf("approval isolation for %s = %q", thread, stored.Spec.Lifecycle.Approval)
		}
	}

	if err := bot.Handle(ctx, slackbot.Envelope{ID: "list", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> list", Timestamp: "1710000011.000100"}}); err != nil {
		t.Fatal(err)
	}
	if got := responses.values[len(responses.values)-1]; got.ThreadTimestamp != "1710000011.000100" || !strings.HasPrefix(got.Text, "Your allocations:") {
		t.Fatalf("list response = %#v", got)
	}
	if err := bot.Handle(ctx, slackbot.Envelope{ID: "cleanup-all", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> done", Timestamp: "1710000012.000100"}}); err != nil {
		t.Fatal(err)
	}
	for _, thread := range threads {
		stored := &servitorv1alpha1.ServitorCluster{}
		if err := kube.Get(ctx, types.NamespacedName{Namespace: "ns", Name: byThread[thread].Name}, stored); err != nil {
			t.Fatal(err)
		}
		if !stored.Spec.Lifecycle.CleanupRequested {
			t.Fatalf("cleanup missing for %s", thread)
		}
		stored.Status.Phase = servitorv1alpha1.PhaseCleanupComplete
		if err := kube.Status().Update(ctx, stored); err != nil {
			t.Fatal(err)
		}
	}
	other := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "ns", Name: byThread["1710000004.000100"].Name}, other); err != nil {
		t.Fatal(err)
	}
	if other.Spec.Lifecycle.CleanupRequested {
		t.Fatal("channel cleanup changed another owner's allocation")
	}
	create("capacity-regained", "U1", "1710000005.000100")
	if got := responses.values[len(responses.values)-1].Text; got != "Planning..." {
		t.Fatalf("post-cleanup create = %q", got)
	}
}

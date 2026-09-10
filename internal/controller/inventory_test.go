package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/inventory"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type inventoryLogs struct{ data []byte }

type inventoryBotResponder struct{ responses []slackbot.Response }

func (r *inventoryBotResponder) Reply(_ context.Context, response slackbot.Response) error {
	r.responses = append(r.responses, response)
	return nil
}

func (l *inventoryLogs) ReadContainerLog(context.Context, string, string, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.data)), nil
}

func TestInventoryRefreshPublishesHourlyAndAdoptsPersistedRun(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, logs := newInventoryHarness(t, &now, targetConfigYAML("vpc-gen2"))
	allocation := &tektonv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{Name: "allocation", Namespace: "servitor", Labels: map[string]string{pipeline.ClusterUIDLabel: "cluster", pipeline.OperationLabel: "apply"}}}
	if err := kube.Create(context.Background(), allocation); err != nil {
		t.Fatal(err)
	}

	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := state.NewInventoryStore(kube, "servitor")
	first, err := store.Get(context.Background(), "target-a")
	if err != nil || first.ActiveRunID == "" || first.RunDeadlineAt == nil || !first.RunDeadlineAt.Time.Equal(now.Add(pipeline.InventoryRunTimeout)) || first.Catalog != nil || first.Revision == "" {
		t.Fatalf("initial state = %+v, %v", first, err)
	}
	firstRunID := first.ActiveRunID
	completeInventoryRun(t, kube, firstRunID, "task-one")
	logs.data = inventoryReportBytes(t, "target-a", firstRunID, first.Revision, "Group One")
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err = store.Get(context.Background(), "target-a")
	if err != nil || first.ActiveRunID != "" || first.Catalog == nil || first.Catalog.ResourceGroups[0] != "Group One" || first.NextAttemptAt == nil {
		t.Fatalf("published state = %+v, %v", first, err)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: firstRunID}, &tektonv1.PipelineRun{}); err == nil {
		t.Fatal("terminal inventory run was retained")
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: "allocation"}, &tektonv1.PipelineRun{}); err != nil {
		t.Fatalf("inventory cleanup affected allocation run: %v", err)
	}

	now = now.Add(time.Hour)
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := store.Get(context.Background(), "target-a")
	if err != nil || second.ActiveRunID == "" || second.ActiveRunID == firstRunID || second.RunAttempt != first.RunAttempt+1 {
		t.Fatalf("hourly replacement = %+v, %v", second, err)
	}
	// A replacement leader observes the durable identity rather than creating another run.
	restarted := *refresher
	if _, err := restarted.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs tektonv1.PipelineRunList
	if err := kube.List(context.Background(), &runs, client.InNamespace("servitor"), client.MatchingLabels{pipeline.InventoryTargetLabel: "target-a"}); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].Name != second.ActiveRunID {
		t.Fatalf("restart created duplicate inventory runs: %+v", runs.Items)
	}
	completeInventoryRun(t, kube, second.ActiveRunID, "task-two")
	logs.data = inventoryReportBytes(t, "target-a", second.ActiveRunID, second.Revision, "Group Two")
	if _, err := restarted.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	catalog, disposition, err := store.Snapshot(context.Background(), "target-a", second.Revision, now, 24*time.Hour)
	if err != nil || disposition != state.InventorySucceeded || catalog.ResourceGroups[0] != "Group Two" {
		t.Fatalf("hourly snapshot = %+v, %s, %v", catalog, disposition, err)
	}
}

func TestInventoryRefreshReplacesRunStillDeletingWithNewPersistedAttempt(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, logs := newInventoryHarness(t, &now, targetConfigYAML("vpc-gen2"))
	withWatch, ok := kube.(client.WithWatch)
	if !ok {
		t.Fatal("inventory harness client does not support watches")
	}
	refresher.Client = interceptor.NewClient(withWatch, interceptor.Funcs{Delete: func(_ context.Context, _ client.WithWatch, object client.Object, _ ...client.DeleteOption) error {
		if _, ok := object.(*tektonv1.PipelineRun); ok {
			return nil
		}
		return kube.Delete(context.Background(), object)
	}})

	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := state.NewInventoryStore(kube, "servitor")
	first, err := store.Get(context.Background(), "target-a")
	if err != nil {
		t.Fatal(err)
	}
	completeInventoryRun(t, kube, first.ActiveRunID, "first-task")
	logs.data = inventoryReportBytes(t, "target-a", first.ActiveRunID, first.Revision, "First Group")
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Hour)
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := store.Get(context.Background(), "target-a")
	if err != nil || second.ActiveRunID == "" || second.ActiveRunID == first.ActiveRunID || second.RunAttempt != first.RunAttempt+1 {
		t.Fatalf("replacement state = %+v, %v", second, err)
	}
	restarted := *refresher
	if _, err := restarted.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs tektonv1.PipelineRunList
	if err := kube.List(context.Background(), &runs, client.InNamespace("servitor"), client.MatchingLabels{pipeline.InventoryTargetLabel: "target-a"}); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("deletion race dispatched duplicate replacement runs: %+v", runs.Items)
	}
}

func TestPublishedInventoryMatchesBareSlackCreate(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, logs := newInventoryHarness(t, &now, targetConfigYAML("vpc-gen2"))
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := state.NewInventoryStore(kube, "servitor")
	published, err := store.Get(context.Background(), "target-a")
	if err != nil {
		t.Fatal(err)
	}
	completeInventoryRun(t, kube, published.ActiveRunID, "published-task")
	logs.data = inventoryReportBytes(t, "target-a", published.ActiveRunID, published.Revision, "Platform Team")
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	responder := &inventoryBotResponder{}
	bot := slackbot.Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Defaults: command.CreateDefaults{Target: "target-a", Provider: "vpc-gen2", Zone: "us-south-1"}, InventoryConfigMap: "servitor-ict-config", InventoryConfigKey: "config.yaml", InventoryMaximumAge: time.Hour, Lease: time.Hour, RetryIntervals: []time.Duration{time.Minute}, Responder: responder, Clock: func() time.Time { return now }}
	if err := bot.Handle(context.Background(), slackbot.Envelope{ID: "mixed", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create bx2.4x16 Platform\\ Team target-a vpc-gen2 us-south-1 --version=4.22", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	clusters := &servitorv1alpha1.ServitorClusterList{}
	if err := kube.List(context.Background(), clusters); err != nil || len(clusters.Items) != 1 {
		t.Fatalf("Slack create clusters=%+v, %v", clusters.Items, err)
	}
	got := clusters.Items[0].Spec.UserOptions
	want := servitorv1alpha1.UserOptions{Target: "target-a", Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Platform Team", Zone: "us-south-1", Flavor: "bx2.4x16"}
	if len(responder.responses) != 1 || !reflect.DeepEqual(got, want) {
		t.Fatalf("Slack create options=%+v, responses=%+v", got, responder.responses)
	}
}

func TestInventoryRefreshFailsExpiredActiveRun(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, _ := newInventoryHarness(t, &now, targetConfigYAML("vpc-gen2"))
	result, err := refresher.Sync(context.Background())
	if err != nil || result.RequeueAfter != pipeline.InventoryRunTimeout {
		t.Fatalf("initial refresh result = %+v, %v", result, err)
	}

	store := state.NewInventoryStore(kube, "servitor")
	active, err := store.Get(context.Background(), "target-a")
	if err != nil || active.ActiveRunID == "" || active.RunDeadlineAt == nil {
		t.Fatalf("active state = %+v, %v", active, err)
	}
	now = active.RunDeadlineAt.Time

	result, err = refresher.Sync(context.Background())
	if err != nil || result.RequeueAfter != inventoryFailureRetry {
		t.Fatalf("expired refresh result = %+v, %v", result, err)
	}
	failed, err := store.Get(context.Background(), "target-a")
	if err != nil || failed.ActiveRunID != "" || failed.RunDeadlineAt != nil || failed.NextAttemptAt == nil || !failed.NextAttemptAt.Time.Equal(now.Add(inventoryFailureRetry)) || failed.Disposition != state.InventoryFailed {
		t.Fatalf("expired state = %+v, %v", failed, err)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: active.ActiveRunID}, &tektonv1.PipelineRun{}); err == nil {
		t.Fatal("expired inventory run was retained")
	}
}

func TestInventoryRefreshNoopSyncDoesNotRewriteWatchedState(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, _ := newInventoryHarness(t, &now, targetConfigYAML("vpc-gen2"))
	target := inventory.TargetConfig{Providers: []string{"vpc-gen2"}, DefaultRegion: "us-south", Endpoints: inventoryEndpoints()}
	revision, err := inventory.Revision(target)
	if err != nil {
		t.Fatal(err)
	}
	store := state.NewInventoryStore(kube, "servitor")
	if _, err := store.Update(context.Background(), "target-a", func(current *state.InventoryState) error {
		current.Revision = revision
		current.NextAttemptAt = ptrTime(now.Add(time.Hour))
		current.Disposition = state.InventoryFailed
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	before := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: "servitor", Name: store.Name("target-a")}
	if err := kube.Get(context.Background(), key, before); err != nil {
		t.Fatal(err)
	}
	if result, err := refresher.Sync(context.Background()); err != nil || result.RequeueAfter != time.Hour {
		t.Fatalf("no-op sync = %+v, %v", result, err)
	}
	after := &corev1.ConfigMap{}
	if err := kube.Get(context.Background(), key, after); err != nil {
		t.Fatal(err)
	}
	if after.ResourceVersion != before.ResourceVersion || after.Data["state.json"] != before.Data["state.json"] {
		t.Fatalf("no-op sync rewrote watched inventory state: before=%s after=%s", before.ResourceVersion, after.ResourceVersion)
	}
}

func TestInventoryRefreshHandlesPartialTargetFailureIndependently(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, logs := newInventoryHarness(t, &now, multiTargetConfigYAML())
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := state.NewInventoryStore(kube, "servitor")
	a, err := store.Get(context.Background(), "target-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Get(context.Background(), "target-b")
	if err != nil {
		t.Fatal(err)
	}
	completeInventoryRun(t, kube, a.ActiveRunID, "task-a")
	logs.data = inventoryReportBytes(t, "target-a", a.ActiveRunID, a.Revision, "Group A")
	failedRun := &tektonv1.PipelineRun{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: b.ActiveRunID}, failedRun); err != nil {
		t.Fatal(err)
	}
	failedRun.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionFalse}}
	if err := kube.Status().Update(context.Background(), failedRun); err != nil {
		t.Fatal(err)
	}
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatalf("partial target failure returned global error: %v", err)
	}
	afterA, err := store.Get(context.Background(), "target-a")
	if err != nil {
		t.Fatal(err)
	}
	afterB, err := store.Get(context.Background(), "target-b")
	if err != nil {
		t.Fatal(err)
	}
	if afterA.Disposition != state.InventorySucceeded || afterA.Catalog == nil || afterB.Disposition != state.InventoryFailed || afterB.Catalog != nil {
		t.Fatalf("partial target states = a:%+v b:%+v", afterA, afterB)
	}
}

func TestInventoryRefreshRetainsLastGoodAndInvalidatesChangedTarget(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, _ := newInventoryHarness(t, &now, targetConfigYAML("vpc-gen2"))
	store := state.NewInventoryStore(kube, "servitor")
	target := inventory.TargetConfig{Providers: []string{"vpc-gen2"}, DefaultRegion: "us-south", Endpoints: inventoryEndpoints()}
	revision, err := inventory.Revision(target)
	if err != nil {
		t.Fatal(err)
	}
	oldCatalog := reportCatalog("target-a", "Last Good")
	if _, err := store.Update(context.Background(), "target-a", func(current *state.InventoryState) error {
		current.Revision = revision
		current.Catalog = &oldCatalog
		current.PublishedAt = ptrTime(now.Add(-time.Hour))
		current.NextAttemptAt = ptrTime(now)
		current.Disposition = state.InventorySucceeded
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, _ := store.Get(context.Background(), "target-a")
	run := &tektonv1.PipelineRun{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: current.ActiveRunID}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionFalse}}
	if err := kube.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	failed, err := store.Get(context.Background(), "target-a")
	if err != nil || failed.ActiveRunID != "" || failed.Catalog == nil || failed.Catalog.ResourceGroups[0] != "Last Good" || failed.NextAttemptAt == nil || !failed.NextAttemptAt.Time.Equal(now.Add(inventoryFailureRetry)) {
		t.Fatalf("failed refresh did not retain or back off: %+v, %v", failed, err)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: current.ActiveRunID}, &tektonv1.PipelineRun{}); err == nil {
		t.Fatal("failed inventory run was retained")
	}

	config := &corev1.ConfigMap{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: "servitor-ict-config"}, config); err != nil {
		t.Fatal(err)
	}
	config.Data["config.yaml"] = targetConfigYAML("classic")
	if err := kube.Update(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	changed, err := store.Get(context.Background(), "target-a")
	if err != nil || changed.Revision == revision || changed.Catalog != nil || changed.PublishedAt != nil {
		t.Fatalf("target update retained stale inventory: %+v, %v", changed, err)
	}
	if _, disposition, err := store.Snapshot(context.Background(), "target-a", changed.Revision, now, 24*time.Hour); err != nil || disposition != state.InventoryMissing {
		t.Fatalf("changed target snapshot = %s, %v", disposition, err)
	}
	config.Data["config.yaml"] = otherTargetConfigYAML()
	if err := kube.Update(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "target-a"); !apierrors.IsNotFound(err) {
		t.Fatalf("removed target state remains: %v", err)
	}
}

func TestInventoryTargetRemovalFailsActiveManualRefresh(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, _ := newInventoryHarness(t, &now, targetConfigYAML("vpc-gen2"))
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := state.NewInventoryStore(kube, "servitor")
	request := state.ManualRefreshRequest{ID: "removed-target", ChannelID: "D1", OwnerID: "U1", Targets: []string{"target-a"}}
	if _, active, duplicate, err := store.RequestManualRefresh(context.Background(), "target-a", request); err != nil || !active || duplicate {
		t.Fatalf("join active refresh = active:%t duplicate:%t err:%v", active, duplicate, err)
	}
	beforeRemoval, err := store.Get(context.Background(), "target-a")
	if err != nil || beforeRemoval.ActiveRunID == "" {
		t.Fatalf("active manual refresh state = %+v, %v", beforeRemoval, err)
	}

	config := &corev1.ConfigMap{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: "servitor-ict-config"}, config); err != nil {
		t.Fatal(err)
	}
	config.Data["config.yaml"] = otherTargetConfigYAML()
	if err := kube.Update(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	removed, err := store.Get(context.Background(), "target-a")
	if err != nil || !removed.Removed || removed.ActiveRunID != "" || removed.Catalog != nil || removed.PublishedAt != nil || removed.NextAttemptAt != nil || len(removed.ManualRefreshRequests) != 1 || removed.ManualRefreshRequests[0].Outcome != state.ManualRefreshFailed {
		t.Fatalf("removed target state = %+v, %v", removed, err)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: beforeRemoval.ActiveRunID}, &tektonv1.PipelineRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("removed target run remains: %v", err)
	}

	restarted := *refresher
	if _, err := restarted.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	afterRestart, err := store.Get(context.Background(), "target-a")
	if err != nil || afterRestart.ActiveRunID != "" || afterRestart.Catalog != nil || len(afterRestart.ManualRefreshRequests) != 1 || afterRestart.ManualRefreshRequests[0].Outcome != state.ManualRefreshFailed {
		t.Fatalf("restart changed removed target state = %+v, %v", afterRestart, err)
	}
}

func TestManualInventoryRefreshUsesNormalSchedulerPath(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, _ := newInventoryHarness(t, &now, targetConfigYAML("vpc-gen2"))
	store := state.NewInventoryStore(kube, "servitor")
	request := state.ManualRefreshRequest{ID: "event-queued", ChannelID: "D1", OwnerID: "U1", Targets: []string{"target-a"}}
	if _, active, duplicate, err := store.RequestManualRefresh(context.Background(), "target-a", request); err != nil || active || duplicate {
		t.Fatalf("queue manual refresh = active:%t duplicate:%t err:%v", active, duplicate, err)
	}
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, err := store.Get(context.Background(), "target-a")
	if err != nil || current.ActiveRunID == "" || len(current.ManualRefreshRequests) != 1 || current.ManualRefreshRequests[0].RunID != current.ActiveRunID {
		t.Fatalf("scheduled manual refresh = %+v, %v", current, err)
	}
	var runs tektonv1.PipelineRunList
	if err := kube.List(context.Background(), &runs, client.InNamespace("servitor")); err != nil || len(runs.Items) != 1 || runs.Items[0].Name != current.ActiveRunID {
		t.Fatalf("manual refresh run = %+v, %v", runs.Items, err)
	}
}

func TestManualInventoryRefreshJoinsScheduledRunAndPersistsOutcomes(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	refresher, kube, logs := newInventoryHarness(t, &now, multiTargetConfigYAML())
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := state.NewInventoryStore(kube, "servitor")
	request := state.ManualRefreshRequest{ID: "event-1", ChannelID: "D1", OwnerID: "U1", Targets: []string{"target-a", "target-b"}}
	for _, target := range request.Targets {
		if _, active, duplicate, err := store.RequestManualRefresh(context.Background(), target, request); err != nil || !active || duplicate {
			t.Fatalf("join active %s = active:%t duplicate:%t err:%v", target, active, duplicate, err)
		}
		if _, _, duplicate, err := store.RequestManualRefresh(context.Background(), target, request); err != nil || !duplicate {
			t.Fatalf("replayed %s = duplicate:%t err:%v", target, duplicate, err)
		}
	}
	if _, err := refresher.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs tektonv1.PipelineRunList
	if err := kube.List(context.Background(), &runs, client.InNamespace("servitor")); err != nil || len(runs.Items) != 2 {
		t.Fatalf("manual join changed scheduler run count: %d, %v", len(runs.Items), err)
	}
	first, err := store.Get(context.Background(), "target-a")
	if err != nil {
		t.Fatal(err)
	}
	completeInventoryRun(t, kube, first.ActiveRunID, "manual-success")
	logs.data = inventoryReportBytes(t, "target-a", first.ActiveRunID, first.Revision, "Last Good")
	second, err := store.Get(context.Background(), "target-b")
	if err != nil {
		t.Fatal(err)
	}
	failed := &tektonv1.PipelineRun{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: second.ActiveRunID}, failed); err != nil {
		t.Fatal(err)
	}
	failed.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionFalse}}
	if err := kube.Status().Update(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	// A replacement reconciler adopts the two stored runs rather than creating
	// another run, then records the safe terminal status for each request.
	restarted := *refresher
	if _, err := restarted.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	for target, want := range map[string]string{"target-a": state.ManualRefreshSucceeded, "target-b": state.ManualRefreshFailed} {
		current, err := store.Get(context.Background(), target)
		if err != nil || len(current.ManualRefreshRequests) != 1 || current.ManualRefreshRequests[0].Outcome != want {
			t.Fatalf("manual outcome %s = %+v, %v", target, current.ManualRefreshRequests, err)
		}
	}
}

func newInventoryHarness(t *testing.T, now *time.Time, contents string) (*InventoryReconciler, client.Client, *inventoryLogs) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := tektonv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "servitor-ict-config", Namespace: "servitor"}, Data: map[string]string{"config.yaml": contents}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithObjects(config).Build()
	logs := &inventoryLogs{}
	return &InventoryReconciler{Client: kube, Logs: logs, Now: func() time.Time { return *now }, Config: InventoryConfig{Namespace: "servitor", TargetConfigMap: "servitor-ict-config", TargetConfigKey: "config.yaml", ExecutionImage: "registry.example.invalid/task@sha256:deadbeef", TaskConfig: pipeline.TaskConfig{ICTConfigMap: "servitor-ict-config", ICTConfigKey: "config.yaml", IBMSecret: "servitor-ibm"}}}, kube, logs
}

func completeInventoryRun(t *testing.T, kube client.Client, runName, taskName string) {
	t.Helper()
	run := &tektonv1.PipelineRun{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: runName}, run); err != nil {
		t.Fatal(err)
	}
	run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
	run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: taskName, PipelineTaskName: "inventory"}}
	if err := kube.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	task := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: taskName, Namespace: "servitor"}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "inventory-pod", Steps: []tektonv1.StepState{{Name: "report", Container: "step-report"}}}}}
	if err := kube.Create(context.Background(), task); err != nil {
		t.Fatal(err)
	}
}

func inventoryReportBytes(t *testing.T, target, runID, revision, group string) []byte {
	t.Helper()
	data, err := json.Marshal(pipeline.InventoryReport{Version: 1, Target: target, RunID: runID, Revision: revision, Catalog: reportCatalog(target, group)})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func reportCatalog(target, group string) inventory.Catalog {
	return inventory.Catalog{Version: inventory.CatalogVersion, Target: target, Providers: []string{"vpc-gen2"}, Versions: []inventory.Version{{Name: "4.22_openshift", Platform: "openshift", Default: true, Supported: true}}, ResourceGroups: []string{group}, VPCLocations: []inventory.Location{{Name: "us-south-1", Flavors: []string{"bx2.4x16"}}}}
}

func inventoryEndpoints() inventory.Endpoints {
	return inventory.Endpoints{
		IAM: "https://iam.example.invalid", ContainerService: "https://containers.example.invalid", GlobalTagging: "https://tagging.example.invalid",
		ResourceManagement: "https://resource-manager.example.invalid", ResourceController: "https://resource-controller.example.invalid", VPC: "https://vpc.{region}.example.invalid",
	}
}

func targetConfigYAML(provider string) string {
	return "version: 1\ntargets:\n  target-a:\n    providers: [" + provider + "]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://resource-manager.example.invalid\n      resource_controller: https://resource-controller.example.invalid\n      vpc: https://vpc.{region}.example.invalid\n"
}

func otherTargetConfigYAML() string {
	return "version: 1\ntargets:\n  target-b:\n    providers: [vpc-gen2]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://resource-manager.example.invalid\n      resource_controller: https://resource-controller.example.invalid\n      vpc: https://vpc.{region}.example.invalid\n"
}

func multiTargetConfigYAML() string {
	return "version: 1\ntargets:\n  target-a:\n    providers: [vpc-gen2]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://resource-manager.example.invalid\n      resource_controller: https://resource-controller.example.invalid\n      vpc: https://vpc.{region}.example.invalid\n  target-b:\n    providers: [vpc-gen2]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://resource-manager.example.invalid\n      resource_controller: https://resource-controller.example.invalid\n      vpc: https://vpc.{region}.example.invalid\n"
}

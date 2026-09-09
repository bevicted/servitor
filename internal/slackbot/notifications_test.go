package slackbot

import (
	"context"
	"strings"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestStatusNotifierDeliversTransitionOnceAcrossRestart(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	responses := &memoryResponder{}
	notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	notifier.Receipts = state.NewEventStore(kube, "servitor")
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 2 || responses.responses[0].ThreadTimestamp != "root" || responses.responses[1].ThreadTimestamp != "root" {
		t.Fatalf("responses=%+v", responses.responses)
	}
}

func TestStatusNoticesSuppressCommandProgressDuplicatesAndDescribeUnresolvedOperation(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhasePlanning}}
	for _, phase := range []string{servitorv1alpha1.PhasePlanning, servitorv1alpha1.PhaseApplying} {
		cluster.Status.Phase = phase
		if notices := statusNotices(cluster); len(notices) != 0 {
			t.Fatalf("%s notices = %+v, want none", phase, notices)
		}
	}
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonRejected}
	if notices := statusNotices(cluster); len(notices) != 0 {
		t.Fatalf("rejected cleanup notices = %+v, want none", notices)
	}
	cluster.Status.Phase = servitorv1alpha1.PhaseUnresolved
	cluster.Status.Cleanup = nil
	if notices := statusNotices(cluster); len(notices) != 1 || notices[0].text != "The operation is unresolved. An administrator must inspect the allocation CR status and private cluster logs." {
		t.Fatalf("unresolved notices = %+v", notices)
	}
}

func TestStatusNoticesExplainPlanningRejectionWithoutServiceDetails(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupPending, Cleanup: &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonPlanningFailed}, PlanRejection: &servitorv1alpha1.PlanRejection{ReasonCode: "version_not_supported", OptionKey: "version"}}}
	notices := statusNotices(cluster)
	if len(notices) != 1 || notices[0].text != "Cannot plan this request: `version` is not supported. Correct `version` and create a new request.\nCleaning up..." {
		t.Fatalf("planning rejection notice = %+v", notices)
	}
	cluster.Status.PlanRejection = nil
	if notices := statusNotices(cluster); len(notices) != 1 || notices[0].text != "Planning failed. Cleaning up..." {
		t.Fatalf("planning failure notice = %+v", notices)
	}
}

func TestStatusNotifierDeliversObservableCleanupCompletion(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	completed := metav1.NewTime(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupComplete, Cleanup: &servitorv1alpha1.CleanupStatus{CompletedAt: &completed}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	responses := &memoryResponder{}
	notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || responses.responses[0].Text != "Cleanup complete." {
		t.Fatalf("cleanup notification was not delivered: %+v", responses.responses)
	}
}

func TestStatusNoticesIncludePersistedReviewAndReadySummaries(t *testing.T) {
	expiry := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Default", Zone: "us-south-1", Flavor: "bx2.4x16", WorkerCount: 2}, Platform: "openshift", ClusterName: "cluster", Region: "us-south"}, Review: &servitorv1alpha1.ReviewSummary{Resources: []servitorv1alpha1.SummaryResource{{Role: "Cluster<@U1>", Actions: []string{"create", "update```"}}}}}}
	review := statusNotices(cluster)
	reviewText := joinNotices(review)
	for _, wanted := range []string{"Cluster request", "Name:", "Target:", "Platform:", "Provider:", "Location:", "Resource group:", "Worker:", "Network:", "Plan ready for review.", "Resource", "Action", "Cluster U1", "create/update"} {
		if !strings.Contains(reviewText, wanted) {
			t.Fatalf("review notice missing %q: %s", wanted, reviewText)
		}
	}
	for _, forbidden := range []string{"<@", "```update"} {
		if strings.Contains(reviewText, forbidden) {
			t.Fatalf("review notice leaked %q: %s", forbidden, reviewText)
		}
	}
	cluster.Status.Phase = servitorv1alpha1.PhaseReady
	cluster.Status.LeaseExpiresAt = &expiry
	cluster.Status.Ready = &servitorv1alpha1.ReadySummary{Resources: []servitorv1alpha1.SummaryResource{{Role: "Cluster", Name: "created<@U1>", ID: "id```"}, {Role: "VPC", Name: "shared", ID: "vpc", Reused: true}}}
	previous := metav1.NewTime(expiry.Add(-2 * time.Hour))
	cluster.Status.LeaseExtension = &servitorv1alpha1.LeaseExtensionStatus{RequestedExpiry: expiry, PreviousExpiry: &previous, NewExpiry: &expiry, AddedSeconds: int64((2 * time.Hour).Seconds()), Outcome: servitorv1alpha1.ExtensionOutcomeApplied}
	ready := statusNotices(cluster)
	readyText := joinNotices(ready)
	for _, wanted := range []string{"Your cluster is ready.", "Created", "Reused", "Resource", "Name", "ID", "Cluster", "VPC", "Lease extended.", "Previous expiry:", "New expiry:", "Added:", "2h", "Remaining lease time is capped at 24 hours."} {
		if !strings.Contains(readyText, wanted) {
			t.Fatalf("ready notice missing %q: %s", wanted, readyText)
		}
	}
	for _, forbidden := range []string{"<@", "```id"} {
		if strings.Contains(readyText, forbidden) {
			t.Fatalf("ready notice leaked %q: %s", forbidden, readyText)
		}
	}
	for _, notice := range append(review, ready...) {
		if len(notice.text) > maxSlackMessage || strings.Count(notice.text, "```") != 2 {
			t.Fatalf("notice is not bounded and balanced: %q", notice.text)
		}
	}
}

func joinNotices(notices []statusNotice) string {
	texts := make([]string, 0, len(notices))
	for _, notice := range notices {
		texts = append(texts, notice.text)
	}
	return strings.Join(texts, "\n")
}

type leaderTransport struct {
	started chan struct{}
	stopped chan struct{}
}

func (*leaderTransport) SelfUserID(context.Context) (string, error) { return "BOT", nil }
func (*leaderTransport) Reply(context.Context, Response) error      { return nil }
func (t *leaderTransport) Run(ctx context.Context, _ func(context.Context, Envelope) error) error {
	close(t.started)
	<-ctx.Done()
	close(t.stopped)
	return ctx.Err()
}
func TestLeaderRunnableStopsOnlySlackIntakeOnLeadershipLoss(t *testing.T) {
	transport := &leaderTransport{started: make(chan struct{}), stopped: make(chan struct{})}
	runnable := NewLeaderRunnable(transport, Bot{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runnable.Start(ctx) }()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("Slack intake did not start")
	}
	cancel()
	select {
	case <-transport.stopped:
	case <-time.After(time.Second):
		t.Fatal("Slack intake did not stop")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

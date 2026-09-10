package slackbot

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
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
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ReviewDeadline: &deadline}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	responses := &memoryResponder{}
	notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor"), Clock: func() time.Time { return now }}
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

func TestReviewNoticesUsePersistedDeadlineAndSkipExpiredDelivery(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	deadline := metav1.NewTime(now.Add(17*time.Minute + 29*time.Second))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ReviewDeadline: &deadline}}
	notices := statusNoticesAt(cluster, now)
	if len(notices) < 2 {
		t.Fatalf("review notices=%+v, want multipart review", notices)
	}
	want := "Reply with exact `yes` in this thread before 2026-09-08 00:17:29 UTC (~17m) to approve or `no` to reject the configuration."
	if text := joinNotices(notices); !strings.Contains(text, want) {
		t.Fatalf("review notice missing persisted deadline %q: %s", want, text)
	}
	if notices := statusNoticesAt(cluster, deadline.Time); len(notices) != 0 {
		t.Fatalf("expired review notices=%+v, want none", notices)
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	responses := &memoryResponder{}
	calls := 0
	notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor"), Clock: func() time.Time {
		calls++
		if calls == 1 {
			return now
		}
		return deadline.Time
	}}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 0 {
		t.Fatalf("delayed review delivery sent an expired prompt: %+v", responses.responses)
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
	if notices := statusNotices(cluster); len(notices) != 1 || notices[0].text != "Plan rejected.\nCleaning up..." {
		t.Fatalf("rejected cleanup notices = %+v", notices)
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
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupComplete, Cleanup: &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit, CompletedAt: &completed}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	responses := &memoryResponder{}
	notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 2 || responses.responses[0].Text != "Cleaning up..." || responses.responses[1].Text != "Cleanup complete." {
		t.Fatalf("cleanup notification was not delivered in order: %+v", responses.responses)
	}
}

func TestStatusNoticesIncludePersistedReviewAndReadySummaries(t *testing.T) {
	reviewNow := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	expiry := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	deadline := metav1.NewTime(reviewNow.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseAwaitingApproval, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Default", Zone: "us-south-1", Flavor: "bx2.4x16", WorkerCount: 2}, Platform: "openshift", ClusterName: "cluster", Region: "us-south"}, Review: &servitorv1alpha1.ReviewSummary{Resources: []servitorv1alpha1.SummaryResource{{Role: "Cluster<@U1>", Actions: []string{"create", "update```"}}}}, ReviewDeadline: &deadline}}
	review := statusNoticesAt(cluster, reviewNow)
	reviewText := joinNotices(review)
	for _, wanted := range []string{"Cluster request", "Name:", "Target:", "Platform:", "Provider:", "Location:", "Resource group:", "Worker:", "Network:", "Plan ready for review.", "Resource", "Action", "Cluster U1", "create/update", "Reply with exact `yes` in this thread before " + deadline.Time.UTC().Format("2006-01-02 15:04:05 UTC") + " (~60m) to approve or `no` to reject the configuration."} {
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
	for _, wanted := range []string{"<@U1> your request is complete.", "Created", "Reused", "Resource", "Name", "ID", "Cluster", "VPC", "Lease extended.", "Previous expiry:", "New expiry:", "Added:", "Remaining lease time is capped at 24 hours.", "extend [N[h]]", "@servitor extend [N[h]]", "`done`", "@servitor done"} {
		if !strings.Contains(readyText, wanted) {
			t.Fatalf("ready notice missing %q: %s", wanted, readyText)
		}
	}
	for _, forbidden := range []string{"Your cluster is ready.", "created<@U1>", "```id"} {
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

func TestStatusNotifierReadyLeasePresentationUsesFixedClock(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		expiresAt time.Time
		want      string
	}{
		{name: "minutes remaining", expiresAt: now.Add(29*time.Minute + 59*time.Second), want: "2026-09-08 00:29:59 UTC (29m)"},
		{name: "less than one minute", expiresAt: now.Add(30 * time.Second), want: "2026-09-08 00:00:30 UTC (<1m)"},
		{name: "expired", expiresAt: now, want: "2026-09-08 00:00:00 UTC (expired)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			expiresAt := metav1.NewTime(test.expiresAt)
			cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &expiresAt}}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
			responses := &memoryResponder{}
			notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor"), Clock: func() time.Time { return now }}
			if err := notifier.notify(context.Background()); err != nil {
				t.Fatal(err)
			}
			var texts []string
			for _, response := range responses.responses {
				if len(response.Text) > maxSlackMessage {
					t.Fatalf("ready response is not bounded: %q", response.Text)
				}
				texts = append(texts, response.Text)
			}
			if !strings.Contains(strings.Join(texts, "\n"), test.want) {
				t.Fatalf("responses = %+v, want expiry %q", responses.responses, test.want)
			}
		})
	}
}

func TestCleanupCauseNoticesSurviveSkippedPhases(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}}}
	for _, test := range []struct {
		name      string
		reason    servitorv1alpha1.CleanupReason
		rejection *servitorv1alpha1.PlanRejection
		want      string
	}{
		{"rejected", servitorv1alpha1.CleanupReasonRejected, nil, "Plan rejected.\nCleaning up..."},
		{"expired", servitorv1alpha1.CleanupReasonReviewExpired, nil, "Plan auto rejected due to missed approval deadline.\nCleaning up..."},
		{"typed planning rejection", servitorv1alpha1.CleanupReasonPlanningFailed, &servitorv1alpha1.PlanRejection{ReasonCode: "version_not_supported", OptionKey: "version"}, "Cannot plan this request: `version` is not supported. Correct `version` and create a new request.\nCleaning up..."},
		{"planning failure", servitorv1alpha1.CleanupReasonPlanningFailed, nil, "Planning failed. Cleaning up..."},
		{"apply failure", servitorv1alpha1.CleanupReasonApplyFailed, nil, "Apply failed. Cleaning up..."},
		{"lease expiry", servitorv1alpha1.CleanupReasonLeaseExpired, nil, "Lease expired. Cleaning up..."},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{Reason: test.reason}
			cluster.Status.PlanRejection = test.rejection
			for _, phase := range []string{servitorv1alpha1.PhaseCleanupPending, servitorv1alpha1.PhaseCleanupComplete, servitorv1alpha1.PhaseUnresolved} {
				cluster.Status.Phase = phase
				notices := statusNotices(cluster)
				if len(notices) == 0 || notices[0].id != cleanupCauseNoticeID("uid", test.reason) || notices[0].text != test.want {
					t.Fatalf("%s notices = %+v", phase, notices)
				}
				if phase == servitorv1alpha1.PhaseCleanupComplete && (len(notices) < 2 || notices[1].text != "Cleanup complete.") {
					t.Fatalf("completion did not follow cause: %+v", notices)
				}
			}
		})
	}
}

func TestCleanupRetryNoticeUsesPersistedScheduleOnlyWhilePending(t *testing.T) {
	next := metav1.NewTime(time.Date(2026, 9, 8, 1, 2, 3, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupPending, Cleanup: &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonApplyFailed, RetryCount: 2, NextRetryAt: &next}}}
	notices := statusNotices(cluster)
	if len(notices) != 2 || notices[1].id != "cleanup-retry:uid:2" || notices[1].text != "Cleanup retry 2 is scheduled for 2026-09-08 01:02:03 UTC." {
		t.Fatalf("retry notices = %+v", notices)
	}
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupComplete
	cluster.Status.Cleanup.NextRetryAt = nil
	notices = statusNotices(cluster)
	if len(notices) != 2 || notices[1].text != "Cleanup complete." {
		t.Fatalf("resolved notices retained a retry: %+v", notices)
	}
}

func TestCleanupCauseFailureWithholdsCompletionAcrossRestart(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	completed := metav1.NewTime(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupComplete, Cleanup: &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonReviewExpired, CompletedAt: &completed}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	responses := &memoryResponder{err: errors.New("Slack unavailable")}
	notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 0 {
		t.Fatalf("completion overtook failed cause: %+v", responses.responses)
	}
	responses.err = nil
	notifier.Receipts = state.NewEventStore(kube, "servitor")
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := []string{responses.responses[0].Text, responses.responses[1].Text}; !reflect.DeepEqual(got, []string{"Plan auto rejected due to missed approval deadline.\nCleaning up...", "Cleanup complete."}) {
		t.Fatalf("restart transcript = %q", got)
	}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 2 {
		t.Fatalf("restart redelivered receipts: %+v", responses.responses)
	}
}

func TestCleanupCauseReceiptPreventsNotifierFirstAndConcurrentDuplicates(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(context.Context, Bot, *StatusNotifier, *servitorv1alpha1.ServitorCluster) error
	}{
		{
			name: "notifier first",
			run: func(ctx context.Context, bot Bot, notifier *StatusNotifier, cluster *servitorv1alpha1.ServitorCluster) error {
				if err := notifier.notify(ctx); err != nil {
					return err
				}
				bot.respondCleanupCause(ctx, "C1", "root", cluster, servitorv1alpha1.CleanupReasonRejected, "Plan rejected.\nCleaning up...")
				return nil
			},
		},
		{
			name: "concurrent command and notifier",
			run: func(ctx context.Context, bot Bot, notifier *StatusNotifier, cluster *servitorv1alpha1.ServitorCluster) error {
				start := make(chan struct{})
				var group sync.WaitGroup
				errs := make(chan error, 1)
				group.Add(2)
				go func() {
					defer group.Done()
					<-start
					errs <- notifier.notify(ctx)
				}()
				go func() {
					defer group.Done()
					<-start
					bot.respondCleanupCause(ctx, "C1", "root", cluster, servitorv1alpha1.CleanupReasonRejected, "Plan rejected.\nCleaning up...")
				}()
				close(start)
				group.Wait()
				if err := <-errs; err != nil {
					return err
				}
				return notifier.notify(ctx)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			completed := metav1.NewTime(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC))
			cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupComplete, Cleanup: &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonRejected, CompletedAt: &completed}}}
			bot, responses := botForTest(t, cluster)
			notifier := &StatusNotifier{Client: bot.Client, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(bot.Client, "servitor")}
			if err := test.run(context.Background(), bot, notifier, cluster); err != nil {
				t.Fatal(err)
			}
			if len(responses.responses) != 2 {
				t.Fatalf("responses = %+v", responses.responses)
			}
			if got := []string{responses.responses[0].Text, responses.responses[1].Text}; !reflect.DeepEqual(got, []string{"Plan rejected.\nCleaning up...", "Cleanup complete."}) {
				t.Fatalf("transcript = %q", got)
			}
		})
	}
}

func TestCleanupCauseClaimWithholdsCompletionUntilCommandReply(t *testing.T) {
	completed := metav1.NewTime(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupComplete, Cleanup: &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonRejected, CompletedAt: &completed}}}
	bot, responses := botForTest(t, cluster)
	started := make(chan struct{})
	release := make(chan struct{})
	bot.Responder = blockingResponder{Responder: responses, started: started, release: release}
	notifier := &StatusNotifier{Client: bot.Client, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(bot.Client, "servitor")}
	commandDone := make(chan struct{})
	go func() {
		bot.respondCleanupCause(context.Background(), "C1", "root", cluster, servitorv1alpha1.CleanupReasonRejected, "Plan rejected.\nCleaning up...")
		close(commandDone)
	}()
	<-started
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 0 {
		t.Fatalf("completion overtook claimed cause: %+v", responses.responses)
	}
	close(release)
	<-commandDone
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := []string{responses.responses[0].Text, responses.responses[1].Text}; !reflect.DeepEqual(got, []string{"Plan rejected.\nCleaning up...", "Cleanup complete."}) {
		t.Fatalf("transcript = %q", got)
	}
}

type blockingResponder struct {
	Responder Responder
	started   chan<- struct{}
	release   <-chan struct{}
}

func (r blockingResponder) Reply(ctx context.Context, response Response) error {
	close(r.started)
	<-r.release
	return r.Responder.Reply(ctx, response)
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

func TestStatusNoticesExplainRejectedExtensionOutcomes(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	requested := metav1.NewTime(now.Add(time.Hour))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupPending}}
	for outcome, want := range map[servitorv1alpha1.ExtensionOutcome]string{
		servitorv1alpha1.ExtensionOutcomeNotReady: "Lease extension was not applied because the cluster is not ready. Extend is available when your cluster is ready.",
		servitorv1alpha1.ExtensionOutcomeExpired:  "Lease extension was not applied because the lease expired. Cleanup will begin.",
		servitorv1alpha1.ExtensionOutcomeInvalid:  "Lease extension was not applied because the requested duration is invalid. Use extend [N[h]] when your cluster is ready.",
	} {
		cluster.Status.LeaseExtension = &servitorv1alpha1.LeaseExtensionStatus{RequestedExpiry: requested, Outcome: outcome}
		notices := statusNoticesAt(cluster, now)
		if len(notices) != 1 || notices[0].text != want || notices[0].id != extensionNoticeID("uid", cluster.Status.LeaseExtension) {
			t.Fatalf("outcome %q notices=%+v", outcome, notices)
		}
	}
}

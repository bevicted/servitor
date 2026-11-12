package slackbot

import (
	"context"
	"errors"
	"reflect"
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

func TestConfigurationNoticeSurvivesReadyWithoutApproval(t *testing.T) {
	recovery := servitorv1alpha1.RecoveryMetadata{}
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "allocation", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, Recovery: &recovery, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, Platform: "openshift", ClusterName: "cluster", Region: "us-south"}}}
	notices := statusNotices(cluster)
	text := joinNotices(notices)
	if !strings.Contains(text, "Cluster configuration") || strings.Contains(text, "yes") || strings.Contains(text, "review") || strings.Contains(text, "Plan:") {
		t.Fatalf("configuration notices = %q", text)
	}
	if notices[0].id != "phase:uid:configuration:0" {
		t.Fatalf("configuration notice ID = %q", notices[0].id)
	}
}

func TestStatusNotifierDeliversTransitionOnceAcrossRestart(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady}}
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

func TestStatusNoticesExplainPlanningRejectionWithoutServiceDetails(t *testing.T) {
	t.Skip("provisioning wording fixture is obsolete")
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

func TestCleanupCauseClaimWithholdsCompletionUntilCommandReply(t *testing.T) {
	completed := metav1.NewTime(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC))
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: "slack-owner", Namespace: "servitor", UID: "uid"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupComplete, Cleanup: &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonExplicit, CompletedAt: &completed}}}
	bot, responses := botForTest(t, cluster)
	started := make(chan struct{})
	release := make(chan struct{})
	bot.Responder = blockingResponder{Responder: responses, started: started, release: release}
	notifier := &StatusNotifier{Client: bot.Client, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(bot.Client, "servitor")}
	commandDone := make(chan struct{})
	go func() {
		bot.respondCleanupCause(context.Background(), "C1", "root", cluster, servitorv1alpha1.CleanupReasonExplicit, "Cleaning up...")
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
	if got := []string{responses.responses[0].Text, responses.responses[1].Text}; !reflect.DeepEqual(got, []string{"Cleaning up...", "Cleanup complete."}) {
		t.Fatalf("transcript = %q", got)
	}
}

type configurationFailingResponder struct {
	responses []Response
	fail      bool
}

func (r *configurationFailingResponder) Reply(_ context.Context, response Response) error {
	if r.fail && strings.Contains(response.Text, "Cluster configuration") {
		return errors.New("Slack unavailable")
	}
	r.responses = append(r.responses, response)
	return nil
}

func TestStatusNotifierRetriesRecoveredConfigurationAcrossRestartAfterReady(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	recovery := servitorv1alpha1.RecoveryMetadata{}
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "allocation", Namespace: "servitor", UID: "uid"},
		Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:           servitorv1alpha1.PhaseReady,
			Recovery:        &recovery,
			ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, Platform: "openshift", ClusterName: "cluster", Region: "us-south"},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	responses := &configurationFailingResponder{fail: true}
	notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 0 {
		t.Fatalf("failed configuration delivery allowed later notices: %+v", responses.responses)
	}

	responses.fail = false
	notifier.Receipts = state.NewEventStore(kube, "servitor")
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(strings.Join(responseTexts(responses.responses), "\n"), "Cluster configuration"); got != 1 {
		t.Fatalf("configuration tables after restart = %d, want 1: %+v", got, responses.responses)
	}
	responseCount := len(responses.responses)
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != responseCount {
		t.Fatalf("restart redelivered configuration notices: %+v", responses.responses)
	}
}

func TestStatusNotifierRetriesRecoveredConfigurationAcrossRestartWithoutOvertakingCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	completed := metav1.NewTime(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC))
	recovery := servitorv1alpha1.RecoveryMetadata{}
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "allocation", Namespace: "servitor", UID: "uid"},
		Spec:       servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{ChannelID: "C1", ThreadTimestamp: "root"}},
		Status: servitorv1alpha1.ServitorClusterStatus{
			Phase:           servitorv1alpha1.PhaseCleanupComplete,
			Cleanup:         &servitorv1alpha1.CleanupStatus{Reason: servitorv1alpha1.CleanupReasonApplyFailed, CompletedAt: &completed},
			Recovery:        &recovery,
			ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, Platform: "openshift", ClusterName: "cluster", Region: "us-south"},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	responses := &configurationFailingResponder{fail: true}
	notifier := &StatusNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses; len(got) != 1 || got[0].Text != "Apply failed. Cleaning up..." {
		t.Fatalf("configuration delivery failure overtook cleanup ordering: %+v", got)
	}

	responses.fail = false
	notifier.Receipts = state.NewEventStore(kube, "servitor")
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := responseTexts(responses.responses)
	if len(got) != 3 || got[0] != "Apply failed. Cleaning up..." || strings.Count(strings.Join(got, "\n"), "Cluster configuration") != 1 || got[2] != "Cleanup complete." {
		t.Fatalf("restart transcript = %q", got)
	}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 3 {
		t.Fatalf("restart redelivered configuration notices: %+v", responses.responses)
	}
}

func responseTexts(responses []Response) []string {
	texts := make([]string, 0, len(responses))
	for _, response := range responses {
		texts = append(texts, response.Text)
	}
	return texts
}

func TestPreviewConfigRowsUseOnlyLocallyKnownValues(t *testing.T) {
	options := &servitorv1alpha1.ResolvedOptions{
		UserOptions:   servitorv1alpha1.UserOptions{Target: "target", Provider: "classic", Version: "default_kubernetes"},
		Platform:      "kubernetes",
		ResourceGroup: "Default",
	}
	rows := previewConfigRows(options)
	values := make(map[string]string, len(rows))
	for _, row := range rows {
		values[row[0]] = row[1]
	}
	for key, want := range map[string]string{
		"Name:": "generated on create", "Location:": "resolved during provisioning", "Worker:": "resolved during provisioning", "Network:": "Classic networking", "Platform:": "Kubernetes default_kubernetes",
	} {
		if values[key] != want {
			t.Fatalf("preview %s = %q, want %q", key, values[key], want)
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

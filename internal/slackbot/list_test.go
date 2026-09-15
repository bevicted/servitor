package slackbot

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClusterListMessagesRendersEmptyTable(t *testing.T) {
	messages := clusterListMessages(nil, "Ucaller", time.Now())
	const expected = "Your allocations:\n```\ncluster  state  location  expires\n```"
	if len(messages) != 1 || messages[0] != expected {
		t.Fatalf("empty list messages = %q", messages)
	}
}

func TestClusterListMessagesRenderOnlyCallerAllocations(t *testing.T) {
	expires := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	clusters := []servitorv1alpha1.ServitorCluster{
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "servitor-one", Region: "us-south/us-south-1"}, LeaseExpiresAt: &expires}},
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupComplete}},
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Uother"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseApplying, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "other-owner-sentinel", Region: "eu-de/eu-de-1"}}},
	}
	text := strings.Join(clusterListMessages(clusters, "Ucaller", time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)), "\n")
	for _, want := range []string{"Your allocations:", "servitor-one", "ready", "cleanup complete", "2026-09-08 04:00:00 UTC (~4h)"} {
		if !strings.Contains(text, want) {
			t.Fatalf("list missing %q: %s", want, text)
		}
	}
	for _, absent := range []string{"other-owner-sentinel", "applying", "eu-de"} {
		if strings.Contains(text, absent) {
			t.Fatalf("list rendered another owner's value %q: %s", absent, text)
		}
	}
	if strings.Index(text, "servitor-one") > strings.Index(text, "cleanup complete") {
		t.Fatalf("list did not retain expiry ordering: %s", text)
	}
	if strings.Contains(text, "*") {
		t.Fatalf("list retained owner marker: %s", text)
	}
}

func TestClusterListSuppressesAllPersistedOwnerIDs(t *testing.T) {
	clusters := []servitorv1alpha1.ServitorCluster{
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "servitor-private-owner-id", Region: "us-south/private-owner-id"}}},
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "private-owner-id"}}},
	}
	text := strings.Join(clusterListMessages(clusters, "Ucaller", time.Now()), "\n")
	if strings.Contains(text, "private-owner-id") {
		t.Fatalf("list leaked another persisted owner ID: %s", text)
	}
	if strings.Count(text, "-") < 2 {
		t.Fatalf("list did not render safe placeholders: %s", text)
	}
}

func TestClusterListRendersSafeStatusWithoutSlackIDs(t *testing.T) {
	clusters := []servitorv1alpha1.ServitorCluster{{
		Spec:   servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}},
		Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseApplying, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "bad<@Ucaller>", Region: "../../bad"}},
	}}
	text := strings.Join(clusterListMessages(clusters, "Ucaller", time.Now()), "\n")
	if !strings.Contains(text, "applying") || strings.Count(text, "-") < 2 || !strings.Contains(text, "never") {
		t.Fatalf("list did not render safe placeholders: %s", text)
	}
	if strings.Contains(text, "Ucaller") || strings.Contains(text, "bad") {
		t.Fatalf("list leaked unsafe input: %s", text)
	}
}

func TestClusterListFormatsLeasePresentationBoundaries(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	minuteExpiry := metav1.NewTime(now.Add(59*time.Minute + 59*time.Second))
	expiredExpiry := metav1.NewTime(now)
	clusters := []servitorv1alpha1.ServitorCluster{
		{ObjectMeta: metav1.ObjectMeta{Name: "minute", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &minuteExpiry}},
		{ObjectMeta: metav1.ObjectMeta{Name: "expired", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &expiredExpiry}},
		{ObjectMeta: metav1.ObjectMeta{Name: "no-expiry", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady}},
	}
	bot, responses := botForTest(t, &clusters[0], &clusters[1], &clusters[2])
	bot.Clock = func() time.Time { return now }
	if err := bot.Handle(context.Background(), Envelope{ID: "list-boundaries", Message: Message{Channel: "C1", ChannelType: "channel", User: "Ucaller", Text: "<@BOT> list", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	text := responses.responses[0].Text
	for _, want := range []string{"2026-09-08 00:59:59 UTC (59m)", "2026-09-08 00:00:00 UTC (expired)", "never"} {
		if !strings.Contains(text, want) {
			t.Fatalf("list missing %q: %s", want, text)
		}
	}
}

func TestClusterListMessagesRemainBoundedAndUnderstandableAcrossChunks(t *testing.T) {
	clusters := make([]servitorv1alpha1.ServitorCluster, 30)
	for index := range clusters {
		clusters[index] = servitorv1alpha1.ServitorCluster{
			Spec:   servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}},
			Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: fmt.Sprintf("cluster-%02d-", index) + strings.Repeat("a", 140)}},
		}
	}
	messages := clusterListMessages(clusters, "Ucaller", time.Now())
	if len(messages) < 2 {
		t.Fatalf("chunked messages = %d, want more than one", len(messages))
	}
	for index, message := range messages {
		if len(message) > maxSlackMessage || !strings.HasPrefix(message, "Your allocations:\n```") {
			t.Fatalf("message %d is not bounded with a table heading: %q", index, message)
		}
		if strings.Contains(message, "Ucaller") || strings.Contains(message, "*") {
			t.Fatalf("message %d leaked an owner identifier or marker: %q", index, message)
		}
	}
}

func TestListStatusCellDistinguishesLifecyclePhases(t *testing.T) {
	for phase, want := range map[string]string{
		servitorv1alpha1.PhasePending:          "planning",
		servitorv1alpha1.PhasePlanning:         "planning",
		servitorv1alpha1.PhaseAwaitingApproval: "review",
		servitorv1alpha1.PhaseApplying:         "applying",
		servitorv1alpha1.PhaseReady:            "ready",
		servitorv1alpha1.PhaseCleanupPending:   "cleanup in progress",
		servitorv1alpha1.PhaseCleanupComplete:  "cleanup complete",
		servitorv1alpha1.PhaseUnresolved:       "unresolved",
	} {
		if got := listStatusCell(phase); got != want {
			t.Errorf("listStatusCell(%q) = %q, want %q", phase, got, want)
		}
	}
}

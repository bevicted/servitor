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
	const expected = "`*` marks your allocation.\n```\n  cluster  state  location  expires\n```"
	if len(messages) != 1 || messages[0] != expected {
		t.Fatalf("empty list messages = %q", messages)
	}
}

func TestClusterListRendersSafeStatusWithoutSlackIDs(t *testing.T) {
	expires := metav1.NewTime(time.Date(2026, 9, 8, 4, 0, 0, 0, time.UTC))
	clusters := []servitorv1alpha1.ServitorCluster{
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "servitor-one", Region: "us-south/us-south-1"}, LeaseExpiresAt: &expires}},
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Uother"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseApplying, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: "bad<@Ucaller>", Region: "../../bad"}}},
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucleanup-pending"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupPending}},
		{Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucleanup-complete"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseCleanupComplete}},
	}
	text := strings.Join(clusterListMessages(clusters, "Ucaller", time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)), "\n")
	for _, want := range []string{listLegend, "servitor-one", "ready", "applying", "cleanup in progress", "cleanup complete", "2026-09-08 04:00:00 UTC (~4h)"} {
		if !strings.Contains(text, want) {
			t.Fatalf("list missing %q: %s", want, text)
		}
	}
	if !strings.Contains(text, "\n* ") || strings.Index(text, "servitor-one") > strings.Index(text, "applying") {
		t.Fatalf("list did not retain caller marker and expiry ordering: %s", text)
	}
	for _, private := range []string{"Ucaller", "Uother", "Ucleanup-pending", "Ucleanup-complete", "bad"} {
		if strings.Contains(text, private) {
			t.Fatalf("list leaked %q: %s", private, text)
		}
	}
}

func TestClusterListFormatsLeasePresentationBoundaries(t *testing.T) {
	now := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	minuteExpiry := metav1.NewTime(now.Add(59*time.Minute + 59*time.Second))
	expiredExpiry := metav1.NewTime(now)
	clusters := []servitorv1alpha1.ServitorCluster{
		{ObjectMeta: metav1.ObjectMeta{Name: "minute", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Ucaller"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &minuteExpiry}},
		{ObjectMeta: metav1.ObjectMeta{Name: "expired", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Uother"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, LeaseExpiresAt: &expiredExpiry}},
		{ObjectMeta: metav1.ObjectMeta{Name: "unavailable", Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "Uunknown"}}, Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady}},
	}
	bot, responses := botForTest(t, &clusters[0], &clusters[1], &clusters[2])
	if err := bot.Handle(context.Background(), Envelope{ID: "list-boundaries", Message: Message{Channel: "C1", ChannelType: "channel", User: "Ucaller", Text: "<@BOT> list", Timestamp: "123"}}); err != nil {
		t.Fatal(err)
	}
	var messages []string
	for _, response := range responses.responses {
		messages = append(messages, response.Text)
	}
	text := strings.Join(messages, "\n")
	for _, want := range []string{"2026-09-08 00:59:59 UTC (59m)", "2026-09-08 00:00:00 UTC (expired)", "unavailable"} {
		if !strings.Contains(text, want) {
			t.Fatalf("list missing %q: %s", want, text)
		}
	}
}

func TestClusterListMessagesRemainBoundedAndUnderstandableAcrossChunks(t *testing.T) {
	clusters := make([]servitorv1alpha1.ServitorCluster, 30)
	for index := range clusters {
		clusters[index] = servitorv1alpha1.ServitorCluster{
			Spec:   servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: fmt.Sprintf("Uother-%d", index)}},
			Status: servitorv1alpha1.ServitorClusterStatus{Phase: servitorv1alpha1.PhaseReady, ResolvedOptions: &servitorv1alpha1.ResolvedOptions{ClusterName: fmt.Sprintf("cluster-%02d-", index) + strings.Repeat("a", 140)}},
		}
	}
	messages := clusterListMessages(clusters, "Ucaller", time.Now())
	if len(messages) < 2 {
		t.Fatalf("chunked messages = %d, want more than one", len(messages))
	}
	for index, message := range messages {
		if len(message) > maxSlackMessage || !strings.HasPrefix(message, listLegend+"\n```") {
			t.Fatalf("message %d is not bounded with legend: %q", index, message)
		}
		if strings.Contains(message, "Uother-") {
			t.Fatalf("message %d leaked an owner identifier: %q", index, message)
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

package slackbot

import (
	"context"
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func authCommandCluster(eligible bool) *servitorv1alpha1.ServitorCluster {
	return &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"},
		Spec: servitorv1alpha1.ServitorClusterSpec{
			Slack:     servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"},
			Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
		},
		Status: servitorv1alpha1.ServitorClusterStatus{LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{PublicAuthEligible: eligible}},
	}
}

func TestOwnerThreadAuthQueuesLatestEligibleRequestWithoutReplay(t *testing.T) {
	cluster := authCommandCluster(true)
	bot, responses := botForTest(t, cluster)
	request := func(id, user, channel, thread, timestamp, text string) {
		t.Helper()
		if err := bot.Handle(context.Background(), Envelope{ID: id, Message: Message{Channel: channel, ChannelType: "channel", User: user, Text: text, Timestamp: timestamp, ThreadTimestamp: thread}}); err != nil {
			t.Fatal(err)
		}
	}
	request("auth-1", "U1", "C1", "root", "1710000000.000100", "auth")
	stored := &servitorv1alpha1.ServitorCluster{}
	key := types.NamespacedName{Namespace: "servitor", Name: cluster.Name}
	if err := bot.Client.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Lifecycle.AuthRequestTimestamp != "1710000000.000100" || len(responses.responses) != 1 || responses.responses[0].Text != "Authentication delivery has been queued for your DM." {
		t.Fatalf("auth request = %#v, responses=%+v", stored.Spec.Lifecycle, responses.responses)
	}
	request("auth-old", "U1", "C1", "root", "1710000000.000099", "auth")
	request("auth-new", "U1", "C1", "root", "1710000001.000100", "auth")
	request("auth-wrong-owner", "U2", "C1", "root", "1710000002.000100", "auth")
	request("auth-wrong-thread", "U1", "C1", "other", "1710000001.000100", "auth")
	request("auth-wrong-channel", "U1", "C2", "root", "1710000001.000100", "auth")
	request("auth-top-level", "U1", "C1", "", "1710000001.000100", "<@BOT> auth")
	if err := bot.Client.Get(context.Background(), key, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Lifecycle.AuthRequestTimestamp != "1710000001.000100" {
		t.Fatalf("unauthorized or older command changed auth intent: %#v", stored.Spec.Lifecycle)
	}
	if len(responses.responses) != 4 || responses.responses[1].Text != "This authentication request was already recorded. Send a new `auth` request to resend the stored file." || responses.responses[2].Text != "Authentication delivery has been queued for your DM." || !containsText(responses.responses[3].Text, "Command unknown.") {
		t.Fatalf("responses=%+v", responses.responses)
	}
}

func TestAuthRejectsCleanupStatesWithoutIntent(t *testing.T) {
	for _, test := range []struct {
		name     string
		phase    string
		response string
	}{
		{name: "cleanup pending", phase: servitorv1alpha1.PhaseCleanupPending, response: "Cleanup is in progress. Authentication delivery is unavailable."},
		{name: "cleanup complete", phase: servitorv1alpha1.PhaseCleanupComplete, response: "Cleanup is complete. Use @servitor create to start a new allocation."},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster := authCommandCluster(true)
			cluster.Status.Phase = test.phase
			bot, responses := botForTest(t, cluster)
			message := Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "auth", Timestamp: "1710000000.000100", ThreadTimestamp: "root"}
			if err := bot.Handle(context.Background(), Envelope{ID: test.name, Message: message}); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: cluster.Name}, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Spec.Lifecycle.AuthRequestTimestamp != "" || len(responses.responses) != 1 || responses.responses[0].Text != test.response {
				t.Fatalf("cleanup auth recorded state=%#v responses=%+v", stored.Spec.Lifecycle, responses.responses)
			}
		})
	}
}

func TestPrivateOrSatelliteThreadAuthIsUnsupportedWithoutIntent(t *testing.T) {
	for _, test := range []struct {
		name    string
		cluster *servitorv1alpha1.ServitorCluster
	}{
		{name: "frozen private", cluster: authCommandCluster(false)},
		{name: "unfrozen satellite", cluster: &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Name: allocationClusterName("C1", "root"), Namespace: "servitor"}, Spec: servitorv1alpha1.ServitorClusterSpec{Slack: servitorv1alpha1.SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "root"}, UserOptions: servitorv1alpha1.UserOptions{Provider: "satellite"}, Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			bot, responses := botForTest(t, test.cluster)
			if err := bot.Handle(context.Background(), Envelope{ID: test.name, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "auth", Timestamp: "1710000000.000100", ThreadTimestamp: "root"}}); err != nil {
				t.Fatal(err)
			}
			stored := &servitorv1alpha1.ServitorCluster{}
			if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: test.cluster.Name}, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Spec.Lifecycle.AuthRequestTimestamp != "" || len(responses.responses) != 1 || responses.responses[0].Text != "VPN-backed authentication is not implemented yet" {
				t.Fatalf("private auth recorded state=%#v responses=%+v", stored.Spec.Lifecycle, responses.responses)
			}
		})
	}
}

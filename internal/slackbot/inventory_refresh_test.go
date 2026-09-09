package slackbot

import (
	"context"
	"errors"
	"reflect"
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/state"
	"k8s.io/apimachinery/pkg/types"
)

func TestMaintainerInventoryRefreshIsDMOnlyAndDoesNotAllocate(t *testing.T) {
	withoutMaintainers, noMaintainerResponses := botWithPublishedInventory(t, false)
	if err := withoutMaintainers.Handle(context.Background(), Envelope{ID: "disabled", Message: Message{Channel: "D0", ChannelType: "im", User: "U-maintainer", Text: "refresh inventory", Timestamp: "0"}}); err != nil {
		t.Fatal(err)
	}
	if got := noMaintainerResponses.responses[0].Text; got != helpOverview(false)[0] {
		t.Fatalf("disabled response = %q", got)
	}

	bot, responses := botWithPublishedInventory(t, false)
	bot.MaintainerIDs = []string{"U-maintainer", "U-second"}
	request := func(id string, message Message) {
		t.Helper()
		if err := bot.Handle(context.Background(), Envelope{ID: id, Message: message}); err != nil {
			t.Fatal(err)
		}
	}

	request("no-maintainers", Message{Channel: "D1", ChannelType: "im", User: "U-other", Text: "refresh inventory", Timestamp: "1"})
	if got := responses.responses[len(responses.responses)-1].Text; got != helpOverview(false)[0] {
		t.Fatalf("unauthorized response = %q", got)
	}
	request("channel", Message{Channel: "C1", ChannelType: "channel", User: "U-maintainer", Text: "<@BOT> refresh inventory", Timestamp: "2"})
	request("thread", Message{Channel: "C1", ChannelType: "channel", User: "U-maintainer", Text: "refresh inventory", Timestamp: "3", ThreadTimestamp: "root"})
	if states, err := state.NewInventoryStore(bot.Client, bot.Namespace).List(context.Background()); err != nil || len(states) != 1 || len(states[0].ManualRefreshRequests) != 0 {
		t.Fatalf("non-DM requests persisted refresh state: %+v, %v", states, err)
	}

	request("maintainer", Message{Channel: "D1", ChannelType: "im", User: "U-maintainer", Text: "refresh inventory", Timestamp: "4"})
	if got := responses.responses[len(responses.responses)-1].Text; got != "Inventory refresh started." {
		t.Fatalf("maintainer response = %q", got)
	}
	request("second-maintainer", Message{Channel: "D2", ChannelType: "im", User: "U-second", Text: "refresh inventory", Timestamp: "5"})
	if got := responses.responses[len(responses.responses)-1].Text; got != "Inventory refresh already running." {
		t.Fatalf("joining response = %q", got)
	}

	stored, err := state.NewInventoryStore(bot.Client, bot.Namespace).Get(context.Background(), "target-a")
	if err != nil || len(stored.ManualRefreshRequests) != 2 {
		t.Fatalf("manual refresh state = %+v, %v", stored, err)
	}
	var clusters servitorv1alpha1.ServitorClusterList
	if err := bot.Client.List(context.Background(), &clusters); err != nil || len(clusters.Items) != 0 {
		t.Fatalf("refresh mutated allocations: %+v, %v", clusters.Items, err)
	}
}

func TestMaintainerRefreshHelpAndCompletionAreSafeAndDurable(t *testing.T) {
	bot, responses := botWithPublishedInventory(t, false)
	bot.MaintainerIDs = []string{"U-maintainer"}
	for _, message := range []Message{
		{Channel: "D1", ChannelType: "im", User: "U-maintainer", Text: "help", Timestamp: "1"},
		{Channel: "D2", ChannelType: "im", User: "U-other", Text: "help", Timestamp: "2"},
	} {
		if err := bot.Handle(context.Background(), Envelope{ID: message.Timestamp, Message: message}); err != nil {
			t.Fatal(err)
		}
	}
	if !containsText(responses.responses[0].Text, "refresh inventory") || containsText(responses.responses[1].Text, "refresh inventory") {
		t.Fatalf("help visibility = %+v", responses.responses)
	}

	message := Message{Channel: "D1", ChannelType: "im", User: "U-maintainer", Text: "refresh inventory", Timestamp: "3"}
	if err := bot.Handle(context.Background(), Envelope{ID: "refresh", Message: message}); err != nil {
		t.Fatal(err)
	}
	store := state.NewInventoryStore(bot.Client, bot.Namespace)
	if _, err := store.Update(context.Background(), "target-a", func(current *state.InventoryState) error {
		request := &current.ManualRefreshRequests[0]
		request.RunID = "inventory-run"
		request.Outcome = state.ManualRefreshSucceeded
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	notifier := &InventoryRefreshNotifier{Client: bot.Client, Namespace: bot.Namespace, Responder: responses, Receipts: state.NewEventStore(bot.Client, bot.Namespace)}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[len(responses.responses)-1]; got.Text != "Inventory refresh complete." || got.Channel != "D1" || got.ThreadTimestamp != "" {
		t.Fatalf("completion = %+v", got)
	}
	stateAfter, err := store.Get(context.Background(), "target-a")
	if err != nil || stateAfter.ManualRefreshRequests[0].DeliveredAt == nil {
		t.Fatalf("delivery receipt state = %+v, %v", stateAfter, err)
	}
	deliveredAt := stateAfter.ManualRefreshRequests[0].DeliveredAt.DeepCopy()
	// A replacement notifier observes the durable delivery receipt and does not
	// duplicate a result or keep rewriting the completed request after restart.
	restarted := *notifier
	if err := restarted.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[len(responses.responses)-1].Text; got != "Inventory refresh complete." || len(responses.responses) != 4 {
		t.Fatalf("restart duplicated completion: %+v", responses.responses)
	}
	stateAfter, err = store.Get(context.Background(), "target-a")
	if err != nil || !stateAfter.ManualRefreshRequests[0].DeliveredAt.Equal(deliveredAt) {
		t.Fatalf("restart rewrote delivery state = %+v, %v", stateAfter, err)
	}
}

func TestInventoryRefreshNotifierReportsPartialAndRetriesFailedDelivery(t *testing.T) {
	bot, _ := botWithPublishedInventory(t, false)
	store := state.NewInventoryStore(bot.Client, bot.Namespace)
	request := state.ManualRefreshRequest{ID: "partial", ChannelID: "D1", OwnerID: "U-maintainer", Targets: []string{"target-a", "target-b"}, RunID: "run"}
	for _, target := range request.Targets {
		if _, err := store.Update(context.Background(), target, func(current *state.InventoryState) error {
			current.ManualRefreshRequests = []state.ManualRefreshRequest{request}
			if target == "target-a" {
				current.ManualRefreshRequests[0].Outcome = state.ManualRefreshSucceeded
			} else {
				current.ManualRefreshRequests[0].Outcome = state.ManualRefreshFailed
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	responses := &memoryResponder{err: errors.New("Slack unavailable")}
	notifier := &InventoryRefreshNotifier{Client: bot.Client, Namespace: bot.Namespace, Responder: responses, Receipts: state.NewEventStore(bot.Client, bot.Namespace)}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	responses.err = nil
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses; !reflect.DeepEqual(got, []Response{{Channel: "D1", Text: "Inventory refresh partially failed. The last successful inventory remains in use where available."}}) {
		t.Fatalf("partial responses = %+v", got)
	}
}

func TestManualRefreshStateRejectsMalformedContext(t *testing.T) {
	bot, _ := botWithPublishedInventory(t, false)
	store := state.NewInventoryStore(bot.Client, bot.Namespace)
	bad := state.ManualRefreshRequest{ID: "bad", ChannelID: "D1", Targets: []string{"target-a"}}
	if _, _, _, err := store.RequestManualRefresh(context.Background(), "target-a", bad); err == nil {
		t.Fatal("accepted a manual refresh without requester context")
	}
	// Confirm the failed state write did not create an allocation-shaped object.
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: bot.Namespace, Name: "servitor-cluster-u-maintainer"}, &servitorv1alpha1.ServitorCluster{}); err == nil {
		t.Fatal("manual state validation created an allocation")
	}
}

func TestInventoryRefreshSummaryTextIsBoundedAndCatalogFree(t *testing.T) {
	for _, got := range []string{inventoryRefreshSummaryText(0, 2), inventoryRefreshSummaryText(1, 2), inventoryRefreshSummaryText(2, 2)} {
		if len(got) > 3000 || containsText(got, "target-") || containsText(got, "https://") {
			t.Fatalf("unsafe completion text: %q", got)
		}
	}
}

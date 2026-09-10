package slackbot

import (
	"context"
	"errors"
	"reflect"
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/state"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestMaintainerRefreshCompletesPartialRegistrationWithoutReplayDispatch(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "targets", Namespace: "servitor"}, Data: map[string]string{"config.yaml": "version: 1\ntargets:\n  target-a:\n    providers: [vpc-gen2]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://resource-manager.example.invalid\n      resource_controller: https://resource-controller.example.invalid\n      vpc: https://vpc.{region}.example.invalid\n  target-b:\n    providers: [vpc-gen2]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://resource-manager.example.invalid\n      resource_controller: https://resource-controller.example.invalid\n      vpc: https://vpc.{region}.example.invalid\n"}}
	blockedName := state.NewInventoryStore(nil, "servitor").Name("target-b")
	blocked := false
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(config).WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, underlying client.WithWatch, object client.Object, options ...client.CreateOption) error {
		if configMap, ok := object.(*corev1.ConfigMap); ok && configMap.Name == blockedName && !blocked {
			blocked = true
			return errors.New("injected target-b state write failure")
		}
		return underlying.Create(ctx, object, options...)
	}}).Build()
	responses := &memoryResponder{}
	bot := Bot{Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Responder: responses, InventoryConfigMap: "targets", InventoryConfigKey: "config.yaml", MaintainerIDs: []string{"U-maintainer"}}
	event := Envelope{ID: "partial-write", Message: Message{Channel: "D1", ChannelType: "im", User: "U-maintainer", Text: "refresh inventory", Timestamp: "1"}}
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses; !reflect.DeepEqual(got, []Response{{Channel: "D1", Text: "Inventory refresh is unavailable. Try again later."}}) {
		t.Fatalf("partial registration response = %+v", got)
	}
	store := state.NewInventoryStore(kube, "servitor")
	registered, err := store.Get(context.Background(), "target-a")
	if err != nil || len(registered.ManualRefreshRequests) != 1 || !reflect.DeepEqual(registered.ManualRefreshRequests[0].Targets, []string{"target-a"}) || registered.ManualRefreshRequests[0].Outcome != state.ManualRefreshFailed {
		t.Fatalf("partial registration state = %+v, %v", registered, err)
	}
	if _, err := store.Get(context.Background(), "target-b"); !apierrors.IsNotFound(err) {
		t.Fatalf("failed target retained incomplete request: %v", err)
	}
	if err := bot.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if got := len(responses.responses); got != 1 {
		t.Fatalf("replay registered another request: %+v", responses.responses)
	}

	notifier := &InventoryRefreshNotifier{Client: kube, Namespace: "servitor", Responder: responses, Receipts: state.NewEventStore(kube, "servitor")}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses; !reflect.DeepEqual(got, []Response{{Channel: "D1", Text: "Inventory refresh is unavailable. Try again later."}, {Channel: "D1", Text: "Inventory refresh failed. The last successful inventory remains in use where available."}}) {
		t.Fatalf("partial registration completion = %+v", got)
	}
	delivered, err := store.Get(context.Background(), "target-a")
	if err != nil || delivered.ManualRefreshRequests[0].DeliveredAt == nil {
		t.Fatalf("partial registration delivery = %+v, %v", delivered, err)
	}
	restarted := *notifier
	if err := restarted.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(responses.responses); got != 2 {
		t.Fatalf("restart duplicated partial-registration completion: %+v", responses.responses)
	}
}

func TestMaintainerRefreshCrashReplayRegistersMissingTargets(t *testing.T) {
	bot, responses := botWithPublishedInventory(t, false)
	bot.MaintainerIDs = []string{"U-maintainer"}
	config := &corev1.ConfigMap{}
	if err := bot.Client.Get(context.Background(), types.NamespacedName{Namespace: bot.Namespace, Name: bot.InventoryConfigMap}, config); err != nil {
		t.Fatal(err)
	}
	config.Data[bot.InventoryConfigKey] += "  target-b:\n    providers: [vpc-gen2]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://resource-manager.example.invalid\n      resource_controller: https://resource-controller.example.invalid\n      vpc: https://vpc.{region}.example.invalid\n"
	if err := bot.Client.Update(context.Background(), config); err != nil {
		t.Fatal(err)
	}

	request := state.ManualRefreshRequest{ID: "crash-after-target-a", ChannelID: "D1", OwnerID: "U-maintainer", Targets: []string{"target-a", "target-b"}}
	store := state.NewInventoryStore(bot.Client, bot.Namespace)
	if _, active, duplicate, err := store.RequestManualRefresh(context.Background(), "target-a", request); err != nil || active || duplicate {
		t.Fatalf("persisted pre-crash target = active:%t duplicate:%t err:%v", active, duplicate, err)
	}

	restarted := bot
	event := Envelope{ID: request.ID, Message: Message{Channel: "D1", ChannelType: "im", User: "U-maintainer", Text: "refresh inventory", Timestamp: "1"}}
	if err := restarted.Handle(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses; !reflect.DeepEqual(got, []Response{{Channel: "D1", Text: "Inventory refresh already running."}}) {
		t.Fatalf("crash replay response = %+v", got)
	}
	for _, target := range request.Targets {
		current, err := store.Get(context.Background(), target)
		if err != nil || len(current.ManualRefreshRequests) != 1 || !reflect.DeepEqual(current.ManualRefreshRequests[0].Targets, request.Targets) {
			t.Fatalf("crash replay target %s = %+v, %v", target, current, err)
		}
		if _, err := store.Update(context.Background(), target, func(current *state.InventoryState) error {
			current.ManualRefreshRequests[0].Outcome = state.ManualRefreshSucceeded
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	notifier := &InventoryRefreshNotifier{Client: bot.Client, Namespace: bot.Namespace, Responder: responses, Receipts: state.NewEventStore(bot.Client, bot.Namespace)}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses; !reflect.DeepEqual(got, []Response{{Channel: "D1", Text: "Inventory refresh already running."}, {Channel: "D1", Text: "Inventory refresh complete."}}) {
		t.Fatalf("crash replay completion = %+v", got)
	}
}

func TestRemovedTargetRefreshRedeliversAfterRestartAndCleansUp(t *testing.T) {
	bot, responses := botWithPublishedInventory(t, false)
	store := state.NewInventoryStore(bot.Client, bot.Namespace)
	request := state.ManualRefreshRequest{ID: "removed", ChannelID: "D1", OwnerID: "U-maintainer", Targets: []string{"target-a"}, RunID: "inventory-run", Outcome: state.ManualRefreshFailed}
	if _, err := store.Update(context.Background(), "target-a", func(current *state.InventoryState) error {
		current.Revision = ""
		current.ActiveRunID = ""
		current.RunDeadlineAt = nil
		current.NextAttemptAt = nil
		current.PublishedAt = nil
		current.Catalog = nil
		current.Disposition = state.InventoryInvalidated
		current.Removed = true
		current.ManualRefreshRequests = []state.ManualRefreshRequest{request}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	responses.err = errors.New("Slack unavailable")
	notifier := &InventoryRefreshNotifier{Client: bot.Client, Namespace: bot.Namespace, Responder: responses, Receipts: state.NewEventStore(bot.Client, bot.Namespace)}
	if err := notifier.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	pending, err := store.Get(context.Background(), "target-a")
	if err != nil || pending.Catalog != nil || !pending.Removed || pending.ManualRefreshRequests[0].DeliveredAt != nil {
		t.Fatalf("failed delivery retained unsafe state = %+v, %v", pending, err)
	}

	responses.err = nil
	restarted := *notifier
	if err := restarted.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses; !reflect.DeepEqual(got, []Response{{Channel: "D1", Text: "Inventory refresh failed. The last successful inventory remains in use where available."}}) {
		t.Fatalf("redelivered removal response = %+v", got)
	}
	if _, err := store.Get(context.Background(), "target-a"); !apierrors.IsNotFound(err) {
		t.Fatalf("delivered removed target state remains: %v", err)
	}
	if err := restarted.notify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(responses.responses); got != 1 {
		t.Fatalf("restart duplicated completion: %+v", responses.responses)
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

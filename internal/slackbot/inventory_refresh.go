package slackbot

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/bevicted/servitor/internal/state"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// InventoryRefreshNotifier delivers one safe summary after every target joined
// by a maintainer request has reached a terminal inventory result.
type InventoryRefreshNotifier struct {
	Client    client.Client
	Namespace string
	Responder Responder
	Receipts  EventStore
	Interval  time.Duration
	Clock     func() time.Time
}

func (n *InventoryRefreshNotifier) NeedLeaderElection() bool { return true }

func (n *InventoryRefreshNotifier) Start(ctx context.Context) error {
	interval := n.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := n.notify(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

type refreshSummary struct {
	request         state.ManualRefreshRequest
	outcome         map[string]string
	deliveryPending bool
}

func (n *InventoryRefreshNotifier) notify(ctx context.Context) error {
	if n.Client == nil || n.Namespace == "" || n.Responder == nil || n.Receipts == nil {
		return fmt.Errorf("inventory refresh notifier is not configured")
	}
	store := state.NewInventoryStore(n.Client, n.Namespace)
	states, err := store.List(ctx)
	if err != nil {
		return err
	}
	summaries := make(map[string]*refreshSummary)
	for _, current := range states {
		for _, request := range current.ManualRefreshRequests {
			summary := summaries[request.ID]
			if summary == nil {
				summary = &refreshSummary{request: request, outcome: make(map[string]string, len(request.Targets))}
				summaries[request.ID] = summary
			}
			summary.outcome[current.Target] = request.Outcome
			summary.deliveryPending = summary.deliveryPending || request.DeliveredAt == nil
		}
	}
	ids := make([]string, 0, len(summaries))
	for id := range summaries {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		summary := summaries[id]
		failed, complete := refreshComplete(summary)
		if !complete || !summary.deliveryPending {
			continue
		}
		receipt := inventoryRefreshReceiptID(id)
		claimed, err := n.Receipts.Claim(ctx, receipt)
		if err != nil {
			return err
		}
		if !claimed {
			delivered, err := n.Receipts.Delivered(ctx, receipt)
			if err != nil {
				return err
			}
			if delivered {
				if err := n.markDelivered(ctx, store, summary.request); err != nil {
					return err
				}
			}
			continue
		}
		if err := n.Responder.Reply(ctx, Response{Channel: summary.request.ChannelID, ThreadTimestamp: summary.request.ThreadTimestamp, Text: inventoryRefreshSummaryText(failed, len(summary.request.Targets))}); err != nil {
			if releaseErr := n.Receipts.Release(ctx, receipt); releaseErr != nil {
				return releaseErr
			}
			continue
		}
		if err := n.Receipts.MarkDelivered(ctx, receipt); err != nil {
			return err
		}
		if err := n.markDelivered(ctx, store, summary.request); err != nil {
			return err
		}
	}
	return nil
}

func refreshComplete(summary *refreshSummary) (failed int, complete bool) {
	if len(summary.request.Targets) == 0 {
		return 0, false
	}
	for _, target := range summary.request.Targets {
		outcome := summary.outcome[target]
		if outcome == "" {
			return 0, false
		}
		if outcome == state.ManualRefreshFailed {
			failed++
		}
	}
	return failed, true
}

func (n *InventoryRefreshNotifier) markDelivered(ctx context.Context, store *state.InventoryStore, request state.ManualRefreshRequest) error {
	for _, target := range request.Targets {
		if err := store.MarkManualRefreshDelivered(ctx, target, request.ID, n.now()); err != nil {
			return err
		}
	}
	return nil
}

func (n *InventoryRefreshNotifier) now() time.Time {
	if n.Clock != nil {
		return n.Clock().UTC()
	}
	return time.Now().UTC()
}

func inventoryRefreshReceiptID(id string) string { return "inventory-refresh:" + id }

func inventoryRefreshSummaryText(failed, total int) string {
	switch failed {
	case 0:
		return "Inventory refresh complete."
	case total:
		return "Inventory refresh failed. The last successful inventory remains in use where available."
	default:
		return "Inventory refresh partially failed. The last successful inventory remains in use where available."
	}
}

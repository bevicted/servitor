// Package state persists Servitor metadata that must survive Socket Mode redelivery.
package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultReceiptConfigMap = "servitor-slack-receipts"
	maxReceipts             = 1024
)

// EventStore records Slack event IDs in a bounded namespaced ConfigMap. It is
// durable across operator restarts and uses resource-version retries so only
// one leader can admit a recent redelivered event. Mutating commands also
// persist their CR-specific idempotency state because an evicted receipt cannot
// identify an arbitrarily old redelivery.
type EventStore struct {
	Client    client.Client
	Namespace string
	Name      string
	Now       func() time.Time
}

// NewEventStore constructs the Kubernetes-backed receipt store.
func NewEventStore(kube client.Client, namespace string) *EventStore {
	return &EventStore{Client: kube, Namespace: namespace, Name: defaultReceiptConfigMap}
}

// Claim returns false for an already handled event. A new receipt is persisted
// before Claim returns, preventing replay from issuing another spec mutation.
func (s *EventStore) Claim(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("claim event: empty ID")
	}
	if s.Client == nil || s.Namespace == "" {
		return false, fmt.Errorf("claim event: Kubernetes receipt store is not configured")
	}
	key := receiptKey(id)
	claimed := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm := &corev1.ConfigMap{}
		err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: s.name()}, cm)
		if apierrors.IsNotFound(err) {
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: s.name(), Namespace: s.Namespace}, Data: map[string]string{key: s.now().Format(time.RFC3339Nano)}}
			if err := s.Client.Create(ctx, cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, s.name(), err)
				}
				return err
			}
			claimed = true
			return nil
		}
		if err != nil {
			return err
		}
		if cm.Data[key] != "" {
			claimed = false
			return nil
		}
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		pruneReceipts(cm.Data, maxReceipts-1)
		cm.Data[key] = s.now().Format(time.RFC3339Nano)
		if err := s.Client.Update(ctx, cm); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("claim event: %w", err)
	}
	return claimed, nil
}

// Seen reports whether an identifier was recorded. It supports best-effort
// notification progress; Slack delivery remains non-transactional.
func (s *EventStore) Seen(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("read receipt: empty ID")
	}
	cm := &corev1.ConfigMap{}
	err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: s.name()}, cm)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read receipt: %w", err)
	}
	return cm.Data[receiptKey(id)] != "", nil
}

func (s *EventStore) name() string {
	if s.Name != "" {
		return s.Name
	}
	return defaultReceiptConfigMap
}
func (s *EventStore) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}
func receiptKey(id string) string {
	digest := sha256.Sum256([]byte(id))
	return "r-" + hex.EncodeToString(digest[:])
}
func pruneReceipts(receipts map[string]string, keep int) {
	if len(receipts) <= keep {
		return
	}
	type entry struct {
		key string
		at  time.Time
	}
	entries := make([]entry, 0, len(receipts))
	for key, value := range receipts {
		at, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			at = time.Time{}
		}
		entries = append(entries, entry{key: key, at: at})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].at.Equal(entries[j].at) {
			return entries[i].key < entries[j].key
		}
		return entries[i].at.Before(entries[j].at)
	})
	for len(entries) >= keep {
		delete(receipts, entries[0].key)
		entries = entries[1:]
	}
}

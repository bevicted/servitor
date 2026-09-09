package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bevicted/servitor/internal/inventory"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	inventoryStatePrefix   = "servitor-inventory-"
	inventoryStateLabel    = "servitor.bevicted.github.io/inventory-state"
	inventoryStateKey      = "state.json"
	maxInventoryStateBytes = 512 * 1024
)

type InventoryDisposition string

const (
	InventoryMissing     InventoryDisposition = "missing"
	InventoryRunning     InventoryDisposition = "running"
	InventorySucceeded   InventoryDisposition = "succeeded"
	InventoryFailed      InventoryDisposition = "failed"
	InventoryExpired     InventoryDisposition = "expired"
	InventoryUnusable    InventoryDisposition = "unusable"
	InventoryInvalidated InventoryDisposition = "invalidated"
)

// InventoryState is the private, target-scoped durable state for discovery.
type InventoryState struct {
	Version       int                  `json:"version"`
	Target        string               `json:"target"`
	Revision      string               `json:"revision"`
	ActiveRunID   string               `json:"activeRunID,omitempty"`
	RunDeadlineAt *metav1.Time         `json:"runDeadlineAt,omitempty"`
	NextAttemptAt *metav1.Time         `json:"nextAttemptAt,omitempty"`
	PublishedAt   *metav1.Time         `json:"publishedAt,omitempty"`
	Catalog       *inventory.Catalog   `json:"catalog,omitempty"`
	Disposition   InventoryDisposition `json:"disposition"`
}

// InventoryStore persists inventory state in namespaced ConfigMaps.
type InventoryStore struct {
	Client    client.Client
	Namespace string
}

func NewInventoryStore(kube client.Client, namespace string) *InventoryStore {
	return &InventoryStore{Client: kube, Namespace: namespace}
}

func (s *InventoryStore) Name(target string) string {
	digest := sha256.Sum256([]byte(target))
	return inventoryStatePrefix + hex.EncodeToString(digest[:])[:16]
}

// Update atomically replaces one complete target state.
func (s *InventoryStore) Update(ctx context.Context, target string, mutate func(*InventoryState) error) (InventoryState, error) {
	if s.Client == nil || s.Namespace == "" || target == "" || mutate == nil {
		return InventoryState{}, errors.New("inventory state store is not configured")
	}
	var result InventoryState
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		key := types.NamespacedName{Namespace: s.Namespace, Name: s.Name(target)}
		cm := &corev1.ConfigMap{}
		err := s.Client.Get(ctx, key, cm)
		created := apierrors.IsNotFound(err)
		if err != nil && !created {
			return err
		}
		state := InventoryState{Version: 1, Target: target, Disposition: InventoryMissing}
		if !created {
			var decodeErr error
			state, decodeErr = decodeInventoryState(cm.Data[inventoryStateKey])
			if decodeErr != nil || state.Target != target {
				return errors.New("stored inventory state is invalid")
			}
		}
		if err := mutate(&state); err != nil {
			return err
		}
		if err := validateInventoryState(state, target); err != nil {
			return err
		}
		encoded, err := json.Marshal(state)
		if err != nil || len(encoded) > maxInventoryStateBytes {
			return errors.New("inventory state exceeds byte limit")
		}
		if created {
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: map[string]string{inventoryStateLabel: "true"}}, Data: map[string]string{inventoryStateKey: string(encoded)}}
			if err := s.Client.Create(ctx, cm); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, key.Name, err)
				}
				return err
			}
		} else {
			if cm.Data == nil {
				cm.Data = map[string]string{}
			}
			cm.Data[inventoryStateKey] = string(encoded)
			if err := s.Client.Update(ctx, cm); err != nil {
				return err
			}
		}
		result = state
		return nil
	})
	if err != nil {
		return InventoryState{}, fmt.Errorf("update inventory state: %w", err)
	}
	return result, nil
}

func (s *InventoryStore) Get(ctx context.Context, target string) (InventoryState, error) {
	cm := &corev1.ConfigMap{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: s.Name(target)}, cm); err != nil {
		return InventoryState{}, err
	}
	state, err := decodeInventoryState(cm.Data[inventoryStateKey])
	if err != nil || state.Target != target || validateInventoryState(state, target) != nil {
		return InventoryState{}, errors.New("stored inventory state is invalid")
	}
	return state, nil
}

func (s *InventoryStore) List(ctx context.Context) ([]InventoryState, error) {
	var maps corev1.ConfigMapList
	if err := s.Client.List(ctx, &maps, client.InNamespace(s.Namespace), client.MatchingLabels{inventoryStateLabel: "true"}); err != nil {
		return nil, err
	}
	states := make([]InventoryState, 0, len(maps.Items))
	for i := range maps.Items {
		state, err := decodeInventoryState(maps.Items[i].Data[inventoryStateKey])
		if err != nil || validateInventoryState(state, state.Target) != nil {
			return nil, errors.New("stored inventory state is invalid")
		}
		states = append(states, state)
	}
	return states, nil
}

func (s *InventoryStore) Delete(ctx context.Context, target string) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: s.Name(target), Namespace: s.Namespace}}
	return client.IgnoreNotFound(s.Client.Delete(ctx, cm))
}

// Snapshot reports whether a target snapshot can safely drive inventory matching.
func (s *InventoryStore) Snapshot(ctx context.Context, target, revision string, now time.Time, maximumAge time.Duration) (inventory.Catalog, InventoryDisposition, error) {
	state, err := s.Get(ctx, target)
	if apierrors.IsNotFound(err) {
		return inventory.Catalog{}, InventoryMissing, nil
	}
	if err != nil {
		return inventory.Catalog{}, InventoryMissing, err
	}
	if state.Revision != revision || state.Catalog == nil || state.PublishedAt == nil {
		return inventory.Catalog{}, InventoryMissing, nil
	}
	if maximumAge <= 0 || now.Before(state.PublishedAt.Time) || now.Sub(state.PublishedAt.Time) > maximumAge {
		return inventory.Catalog{}, InventoryExpired, nil
	}
	if state.Catalog.Validate() != nil || state.Catalog.Target != target {
		return inventory.Catalog{}, InventoryUnusable, nil
	}
	return *state.Catalog, InventorySucceeded, nil
}

func decodeInventoryState(data string) (InventoryState, error) {
	if data == "" || len(data) > maxInventoryStateBytes {
		return InventoryState{}, errors.New("inventory state is empty or oversized")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(data))
	decoder.DisallowUnknownFields()
	var state InventoryState
	if err := decoder.Decode(&state); err != nil {
		return InventoryState{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return InventoryState{}, errors.New("inventory state has multiple documents")
	}
	return state, nil
}

func validateInventoryState(state InventoryState, target string) error {
	if state.Version != 1 || state.Target != target || target == "" || state.Disposition == "" {
		return errors.New("inventory state is invalid")
	}
	return nil
}

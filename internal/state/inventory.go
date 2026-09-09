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
	inventoryStatePrefix     = "servitor-inventory-"
	inventoryStateLabel      = "servitor.bevicted.github.io/inventory-state"
	inventoryStateKey        = "state.json"
	maxInventoryStateBytes   = 512 * 1024
	maxManualRefreshRequests = 512
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
	Version               int                    `json:"version"`
	Target                string                 `json:"target"`
	Revision              string                 `json:"revision"`
	ActiveRunID           string                 `json:"activeRunID,omitempty"`
	RunAttempt            uint64                 `json:"runAttempt,omitempty"`
	RunDeadlineAt         *metav1.Time           `json:"runDeadlineAt,omitempty"`
	NextAttemptAt         *metav1.Time           `json:"nextAttemptAt,omitempty"`
	PublishedAt           *metav1.Time           `json:"publishedAt,omitempty"`
	Catalog               *inventory.Catalog     `json:"catalog,omitempty"`
	Disposition           InventoryDisposition   `json:"disposition"`
	Removed               bool                   `json:"removed,omitempty"`
	ManualRefreshRequests []ManualRefreshRequest `json:"manualRefreshRequests,omitempty"`
}

// ManualRefreshRequest records one maintainer DM and its eventual safe reply.
// A copy is stored for every configured target so it can join that target's
// automatic refresh without a separate manual execution path.
type ManualRefreshRequest struct {
	ID              string       `json:"id"`
	ChannelID       string       `json:"channelID"`
	ThreadTimestamp string       `json:"threadTimestamp,omitempty"`
	OwnerID         string       `json:"ownerID"`
	Targets         []string     `json:"targets"`
	RunID           string       `json:"runID,omitempty"`
	Outcome         string       `json:"outcome,omitempty"`
	DeliveredAt     *metav1.Time `json:"deliveredAt,omitempty"`
}

const (
	ManualRefreshSucceeded = "succeeded"
	ManualRefreshFailed    = "failed"
)

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
		} else if cm.Data[inventoryStateKey] != string(encoded) {
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

// RequestManualRefresh persists a maintainer request before the controller
// observes it. It joins an active automatic or manual run when one exists.
func (s *InventoryStore) RequestManualRefresh(ctx context.Context, target string, request ManualRefreshRequest) (state InventoryState, alreadyRunning, duplicate bool, err error) {
	state, err = s.Update(ctx, target, func(current *InventoryState) error {
		for _, existing := range current.ManualRefreshRequests {
			if existing.ID == request.ID {
				duplicate = true
				alreadyRunning = true
				return nil
			}
			if existing.Outcome == "" {
				alreadyRunning = true
			}
		}
		request.RunID = current.ActiveRunID
		current.ManualRefreshRequests = pruneManualRefreshRequests(append(current.ManualRefreshRequests, request))
		if current.ActiveRunID != "" {
			alreadyRunning = true
			return nil
		}
		// A manual request uses the ordinary scheduler path, but it must not wait
		// for an hourly or failure-backoff deadline.
		current.NextAttemptAt = nil
		return nil
	})
	return state, alreadyRunning, duplicate, err
}

// FailManualRefreshRegistration completes a partially registered request so a
// later target write cannot leave its requester waiting forever.
func (s *InventoryStore) FailManualRefreshRegistration(ctx context.Context, target, id string, registeredTargets []string) error {
	_, err := s.Update(ctx, target, func(current *InventoryState) error {
		for index := range current.ManualRefreshRequests {
			request := &current.ManualRefreshRequests[index]
			if request.ID != id {
				continue
			}
			request.Targets = append([]string(nil), registeredTargets...)
			request.Outcome = ManualRefreshFailed
			return nil
		}
		return errors.New("manual refresh request is missing")
	})
	return err
}

// BindManualRefreshes associates queued requests with the run the controller
// selected through its ordinary per-target scheduling path.
func BindManualRefreshes(state *InventoryState, runID string) {
	for index := range state.ManualRefreshRequests {
		request := &state.ManualRefreshRequests[index]
		if request.RunID == "" && request.Outcome == "" {
			request.RunID = runID
		}
	}
}

// CompleteManualRefreshes records a safe terminal outcome for requests that
// joined runID. The notifier owns delivery acknowledgement separately.
func CompleteManualRefreshes(state *InventoryState, runID, outcome string) {
	for index := range state.ManualRefreshRequests {
		request := &state.ManualRefreshRequests[index]
		if request.RunID == runID && request.Outcome == "" {
			request.Outcome = outcome
		}
	}
}

// FailManualRefreshes resolves unfinished requests when their target is removed.
func FailManualRefreshes(state *InventoryState) {
	for index := range state.ManualRefreshRequests {
		request := &state.ManualRefreshRequests[index]
		if request.Outcome == "" {
			request.Outcome = ManualRefreshFailed
		}
	}
}

// HasUndeliveredManualRefreshes reports whether a safe completion reply remains due.
func HasUndeliveredManualRefreshes(state InventoryState) bool {
	for _, request := range state.ManualRefreshRequests {
		if request.DeliveredAt == nil {
			return true
		}
	}
	return false
}

// MarkManualRefreshDelivered records a completion reply that Slack accepted.
func (s *InventoryStore) MarkManualRefreshDelivered(ctx context.Context, target, id string, deliveredAt time.Time) error {
	_, err := s.Update(ctx, target, func(current *InventoryState) error {
		for index := range current.ManualRefreshRequests {
			request := &current.ManualRefreshRequests[index]
			if request.ID == id {
				request.DeliveredAt = ptrInventoryTime(deliveredAt)
			}
		}
		return nil
	})
	return err
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

// DeleteRemovedIfDelivered cleans up removed-target state after all completion
// replies persisted in it have reached Slack.
func (s *InventoryStore) DeleteRemovedIfDelivered(ctx context.Context, target string) error {
	if s.Client == nil || s.Namespace == "" || target == "" {
		return errors.New("inventory state store is not configured")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		key := types.NamespacedName{Namespace: s.Namespace, Name: s.Name(target)}
		cm := &corev1.ConfigMap{}
		if err := s.Client.Get(ctx, key, cm); err != nil {
			return client.IgnoreNotFound(err)
		}
		current, err := decodeInventoryState(cm.Data[inventoryStateKey])
		if err != nil || current.Target != target {
			return errors.New("stored inventory state is invalid")
		}
		if !current.Removed || HasUndeliveredManualRefreshes(current) {
			return nil
		}
		return client.IgnoreNotFound(s.Client.Delete(ctx, cm))
	})
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
	if state.Version != 1 || state.Target != target || target == "" || state.Disposition == "" || len(state.ManualRefreshRequests) > maxManualRefreshRequests {
		return errors.New("inventory state is invalid")
	}
	seen := make(map[string]struct{}, len(state.ManualRefreshRequests))
	for _, request := range state.ManualRefreshRequests {
		if request.ID == "" || request.ChannelID == "" || request.OwnerID == "" || len(request.Targets) == 0 || (request.Outcome != "" && request.Outcome != ManualRefreshSucceeded && request.Outcome != ManualRefreshFailed) {
			return errors.New("inventory state is invalid")
		}
		if _, found := seen[request.ID]; found {
			return errors.New("inventory state is invalid")
		}
		seen[request.ID] = struct{}{}
	}
	return nil
}

func pruneManualRefreshRequests(requests []ManualRefreshRequest) []ManualRefreshRequest {
	if len(requests) < maxManualRefreshRequests {
		return requests
	}
	for index, request := range requests {
		if request.DeliveredAt != nil {
			return append(requests[:index], requests[index+1:]...)
		}
	}
	return requests
}

func ptrInventoryTime(value time.Time) *metav1.Time {
	result := metav1.NewTime(value.UTC())
	return &result
}

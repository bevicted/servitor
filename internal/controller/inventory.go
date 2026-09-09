package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bevicted/servitor/internal/inventory"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/state"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const inventoryFailureRetry = 5 * time.Minute

var errStaleInventoryRun = errors.New("inventory run is no longer active")

// InventoryConfig identifies the leader-owned private inventory refresh inputs.
type InventoryConfig struct {
	Namespace       string
	TargetConfigMap string
	TargetConfigKey string
	ExecutionImage  string
	TaskConfig      pipeline.TaskConfig
	RefreshInterval time.Duration
	MaximumAge      time.Duration
}

// InventoryReconciler owns target-scoped inventory state and PipelineRuns. It
// never reads credentials or changes allocation resources.
type InventoryReconciler struct {
	client.Client
	Config InventoryConfig
	Logs   pipeline.LogReader
	Now    func() time.Time
}

func (r *InventoryReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if request.Namespace != r.Config.Namespace || request.Name != r.Config.TargetConfigMap {
		return ctrl.Result{}, nil
	}
	return r.Sync(ctx)
}

// Sync processes all configured targets. It is exported for controllable-clock
// Kubernetes-client harnesses; manager wiring invokes it through Reconcile.
func (r *InventoryReconciler) Sync(ctx context.Context) (ctrl.Result, error) {
	if err := r.validate(); err != nil {
		return ctrl.Result{}, err
	}
	configured, err := r.loadTargets(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	store := state.NewInventoryStore(r.Client, r.Config.Namespace)
	states, err := store.List(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, stored := range states {
		if _, found := configured.Targets[stored.Target]; !found {
			if err := r.deleteInventoryRun(ctx, stored.Target, stored.ActiveRunID); err != nil {
				return ctrl.Result{}, err
			}
			if err := store.Delete(ctx, stored.Target); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	now := r.now()
	var wakeAt *time.Time
	for target, targetConfig := range configured.Targets {
		revision, err := inventory.Revision(targetConfig)
		if err != nil {
			return ctrl.Result{}, err
		}
		next, err := r.refreshTarget(ctx, store, target, revision, now)
		if err != nil {
			return ctrl.Result{}, err
		}
		if next != nil && (wakeAt == nil || next.Before(*wakeAt)) {
			wakeAt = next
		}
	}
	if wakeAt == nil {
		return ctrl.Result{}, nil
	}
	wait := wakeAt.Sub(r.now())
	if wait < 0 {
		wait = 0
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

func (r *InventoryReconciler) refreshTarget(ctx context.Context, store *state.InventoryStore, target, revision string, now time.Time) (*time.Time, error) {
	var obsolete string
	current, err := store.Update(ctx, target, func(current *state.InventoryState) error {
		if current.Revision == revision {
			return nil
		}
		obsolete = current.ActiveRunID
		current.Revision = revision
		current.ActiveRunID = ""
		current.RunDeadlineAt = nil
		current.NextAttemptAt = nil
		current.PublishedAt = nil
		current.Catalog = nil
		current.Disposition = state.InventoryInvalidated
		return nil
	})
	if err != nil {
		return nil, err
	}
	if obsolete != "" {
		if err := r.deleteInventoryRun(ctx, target, obsolete); err != nil {
			return nil, err
		}
	}
	if current.ActiveRunID != "" {
		return r.observeInventoryRun(ctx, store, current, now)
	}
	if current.NextAttemptAt != nil && now.Before(current.NextAttemptAt.Time) {
		return &current.NextAttemptAt.Time, nil
	}

	runID := pipeline.DeterministicInventoryRunName(target, revision)
	current, err = store.Update(ctx, target, func(current *state.InventoryState) error {
		if current.Revision != revision {
			return errStaleInventoryRun
		}
		if current.ActiveRunID != "" {
			return nil
		}
		current.ActiveRunID = runID
		current.RunDeadlineAt = ptrTime(now.Add(pipeline.InventoryRunTimeout))
		current.NextAttemptAt = nil
		current.Disposition = state.InventoryRunning
		return nil
	})
	if errors.Is(err, errStaleInventoryRun) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if current.ActiveRunID != runID {
		return r.observeInventoryRun(ctx, store, current, now)
	}
	if err := r.ensureInventoryRun(ctx, target, runID, revision); err != nil {
		return r.failInventoryRun(ctx, store, target, runID, revision, now)
	}
	wake := now.Add(pipeline.InventoryRunTimeout)
	return &wake, nil
}

func (r *InventoryReconciler) observeInventoryRun(ctx context.Context, store *state.InventoryStore, current state.InventoryState, now time.Time) (*time.Time, error) {
	if current.RunDeadlineAt != nil && !now.Before(current.RunDeadlineAt.Time) {
		return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
	}
	run := &tektonv1.PipelineRun{}
	err := r.Get(ctx, types.NamespacedName{Namespace: r.Config.Namespace, Name: current.ActiveRunID}, run)
	if apierrors.IsNotFound(err) {
		if err := r.ensureInventoryRun(ctx, current.Target, current.ActiveRunID, current.Revision); err != nil {
			return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
		}
		if current.RunDeadlineAt != nil {
			return &current.RunDeadlineAt.Time, nil
		}
		wake := now.Add(r.refreshInterval())
		return &wake, nil
	}
	if err != nil {
		return nil, err
	}
	if !pipeline.MatchingInventoryRun(run, current.Target, current.ActiveRunID) {
		return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
	}
	done, success := pipeline.Succeeded(run)
	if !done {
		if current.RunDeadlineAt != nil {
			return &current.RunDeadlineAt.Time, nil
		}
		wake := now.Add(r.refreshInterval())
		return &wake, nil
	}
	if !success {
		return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
	}
	if r.Logs == nil {
		return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
	}
	taskName := pipeline.InventoryReportTaskRunName(run)
	if taskName == "" {
		return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
	}
	task := &tektonv1.TaskRun{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.Config.Namespace, Name: taskName}, task); err != nil {
		return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
	}
	container := pipeline.InventoryReportContainer(task)
	if task.Status.PodName == "" || container == "" {
		return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
	}
	report, err := pipeline.ReadInventoryReport(ctx, r.Logs, r.Config.Namespace, task.Status.PodName, container, current.Target, current.ActiveRunID, current.Revision)
	if err != nil {
		return r.failInventoryRun(ctx, store, current.Target, current.ActiveRunID, current.Revision, now)
	}
	published := now
	_, err = store.Update(ctx, current.Target, func(next *state.InventoryState) error {
		if next.Revision != current.Revision || next.ActiveRunID != current.ActiveRunID {
			return errStaleInventoryRun
		}
		next.ActiveRunID = ""
		next.RunDeadlineAt = nil
		next.Catalog = &report.Catalog
		next.PublishedAt = ptrTime(published)
		next.NextAttemptAt = ptrTime(published.Add(r.refreshInterval()))
		next.Disposition = state.InventorySucceeded
		return nil
	})
	if errors.Is(err, errStaleInventoryRun) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := r.deleteInventoryRun(ctx, current.Target, current.ActiveRunID); err != nil {
		return nil, err
	}
	wake := published.Add(r.refreshInterval())
	return &wake, nil
}

func (r *InventoryReconciler) failInventoryRun(ctx context.Context, store *state.InventoryStore, target, runID, revision string, now time.Time) (*time.Time, error) {
	nextAttempt := now.Add(inventoryFailureRetry)
	_, err := store.Update(ctx, target, func(current *state.InventoryState) error {
		if current.Revision != revision || current.ActiveRunID != runID {
			return errStaleInventoryRun
		}
		current.ActiveRunID = ""
		current.RunDeadlineAt = nil
		current.NextAttemptAt = ptrTime(nextAttempt)
		current.Disposition = state.InventoryFailed
		return nil
	})
	if errors.Is(err, errStaleInventoryRun) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := r.deleteInventoryRun(ctx, target, runID); err != nil {
		return nil, err
	}
	return &nextAttempt, nil
}

func (r *InventoryReconciler) ensureInventoryRun(ctx context.Context, target, runID, revision string) error {
	run, err := pipeline.NewInventoryRun(r.Config.Namespace, r.Config.ExecutionImage, target, runID, revision, r.Config.TaskConfig)
	if err != nil {
		return err
	}
	if err := r.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (r *InventoryReconciler) deleteInventoryRun(ctx context.Context, target, runID string) error {
	if runID == "" {
		return nil
	}
	run := &tektonv1.PipelineRun{}
	key := types.NamespacedName{Namespace: r.Config.Namespace, Name: runID}
	if err := r.Get(ctx, key, run); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !pipeline.MatchingInventoryRun(run, target, runID) {
		return nil
	}
	return r.Delete(ctx, run)
}

func (r *InventoryReconciler) loadTargets(ctx context.Context) (inventory.Config, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.Config.Namespace, Name: r.Config.TargetConfigMap}, cm); err != nil {
		return inventory.Config{}, fmt.Errorf("read inventory target configuration: %w", err)
	}
	configured, err := inventory.LoadConfig([]byte(cm.Data[r.Config.TargetConfigKey]))
	if err != nil {
		return inventory.Config{}, errors.New("inventory target configuration is invalid")
	}
	return configured, nil
}

func (r *InventoryReconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&corev1.ConfigMap{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(object client.Object) bool {
			return object.GetNamespace() == r.Config.Namespace && object.GetName() == r.Config.TargetConfigMap
		}))).
		Watches(&tektonv1.PipelineRun{}, handler.EnqueueRequestsFromMapFunc(r.mapInventoryRun)).
		Complete(r)
}

func (r *InventoryReconciler) mapInventoryRun(_ context.Context, object client.Object) []ctrl.Request {
	if object.GetNamespace() != r.Config.Namespace || object.GetLabels()[pipeline.InventoryRunLabel] == "" || object.GetLabels()[pipeline.InventoryTargetLabel] == "" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: r.Config.Namespace, Name: r.Config.TargetConfigMap}}}
}

func (r *InventoryReconciler) validate() error {
	if r.Client == nil || r.Config.Namespace == "" || r.Config.TargetConfigMap == "" || r.Config.TargetConfigKey == "" || r.Config.ExecutionImage == "" || r.Config.TaskConfig.ICTConfigMap == "" || r.Config.TaskConfig.ICTConfigKey == "" || r.Config.TaskConfig.IBMSecret == "" {
		return errors.New("inventory refresher is not configured")
	}
	if r.refreshInterval() <= 0 || r.maximumAge() < r.refreshInterval() {
		return errors.New("inventory refresh policy is invalid")
	}
	return nil
}

func (r *InventoryReconciler) refreshInterval() time.Duration {
	if r.Config.RefreshInterval > 0 {
		return r.Config.RefreshInterval
	}
	return time.Hour
}
func (r *InventoryReconciler) maximumAge() time.Duration {
	if r.Config.MaximumAge > 0 {
		return r.Config.MaximumAge
	}
	return 24 * time.Hour
}
func (r *InventoryReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}
func ptrTime(value time.Time) *metav1.Time { result := metav1.NewTime(value); return &result }

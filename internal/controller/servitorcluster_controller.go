// Package controller reconciles ServitorCluster operations.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/pipeline"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

const cleanupNotificationGrace = 10 * time.Second

// Config values are loaded once at manager startup. Existing status snapshots always win.
type Config struct {
	Namespace        string
	Defaults         servitorv1alpha1.ResolvedOptions
	Backend          servitorv1alpha1.BackendIdentity
	BackendPrefix    string
	ExecutionImage   string
	TaskConfig       pipeline.TaskConfig
	ReviewTimeout    time.Duration
	OpenShiftFlavor  string
	KubernetesFlavor string
}

// Reconciler is the sole status writer for ServitorCluster.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Config   Config
	Logs     pipeline.LogReader
	Now      func() time.Time
	LogRetry time.Duration
}

// +kubebuilder:rbac:groups=servitor.bevicted.github.io,resources=servitorclusters,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=servitor.bevicted.github.io,resources=servitorclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=servitor.bevicted.github.io,resources=servitorclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns;taskruns,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;create;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	if r.Config.Namespace != "" && request.Namespace != r.Config.Namespace {
		return ctrl.Result{}, nil
	}
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := r.Get(ctx, request.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if err := cluster.Spec.Validate(); err != nil {
		return r.unresolved(ctx, cluster, "InvalidSpec", err)
	}
	if cluster.Status.Phase == servitorv1alpha1.PhaseCleanupComplete || cluster.Status.Cleanup != nil && cluster.Status.Cleanup.CompletedAt != nil {
		if cluster.DeletionTimestamp.IsZero() {
			if cluster.Status.Cleanup != nil && cluster.Status.Cleanup.CompletedAt != nil {
				if remaining := cluster.Status.Cleanup.CompletedAt.Time.Add(cleanupNotificationGrace).Sub(r.now()); remaining > 0 {
					return ctrl.Result{RequeueAfter: remaining}, nil
				}
			}
			return ctrl.Result{}, r.Delete(ctx, cluster)
		}
		return r.removeFinalizer(ctx, cluster)
	}
	if cluster.DeletionTimestamp.IsZero() && !contains(cluster.Finalizers, servitorv1alpha1.CleanupFinalizer) {
		cluster.Finalizers = append(cluster.Finalizers, servitorv1alpha1.CleanupFinalizer)
		return ctrl.Result{}, r.Update(ctx, cluster)
	}

	if !cluster.DeletionTimestamp.IsZero() {
		if cluster.Status.Cleanup == nil {
			return r.requestCleanup(ctx, cluster, servitorv1alpha1.CleanupReasonDeletion)
		}
		return r.reconcileCleanup(ctx, cluster)
	}
	if cluster.Spec.Lifecycle.RequestedExpiry != nil && cluster.Status.Phase != servitorv1alpha1.PhaseReady && !extensionRecorded(cluster) {
		return r.recordExtensionOutcome(ctx, cluster, servitorv1alpha1.ExtensionOutcomeNotReady, nil, nil)
	}
	if cluster.Spec.Lifecycle.CleanupRequested && cluster.Status.Cleanup == nil {
		return r.requestCleanup(ctx, cluster, servitorv1alpha1.CleanupReasonExplicit)
	}
	if cluster.Status.Cleanup != nil || cluster.Status.CleanupRequested {
		return r.reconcileCleanup(ctx, cluster)
	}
	if cluster.Status.ResolvedOptions == nil {
		if err := r.snapshot(cluster); err != nil {
			return r.unresolved(ctx, cluster, "InvalidResolvedOptions", err)
		}
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	if cluster.Status.LifecycleSnapshot == nil {
		return r.unresolved(ctx, cluster, "LifecycleSnapshotMissing", errors.New("lifecycle policy snapshot is missing"))
	}
	if !matchesLifecycleSnapshot(cluster.Spec.Lifecycle, *cluster.Status.LifecycleSnapshot) {
		return r.unresolved(ctx, cluster, "LifecyclePolicyChanged", errors.New("immutable lifecycle policy changed"))
	}
	if cluster.Status.Phase == servitorv1alpha1.PhaseAwaitingApproval {
		return r.reconcileApproval(ctx, cluster)
	}
	if cluster.Status.Phase == servitorv1alpha1.PhaseReady {
		return r.reconcileReady(ctx, cluster)
	}
	if cluster.Status.Phase == servitorv1alpha1.PhaseCleanupComplete || cluster.Status.Phase == servitorv1alpha1.PhaseUnresolved {
		return ctrl.Result{}, nil
	}
	if cluster.Status.Operation == nil {
		now := metav1.NewTime(r.now())
		operation := planningID(string(cluster.UID))
		cluster.Status.Operation = &servitorv1alpha1.OperationReference{ID: operation, Kind: "plan", PipelineRunName: pipeline.DeterministicRunName(string(cluster.UID), operation), StartedAt: now}
		cluster.Status.Phase = servitorv1alpha1.PhasePlanning
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	return r.observeOperation(ctx, cluster)
}

// reconcileReady observes durable lease state. It intentionally uses requeues
// rather than process-local timers, so expiry remains correct after restarts.
func (r *Reconciler) reconcileReady(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	expiry := cluster.Status.LeaseExpiresAt
	if expiry == nil {
		return r.recordExtensionOutcome(ctx, cluster, servitorv1alpha1.ExtensionOutcomeNotReady, nil, nil)
	}
	if !r.now().Before(expiry.Time) {
		if cluster.Spec.Lifecycle.RequestedExpiry != nil && !extensionRecorded(cluster) {
			r.setExtensionOutcome(cluster, servitorv1alpha1.ExtensionOutcomeExpired, expiry, expiry)
		}
		return r.requestCleanup(ctx, cluster, servitorv1alpha1.CleanupReasonLeaseExpired)
	}
	if requested := cluster.Spec.Lifecycle.RequestedExpiry; requested != nil && !extensionRecorded(cluster) {
		increment := requested.Time.Sub(expiry.Time)
		if increment < time.Hour || increment > 24*time.Hour || increment%time.Hour != 0 {
			return r.recordExtensionOutcome(ctx, cluster, servitorv1alpha1.ExtensionOutcomeInvalid, expiry, expiry)
		}
		maximum := r.now().Add(24 * time.Hour)
		newExpiry := requested.Time.UTC()
		if newExpiry.After(maximum) {
			newExpiry = maximum
		}
		return r.recordExtensionOutcome(ctx, cluster, servitorv1alpha1.ExtensionOutcomeApplied, expiry, &metav1.Time{Time: newExpiry})
	}
	return ctrl.Result{RequeueAfter: expiry.Time.Sub(r.now())}, nil
}

// extensionRecorded makes the persisted target an idempotency key. Retried
// spec updates and reconciles therefore preserve the original result.
func extensionRecorded(cluster *servitorv1alpha1.ServitorCluster) bool {
	return cluster.Spec.Lifecycle.RequestedExpiry != nil && cluster.Status.LeaseExtension != nil && cluster.Status.LeaseExtension.RequestedExpiry.Equal(cluster.Spec.Lifecycle.RequestedExpiry)
}

func (r *Reconciler) recordExtensionOutcome(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, outcome servitorv1alpha1.ExtensionOutcome, previous, next *metav1.Time) (ctrl.Result, error) {
	if !r.setExtensionOutcome(cluster, outcome, previous, next) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	if outcome == servitorv1alpha1.ExtensionOutcomeApplied && next != nil {
		return ctrl.Result{RequeueAfter: next.Time.Sub(r.now())}, nil
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) setExtensionOutcome(cluster *servitorv1alpha1.ServitorCluster, outcome servitorv1alpha1.ExtensionOutcome, previous, next *metav1.Time) bool {
	requested := cluster.Spec.Lifecycle.RequestedExpiry
	if requested == nil {
		return false
	}
	result := &servitorv1alpha1.LeaseExtensionStatus{
		RequestedExpiry: metav1.NewTime(requested.Time.UTC()),
		Outcome:         outcome,
	}
	if previous != nil {
		result.PreviousExpiry = previous.DeepCopy()
	}
	if next != nil {
		result.NewExpiry = next.DeepCopy()
	}
	if previous != nil && next != nil {
		result.AddedSeconds = int64(next.Time.Sub(previous.Time).Seconds())
	}
	cluster.Status.LeaseExtension = result
	if outcome == servitorv1alpha1.ExtensionOutcomeApplied && next != nil {
		cluster.Status.LeaseExpiresAt = next.DeepCopy()
	}
	if outcome == servitorv1alpha1.ExtensionOutcomeExpired {
		setCondition(cluster, "Ready", metav1.ConditionFalse, "LeaseExpired", "the lease expired before the extension could be applied")
	}
	return true
}

func (r *Reconciler) reconcileApproval(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	deadline := cluster.Status.ReviewDeadline
	if deadline == nil || !r.now().Before(deadline.Time) {
		return r.requestCleanup(ctx, cluster, servitorv1alpha1.CleanupReasonReviewExpired)
	}
	switch cluster.Spec.Lifecycle.Approval {
	case "rejected":
		return r.requestCleanup(ctx, cluster, servitorv1alpha1.CleanupReasonRejected)
	case "":
		if cluster.Status.ReviewApproval == "approved" {
			cluster.Status.ReviewGeneration = cluster.Generation
			cluster.Status.ReviewApproval = ""
			return ctrl.Result{RequeueAfter: deadline.Time.Sub(r.now())}, r.Status().Update(ctx, cluster)
		}
		return ctrl.Result{RequeueAfter: deadline.Time.Sub(r.now())}, nil
	case "approved":
		if cluster.Status.ReviewGeneration == 0 || cluster.Generation <= cluster.Status.ReviewGeneration || cluster.Status.ReviewApproval == "approved" {
			return ctrl.Result{RequeueAfter: deadline.Time.Sub(r.now())}, nil
		}
		operation := applyID(string(cluster.UID))
		cluster.Status.Operation = &servitorv1alpha1.OperationReference{ID: operation, Kind: "apply", PipelineRunName: pipeline.DeterministicRunName(string(cluster.UID), operation), StartedAt: metav1.NewTime(r.now())}
		// Persist this before run creation: a lost create response must be treated as
		// an apply that may have reached Terraform.
		cluster.Status.ApplyDispatched = true
		cluster.Status.Phase = servitorv1alpha1.PhaseApplying
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	return ctrl.Result{RequeueAfter: deadline.Time.Sub(r.now())}, nil
}

func (r *Reconciler) requestCleanup(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, reason servitorv1alpha1.CleanupReason) (ctrl.Result, error) {
	if cluster.Status.Cleanup == nil {
		cluster.Status.Cleanup = &servitorv1alpha1.CleanupStatus{
			Reason:          reason,
			RequestedAt:     metav1.NewTime(r.now()),
			RequiresDestroy: applyMayHaveRun(cluster),
		}
	}
	cluster.Status.CleanupRequested = true
	if reason == servitorv1alpha1.CleanupReasonApplyFailed {
		cluster.Status.Diagnostic = "ApplyFailed"
	}
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	setCondition(cluster, "Ready", metav1.ConditionFalse, string(cluster.Status.Cleanup.Reason), "cleanup is required")
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

func applyMayHaveRun(cluster *servitorv1alpha1.ServitorCluster) bool {
	if cluster.Status.ApplyDispatched || cluster.Status.Ready != nil || cluster.Status.Phase == servitorv1alpha1.PhaseReady || cluster.Status.Phase == servitorv1alpha1.PhaseApplying {
		return true
	}
	return cluster.Status.Operation != nil && cluster.Status.Operation.Kind == "apply"
}

func (r *Reconciler) reconcileCleanup(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	if cluster.Status.Cleanup == nil {
		return r.requestCleanup(ctx, cluster, servitorv1alpha1.CleanupReasonExplicit)
	}
	cleanup := cluster.Status.Cleanup
	if cleanup.CompletedAt != nil {
		return r.removeFinalizer(ctx, cluster)
	}
	if cluster.Status.Phase == servitorv1alpha1.PhaseUnresolved {
		return ctrl.Result{}, nil
	}
	if cleanup.NextRetryAt != nil && r.now().Before(cleanup.NextRetryAt.Time) {
		return ctrl.Result{RequeueAfter: cleanup.NextRetryAt.Time.Sub(r.now())}, nil
	}
	if cluster.Status.Operation != nil {
		return r.waitForCleanupOperation(ctx, cluster)
	}
	if !cleanup.RequiresDestroy {
		return r.completeCleanup(ctx, cluster)
	}
	operation := destroyID(string(cluster.UID), cleanup.RetryCount)
	cluster.Status.Operation = &servitorv1alpha1.OperationReference{
		ID: operation, Kind: "destroy", PipelineRunName: pipeline.DeterministicRunName(string(cluster.UID), operation), StartedAt: metav1.NewTime(r.now()),
	}
	cleanup.NextRetryAt = nil
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

// waitForCleanupOperation serializes destroy behind any persisted plan/apply run.
func (r *Reconciler) waitForCleanupOperation(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	operation := cluster.Status.Operation
	run := &tektonv1.PipelineRun{}
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: operation.PipelineRunName}, run)
	if apierrors.IsNotFound(err) {
		if operation.Dispatched {
			reason := "OperationRunMissing"
			if operation.Kind == "destroy" {
				reason = "DestroyRunMissing"
			}
			return r.unresolved(ctx, cluster, reason, errors.New("persisted dispatched PipelineRun is missing"))
		}
		if operation.Kind == "destroy" {
			created, buildErr := pipeline.NewDestroyRun(cluster, r.Config.TaskConfig)
			if buildErr != nil {
				return r.unresolved(ctx, cluster, "InvalidOperation", buildErr)
			}
			if createErr := r.Create(ctx, created); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
				return ctrl.Result{}, createErr
			}
			operation.Dispatched = true
			return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, cluster)
		}
		// The operation was persisted but never observed in the API. It cannot be
		// running; apply ownership was already conservatively recorded at launch.
		cluster.Status.Operation = nil
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !pipeline.MatchingRun(run, string(cluster.UID), operation.ID) {
		return r.unresolved(ctx, cluster, "OperationIdentityMismatch", errors.New("existing PipelineRun does not match active operation"))
	}
	done, succeeded := pipeline.Succeeded(run)
	if !done {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	if operation.Kind != "destroy" {
		if operation.Kind == "apply" {
			cluster.Status.ApplyDispatched = true
		}
		cluster.Status.Operation = nil
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	if !succeeded {
		return r.recordDestroyFailure(ctx, cluster)
	}
	if err := r.consumeDestroyReport(ctx, cluster, run); err != nil {
		return r.handleDestroyReportError(ctx, cluster, err)
	}
	return r.completeCleanup(ctx, cluster)
}

func (r *Reconciler) consumeDestroyReport(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, run *tektonv1.PipelineRun) error {
	taskRunName := pipeline.ReportTaskRunName(run)
	if taskRunName == "" {
		return errors.New("completed destroy PipelineRun has no operation TaskRun")
	}
	taskRun := &tektonv1.TaskRun{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: taskRunName}, taskRun); err != nil {
		return err
	}
	container := pipeline.ReportContainer(taskRun)
	if taskRun.Status.PodName == "" || container == "" {
		return errors.New("completed destroy operation has no report container log")
	}
	if r.Logs == nil {
		return errors.New("Pod log client is not configured")
	}
	_, err := pipeline.ReadReport(ctx, r.Logs, cluster.Namespace, taskRun.Status.PodName, container, string(cluster.UID), cluster.Status.Operation.ID)
	return err
}

func (r *Reconciler) handleDestroyReportError(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, err error) (ctrl.Result, error) {
	if pipeline.IsLogReadError(err) && !apierrors.IsNotFound(err) && !apierrors.IsBadRequest(err) {
		return ctrl.Result{RequeueAfter: r.logRetry()}, nil
	}
	return r.unresolved(ctx, cluster, "DestroyReportMissing", err)
}

func (r *Reconciler) recordDestroyFailure(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	cleanup := cluster.Status.Cleanup
	cluster.Status.Operation = nil
	cluster.Status.Diagnostic = "DestroyFailed"
	retrySeconds := cluster.Status.LifecycleSnapshot.RetrySeconds
	if cleanup.RetryCount >= len(retrySeconds) {
		cleanup.NextRetryAt = nil
		cluster.Status.Phase = servitorv1alpha1.PhaseUnresolved
		setCondition(cluster, "Ready", metav1.ConditionFalse, "Unresolved", "destroy retries are exhausted; recovery context and finalizer are retained")
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	delay := time.Duration(retrySeconds[cleanup.RetryCount]) * time.Second
	cleanup.RetryCount++
	next := metav1.NewTime(r.now().Add(delay))
	cleanup.NextRetryAt = &next
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	setCondition(cluster, "Ready", metav1.ConditionFalse, "DestroyRetry", "destroy failed; retry is scheduled")
	return ctrl.Result{RequeueAfter: delay}, r.Status().Update(ctx, cluster)
}

func (r *Reconciler) completeCleanup(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	if err := r.deleteTerminalOperationRuns(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	now := metav1.NewTime(r.now())
	cluster.Status.Operation = nil
	cluster.Status.Cleanup.NextRetryAt = nil
	cluster.Status.Cleanup.CompletedAt = &now
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupComplete
	cluster.Status.Diagnostic = ""
	setCondition(cluster, "Ready", metav1.ConditionFalse, "CleanupComplete", "cleanup completed")
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: cleanupNotificationGrace}, nil
}

func (r *Reconciler) deleteTerminalOperationRuns(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) error {
	var runs tektonv1.PipelineRunList
	if err := r.List(ctx, &runs, client.InNamespace(cluster.Namespace), client.MatchingLabels{pipeline.ClusterUIDLabel: string(cluster.UID)}); err != nil {
		return err
	}
	for i := range runs.Items {
		done, _ := pipeline.Succeeded(&runs.Items[i])
		if !done {
			return fmt.Errorf("active PipelineRun %q cannot be removed during cleanup", runs.Items[i].Name)
		}
		if err := r.Delete(ctx, &runs.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *Reconciler) removeFinalizer(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	if !contains(cluster.Finalizers, servitorv1alpha1.CleanupFinalizer) {
		return ctrl.Result{}, nil
	}
	finalizers := make([]string, 0, len(cluster.Finalizers)-1)
	for _, finalizer := range cluster.Finalizers {
		if finalizer != servitorv1alpha1.CleanupFinalizer {
			finalizers = append(finalizers, finalizer)
		}
	}
	cluster.Finalizers = finalizers
	return ctrl.Result{}, r.Update(ctx, cluster)
}

func (r *Reconciler) observeOperation(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	operation := cluster.Status.Operation
	run := &tektonv1.PipelineRun{}
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: operation.PipelineRunName}, run)
	if apierrors.IsNotFound(err) {
		if operation.Dispatched {
			return r.unresolved(ctx, cluster, "OperationRunMissing", errors.New("persisted dispatched PipelineRun is missing"))
		}
		var created *tektonv1.PipelineRun
		var buildErr error
		switch operation.Kind {
		case "plan":
			created, buildErr = pipeline.NewPlanningRun(cluster, r.Config.TaskConfig)
		case "apply":
			created, buildErr = pipeline.NewApplyRun(cluster, r.Config.TaskConfig)
		default:
			buildErr = errors.New("unsupported operation kind")
		}
		if buildErr != nil {
			return r.unresolved(ctx, cluster, "InvalidOperation", buildErr)
		}
		if createErr := r.Create(ctx, created); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return ctrl.Result{}, createErr
		}
		operation.Dispatched = true
		return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, cluster)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !pipeline.MatchingRun(run, string(cluster.UID), operation.ID) {
		return r.unresolved(ctx, cluster, "OperationIdentityMismatch", errors.New("existing PipelineRun does not match active operation"))
	}
	done, succeeded := pipeline.Succeeded(run)
	if !done {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	if !succeeded {
		if operation.Kind == "apply" {
			return r.requestCleanup(ctx, cluster, servitorv1alpha1.CleanupReasonApplyFailed)
		}
		return r.requestCleanup(ctx, cluster, servitorv1alpha1.CleanupReasonPlanningFailed)
	}
	if operation.Adopted {
		return ctrl.Result{}, nil
	}
	return r.adoptReport(ctx, cluster, run)
}

func (r *Reconciler) adoptReport(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, run *tektonv1.PipelineRun) (ctrl.Result, error) {
	taskRunName := pipeline.ReportTaskRunName(run)
	if taskRunName == "" {
		return r.unresolved(ctx, cluster, "ReportMissing", errors.New("completed PipelineRun has no operation TaskRun"))
	}
	taskRun := &tektonv1.TaskRun{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: taskRunName}, taskRun); err != nil {
		if apierrors.IsNotFound(err) {
			return r.unresolved(ctx, cluster, "ReportMissing", errors.New("completed operation TaskRun is missing"))
		}
		return ctrl.Result{}, err
	}
	container := pipeline.ReportContainer(taskRun)
	if taskRun.Status.PodName == "" || container == "" {
		return r.unresolved(ctx, cluster, "ReportMissing", errors.New("completed operation has no report container log"))
	}
	if r.Logs == nil {
		return ctrl.Result{}, errors.New("Pod log client is not configured")
	}
	report, err := pipeline.ReadReport(ctx, r.Logs, cluster.Namespace, taskRun.Status.PodName, container, string(cluster.UID), cluster.Status.Operation.ID)
	if err != nil {
		if pipeline.IsLogReadError(err) {
			if apierrors.IsNotFound(err) || apierrors.IsBadRequest(err) {
				return r.unresolved(ctx, cluster, "ReportMissing", err)
			}
			return ctrl.Result{RequeueAfter: r.logRetry()}, nil
		}
		return r.unresolved(ctx, cluster, "InvalidReport", err)
	}
	cluster.Status.Operation.Adopted = true
	if cluster.Status.Operation.Kind == "plan" {
		cluster.Status.ResolvedOptions = &report.ResolvedOptions
		cluster.Status.Recovery = &report.Recovery
		cluster.Status.Review = &report.Review
		cluster.Status.Phase = servitorv1alpha1.PhaseAwaitingApproval
		deadline := metav1.NewTime(r.now().Add(r.reviewTimeout()))
		cluster.Status.ReviewDeadline = &deadline
		cluster.Status.ReviewGeneration = cluster.Generation
		cluster.Status.ReviewApproval = cluster.Spec.Lifecycle.Approval
		setCondition(cluster, "PlanningSucceeded", metav1.ConditionTrue, "ReportAdopted", "validated planning report adopted")
	} else {
		cluster.Status.Ready = &report.Ready
		cluster.Status.Phase = servitorv1alpha1.PhaseReady
		// This is the sole Ready transition. Never recompute this persisted
		// deadline during report adoption or later duplicate reconciles.
		if cluster.Status.LeaseExpiresAt == nil {
			expiry := metav1.NewTime(r.now().Add(time.Duration(cluster.Status.LifecycleSnapshot.InitialLeaseSeconds) * time.Second))
			cluster.Status.LeaseExpiresAt = &expiry
		}
		setCondition(cluster, "Ready", metav1.ConditionTrue, "ReportAdopted", "validated apply report adopted")
	}
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

func (r *Reconciler) snapshot(cluster *servitorv1alpha1.ServitorCluster) error {
	resolved := r.Config.Defaults
	// Platform and VPC worker flavor are derived from the selected request
	// version and provider, rather than inherited from another default version.
	resolved.Platform = ""
	resolved.Flavor = ""
	overlay(&resolved.UserOptions, cluster.Spec.UserOptions)
	platform, err := command.InferPlatform(resolved.Version)
	if err != nil {
		return fmt.Errorf("infer platform: %w", err)
	}
	if resolved.Platform == "" {
		resolved.Platform = platform
	} else if resolved.Platform != platform {
		return fmt.Errorf("platform %q does not match version %q", resolved.Platform, resolved.Version)
	}
	if resolved.Provider == "vpc-gen2" && resolved.Flavor == "" {
		if resolved.Platform == "openshift" {
			resolved.Flavor = r.Config.OpenShiftFlavor
		} else {
			resolved.Flavor = r.Config.KubernetesFlavor
		}
	}
	if resolved.Provider == "classic" {
		resolved.VPCID = ""
	}
	cluster.Status.ResolvedOptions = &resolved
	cluster.Status.LifecycleSnapshot = &servitorv1alpha1.LifecycleSnapshot{
		InitialLeaseSeconds: cluster.Spec.Lifecycle.InitialLeaseSeconds,
		RetrySeconds:        append([]int64(nil), cluster.Spec.Lifecycle.RetrySeconds...),
	}
	backend := r.Config.Backend
	if backend.Key == "" {
		backend.Key = strings.Trim(strings.TrimSpace(r.Config.BackendPrefix), "/") + "/" + string(cluster.UID) + ".tfstate"
	}
	cluster.Status.Backend = &backend
	cluster.Status.ExecutionImage = r.Config.ExecutionImage
	cluster.Status.Phase = servitorv1alpha1.PhasePending
	return nil
}

func matchesLifecycleSnapshot(policy servitorv1alpha1.LifecyclePolicy, snapshot servitorv1alpha1.LifecycleSnapshot) bool {
	if policy.InitialLeaseSeconds != snapshot.InitialLeaseSeconds || len(policy.RetrySeconds) != len(snapshot.RetrySeconds) {
		return false
	}
	for index, retry := range policy.RetrySeconds {
		if retry != snapshot.RetrySeconds[index] {
			return false
		}
	}
	return true
}

func (r *Reconciler) unresolved(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, reason string, err error) (ctrl.Result, error) {
	cluster.Status.Phase = servitorv1alpha1.PhaseUnresolved
	cluster.Status.Diagnostic = reason
	setCondition(cluster, "Ready", metav1.ConditionFalse, reason, err.Error())
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}
func (r *Reconciler) logRetry() time.Duration {
	if r.LogRetry > 0 {
		return r.LogRetry
	}
	return 10 * time.Second
}
func (r *Reconciler) reviewTimeout() time.Duration {
	if r.Config.ReviewTimeout > 0 {
		return r.Config.ReviewTimeout
	}
	return 5 * time.Minute
}
func planningID(uid string) string { return operationID(uid, "plan") }
func applyID(uid string) string    { return operationID(uid, "apply") }
func destroyID(uid string, retry int) string {
	return operationID(uid, fmt.Sprintf("destroy-%d", retry))
}
func operationID(uid, kind string) string {
	digest := sha256.Sum256([]byte(uid + "\x00" + kind))
	return kind + "-" + hex.EncodeToString(digest[:])[:16]
}
func contains(items []string, item string) bool {
	for _, value := range items {
		if value == item {
			return true
		}
	}
	return false
}
func setCondition(cluster *servitorv1alpha1.ServitorCluster, kind string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: kind, Status: status, Reason: reason, Message: message, ObservedGeneration: cluster.Generation, LastTransitionTime: metav1.NewTime(time.Now().UTC())})
}

func overlay(dst *servitorv1alpha1.UserOptions, supplied servitorv1alpha1.UserOptions) {
	if supplied.Target != "" {
		dst.Target = supplied.Target
	}
	if supplied.Provider != "" {
		dst.Provider = supplied.Provider
	}
	if supplied.Platform != "" {
		dst.Platform = supplied.Platform
	}
	if supplied.Version != "" {
		dst.Version = supplied.Version
	}
	if supplied.ResourceGroup != "" {
		dst.ResourceGroup = supplied.ResourceGroup
	}
	if supplied.Zone != "" {
		dst.Zone = supplied.Zone
	}
	if supplied.Flavor != "" {
		dst.Flavor = supplied.Flavor
	}
	if supplied.VPCID != "" {
		dst.VPCID = supplied.VPCID
	}
	if supplied.Datacenter != "" {
		dst.Datacenter = supplied.Datacenter
	}
	if supplied.MachineType != "" {
		dst.MachineType = supplied.MachineType
	}
	if supplied.PublicVLANID != "" {
		dst.PublicVLANID = supplied.PublicVLANID
	}
	if supplied.PrivateVLANID != "" {
		dst.PrivateVLANID = supplied.PrivateVLANID
	}
	if supplied.SatelliteManagedFrom != "" {
		dst.SatelliteManagedFrom = supplied.SatelliteManagedFrom
	}
	if supplied.SatelliteLocationID != "" {
		dst.SatelliteLocationID = supplied.SatelliteLocationID
	}
	if supplied.SatelliteHostImage != "" {
		dst.SatelliteHostImage = supplied.SatelliteHostImage
	}
	if supplied.SatelliteHostProfile != "" {
		dst.SatelliteHostProfile = supplied.SatelliteHostProfile
	}
	if supplied.SatelliteSSHKeyID != "" {
		dst.SatelliteSSHKeyID = supplied.SatelliteSSHKeyID
	}
	if supplied.SatelliteWorkerOperatingSystem != "" {
		dst.SatelliteWorkerOperatingSystem = supplied.SatelliteWorkerOperatingSystem
	}
	if supplied.Name != "" {
		dst.Name = supplied.Name
	}
	if supplied.WorkerCount != 0 {
		dst.WorkerCount = supplied.WorkerCount
	}
	if supplied.SubnetIDs != nil {
		dst.SubnetIDs = append([]string(nil), supplied.SubnetIDs...)
	}
	if supplied.PublicGatewayIDs != nil {
		dst.PublicGatewayIDs = append([]string(nil), supplied.PublicGatewayIDs...)
	}
	if supplied.SatelliteZones != nil {
		dst.SatelliteZones = append([]string(nil), supplied.SatelliteZones...)
	}
	if supplied.SatelliteWorkerInstanceIDs != nil {
		dst.SatelliteWorkerInstanceIDs = append([]string(nil), supplied.SatelliteWorkerInstanceIDs...)
	}
}

type podLogs struct{ client kubernetes.Interface }

func (p podLogs) ReadContainerLog(ctx context.Context, namespace, pod, container string) (io.ReadCloser, error) {
	return p.client.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{Container: container}).Stream(ctx)
}

// NewPodLogReader adapts the Kubernetes API to the bounded report reader.
func NewPodLogReader(client kubernetes.Interface) pipeline.LogReader { return podLogs{client: client} }

func (r *Reconciler) SetupWithManager(manager ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(manager).For(&servitorv1alpha1.ServitorCluster{}).Watches(&tektonv1.PipelineRun{}, handler.EnqueueRequestsFromMapFunc(r.mapPipelineRun)).Complete(r)
}
func (r *Reconciler) mapPipelineRun(ctx context.Context, object client.Object) []ctrl.Request {
	run, ok := object.(*tektonv1.PipelineRun)
	if !ok || run.Labels[pipeline.ClusterUIDLabel] == "" || (r.Config.Namespace != "" && run.Namespace != r.Config.Namespace) {
		return nil
	}
	var clusters servitorv1alpha1.ServitorClusterList
	if err := r.List(ctx, &clusters, client.InNamespace(run.Namespace)); err != nil {
		return nil
	}
	requests := make([]ctrl.Request, 0, 1)
	for _, cluster := range clusters.Items {
		if string(cluster.UID) == run.Labels[pipeline.ClusterUIDLabel] {
			requests = append(requests, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}})
		}
	}
	return requests
}

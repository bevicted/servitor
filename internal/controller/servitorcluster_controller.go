// Package controller reconciles ServitorCluster planning operations.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
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

// Config values are loaded once at manager startup. Existing status snapshots always win.
type Config struct {
	Namespace      string
	Defaults       servitorv1alpha1.ResolvedOptions
	Backend        servitorv1alpha1.BackendIdentity
	BackendPrefix  string
	ExecutionImage string
	ReviewTimeout  time.Duration
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

// +kubebuilder:rbac:groups=servitor.bevicted.github.io,resources=servitorclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=servitor.bevicted.github.io,resources=servitorclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=servitor.bevicted.github.io,resources=servitorclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns;taskruns,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
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
	if cluster.DeletionTimestamp.IsZero() {
		if !contains(cluster.Finalizers, servitorv1alpha1.CleanupFinalizer) {
			cluster.Finalizers = append(cluster.Finalizers, servitorv1alpha1.CleanupFinalizer)
			return ctrl.Result{}, r.Update(ctx, cluster)
		}
	} else if !cluster.Status.CleanupRequested {
		cluster.Status.CleanupRequested = true
		if cluster.Status.Phase != servitorv1alpha1.PhaseApplying {
			cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
		}
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}

	if cluster.Status.ResolvedOptions == nil {
		r.snapshot(cluster)
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	if cluster.Status.Phase == servitorv1alpha1.PhaseAwaitingApproval {
		return r.reconcileApproval(ctx, cluster)
	}
	if cluster.Status.Phase == servitorv1alpha1.PhaseReady || cluster.Status.Phase == servitorv1alpha1.PhaseCleanupPending || cluster.Status.Phase == servitorv1alpha1.PhaseUnresolved {
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

func (r *Reconciler) reconcileApproval(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	deadline := cluster.Status.ReviewDeadline
	if deadline == nil || !r.now().Before(deadline.Time) {
		cluster.Status.CleanupRequested = true
		cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
		setCondition(cluster, "Ready", metav1.ConditionFalse, "ApprovalExpired", "approval was not received before the review deadline")
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	switch cluster.Spec.Lifecycle.Approval {
	case "rejected":
		cluster.Status.CleanupRequested = true
		cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
		setCondition(cluster, "Ready", metav1.ConditionFalse, "ApprovalRejected", "configuration approval was rejected")
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
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
		cluster.Status.Phase = servitorv1alpha1.PhaseApplying
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
	return ctrl.Result{RequeueAfter: deadline.Time.Sub(r.now())}, nil
}

func (r *Reconciler) observeOperation(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	operation := cluster.Status.Operation
	run := &tektonv1.PipelineRun{}
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: operation.PipelineRunName}, run)
	if apierrors.IsNotFound(err) {
		var created *tektonv1.PipelineRun
		var buildErr error
		switch operation.Kind {
		case "plan":
			created, buildErr = pipeline.NewPlanningRun(cluster)
		case "apply":
			created, buildErr = pipeline.NewApplyRun(cluster)
		default:
			buildErr = errors.New("unsupported operation kind")
		}
		if buildErr != nil {
			return r.unresolved(ctx, cluster, "InvalidOperation", buildErr)
		}
		if createErr := r.Create(ctx, created); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return ctrl.Result{}, createErr
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
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
			cluster.Status.CleanupRequested = true
			cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
			cluster.Status.Diagnostic = "ApplyFailed"
			setCondition(cluster, "Ready", metav1.ConditionFalse, "ApplyFailed", "apply PipelineRun failed; cleanup is required")
			return ctrl.Result{}, r.Status().Update(ctx, cluster)
		}
		return r.unresolved(ctx, cluster, "PlanningFailed", errors.New("planning PipelineRun failed"))
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
		deadline := r.now().Add(r.reviewTimeout())
		if cluster.Spec.Lifecycle.ConfirmationDeadline != nil {
			deadline = cluster.Spec.Lifecycle.ConfirmationDeadline.Time
		}
		cluster.Status.ReviewDeadline = &metav1.Time{Time: deadline}
		cluster.Status.ReviewGeneration = cluster.Generation
		cluster.Status.ReviewApproval = cluster.Spec.Lifecycle.Approval
		setCondition(cluster, "PlanningSucceeded", metav1.ConditionTrue, "ReportAdopted", "validated planning report adopted")
	} else {
		cluster.Status.Ready = &report.Ready
		if cluster.Status.CleanupRequested {
			cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
		} else {
			cluster.Status.Phase = servitorv1alpha1.PhaseReady
			setCondition(cluster, "Ready", metav1.ConditionTrue, "ReportAdopted", "validated apply report adopted")
		}
	}
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

func (r *Reconciler) snapshot(cluster *servitorv1alpha1.ServitorCluster) {
	resolved := r.Config.Defaults
	overlay(&resolved.UserOptions, cluster.Spec.UserOptions)
	cluster.Status.ResolvedOptions = &resolved
	backend := r.Config.Backend
	if backend.Key == "" {
		backend.Key = strings.Trim(strings.TrimSpace(r.Config.BackendPrefix), "/") + "/" + string(cluster.UID) + ".tfstate"
	}
	cluster.Status.Backend = &backend
	cluster.Status.ExecutionImage = r.Config.ExecutionImage
	cluster.Status.Phase = servitorv1alpha1.PhasePending
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
func planningID(uid string) string {
	digest := sha256.Sum256([]byte(uid))
	return "plan-" + hex.EncodeToString(digest[:])[:16]
}
func applyID(uid string) string {
	digest := sha256.Sum256([]byte(uid + "\\x00apply"))
	return "apply-" + hex.EncodeToString(digest[:])[:16]
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

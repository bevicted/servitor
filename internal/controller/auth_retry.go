package controller

import (
	"context"
	"errors"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	authRetryPending     = "Pending"
	authRetryAvailable   = "Available"
	authRetryUnavailable = "Unavailable"
	authRetryCancelled   = "Cancelled"
)

// reconcileAuthRetry consumes exactly one newer owner intent. It deliberately
// starts no work for a terminal request; another attempt needs another command.
func (r *Reconciler) reconcileAuthRetry(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, bool, error) {
	request := cluster.Spec.Lifecycle.AuthRetryRequestTimestamp
	if request == "" || (cluster.Status.AuthRetry != nil && cluster.Status.AuthRetry.RequestTimestamp == request) {
		return ctrl.Result{}, false, nil
	}
	if !authRetryEligible(cluster) {
		return ctrl.Result{}, false, nil
	}
	if !servitorv1alpha1.ValidExecutionImage(r.Config.ExecutionImage) {
		result, err := r.unresolved(ctx, cluster, "InvalidAuthRetryExecutionImage", errors.New("auth retry execution image is not an immutable digest"))
		return result, true, err
	}
	operation := operationID(string(cluster.UID), "auth-retry-"+request)
	attempt := initialAuthAttemptID(string(cluster.UID), operation, request)
	cluster.Status.AuthRetry = &servitorv1alpha1.AuthRetryStatus{RequestTimestamp: request, AttemptID: attempt, ExecutionImage: r.Config.ExecutionImage, Outcome: authRetryPending}
	cluster.Status.Operation = &servitorv1alpha1.OperationReference{ID: operation, Kind: "auth-retry", AuthAttemptID: attempt, PipelineRunName: pipeline.DeterministicRunName(string(cluster.UID), operation), StartedAt: metav1.NewTime(r.now())}
	return ctrl.Result{RequeueAfter: cleanupProgressRequeue}, true, r.Status().Update(ctx, cluster)
}

func authRetryEligible(cluster *servitorv1alpha1.ServitorCluster) bool {
	return cluster.Status.Phase == servitorv1alpha1.PhaseReady && cluster.Status.Ready != nil && cluster.Status.ResolvedOptions != nil && cluster.Status.ResolvedOptions.Provider == "vpc-gen2" && cluster.Status.ResolvedOptions.PrivateOnly && cluster.Status.ResolvedOptions.Network.AuthPolicy != nil && cluster.Status.Auth != nil && cluster.Status.Auth.Availability == "unavailable" && len(cluster.Status.AuthCertificateReferences) < servitorv1alpha1.MaxAuthCertificateReferences && cluster.Status.Operation == nil && cluster.Status.Cleanup == nil && !cluster.Status.CleanupRequested && !cluster.Spec.Lifecycle.CleanupRequested
}

func clearedAuthCertificateReferencesAreTracked(cluster *servitorv1alpha1.ServitorCluster, references []servitorv1alpha1.AuthCertificateReference) bool {
	if len(references) == 0 {
		return true
	}
	tracked := make(map[servitorv1alpha1.AuthCertificateReference]struct{}, len(cluster.Status.AuthCertificateReferences))
	for _, reference := range cluster.Status.AuthCertificateReferences {
		tracked[reference] = struct{}{}
	}
	for _, reference := range references {
		if _, found := tracked[reference]; !found {
			return false
		}
	}
	return true
}

func (r *Reconciler) finishAuthRetry(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, status servitorv1alpha1.AuthStatus) (ctrl.Result, error) {
	if cluster.Status.Operation == nil || cluster.Status.Operation.Kind != "auth-retry" || cluster.Status.AuthRetry == nil || cluster.Status.AuthRetry.AttemptID != operationAuthAttempt(cluster.Status.Operation) || cluster.Status.AuthRetry.RequestTimestamp != cluster.Spec.Lifecycle.AuthRetryRequestTimestamp || cluster.Status.Cleanup != nil || cluster.Status.CleanupRequested || cluster.Spec.Lifecycle.CleanupRequested {
		if cluster.Status.Operation != nil && cluster.Status.Operation.Kind == "auth-retry" {
			if err := r.revokeAuthAttempt(ctx, cluster, operationAuthAttempt(cluster.Status.Operation)); err != nil {
				return ctrl.Result{}, err
			}
		}
		return r.unresolved(ctx, cluster, "AuthRetryFenced", errors.New("auth retry result was fenced"))
	}
	attempt := operationAuthAttempt(cluster.Status.Operation)
	if status.AttemptID == "" { // pre-auth-attempt reports used the operation as ownership identity.
		status.AttemptID = attempt
	}
	if status.AttemptID != attempt {
		return r.unresolved(ctx, cluster, "InvalidAuthRetryReport", errors.New("auth retry attempt does not match the active attempt"))
	}
	if status.Availability == "unavailable" && status.Certificate == nil {
		certificate, err := r.recoverPendingAuthCertificateReference(ctx, cluster, attempt)
		if err != nil {
			return ctrl.Result{}, err
		}
		if certificate != nil {
			status.Reason = "certificate-cleanup-pending"
			status.CleanupOutcome = "pending"
			status.CleanupReason = "unknown"
			status.CleanupStage = ""
			status.Certificate = certificate
		}
	}
	if !clearedAuthCertificateReferencesAreTracked(cluster, status.ClearedCertificateReferences) {
		return r.unresolved(ctx, cluster, "InvalidAuthRetryReport", errors.New("auth retry cleared unknown certificate references"))
	}
	if status.Availability == "unavailable" {
		if err := r.revokeAuthPublication(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
	} else if err := r.revokeAuthAttempt(ctx, cluster, attempt); err != nil {
		return ctrl.Result{}, err
	}
	status.AttemptID = attempt
	clearAuthCertificateReferences(cluster, status.ClearedCertificateReferences)
	status.ClearedCertificateReferences = nil
	trackAuthCertificateReference(cluster, status.Certificate)
	cluster.Status.Auth = &status
	cluster.Status.AuthRetry.Outcome = authRetryUnavailable
	if status.Availability == "available" {
		cluster.Status.AuthRetry.Outcome = authRetryAvailable
	}
	cluster.Status.Operation = nil
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

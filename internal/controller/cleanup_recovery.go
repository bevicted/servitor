package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const cleanupRecoveryReason = servitorv1alpha1.CleanupRecoveryReasonTaskExecutionContractMismatch

type cleanupRecoveryRequest struct {
	UID               string `json:"uid"`
	Generation        int64  `json:"generation"`
	StatusFingerprint string `json:"statusFingerprint"`
	ExecutionImage    string `json:"executionImage"`
	Reason            string `json:"reason"`
}

type cleanupRecoveryStatusFingerprintInput struct {
	Phase                     string                                      `json:"phase"`
	CleanupReason             servitorv1alpha1.CleanupReason              `json:"cleanupReason"`
	CleanupRetryCount         int                                         `json:"cleanupRetryCount"`
	ExecutionImage            string                                      `json:"executionImage"`
	ResolvedOptionsSHA256     string                                      `json:"resolvedOptionsSHA256"`
	RecoverySHA256            string                                      `json:"recoverySHA256"`
	BackendSHA256             string                                      `json:"backendSHA256"`
	CleanupRecoverySHA256     string                                      `json:"cleanupRecoverySHA256"`
	AuthCertificateReferences []servitorv1alpha1.AuthCertificateReference `json:"authCertificateReferences"`
}

func (r *Reconciler) startCleanupRecovery(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, error) {
	request, ok := cleanupRecoveryRequestFor(cluster, r.Config.ExecutionImage)
	if !ok || !cleanupRecoveryEligible(cluster, r.Config.ExecutionImage) {
		return ctrl.Result{}, nil
	}
	operation := operationID(string(cluster.UID), "cleanup-recovery-"+cluster.ResourceVersion)
	now := metav1.NewTime(r.now())
	attempt := 1
	var history []servitorv1alpha1.CleanupRecoveryAttempt
	if previous := cluster.Status.CleanupRecovery; previous != nil {
		attempt = cleanupRecoveryAttempt(previous) + 1
		history = []servitorv1alpha1.CleanupRecoveryAttempt{{
			ExecutionImage: previous.ExecutionImage,
			OperationID:    previous.OperationID,
			Outcome:        previous.State,
			Timestamp:      *previous.CompletedAt,
		}}
	}
	cluster.Status.CleanupRecovery = &servitorv1alpha1.CleanupRecoveryStatus{
		Attempt:                attempt,
		ExecutionImage:         request.ExecutionImage,
		OperationID:            operation,
		RequestResourceVersion: cluster.ResourceVersion,
		StartedAt:              now,
		State:                  servitorv1alpha1.CleanupRecoveryPending,
		History:                history,
	}
	cluster.Status.Operation = &servitorv1alpha1.OperationReference{
		ID:              operation,
		Kind:            "destroy",
		PipelineRunName: pipeline.DeterministicRunName(string(cluster.UID), operation),
		StartedAt:       now,
	}
	cluster.Status.Phase = servitorv1alpha1.PhaseCleanupPending
	cluster.Status.Diagnostic = ""
	setCondition(cluster, "Ready", metav1.ConditionFalse, "CleanupRecovery", "administrator-approved cleanup recovery is running")
	return ctrl.Result{RequeueAfter: cleanupProgressRequeue}, r.Status().Update(ctx, cluster)
}

func cleanupRecoveryRequestFor(cluster *servitorv1alpha1.ServitorCluster, executionImage string) (cleanupRecoveryRequest, bool) {
	value := cluster.Annotations[servitorv1alpha1.CleanupRecoveryRequestAnnotation]
	var request cleanupRecoveryRequest
	if value == "" || json.Unmarshal([]byte(value), &request) != nil {
		return cleanupRecoveryRequest{}, false
	}
	fingerprint, ok := cleanupRecoveryStatusFingerprint(cluster)
	return request, ok &&
		request.UID == string(cluster.UID) &&
		request.Generation == cluster.Generation &&
		request.StatusFingerprint == fingerprint &&
		request.ExecutionImage == executionImage &&
		request.Reason == cleanupRecoveryReason
}

func cleanupRecoveryStatusFingerprint(cluster *servitorv1alpha1.ServitorCluster) (string, bool) {
	if cluster.Status.Cleanup == nil || cluster.Status.ResolvedOptions == nil || cluster.Status.Recovery == nil || cluster.Status.Backend == nil {
		return "", false
	}
	resolvedOptions, ok := cleanupRecoveryIdentityHash(cluster.Status.ResolvedOptions)
	if !ok {
		return "", false
	}
	recovery, ok := cleanupRecoveryIdentityHash(cluster.Status.Recovery)
	if !ok {
		return "", false
	}
	backend, ok := cleanupRecoveryIdentityHash(cluster.Status.Backend)
	if !ok {
		return "", false
	}
	cleanupRecovery := ""
	if cluster.Status.CleanupRecovery != nil {
		cleanupRecovery, ok = cleanupRecoveryIdentityHash(cluster.Status.CleanupRecovery)
		if !ok {
			return "", false
		}
	}
	references := append([]servitorv1alpha1.AuthCertificateReference(nil), cluster.Status.AuthCertificateReferences...)
	sort.Slice(references, func(left, right int) bool {
		if references[left].ID != references[right].ID {
			return references[left].ID < references[right].ID
		}
		if references[left].AllocationUID != references[right].AllocationUID {
			return references[left].AllocationUID < references[right].AllocationUID
		}
		return references[left].AttemptID < references[right].AttemptID
	})
	input := cleanupRecoveryStatusFingerprintInput{
		Phase:                     cluster.Status.Phase,
		CleanupReason:             cluster.Status.Cleanup.Reason,
		CleanupRetryCount:         cluster.Status.Cleanup.RetryCount,
		ExecutionImage:            cluster.Status.ExecutionImage,
		ResolvedOptionsSHA256:     resolvedOptions,
		RecoverySHA256:            recovery,
		BackendSHA256:             backend,
		CleanupRecoverySHA256:     cleanupRecovery,
		AuthCertificateReferences: references,
	}
	return cleanupRecoveryIdentityHash(input)
}

func cleanupRecoveryIdentityHash(value any) (string, bool) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), true
}

func cleanupRecoveryEligible(cluster *servitorv1alpha1.ServitorCluster, executionImage string) bool {
	cleanup := cluster.Status.Cleanup
	if cluster.Status.Phase != servitorv1alpha1.PhaseUnresolved ||
		!cluster.Status.CleanupRequested ||
		cleanup == nil || !cleanup.RequiresDestroy || cleanup.NextRetryAt != nil ||
		cluster.Status.LifecycleSnapshot == nil || cleanup.RetryCount < len(cluster.Status.LifecycleSnapshot.RetrySeconds) ||
		cluster.Status.Operation != nil ||
		cluster.Status.ResolvedOptions == nil || cluster.Status.Backend == nil || cluster.Status.Recovery == nil ||
		!servitorv1alpha1.ValidExecutionImage(cluster.Status.ExecutionImage) ||
		!servitorv1alpha1.ValidExecutionImage(executionImage) || executionImage == cluster.Status.ExecutionImage {
		return false
	}
	previous := cluster.Status.CleanupRecovery
	if previous == nil {
		return true
	}
	return cleanupRecoveryAttempt(previous) == 1 &&
		servitorv1alpha1.ValidExecutionImage(previous.ExecutionImage) &&
		previous.State == servitorv1alpha1.CleanupRecoveryFailed &&
		previous.CompletedAt != nil &&
		len(previous.History) == 0 &&
		executionImage != previous.ExecutionImage
}

func cleanupRecoveryAttempt(recovery *servitorv1alpha1.CleanupRecoveryStatus) int {
	if recovery.Attempt == 0 {
		return 1
	}
	return recovery.Attempt
}

func cleanupRecoveryOperation(cluster *servitorv1alpha1.ServitorCluster) bool {
	return cluster.Status.CleanupRecovery != nil &&
		cluster.Status.CleanupRecovery.State == servitorv1alpha1.CleanupRecoveryPending &&
		cluster.Status.Operation != nil &&
		cluster.Status.Operation.Kind == "destroy" &&
		cluster.Status.Operation.ID == cluster.Status.CleanupRecovery.OperationID
}

func (r *Reconciler) failCleanupRecovery(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, diagnostic string) (ctrl.Result, error) {
	if !cleanupRecoveryOperation(cluster) {
		return r.unresolved(ctx, cluster, diagnostic, errors.New("cleanup recovery failed"))
	}
	now := metav1.NewTime(r.now())
	cluster.Status.CleanupRecovery.State = servitorv1alpha1.CleanupRecoveryFailed
	cluster.Status.CleanupRecovery.CompletedAt = &now
	cluster.Status.Operation = nil
	cluster.Status.Phase = servitorv1alpha1.PhaseUnresolved
	cluster.Status.Diagnostic = diagnostic
	setCondition(cluster, "Ready", metav1.ConditionFalse, diagnostic, "cleanup recovery failed; no further destroy will be started")
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

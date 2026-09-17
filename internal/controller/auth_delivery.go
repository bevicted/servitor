package controller

import (
	"context"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	authDeliveryTimeout     = time.Minute
	maxAuthBundleBytes      = 2 << 20
	maxAuthFileBytes        = 1 << 20
	authDeliveryDelivered   = "Delivered"
	authDeliveryFailed      = "Failed"
	authDeliveryUnavailable = "Unavailable"
	authDeliveryPending     = "Pending"
	authDeliveryCancelled   = "Cancelled"
)

// AuthFileDelivery sends one complete stored bundle only to the persisted allocation owner.
// Implementations must not log or retain credential-bearing bytes.
type AuthFileDelivery interface {
	DeliverAuthBundle(context.Context, string, string, string, string, []byte, []byte) error
}

// reconcileAuthDelivery consumes an explicit request before any Secret or Slack
// side effect. A crash after the status write intentionally suppresses delivery
// until the owner sends a newer request; a later reconciliation records the
// uncertain attempt as a sanitized failure without replaying it.
func (r *Reconciler) reconcileAuthDelivery(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster) (ctrl.Result, bool, error) {
	request := cluster.Spec.Lifecycle.AuthRequestTimestamp
	if request == "" {
		return ctrl.Result{}, false, nil
	}
	if delivery := cluster.Status.AuthDelivery; delivery != nil && delivery.RequestTimestamp == request {
		if delivery.Outcome == authDeliveryPending {
			return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryFailed)
		}
		return ctrl.Result{}, false, nil
	}
	if !authDeliveryEligible(cluster) {
		return ctrl.Result{}, false, nil
	}

	cluster.Status.AuthDelivery = &servitorv1alpha1.AuthDeliveryStatus{
		RequestTimestamp: request,
		AttemptTimestamp: request,
		Outcome:          authDeliveryPending,
	}
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, true, err
	}

	current := &servitorv1alpha1.ServitorCluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.directReader().Get(ctx, key, current); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, true, nil
		}
		return ctrl.Result{}, true, err
	}
	if current.UID != cluster.UID || current.Status.AuthDelivery == nil || current.Status.AuthDelivery.RequestTimestamp != request || current.Status.AuthDelivery.Outcome != authDeliveryPending {
		return ctrl.Result{}, true, nil
	}
	if !current.DeletionTimestamp.IsZero() || current.Spec.Lifecycle.CleanupRequested || current.Status.CleanupRequested || current.Status.Cleanup != nil || current.Status.LeaseExpiresAt == nil || !r.now().Before(current.Status.LeaseExpiresAt.Time) {
		return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, current, authDeliveryCancelled)
	}
	cluster = current

	status := deliveryStatus(cluster)
	if status == nil || !validDeliveryStatus(*status, r.now()) {
		return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryUnavailable)
	}
	secret := &corev1.Secret{}
	key = types.NamespacedName{Namespace: cluster.Namespace, Name: authResourceName(cluster)}
	if err := r.directReader().Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryUnavailable)
		}
		return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryFailed)
	}
	if err := validateAuthSecret(secret, cluster, applyID(string(cluster.UID))); err != nil {
		return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryUnavailable)
	}
	kubeconfig, vpn, valid := deliveryBundle(secret.Data, *status)
	if !valid || r.AuthDelivery == nil {
		return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryUnavailable)
	}
	deliveryCtx, cancel := context.WithTimeout(ctx, authDeliveryTimeout)
	err := r.AuthDelivery.DeliverAuthBundle(deliveryCtx, cluster.Spec.Slack.OwnerID, clusterName(cluster), status.Mode, status.Expiry, kubeconfig, vpn)
	cancel()
	if err != nil {
		return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryFailed)
	}
	return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryDelivered)
}

func (r *Reconciler) setAuthDeliveryOutcome(ctx context.Context, cluster *servitorv1alpha1.ServitorCluster, outcome string) error {
	cluster.Status.AuthDelivery.Outcome = outcome
	return r.Status().Update(ctx, cluster)
}

func authDeliveryEligible(cluster *servitorv1alpha1.ServitorCluster) bool {
	if cluster.Spec.UserOptions.Provider == "satellite" {
		return false
	}
	if resolved := cluster.Status.ResolvedOptions; resolved != nil && resolved.Provider == "satellite" {
		return false
	}
	return authEligible(cluster)
}

func authStatus(cluster *servitorv1alpha1.ServitorCluster) *servitorv1alpha1.AuthStatus {
	if cluster.Status.Auth != nil {
		return cluster.Status.Auth
	}
	return cluster.Status.PublicAuth
}

func deliveryStatus(cluster *servitorv1alpha1.ServitorCluster) *servitorv1alpha1.AuthStatus {
	status := authStatus(cluster)
	if status == nil {
		return nil
	}
	if status.Mode == "" && cluster.Status.Auth == nil && cluster.Status.PublicAuth != nil {
		legacy := *status
		legacy.Mode = "public"
		return &legacy
	}
	return status
}

func validDeliveryStatus(status servitorv1alpha1.AuthStatus, now time.Time) bool {
	if status.Availability != "available" {
		return false
	}
	switch status.Mode {
	case "public":
		return status.Expiry == ""
	case "vpn":
		expiry, err := time.Parse(time.RFC3339, status.Expiry)
		return err == nil && now.Before(expiry)
	default:
		return false
	}
}

func deliveryBundle(data map[string][]byte, status servitorv1alpha1.AuthStatus) ([]byte, []byte, bool) {
	expected := 1
	if status.Mode == "vpn" {
		expected++
	}
	if len(data) != expected {
		return nil, nil, false
	}
	kubeconfig := data[authSecretDataName]
	vpn := data[authSecretVPNDataName]
	if len(kubeconfig) == 0 || len(kubeconfig) > maxAuthFileBytes || len(vpn) > maxAuthFileBytes || len(kubeconfig)+len(vpn) > maxAuthBundleBytes {
		return nil, nil, false
	}
	if status.Mode == "public" && len(vpn) != 0 {
		return nil, nil, false
	}
	if status.Mode == "vpn" && len(vpn) == 0 {
		return nil, nil, false
	}
	return append([]byte(nil), kubeconfig...), append([]byte(nil), vpn...), true
}

func clusterName(cluster *servitorv1alpha1.ServitorCluster) string {
	if cluster.Status.ResolvedOptions != nil && cluster.Status.ResolvedOptions.ClusterName != "" {
		return cluster.Status.ResolvedOptions.ClusterName
	}
	return "requested cluster"
}

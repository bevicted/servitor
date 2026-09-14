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
	maxAuthKubeconfigBytes  = 1 << 20
	authDeliveryDelivered   = "Delivered"
	authDeliveryFailed      = "Failed"
	authDeliveryUnavailable = "Unavailable"
	authDeliveryPending     = "Pending"
	authDeliveryCancelled   = "Cancelled"
)

// AuthFileDelivery sends a stored kubeconfig only to the persisted allocation owner.
// Implementations must not log or retain kubeconfig bytes.
type AuthFileDelivery interface {
	DeliverKubeconfig(context.Context, string, string, []byte) error
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
	if cluster.Status.LifecycleSnapshot == nil || !cluster.Status.LifecycleSnapshot.PublicAuthEligible {
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
	if err := r.Get(ctx, key, current); err != nil {
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

	if cluster.Status.PublicAuth == nil || cluster.Status.PublicAuth.Availability != "available" {
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
	kubeconfig := secret.Data[authSecretDataName]
	if len(kubeconfig) == 0 || len(kubeconfig) > maxAuthKubeconfigBytes || r.AuthDelivery == nil {
		return ctrl.Result{}, true, r.setAuthDeliveryOutcome(ctx, cluster, authDeliveryUnavailable)
	}
	copyBytes := append([]byte(nil), kubeconfig...)
	deliveryCtx, cancel := context.WithTimeout(ctx, authDeliveryTimeout)
	err := r.AuthDelivery.DeliverKubeconfig(deliveryCtx, cluster.Spec.Slack.OwnerID, clusterName(cluster), copyBytes)
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

func clusterName(cluster *servitorv1alpha1.ServitorCluster) string {
	if cluster.Status.ResolvedOptions != nil && cluster.Status.ResolvedOptions.ClusterName != "" {
		return cluster.Status.ResolvedOptions.ClusterName
	}
	return "requested cluster"
}

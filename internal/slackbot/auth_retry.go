package slackbot

import (
	"context"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
)

// authRetry records a separate, monotonic owner-thread intent. Plain auth is
// deliberately delivery-only and cannot reach this path.
func (b Bot) authRetry(ctx context.Context, message Message, thread string, respond func(string)) {
	cluster, err := b.ownerCluster(ctx, message, thread)
	if err != nil || !ownsThread(cluster, message, thread, true) {
		return
	}
	if !authRetryEligible(cluster) || cluster.Status.LeaseExpiresAt == nil || !b.now().Before(cluster.Status.LeaseExpiresAt.Time) {
		respond("Authentication retry is unavailable for this allocation.")
		return
	}
	if _, _, valid := splitSlackTimestamp(message.Timestamp); !valid {
		respond(rejectedText("Unable to record the authentication retry."))
		return
	}
	stateText := ""
	updated, err := b.updateIntent(ctx, cluster.Name, func(current *servitorv1alpha1.ServitorCluster) (bool, error) {
		if !ownsThread(current, message, thread, true) || !authRetryEligible(current) || current.Status.LeaseExpiresAt == nil || !b.now().Before(current.Status.LeaseExpiresAt.Time) {
			stateText = "Authentication retry is unavailable for this allocation."
			return false, nil
		}
		if staleAuthEvent(current.Spec.Lifecycle.AuthRetryRequestTimestamp, message.Timestamp) {
			stateText = "This authentication retry was already recorded. Send a newer `auth retry` request."
			return false, nil
		}
		current.Spec.Lifecycle.AuthRetryRequestTimestamp = message.Timestamp
		return true, nil
	})
	if err != nil {
		respond(rejectedText("Unable to record the authentication retry."))
		return
	}
	if updated {
		respond("Authentication retry has been queued.")
	} else if stateText != "" {
		respond(stateText)
	}
}

func authRetryEligible(cluster *servitorv1alpha1.ServitorCluster) bool {
	return cluster.Status.Phase == servitorv1alpha1.PhaseReady && cluster.Status.Ready != nil && cluster.Status.ResolvedOptions != nil && cluster.Status.ResolvedOptions.Provider == "vpc-gen2" && cluster.Status.ResolvedOptions.PrivateOnly && cluster.Status.ResolvedOptions.Network.AuthPolicy != nil && cluster.Status.Auth != nil && cluster.Status.Auth.Availability == "unavailable" && cluster.Status.Operation == nil && cluster.Status.Cleanup == nil && !cluster.Status.CleanupRequested && !cluster.Spec.Lifecycle.CleanupRequested
}

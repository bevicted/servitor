package v1alpha1

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestServitorClusterSpecRequiresWholeHourInitialLease(t *testing.T) {
	spec := ServitorClusterSpec{
		Slack: SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"},
		Lifecycle: LifecyclePolicy{
			InitialLeaseSeconds: 3601,
			RetrySeconds:        []int64{60},
		},
	}
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "whole number of hours") {
		t.Fatalf("Validate() = %v, want whole-hour error", err)
	}
}

func TestServitorClusterDeepCopyPreservesLeaseStatus(t *testing.T) {
	expiry := metav1.Now()
	cluster := &ServitorCluster{Status: ServitorClusterStatus{
		LeaseExpiresAt: &expiry,
		LeaseExtension: &LeaseExtensionStatus{
			RequestedExpiry: expiry,
			PreviousExpiry:  &expiry,
			NewExpiry:       &expiry,
			Outcome:         ExtensionOutcomeApplied,
		},
	}}
	copy := cluster.DeepCopy()
	copy.Status.LeaseExpiresAt.Time = copy.Status.LeaseExpiresAt.Time.AddDate(0, 0, 1)
	copy.Status.LeaseExtension.NewExpiry.Time = copy.Status.LeaseExtension.NewExpiry.Time.AddDate(0, 0, 1)
	if cluster.Status.LeaseExpiresAt.Equal(copy.Status.LeaseExpiresAt) || cluster.Status.LeaseExtension.NewExpiry.Equal(copy.Status.LeaseExtension.NewExpiry) {
		t.Fatal("DeepCopy() aliases lease timestamp pointers")
	}
}

package main

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func TestAuthRetryFenceRejectsCancelledAndStaleAttempts(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	fence := authRetryFence{Namespace: "ns", Name: "cluster", UID: "uid", Operation: "auth-retry-current", AttemptID: "retry-attempt-current", RequestTimestamp: "1710000000.000001", TokenPath: "/token", CAPath: "/ca"}
	valid := func() *unstructured.Unstructured {
		cluster := &unstructured.Unstructured{Object: map[string]any{
			"spec": map[string]any{"lifecycle": map[string]any{"cleanupRequested": false, "authRetryRequestTimestamp": fence.RequestTimestamp}},
			"status": map[string]any{
				"phase":          "Ready",
				"leaseExpiresAt": now.Add(time.Hour).Format(time.RFC3339),
				"operation":      map[string]any{"id": fence.Operation, "kind": "auth-retry"},
				"authRetry":      map[string]any{"requestTimestamp": fence.RequestTimestamp, "attemptID": fence.AttemptID, "outcome": "Pending"},
			},
		}}
		cluster.SetUID(types.UID(fence.UID))
		return cluster
	}
	if !authRetryFenceMatches(valid(), fence, now) {
		t.Fatal("current Ready attempt was fenced")
	}
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
	}{
		{"cleanup requested", func(t *testing.T, cluster *unstructured.Unstructured) {
			if err := unstructured.SetNestedField(cluster.Object, true, "spec", "lifecycle", "cleanupRequested"); err != nil {
				t.Fatal(err)
			}
		}},
		{"cleanup started", func(t *testing.T, cluster *unstructured.Unstructured) {
			if err := unstructured.SetNestedMap(cluster.Object, map[string]any{"reason": "Explicit"}, "status", "cleanup"); err != nil {
				t.Fatal(err)
			}
		}},
		{"expired", func(t *testing.T, cluster *unstructured.Unstructured) {
			if err := unstructured.SetNestedField(cluster.Object, now.Add(-time.Second).Format(time.RFC3339), "status", "leaseExpiresAt"); err != nil {
				t.Fatal(err)
			}
		}},
		{"stale operation", func(t *testing.T, cluster *unstructured.Unstructured) {
			if err := unstructured.SetNestedField(cluster.Object, "auth-retry-old", "status", "operation", "id"); err != nil {
				t.Fatal(err)
			}
		}},
		{"newer status request identity", func(t *testing.T, cluster *unstructured.Unstructured) {
			if err := unstructured.SetNestedField(cluster.Object, "1710000001.000001", "status", "authRetry", "requestTimestamp"); err != nil {
				t.Fatal(err)
			}
		}},
		{"newer spec request while status is pending", func(t *testing.T, cluster *unstructured.Unstructured) {
			if err := unstructured.SetNestedField(cluster.Object, "1710000001.000001", "spec", "lifecycle", "authRetryRequestTimestamp"); err != nil {
				t.Fatal(err)
			}
		}},
		{"stale attempt", func(t *testing.T, cluster *unstructured.Unstructured) {
			if err := unstructured.SetNestedField(cluster.Object, "auth-retry-old", "status", "authRetry", "attemptID"); err != nil {
				t.Fatal(err)
			}
		}},
		{"deletion timestamp", func(_ *testing.T, cluster *unstructured.Unstructured) {
			cluster.SetDeletionTimestamp(&metav1.Time{Time: now})
		}},
		{"terminal attempt", func(t *testing.T, cluster *unstructured.Unstructured) {
			if err := unstructured.SetNestedField(cluster.Object, "Cancelled", "status", "authRetry", "outcome"); err != nil {
				t.Fatal(err)
			}
		}},
		{"replaced allocation", func(_ *testing.T, cluster *unstructured.Unstructured) { cluster.SetUID("replacement") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster := valid()
			test.mutate(t, cluster)
			if authRetryFenceMatches(cluster, fence, now) {
				t.Fatal("stale or cancelled retry passed the live fence")
			}
		})
	}
}

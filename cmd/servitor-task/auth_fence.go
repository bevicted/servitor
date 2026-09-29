package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const authFenceTimeout = 15 * time.Second

var servitorClusterGVR = schema.GroupVersionResource{
	Group: "servitor.bevicted.github.io", Version: "v1alpha1", Resource: "servitorclusters",
}

type authRetryFence struct {
	Namespace        string
	Name             string
	UID              string
	Operation        string
	AttemptID        string
	RequestTimestamp string
	TokenPath        string
	CAPath           string
}

func (f authRetryFence) valid() bool {
	return f.Namespace != "" && f.Name != "" && f.UID != "" && f.Operation != "" && f.AttemptID != "" && f.RequestTimestamp != "" && f.TokenPath != "" && f.CAPath != ""
}

var checkAuthRetryFence = liveAuthRetryFence

// liveAuthRetryFence uses only the attempt ServiceAccount's resourceName-scoped
// get permission. Every API failure is a fence failure; auth issuance must fail
// closed rather than proceed from a stale cached object.
func liveAuthRetryFence(ctx context.Context, fence authRetryFence) error {
	if !fence.valid() {
		return errors.New("auth retry fence inputs are invalid")
	}
	token, err := os.ReadFile(fence.TokenPath)
	if err != nil || strings.TrimSpace(string(token)) == "" {
		return errors.New("auth retry fence token is unavailable")
	}
	client, err := dynamic.NewForConfig(&rest.Config{
		Host:        "https://kubernetes.default.svc",
		BearerToken: strings.TrimSpace(string(token)),
		TLSClientConfig: rest.TLSClientConfig{
			CAFile: fence.CAPath,
		},
	})
	if err != nil {
		return errors.New("auth retry fence client is unavailable")
	}
	fenceCtx, cancel := context.WithTimeout(ctx, authFenceTimeout)
	defer cancel()
	cluster, err := client.Resource(servitorClusterGVR).Namespace(fence.Namespace).Get(fenceCtx, fence.Name, metav1.GetOptions{})
	if err != nil {
		return errors.New("auth retry fence read failed")
	}
	if !authRetryFenceMatches(cluster, fence, time.Now()) {
		return errors.New("auth retry is fenced")
	}
	return nil
}

func authRetryFenceMatches(cluster *unstructured.Unstructured, fence authRetryFence, now time.Time) bool {
	deletion := cluster.GetDeletionTimestamp()
	if string(cluster.GetUID()) != fence.UID || deletion != nil && !deletion.IsZero() {
		return false
	}
	phase, _, _ := unstructured.NestedString(cluster.Object, "status", "phase")
	if phase != servitorv1alpha1.PhaseReady || nestedBool(cluster, "spec", "lifecycle", "cleanupRequested") || nestedBool(cluster, "status", "cleanupRequested") || nestedMapPresent(cluster, "status", "cleanup") {
		return false
	}
	lease, found, err := unstructured.NestedString(cluster.Object, "status", "leaseExpiresAt")
	if err != nil || !found {
		return false
	}
	expires, err := time.Parse(time.RFC3339, lease)
	if err != nil || !now.Before(expires) {
		return false
	}
	specRequestTimestamp, _, _ := unstructured.NestedString(cluster.Object, "spec", "lifecycle", "authRetryRequestTimestamp")
	operationID, _, _ := unstructured.NestedString(cluster.Object, "status", "operation", "id")
	operationKind, _, _ := unstructured.NestedString(cluster.Object, "status", "operation", "kind")
	requestTimestamp, _, _ := unstructured.NestedString(cluster.Object, "status", "authRetry", "requestTimestamp")
	attemptID, _, _ := unstructured.NestedString(cluster.Object, "status", "authRetry", "attemptID")
	outcome, _, _ := unstructured.NestedString(cluster.Object, "status", "authRetry", "outcome")
	return specRequestTimestamp == fence.RequestTimestamp && operationID == fence.Operation && operationKind == "auth-retry" && requestTimestamp == fence.RequestTimestamp && attemptID == fence.AttemptID && outcome == "Pending"
}

func nestedBool(cluster *unstructured.Unstructured, fields ...string) bool {
	value, found, err := unstructured.NestedBool(cluster.Object, fields...)
	return err == nil && found && value
}

func nestedMapPresent(cluster *unstructured.Unstructured, fields ...string) bool {
	value, found, err := unstructured.NestedMap(cluster.Object, fields...)
	return err == nil && found && len(value) > 0
}

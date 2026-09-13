package pipeline

import (
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestNewEligiblePublicApplyUsesScopedPublisherAndApplyBudget(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: types.UID("allocation-uid")},
		Status: servitorv1alpha1.ServitorClusterStatus{
			ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22", Target: "public-target"}, ClusterName: "frozen"},
			LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{PublicAuthEligible: true},
			Backend:           &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "key", Region: "region", Endpoint: "https://s3.example.invalid"},
			ExecutionImage:    "registry.example/servitor-task@sha256:deadbeef",
			Recovery:          &servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"},
			Operation:         &servitorv1alpha1.OperationReference{ID: "apply-a", Kind: "apply", PipelineRunName: "run"},
		},
	}
	run, err := NewApplyRun(cluster, testTaskConfig)
	if err != nil {
		t.Fatal(err)
	}
	if run.Spec.TaskRunTemplate.ServiceAccountName != AuthResourceName(string(cluster.UID)) || run.Spec.TaskRunTemplate.PodTemplate == nil || run.Spec.TaskRunTemplate.PodTemplate.AutomountServiceAccountToken == nil || *run.Spec.TaskRunTemplate.PodTemplate.AutomountServiceAccountToken {
		t.Fatalf("eligible apply identity = %#v", run.Spec.TaskRunTemplate)
	}
	if run.Spec.Timeouts == nil || run.Spec.Timeouts.Pipeline == nil || run.Spec.Timeouts.Tasks == nil || run.Spec.Timeouts.Pipeline.Duration != 120*time.Minute || run.Spec.Timeouts.Tasks.Duration != 115*time.Minute {
		t.Fatalf("apply timeouts = %#v", run.Spec.Timeouts)
	}
	params := map[string]string{}
	for _, param := range run.Spec.Params {
		params[param.Name] = param.Value.StringVal
	}
	if params["public-auth-eligible"] != "true" || params["auth-secret"] != AuthResourceName(string(cluster.UID)) {
		t.Fatalf("public auth params = %#v", params)
	}
}

func TestIneligibleApplyKeepsTheEmptyPermissionTaskAccount(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: "private-uid"}, Status: servitorv1alpha1.ServitorClusterStatus{
		ResolvedOptions:   &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "satellite", Version: "4.22"}, ClusterName: "frozen"},
		LifecycleSnapshot: &servitorv1alpha1.LifecycleSnapshot{}, Backend: &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "key", Region: "region", Endpoint: "https://s3.example.invalid"}, ExecutionImage: "image", Recovery: &servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"}, Operation: &servitorv1alpha1.OperationReference{ID: "apply-a", Kind: "apply"},
	}}
	run, err := NewApplyRun(cluster, testTaskConfig)
	if err != nil {
		t.Fatal(err)
	}
	if run.Spec.TaskRunTemplate.ServiceAccountName != "servitor-task" {
		t.Fatalf("ineligible apply identity = %q", run.Spec.TaskRunTemplate.ServiceAccountName)
	}
	for _, param := range run.Spec.Params {
		if param.Name == "auth-secret" && param.Value.StringVal != "" {
			t.Fatalf("ineligible apply received an auth Secret: %q", param.Value.StringVal)
		}
	}
}

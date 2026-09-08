package pipeline

import (
	"encoding/json"
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestNewPlanningRunSerializesICTBackendConfig(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: types.UID("cluster-uid")},
		Status: servitorv1alpha1.ServitorClusterStatus{
			ResolvedOptions: &servitorv1alpha1.ResolvedOptions{},
			Backend: &servitorv1alpha1.BackendIdentity{
				Version:                   1,
				Bucket:                    "ict-state-bucket",
				Key:                       "servitor/cluster-uid.tfstate",
				Region:                    "us-south",
				Endpoint:                  "https://s3.us-south.example.invalid",
				SkipCredentialsValidation: true,
				SkipMetadataAPICheck:      true,
				SkipRegionValidation:      true,
				SkipRequestingAccountID:   true,
				ForcePathStyle:            true,
			},
			ExecutionImage: "registry.example/ict@sha256:deadbeef",
			Operation:      &servitorv1alpha1.OperationReference{ID: "plan-a", Kind: "plan", PipelineRunName: "run"},
		},
	}
	run, err := NewPlanningRun(cluster)
	if err != nil {
		t.Fatal(err)
	}
	var serialized string
	for _, param := range run.Spec.Params {
		if param.Name == "backend" {
			serialized = param.Value.StringVal
		}
	}
	var backend map[string]any
	if err := json.Unmarshal([]byte(serialized), &backend); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"version", "bucket", "key", "region", "endpoint", "skip_credentials_validation", "skip_metadata_api_check", "skip_region_validation", "skip_requesting_account_id", "force_path_style"} {
		if _, ok := backend[field]; !ok {
			t.Fatalf("ICT backend config is missing %q: %s", field, serialized)
		}
	}
	if _, ok := backend["skipCredentialsValidation"]; ok {
		t.Fatalf("ICT backend config used status field names: %s", serialized)
	}
}

func TestNewApplyRunUsesFrozenPlanningInputs(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: types.UID("cluster-uid")},
		Status: servitorv1alpha1.ServitorClusterStatus{
			ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "frozen"},
			Backend:         &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "key", Region: "us-south", Endpoint: "https://s3.example.invalid"},
			ExecutionImage:  "registry.example/ict@sha256:frozen",
			Recovery:        &servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"},
			Operation:       &servitorv1alpha1.OperationReference{ID: "apply-a", Kind: "apply", PipelineRunName: "run"},
		},
	}
	run, err := NewApplyRun(cluster)
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]string{}
	for _, param := range run.Spec.Params {
		params[param.Name] = param.Value.StringVal
	}
	if params["operation-kind"] != "apply" || params["execution-image"] != cluster.Status.ExecutionImage || params["resolved-options"] == "" || params["recovery"] == "" || params["backend"] == "" {
		t.Fatalf("apply did not use frozen inputs: %#v", params)
	}
}

func TestNewDestroyRunUsesFrozenContextAndHasNoOwner(t *testing.T) {
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: types.UID("cluster-uid")},
		Status: servitorv1alpha1.ServitorClusterStatus{
			ResolvedOptions: &servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "frozen"},
			Backend:         &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "key", Region: "us-south", Endpoint: "https://s3.example.invalid"},
			ExecutionImage:  "registry.example/ict@sha256:frozen",
			Recovery:        &servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"},
			Operation:       &servitorv1alpha1.OperationReference{ID: "destroy-a", Kind: "destroy", PipelineRunName: "run"},
		},
	}
	run, err := NewDestroyRun(cluster)
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]string{}
	for _, param := range run.Spec.Params {
		params[param.Name] = param.Value.StringVal
	}
	if params["operation-kind"] != "destroy" || params["recovery"] == "" || len(run.OwnerReferences) != 0 {
		t.Fatalf("destroy run is not detached and frozen: %#v", run)
	}
}

func TestReportContainerReturnsActualStepContainer(t *testing.T) {
	taskRun := &tektonv1.TaskRun{Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{Steps: []tektonv1.StepState{{Name: "execute", Container: "step-execute"}, {Name: ReportContainerName, Container: "step-report"}}}}}
	if got := ReportContainer(taskRun); got != "step-report" {
		t.Fatalf("report container = %q, want step-report", got)
	}
}

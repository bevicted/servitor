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
			Operation:      &servitorv1alpha1.OperationReference{ID: "plan-a", PipelineRunName: "run"},
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

func TestReportContainerReturnsActualStepContainer(t *testing.T) {
	taskRun := &tektonv1.TaskRun{Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{Steps: []tektonv1.StepState{{Name: "execute", Container: "step-execute"}, {Name: ReportContainerName, Container: "step-report"}}}}}
	if got := ReportContainer(taskRun); got != "step-report" {
		t.Fatalf("report container = %q, want step-report", got)
	}
}

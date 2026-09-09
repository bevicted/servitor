package pipeline

import (
	"encoding/json"
	"testing"
	"time"

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
	run, err := NewPlanningRun(cluster, testTaskConfig)
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]string{}
	for _, param := range run.Spec.Params {
		params[param.Name] = param.Value.StringVal
	}
	serialized := params["backend"]
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
	if run.Spec.Timeouts == nil || run.Spec.Timeouts.Pipeline == nil || run.Spec.Timeouts.Tasks == nil || run.Spec.Timeouts.Pipeline.Duration != 100*time.Minute || run.Spec.Timeouts.Tasks.Duration != 95*time.Minute {
		t.Fatalf("PipelineRun timeouts = %#v, want 100m pipeline and 95m tasks", run.Spec.Timeouts)
	}
	if run.Spec.TaskRunTemplate.ServiceAccountName != "servitor-task" || run.Spec.TaskRunTemplate.PodTemplate != nil {
		t.Fatalf("PipelineRun task security = %#v, want servitor-task without a fixed pod security context", run.Spec.TaskRunTemplate)
	}
	for name, want := range map[string]string{"ict-config-map": testTaskConfig.ICTConfigMap, "ict-config-key": testTaskConfig.ICTConfigKey, "cos-secret": testTaskConfig.COSSecret, "ibm-secret": testTaskConfig.IBMSecret} {
		if got := params[name]; got != want {
			t.Errorf("PipelineRun param %q = %q, want %q", name, got, want)
		}
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
	run, err := NewApplyRun(cluster, testTaskConfig)
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
	run, err := NewDestroyRun(cluster, testTaskConfig)
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

var testTaskConfig = TaskConfig{
	ICTConfigMap: "configured-ict-config",
	ICTConfigKey: "configured.yaml",
	COSSecret:    "configured-cos",
	IBMSecret:    "configured-ibm",
}

func TestReportContainerReturnsActualStepContainer(t *testing.T) {
	taskRun := &tektonv1.TaskRun{Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{Steps: []tektonv1.StepState{{Name: "execute", Container: "step-execute"}, {Name: ReportContainerName, Container: "step-report"}}}}}
	if got := ReportContainer(taskRun); got != "step-report" {
		t.Fatalf("report container = %q, want step-report", got)
	}
}

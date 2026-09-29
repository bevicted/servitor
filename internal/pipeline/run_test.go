package pipeline

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
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
	if run.Spec.TaskRunTemplate.ServiceAccountName != "servitor-task" || run.Spec.TaskRunTemplate.PodTemplate == nil || run.Spec.TaskRunTemplate.PodTemplate.AutomountServiceAccountToken == nil || *run.Spec.TaskRunTemplate.PodTemplate.AutomountServiceAccountToken {
		t.Fatalf("PipelineRun task security = %#v, want servitor-task with automatic token mounting disabled", run.Spec.TaskRunTemplate)
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
	var options servitorv1alpha1.ResolvedOptions
	if err := json.Unmarshal([]byte(params["resolved-options"]), &options); err != nil || options.ClusterName != cluster.Status.ResolvedOptions.ClusterName {
		t.Fatalf("apply resolved options = %+v, err=%v", options, err)
	}
}

func TestOperationRunsKeepEachAllocationBackend(t *testing.T) {
	allocations := []*servitorv1alpha1.ServitorCluster{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: types.UID("allocation-one")}, Status: servitorv1alpha1.ServitorClusterStatus{ResolvedOptions: &servitorv1alpha1.ResolvedOptions{}, Backend: &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "servitor/allocation-one.tfstate", Region: "us-south", Endpoint: "https://s3.example.invalid"}, ExecutionImage: "registry.example/ict@sha256:one", Recovery: &servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"}}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: types.UID("allocation-two")}, Status: servitorv1alpha1.ServitorClusterStatus{ResolvedOptions: &servitorv1alpha1.ResolvedOptions{}, Backend: &servitorv1alpha1.BackendIdentity{Version: 1, Bucket: "bucket", Key: "servitor/allocation-two.tfstate", Region: "us-south", Endpoint: "https://s3.example.invalid"}, ExecutionImage: "registry.example/ict@sha256:two", Recovery: &servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"}}},
	}
	for _, allocation := range allocations {
		for _, operation := range []struct {
			kind  string
			build func(*servitorv1alpha1.ServitorCluster, TaskConfig) (*tektonv1.PipelineRun, error)
		}{
			{"plan", NewPlanningRun}, {"apply", NewApplyRun}, {"destroy", NewDestroyRun},
		} {
			allocation.Status.Operation = &servitorv1alpha1.OperationReference{ID: operation.kind + "-id", Kind: operation.kind, PipelineRunName: operation.kind + "-run"}
			run, err := operation.build(allocation, testTaskConfig)
			if err != nil {
				t.Fatal(err)
			}
			for _, parameter := range run.Spec.Params {
				if parameter.Name != "backend" {
					continue
				}
				var backend servitorv1alpha1.BackendIdentity
				if err := json.Unmarshal([]byte(parameter.Value.StringVal), &backend); err != nil || backend.Key != allocation.Status.Backend.Key {
					t.Fatalf("%s backend = %#v, err=%v", operation.kind, backend, err)
				}
			}
		}
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
	if params["operation-kind"] != "destroy" || params["execution-image"] != cluster.Status.ExecutionImage || params["recovery"] == "" || len(run.OwnerReferences) != 0 {
		t.Fatalf("destroy run is not detached and frozen: %#v", run)
	}
}

var testTaskConfig = TaskConfig{
	ICTConfigMap: "configured-ict-config",
	ICTConfigKey: "configured.yaml",
	COSSecret:    "configured-cos",
	IBMSecret:    "configured-ibm",
}

func TestDeterministicRunNameBoundsAuthRetryTimestampIDs(t *testing.T) {
	const uid = "cluster-uid"
	const operationPrefix = "auth-retry-"
	const operationSuffix = "-0123456789abcdef"
	maxTimestampLen := maxPipelineRunNameLen - len(pipelineRunNamePrefix) - 1 - pipelineRunNameHashLen - len(operationPrefix) - len(operationSuffix)
	maxOperation := operationPrefix + strings.Repeat("1", maxTimestampLen) + operationSuffix
	overlongPrefix := operationPrefix + strings.Repeat("1", maxTimestampLen+20) + operationSuffix

	for _, operation := range []string{maxOperation, overlongPrefix + "a", overlongPrefix + "b", "auth-retry-1790236593.529019-0123456789abcdef"} {
		name := DeterministicRunName(uid, operation)
		if len(name) > maxPipelineRunNameLen {
			t.Fatalf("run name length = %d, want at most %d: %q", len(name), maxPipelineRunNameLen, name)
		}
		if errors := validation.IsDNS1123Label(name); len(errors) != 0 {
			t.Fatalf("run name %q is not a DNS label: %v", name, errors)
		}
		digest := sha256.Sum256([]byte(uid + "\x00" + operation))
		if !strings.HasSuffix(name, "-"+hex.EncodeToString(digest[:])[:pipelineRunNameHashLen]) {
			t.Fatalf("run name %q does not hash complete operation %q", name, operation)
		}
	}

	first := DeterministicRunName(uid, overlongPrefix+"a")
	second := DeterministicRunName(uid, overlongPrefix+"b")
	if first == second {
		t.Fatalf("long auth retries with the same truncated prefix collided: %q", first)
	}
	if len(DeterministicRunName(uid, maxOperation)) != maxPipelineRunNameLen {
		t.Fatalf("max auth retry name length = %d, want %d", len(DeterministicRunName(uid, maxOperation)), maxPipelineRunNameLen)
	}
}

func TestDeterministicRunNameSupportsAllOperationKinds(t *testing.T) {
	for _, test := range []struct {
		operation string
		kind      string
	}{
		{"plan-0123456789abcdef", "plan"},
		{"apply-0123456789abcdef", "apply"},
		{"destroy-1-0123456789abcdef", "destroy"},
		{"auth-retry-1790236593.529019-0123456789abcdef", "auth-retry"},
	} {
		name := DeterministicRunName("cluster-uid", test.operation)
		if !strings.HasPrefix(name, pipelineRunNamePrefix+test.kind+"-") {
			t.Fatalf("run name %q lost operation kind from %q", name, test.operation)
		}
		if errors := validation.IsDNS1123Label(name); len(errors) != 0 {
			t.Fatalf("run name %q is not a DNS label: %v", name, errors)
		}
	}

	if errors := validation.IsDNS1123Label(DeterministicRunName("cluster-uid", "bad_component")); len(errors) == 0 {
		t.Fatal("invalid operation component was sanitized instead of rejected")
	}
}

func TestOperationTaskScopesAuthTmpfsToICTAuth(t *testing.T) {
	data, err := os.ReadFile("../../config/tekton/servitor-operation.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var task struct {
		Spec struct {
			Volumes []struct {
				Name     string `yaml:"name"`
				EmptyDir struct {
					Medium    string `yaml:"medium"`
					SizeLimit string `yaml:"sizeLimit"`
				} `yaml:"emptyDir"`
			} `yaml:"volumes"`
			Steps []struct {
				Name string   `yaml:"name"`
				Args []string `yaml:"args"`
				Env  []struct {
					Name  string `yaml:"name"`
					Value string `yaml:"value"`
				} `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"spec"`
	}
	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&task); err != nil {
		t.Fatal(err)
	}
	volumes := map[string]struct {
		medium, limit string
	}{}
	for _, volume := range task.Spec.Volumes {
		volumes[volume.Name] = struct{ medium, limit string }{volume.EmptyDir.Medium, volume.EmptyDir.SizeLimit}
	}
	if got := volumes["auth-tmpfs"]; got.medium != "Memory" || got.limit != "512Mi" {
		t.Fatalf("auth tmpfs = %#v, want memory-backed 512Mi", got)
	}
	if got := volumes["auth-data"]; got.medium != "Memory" || got.limit != "4Mi" {
		t.Fatalf("auth bundle tmpfs = %#v, want memory-backed 4Mi", got)
	}
	for _, step := range task.Spec.Steps {
		if step.Name != "execute" {
			continue
		}
		var genericTMPDIR string
		for _, env := range step.Env {
			switch env.Name {
			case "TMPDIR":
				genericTMPDIR = env.Value
			case "TF_DATA_DIR", "TF_PLUGIN_CACHE_DIR", "TF_WORKSPACE", "XDG_CACHE_HOME":
				t.Fatalf("generic execute step injects auth Terraform environment %q", env.Name)
			}
		}
		if genericTMPDIR != "/operation" {
			t.Fatalf("generic execute TMPDIR = %q, want /operation", genericTMPDIR)
		}
		for index, arg := range step.Args {
			if arg == "-auth-tmpfs-dir" && index+1 < len(step.Args) && step.Args[index+1] == "/auth-tmpfs" {
				return
			}
		}
		t.Fatal("execute step does not pass /auth-tmpfs to ICT auth")
	}
	t.Fatal("operation task has no execute step")
}

func TestOperationTaskPublishStepHasIBMSecretForOwnedCleanup(t *testing.T) {
	data, err := os.ReadFile("../../config/tekton/servitor-operation.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var task struct {
		Spec struct {
			Steps []struct {
				Name    string `yaml:"name"`
				EnvFrom []struct {
					SecretRef struct {
						Name string `yaml:"name"`
					} `yaml:"secretRef"`
				} `yaml:"envFrom"`
			} `yaml:"steps"`
		} `yaml:"spec"`
	}
	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&task); err != nil {
		t.Fatal(err)
	}
	for _, step := range task.Spec.Steps {
		if step.Name != "publish" {
			continue
		}
		if len(step.EnvFrom) != 1 || step.EnvFrom[0].SecretRef.Name != "$(params.ibm-secret)" {
			t.Fatalf("publish credentials = %#v, want only the existing IBM Secret", step.EnvFrom)
		}
		return
	}
	t.Fatal("operation task has no publish step")
}

func TestOperationTaskPassesDistinctFenceAndPriorCleanupContext(t *testing.T) {
	data, err := os.ReadFile("../../config/tekton/servitor-operation.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var task struct {
		Spec struct {
			Params []struct {
				Name string `yaml:"name"`
			} `yaml:"params"`
			Steps []struct {
				Name string   `yaml:"name"`
				Args []string `yaml:"args"`
			} `yaml:"steps"`
		} `yaml:"spec"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&task); err != nil {
		t.Fatal(err)
	}
	params := map[string]bool{}
	for _, param := range task.Spec.Params {
		params[param.Name] = true
	}
	for _, name := range []string{"auth-attempt-id", "auth-cleanup-outcome", "auth-cleanup-reason"} {
		if !params[name] {
			t.Fatalf("operation Task omits %q", name)
		}
	}
	for _, step := range task.Spec.Steps {
		if step.Name != "execute" && step.Name != "publish" {
			continue
		}
		argument := func(name string) string {
			for index := 0; index+1 < len(step.Args); index++ {
				if step.Args[index] == name {
					return step.Args[index+1]
				}
			}
			return ""
		}
		if argument("-auth-fence-attempt-id") != "$(params.auth-attempt-id)" {
			t.Fatalf("%s fence attempt = %q", step.Name, argument("-auth-fence-attempt-id"))
		}
		if step.Name == "execute" && (argument("-auth-cleanup-outcome") != "$(params.auth-cleanup-outcome)" || argument("-auth-cleanup-reason") != "$(params.auth-cleanup-reason)") {
			t.Fatalf("execute cleanup context = %#v", step.Args)
		}
	}
	var pipeline struct {
		Spec struct {
			Params []struct {
				Name string `yaml:"name"`
			} `yaml:"params"`
			Tasks []struct {
				Params []struct {
					Name  string `yaml:"name"`
					Value string `yaml:"value"`
				} `yaml:"params"`
			} `yaml:"tasks"`
		} `yaml:"spec"`
	}
	if err := decoder.Decode(&pipeline); err != nil {
		t.Fatal(err)
	}
	forwarded := map[string]string{}
	for _, param := range pipeline.Spec.Tasks[0].Params {
		forwarded[param.Name] = param.Value
	}
	for name, value := range map[string]string{"auth-attempt-id": "$(params.auth-attempt-id)", "auth-cleanup-outcome": "$(params.auth-cleanup-outcome)", "auth-cleanup-reason": "$(params.auth-cleanup-reason)"} {
		if forwarded[name] != value {
			t.Fatalf("Pipeline %s = %q, want %q", name, forwarded[name], value)
		}
	}
}

func TestReportContainerReturnsActualStepContainer(t *testing.T) {
	taskRun := &tektonv1.TaskRun{Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{Steps: []tektonv1.StepState{{Name: "execute", Container: "step-execute"}, {Name: ReportContainerName, Container: "step-report"}}}}}
	if got := ReportContainer(taskRun); got != "step-report" {
		t.Fatalf("report container = %q, want step-report", got)
	}
}

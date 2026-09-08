package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	OperationLabel  = "servitor.bevicted.github.io/operation"
	ClusterUIDLabel = "servitor.bevicted.github.io/cluster-uid"
	PipelineName    = "servitor-operation"
)

// DeterministicRunName is stable across controller restarts and bounded for Kubernetes names.
func DeterministicRunName(uid, operation string) string {
	digest := sha256.Sum256([]byte(uid + "\x00" + operation))
	return fmt.Sprintf("servitor-%s-%s", operation, hex.EncodeToString(digest[:])[:12])
}

// NewPlanningRun constructs one disposable planning PipelineRun. It has no owner reference,
// so foreground CR deletion cannot cancel an active operation.
func NewPlanningRun(cluster *servitorv1alpha1.ServitorCluster) (*tektonv1.PipelineRun, error) {
	return newOperationRun(cluster, "plan")
}

// NewApplyRun constructs a fresh, noninteractive apply from frozen planning status.
func NewApplyRun(cluster *servitorv1alpha1.ServitorCluster) (*tektonv1.PipelineRun, error) {
	return newOperationRun(cluster, "apply")
}

func newOperationRun(cluster *servitorv1alpha1.ServitorCluster, kind string) (*tektonv1.PipelineRun, error) {
	if cluster.Status.Operation == nil || cluster.Status.Operation.Kind != kind || cluster.Status.ResolvedOptions == nil || cluster.Status.Backend == nil || cluster.Status.ExecutionImage == "" {
		return nil, fmt.Errorf("%s operation was not persisted", kind)
	}
	options, err := json.Marshal(cluster.Status.ResolvedOptions)
	if err != nil {
		return nil, fmt.Errorf("encode resolved options: %w", err)
	}
	backend, err := encodeBackendConfig(*cluster.Status.Backend)
	if err != nil {
		return nil, fmt.Errorf("encode backend identity: %w", err)
	}
	operation := cluster.Status.Operation
	params := tektonv1.Params{
		{Name: "operation-id", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: operation.ID}},
		{Name: "operation-kind", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: kind}},
		{Name: "cluster-uid", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: string(cluster.UID)}},
		{Name: "execution-image", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: cluster.Status.ExecutionImage}},
		{Name: "resolved-options", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: string(options)}},
		{Name: "backend", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: string(backend)}},
	}
	if kind == "apply" {
		if cluster.Status.Recovery == nil {
			return nil, fmt.Errorf("apply recovery metadata was not persisted")
		}
		recovery, err := json.Marshal(cluster.Status.Recovery)
		if err != nil {
			return nil, fmt.Errorf("encode recovery metadata: %w", err)
		}
		params = append(params, tektonv1.Param{Name: "recovery", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: string(recovery)}})
	}
	return &tektonv1.PipelineRun{
		TypeMeta: metav1.TypeMeta{APIVersion: "tekton.dev/v1", Kind: "PipelineRun"},
		ObjectMeta: metav1.ObjectMeta{
			Name: operation.PipelineRunName, Namespace: cluster.Namespace,
			Labels: map[string]string{OperationLabel: operation.ID, ClusterUIDLabel: string(cluster.UID)},
		},
		Spec: tektonv1.PipelineRunSpec{PipelineRef: &tektonv1.PipelineRef{Name: PipelineName}, Params: params},
	}, nil
}

func MatchingRun(run *tektonv1.PipelineRun, uid, operation string) bool {
	return run.Labels[ClusterUIDLabel] == uid && run.Labels[OperationLabel] == operation
}

func ReportTaskRunName(run *tektonv1.PipelineRun) string {
	for _, child := range run.Status.ChildReferences {
		if child.Kind == "TaskRun" && child.PipelineTaskName == "operation" {
			return child.Name
		}
	}
	return ""
}

func ReportContainer(taskRun *tektonv1.TaskRun) string {
	for _, step := range taskRun.Status.Steps {
		if step.Name == ReportContainerName && step.Container != "" {
			return step.Container
		}
	}
	return ""
}

type backendConfig struct {
	Version                   int    `json:"version"`
	Bucket                    string `json:"bucket"`
	Key                       string `json:"key"`
	Region                    string `json:"region"`
	Endpoint                  string `json:"endpoint"`
	SkipCredentialsValidation bool   `json:"skip_credentials_validation"`
	SkipMetadataAPICheck      bool   `json:"skip_metadata_api_check"`
	SkipRegionValidation      bool   `json:"skip_region_validation"`
	SkipRequestingAccountID   bool   `json:"skip_requesting_account_id"`
	ForcePathStyle            bool   `json:"force_path_style,omitempty"`
	UseLockfile               bool   `json:"use_lockfile,omitempty"`
}

func encodeBackendConfig(identity servitorv1alpha1.BackendIdentity) ([]byte, error) {
	if identity.Version != 1 || identity.Bucket == "" || identity.Key == "" || identity.Region == "" || identity.Endpoint == "" {
		return nil, fmt.Errorf("backend identity is incomplete")
	}
	return json.Marshal(backendConfig{
		Version:                   identity.Version,
		Bucket:                    identity.Bucket,
		Key:                       identity.Key,
		Region:                    identity.Region,
		Endpoint:                  identity.Endpoint,
		SkipCredentialsValidation: identity.SkipCredentialsValidation,
		SkipMetadataAPICheck:      identity.SkipMetadataAPICheck,
		SkipRegionValidation:      identity.SkipRegionValidation,
		SkipRequestingAccountID:   identity.SkipRequestingAccountID,
		ForcePathStyle:            identity.ForcePathStyle,
		UseLockfile:               identity.UseLockfile,
	})
}

func Succeeded(run *tektonv1.PipelineRun) (done, success bool) {
	for _, condition := range run.Status.Status.Conditions {
		if condition.Type == "Succeeded" {
			switch string(condition.Status) {
			case "True":
				return true, true
			case "False":
				return true, false
			default:
				return false, false
			}
		}
	}
	return false, false
}

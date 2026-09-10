package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bevicted/servitor/internal/inventory"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	MaxInventoryReportBytes = 512 * 1024
	InventoryPipelineName   = "servitor-inventory"
	InventoryRunTimeout     = 15 * time.Minute
	InventoryRunLabel       = "servitor.bevicted.github.io/inventory-run"
	InventoryTargetLabel    = "servitor.bevicted.github.io/inventory-target"
	inventoryTaskName       = "inventory"
)

// InventoryReport is the isolated, controller-valid output of one inventory run.
type InventoryReport struct {
	Version  int               `json:"version"`
	Target   string            `json:"target"`
	RunID    string            `json:"runID"`
	Revision string            `json:"revision"`
	Catalog  inventory.Catalog `json:"catalog"`
}

func (r InventoryReport) Validate(target, runID, revision string) error {
	if r.Version != 1 || r.Target != target || r.RunID != runID || r.Revision != revision || !safeInventoryID(target) || !safeInventoryID(runID) || !safeInventoryRevision(revision) {
		return errors.New("inventory report identity does not match the active run")
	}
	if r.Catalog.Target != target || r.Catalog.Validate() != nil {
		return errors.New("inventory report catalog is invalid")
	}
	return nil
}

// DecodeInventoryReport accepts exactly one complete inventory report, bounded separately from allocation reports.
func DecodeInventoryReport(data []byte, target, runID, revision string) (InventoryReport, error) {
	if len(data) == 0 || len(data) > MaxInventoryReportBytes {
		return InventoryReport{}, errors.New("inventory report length is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var report InventoryReport
	if err := decoder.Decode(&report); err != nil {
		return InventoryReport{}, errors.New("decode inventory report failed")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return InventoryReport{}, errors.New("inventory report must contain exactly one JSON document")
	}
	if err := report.Validate(target, runID, revision); err != nil {
		return InventoryReport{}, fmt.Errorf("validate inventory report: %w", err)
	}
	return report, nil
}

// ReadInventoryReport reads only the designated report container with the inventory byte limit.
func ReadInventoryReport(ctx context.Context, reader LogReader, namespace, podName, container, target, runID, revision string) (InventoryReport, error) {
	if podName == "" || container == "" {
		return InventoryReport{}, errors.New("inventory report Pod or container is invalid")
	}
	stream, err := reader.ReadContainerLog(ctx, namespace, podName, container)
	if err != nil {
		return InventoryReport{}, fmt.Errorf("%w: %w", errReadReportLog, err)
	}
	defer func() { _ = stream.Close() }()
	data, err := io.ReadAll(io.LimitReader(stream, MaxInventoryReportBytes+1))
	if err != nil {
		return InventoryReport{}, fmt.Errorf("%w: %w", errReadReportLog, err)
	}
	return DecodeInventoryReport(data, target, runID, revision)
}

// DeterministicInventoryRunName creates an inventory identity distinct from allocation operation names.
// Attempt is persisted by the controller, so the identity remains stable across
// restart while never colliding with a terminal run still being deleted.
func DeterministicInventoryRunName(target, revision string, attempt uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", target, revision, attempt)))
	return "servitor-inventory-" + hex.EncodeToString(digest[:])[:16]
}

// NewInventoryRun builds an independent PipelineRun without allocation labels, COS inputs, or owner references.
func NewInventoryRun(namespace, image, target, runID, revision string, taskConfig TaskConfig) (*tektonv1.PipelineRun, error) {
	if namespace == "" || image == "" || !safeInventoryID(target) || !safeInventoryID(runID) || revision == "" || taskConfig.ICTConfigMap == "" || taskConfig.ICTConfigKey == "" || taskConfig.IBMSecret == "" {
		return nil, errors.New("inventory run configuration is incomplete")
	}
	return &tektonv1.PipelineRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: "tekton.dev/v1", Kind: "PipelineRun"},
		ObjectMeta: metav1.ObjectMeta{Name: runID, Namespace: namespace, Labels: map[string]string{InventoryRunLabel: runID, InventoryTargetLabel: target}},
		Spec: tektonv1.PipelineRunSpec{
			PipelineRef: &tektonv1.PipelineRef{Name: InventoryPipelineName},
			Params: tektonv1.Params{
				{Name: "inventory-target", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: target}},
				{Name: "inventory-run-id", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: runID}},
				{Name: "inventory-revision", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: revision}},
				{Name: "execution-image", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: image}},
				{Name: "ict-config-map", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: taskConfig.ICTConfigMap}},
				{Name: "ict-config-key", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: taskConfig.ICTConfigKey}},
				{Name: "ibm-secret", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: taskConfig.IBMSecret}},
			},
			TaskRunTemplate: operationTaskRunTemplate(),
			Timeouts:        &tektonv1.TimeoutFields{Pipeline: &metav1.Duration{Duration: InventoryRunTimeout}, Tasks: &metav1.Duration{Duration: 14 * time.Minute}},
		},
	}, nil
}

func MatchingInventoryRun(run *tektonv1.PipelineRun, target, runID string) bool {
	return run.Labels[InventoryTargetLabel] == target && run.Labels[InventoryRunLabel] == runID && run.Labels[ClusterUIDLabel] == "" && run.Labels[OperationLabel] == ""
}

func InventoryReportTaskRunName(run *tektonv1.PipelineRun) string {
	for _, child := range run.Status.ChildReferences {
		if child.Kind == "TaskRun" && child.PipelineTaskName == inventoryTaskName {
			return child.Name
		}
	}
	return ""
}

// InventoryReportContainer identifies the credential-free report step.
func InventoryReportContainer(taskRun *tektonv1.TaskRun) string {
	for _, step := range taskRun.Status.Steps {
		if step.Name == "report" && step.Container != "" {
			return step.Container
		}
	}
	return ""
}

func safeInventoryRevision(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.') {
			return false
		}
	}
	return true
}

func safeInventoryID(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	for index, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-') || (char == '-' && (index == 0 || index == len(value)-1)) {
			return false
		}
	}
	return true
}

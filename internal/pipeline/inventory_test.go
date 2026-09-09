package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bevicted/servitor/internal/inventory"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func validInventoryReport(t *testing.T) []byte {
	t.Helper()
	data, err := json.Marshal(InventoryReport{Version: 1, Target: "target-a", RunID: "inventory-a", Revision: "revision-a", Catalog: inventory.Catalog{Version: inventory.CatalogVersion, Target: "target-a", Providers: []string{"vpc-gen2"}, Versions: []inventory.Version{{Name: "4.22_openshift", Platform: "openshift", Default: true, Supported: true}}, ResourceGroups: []string{"Default"}, VPCLocations: []inventory.Location{{Name: "us-south-1", Flavors: []string{"bx2.4x16"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestInventoryReportRejectsWrongIdentityExtraDataAndOversize(t *testing.T) {
	data := validInventoryReport(t)
	if _, err := DecodeInventoryReport(data, "target-a", "inventory-a", "revision-a"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "extra JSON", data: append(data, []byte("{}")...)},
		{name: "oversized", data: bytes.Repeat([]byte("x"), MaxInventoryReportBytes+1)},
		{name: "unsafe catalog", data: []byte(strings.Replace(string(data), "Default", "bad\\nvalue", 1))},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeInventoryReport(test.data, "target-a", "inventory-a", "revision-a"); err == nil {
				t.Fatal("accepted invalid inventory report")
			}
		})
	}
	if _, err := DecodeInventoryReport(data, "other-target", "inventory-a", "revision-a"); err == nil {
		t.Fatal("accepted wrong inventory target")
	}
	if _, err := ReadInventoryReport(context.Background(), testLogs{data: data}, "ns", "pod", "step-report", "target-a", "inventory-a", "revision-a"); err != nil {
		t.Fatal(err)
	}
}

func TestNewInventoryRunIsIndependentAndCredentialIsolated(t *testing.T) {
	run, err := NewInventoryRun("ns", "registry.example.invalid/task@sha256:deadbeef", "target-a", "inventory-a", "revision-a", testTaskConfig)
	if err != nil {
		t.Fatal(err)
	}
	if run.Spec.PipelineRef.Name != InventoryPipelineName || !MatchingInventoryRun(run, "target-a", "inventory-a") {
		t.Fatalf("inventory identity = %#v", run)
	}
	if run.Labels[ClusterUIDLabel] != "" || run.Labels[OperationLabel] != "" {
		t.Fatalf("inventory run has allocation labels: %v", run.Labels)
	}
	params := map[string]string{}
	for _, param := range run.Spec.Params {
		params[param.Name] = param.Value.StringVal
	}
	for _, forbidden := range []string{"cos-secret", "backend", "resolved-options", "recovery", "operation-id", "cluster-uid"} {
		if _, found := params[forbidden]; found {
			t.Fatalf("inventory run exposed allocation parameter %q: %v", forbidden, params)
		}
	}
	if params["ibm-secret"] != testTaskConfig.IBMSecret || run.Spec.TaskRunTemplate.ServiceAccountName != "servitor-task" {
		t.Fatalf("inventory run execution config = %#v", run.Spec)
	}
	run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, PipelineTaskName: inventoryTaskName, Name: "inventory-task"}}
	if InventoryReportTaskRunName(run) != "inventory-task" {
		t.Fatal("inventory report task lookup failed")
	}
	task := &tektonv1.TaskRun{Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{Steps: []tektonv1.StepState{{Name: "report", Container: "step-report"}}}}}
	if InventoryReportContainer(task) != "step-report" {
		t.Fatal("inventory report container lookup failed")
	}
}

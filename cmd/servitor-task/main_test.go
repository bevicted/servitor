package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
)

func TestRunPlanUsesInitializedWorkspaceAndSanitizesOversizedPlan(t *testing.T) {
	directory := t.TempDir()
	workspace := filepath.Join(directory, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, ".cluster"), 0o700); err != nil {
		t.Fatal(err)
	}
	backendFile := filepath.Join(directory, "backend.json")
	resultFile := filepath.Join(directory, "result.json")
	reportFile := filepath.Join(directory, "report.json")
	planResultFile := filepath.Join(directory, "plan-result.json")
	planShowFile := filepath.Join(directory, "plan-show.json")
	recovery := servitorv1alpha1.RecoveryMetadata{
		Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Endpoints: map[string]string{
			"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
		},
		Values: servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"},
	}
	result := ictPlanResult{Version: 1, StateID: "plan-a", PlanPath: filepath.Join(workspace, ".cluster", "create.tfplan"), Values: recovery.Values}
	result.Recovery.Version = recovery.Version
	result.Recovery.Target = recovery.Target
	result.Recovery.Endpoints = recovery.Endpoints
	result.Recovery.Values = recovery.Values
	result.Recovery.TFVarsSHA256 = recovery.TFVarsSHA256
	if err := writeJSON(planResultFile, result); err != nil {
		t.Fatal(err)
	}
	planShow := []byte(`{"format_version":"1.2","resource_changes":[],"ignored":"` + strings.Repeat("x", pipeline.MaxReportBytes) + `"}`)
	if len(planShow) <= pipeline.MaxReportBytes {
		t.Fatal("Terraform plan fixture is not oversized")
	}
	if err := os.WriteFile(planShowFile, planShow, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLAN_RESULT", planResultFile)
	t.Setenv("PLAN_SHOW", planShowFile)
	t.Setenv("TRACE_FILE", filepath.Join(directory, "terraform.args"))
	t.Setenv("ICT_TRACE_FILE", filepath.Join(directory, "ict.args"))
	t.Setenv("WORKSPACE", workspace)
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ICT_TRACE_FILE\"\n[ \"$1\" = plan ] && [ \"$2\" = plan-a ] || exit 2\ncp \"$PLAN_RESULT\" \"$6\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	terraform := filepath.Join(directory, "terraform")
	if err := os.WriteFile(terraform, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$TRACE_FILE\"\n[ \"$1\" = \"-chdir=$WORKSPACE\" ] && [ \"$2\" = show ] && [ \"$3\" = -json ] && [ \"$4\" = .cluster/create.tfplan ] || exit 3\ncat \"$PLAN_SHOW\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}}
	if err := runPlan(context.Background(), "uid", "plan-a", options, backendFile, resultFile, reportFile, ict, terraform); err != nil {
		t.Fatal(err)
	}
	trace, err := os.ReadFile(filepath.Join(directory, "terraform.args"))
	if err != nil {
		t.Fatal(err)
	}
	wantTrace := "-chdir=" + workspace + "\nshow\n-json\n.cluster/create.tfplan\n"
	if got := string(trace); got != wantTrace {
		t.Fatalf("terraform command = %q, want %q", got, wantTrace)
	}
	ictTrace, err := os.ReadFile(filepath.Join(directory, "ict.args"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(ictTrace); !strings.Contains(got, "--prefix\nservitor\n") || strings.Contains(got, "--name\n") || strings.Contains(got, "--owner\n") {
		t.Fatalf("ICT plan arguments = %q, want fixed generated-name prefix without caller name or owner", got)
	}
	report, err := os.ReadFile(reportFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(report) > pipeline.MaxReportBytes {
		t.Fatalf("sanitized report length = %d, limit = %d", len(report), pipeline.MaxReportBytes)
	}
	decoded, err := pipeline.DecodeReport(report, "uid", "plan-a")
	if err != nil {
		t.Fatalf("plan report is not controller-valid: %v", err)
	}
	if decoded.ResolvedOptions.ClusterName != recovery.Values.ClusterName || decoded.ResolvedOptions.Region != recovery.Values.Region || decoded.ResolvedOptions.Version != recovery.Values.KubeVersion || decoded.ResolvedOptions.WorkerCount != recovery.Values.WorkerCount || decoded.ResolvedOptions.Zone != recovery.Values.Zone || decoded.ResolvedOptions.Flavor != recovery.Values.Flavor {
		t.Fatalf("plan report did not adopt ICT-normalized options: %+v", decoded.ResolvedOptions)
	}
	if strings.Contains(string(report), workspace) {
		t.Fatal("plan workspace was exposed in the report")
	}
}

func TestResolvedOptionsFromValuesAdoptsAllProviderValues(t *testing.T) {
	tests := []struct {
		name   string
		values servitorv1alpha1.RecoveryValues
		want   servitorv1alpha1.UserOptions
	}{
		{
			name:   "vpc",
			values: servitorv1alpha1.RecoveryValues{ClusterName: "vpc-cluster", ResourceGroupName: "group", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}},
			want:   servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Platform: "openshift", Version: "4.22_openshift", ResourceGroup: "group", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}},
		},
		{
			name:   "classic",
			values: servitorv1alpha1.RecoveryValues{ClusterName: "classic-cluster", ResourceGroupName: "group", Region: "us-south", ClusterMode: "classic", Platform: "kubernetes", KubeVersion: "1.31", WorkerCount: 3, Datacenter: "dal10", MachineType: "bx2.4x16", PublicVLANID: "123", PrivateVLANID: "456"},
			want:   servitorv1alpha1.UserOptions{Target: "target", Provider: "classic", Platform: "kubernetes", Version: "1.31", ResourceGroup: "group", WorkerCount: 3, Datacenter: "dal10", MachineType: "bx2.4x16", PublicVLANID: "123", PrivateVLANID: "456"},
		},
		{
			name:   "satellite",
			values: servitorv1alpha1.RecoveryValues{ClusterName: "satellite-cluster", ResourceGroupName: "group", Region: "us-south", ClusterMode: "satellite", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 3, VPCID: "vpc", SubnetIDs: []string{"subnet-a", "subnet-b", "subnet-c"}, PublicGatewayIDs: []string{"gateway-a", "gateway-b", "gateway-c"}, SatelliteZones: []string{"us-south-1", "us-south-2", "us-south-3"}, SatelliteManagedFrom: "us-south", SatelliteLocationID: "location", SatelliteHostImage: "image", SatelliteHostProfile: "bx2-4x16", SatelliteSSHKeyID: "key", SatelliteWorkerInstanceIDs: []string{"worker-a", "worker-b", "worker-c"}, SatelliteWorkerOperatingSystem: "RHCOS"},
			want:   servitorv1alpha1.UserOptions{Target: "target", Provider: "satellite", Platform: "openshift", Version: "4.22_openshift", ResourceGroup: "group", WorkerCount: 3, VPCID: "vpc", SubnetIDs: []string{"subnet-a", "subnet-b", "subnet-c"}, PublicGatewayIDs: []string{"gateway-a", "gateway-b", "gateway-c"}, SatelliteZones: []string{"us-south-1", "us-south-2", "us-south-3"}, SatelliteManagedFrom: "us-south", SatelliteLocationID: "location", SatelliteHostImage: "image", SatelliteHostProfile: "bx2-4x16", SatelliteSSHKeyID: "key", SatelliteWorkerInstanceIDs: []string{"worker-a", "worker-b", "worker-c"}, SatelliteWorkerOperatingSystem: "RHCOS"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolvedOptionsFromValues(servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", VPCID: "stale-vpc", MachineType: "stale-machine"}}, test.values)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(resolved.UserOptions, test.want) || resolved.ClusterName != test.values.ClusterName || resolved.Region != test.values.Region {
				t.Fatalf("resolved = %+v, want options %+v", resolved, test.want)
			}
		})
	}
}

func TestOptionArgsOmitsVPCIDForClassic(t *testing.T) {
	args := optionArgs(servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "classic", Platform: "kubernetes", Version: "1.31", Datacenter: "dal10", MachineType: "bx2.4x16", PublicVLANID: "123", PrivateVLANID: "456"}})
	for _, arg := range args {
		if arg == "--vpc-id" {
			t.Fatalf("Classic argv included --vpc-id: %q", args)
		}
	}
}

func TestRunDestroyUsesFrozenContextAndProducesValidatedReport(t *testing.T) {
	directory := t.TempDir()
	backendFile := filepath.Join(directory, "backend.json")
	recoveryFile := filepath.Join(directory, "recovery.json")
	resultFile := filepath.Join(directory, "result.json")
	reportFile := filepath.Join(directory, "report.json")
	if err := writeJSON(backendFile, backendConfig{Version: 1, Bucket: "bucket", Key: "key", Region: "us-south", Endpoint: "https://s3.example.invalid"}); err != nil {
		t.Fatal(err)
	}
	recovery := servitorv1alpha1.RecoveryMetadata{
		Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Endpoints: map[string]string{
			"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
		},
		Values: servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"},
	}
	if err := writeJSON(recoveryFile, recovery); err != nil {
		t.Fatal(err)
	}
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\n[ \"$1\" = destroy ] && [ \"$2\" = destroy-a ] || exit 2\n[ -f \"$4\" ] || exit 3\nprintf '%s' '{\"version\":1,\"operation\":\"destroy\"}' > \"$8\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "frozen"}
	if err := runDestroy(context.Background(), "uid", "destroy-a", options, backendFile, recoveryFile, resultFile, reportFile, ict); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(reportFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.DecodeReport(data, "uid", "destroy-a"); err != nil {
		t.Fatalf("destroy report is not controller-valid: %v", err)
	}
	var context ictContext
	data, err = os.ReadFile(filepath.Join(directory, "context.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &context); err != nil || context.StateID != "destroy-a" || context.Recovery.TFVarsSHA256 != recovery.TFVarsSHA256 || context.Values.ClusterName != recovery.Values.ClusterName {
		t.Fatalf("destroy context = %+v, err=%v", context, err)
	}
}

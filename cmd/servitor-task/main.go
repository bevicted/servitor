// servitor-task runs one ICT planning operation and writes its sanitized report to task-local storage.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/terraformview"
)

type ictPlanResult struct {
	PlanPath string `json:"plan_path"`
	Values   struct {
		ClusterName string `json:"cluster_name"`
		Region      string `json:"region"`
	} `json:"values"`
	Recovery struct {
		Version      int               `json:"version"`
		Target       string            `json:"target"`
		Endpoints    map[string]string `json:"endpoints"`
		TFVarsSHA256 string            `json:"tfvars_sha256"`
	} `json:"recovery"`
}

func main() {
	var uid, operation, optionsFile, backendFile, resultFile, reportFile, ictPath, terraformPath, optionsJSON, backendJSON string
	flag.StringVar(&uid, "cluster-uid", "", "ServitorCluster UID")
	flag.StringVar(&operation, "operation-id", "", "persisted operation ID")
	flag.StringVar(&optionsFile, "resolved-options", "", "task-local frozen options JSON")
	flag.StringVar(&optionsJSON, "resolved-options-json", "", "frozen options JSON parameter")
	flag.StringVar(&backendFile, "backend-config", "", "task-local backend config JSON")
	flag.StringVar(&backendJSON, "backend-json", "", "backend JSON parameter")
	flag.StringVar(&resultFile, "ict-result", "", "task-local ICT result JSON")
	flag.StringVar(&reportFile, "report", "", "task-local report JSON")
	flag.StringVar(&ictPath, "ict", "ict", "ICT executable")
	flag.StringVar(&terraformPath, "terraform", "terraform", "Terraform executable")
	flag.Parse()
	err := materializeParameterFile(optionsFile, optionsJSON)
	if err == nil {
		err = materializeParameterFile(backendFile, backendJSON)
	}
	if err == nil {
		err = run(context.Background(), uid, operation, optionsFile, backendFile, resultFile, reportFile, ictPath, terraformPath)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "servitor-task:", err)
		os.Exit(1)
	}
}

func materializeParameterFile(path, contents string) error {
	if contents == "" {
		return nil
	}
	if !filepath.IsAbs(path) {
		return errors.New("task-local file paths must be absolute")
	}
	return os.WriteFile(path, []byte(contents), 0o600)
}

func run(ctx context.Context, uid, operation, optionsFile, backendFile, resultFile, reportFile, ictPath, terraformPath string) error {
	if uid == "" || operation == "" {
		return errors.New("cluster UID and operation ID are required")
	}
	for _, path := range []string{optionsFile, backendFile, resultFile, reportFile} {
		if !filepath.IsAbs(path) {
			return errors.New("task-local file paths must be absolute")
		}
	}
	optionsData, err := os.ReadFile(optionsFile)
	if err != nil {
		return fmt.Errorf("read resolved options: %w", err)
	}
	var options servitorv1alpha1.ResolvedOptions
	decoder := json.NewDecoder(bytesReader(optionsData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&options); err != nil {
		return fmt.Errorf("decode resolved options: %w", err)
	}
	args := append([]string{"plan", operation, "--backend-config", backendFile, "--result-file", resultFile}, optionArgs(options)...)
	if _, err := (command.Runner{MaxOutput: 64 * 1024, Log: os.Stderr}).Run(ctx, ictPath, args...); err != nil {
		return err
	}
	resultData, err := os.ReadFile(resultFile)
	if err != nil {
		return fmt.Errorf("read ICT result: %w", err)
	}
	var result ictPlanResult
	if err := json.Unmarshal(resultData, &result); err != nil || result.PlanPath == "" {
		return errors.New("ICT produced no valid planning result")
	}
	shown, err := (command.Runner{MaxOutput: pipeline.MaxReportBytes}).Run(ctx, terraformPath, "show", "-json", result.PlanPath)
	if err != nil || shown.StdoutTruncated {
		return errors.New("cannot obtain bounded Terraform plan review")
	}
	plan, err := terraformview.ParsePlan([]byte(shown.Stdout))
	if err != nil {
		return fmt.Errorf("sanitize Terraform plan: %w", err)
	}
	review := servitorv1alpha1.ReviewSummary{Resources: make([]servitorv1alpha1.SummaryResource, 0, len(plan.Resources))}
	for _, resource := range plan.Resources {
		review.Resources = append(review.Resources, servitorv1alpha1.SummaryResource{Role: resource.Role(), ID: resource.ID, Name: resource.DisplayName, Actions: resource.Actions, Reused: resource.Reused})
	}
	options.ClusterName, options.Region = result.Values.ClusterName, result.Values.Region
	report := pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: options, Recovery: servitorv1alpha1.RecoveryMetadata{Version: result.Recovery.Version, Target: result.Recovery.Target, Endpoints: result.Recovery.Endpoints, Values: options, TFVarsSHA256: result.Recovery.TFVarsSHA256}, Review: review}
	if err := report.Validate(uid, operation); err != nil {
		return err
	}
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > pipeline.MaxReportBytes {
		return errors.New("sanitized report exceeds byte limit")
	}
	return os.WriteFile(reportFile, data, 0o600)
}

func optionArgs(options servitorv1alpha1.ResolvedOptions) []string {
	o := options.UserOptions
	args := []string{}
	add := func(name, value string) {
		if value != "" {
			args = append(args, name, value)
		}
	}
	add("--target", o.Target)
	add("--provider", o.Provider)
	add("--platform", o.Platform)
	add("--version", o.Version)
	add("--resource-group", o.ResourceGroup)
	add("--zone", o.Zone)
	add("--flavor", o.Flavor)
	add("--vpc-id", o.VPCID)
	add("--datacenter", o.Datacenter)
	add("--machine-type", o.MachineType)
	add("--public-vlan-id", o.PublicVLANID)
	add("--private-vlan-id", o.PrivateVLANID)
	add("--satellite-managed-from", o.SatelliteManagedFrom)
	add("--satellite-location-id", o.SatelliteLocationID)
	add("--satellite-host-image", o.SatelliteHostImage)
	add("--satellite-host-profile", o.SatelliteHostProfile)
	add("--satellite-ssh-key-id", o.SatelliteSSHKeyID)
	add("--satellite-worker-operating-system", o.SatelliteWorkerOperatingSystem)
	add("--name", o.Name)
	for _, value := range o.SubnetIDs {
		add("--subnet-id", value)
	}
	for _, value := range o.PublicGatewayIDs {
		add("--public-gateway-id", value)
	}
	for _, value := range o.SatelliteZones {
		add("--satellite-zone", value)
	}
	for _, value := range o.SatelliteWorkerInstanceIDs {
		add("--satellite-worker-instance-id", value)
	}
	if o.WorkerCount > 0 {
		add("--worker-count", strconv.Itoa(o.WorkerCount))
	}
	return args
}

// bytesReader keeps strict decoder setup obvious without exposing arbitrary reader behavior.
func bytesReader(data []byte) *bytes.Reader { return bytes.NewReader(data) }

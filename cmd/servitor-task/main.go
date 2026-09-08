// servitor-task runs one ICT operation and writes its sanitized report to task-local storage.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/terraformview"
)

type ictPlanResult struct {
	Version  int                             `json:"version"`
	StateID  string                          `json:"state_id"`
	PlanPath string                          `json:"plan_path"`
	Values   servitorv1alpha1.RecoveryValues `json:"values"`
	Backend  backendConfig                   `json:"backend"`
	Recovery struct {
		Version                          int                             `json:"version"`
		Target                           string                          `json:"target"`
		Endpoints                        map[string]string               `json:"endpoints"`
		Values                           servitorv1alpha1.RecoveryValues `json:"values"`
		SatelliteSSHPublicKeyFingerprint string                          `json:"satellite_ssh_public_key_fingerprint"`
		TFVarsSHA256                     string                          `json:"tfvars_sha256"`
	} `json:"recovery"`
}

type ictOperationResult struct {
	Version   int    `json:"version"`
	Operation string `json:"operation"`
	Workspace string `json:"workspace"`
}

type ictContext struct {
	Version  int                             `json:"version"`
	StateID  string                          `json:"state_id"`
	Values   servitorv1alpha1.RecoveryValues `json:"values"`
	Recovery struct {
		Version                          int                             `json:"version"`
		Target                           string                          `json:"target"`
		Endpoints                        map[string]string               `json:"endpoints"`
		Values                           servitorv1alpha1.RecoveryValues `json:"values"`
		SatelliteSSHPublicKeyFingerprint string                          `json:"satellite_ssh_public_key_fingerprint"`
		TFVarsSHA256                     string                          `json:"tfvars_sha256"`
	} `json:"recovery"`
	Backend  backendConfig `json:"backend"`
	PlanPath string        `json:"plan_path"`
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

func main() {
	var uid, operation, kind, optionsFile, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath, optionsJSON, backendJSON, recoveryJSON string
	flag.StringVar(&uid, "cluster-uid", "", "ServitorCluster UID")
	flag.StringVar(&operation, "operation-id", "", "persisted operation ID")
	flag.StringVar(&kind, "operation-kind", "", "plan, apply, or destroy")
	flag.StringVar(&optionsFile, "resolved-options", "", "task-local frozen options JSON")
	flag.StringVar(&optionsJSON, "resolved-options-json", "", "frozen options JSON parameter")
	flag.StringVar(&backendFile, "backend-config", "", "task-local backend config JSON")
	flag.StringVar(&backendJSON, "backend-json", "", "backend JSON parameter")
	flag.StringVar(&recoveryFile, "recovery", "", "task-local frozen recovery JSON")
	flag.StringVar(&recoveryJSON, "recovery-json", "", "frozen recovery JSON parameter")
	flag.StringVar(&resultFile, "ict-result", "", "task-local ICT result JSON")
	flag.StringVar(&reportFile, "report", "", "task-local report JSON")
	flag.StringVar(&ictPath, "ict", "ict", "ICT executable")
	flag.StringVar(&terraformPath, "terraform", "terraform", "Terraform executable")
	flag.Parse()
	err := materializeParameterFile(optionsFile, optionsJSON)
	if err == nil {
		err = materializeParameterFile(backendFile, backendJSON)
	}
	if err == nil && recoveryJSON != "" {
		err = materializeParameterFile(recoveryFile, recoveryJSON)
	}
	if err == nil {
		err = run(context.Background(), uid, operation, kind, optionsFile, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath)
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

func run(ctx context.Context, uid, operation, kind, optionsFile, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath string) error {
	if uid == "" || operation == "" || (kind != "plan" && kind != "apply" && kind != "destroy") {
		return errors.New("cluster UID, operation ID, and operation kind are required")
	}
	paths := []string{optionsFile, backendFile, resultFile, reportFile}
	if kind == "apply" || kind == "destroy" {
		paths = append(paths, recoveryFile)
	}
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			return errors.New("task-local file paths must be absolute")
		}
	}
	options, err := readJSON[servitorv1alpha1.ResolvedOptions](optionsFile)
	if err != nil {
		return fmt.Errorf("read resolved options: %w", err)
	}
	if kind == "plan" {
		return runPlan(ctx, uid, operation, options, backendFile, resultFile, reportFile, ictPath, terraformPath)
	}
	if kind == "apply" {
		return runApply(ctx, uid, operation, options, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath)
	}
	return runDestroy(ctx, uid, operation, options, backendFile, recoveryFile, resultFile, reportFile, ictPath)
}

func runPlan(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, backendFile, resultFile, reportFile, ictPath, terraformPath string) error {
	args := append([]string{"plan", operation, "--backend-config", backendFile, "--result-file", resultFile}, optionArgs(options)...)
	if _, err := (command.Runner{MaxOutput: 64 * 1024, Log: os.Stderr}).Run(ctx, ictPath, args...); err != nil {
		return err
	}
	result, err := readJSON[ictPlanResult](resultFile)
	if err != nil || result.Version != 1 || result.StateID != operation || !filepath.IsAbs(result.PlanPath) {
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
	options.ClusterName, options.Region = result.Values.ClusterName, result.Values.Region
	recovery := servitorv1alpha1.RecoveryMetadata{Version: result.Recovery.Version, Target: result.Recovery.Target, Endpoints: result.Recovery.Endpoints, Values: result.Recovery.Values, SatelliteSSHPublicKeyFingerprint: result.Recovery.SatelliteSSHPublicKeyFingerprint, TFVarsSHA256: result.Recovery.TFVarsSHA256}
	return writeReport(reportFile, pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: options, Recovery: recovery, Review: summaryFromPlan(plan)})
}

func runApply(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath string) error {
	recovery, contextFile, err := frozenContext(operation, backendFile, recoveryFile, resultFile)
	if err != nil {
		return err
	}
	if _, err := (command.Runner{MaxOutput: 64 * 1024, Log: os.Stderr}).Run(ctx, ictPath, "apply", operation, "--context-file", contextFile, "--backend-config", backendFile, "--result-file", resultFile, "--auto-approve"); err != nil {
		return err
	}
	result, err := readJSON[ictOperationResult](resultFile)
	if err != nil || result.Version != 1 || result.Operation != "apply" || !filepath.IsAbs(result.Workspace) {
		return errors.New("ICT produced no valid apply result")
	}
	shown, err := (command.Runner{MaxOutput: pipeline.MaxReportBytes}).Run(ctx, terraformPath, "-chdir="+result.Workspace, "show", "-json")
	if err != nil || shown.StdoutTruncated {
		return errors.New("cannot obtain bounded Terraform ready summary")
	}
	state, err := terraformview.ParseState([]byte(shown.Stdout))
	if err != nil {
		return fmt.Errorf("sanitize Terraform state: %w", err)
	}
	return writeReport(reportFile, pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: options, Recovery: recovery, Ready: summaryFromState(state)})
}

func runDestroy(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, backendFile, recoveryFile, resultFile, reportFile, ictPath string) error {
	recovery, contextFile, err := frozenContext(operation, backendFile, recoveryFile, resultFile)
	if err != nil {
		return err
	}
	if _, err := (command.Runner{MaxOutput: 64 * 1024, Log: os.Stderr}).Run(ctx, ictPath, "destroy", operation, "--context-file", contextFile, "--backend-config", backendFile, "--result-file", resultFile); err != nil {
		return err
	}
	result, err := readJSON[ictOperationResult](resultFile)
	if err != nil || result.Version != 1 || result.Operation != "destroy" {
		return errors.New("ICT produced no valid destroy result")
	}
	return writeReport(reportFile, pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: options, Recovery: recovery})
}

func frozenContext(operation, backendFile, recoveryFile, resultFile string) (servitorv1alpha1.RecoveryMetadata, string, error) {
	recovery, err := readJSON[servitorv1alpha1.RecoveryMetadata](recoveryFile)
	if err != nil {
		return servitorv1alpha1.RecoveryMetadata{}, "", fmt.Errorf("read recovery metadata: %w", err)
	}
	backend, err := readJSON[backendConfig](backendFile)
	if err != nil {
		return servitorv1alpha1.RecoveryMetadata{}, "", fmt.Errorf("read backend configuration: %w", err)
	}
	contextFile := filepath.Join(filepath.Dir(resultFile), "context.json")
	handoff := ictContext{Version: 1, StateID: operation, Values: recovery.Values, Backend: backend, PlanPath: filepath.Join(filepath.Dir(resultFile), "disposable.tfplan")}
	handoff.Recovery.Version = recovery.Version
	handoff.Recovery.Target = recovery.Target
	handoff.Recovery.Endpoints = recovery.Endpoints
	handoff.Recovery.Values = recovery.Values
	handoff.Recovery.SatelliteSSHPublicKeyFingerprint = recovery.SatelliteSSHPublicKeyFingerprint
	handoff.Recovery.TFVarsSHA256 = recovery.TFVarsSHA256
	if err := writeJSON(contextFile, handoff); err != nil {
		return servitorv1alpha1.RecoveryMetadata{}, "", fmt.Errorf("write frozen operation context: %w", err)
	}
	return recovery, contextFile, nil
}

func readJSON[T any](path string) (T, error) {
	var value T
	data, err := os.ReadFile(path)
	if err != nil {
		return value, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return value, errors.New("multiple JSON documents")
	}
	return value, nil
}

func writeJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func writeReport(path string, report pipeline.Report) error {
	if err := report.Validate(report.ClusterUID, report.OperationID); err != nil {
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
	return os.WriteFile(path, data, 0o600)
}

func summaryFromPlan(plan terraformview.Plan) servitorv1alpha1.ReviewSummary {
	summary := servitorv1alpha1.ReviewSummary{Resources: make([]servitorv1alpha1.SummaryResource, 0, len(plan.Resources))}
	for _, resource := range plan.Resources {
		summary.Resources = append(summary.Resources, servitorv1alpha1.SummaryResource{Role: resource.Role(), ID: resource.ID, Name: resource.DisplayName, Actions: resource.Actions, Reused: resource.Reused})
	}
	return summary
}

func summaryFromState(state terraformview.State) servitorv1alpha1.ReadySummary {
	summary := servitorv1alpha1.ReadySummary{Resources: make([]servitorv1alpha1.SummaryResource, 0, len(state.Resources))}
	for _, resource := range state.Resources {
		summary.Resources = append(summary.Resources, servitorv1alpha1.SummaryResource{Role: resource.Role(), ID: resource.ID, Name: resource.DisplayName, Reused: resource.Reused})
	}
	return summary
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

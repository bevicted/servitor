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
	"strings"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/inventory"
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

const maxTerraformShowBytes = 4 * 1024 * 1024

func main() {
	var uid, operation, kind, optionsFile, backendFile, recoveryFile, resultFile, reportFile, emitReport, ictPath, terraformPath, optionsJSON, backendJSON, recoveryJSON string
	var inventoryConfig, inventoryTarget, inventoryRunID, inventoryRevision, inventoryReport, emitInventoryReport, apiKeyEnv string
	var planningInventoryConfig string
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
	flag.StringVar(&emitReport, "emit-report", "", "emit one validated task-local report JSON document")
	flag.StringVar(&inventoryConfig, "inventory-config", "", "mounted inventory target configuration")
	flag.StringVar(&planningInventoryConfig, "planning-inventory-config", "/etc/servitor/ict/config.yaml", "mounted configuration for planning validation")
	flag.StringVar(&inventoryTarget, "inventory-target", "", "configured inventory target")
	flag.StringVar(&inventoryRunID, "inventory-run-id", "", "inventory run identity")
	flag.StringVar(&inventoryRevision, "inventory-revision", "", "target configuration revision")
	flag.StringVar(&inventoryReport, "inventory-report", "", "task-local inventory report JSON")
	flag.StringVar(&emitInventoryReport, "emit-inventory-report", "", "emit one validated task-local inventory report JSON document")
	flag.StringVar(&apiKeyEnv, "ibm-api-key-env", "IBMCLOUD_API_KEY", "environment variable containing the IBM API key")
	flag.StringVar(&ictPath, "ict", "ict", "ICT executable")
	flag.StringVar(&terraformPath, "terraform", "terraform", "Terraform executable")
	flag.Parse()
	if emitReport != "" || emitInventoryReport != "" {
		var err error
		if emitReport != "" {
			err = emitValidatedReport(emitReport)
		} else {
			err = emitValidatedInventoryReport(emitInventoryReport, inventoryTarget, inventoryRunID, inventoryRevision)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "servitor-task:", err)
			os.Exit(1)
		}
		return
	}
	if inventoryConfig != "" || inventoryTarget != "" || inventoryRunID != "" || inventoryRevision != "" || inventoryReport != "" {
		if err := runInventory(context.Background(), inventoryConfig, inventoryTarget, inventoryRunID, inventoryRevision, inventoryReport, os.Getenv(apiKeyEnv)); err != nil {
			fmt.Fprintln(os.Stderr, "servitor-task:", err)
			os.Exit(1)
		}
		return
	}
	err := materializeParameterFile(optionsFile, optionsJSON)
	if err == nil {
		err = materializeParameterFile(backendFile, backendJSON)
	}
	if err == nil && recoveryJSON != "" {
		err = materializeParameterFile(recoveryFile, recoveryJSON)
	}
	if err == nil {
		err = run(context.Background(), uid, operation, kind, optionsFile, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath, planningInventoryConfig, os.Getenv(apiKeyEnv))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "servitor-task:", err)
		os.Exit(1)
	}
}

func emitValidatedReport(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("task-local file paths must be absolute")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > pipeline.MaxReportBytes || data[len(data)-1] != '\n' {
		return errors.New("report is not a bounded complete JSON document")
	}
	var report pipeline.Report
	if err := json.Unmarshal(data, &report); err != nil {
		return fmt.Errorf("decode report: %w", err)
	}
	if err := report.Validate(report.ClusterUID, report.OperationID); err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func emitValidatedInventoryReport(path, target, runID, revision string) error {
	if !filepath.IsAbs(path) {
		return errors.New("task-local file paths must be absolute")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > pipeline.MaxInventoryReportBytes || data[len(data)-1] != '\n' {
		return errors.New("inventory report is not a bounded complete JSON document")
	}
	if _, err := pipeline.DecodeInventoryReport(data, target, runID, revision); err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func runInventory(ctx context.Context, configPath, target, runID, revision, reportPath, apiKey string) error {
	if !filepath.IsAbs(configPath) || !filepath.IsAbs(reportPath) || target == "" || runID == "" || revision == "" {
		return errors.New("inventory configuration, identity, and report paths are required")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return errors.New("inventory configuration is unavailable")
	}
	config, err := inventory.LoadConfig(data)
	if err != nil {
		return err
	}
	catalog, err := inventory.Discover(ctx, config, target, apiKey, nil)
	if err != nil {
		return inventory.RedactedError(err)
	}
	report := pipeline.InventoryReport{Version: 1, Target: target, RunID: runID, Revision: revision, Catalog: catalog}
	if err := report.Validate(target, runID, revision); err != nil {
		return err
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		return errors.New("encode inventory report failed")
	}
	encoded = append(encoded, '\n')
	if len(encoded) > pipeline.MaxInventoryReportBytes {
		return errors.New("inventory report exceeds byte limit")
	}
	return os.WriteFile(reportPath, encoded, 0o600)
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

func run(ctx context.Context, uid, operation, kind, optionsFile, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath, planningInventoryConfig, apiKey string) error {
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
		return runPlan(ctx, uid, operation, options, backendFile, resultFile, reportFile, ictPath, terraformPath, planningInventoryConfig, apiKey)
	}
	if kind == "apply" {
		return runApply(ctx, uid, operation, options, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath)
	}
	return runDestroy(ctx, uid, operation, options, backendFile, recoveryFile, resultFile, reportFile, ictPath)
}

func runPlan(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, backendFile, resultFile, reportFile, ictPath, terraformPath, planningInventoryConfig, apiKey string) error {
	options, rejection, err := validatePlanOptions(ctx, planningInventoryConfig, apiKey, options)
	if err != nil {
		return err
	}
	if rejection != nil {
		return writeReport(reportFile, pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, PlanRejection: rejection})
	}
	platform, err := command.InferPlatform(options.Version)
	if err != nil || options.Platform != platform {
		return errors.New("resolved platform does not match version")
	}
	args := append([]string{"plan", operation, "--backend-config", backendFile, "--result-file", resultFile, "--prefix", "servitor"}, optionArgs(options)...)
	if _, err := (command.Runner{MaxOutput: 64 * 1024, Log: os.Stderr}).Run(ctx, ictPath, args...); err != nil {
		return err
	}
	result, err := readJSON[ictPlanResult](resultFile)
	if err != nil || result.Version != 1 || result.StateID != operation || !filepath.IsAbs(result.PlanPath) {
		return errors.New("ICT produced no valid planning result")
	}
	workspace := filepath.Dir(filepath.Dir(result.PlanPath))
	planPath := filepath.Join(filepath.Base(filepath.Dir(result.PlanPath)), filepath.Base(result.PlanPath))
	shown, err := (command.Runner{MaxOutput: maxTerraformShowBytes}).Run(ctx, terraformPath, "-chdir="+workspace, "show", "-json", planPath)
	if err != nil || shown.StdoutTruncated {
		return errors.New("cannot obtain bounded Terraform plan review")
	}
	plan, err := terraformview.ParsePlan([]byte(shown.Stdout))
	if err != nil {
		return fmt.Errorf("sanitize Terraform plan: %w", err)
	}
	options, err = resolvedOptionsFromValues(options, result.Values)
	if err != nil {
		return err
	}
	recoveryOptions, err := resolvedOptionsFromValues(options, result.Recovery.Values)
	if err != nil {
		return fmt.Errorf("ICT produced invalid recovery values: %w", err)
	}
	if recoveryOptions.Version != options.Version || recoveryOptions.Platform != options.Platform {
		return errors.New("ICT recovery values are inconsistent with planning result")
	}
	recovery := servitorv1alpha1.RecoveryMetadata{Version: result.Recovery.Version, Target: result.Recovery.Target, Endpoints: result.Recovery.Endpoints, Values: result.Recovery.Values, SatelliteSSHPublicKeyFingerprint: result.Recovery.SatelliteSSHPublicKeyFingerprint, TFVarsSHA256: result.Recovery.TFVarsSHA256}
	return writeReport(reportFile, pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: options, Recovery: recovery, Review: summaryFromPlan(plan)})
}

func validatePlanOptions(ctx context.Context, configPath, apiKey string, options servitorv1alpha1.ResolvedOptions) (servitorv1alpha1.ResolvedOptions, *servitorv1alpha1.PlanRejection, error) {
	if !filepath.IsAbs(configPath) {
		return servitorv1alpha1.ResolvedOptions{}, nil, errors.New("selected option validation configuration is unavailable")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return servitorv1alpha1.ResolvedOptions{}, nil, errors.New("selected option validation configuration is unavailable")
	}
	config, err := inventory.LoadConfig(data)
	if err != nil {
		return servitorv1alpha1.ResolvedOptions{}, nil, errors.New("selected option validation configuration is invalid")
	}
	target, ok := config.Targets[options.Target]
	if !ok {
		return options, &servitorv1alpha1.PlanRejection{ReasonCode: "target_not_configured", OptionKey: "target"}, nil
	}
	if !containsOption(target.Providers, options.Provider) {
		return options, &servitorv1alpha1.PlanRejection{ReasonCode: "provider_not_supported", OptionKey: "provider"}, nil
	}
	catalog, err := inventory.Discover(ctx, config, options.Target, apiKey, nil)
	if err != nil {
		return servitorv1alpha1.ResolvedOptions{}, nil, inventory.RedactedError(err)
	}
	version, platform, ok := supportedPlanVersion(catalog.Versions, options.Version)
	if !ok {
		return options, &servitorv1alpha1.PlanRejection{ReasonCode: "version_not_supported", OptionKey: "version"}, nil
	}
	if options.Provider == "satellite" && platform != "openshift" {
		return options, &servitorv1alpha1.PlanRejection{ReasonCode: "provider_not_supported", OptionKey: "provider"}, nil
	}
	options.Version = version
	options.Platform = platform
	if options.ResourceGroup != "" && !containsOption(catalog.ResourceGroups, options.ResourceGroup) {
		return options, &servitorv1alpha1.PlanRejection{ReasonCode: "option_not_available", OptionKey: "resource-group"}, nil
	}
	switch options.Provider {
	case "vpc-gen2":
		location, found := planLocation(catalog.VPCLocations, options.Zone)
		if options.Zone != "" && !found {
			return options, &servitorv1alpha1.PlanRejection{ReasonCode: "option_not_available", OptionKey: "zone"}, nil
		}
		if options.Flavor != "" && found && !containsOption(location.Flavors, options.Flavor) {
			return options, &servitorv1alpha1.PlanRejection{ReasonCode: "option_not_available", OptionKey: "flavor"}, nil
		}
	case "classic":
		location, found := planLocation(catalog.ClassicLocations, options.Datacenter)
		if options.Datacenter != "" && !found {
			return options, &servitorv1alpha1.PlanRejection{ReasonCode: "option_not_available", OptionKey: "datacenter"}, nil
		}
		if options.MachineType != "" && found && !containsOption(location.Flavors, options.MachineType) {
			return options, &servitorv1alpha1.PlanRejection{ReasonCode: "option_not_available", OptionKey: "machine-type"}, nil
		}
	case "satellite":
		if options.SatelliteHostProfile != "" && !hasSatelliteProfile(catalog.SatelliteProfile, options.SatelliteZones, options.SatelliteHostProfile) {
			return options, &servitorv1alpha1.PlanRejection{ReasonCode: "option_not_available", OptionKey: "satellite-host-profile"}, nil
		}
	}
	return options, nil, nil
}

func supportedPlanVersion(versions []inventory.Version, requested string) (string, string, bool) {
	platform, err := command.InferPlatform(requested)
	if err != nil {
		return "", "", false
	}
	if command.IsCloudDefault(requested) {
		var defaultVersion string
		for _, version := range versions {
			if version.Supported && version.Platform == platform && version.Default {
				if defaultVersion != "" {
					return "", "", false
				}
				defaultVersion = version.Name
			}
		}
		return defaultVersion, platform, defaultVersion != ""
	}
	requested = streamVersion(requested)
	for _, version := range versions {
		stream := strings.TrimSuffix(version.Name, "_openshift")
		if version.Supported && version.Platform == platform && (version.Name == requested || stream == requested) {
			return version.Name, version.Platform, true
		}
	}
	return "", "", false
}

func streamVersion(version string) string {
	platformSuffix := strings.TrimSuffix(version, "_openshift")
	parts := strings.Split(platformSuffix, ".")
	if len(parts) == 3 {
		platformSuffix = strings.Join(parts[:2], ".")
	}
	return platformSuffix
}

func containsOption(options []string, selected string) bool {
	for _, option := range options {
		if option == selected {
			return true
		}
	}
	return false
}

func planLocation(locations []inventory.Location, selected string) (inventory.Location, bool) {
	for _, location := range locations {
		if location.Name == selected {
			return location, true
		}
	}
	return inventory.Location{}, false
}

func hasSatelliteProfile(profiles []inventory.Profile, zones []string, selected string) bool {
	region := selectedSatelliteRegion(zones)
	for _, profile := range profiles {
		if profile.Region == region && profile.Name == selected {
			return true
		}
	}
	return false
}

func selectedSatelliteRegion(zones []string) string {
	region := ""
	for _, zone := range zones {
		index := strings.LastIndex(zone, "-")
		if index <= 0 {
			return ""
		}
		current := zone[:index]
		if region != "" && region != current {
			return ""
		}
		region = current
	}
	return region
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
	shown, err := (command.Runner{MaxOutput: maxTerraformShowBytes}).Run(ctx, terraformPath, "-chdir="+result.Workspace, "show", "-json")
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

func resolvedOptionsFromValues(options servitorv1alpha1.ResolvedOptions, values servitorv1alpha1.RecoveryValues) (servitorv1alpha1.ResolvedOptions, error) {
	provider := map[string]string{"vpc": "vpc-gen2", "classic": "classic", "satellite": "satellite"}[values.ClusterMode]
	if provider == "" {
		return servitorv1alpha1.ResolvedOptions{}, errors.New("ICT produced an unsupported cluster mode")
	}
	platform, err := command.InferPlatform(values.KubeVersion)
	if err != nil || values.Platform != platform {
		return servitorv1alpha1.ResolvedOptions{}, errors.New("ICT produced a platform inconsistent with its version")
	}
	options.UserOptions = servitorv1alpha1.UserOptions{
		Target:                         options.Target,
		Provider:                       provider,
		Version:                        values.KubeVersion,
		ResourceGroup:                  values.ResourceGroupName,
		Zone:                           values.Zone,
		Flavor:                         values.Flavor,
		VPCID:                          values.VPCID,
		Datacenter:                     values.Datacenter,
		MachineType:                    values.MachineType,
		PublicVLANID:                   values.PublicVLANID,
		PrivateVLANID:                  values.PrivateVLANID,
		SubnetIDs:                      append([]string(nil), values.SubnetIDs...),
		PublicGatewayIDs:               append([]string(nil), values.PublicGatewayIDs...),
		SatelliteZones:                 append([]string(nil), values.SatelliteZones...),
		SatelliteManagedFrom:           values.SatelliteManagedFrom,
		SatelliteLocationID:            values.SatelliteLocationID,
		SatelliteHostImage:             values.SatelliteHostImage,
		SatelliteHostProfile:           values.SatelliteHostProfile,
		SatelliteSSHKeyID:              values.SatelliteSSHKeyID,
		SatelliteWorkerInstanceIDs:     append([]string(nil), values.SatelliteWorkerInstanceIDs...),
		SatelliteWorkerOperatingSystem: values.SatelliteWorkerOperatingSystem,
		WorkerCount:                    values.WorkerCount,
	}
	options.Platform = platform
	options.ClusterName = values.ClusterName
	options.Region = values.Region
	return options, nil
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
	add("--platform", options.Platform)
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

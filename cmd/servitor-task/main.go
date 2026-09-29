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
	"reflect"
	"strconv"
	"strings"
	"time"

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
	Version        int              `json:"version"`
	Operation      string           `json:"operation"`
	Workspace      string           `json:"workspace"`
	AuthCleanup    string           `json:"auth_cleanup,omitempty"`
	Reason         string           `json:"reason,omitempty"`
	CleanupOutcome string           `json:"cleanup_outcome,omitempty"`
	CleanupReason  string           `json:"cleanup_reason,omitempty"`
	CleanupStage   string           `json:"cleanup_stage,omitempty"`
	Certificate    *authCertificate `json:"certificate,omitempty"`
}

type authCertificate struct {
	ID            string `json:"id,omitempty"`
	AllocationUID string `json:"allocation_uid"`
	AttemptID     string `json:"attempt_id"`
}

type authManifest struct {
	Version                      int               `json:"version"`
	Availability                 string            `json:"availability"`
	Mode                         string            `json:"mode,omitempty"`
	Expiry                       string            `json:"expiry,omitempty"`
	Reason                       string            `json:"reason,omitempty"`
	CleanupOutcome               string            `json:"cleanup_outcome,omitempty"`
	CleanupReason                string            `json:"cleanup_reason,omitempty"`
	CleanupStage                 string            `json:"cleanup_stage,omitempty"`
	Certificate                  *authCertificate  `json:"certificate,omitempty"`
	ClearedCertificateReferences []authCertificate `json:"cleared_certificate_references,omitempty"`
	Artifacts                    []struct {
		Name string `json:"name"`
	} `json:"artifacts,omitempty"`
}

const applyTimeout = 105 * time.Minute

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

// ictAuthContext is the backend-free contract consumed only by ICT auth and
// auth-cleanup. It intentionally cannot represent a COS backend or plan.
type ictAuthContext struct {
	Version  int                             `json:"version"`
	StateID  string                          `json:"state_id"`
	Values   servitorv1alpha1.RecoveryValues `json:"values"`
	Recovery struct {
		Endpoints                        map[string]string               `json:"endpoints"`
		Values                           servitorv1alpha1.RecoveryValues `json:"values"`
		SatelliteSSHPublicKeyFingerprint string                          `json:"satellite_ssh_public_key_fingerprint"`
		TFVarsSHA256                     string                          `json:"tfvars_sha256"`
	} `json:"recovery"`
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
	var authManifestFile, authOutputDir, authTmpfsDir, authAttemptID, authCertificateRefsJSON, authPrimaryReason, authCleanupOutcome, authCleanupReason, authCleanupStage, publishAuthSecret, publisherToken, publisherCA, publisherNamespace, authCleanupContext string
	var authFenceCluster, authFenceNamespace, authFenceAttemptID, authFenceRequestTimestamp, authFenceToken, authFenceCA string
	var authEligible, publishAuth bool
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
	flag.StringVar(&authManifestFile, "auth-manifest", "", "private optional public-auth manifest path")
	flag.StringVar(&authOutputDir, "auth-output-dir", "", "private optional public-auth output directory")
	flag.StringVar(&authTmpfsDir, "auth-tmpfs-dir", "", "memory-backed auth Terraform state directory")
	flag.StringVar(&authAttemptID, "auth-attempt-id", "", "persisted auth ownership attempt ID")
	flag.StringVar(&authCertificateRefsJSON, "auth-certificate-refs-json", "[]", "bounded persisted certificate references for destroy reconciliation")
	flag.StringVar(&authPrimaryReason, "auth-primary-reason", "", "persisted bounded auth failure reason for retry cleanup")
	flag.StringVar(&authCleanupOutcome, "auth-cleanup-outcome", "not-required", "persisted bounded auth cleanup outcome for retry")
	flag.StringVar(&authCleanupReason, "auth-cleanup-reason", "", "persisted bounded auth cleanup reason for retry")
	flag.StringVar(&authCleanupStage, "auth-cleanup-stage", "", "persisted bounded auth cleanup stage for retry")
	flag.BoolVar(&authEligible, "auth-eligible", false, "frozen auth acquisition eligibility")
	flag.BoolVar(&publishAuth, "publish-auth", false, "publish optional public auth without failing the operation")
	flag.StringVar(&publishAuthSecret, "publish-auth-secret", "", "allocation-bound Secret to update")
	flag.StringVar(&publisherToken, "publisher-token", "", "projected publisher token path")
	flag.StringVar(&publisherCA, "publisher-ca", "", "projected Kubernetes CA path")
	flag.StringVar(&publisherNamespace, "publisher-namespace", "", "publisher namespace")
	flag.StringVar(&authCleanupContext, "auth-cleanup-context", "", "frozen auth attempt context for publication cleanup")
	flag.StringVar(&authFenceCluster, "auth-fence-cluster", "", "live ServitorCluster name for auth retry fencing")
	flag.StringVar(&authFenceNamespace, "auth-fence-namespace", "", "live ServitorCluster namespace for auth retry fencing")
	flag.StringVar(&authFenceAttemptID, "auth-fence-attempt-id", "", "active auth retry attempt ID for live fencing")
	flag.StringVar(&authFenceRequestTimestamp, "auth-fence-request-timestamp", "", "persisted auth retry request identity for live fencing")
	flag.StringVar(&authFenceToken, "auth-fence-token", "", "projected attempt token for auth retry fencing")
	flag.StringVar(&authFenceCA, "auth-fence-ca", "", "projected Kubernetes CA for auth retry fencing")
	flag.StringVar(&inventoryConfig, "inventory-config", "", "mounted inventory target configuration")
	flag.StringVar(&planningInventoryConfig, "planning-inventory-config", "/etc/servitor/ict/config.yaml", "mounted configuration for planning validation")
	flag.StringVar(&inventoryTarget, "inventory-target", "", "configured inventory target")
	flag.StringVar(&inventoryRunID, "inventory-run-id", "", "inventory run identity")
	flag.StringVar(&inventoryRevision, "inventory-revision", "", "target configuration revision")
	flag.StringVar(&inventoryReport, "inventory-report", "", "task-local inventory report JSON")
	flag.StringVar(&emitInventoryReport, "emit-inventory-report", "", "emit one validated task-local inventory report JSON document")
	flag.StringVar(&apiKeyEnv, "ibm-api-key-env", "IC_API_KEY", "environment variable containing the IBM API key")
	flag.StringVar(&ictPath, "ict", trustedICTExecutable, "trusted ICT executable")
	flag.StringVar(&terraformPath, "terraform", "terraform", "Terraform executable")
	flag.Parse()
	if publishAuth {
		options, err := readJSON[servitorv1alpha1.ResolvedOptions](optionsFile)
		if err == nil {
			var fences []authRetryFence
			if authFenceRequestTimestamp != "" {
				fences = append(fences, authRetryFence{Namespace: authFenceNamespace, Name: authFenceCluster, UID: uid, Operation: operation, AttemptID: authFenceAttemptID, RequestTimestamp: authFenceRequestTimestamp, TokenPath: authFenceToken, CAPath: authFenceCA})
			}
			err = publishPublicAuthWithAttempt(authEligible, options, publishAuthSecret, uid, operation, authAttemptID, authManifestFile, authOutputDir, reportFile, publisherNamespace, publisherToken, publisherCA, ictPath, authCleanupContext, fences...)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "servitor-task:", err)
			os.Exit(1)
		}
		return
	}
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
		err = run(context.Background(), uid, operation, kind, optionsFile, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath, planningInventoryConfig, os.Getenv(apiKeyEnv), authEligible, authManifestFile, authOutputDir, authTmpfsDir, authAttemptID, authPrimaryReason, authCleanupOutcome, authCleanupReason, authCleanupStage, authRetryFence{Namespace: authFenceNamespace, Name: authFenceCluster, UID: uid, Operation: operation, AttemptID: authFenceAttemptID, RequestTimestamp: authFenceRequestTimestamp, TokenPath: authFenceToken, CAPath: authFenceCA}, authCertificateRefsJSON)
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

func run(ctx context.Context, uid, operation, kind, optionsFile, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath, planningInventoryConfig, apiKey string, publicAuthEligible bool, authManifestFile, authOutputDir, authTmpfsDir, authAttemptID, authPrimaryReason, authCleanupOutcome, authCleanupReason, authCleanupStage string, fence authRetryFence, authCertificateRefsJSON ...string) error {
	if uid == "" || operation == "" || (kind != "plan" && kind != "apply" && kind != "destroy" && kind != "auth-retry") {
		return errors.New("cluster UID, operation ID, and operation kind are required")
	}
	paths := []string{optionsFile, backendFile, resultFile, reportFile}
	if kind == "apply" || kind == "destroy" || kind == "auth-retry" {
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
		var authTmpfs []string
		if authTmpfsDir != "" {
			authTmpfs = []string{authTmpfsDir}
		}
		return runApplyWithAttempt(ctx, uid, operation, options, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath, publicAuthEligible, authManifestFile, authOutputDir, authAttemptID, authTmpfs...)
	}
	if kind == "auth-retry" {
		if !fence.valid() {
			return errors.New("auth retry fence inputs are required")
		}
		references, err := authCertificateReferences(authCertificateRefsJSON...)
		if err != nil {
			return err
		}
		return runAuthRetryWithAttempt(ctx, uid, operation, options, recoveryFile, resultFile, reportFile, ictPath, authManifestFile, authOutputDir, authTmpfsDir, authAttemptID, authPrimaryReason, authCleanupOutcome, authCleanupReason, authCleanupStage, fence, references)
	}
	refs, err := authCertificateReferences(authCertificateRefsJSON...)
	if err != nil {
		return err
	}
	return runDestroy(ctx, uid, operation, options, backendFile, recoveryFile, resultFile, reportFile, ictPath, refs)
}

func authCertificateReferences(values ...string) ([]servitorv1alpha1.AuthCertificateReference, error) {
	if len(values) == 0 || values[0] == "" {
		return nil, nil
	}
	if len(values) != 1 || len(values[0]) > 8*1024 {
		return nil, errors.New("auth certificate references are invalid")
	}
	var references []servitorv1alpha1.AuthCertificateReference
	if err := json.Unmarshal([]byte(values[0]), &references); err != nil || len(references) > servitorv1alpha1.MaxAuthCertificateReferences {
		return nil, errors.New("auth certificate references are invalid")
	}
	seen := make(map[servitorv1alpha1.AuthCertificateReference]struct{}, len(references))
	result := make([]servitorv1alpha1.AuthCertificateReference, 0, len(references))
	for _, reference := range references {
		if len(reference.ID) > 256 || reference.AllocationUID == "" || len(reference.AllocationUID) > 128 || reference.AttemptID == "" || len(reference.AttemptID) > 128 {
			return nil, errors.New("auth certificate references are invalid")
		}
		if _, duplicate := seen[reference]; duplicate {
			continue
		}
		seen[reference] = struct{}{}
		result = append(result, reference)
	}
	return result, nil
}

func runPlan(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, backendFile, resultFile, reportFile, ictPath, terraformPath, planningInventoryConfig, apiKey string) error {
	if err := validateFrozenNetwork(options); err != nil {
		return err
	}
	if options.Provider == "satellite" {
		return writeReport(reportFile, pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, PlanRejection: &servitorv1alpha1.PlanRejection{ReasonCode: "provider_not_supported", OptionKey: "provider"}})
	}
	if err := validateHeadlamp(options); err != nil {
		return err
	}
	options, rejection, err := validatePlanOptions(ctx, planningInventoryConfig, apiKey, options)
	if err != nil {
		return err
	}
	if rejection != nil {
		return writeReport(reportFile, pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, PlanRejection: rejection})
	}
	expectedEndpoints, err := frozenRecoveryEndpoints(planningInventoryConfig, options)
	if err != nil {
		return err
	}
	platform, err := command.InferPlatform(options.Version)
	if err != nil || options.Platform != platform {
		return errors.New("resolved platform does not match version")
	}
	args := append([]string{"plan", operation, "--backend-config", backendFile, "--result-file", resultFile, "--prefix", "servitor"}, optionArgs(options)...)
	if _, err := runICT(ctx, ictPath, 64*1024, os.Stdout, os.Stderr, os.Stderr, args...); err != nil {
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
	if err := rejectManagedNetworkPlan(plan); err != nil {
		return err
	}
	options, err = resolvedOptionsFromValues(options, result.Values)
	if err != nil {
		return err
	}
	if err := validateRecoveredNetwork(options, result.Values); err != nil {
		return err
	}
	recoveryOptions, err := resolvedOptionsFromValues(options, result.Recovery.Values)
	if err != nil {
		return fmt.Errorf("ICT produced invalid recovery values: %w", err)
	}
	if recoveryOptions.Version != options.Version || recoveryOptions.Platform != options.Platform || recoveryOptions.Headlamp != options.Headlamp {
		return errors.New("ICT recovery values are inconsistent with planning result")
	}
	if err := validateRecoveredNetwork(options, result.Recovery.Values); err != nil {
		return err
	}
	if err := validateFrozenRecoveryEndpoints(options.Target, expectedEndpoints, result.Recovery.Target, result.Recovery.Endpoints); err != nil {
		return err
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

func frozenRecoveryEndpoints(configPath string, options servitorv1alpha1.ResolvedOptions) (map[string]string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, errors.New("selected endpoint configuration is unavailable")
	}
	configured, err := inventory.LoadConfig(data)
	if err != nil {
		return nil, errors.New("selected endpoint configuration is invalid")
	}
	target, found := configured.Targets[options.Target]
	if !found {
		return nil, errors.New("frozen target is not configured")
	}
	endpoints := map[string]string{
		"IAM":                target.Endpoints.IAM,
		"ContainerService":   target.Endpoints.ContainerService,
		"GlobalTagging":      target.Endpoints.GlobalTagging,
		"ResourceManagement": target.Endpoints.ResourceManagement,
		"ResourceController": target.Endpoints.ResourceController,
	}
	switch options.Provider {
	case "vpc-gen2":
		endpoints["VPC"] = strings.ReplaceAll(target.Endpoints.VPC, "{region}", options.Network.VPCRegion)
	case "satellite":
		endpoints["VPC"] = strings.ReplaceAll(target.Endpoints.VPC, "{region}", options.Region)
		endpoints["Satellite"] = target.Endpoints.Satellite
		endpoints["SatelliteConfig"] = target.Endpoints.SatelliteConfig
	}
	return endpoints, nil
}

func validateFrozenRecoveryEndpoints(target string, expected map[string]string, actualTarget string, actual map[string]string) error {
	if actualTarget != target {
		return errors.New("ICT recovery target does not match frozen target")
	}
	if !reflect.DeepEqual(actual, expected) {
		return errors.New("ICT recovery endpoints do not match frozen target configuration")
	}
	return nil
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

func runApply(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath string, publicAuthEligible bool, authManifestFile, authOutputDir string, authTmpfsDirs ...string) error {
	return runApplyWithAttempt(ctx, uid, operation, options, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath, publicAuthEligible, authManifestFile, authOutputDir, operation, authTmpfsDirs...)
}

func runApplyWithAttempt(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, backendFile, recoveryFile, resultFile, reportFile, ictPath, terraformPath string, publicAuthEligible bool, authManifestFile, authOutputDir, authAttemptID string, authTmpfsDirs ...string) error {
	if err := validateFrozenNetwork(options); err != nil {
		return err
	}
	if options.Provider == "satellite" {
		return errors.New("Satellite provisioning is not supported")
	}
	if err := validateHeadlamp(options); err != nil {
		return err
	}
	recovery, contextFile, err := frozenContext(operation, backendFile, recoveryFile, resultFile)
	if err != nil {
		return err
	}
	if err := recovery.Validate(); err != nil {
		return errors.New("frozen recovery metadata is invalid")
	}
	if err := validateRecoveredNetwork(options, recovery.Values); err != nil {
		return err
	}
	if publicAuthEligible && (!filepath.IsAbs(authManifestFile) || !filepath.IsAbs(authOutputDir)) {
		return errors.New("public auth paths must be absolute")
	}
	applyCtx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()
	freshPlanFile := filepath.Join(filepath.Dir(resultFile), "fresh-plan.json")
	reviewArgs := []string{"review", operation, "--context-file", contextFile, "--backend-config", backendFile, "--result-file", freshPlanFile}
	review, err := runICT(applyCtx, ictPath, 64*1024, os.Stdout, os.Stderr, os.Stderr, reviewArgs...)
	if err != nil {
		return err
	}
	if review.StdoutTruncated || review.StderrTruncated {
		return errors.New("ICT fresh plan review output exceeded limit")
	}
	fresh, err := readJSON[ictPlanResult](freshPlanFile)
	if err != nil || fresh.Version != 1 || fresh.StateID != operation || !filepath.IsAbs(fresh.PlanPath) {
		return errors.New("ICT produced no valid fresh planning result")
	}
	if err := validateRecoveredNetwork(options, fresh.Values); err != nil {
		return err
	}
	if err := validateRecoveredNetwork(options, fresh.Recovery.Values); err != nil {
		return err
	}
	workspace := filepath.Dir(filepath.Dir(fresh.PlanPath))
	planPath := filepath.Join(filepath.Base(filepath.Dir(fresh.PlanPath)), filepath.Base(fresh.PlanPath))
	shown, err := (command.Runner{MaxOutput: maxTerraformShowBytes}).Run(applyCtx, terraformPath, "-chdir="+workspace, "show", "-json", planPath)
	if err != nil || shown.StdoutTruncated {
		return errors.New("cannot obtain bounded Terraform fresh plan review")
	}
	plan, err := terraformview.ParsePlan([]byte(shown.Stdout))
	if err != nil {
		return fmt.Errorf("sanitize Terraform fresh plan: %w", err)
	}
	if err := rejectManagedNetworkPlan(plan); err != nil {
		return err
	}
	args := []string{"apply", operation, "--context-file", contextFile, "--backend-config", backendFile, "--result-file", resultFile, "--auto-approve"}
	if publicAuthEligible {
		args = append(args, "--auth-manifest-file", authManifestFile, "--auth-output-dir", authOutputDir)
	}
	if _, err := runICT(applyCtx, ictPath, 64*1024, os.Stdout, os.Stderr, os.Stderr, args...); err != nil {
		return err
	}
	result, err := readJSON[ictOperationResult](resultFile)
	if err != nil || result.Version != 1 || result.Operation != "apply" || !filepath.IsAbs(result.Workspace) {
		return errors.New("ICT produced no valid apply result")
	}
	shown, err = (command.Runner{MaxOutput: maxTerraformShowBytes}).Run(applyCtx, terraformPath, "-chdir="+result.Workspace, "show", "-json")
	if err != nil || shown.StdoutTruncated {
		return errors.New("cannot obtain bounded Terraform ready summary")
	}
	state, err := terraformview.ParseState([]byte(shown.Stdout))
	if err != nil {
		return fmt.Errorf("sanitize Terraform state: %w", err)
	}
	if err := rejectManagedNetworkResources(state.Resources); err != nil {
		return err
	}
	report := pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: options, Recovery: recovery, Ready: summaryFromState(state)}
	if err := writeReport(reportFile, report); err != nil {
		return err
	}
	if !publicAuthEligible {
		return nil
	}
	// Unit callers that omit the new mounted tmpfs retain the pre-auth
	// infrastructure seam. Production always supplies exactly one mount.
	if len(authTmpfsDirs) == 0 {
		return nil
	}
	if len(authTmpfsDirs) != 1 || !filepath.IsAbs(authTmpfsDirs[0]) {
		return writeUnavailableAuthManifest(authManifestFile, "auth-state-failure")
	}
	authContextFile, err := authContext(operation, recovery, resultFile)
	if err != nil {
		return err
	}
	// Infrastructure is already reported. Every expected auth failure is
	// converted to a bounded manifest so publish can retain Ready.
	attempt, err := authAttemptFor(uid, authAttemptID)
	if err != nil {
		return err
	}
	if _, err := runICT(ctx, ictPath, 64*1024, io.Discard, io.Discard, nil, "auth", operation, "--context-file", authContextFile, "--result-file", resultFile, "--auth-manifest-file", authManifestFile, "--auth-output-dir", authOutputDir, "--auth-tmpfs-dir", authTmpfsDirs[0], "--auth-allocation-uid", attempt.AllocationUID, "--auth-attempt-id", attempt.AttemptID); err != nil {
		return cleanupAfterAuthFailure(ctx, ictPath, operation, authContextFile, resultFile, authManifestFile, attempt)
	}
	if _, err := readJSON[authManifest](authManifestFile); err != nil {
		return cleanupAfterAuthFailure(ctx, ictPath, operation, authContextFile, resultFile, authManifestFile, attempt)
	}
	return nil
}

// runAuthRetry deliberately invokes only ICT auth. It neither initializes nor
// supplies a Terraform backend and reports a bounded unavailable result.
func runAuthRetry(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, recoveryFile, resultFile, reportFile, ictPath, authManifestFile, authOutputDir, authTmpfsDir, primaryReason string, fence authRetryFence, persistedReferences ...[]servitorv1alpha1.AuthCertificateReference) error {
	return runAuthRetryWithAttempt(ctx, uid, operation, options, recoveryFile, resultFile, reportFile, ictPath, authManifestFile, authOutputDir, authTmpfsDir, operation, primaryReason, "not-required", "", "", fence, persistedReferences...)
}

func runAuthRetryWithAttempt(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, recoveryFile, resultFile, reportFile, ictPath, authManifestFile, authOutputDir, authTmpfsDir, authAttemptID, primaryReason, priorCleanupOutcome, priorCleanupReason, priorCleanupStage string, fence authRetryFence, persistedReferences ...[]servitorv1alpha1.AuthCertificateReference) error {
	if !filepath.IsAbs(authManifestFile) || !filepath.IsAbs(authOutputDir) || !filepath.IsAbs(authTmpfsDir) {
		return errors.New("auth retry paths are invalid")
	}
	recovery, err := readJSON[servitorv1alpha1.RecoveryMetadata](recoveryFile)
	if err != nil || recovery.Validate() != nil || !validateRecoveredNetworkNoError(options, recovery.Values) {
		return errors.New("frozen auth retry context is invalid")
	}
	// ICT's auth command validates a backend-free frozen handoff. The runtime
	// attempt is passed separately and never changes policy.
	contextFile, err := authContext(operation, recovery, resultFile)
	if err != nil {
		return err
	}
	attempt, err := authAttemptFor(uid, authAttemptID)
	if err != nil {
		return err
	}
	report := pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: options, Recovery: recovery}
	if err := writeReport(reportFile, report); err != nil {
		return errors.New("persist infrastructure report")
	}
	if err := checkAuthRetryFence(ctx, fence); err != nil {
		return writeUnavailableAuthManifestWithCleanup(authManifestFile, "auth-retry-fenced", "not-required", "", "", nil)
	}
	if !servitorv1alpha1.ValidAuthFailureReason(primaryReason) {
		primaryReason = "auth-state-failure"
	}
	priorCleanupOutcome, priorCleanupReason, priorCleanupStage, err = authRetryCleanupContext(priorCleanupOutcome, priorCleanupReason, priorCleanupStage)
	if err != nil {
		return err
	}
	references, err := authRetryCleanupReferences(uid, attempt.AttemptID, persistedReferences...)
	if err != nil {
		return err
	}
	if cleanup, err := precleanAuthRetry(ctx, ictPath, operation, contextFile, resultFile, references); err != nil {
		return err
	} else if cleanup.AuthCleanup == "pending" {
		certificate := cleanup.Certificate
		if certificate == nil {
			certificate = authCertificateFromAttempt(attempt)
		}
		return writeUnavailableAuthManifestWithCleanup(authManifestFile, primaryReason, "pending", updatedAuthRetryCleanupReason(priorCleanupOutcome, priorCleanupReason, cleanup.CleanupReason), updatedAuthRetryCleanupStage(priorCleanupOutcome, priorCleanupStage, cleanup.CleanupStage), certificate)
	}
	// A successful preclean resolves all prior ownership, so no prior cleanup
	// state is carried into the new auth generation.
	cleared := references[1:]
	if err := checkAuthRetryFence(ctx, fence); err != nil {
		if err := writeUnavailableAuthManifestWithCleanup(authManifestFile, "auth-retry-fenced", "not-required", "", "", nil); err != nil {
			return err
		}
		return recordClearedCertificateReferences(authManifestFile, cleared)
	}
	if _, err := runICT(ctx, ictPath, 64*1024, io.Discard, io.Discard, nil, "auth", operation, "--context-file", contextFile, "--result-file", resultFile, "--auth-manifest-file", authManifestFile, "--auth-output-dir", authOutputDir, "--auth-tmpfs-dir", authTmpfsDir, "--auth-allocation-uid", attempt.AllocationUID, "--auth-attempt-id", attempt.AttemptID); err != nil {
		if err := cleanupAfterAuthFailure(ctx, ictPath, operation, contextFile, resultFile, authManifestFile, attempt); err != nil {
			return err
		}
		return recordClearedCertificateReferences(authManifestFile, cleared)
	}
	if _, err := readJSON[authManifest](authManifestFile); err != nil {
		if err := cleanupAfterAuthFailure(ctx, ictPath, operation, contextFile, resultFile, authManifestFile, attempt); err != nil {
			return err
		}
		return recordClearedCertificateReferences(authManifestFile, cleared)
	}
	if err := checkAuthRetryFence(ctx, fence); err != nil {
		if err := fenceAuthRetryAfterIssuance(ctx, ictPath, operation, contextFile, resultFile, authManifestFile, attempt); err != nil {
			return err
		}
	}
	return recordClearedCertificateReferences(authManifestFile, cleared)
}

// authRetryCleanupReferences includes the allocation tuple even when no
// certificate ID was retained. ICT verifies each tuple before auth can issue.
func authRetryCleanupContext(outcome, reason, stage string) (string, string, string, error) {
	if outcome == "" {
		return "not-required", "", "", nil
	}
	if !validAuthCleanup(outcome, reason, stage) {
		return "", "", "", errors.New("auth retry cleanup context is invalid")
	}
	return outcome, reason, stage, nil
}

func validAuthCleanup(outcome, reason, stage string) bool {
	if !servitorv1alpha1.ValidAuthCleanupOutcome(outcome) || stage != "" && !servitorv1alpha1.ValidAuthCleanupStage(stage) {
		return false
	}
	if outcome == "pending" {
		return servitorv1alpha1.ValidAuthCleanupReason(reason)
	}
	return reason == "" && stage == ""
}

func updatedAuthRetryCleanupReason(priorOutcome, priorReason, currentReason string) string {
	if servitorv1alpha1.ValidAuthCleanupReason(currentReason) {
		return currentReason
	}
	if priorOutcome == "pending" && servitorv1alpha1.ValidAuthCleanupReason(priorReason) {
		return priorReason
	}
	return "unknown"
}

func updatedAuthRetryCleanupStage(priorOutcome, priorStage, currentStage string) string {
	if servitorv1alpha1.ValidAuthCleanupStage(currentStage) {
		return currentStage
	}
	if priorOutcome == "pending" && servitorv1alpha1.ValidAuthCleanupStage(priorStage) {
		return priorStage
	}
	return ""
}

func authRetryCleanupReferences(uid, attemptID string, persisted ...[]servitorv1alpha1.AuthCertificateReference) ([]servitorv1alpha1.AuthCertificateReference, error) {
	if uid == "" || attemptID == "" || len(uid) > 128 || len(attemptID) > 128 {
		return nil, errors.New("auth certificate reference ownership is invalid")
	}
	references := []servitorv1alpha1.AuthCertificateReference{{AllocationUID: uid, AttemptID: attemptID}}
	seen := map[servitorv1alpha1.AuthCertificateReference]struct{}{references[0]: {}}
	for _, batch := range persisted {
		for _, reference := range batch {
			if reference.AllocationUID != uid || reference.AttemptID == "" || len(reference.ID) > 256 || len(reference.AttemptID) > 128 {
				return nil, errors.New("auth certificate reference ownership is invalid")
			}
			if _, duplicate := seen[reference]; duplicate {
				continue
			}
			seen[reference] = struct{}{}
			references = append(references, reference)
		}
	}
	if len(references) > servitorv1alpha1.MaxAuthCertificateReferences+1 {
		return nil, errors.New("auth certificate references exceed the bound")
	}
	return references, nil
}

// precleanAuthRetry runs every ownership-bound cleanup before ICT auth. A
// failed or nonterminal cleanup is a bounded unavailable result, never a
// reason to issue another certificate.
func precleanAuthRetry(ctx context.Context, ictPath, operation, contextFile, resultFile string, references []servitorv1alpha1.AuthCertificateReference) (ictOperationResult, error) {
	var pending *ictOperationResult
	for index, certificate := range references {
		cleanupResult := filepath.Join(filepath.Dir(resultFile), fmt.Sprintf("auth-retry-preclean-%d.json", index))
		args := []string{"auth-cleanup", operation, "--context-file", contextFile, "--result-file", cleanupResult, "--certificate-allocation-uid", certificate.AllocationUID}
		if certificate.ID != "" {
			args = append(args, "--certificate-id", certificate.ID)
		}
		if certificate.AttemptID != "" {
			args = append(args, "--certificate-attempt-id", certificate.AttemptID)
		}
		cleanupCtx, cancel := context.WithTimeout(ctx, time.Minute)
		_, cleanupErr := runICT(cleanupCtx, ictPath, 64*1024, io.Discard, io.Discard, nil, args...)
		cancel()
		cleanup, readErr := readJSON[ictOperationResult](cleanupResult)
		if cleanupErr != nil || readErr != nil || !authCleanupSucceeded(cleanup) {
			result := ictOperationResult{Version: 1, Operation: "auth-cleanup", AuthCleanup: "pending", CleanupReason: cleanupFailureReason(cleanup, cleanupErr, readErr), CleanupStage: cleanupFailureStage(cleanup), Certificate: cleanup.Certificate}
			if pending == nil {
				pending = &result
			}
		}
	}
	if pending != nil {
		return *pending, nil
	}
	return ictOperationResult{Version: 1, Operation: "auth-cleanup", AuthCleanup: "cleaned"}, nil
}

func authCleanupSucceeded(result ictOperationResult) bool {
	return result.Version == 1 && result.Operation == "auth-cleanup" && (result.AuthCleanup == "cleaned" || result.AuthCleanup == "not-found")
}

func recordClearedCertificateReferences(path string, references []servitorv1alpha1.AuthCertificateReference) error {
	if len(references) == 0 {
		return nil
	}
	manifest, err := readJSON[authManifest](path)
	if err != nil {
		return err
	}
	manifest.ClearedCertificateReferences = make([]authCertificate, 0, len(references))
	for _, reference := range references {
		manifest.ClearedCertificateReferences = append(manifest.ClearedCertificateReferences, authCertificate{ID: reference.ID, AllocationUID: reference.AllocationUID, AttemptID: reference.AttemptID})
	}
	return writeJSON(path, manifest)
}

func validateRecoveredNetworkNoError(options servitorv1alpha1.ResolvedOptions, values servitorv1alpha1.RecoveryValues) bool {
	return validateRecoveredNetwork(options, values) == nil
}

func authContext(operation string, recovery servitorv1alpha1.RecoveryMetadata, resultFile string) (string, error) {
	contextFile := filepath.Join(filepath.Dir(resultFile), "auth-context.json")
	handoff := ictAuthContext{Version: 1, StateID: operation, Values: recovery.Values}
	handoff.Recovery.Endpoints = recovery.Endpoints
	handoff.Recovery.Values = recovery.Values
	handoff.Recovery.SatelliteSSHPublicKeyFingerprint = recovery.SatelliteSSHPublicKeyFingerprint
	handoff.Recovery.TFVarsSHA256 = recovery.TFVarsSHA256
	if err := writeJSON(contextFile, handoff); err != nil {
		return "", errors.New("write auth context")
	}
	return contextFile, nil
}

func writeUnavailableAuthManifest(path, reason string) error {
	return writeUnavailableAuthManifestWithCleanup(path, reason, "not-required", "", "", nil)
}

// cleanupAfterAuthFailure reconciles by ownership even if ICT failed before it
// could identify an issued certificate or write a manifest.
func cleanupAfterAuthFailure(ctx context.Context, ictPath, operation, contextFile, resultFile, manifestPath string, attempt servitorv1alpha1.AuthAttempt) error {
	primaryReason := "auth-state-failure"
	if prior, err := readJSON[authManifest](manifestPath); err == nil && servitorv1alpha1.ValidAuthFailureReason(prior.Reason) && prior.Reason != "certificate-cleanup-pending" {
		primaryReason = prior.Reason
	}
	fallback := authCertificateFromAttempt(attempt)
	cleanupResult := filepath.Join(filepath.Dir(resultFile), "auth-failure-cleanup.json")
	cleanupCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, cleanupErr := runICT(cleanupCtx, ictPath, 64*1024, io.Discard, io.Discard, nil, "auth-cleanup", operation, "--context-file", contextFile, "--result-file", cleanupResult, "--certificate-allocation-uid", attempt.AllocationUID, "--certificate-attempt-id", attempt.AttemptID)
	cleanup, readErr := readJSON[ictOperationResult](cleanupResult)
	if cleanupErr == nil && readErr == nil && cleanup.Version == 1 && cleanup.Operation == "auth-cleanup" && authCleanupSucceeded(cleanup) {
		return writeUnavailableAuthManifestWithCleanup(manifestPath, primaryReason, "cleaned", "", "", nil)
	}
	if cleanup.Certificate != nil {
		fallback = cleanup.Certificate
	}
	return writeUnavailableAuthManifestWithCleanup(manifestPath, primaryReason, "pending", cleanupFailureReason(cleanup, cleanupErr, readErr), cleanupFailureStage(cleanup), fallback)
}

func authCertificateFromAttempt(attempt servitorv1alpha1.AuthAttempt) *authCertificate {
	if attempt.AllocationUID == "" || attempt.AttemptID == "" {
		return nil
	}
	return &authCertificate{AllocationUID: attempt.AllocationUID, AttemptID: attempt.AttemptID}
}

func authAttemptFor(uid, attemptID string) (servitorv1alpha1.AuthAttempt, error) {
	if uid == "" || attemptID == "" || len(uid) > 128 || len(attemptID) > 128 {
		return servitorv1alpha1.AuthAttempt{}, errors.New("frozen auth attempt is invalid")
	}
	return servitorv1alpha1.AuthAttempt{AllocationUID: uid, AttemptID: attemptID}, nil
}

func writeUnavailableAuthManifestWithCleanup(path, reason, cleanupOutcome, cleanupReason, cleanupStage string, certificate *authCertificate) error {
	if !servitorv1alpha1.ValidAuthFailureReason(reason) || !validAuthCleanup(cleanupOutcome, cleanupReason, cleanupStage) {
		return errors.New("invalid auth failure cleanup")
	}
	if cleanupOutcome == "pending" && certificate == nil {
		return errors.New("pending auth cleanup requires ownership")
	}
	return writeJSON(path, authManifest{Version: 1, Availability: "unavailable", Reason: reason, CleanupOutcome: cleanupOutcome, CleanupReason: cleanupReason, CleanupStage: cleanupStage, Certificate: certificate})
}

func cleanupFailureReason(result ictOperationResult, commandErr, readErr error) string {
	if result.Version == 1 && result.Operation == "auth-cleanup" && servitorv1alpha1.ValidAuthCleanupReason(result.CleanupReason) {
		return result.CleanupReason
	}
	if commandErr != nil {
		return "transport"
	}
	if readErr != nil {
		return "unknown"
	}
	return "unknown"
}

func cleanupFailureStage(result ictOperationResult) string {
	if result.Version == 1 && result.Operation == "auth-cleanup" && servitorv1alpha1.ValidAuthCleanupStage(result.CleanupStage) {
		return result.CleanupStage
	}
	return ""
}

// fenceAuthRetryAfterIssuance removes any newly issued allocation certificate
// before reporting the terminal fence outcome. ICT verifies ownership again,
// including when the manifest was not written completely.
func fenceAuthRetryAfterIssuance(ctx context.Context, ictPath, operation, contextFile, resultFile, manifestPath string, attempt servitorv1alpha1.AuthAttempt) error {
	manifest, _ := readJSON[authManifest](manifestPath)
	cleanupResult := filepath.Join(filepath.Dir(resultFile), "auth-retry-fence-cleanup.json")
	args := []string{"auth-cleanup", operation, "--context-file", contextFile, "--result-file", cleanupResult}
	if manifest.Certificate != nil && manifest.Certificate.ID != "" {
		args = append(args, "--certificate-id", manifest.Certificate.ID)
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	args = append(args, "--certificate-allocation-uid", attempt.AllocationUID, "--certificate-attempt-id", attempt.AttemptID)
	_, cleanupErr := runICT(cleanupCtx, ictPath, 64*1024, io.Discard, io.Discard, nil, args...)
	cleanup, readErr := readJSON[ictOperationResult](cleanupResult)
	if cleanupErr != nil || readErr != nil || !authCleanupSucceeded(cleanup) {
		certificate := manifest.Certificate
		if cleanup.Certificate != nil {
			certificate = cleanup.Certificate
		}
		if certificate == nil {
			certificate = authCertificateFromAttempt(attempt)
		}
		return writeUnavailableAuthManifestWithCleanup(manifestPath, "auth-retry-fenced", "pending", cleanupFailureReason(cleanup, cleanupErr, readErr), cleanupFailureStage(cleanup), certificate)
	}
	return writeUnavailableAuthManifestWithCleanup(manifestPath, "auth-retry-fenced", "cleaned", "", "", nil)
}

func runDestroy(ctx context.Context, uid, operation string, options servitorv1alpha1.ResolvedOptions, backendFile, recoveryFile, resultFile, reportFile, ictPath string, references ...[]servitorv1alpha1.AuthCertificateReference) error {
	if err := validateHeadlamp(options); err != nil {
		return err
	}
	recovery, contextFile, err := frozenContext(operation, backendFile, recoveryFile, resultFile)
	if err != nil {
		return err
	}
	if recovery.Values.Headlamp != options.Headlamp {
		return errors.New("frozen recovery Headlamp selection does not match the request")
	}
	if recovery.Values.AuthPolicy != nil {
		authContextFile, err := authContext(operation, recovery, resultFile)
		if err != nil {
			return err
		}
		cleanupReferences := []servitorv1alpha1.AuthCertificateReference{{AllocationUID: uid}}
		seen := map[servitorv1alpha1.AuthCertificateReference]struct{}{cleanupReferences[0]: {}}
		for _, batch := range references {
			for _, certificate := range batch {
				if certificate.AllocationUID != uid {
					return errors.New("auth certificate reference ownership is invalid")
				}
				if _, duplicate := seen[certificate]; duplicate {
					continue
				}
				seen[certificate] = struct{}{}
				cleanupReferences = append(cleanupReferences, certificate)
			}
		}
		for index, certificate := range cleanupReferences {
			cleanupResult := filepath.Join(filepath.Dir(resultFile), fmt.Sprintf("auth-cleanup-%d.json", index))
			args := []string{"auth-cleanup", operation, "--context-file", authContextFile, "--result-file", cleanupResult, "--certificate-allocation-uid", certificate.AllocationUID}
			if certificate.ID != "" {
				args = append(args, "--certificate-id", certificate.ID)
			}
			if certificate.AttemptID != "" {
				args = append(args, "--certificate-attempt-id", certificate.AttemptID)
			}
			if _, err := runICT(ctx, ictPath, 64*1024, io.Discard, io.Discard, nil, args...); err != nil {
				return errors.New("allocation certificate cleanup failed")
			}
			cleanup, err := readJSON[ictOperationResult](cleanupResult)
			if err != nil || !authCleanupSucceeded(cleanup) {
				return errors.New("allocation certificate cleanup is pending")
			}
		}
	}
	if _, err := runICT(ctx, ictPath, 64*1024, os.Stdout, os.Stderr, os.Stderr, "destroy", operation, "--context-file", contextFile, "--backend-config", backendFile, "--result-file", resultFile); err != nil {
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
	return replaceReportFile(path, data)
}

func replaceReportFile(path string, data []byte) error {
	return replaceReportFileWithRename(path, data, os.Rename)
}

func replaceReportFileWithRename(path string, data []byte, rename func(string, string) error) (err error) {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".report-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(temporary.Name())
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err = temporary.Write(data); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	return rename(temporary.Name(), path)
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

func validateFrozenNetwork(options servitorv1alpha1.ResolvedOptions) error {
	if options.Provider != "vpc-gen2" {
		if options.Provider == "classic" && options.Network != (servitorv1alpha1.FrozenNetwork{}) {
			return errors.New("Classic options must not contain VPC networking")
		}
		return nil
	}
	network := options.Network
	if network.BindingID == "" || network.AccountID == "" || network.VPCRegion == "" || network.VPCID == "" || network.SubnetID == "" || network.PublicGatewayID == "" || network.Zone == "" || options.Zone != network.Zone || options.Region != "" && options.Region != network.VPCRegion {
		return errors.New("VPC options require one frozen existing network binding")
	}
	return nil
}

func validateRecoveredNetwork(options servitorv1alpha1.ResolvedOptions, values servitorv1alpha1.RecoveryValues) error {
	if options.Headlamp != values.Headlamp {
		return errors.New("ICT Headlamp selection does not match the frozen request")
	}
	if options.Provider != "vpc-gen2" {
		return nil
	}
	network := options.Network
	if values.AccountID != network.AccountID || values.VPCRegion != network.VPCRegion || values.VPCID != network.VPCID || values.Zone != network.Zone || len(values.SubnetIDs) != 1 || values.SubnetIDs[0] != network.SubnetID || len(values.PublicGatewayIDs) != 1 || values.PublicGatewayIDs[0] != network.PublicGatewayID || !sameFrozenAuthPolicy(values.AuthPolicy, network.AuthPolicy) {
		return errors.New("ICT network values do not match the frozen existing network binding")
	}
	return nil
}

func sameFrozenAuthPolicy(left, right *servitorv1alpha1.FrozenAuthPolicy) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.VPNServerID == right.VPNServerID &&
		left.SecretsManagerID == right.SecretsManagerID &&
		left.SecretsManagerRegion == right.SecretsManagerRegion &&
		left.SecretGroupID == right.SecretGroupID &&
		left.CertificateTemplate == right.CertificateTemplate &&
		left.Issuer == right.Issuer &&
		left.TTL == right.TTL
}

func rejectManagedNetworkPlan(plan terraformview.Plan) error {
	return rejectManagedNetworkResources(plan.Resources)
}

func rejectManagedNetworkResources(resources []terraformview.Resource) error {
	for _, resource := range resources {
		if resource.Mode != "managed" {
			continue
		}
		switch resource.Type {
		case "ibm_is_vpc", "ibm_is_subnet", "ibm_is_public_gateway", "ibm_is_public_gateways", "ibm_is_subnet_public_gateway_attachment":
			return errors.New("Terraform plan attempts to manage shared VPC networking")
		}
	}
	return nil
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
	if options.ResourceGroup != "" && options.ResourceGroup != values.ResourceGroupName {
		return servitorv1alpha1.ResolvedOptions{}, errors.New("ICT recovery resource group does not match frozen configured default")
	}
	if options.PrivateOnly != values.PrivateOnly {
		return servitorv1alpha1.ResolvedOptions{}, errors.New("ICT recovery private-only policy does not match frozen request")
	}
	if options.Headlamp != values.Headlamp {
		return servitorv1alpha1.ResolvedOptions{}, errors.New("ICT recovery Headlamp selection does not match frozen request")
	}
	options.UserOptions = servitorv1alpha1.UserOptions{
		Target:                         options.Target,
		Provider:                       provider,
		Version:                        values.KubeVersion,
		Headlamp:                       values.Headlamp,
		PrivateOnly:                    values.PrivateOnly,
		Zone:                           values.Zone,
		Flavor:                         values.Flavor,
		Datacenter:                     values.Datacenter,
		MachineType:                    values.MachineType,
		PublicVLANID:                   values.PublicVLANID,
		PrivateVLANID:                  values.PrivateVLANID,
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

func validateHeadlamp(options servitorv1alpha1.ResolvedOptions) error {
	if options.Headlamp && (options.Platform != "kubernetes" || options.Provider != "vpc-gen2" && options.Provider != "classic") {
		return errors.New("headlamp requires Kubernetes on VPC Gen 2 or Classic")
	}
	return nil
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
	add("--resource-group", options.ResourceGroup)
	args = append(args, "--headlamp="+strconv.FormatBool(o.Headlamp))
	if o.PrivateOnly {
		args = append(args, "--private-only")
	}
	add("--zone", o.Zone)
	add("--flavor", o.Flavor)
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
	if options.Provider == "vpc-gen2" {
		add("--account-id", options.Network.AccountID)
		add("--vpc-region", options.Network.VPCRegion)
		add("--vpc-id", options.Network.VPCID)
		add("--subnet-id", options.Network.SubnetID)
		add("--public-gateway-id", options.Network.PublicGatewayID)
		if policy := options.Network.AuthPolicy; policy != nil {
			add("--auth-vpn-server-id", policy.VPNServerID)
			add("--auth-secrets-manager-id", policy.SecretsManagerID)
			add("--auth-secrets-manager-region", policy.SecretsManagerRegion)
			add("--auth-secret-group-id", policy.SecretGroupID)
			add("--auth-certificate-template", policy.CertificateTemplate)
			add("--auth-issuer", policy.Issuer)
			add("--auth-ttl", policy.TTL)
		}
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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateAcceptsCompleteOpenShiftConfiguration(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateFailsClosedForRequiredDeploymentInputs(t *testing.T) {
	for _, field := range []struct {
		name  string
		clear func(*Config)
	}{
		{"namespace", func(c *Config) { c.Namespace = "" }},
		{"execution image", func(c *Config) { c.Images.Execution = "registry.example/task:latest" }},
		{"COS endpoint", func(c *Config) { c.COS.Endpoint = "" }},
		{"Slack secret", func(c *Config) { c.Secrets.Slack = "" }},
		{"ICT ConfigMap", func(c *Config) { c.ICT.TargetConfigMap = "" }},
	} {
		t.Run(field.name, func(t *testing.T) {
			config := validConfig()
			field.clear(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}
}

func TestValidateRejectsUnsafeCOSEndpoints(t *testing.T) {
	for _, endpoint := range []string{
		"http://s3.example.invalid",
		"https://user:secret@s3.example.invalid",
		"https://user@s3.example.invalid",
		"https://s3.example.invalid?api_key=value",
		"https://s3.example.invalid#token=value",
		"https:///missing-host",
		" https://s3.example.invalid",
		"https://s3.example.invalid/password=secret",
		"https://s3.example.invalid/p%61ssword=value",
	} {
		t.Run(endpoint, func(t *testing.T) {
			config := validConfig()
			config.COS.Endpoint = endpoint
			if err := config.Validate(); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}
}

func TestValidateRejectsInvalidExecutionImages(t *testing.T) {
	for _, image := range []string{
		"registry.example.invalid/servitor-task:latest",
		"@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"registry.example.invalid/servitor-task@sha256:",
		"registry.example.invalid/servitor-task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"registry.example.invalid/servitor-task@sha256:gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
		"registry.example.invalid/servitor-task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaextra",
	} {
		t.Run(image, func(t *testing.T) {
			config := validConfig()
			config.Images.Execution = image
			if err := config.Validate(); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}
}

func TestLoadRejectsUnknownFieldsAndMultipleDocuments(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "config.yaml")
	contents := "namespace: servitor\nunknown: value\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("Load() error = %v, want unknown-field error", err)
	}
	if err := os.WriteFile(path, []byte(validYAML()+"---\n"+validYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("Load() error = %v, want multiple-document error", err)
	}
}

func TestLoadUsesMountedWorkloadReferences(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "config.yaml")
	if err := os.WriteFile(path, []byte(validYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	for environment, value := range map[string]string{
		"SERVITOR_ICT_CONFIG_MAP": "configured-ict-config",
		"SERVITOR_ICT_CONFIG_KEY": "configured.yaml",
		"SERVITOR_SLACK_SECRET":   "configured-slack",
		"SERVITOR_COS_SECRET":     "configured-cos",
		"SERVITOR_IBM_SECRET":     "configured-ibm",
	} {
		t.Setenv(environment, value)
	}
	configuration, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if configuration.ICT.TargetConfigMap != "configured-ict-config" || configuration.ICT.TargetConfigKey != "configured.yaml" || configuration.Secrets.Slack != "configured-slack" || configuration.Secrets.COS != "configured-cos" || configuration.Secrets.IBM != "configured-ibm" {
		t.Fatalf("mounted workload references were not used: %#v", configuration)
	}
}

func TestSecretsFromEnvDoesNotExposeValues(t *testing.T) {
	t.Setenv("SLACK_BOT_TOKEN", "secret-bot-token")
	t.Setenv("SLACK_APP_TOKEN", "")
	_, err := SecretsFromEnv()
	if err == nil || strings.Contains(err.Error(), "secret-bot-token") || !strings.Contains(err.Error(), "SLACK_APP_TOKEN") {
		t.Fatalf("SecretsFromEnv() error = %v", err)
	}
}

func TestResolvePathUsesMountedDefault(t *testing.T) {
	t.Setenv("SERVITOR_CONFIG", "")
	if got, err := ResolvePath(""); err != nil || got != defaultPath {
		t.Fatalf("ResolvePath() = %q, %v", got, err)
	}
}

func validConfig() Config {
	return Config{
		Namespace: "servitor",
		Slack:     SlackConfig{ChannelID: "C123"},
		Defaults:  DefaultsConfig{Version: "4.22", Target: "production", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "vpc", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"},
		Lifecycle: LifecycleConfig{ConfirmationTimeout: 5 * time.Minute, Lease: 4 * time.Hour, RetryIntervals: []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}},
		ICT:       ICTConfig{TargetConfigMap: "servitor-ict-config", TargetConfigKey: "config.yaml"},
		COS:       COSConfig{Endpoint: "https://s3.us-south.example.invalid", Bucket: "ict-state-bucket", Region: "us-south", KeyPrefix: "servitor", SkipCredentialsValidation: true, SkipMetadataAPICheck: true, SkipRegionValidation: true, SkipRequestingAccountID: true, ForcePathStyle: true},
		Images:    ImagesConfig{Execution: "registry.example.invalid/servitor-task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Secrets:   SecretsConfig{Slack: "servitor-slack", COS: "servitor-cos", IBM: "servitor-ibm"},
	}
}

func validYAML() string {
	return `namespace: servitor
slack: {channel_id: C123}
defaults: {version: "4.22", target: production, provider: vpc-gen2, resource_group: Default, zone: us-south-1, vpc_id: vpc, openshift_flavor: bx2.4x16, kubernetes_flavor: bx2.2x8}
lifecycle: {confirmation_timeout: 5m, lease: 4h, retry_intervals: [1m, 5m, 15m]}
ict: {target_config_map: servitor-ict-config, target_config_key: config.yaml}
cos: {endpoint: https://s3.us-south.example.invalid, bucket: ict-state-bucket, region: us-south, key_prefix: servitor, skip_credentials_validation: true, skip_metadata_api_check: true, skip_region_validation: true, skip_requesting_account_id: true, force_path_style: true}
images: {execution: "registry.example.invalid/servitor-task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
secrets: {slack: servitor-slack, cos: servitor-cos, ibm: servitor-ibm}
`
}

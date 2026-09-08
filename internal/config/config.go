// Package config loads Servitor's non-secret OpenShift operator configuration.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/bevicted/servitor/internal/command"
	"gopkg.in/yaml.v3"
)

const defaultPath = "/etc/servitor/config/config.yaml"

var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// Config contains only non-secret deployment inputs. Kubernetes Secret names
// identify credentials; credential values are read by the workloads that need them.
type Config struct {
	Namespace string          `yaml:"namespace"`
	Slack     SlackConfig     `yaml:"slack"`
	Defaults  DefaultsConfig  `yaml:"defaults"`
	Lifecycle LifecycleConfig `yaml:"lifecycle"`
	ICT       ICTConfig       `yaml:"ict"`
	COS       COSConfig       `yaml:"cos"`
	Images    ImagesConfig    `yaml:"images"`
	Secrets   SecretsConfig   `yaml:"secrets"`
}

type SlackConfig struct {
	ChannelID string `yaml:"channel_id"`
}

type DefaultsConfig struct {
	Version          string `yaml:"version"`
	Target           string `yaml:"target"`
	Provider         string `yaml:"provider"`
	ResourceGroup    string `yaml:"resource_group"`
	Zone             string `yaml:"zone"`
	VPCID            string `yaml:"vpc_id"`
	OpenShiftFlavor  string `yaml:"openshift_flavor"`
	KubernetesFlavor string `yaml:"kubernetes_flavor"`
}

type LifecycleConfig struct {
	ConfirmationTimeout time.Duration   `yaml:"confirmation_timeout"`
	Lease               time.Duration   `yaml:"lease"`
	RetryIntervals      []time.Duration `yaml:"retry_intervals"`
}

// ICTConfig names the ConfigMap-mounted ICT target configuration used only by tasks.
type ICTConfig struct {
	TargetConfigMap string `yaml:"target_config_map"`
	TargetConfigKey string `yaml:"target_config_key"`
}

type COSConfig struct {
	Endpoint                  string `yaml:"endpoint"`
	Bucket                    string `yaml:"bucket"`
	Region                    string `yaml:"region"`
	KeyPrefix                 string `yaml:"key_prefix"`
	SkipCredentialsValidation bool   `yaml:"skip_credentials_validation"`
	SkipMetadataAPICheck      bool   `yaml:"skip_metadata_api_check"`
	SkipRegionValidation      bool   `yaml:"skip_region_validation"`
	SkipRequestingAccountID   bool   `yaml:"skip_requesting_account_id"`
	ForcePathStyle            bool   `yaml:"force_path_style"`
	UseLockfile               bool   `yaml:"use_lockfile"`
}

type ImagesConfig struct {
	Execution string `yaml:"execution"`
}

type SecretsConfig struct {
	Slack string `yaml:"slack"`
	COS   string `yaml:"cos"`
	IBM   string `yaml:"ibm"`
}

// Secrets holds Slack credentials read from the controller's environment only.
type Secrets struct {
	BotToken string
	AppToken string
}

// ResolvePath selects an explicitly supplied or mounted configuration path.
func ResolvePath(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if path := os.Getenv("SERVITOR_CONFIG"); path != "" {
		return path, nil
	}
	return defaultPath, nil
}

// Load reads and validates exactly one strict YAML configuration document.
func Load(path string) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %q: %w", path, err)
	}
	var config Config
	decoder := yaml.NewDecoder(strings.NewReader(string(contents)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("config: decode: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, errors.New("config: contains more than one YAML document")
		}
		return Config{}, fmt.Errorf("config: decode: %w", err)
	}
	config.applyWorkloadReferencesFromEnv()
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// applyWorkloadReferencesFromEnv reads resource names from the mounted ConfigMap.
// These names must remain separate from Secret credential values.
func (c *Config) applyWorkloadReferencesFromEnv() {
	for _, item := range []struct {
		value *string
		env   string
	}{
		{&c.ICT.TargetConfigMap, "SERVITOR_ICT_CONFIG_MAP"},
		{&c.ICT.TargetConfigKey, "SERVITOR_ICT_CONFIG_KEY"},
		{&c.Secrets.Slack, "SERVITOR_SLACK_SECRET"},
		{&c.Secrets.COS, "SERVITOR_COS_SECRET"},
		{&c.Secrets.IBM, "SERVITOR_IBM_SECRET"},
	} {
		if value := os.Getenv(item.env); value != "" {
			*item.value = value
		}
	}
}

// SecretsFromEnv reads required Slack credentials without putting their values in errors.
func SecretsFromEnv() (Secrets, error) {
	secrets := Secrets{BotToken: os.Getenv("SLACK_BOT_TOKEN"), AppToken: os.Getenv("SLACK_APP_TOKEN")}
	if secrets.BotToken == "" {
		return Secrets{}, errors.New("SLACK_BOT_TOKEN is required")
	}
	if secrets.AppToken == "" {
		return Secrets{}, errors.New("SLACK_APP_TOKEN is required")
	}
	return secrets, nil
}

// Validate fails closed before the manager can begin Slack event intake.
func (c Config) Validate() error {
	if err := requiredDNSLabel("namespace", c.Namespace); err != nil {
		return err
	}
	if c.Slack.ChannelID == "" {
		return errors.New("config: slack.channel_id is required")
	}
	for _, field := range []struct{ name, value string }{
		{"ict.target_config_map", c.ICT.TargetConfigMap},
		{"secrets.slack", c.Secrets.Slack},
		{"secrets.cos", c.Secrets.COS},
		{"secrets.ibm", c.Secrets.IBM},
	} {
		if err := requiredDNSLabel(field.name, field.value); err != nil {
			return err
		}
	}
	for _, field := range []struct{ name, value string }{
		{"ict.target_config_key", c.ICT.TargetConfigKey},
		{"defaults.version", c.Defaults.Version}, {"defaults.target", c.Defaults.Target},
		{"defaults.provider", c.Defaults.Provider}, {"defaults.resource_group", c.Defaults.ResourceGroup},
		{"defaults.zone", c.Defaults.Zone}, {"defaults.vpc_id", c.Defaults.VPCID},
		{"defaults.openshift_flavor", c.Defaults.OpenShiftFlavor}, {"defaults.kubernetes_flavor", c.Defaults.KubernetesFlavor},
		{"cos.endpoint", c.COS.Endpoint}, {"cos.bucket", c.COS.Bucket}, {"cos.region", c.COS.Region},
		{"cos.key_prefix", strings.Trim(c.COS.KeyPrefix, "/")}, {"images.execution", c.Images.Execution},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("config: %s is required", field.name)
		}
	}
	if !strings.HasPrefix(c.COS.Endpoint, "https://") {
		return errors.New("config: cos.endpoint must use https")
	}
	if strings.Contains(c.COS.KeyPrefix, "..") {
		return errors.New("config: cos.key_prefix must not contain '..'")
	}
	if !strings.Contains(c.Images.Execution, "@sha256:") {
		return errors.New("config: images.execution must be digest-pinned")
	}
	if c.COS.UseLockfile {
		return errors.New("config: cos.use_lockfile is unsupported by the pinned Terraform runtime")
	}
	if _, err := command.InferPlatform(c.Defaults.Version); err != nil {
		return fmt.Errorf("config: defaults.version is invalid: %w", err)
	}
	if c.Lifecycle.ConfirmationTimeout <= 0 {
		return errors.New("config: lifecycle.confirmation_timeout must be positive")
	}
	if c.Lifecycle.Lease < time.Hour || c.Lifecycle.Lease > 24*time.Hour || c.Lifecycle.Lease%time.Hour != 0 {
		return errors.New("config: lifecycle.lease must be a whole-hour duration from 1h through 24h")
	}
	if len(c.Lifecycle.RetryIntervals) == 0 || len(c.Lifecycle.RetryIntervals) > 8 {
		return errors.New("config: lifecycle.retry_intervals must contain from 1 through 8 values")
	}
	for _, interval := range c.Lifecycle.RetryIntervals {
		if interval <= 0 || interval > 24*time.Hour {
			return errors.New("config: lifecycle.retry_intervals contains an invalid duration")
		}
	}
	return nil
}

func requiredDNSLabel(field, value string) error {
	if len(value) == 0 || len(value) > 63 || !dnsLabel.MatchString(value) {
		return fmt.Errorf("config: %s must be a DNS label", field)
	}
	return nil
}

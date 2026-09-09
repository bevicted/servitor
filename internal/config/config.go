// Package config loads Servitor's non-secret OpenShift operator configuration.
package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
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
	Inventory InventoryConfig `yaml:"inventory"`
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

// InventoryConfig controls leader-owned private discovery scheduling.
type InventoryConfig struct {
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	MaximumAge      time.Duration `yaml:"maximum_age"`
}

const (
	DefaultInventoryRefreshInterval = time.Hour
	DefaultInventoryMaximumAge      = 24 * time.Hour
)

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
	config.applyInventoryDefaults()
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// applyWorkloadReferencesFromEnv reads resource names from the mounted ConfigMap.
// These names must remain separate from Secret credential values.
func (c *Config) applyInventoryDefaults() {
	if c.Inventory.RefreshInterval == 0 {
		c.Inventory.RefreshInterval = DefaultInventoryRefreshInterval
	}
	if c.Inventory.MaximumAge == 0 {
		c.Inventory.MaximumAge = DefaultInventoryMaximumAge
	}
}

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
	if !validCOSEndpoint(c.COS.Endpoint) {
		return errors.New("config: cos.endpoint must be a canonical credential-free https URL")
	}
	if strings.Contains(c.COS.KeyPrefix, "..") {
		return errors.New("config: cos.key_prefix must not contain '..'")
	}
	if !validExecutionImage(c.Images.Execution) {
		return errors.New("config: images.execution must have a non-empty image name and a 64-character hexadecimal sha256 digest")
	}
	if c.COS.UseLockfile {
		return errors.New("config: cos.use_lockfile is unsupported by the pinned Terraform runtime")
	}
	if command.IsCloudDefault(c.Defaults.Version) {
		return errors.New("config: defaults.version must be a numeric stream, not a cloud-default alias")
	}
	if _, err := command.InferPlatform(c.Defaults.Version); err != nil {
		return fmt.Errorf("config: defaults.version is invalid: %w", err)
	}
	if c.Lifecycle.ConfirmationTimeout <= 0 {
		return errors.New("config: lifecycle.confirmation_timeout must be positive")
	}
	refreshInterval, maximumAge := c.Inventory.RefreshInterval, c.Inventory.MaximumAge
	if refreshInterval == 0 {
		refreshInterval = DefaultInventoryRefreshInterval
	}
	if maximumAge == 0 {
		maximumAge = DefaultInventoryMaximumAge
	}
	if refreshInterval <= 0 || maximumAge <= 0 || maximumAge < refreshInterval {
		return errors.New("config: inventory refresh_interval and maximum_age must be positive, and maximum_age must not be less than refresh_interval")
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

// InventoryRefreshInterval returns the configured interval or its safe default.
func (c Config) InventoryRefreshInterval() time.Duration {
	if c.Inventory.RefreshInterval > 0 {
		return c.Inventory.RefreshInterval
	}
	return DefaultInventoryRefreshInterval
}

// InventoryMaximumAge returns the configured maximum usable age or its safe default.
func (c Config) InventoryMaximumAge() time.Duration {
	if c.Inventory.MaximumAge > 0 {
		return c.Inventory.MaximumAge
	}
	return DefaultInventoryMaximumAge
}

func validCOSEndpoint(value string) bool {
	if len(value) == 0 || len(value) > 512 || strings.TrimSpace(value) != value {
		return false
	}
	endpoint, err := url.ParseRequestURI(value)
	if err != nil || endpoint.String() != value || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return false
	}
	for _, component := range []string{endpoint.Scheme, endpoint.Opaque, endpoint.Host, endpoint.Path, endpoint.RawPath, endpoint.RawQuery, endpoint.Fragment, endpoint.RawFragment} {
		if containsCredentialMarker(component) {
			return false
		}
	}
	return true
}

func validExecutionImage(value string) bool {
	imageName, digest, found := strings.Cut(value, "@sha256:")
	if !found || imageName == "" || strings.TrimSpace(imageName) != imageName || len(digest) != 64 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func containsCredentialMarker(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "password") || strings.Contains(lower, "secret") || strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") || strings.Contains(lower, "access_key") || strings.Contains(lower, "authorization") || strings.Contains(lower, "bearer ") || strings.Contains(lower, "token=")
}

func requiredDNSLabel(field, value string) error {
	if len(value) == 0 || len(value) > 63 || !dnsLabel.MatchString(value) {
		return fmt.Errorf("config: %s must be a DNS label", field)
	}
	return nil
}

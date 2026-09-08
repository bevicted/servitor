// Package config loads Servitor's non-secret operator configuration.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bevicted/servitor/internal/command"
	"gopkg.in/yaml.v3"
)

// Config contains non-secret operator settings.
type Config struct {
	Slack     SlackConfig     `yaml:"slack"`
	ICT       ICTConfig       `yaml:"ict"`
	Paths     PathsConfig     `yaml:"paths"`
	Defaults  DefaultsConfig  `yaml:"defaults"`
	Lifecycle LifecycleConfig `yaml:"lifecycle"`
	Logs      LogsConfig      `yaml:"logs"`
}

type SlackConfig struct {
	ChannelID    string `yaml:"channel_id"`
	MaintainerID string `yaml:"maintainer_id"`
}

type ICTConfig struct {
	Path          string `yaml:"path"`
	ConfigPath    string `yaml:"config_path"`
	TerraformPath string `yaml:"terraform_path"`
}

type PathsConfig struct {
	State string `yaml:"state"`
	Logs  string `yaml:"logs"`
}

type DefaultsConfig struct {
	Version          string `yaml:"version"`
	Target           string `yaml:"target"`
	Provider         string `yaml:"provider"`
	ResourceGroup    string `yaml:"resource_group"`
	Zone             string `yaml:"zone"`
	VPCName          string `yaml:"vpc_name"`
	VPCID            string `yaml:"vpc_id"`
	OpenShiftFlavor  string `yaml:"openshift_flavor"`
	KubernetesFlavor string `yaml:"kubernetes_flavor"`
}

type LifecycleConfig struct {
	ConfirmationTimeout time.Duration   `yaml:"confirmation_timeout"`
	Lease               time.Duration   `yaml:"lease"`
	RetryIntervals      []time.Duration `yaml:"retry_intervals"`
}

type LogsConfig struct {
	MaxSizeBytes      int64         `yaml:"max_size_bytes"`
	ResolvedRetention time.Duration `yaml:"resolved_retention"`
}

// Secrets holds Slack credentials, which are intentionally not accepted in YAML.
type Secrets struct {
	BotToken string
	AppToken string
}

// ResolvePath returns the operator configuration path using the standard precedence.
func ResolvePath(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if environment := os.Getenv("SERVITOR_CONFIG"); environment != "" {
		return environment, nil
	}
	directory := os.Getenv("XDG_CONFIG_HOME")
	if directory == "" {
		var err error
		directory, err = os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("resolve configuration directory: %w", err)
		}
	}
	return resolveDefaultPath(directory, os.UserHomeDir, os.Stat)
}

func resolveDefaultPath(directory string, userHomeDir func() (string, error), stat func(string) (os.FileInfo, error)) (string, error) {
	primary := filepath.Join(directory, "servitor", "config.yaml")
	if _, err := stat(primary); err == nil || !errors.Is(err, os.ErrNotExist) {
		return primary, nil
	}

	home, err := userHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve fallback configuration directory: %w", err)
	}
	fallback := filepath.Join(home, ".config", "servitor", "config.yaml")
	if filepath.Clean(primary) == filepath.Clean(fallback) {
		return primary, nil
	}
	return fallback, nil
}

// Load reads and validates a single YAML configuration document.
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
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// SecretsFromEnv reads required Slack credentials without ever returning them in errors.
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

// Validate verifies safe configuration and prepares private runtime directories.
func (c Config) Validate() error {
	if c.Slack.ChannelID == "" {
		return errors.New("config: slack.channel_id is required")
	}
	if c.Slack.MaintainerID == "" {
		return errors.New("config: slack.maintainer_id is required")
	}
	if err := executable("ict.path", c.ICT.Path); err != nil {
		return err
	}
	if err := executable("ict.terraform_path", c.ICT.TerraformPath); err != nil {
		return err
	}
	if err := readableFile("ict.config_path", c.ICT.ConfigPath); err != nil {
		return err
	}
	if err := privateDirectory("paths.state", c.Paths.State); err != nil {
		return err
	}
	if err := privateDirectory("paths.logs", c.Paths.Logs); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"defaults.version", c.Defaults.Version},
		{"defaults.target", c.Defaults.Target},
		{"defaults.provider", c.Defaults.Provider},
		{"defaults.resource_group", c.Defaults.ResourceGroup},
		{"defaults.zone", c.Defaults.Zone},
		{"defaults.vpc_name", c.Defaults.VPCName},
		{"defaults.vpc_id", c.Defaults.VPCID},
		{"defaults.openshift_flavor", c.Defaults.OpenShiftFlavor},
		{"defaults.kubernetes_flavor", c.Defaults.KubernetesFlavor},
	} {
		if field.value == "" {
			return fmt.Errorf("config: %s is required", field.name)
		}
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
	retrySchedule := []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}
	if len(c.Lifecycle.RetryIntervals) != len(retrySchedule) {
		return errors.New("config: lifecycle.retry_intervals must be [1m, 5m, 15m]")
	}
	for index, interval := range c.Lifecycle.RetryIntervals {
		if interval != retrySchedule[index] {
			return errors.New("config: lifecycle.retry_intervals must be [1m, 5m, 15m]")
		}
	}
	if c.Logs.MaxSizeBytes <= 0 {
		return errors.New("config: logs.max_size_bytes must be positive")
	}
	if c.Logs.ResolvedRetention <= 0 {
		return errors.New("config: logs.resolved_retention must be positive")
	}
	return nil
}

func readableFile(field, path string) error {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return fmt.Errorf("config: %s must name a readable file", field)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: %s must name a readable file", field)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("config: %s must name a readable file", field)
	}
	return nil
}

func executable(field, path string) error {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
		return fmt.Errorf("config: ict.%s must name an executable file", strings.TrimPrefix(field, "ict."))
	}
	return nil
}

func privateDirectory(field, path string) error {
	if path == "" {
		return fmt.Errorf("config: %s is required", field)
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("config: %s: create directory: %w", field, err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("config: %s: protect directory: %w", field, err)
	}
	info, err := os.Stat(filepath.Clean(path))
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("config: %s must be a private writable directory", field)
	}
	probe, err := os.CreateTemp(path, ".servitor-write-check-")
	if err != nil {
		return fmt.Errorf("config: %s must be writable", field)
	}
	if err := probe.Close(); err != nil {
		return fmt.Errorf("config: %s must be writable: close probe: %w", field, err)
	}
	if err := os.Remove(probe.Name()); err != nil {
		return fmt.Errorf("config: %s must be writable: remove probe: %w", field, err)
	}
	return nil
}

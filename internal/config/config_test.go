package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateCreatesPrivateRuntimeDirectories(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	config := validConfig(t, directory)
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	for _, path := range []string{config.Paths.State, config.Paths.Logs} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Errorf("%s mode = %o, want 700", path, info.Mode().Perm())
		}
	}
}

func TestValidateRejectsUnreadableICTConfigPath(t *testing.T) {
	directory := t.TempDir()
	config := validConfig(t, directory)
	if err := os.Chmod(config.ICT.ConfigPath, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(config.ICT.ConfigPath, 0o600); err != nil {
			t.Error(err)
		}
	})
	if file, err := os.Open(config.ICT.ConfigPath); err == nil {
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		t.Skip("current user can read a mode 000 file")
	}

	err := config.Validate()
	if err == nil || !strings.Contains(err.Error(), "ict.config_path") {
		t.Fatalf("Validate() error = %v, want ict.config_path error", err)
	}
}

func TestValidateIdentifiesEmptyDefaultField(t *testing.T) {
	fields := []struct {
		name  string
		clear func(*DefaultsConfig)
	}{
		{"version", func(defaults *DefaultsConfig) { defaults.Version = "" }},
		{"target", func(defaults *DefaultsConfig) { defaults.Target = "" }},

		{"provider", func(defaults *DefaultsConfig) { defaults.Provider = "" }},
		{"resource_group", func(defaults *DefaultsConfig) { defaults.ResourceGroup = "" }},
		{"zone", func(defaults *DefaultsConfig) { defaults.Zone = "" }},
		{"vpc_name", func(defaults *DefaultsConfig) { defaults.VPCName = "" }},
		{"vpc_id", func(defaults *DefaultsConfig) { defaults.VPCID = "" }},
		{"openshift_flavor", func(defaults *DefaultsConfig) { defaults.OpenShiftFlavor = "" }},
		{"kubernetes_flavor", func(defaults *DefaultsConfig) { defaults.KubernetesFlavor = "" }},
	}

	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			config := validConfig(t, t.TempDir())
			field.clear(&config.Defaults)

			err := config.Validate()
			if err == nil || !strings.Contains(err.Error(), "defaults."+field.name) {
				t.Fatalf("Validate() error = %v, want defaults.%s error", err, field.name)
			}
		})
	}
}

func TestResolvePathUsesExplicitEnvironmentThenXDG(t *testing.T) {
	directory := t.TempDir()
	explicit := filepath.Join(directory, "explicit.yaml")
	environment := filepath.Join(directory, "environment.yaml")
	t.Setenv("SERVITOR_CONFIG", environment)
	if got, err := ResolvePath(explicit); err != nil || got != explicit {
		t.Fatalf("ResolvePath(explicit) = %q, %v; want %q, nil", got, err, explicit)
	}
	if got, err := ResolvePath(""); err != nil || got != environment {
		t.Fatalf("ResolvePath(environment) = %q, %v; want %q, nil", got, err, environment)
	}

	xdg := filepath.Join(directory, "xdg-config")
	want := filepath.Join(xdg, "servitor", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(want), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(want, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SERVITOR_CONFIG", "")
	t.Setenv("XDG_CONFIG_HOME", xdg)
	if got, err := ResolvePath(""); err != nil || got != want {
		t.Fatalf("ResolvePath(XDG_CONFIG_HOME) = %q, %v; want %q, nil", got, err, want)
	}
}

func TestResolvePathUsesPlatformPrimary(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SERVITOR_CONFIG", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", home)
	directory, err := os.UserConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	primary := filepath.Join(directory, "servitor", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(primary), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(primary, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := ResolvePath(""); err != nil || got != primary {
		t.Fatalf("ResolvePath(platform primary) = %q, %v; want %q, nil", got, err, primary)
	}
}

func TestResolveDefaultPathFallsBackToLegacyHomeConfig(t *testing.T) {
	directory := t.TempDir()
	home := t.TempDir()
	fallback := filepath.Join(home, ".config", "servitor", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(fallback), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fallback, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := resolveDefaultPath(directory, func() (string, error) { return home, nil }, os.Stat)
	if err != nil || got != fallback {
		t.Fatalf("resolveDefaultPath() = %q, %v; want %q, nil", got, err, fallback)
	}
}

func TestResolveDefaultPathKeepsExistingPrimary(t *testing.T) {
	directory := t.TempDir()
	home := t.TempDir()
	primary := filepath.Join(directory, "servitor", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(primary), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(primary, []byte("unexpected: value\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := resolveDefaultPath(directory, func() (string, error) { return home, nil }, os.Stat)
	if err != nil || got != primary {
		t.Fatalf("resolveDefaultPath() = %q, %v; want %q, nil", got, err, primary)
	}
	if _, err := Load(got); err == nil || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("Load(%q) error = %v, want primary configuration error", got, err)
	}
}

func TestResolveDefaultPathDoesNotFallBackOnPrimaryError(t *testing.T) {
	directory := t.TempDir()
	primary := filepath.Join(directory, "servitor", "config.yaml")
	homeCalled := false
	got, err := resolveDefaultPath(directory, func() (string, error) {
		homeCalled = true
		return t.TempDir(), nil
	}, func(path string) (os.FileInfo, error) {
		if path != primary {
			t.Fatalf("stat path = %q, want %q", path, primary)
		}
		return nil, os.ErrPermission
	})
	if err != nil || got != primary {
		t.Fatalf("resolveDefaultPath() = %q, %v; want %q, nil", got, err, primary)
	}
	if homeCalled {
		t.Fatal("resolveDefaultPath() consulted the fallback after a primary error")
	}
}

func TestResolveDefaultPathAvoidsDuplicateCandidate(t *testing.T) {
	home := t.TempDir()
	directory := filepath.Join(home, ".config")
	primary := filepath.Join(directory, "servitor", "config.yaml")
	statCalls := 0
	got, err := resolveDefaultPath(directory, func() (string, error) { return home, nil }, func(path string) (os.FileInfo, error) {
		statCalls++
		if path != primary {
			t.Fatalf("stat path = %q, want %q", path, primary)
		}
		return nil, os.ErrNotExist
	})
	if err != nil || got != primary {
		t.Fatalf("resolveDefaultPath() = %q, %v; want %q, nil", got, err, primary)
	}
	if statCalls != 1 {
		t.Fatalf("stat calls = %d, want 1", statCalls)
	}
}

func TestLoadRejectsUnknownField(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	config := validConfig(t, directory)
	contents := "slack:\n  channel_id: C1\n  maintainer_id: U1\n  unexpected: value\n"
	contents += "ict:\n  path: " + config.ICT.Path + "\n  config_path: " + config.ICT.ConfigPath + "\n  terraform_path: " + config.ICT.TerraformPath + "\n"
	contents += "paths:\n  state: " + config.Paths.State + "\n  logs: " + config.Paths.Logs + "\n"
	contents += "defaults:\n  version: 1.31\n  target: synthetic-target\n  provider: vpc-gen2\n  resource_group: Default\n  zone: us-south-1\n  vpc_name: synthetic-vpc\n  vpc_id: id\n  openshift_flavor: bx2.4x16\n  kubernetes_flavor: bx2.2x8\n"
	contents += "lifecycle:\n  confirmation_timeout: 5m\n  lease: 4h\n  retry_intervals: [1m, 5m, 15m]\nlogs:\n  max_size_bytes: 1\n  resolved_retention: 720h\n"
	path := filepath.Join(directory, "servitor.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("Load() error = %v, want unknown-field error", err)
	}
}

func TestValidateRejectsInvalidLease(t *testing.T) {
	for _, lease := range []time.Duration{0, -time.Hour, time.Minute, 90 * time.Minute, 25 * time.Hour} {
		t.Run(lease.String(), func(t *testing.T) {
			config := validConfig(t, t.TempDir())
			config.Lifecycle.Lease = lease
			if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "lifecycle.lease") {
				t.Fatalf("Validate() error = %v, want lifecycle.lease error", err)
			}
		})
	}
}

func TestValidateRejectsInvalidDefaultVersion(t *testing.T) {
	config := validConfig(t, t.TempDir())
	config.Defaults.Version = "2.0"
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "defaults.version") {
		t.Fatalf("Validate() error = %v, want defaults.version error", err)
	}
}

func TestSecretsFromEnvDoesNotExposeValue(t *testing.T) {
	t.Setenv("SLACK_BOT_TOKEN", "secret-bot-token")
	t.Setenv("SLACK_APP_TOKEN", "")
	_, err := SecretsFromEnv()
	if err == nil || strings.Contains(err.Error(), "secret-bot-token") || !strings.Contains(err.Error(), "SLACK_APP_TOKEN") {
		t.Fatalf("SecretsFromEnv() error = %v", err)
	}
}

func validConfig(t *testing.T, directory string) Config {
	t.Helper()
	executable := filepath.Join(directory, "tool")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ictConfig := filepath.Join(directory, "ict.yaml")
	if err := os.WriteFile(ictConfig, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return Config{
		Slack:     SlackConfig{ChannelID: "C1", MaintainerID: "U1"},
		ICT:       ICTConfig{Path: executable, TerraformPath: executable, ConfigPath: ictConfig},
		Paths:     PathsConfig{State: filepath.Join(directory, "state"), Logs: filepath.Join(directory, "logs")},
		Defaults:  DefaultsConfig{Version: "1.31", Target: "synthetic-target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCName: "synthetic-vpc", VPCID: "id", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"},
		Lifecycle: LifecycleConfig{ConfirmationTimeout: 5 * time.Minute, Lease: 4 * time.Hour, RetryIntervals: []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}},
		Logs:      LogsConfig{MaxSizeBytes: 1, ResolvedRetention: 30 * 24 * time.Hour},
	}
}

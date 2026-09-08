package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
)

func TestBinaryFixtureDoneDestroysOnlyCallerState(t *testing.T) {
	directory := t.TempDir()
	binary := filepath.Join(directory, "servitor")
	build := exec.Command("go", "build", "-o", binary, "./cmd/servitor")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build servitor: %v\n%s", err, output)
	}

	statePath := filepath.Join(directory, "ict-state")
	if err := os.WriteFile(statePath, []byte("U1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ictRoot := filepath.Join(directory, "ict-root")
	if err := os.MkdirAll(filepath.Join(ictRoot, "U1"), 0o700); err != nil {
		t.Fatal(err)
	}
	ictRoot, err := filepath.EvalSymlinks(ictRoot)
	if err != nil {
		t.Fatal(err)
	}
	record, err := json.Marshal(state.LifecycleRecord{UserID: "U1", Channel: "C1", ThreadTimestamp: "123.456", Status: "ready", LeaseExpiresAt: time.Now().Add(time.Hour), UpdatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ictRoot, "U1", ".servitor-lifecycle.json"), record, 0o600); err != nil {
		t.Fatal(err)
	}
	callsPath := filepath.Join(directory, "ict-calls")
	ictPath := writeExecutable(t, directory, "ict", "#!/bin/sh\ncase \"$1\" in\nlist) if [ \"$2\" = --output ] && [ \"$3\" = json ]; then if [ -f \"$SERVITOR_ICT_STATE\" ]; then printf '{\\\"version\\\":1,\\\"state_root\\\":\\\"%s\\\",\\\"workspaces\\\":[{\\\"id\\\":\\\"U1\\\",\\\"path\\\":\\\"%s/U1\\\"}]}' \"$SERVITOR_ICT_ROOT\" \"$SERVITOR_ICT_ROOT\"; else printf '{\\\"version\\\":1,\\\"state_root\\\":\\\"%s\\\",\\\"workspaces\\\":[]}' \"$SERVITOR_ICT_ROOT\"; fi; else if [ -f \"$SERVITOR_ICT_STATE\" ]; then cat \"$SERVITOR_ICT_STATE\"; fi; fi ;;\ndestroy) [ \"$#\" = 2 ] && [ \"$2\" = U1 ] || exit 2; printf 'destroy %s\\n' \"$2\" >> \"$SERVITOR_ICT_CALLS\"; rm -rf \"$SERVITOR_ICT_ROOT/U1\"; rm \"$SERVITOR_ICT_STATE\" ;;\n*) exit 2 ;;\nesac\n")
	terraformPath := writeExecutable(t, directory, "terraform", "#!/bin/sh\nexit 0\n")
	ictConfigPath := filepath.Join(directory, "ict.yaml")
	if err := os.WriteFile(ictConfigPath, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "servitor.yaml")
	config := fmt.Sprintf(`slack:
  channel_id: C1
  maintainer_id: U-maintainer
ict:
  path: %s
  config_path: %s
  terraform_path: %s
paths:
  state: %s
  logs: %s
defaults:
  version: 1.31
  target: synthetic-target
  provider: vpc-gen2
  resource_group: Default
  zone: us-south-1
  vpc_name: synthetic-vpc
  vpc_id: synthetic-vpc-id
  openshift_flavor: bx2.4x16
  kubernetes_flavor: bx2.2x8
lifecycle:
  confirmation_timeout: 5m
  lease: 4h
  retry_intervals: [1m, 5m, 15m]
logs:
  max_size_bytes: 10485760
  resolved_retention: 720h
`, ictPath, ictConfigPath, terraformPath, filepath.Join(directory, "state"), filepath.Join(directory, "logs"))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(directory, "socket-mode.json")
	fixture, err := json.Marshal(slackbot.SocketModeFixture{SelfUserID: "B1", Events: []slackbot.FixtureEvent{
		{ID: "Ev-done", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> done", Timestamp: "123.456"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Cleanup attempt 1 started."}},
		{ID: "Ev-destroy", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> destroy", Timestamp: "789.012"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixturePath, fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(directory, "socket-mode-result.json")
	run := exec.Command(binary, "-config", configPath, "-socket-mode-fixture", fixturePath, "-socket-mode-fixture-output", resultPath)
	run.Env = append(os.Environ(), "SLACK_BOT_TOKEN=fake-bot-token", "SLACK_APP_TOKEN=fake-app-token", "SERVITOR_ICT_STATE="+statePath, "SERVITOR_ICT_ROOT="+ictRoot, "SERVITOR_ICT_CALLS="+callsPath)
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("run servitor fixture: %v\n%s", err, output)
	}
	calls, err := os.ReadFile(callsPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(calls); got != "destroy U1\n" {
		t.Errorf("ICT calls = %q, want exact caller state ID", got)
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Errorf("ICT state remains after destroy: %v", err)
	}
	contents, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	var result slackbot.FixtureResult
	if err := json.Unmarshal(contents, &result); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(responseTexts(result.Responses), "\n"); got != "Command accepted.\nCleaning up...\nStarting cleanup.\nCleanup attempt 1 started.\nCommand accepted.\nCleaning up...\nCleanup is already in progress.\nCleanup complete." {
		t.Errorf("responses = %q", got)
	}
	for _, response := range result.Responses {
		if response.ThreadTimestamp == "" {
			t.Errorf("response %+v is not threaded", response)
		}
	}
}

func responseTexts(responses []slackbot.Response) []string {
	texts := make([]string, len(responses))
	for index, response := range responses {
		texts[index] = response.Text
	}
	return texts
}

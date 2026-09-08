package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/slackbot"
)

func TestBinaryFixtureSocketModeDeduplicatesICTList(t *testing.T) {
	directory := t.TempDir()
	binary := filepath.Join(directory, "servitor")
	build := exec.Command("go", "build", "-o", binary, "./cmd/servitor")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build servitor: %v\n%s", err, output)
	}

	callsPath := filepath.Join(directory, "ict-calls")
	ictRoot := filepath.Join(directory, "ict-root")
	if err := os.MkdirAll(ictRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	ictRoot, err := filepath.EvalSymlinks(ictRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(ictRoot, "default"), 0o700); err != nil {
		t.Fatal(err)
	}
	ictPath := writeExecutable(t, directory, "ict", "#!/bin/sh\nif [ \"$1\" != list ]; then exit 2; fi\nprintf 'list\\n' >> \"$SERVITOR_ICT_CALLS\"\nif [ \"$2\" = --output ] && [ \"$3\" = json ]; then printf '{\\\"version\\\":1,\\\"state_root\\\":\\\"%s\\\",\\\"workspaces\\\":[{\\\"id\\\":\\\"default\\\",\\\"path\\\":\\\"%s/default\\\"}]}' \"$SERVITOR_ICT_ROOT\" \"$SERVITOR_ICT_ROOT\"; else printf 'state-b\\nstate-a\\n'; fi\n")
	terraformPath := writeExecutable(t, directory, "terraform", "#!/bin/sh\nexit 0\n")
	ictConfigPath := filepath.Join(directory, "ict.yaml")
	if err := os.WriteFile(ictConfigPath, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(directory, "socket-mode.json")
	fixture := slackbot.SocketModeFixture{
		SelfUserID: "B1",
		Events: []slackbot.FixtureEvent{
			{ID: "Ev-replayed", Message: slackbot.Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "list", Timestamp: "1"}},
			{ID: "Ev-replayed", Message: slackbot.Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "list", Timestamp: "1"}},
		},
	}
	fixtureContents, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixturePath, fixtureContents, 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "servitor.yaml")
	configContents := fmt.Sprintf(`slack:
  channel_id: C1
  maintainer_id: U1
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
	if err := os.WriteFile(configPath, []byte(configContents), 0o600); err != nil {
		t.Fatal(err)
	}

	resultPath := filepath.Join(directory, "socket-mode-result.json")
	run := exec.Command(binary, "-config", configPath, "-socket-mode-fixture", fixturePath, "-socket-mode-fixture-output", resultPath)
	run.Env = append(os.Environ(), "SLACK_BOT_TOKEN=fake-bot-token", "SLACK_APP_TOKEN=fake-app-token", "SERVITOR_ICT_CALLS="+callsPath, "SERVITOR_ICT_ROOT="+ictRoot)
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("run servitor fixture: %v\n%s", err, output)
	}
	for _, want := range []string{"startup config_path=", "startup phase=configuration-loaded", "startup phase=slack-authenticated", "startup phase=reconciliation-complete", "ready; accepting Socket Mode events"} {
		if !strings.Contains(string(output), want) {
			t.Fatalf("stderr startup output missing %q:\n%s", want, output)
		}
	}
	mainLog, err := os.ReadFile(filepath.Join(directory, "logs", "servitor.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ready; accepting Socket Mode events", `event_id="Ev-replayed"`, `category="list"`, `admission=accepted`} {
		if !strings.Contains(string(mainLog), want) {
			t.Fatalf("private main log missing %q:\n%s", want, mainLog)
		}
	}

	var result slackbot.FixtureResult
	contents, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(contents, &result); err != nil {
		t.Fatal(err)
	}
	if got := len(result.Acknowledgements); got != 2 {
		t.Errorf("acknowledgements = %d, want 2", got)
	}
	if got := len(result.Responses); got != 1 {
		t.Fatalf("responses = %d, want 1", got)
	}
	response := result.Responses[0]
	if response.Channel != "D1" || response.ThreadTimestamp != "" || strings.Count(response.Text, "```") != 2 || !strings.Contains(response.Text, "cluster") || strings.Contains(response.Text, "default") {

		t.Errorf("response = %+v, want an unthreaded empty lifecycle table", response)
	}
	resultText := string(contents)
	for _, forbidden := range []string{configPath, ictConfigPath, ictRoot} {
		if strings.Contains(resultText, forbidden) {
			t.Fatalf("Slack fixture output leaked private path %q: %s", forbidden, resultText)
		}
	}
	calls, err := os.ReadFile(callsPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(calls), "list\n"); got != 2 {
		t.Errorf("ict list calls = %d, want 2: startup reconciliation plus DM list", got)
	}
}

func TestWaitForCleanupDrainsActivityAfterTransportStops(t *testing.T) {
	for _, runErr := range []error{context.Canceled, errors.New("transport stopped")} {
		t.Run(runErr.Error(), func(t *testing.T) {
			var activity sync.WaitGroup
			activity.Add(1)
			finished := make(chan error, 1)
			go func() { finished <- waitForCleanup(runErr, &activity) }()

			select {
			case err := <-finished:
				t.Fatalf("waitForCleanup returned before activity completed: %v", err)
			case <-time.After(25 * time.Millisecond):
			}

			activity.Done()
			select {
			case err := <-finished:
				if errors.Is(runErr, context.Canceled) {
					if err != nil {
						t.Fatalf("waitForCleanup error = %v, want nil", err)
					}
				} else if !errors.Is(err, runErr) {
					t.Fatalf("waitForCleanup error = %v, want %v", err, runErr)
				}
			case <-time.After(time.Second):
				t.Fatal("waitForCleanup did not return after activity completed")
			}
		})
	}
}

func writeExecutable(t *testing.T, directory, name, contents string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

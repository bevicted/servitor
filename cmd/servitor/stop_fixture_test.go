package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
)

func TestBinaryGracefulStopDrainsCleanupAndKeepsSocketResponsive(t *testing.T) {
	directory := t.TempDir()
	binary := buildStopFixtureBinary(t, directory)
	ictRoot := filepath.Join(directory, "ict-root")
	if err := os.MkdirAll(filepath.Join(ictRoot, "U1"), 0o700); err != nil {
		t.Fatal(err)
	}
	var err error
	ictRoot, err = filepath.EvalSymlinks(ictRoot)
	if err != nil {
		t.Fatal(err)
	}
	writeWorkspaceRecord(t, filepath.Join(ictRoot, "U1"), state.LifecycleRecord{UserID: "U1", Channel: "C1", ThreadTimestamp: "1", Status: "ready", LeaseExpiresAt: time.Now().Add(time.Hour), UpdatedAt: time.Now()})

	release := filepath.Join(directory, "release-destroy")
	calls := filepath.Join(directory, "ict-calls")
	ictPath := writeExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list)
  if [ "$2" != --output ] || [ "$3" != json ]; then exit 2; fi
  if [ -d "$SERVITOR_ICT_ROOT/U1" ]; then
    printf '{"version":1,"state_root":"%s","workspaces":[{"id":"U1","path":"%s/U1"}]}' "$SERVITOR_ICT_ROOT" "$SERVITOR_ICT_ROOT"
  else
    printf '{"version":1,"state_root":"%s","workspaces":[]}' "$SERVITOR_ICT_ROOT"
  fi
  ;;
destroy)
  printf 'destroy %s\n' "$2" >> "$SERVITOR_ICT_CALLS"
  while [ ! -f "$SERVITOR_RELEASE" ]; do sleep 0.01; done
  rm -rf "$SERVITOR_ICT_ROOT/$2"
  ;;
*) exit 2 ;;
esac
`)
	config := writeStopFixtureConfig(t, directory, ictPath)
	result := filepath.Join(directory, "result.json")
	fixture := filepath.Join(directory, "fixture.json")
	writeStopFixture(t, fixture, slackbot.SocketModeFixture{
		SelfUserID: "B1",
		Events: []slackbot.FixtureEvent{
			{ID: "done", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> done", Timestamp: "1"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Cleanup attempt 1 started."}},
			{ID: "stop", Message: slackbot.Message{Channel: "DM", ChannelType: "im", User: "U-maintainer", Text: "stop", Timestamp: "2"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Mode: draining-to-stop"}},
			{ID: "status", Message: slackbot.Message{Channel: "DM", ChannelType: "im", User: "U-maintainer", Text: "status", Timestamp: "3"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Active cleanup: 1"}},
		},
		Hold: true, HoldPath: filepath.Join(directory, "held"),
	})
	command := exec.Command(binary, "-config", config, "-socket-mode-fixture", fixture, "-socket-mode-fixture-output", result)
	command.Env = append(os.Environ(), "SLACK_BOT_TOKEN=fake-bot-token", "SLACK_APP_TOKEN=fake-app-token", "SERVITOR_ICT_ROOT="+ictRoot, "SERVITOR_ICT_CALLS="+calls, "SERVITOR_RELEASE="+release)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFixtureText(t, result, "Active cleanup: 1")
	texts := fixtureTexts(t, result)
	if strings.Contains(texts, "Servitor has stopped") {
		t.Fatalf("graceful stop completed before blocked cleanup: %q", texts)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("graceful stop exit: %v\n%s", err, output.String())
	}
	texts = fixtureTexts(t, result)
	if !strings.Contains(texts, "Cleanup complete.") || !strings.Contains(texts, "Servitor has stopped after draining active work.") {
		t.Fatalf("fixture responses = %q", texts)
	}
	if strings.Index(texts, "Cleanup complete.") > strings.Index(texts, "Servitor has stopped after draining active work.") {
		t.Fatalf("final stop notice preceded cleanup completion: %q", texts)
	}
	callsContents, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(callsContents); got != "destroy U1\n" {
		t.Fatalf("destroy calls = %q", got)
	}
}

func TestBinarySignalDuringGracefulStopDoesNotDuplicateCleanup(t *testing.T) {
	directory := t.TempDir()
	binary := buildStopFixtureBinary(t, directory)
	ictRoot := filepath.Join(directory, "ict-root")
	if err := os.MkdirAll(filepath.Join(ictRoot, "U1"), 0o700); err != nil {
		t.Fatal(err)
	}
	var err error
	ictRoot, err = filepath.EvalSymlinks(ictRoot)
	if err != nil {
		t.Fatal(err)
	}
	writeWorkspaceRecord(t, filepath.Join(ictRoot, "U1"), state.LifecycleRecord{UserID: "U1", Channel: "C1", ThreadTimestamp: "1", Status: "ready", LeaseExpiresAt: time.Now().Add(time.Hour), UpdatedAt: time.Now()})
	release := filepath.Join(directory, "release-destroy")
	calls := filepath.Join(directory, "ict-calls")
	ictPath := writeExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list)
  if [ -d "$SERVITOR_ICT_ROOT/U1" ]; then printf '{"version":1,"state_root":"%s","workspaces":[{"id":"U1","path":"%s/U1"}]}' "$SERVITOR_ICT_ROOT" "$SERVITOR_ICT_ROOT"; else printf '{"version":1,"state_root":"%s","workspaces":[]}' "$SERVITOR_ICT_ROOT"; fi
  ;;
destroy) printf 'destroy %s\n' "$2" >> "$SERVITOR_ICT_CALLS"; while [ ! -f "$SERVITOR_RELEASE" ]; do sleep 0.01; done; rm -rf "$SERVITOR_ICT_ROOT/$2" ;;
*) exit 2 ;;
esac
`)
	config := writeStopFixtureConfig(t, directory, ictPath)
	result := filepath.Join(directory, "result.json")
	fixture := filepath.Join(directory, "fixture.json")
	writeStopFixture(t, fixture, slackbot.SocketModeFixture{SelfUserID: "B1", Events: []slackbot.FixtureEvent{
		{ID: "done", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> done", Timestamp: "1"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Cleanup attempt 1 started."}},
		{ID: "stop", Message: slackbot.Message{Channel: "DM", ChannelType: "im", User: "U-maintainer", Text: "stop", Timestamp: "2"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Mode: draining-to-stop"}},
	}, Hold: true, HoldPath: filepath.Join(directory, "held")})
	command := exec.Command(binary, "-config", config, "-socket-mode-fixture", fixture, "-socket-mode-fixture-output", result)
	command.Env = append(os.Environ(), "SLACK_BOT_TOKEN=fake-bot-token", "SLACK_APP_TOKEN=fake-app-token", "SERVITOR_ICT_ROOT="+ictRoot, "SERVITOR_ICT_CALLS="+calls, "SERVITOR_RELEASE="+release)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFixtureText(t, result, "Mode: draining-to-stop")
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("signal/graceful-stop race exit: %v\n%s", err, output.String())
	}
	contents, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(contents); got != "destroy U1\n" {
		t.Fatalf("duplicate cleanup after signal/stop race: %q", got)
	}
	if _, err := os.Stat(filepath.Join(directory, "state", "admission.json")); err != nil {
		t.Fatalf("transient stop did not recover as paused after emergency signal: %v", err)
	}
}

func TestBinaryGracefulStopWaitsThroughCleanupRetriesAndFinalFailure(t *testing.T) {
	directory := t.TempDir()
	binary := buildStopFixtureBinary(t, directory)
	ictRoot := filepath.Join(directory, "ict-root")
	if err := os.MkdirAll(filepath.Join(ictRoot, "U1"), 0o700); err != nil {
		t.Fatal(err)
	}
	var err error
	ictRoot, err = filepath.EvalSymlinks(ictRoot)
	if err != nil {
		t.Fatal(err)
	}
	writeWorkspaceRecord(t, filepath.Join(ictRoot, "U1"), state.LifecycleRecord{UserID: "U1", Channel: "C1", ThreadTimestamp: "1", Status: "ready", LeaseExpiresAt: time.Now().Add(time.Hour), UpdatedAt: time.Now()})
	calls := filepath.Join(directory, "ict-calls")
	ictPath := writeExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list) printf '{"version":1,"state_root":"%s","workspaces":[{"id":"U1","path":"%s/U1"}]}' "$SERVITOR_ICT_ROOT" "$SERVITOR_ICT_ROOT" ;;
destroy) printf 'destroy %s\n' "$2" >> "$SERVITOR_ICT_CALLS"; exit 1 ;;
*) exit 2 ;;
esac
`)
	config := writeStopFixtureConfig(t, directory, ictPath)
	clock := filepath.Join(directory, "clock")
	at := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	if err := os.WriteFile(clock, []byte(at.Format(time.RFC3339Nano)), 0o600); err != nil {
		t.Fatal(err)
	}
	result := filepath.Join(directory, "result.json")
	fixture := filepath.Join(directory, "fixture.json")
	writeStopFixture(t, fixture, slackbot.SocketModeFixture{
		SelfUserID: "B1",
		Events: []slackbot.FixtureEvent{
			{ID: "done", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> done", Timestamp: "1"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Retrying in 1m0s"}},
			{ID: "stop", Message: slackbot.Message{Channel: "DM", ChannelType: "im", User: "U-maintainer", Text: "stop", Timestamp: "2"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Mode: draining-to-stop"}},
		}, Hold: true, HoldPath: filepath.Join(directory, "held"),
	})
	command := exec.Command(binary, "-config", config, "-socket-mode-fixture", fixture, "-socket-mode-fixture-output", result, "-socket-mode-fixture-clock", clock)
	command.Env = append(os.Environ(), "SLACK_BOT_TOKEN=fake-bot-token", "SLACK_APP_TOKEN=fake-app-token", "SERVITOR_ICT_ROOT="+ictRoot, "SERVITOR_ICT_CALLS="+calls)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	advanceFixtureClockUntil(t, clock, at, result, "Retrying in 5m0s")
	advanceFixtureClockUntil(t, clock, at.Add(time.Hour), result, "Retrying in 15m0s")
	advanceFixtureClockUntil(t, clock, at.Add(2*time.Hour), result, "Cleanup remains unresolved.")
	if err := command.Wait(); err != nil {
		t.Fatalf("graceful stop after final cleanup failure: %v\n%s", err, output.String())
	}
	if got := fixtureTexts(t, result); !strings.Contains(got, "Servitor has stopped after draining active work.") {
		t.Fatalf("fixture responses = %q", got)
	}
	contents, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(contents), "destroy U1\n"); got != 4 {
		t.Fatalf("destroy attempts = %d, want 4: %q", got, contents)
	}
}

func TestBinaryGracefulStopDrainsApply(t *testing.T) {
	directory := t.TempDir()
	binary := buildStopFixtureBinary(t, directory)
	ictRoot := filepath.Join(directory, "ict-root")
	if err := os.MkdirAll(ictRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	var err error
	ictRoot, err = filepath.EvalSymlinks(ictRoot)
	if err != nil {
		t.Fatal(err)
	}
	release := filepath.Join(directory, "release-apply")
	ictPath := writeExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list)
  if [ "$2" != --output ] || [ "$3" != json ]; then exit 2; fi
  if [ -d "$SERVITOR_ICT_ROOT/U1" ]; then
    printf '{"version":1,"state_root":"%s","workspaces":[{"id":"U1","path":"%s/U1"}]}' "$SERVITOR_ICT_ROOT" "$SERVITOR_ICT_ROOT"
  else
    printf '{"version":1,"state_root":"%s","workspaces":[]}' "$SERVITOR_ICT_ROOT"
  fi
  ;;
create)
  mkdir -p "$SERVITOR_ICT_ROOT/U1"
  printf 'Do you want to perform these actions?\n'
  read answer
  [ "$answer" = yes ] || exit 1
  while [ ! -f "$SERVITOR_RELEASE" ]; do sleep 0.01; done
  ;;
*) exit 2 ;;
esac
`)
	terraformPath := writeExecutable(t, directory, "terraform", `#!/bin/sh
if [ "$2" = show ] && [ "$3" = -json ]; then
  case "$4" in
  *.tfplan) printf '{"format_version":"1.2","resource_changes":[]}' ;;
  *) printf '{"format_version":"1.0","values":{"root_module":{"resources":[]}}}' ;;
  esac
else
  exit 2
fi
`)
	config := writeStopFixtureConfigWithTerraform(t, directory, ictPath, terraformPath)
	result := filepath.Join(directory, "result.json")
	fixture := filepath.Join(directory, "fixture.json")
	writeStopFixture(t, fixture, slackbot.SocketModeFixture{
		SelfUserID: "B1",
		Events: []slackbot.FixtureEvent{
			{ID: "create", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> create", Timestamp: "1"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Reply with exact `yes`"}},
			{ID: "approve", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "2", ThreadTimestamp: "1"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Plan approved."}},
			{ID: "stop", Message: slackbot.Message{Channel: "DM", ChannelType: "im", User: "U-maintainer", Text: "stop", Timestamp: "3"}, WaitFor: &slackbot.FixtureWait{ResponseContains: "Active applies: 1"}},
		}, Hold: true, HoldPath: filepath.Join(directory, "held"),
	})
	command := exec.Command(binary, "-config", config, "-socket-mode-fixture", fixture, "-socket-mode-fixture-output", result)
	command.Env = append(os.Environ(), "SLACK_BOT_TOKEN=fake-bot-token", "SLACK_APP_TOKEN=fake-app-token", "SERVITOR_ICT_ROOT="+ictRoot, "SERVITOR_RELEASE="+release)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFixtureText(t, result, "Active applies: 1")
	if strings.Contains(fixtureTexts(t, result), "Servitor has stopped") {
		t.Fatal("graceful stop completed before blocked apply")
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("graceful stop after apply: %v\n%s", err, output.String())
	}
	if got := fixtureTexts(t, result); !strings.Contains(got, "Your cluster is ready.") || !strings.Contains(got, "Servitor has stopped after draining active work.") {
		t.Fatalf("fixture responses = %q", got)
	}
}

func TestBinaryGracefulStopIgnoresReadyLeaseAndFinalNoticeFailure(t *testing.T) {
	directory := t.TempDir()
	binary := buildStopFixtureBinary(t, directory)
	ictRoot := filepath.Join(directory, "ict-root")
	if err := os.MkdirAll(filepath.Join(ictRoot, "U1"), 0o700); err != nil {
		t.Fatal(err)
	}
	var err error
	ictRoot, err = filepath.EvalSymlinks(ictRoot)
	if err != nil {
		t.Fatal(err)
	}
	expiry := time.Now().Add(time.Hour)
	writeWorkspaceRecord(t, filepath.Join(ictRoot, "U1"), state.LifecycleRecord{UserID: "U1", Channel: "C1", ThreadTimestamp: "1", Status: "ready", LeaseExpiresAt: expiry, UpdatedAt: time.Now()})
	ictPath := writeExecutable(t, directory, "ict", `#!/bin/sh
if [ "$1" = list ] && [ "$2" = --output ] && [ "$3" = json ]; then printf '{"version":1,"state_root":"%s","workspaces":[{"id":"U1","path":"%s/U1"}]}' "$SERVITOR_ICT_ROOT" "$SERVITOR_ICT_ROOT"; else exit 2; fi
`)
	config := writeStopFixtureConfig(t, directory, ictPath)
	result := filepath.Join(directory, "result.json")
	fixture := filepath.Join(directory, "fixture.json")
	writeStopFixture(t, fixture, slackbot.SocketModeFixture{
		SelfUserID: "B1", ReplyFailureContains: "Servitor has stopped after draining active work.",
		Events: []slackbot.FixtureEvent{{ID: "stop", Message: slackbot.Message{Channel: "DM", ChannelType: "im", User: "U-maintainer", Text: "stop", Timestamp: "1"}}},
	})
	run := exec.Command(binary, "-config", config, "-socket-mode-fixture", fixture, "-socket-mode-fixture-output", result)
	run.Env = append(os.Environ(), "SLACK_BOT_TOKEN=fake-bot-token", "SLACK_APP_TOKEN=fake-app-token", "SERVITOR_ICT_ROOT="+ictRoot)
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("graceful stop with final notice failure: %v\n%s", err, output)
	}
	if _, err := os.Stat(filepath.Join(ictRoot, "U1", ".servitor-lifecycle.json")); err != nil {
		t.Fatalf("future ready lease was changed: %v", err)
	}
	mainLog, err := os.ReadFile(filepath.Join(directory, "logs", "servitor.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mainLog), "deliver graceful-stop final maintainer DM") {
		t.Fatalf("final message failure was not privately logged: %s", mainLog)
	}
	if _, err := os.Stat(filepath.Join(directory, "state", "admission.json")); !os.IsNotExist(err) {
		t.Fatalf("completed stop retained transient admission marker: %v", err)
	}
}

func buildStopFixtureBinary(t *testing.T, directory string) string {
	t.Helper()
	binary := filepath.Join(directory, "servitor")
	build := exec.Command("go", "build", "-o", binary, "./cmd/servitor")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build servitor: %v\n%s", err, output)
	}
	return binary
}

func writeStopFixtureConfig(t *testing.T, directory, ictPath string) string {
	t.Helper()
	terraformPath := writeExecutable(t, directory, "terraform", "#!/bin/sh\nexit 0\n")
	return writeStopFixtureConfigWithTerraform(t, directory, ictPath, terraformPath)
}

func writeStopFixtureConfigWithTerraform(t *testing.T, directory, ictPath, terraformPath string) string {
	t.Helper()
	ictConfig := filepath.Join(directory, "ict.yaml")
	if err := os.WriteFile(ictConfig, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "servitor.yaml")
	if err := os.WriteFile(config, []byte(binaryRestartConfig(ictPath, ictConfig, terraformPath, directory)), 0o600); err != nil {
		t.Fatal(err)
	}
	return config
}

func writeStopFixture(t *testing.T, path string, fixture slackbot.SocketModeFixture) {
	t.Helper()
	contents, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForFixtureText(t *testing.T, result, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if contents, err := os.ReadFile(result); err == nil && strings.Contains(string(contents), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	contents, _ := os.ReadFile(result)
	t.Fatalf("fixture result did not contain %q: %s", want, contents)
}

func advanceFixtureClockUntil(t *testing.T, clock string, at time.Time, result, want string) {
	t.Helper()
	if contents, err := os.ReadFile(clock); err == nil {
		if current, err := time.Parse(time.RFC3339Nano, string(contents)); err == nil && current.After(at) {
			at = current
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		at = at.Add(time.Hour)
		if err := os.WriteFile(clock, []byte(at.Format(time.RFC3339Nano)), 0o600); err != nil {
			t.Fatal(err)
		}
		if contents, err := os.ReadFile(result); err == nil && strings.Contains(string(contents), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	contents, _ := os.ReadFile(result)
	t.Fatalf("fixture result did not reach %q: %s", want, contents)
}

func fixtureTexts(t *testing.T, result string) string {
	t.Helper()
	contents, err := os.ReadFile(result)
	if err != nil {
		t.Fatal(err)
	}
	var observed slackbot.FixtureResult
	if err := json.Unmarshal(contents, &observed); err != nil {
		t.Fatal(err)
	}
	return strings.Join(responseTexts(observed.Responses), "\n")
}

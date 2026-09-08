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

func TestBinaryRestartReconcilesWorkspaceLocalLifecycleState(t *testing.T) {
	directory := t.TempDir()
	binary := filepath.Join(directory, "servitor")
	build := exec.Command("go", "build", "-o", binary, "./cmd/servitor")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build servitor: %v\n%s", err, output)
	}

	ictRoot := filepath.Join(directory, "ict-root")
	for _, id := range []string{"U1", "U2", "default"} {
		if err := os.MkdirAll(filepath.Join(ictRoot, id), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	ictRoot, err := filepath.EvalSymlinks(ictRoot)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	writeWorkspaceRecord(t, filepath.Join(ictRoot, "U1"), state.LifecycleRecord{UserID: "U1", Channel: "C1", ThreadTimestamp: "1.1", Status: "ready", LeaseExpiresAt: now.Add(time.Hour), UpdatedAt: now})
	// U2 has no lifecycle record, modeling a crash before interrupted creation
	// metadata could be persisted.

	calls := filepath.Join(directory, "ict-calls")
	ictPath := writeExecutable(t, directory, "ict", `#!/bin/sh
case "$1" in
list)
  if [ "$2" != --output ] || [ "$3" != json ]; then exit 2; fi
  printf '{"version":1,"state_root":"%s","workspaces":[' "$SERVITOR_ICT_ROOT"
  first=1
  for workspace in "$SERVITOR_ICT_ROOT"/*; do
    [ -d "$workspace" ] || continue
    id=${workspace##*/}
    if [ "$first" = 0 ]; then printf ','; fi
    first=0
    printf '{"id":"%s","path":"%s"}' "$id" "$workspace"
  done
  printf ']}'
  ;;
destroy)
  printf 'destroy %s\n' "$2" >> "$SERVITOR_ICT_CALLS"
  rm -rf "$SERVITOR_ICT_ROOT/$2"
  ;;
*) exit 2 ;;
esac
`)
	terraformPath := writeExecutable(t, directory, "terraform", "#!/bin/sh\nexit 0\n")
	ictConfig := filepath.Join(directory, "ict.yaml")
	if err := os.WriteFile(ictConfig, []byte("version: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "servitor.yaml")
	if err := os.WriteFile(config, []byte(binaryRestartConfig(ictPath, ictConfig, terraformPath, directory)), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(directory, "fixture.json")
	contents, err := json.Marshal(slackbot.SocketModeFixture{SelfUserID: "B1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	runFixtureBinary(t, binary, config, fixture, filepath.Join(directory, "first.json"), ictRoot, calls)
	if _, err := os.Stat(filepath.Join(ictRoot, "U1", ".servitor-lifecycle.json")); err != nil {
		t.Fatalf("ready workspace-local record: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ictRoot, "U2")); !os.IsNotExist(err) {
		t.Fatalf("interrupted workspace remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(ictRoot, "default")); err != nil {
		t.Fatalf("unrelated default workspace was changed: %v", err)
	}
	assertDestroyCalls(t, calls, "destroy U2\n")

	runFixtureBinary(t, binary, config, fixture, filepath.Join(directory, "second.json"), ictRoot, calls)
	assertDestroyCalls(t, calls, "destroy U2\n")
}

func writeWorkspaceRecord(t *testing.T, workspace string, record state.LifecycleRecord) {
	t.Helper()
	contents, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".servitor-lifecycle.json"), contents, 0o600); err != nil {
		t.Fatal(err)
	}
}

func binaryRestartConfig(ictPath, ictConfig, terraformPath, directory string) string {
	return fmt.Sprintf(`slack:
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
`, ictPath, ictConfig, terraformPath, filepath.Join(directory, "state"), filepath.Join(directory, "logs"))
}

func runFixtureBinary(t *testing.T, binary, config, fixture, result, ictRoot, calls string) {
	t.Helper()
	run := exec.Command(binary, "-config", config, "-socket-mode-fixture", fixture, "-socket-mode-fixture-output", result)
	run.Env = append(os.Environ(), "SLACK_BOT_TOKEN=fake-bot-token", "SLACK_APP_TOKEN=fake-app-token", "SERVITOR_ICT_ROOT="+ictRoot, "SERVITOR_ICT_CALLS="+calls)
	if output, err := run.CombinedOutput(); err != nil {
		t.Fatalf("run servitor fixture: %v\n%s", err, output)
	}
}

func assertDestroyCalls(t *testing.T, path, want string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(contents)); got != strings.TrimSpace(want) {
		t.Fatalf("ICT destroy calls = %q, want %q", got, strings.TrimSpace(want))
	}
}

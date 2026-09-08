package ict

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bevicted/servitor/internal/diagnostics"
)

func TestDestroyUsesExactCallerStateID(t *testing.T) {
	directory := t.TempDir()
	arguments := filepath.Join(directory, "arguments")
	path := writeICT(t, directory, "ict", "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$SERVITOR_ICT_ARGUMENTS\"\n[ \"$1\" = destroy ] && [ \"$2\" = U1 ] && [ \"$#\" = 2 ]\n")
	t.Setenv("SERVITOR_ICT_ARGUMENTS", arguments)
	client := NewClient(path, nil)
	if err := client.Destroy(context.Background(), "U1"); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(contents), "destroy\nU1\n"; got != want {
		t.Errorf("ict argv = %q, want %q", got, want)
	}
}

func TestListDiagnosticReturnsUnmodifiedOutputFromPrivateWriter(t *testing.T) {
	root := t.TempDir()
	logger, err := diagnostics.Open(filepath.Join(root, "diagnostics"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	path := writeICT(t, root, "ict", "#!/bin/sh\nprintf 'U1\\nU2\\n'\n")
	var shared bytes.Buffer
	client := NewClient(path, &shared)
	client.Diagnostics = logger

	output, err := client.ListDiagnostic(context.Background(), "U1", "111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if output != "U1\nU2\n" {
		t.Errorf("list output = %q, want unmodified stdout", output)
	}
	if shared.Len() != 0 {
		t.Fatalf("shared log received diagnostic output: %q", shared.String())
	}
}

func TestListDiagnosticKeepsPreflightOutputOutOfSharedLog(t *testing.T) {
	root := t.TempDir()
	logger, err := diagnostics.Open(filepath.Join(root, "diagnostics"), 4096)
	if err != nil {
		t.Fatal(err)
	}
	path := writeICT(t, root, "ict", "#!/bin/sh\nprintf 'terraform-sensitive-output\\n' >&2\nexit 1\n")
	var shared bytes.Buffer
	client := NewClient(path, &shared)
	client.Diagnostics = logger
	const diagnosticID = "111111111111111111111111"

	if _, err := client.ListDiagnostic(context.Background(), "U1", diagnosticID); err == nil {
		t.Fatal("ListDiagnostic succeeded")
	}
	if shared.Len() != 0 {
		t.Fatalf("shared log received diagnostic output: %q", shared.String())
	}
	writer, err := logger.Writer(diagnosticID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	segments, err := writer.Segments()
	if err != nil || len(segments) == 0 {
		t.Fatalf("diagnostic segments = %v, %v", segments, err)
	}
	var contents strings.Builder
	for _, segment := range segments {
		data, err := os.ReadFile(segment)
		if err != nil {
			t.Fatal(err)
		}
		contents.Write(data)
	}
	for _, want := range []string{"kind=argv", "list", "terraform-sensitive-output"} {
		if !strings.Contains(contents.String(), want) {
			t.Fatalf("diagnostic log missing %q: %s", want, contents.String())
		}
	}
}

func TestWorkspaceInventoryDiagnosticLogsSafeProcessBoundaries(t *testing.T) {
	const diagnosticID = "111111111111111111111111"
	for _, test := range []struct {
		name       string
		script     string
		wantResult string
		wantErr    bool
	}{
		{
			name:       "completed",
			script:     "#!/bin/sh\n[ \"$1\" = list ] && [ \"$2\" = --output ] && [ \"$3\" = json ] && [ \"$#\" = 3 ] || exit 2\nprintf '%s' \"$SERVITOR_ICT_INVENTORY\"\n",
			wantResult: "completed",
		},
		{
			name:       "failed",
			script:     "#!/bin/sh\nprintf 'xoxb-secret /private/state' >&2\nexit 1\n",
			wantResult: "failed",
			wantErr:    true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			stateRoot := filepath.Join(root, "state")
			if err := os.Mkdir(stateRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			resolvedRoot, err := filepath.EvalSymlinks(stateRoot)
			if err != nil {
				t.Fatal(err)
			}
			stateRoot = resolvedRoot
			t.Setenv("SERVITOR_ICT_INVENTORY", `{"version":1,"state_root":"`+stateRoot+`","workspaces":[]}`)
			logger, err := diagnostics.Open(filepath.Join(root, "diagnostics"), 4096)
			if err != nil {
				t.Fatal(err)
			}
			var shared, logs bytes.Buffer
			client := NewClient(writeICT(t, root, "ict", test.script), &shared)
			client.Diagnostics = logger
			client.Logf = func(format string, args ...any) { fmt.Fprintf(&logs, format, args...) }

			_, err = client.WorkspaceInventoryDiagnostic(context.Background(), "U1", diagnosticID)
			if (err != nil) != test.wantErr {
				t.Fatalf("WorkspaceInventoryDiagnostic() error = %v, want error %v", err, test.wantErr)
			}
			for _, result := range []string{"started", test.wantResult} {
				want := `subprocess operation="list" state_id="U1" diagnostic_id="` + diagnosticID + `" result="` + result + `"`
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("main log missing %q: %s", want, logs.String())
				}
			}
			for _, forbidden := range []string{"xoxb-secret", "/private/state", stateRoot} {
				if strings.Contains(logs.String(), forbidden) {
					t.Fatalf("main log leaked %q: %s", forbidden, logs.String())
				}
			}
			if shared.Len() != 0 {
				t.Fatalf("shared log received diagnostic output: %q", shared.String())
			}
		})
	}
}

func TestWithDiagnosticFailsWhenPrivateWriterCannotBeCreated(t *testing.T) {
	root := filepath.Join(t.TempDir(), "diagnostics")
	logger, err := diagnostics.Open(root, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var shared bytes.Buffer
	client := NewClient("/unused/ict", &shared)
	client.Diagnostics = logger

	if _, err := client.WithDiagnostic("111111111111111111111111", "U1"); err == nil {
		t.Fatal("WithDiagnostic succeeded after private writer creation failure")
	}
	if shared.Len() != 0 {
		t.Fatalf("shared log received lifecycle output after writer failure: %q", shared.String())
	}
}

func TestHasStateMatchesExactWorkspace(t *testing.T) {
	directory := t.TempDir()
	root := filepath.Join(directory, "ict")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"U10", "U1"} {
		if err := os.Mkdir(filepath.Join(root, id), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	path := writeICT(t, directory, "ict-bin", "#!/bin/sh\n[ \"$1\" = list ] && [ \"$2\" = --output ] && [ \"$3\" = json ] && [ \"$#\" = 3 ] || exit 2\nprintf '%s' '{\"version\":1,\"state_root\":\"'\"$SERVITOR_ICT_ROOT\"'\",\"workspaces\":[{\"id\":\"U10\",\"path\":\"'\"$SERVITOR_ICT_ROOT\"'/U10\"},{\"id\":\"U1\",\"path\":\"'\"$SERVITOR_ICT_ROOT\"'/U1\"}]}'\n")
	t.Setenv("SERVITOR_ICT_ROOT", root)
	client := NewClient(path, nil)
	hasState, err := client.HasState(context.Background(), "U1")
	if err != nil || !hasState {
		t.Fatalf("HasState(U1) = (%v, %v), want (true, nil)", hasState, err)
	}
	hasState, err = client.HasState(context.Background(), "U")
	if err != nil || hasState {
		t.Fatalf("HasState(U) = (%v, %v), want (false, nil)", hasState, err)
	}
}

func TestStartCreateUsesFixedPrefixAndExactStateID(t *testing.T) {
	directory := t.TempDir()
	arguments := filepath.Join(directory, "arguments")
	path := writeICT(t, directory, "ict", "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$SERVITOR_ICT_ARGUMENTS\"\nread answer\n")
	t.Setenv("SERVITOR_ICT_ARGUMENTS", arguments)
	client := NewClient(path, nil)
	process, err := client.StartCreate("U1", "/private/ict.yaml", []string{"--version", "4.22"})
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Confirm(false); err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(contents), "create\nU1\n--config\n/private/ict.yaml\n--prefix\nservitor\n--confirm-stdin\n--version\n4.22\n"; got != want {
		t.Errorf("ict argv = %q, want %q", got, want)
	}
}

func TestBoundedOutputLimitsPrivateOperationLog(t *testing.T) {
	var log bytes.Buffer
	output := boundedOutput{limit: 4, log: &log}

	if written, err := output.Write([]byte("abcdef")); written != 6 || err != nil {
		t.Fatalf("Write() = (%d, %v), want (6, nil)", written, err)
	}
	if written, err := output.Write([]byte("gh")); written != 2 || err != nil {
		t.Fatalf("Write() = (%d, %v), want (2, nil)", written, err)
	}
	if got, want := log.String(), "abcdefgh"; got != want {
		t.Errorf("private operation log = %q, want %q", got, want)
	}
	if got, want := output.String(), "abcd\n[output truncated]"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestBoundedOutputDetectsPromptAfterPrivateLogCap(t *testing.T) {
	var log bytes.Buffer
	output := boundedOutput{limit: 4, log: &log}
	if _, err := output.Write([]byte("abcdefghDo you want to perform ")); err != nil {
		t.Fatal(err)
	}
	if _, err := output.Write([]byte("these actions?")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-output.Prompted():
	default:
		t.Fatal("approval prompt after output cap was not detected")
	}
	if got, want := log.String(), "abcdefghDo you want to perform these actions?"; got != want {
		t.Errorf("private operation log = %q, want %q", got, want)
	}
}

func writeICT(t *testing.T, directory, name, contents string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

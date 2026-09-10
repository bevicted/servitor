package command

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunnerUsesArgumentVectorAndBoundsOutput(t *testing.T) {
	if os.Getenv("SERVITOR_COMMAND_HELPER") == "1" {
		if os.Args[len(os.Args)-1] != "literal;not-a-shell-command" {
			os.Exit(2)
		}
		_, _ = os.Stdout.WriteString(strings.Repeat("x", 32))
		_, _ = os.Stderr.WriteString(strings.Repeat("y", 32))
		return
	}
	t.Setenv("SERVITOR_COMMAND_HELPER", "1")
	var stdout, stderr bytes.Buffer
	runner := Runner{MaxOutput: 8, Stdout: &stdout, Stderr: &stderr}
	result, err := runner.Run(context.Background(), os.Args[0], "-test.run=TestRunnerUsesArgumentVectorAndBoundsOutput", "--", "literal;not-a-shell-command")
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "xxxxxxxx\n[output truncated]" || result.Stderr != "yyyyyyyy\n[output truncated]" || !result.StdoutTruncated || !result.StderrTruncated {
		t.Errorf("result = %#v", result)
	}
	if !strings.HasPrefix(stdout.String(), strings.Repeat("x", 32)) || stderr.String() != strings.Repeat("y", 32) {
		t.Errorf("streamed output = stdout:%q stderr:%q", stdout.String(), stderr.String())
	}
}

func TestRunnerFailureLogRedactsSensitiveInputs(t *testing.T) {
	if os.Getenv("SERVITOR_COMMAND_FAILURE_HELPER") == "1" {
		_, _ = os.Stderr.WriteString(os.Args[len(os.Args)-1] + os.Getenv("SERVITOR_COMMAND_ENV"))
		os.Exit(23)
	}
	const argumentSecret = "argument-secret"
	const environmentSecret = "environment-secret"
	t.Setenv("SERVITOR_COMMAND_FAILURE_HELPER", "1")
	t.Setenv("SERVITOR_COMMAND_ENV", environmentSecret)
	var logs bytes.Buffer

	result, err := (Runner{Log: &logs}).Run(context.Background(), os.Args[0], "-test.run=TestRunnerFailureLogRedactsSensitiveInputs", "--", argumentSecret)
	if err == nil {
		t.Fatal("Run returned nil error")
	}
	for _, secret := range []string{argumentSecret, environmentSecret} {
		if !strings.Contains(result.Stderr, secret) {
			t.Errorf("helper output = %q, want secret %q", result.Stderr, secret)
		}
		if strings.Contains(logs.String(), secret) || strings.Contains(err.Error(), secret) {
			t.Errorf("failure diagnostics contain secret %q: logs=%q error=%q", secret, logs.String(), err)
		}
	}
	if !strings.Contains(logs.String(), os.Args[0]) || !strings.Contains(logs.String(), "exit status 23") {
		t.Errorf("failure diagnostics = %q, want executable and exit status", logs.String())
	}
}

func TestRunnerResolvesBareNamesAndPreservesExplicitPaths(t *testing.T) {
	directory := t.TempDir()
	barePath := filepath.Join(directory, "bare-command")
	explicitPath := filepath.Join(directory, "explicit-command")
	for path, output := range map[string]string{barePath: "bare", explicitPath: "explicit"} {
		if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf '"+output+"'\n"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", directory)

	result, err := (Runner{}).Run(context.Background(), "bare-command")
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "bare" {
		t.Errorf("bare command output = %q, want %q", result.Stdout, "bare")
	}

	result, err = (Runner{}).Run(context.Background(), explicitPath)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "explicit" {
		t.Errorf("explicit command output = %q, want %q", result.Stdout, "explicit")
	}
}

func TestRunnerReportsTruncationAtBoundaries(t *testing.T) {
	path := t.TempDir() + "/output"
	body := `#!/bin/sh
case "$1" in
near) head -c 65535 /dev/zero | tr '\000' x ;;
exact) head -c 65536 /dev/zero | tr '\000' x ;;
over) head -c 65537 /dev/zero | tr '\000' x; head -c 65537 /dev/zero | tr '\000' y >&2 ;;
esac
`
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		truncated bool
	}{
		{name: "near"},
		{name: "exact"},
		{name: "over", truncated: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := (Runner{}).Run(context.Background(), path, test.name)
			if err != nil {
				t.Fatal(err)
			}
			if result.StdoutTruncated != test.truncated || result.StderrTruncated != test.truncated {
				t.Fatalf("truncation = stdout:%t stderr:%t, want both %t", result.StdoutTruncated, result.StderrTruncated, test.truncated)
			}
			if test.truncated && (!strings.HasSuffix(result.Stdout, "\n[output truncated]") || !strings.HasSuffix(result.Stderr, "\n[output truncated]")) {
				t.Fatalf("truncated output markers = stdout:%q stderr:%q", result.Stdout[len(result.Stdout)-32:], result.Stderr[len(result.Stderr)-32:])
			}
		})
	}
}

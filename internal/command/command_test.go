package command

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/bevicted/servitor/internal/diagnostics"
)

func TestRunnerLogsStartProcessFailureToDiagnosticWriter(t *testing.T) {
	logger, err := diagnostics.Open(t.TempDir(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := logger.Writer("111111111111111111111111", "U1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (Runner{Log: writer}).Run(context.Background(), t.TempDir()+"/missing-command"); err == nil {
		t.Fatal("Run succeeded for a missing executable")
	}
	segments, err := writer.Segments()
	if err != nil {
		t.Fatal(err)
	}
	var contents strings.Builder
	for _, segment := range segments {
		data, err := os.ReadFile(segment)
		if err != nil {
			t.Fatal(err)
		}
		contents.Write(data)
	}
	for _, want := range []string{"kind=argv", "missing-command", "kind=error", "start command:"} {
		if !strings.Contains(contents.String(), want) {
			t.Fatalf("diagnostic log missing %q: %s", want, contents.String())
		}
	}
}

func TestRunnerFlushesUnterminatedDiagnosticOutput(t *testing.T) {
	if os.Getenv("SERVITOR_COMMAND_DIAGNOSTIC_HELPER") == "1" {
		_, _ = os.Stdout.WriteString(`{"values":{"remote":"value"}}`)
		os.Exit(0)
	}
	t.Setenv("SERVITOR_COMMAND_DIAGNOSTIC_HELPER", "1")
	logger, err := diagnostics.Open(t.TempDir(), 200)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := logger.Writer("111111111111111111111111", "U1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (Runner{Log: writer}).Run(context.Background(), os.Args[0], "-test.run=^TestRunnerFlushesUnterminatedDiagnosticOutput$", "--"); err != nil {
		t.Fatal(err)
	}
	segments, err := writer.Segments()
	if err != nil {
		t.Fatal(err)
	}
	var contents strings.Builder
	for _, segment := range segments {
		data, err := os.ReadFile(segment)
		if err != nil {
			t.Fatal(err)
		}
		contents.Write(data)
	}
	if !strings.Contains(contents.String(), "kind=output {\"values\":{\"remote\":\"value\"}}\n") {
		t.Fatalf("unterminated output was not framed: %q", contents.String())
	}
}

func TestRunnerUsesArgumentVectorAndBoundsOutput(t *testing.T) {
	if os.Getenv("SERVITOR_COMMAND_HELPER") == "1" {
		if os.Args[len(os.Args)-1] != "literal;not-a-shell-command" {
			os.Exit(2)
		}
		os.Stdout.WriteString(strings.Repeat("x", 32))
		os.Stderr.WriteString(strings.Repeat("y", 32))
		return
	}
	t.Setenv("SERVITOR_COMMAND_HELPER", "1")
	runner := Runner{MaxOutput: 8}
	result, err := runner.Run(context.Background(), os.Args[0], "-test.run=TestRunnerUsesArgumentVectorAndBoundsOutput", "--", "literal;not-a-shell-command")
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "xxxxxxxx\n[output truncated]" || result.Stderr != "yyyyyyyy\n[output truncated]" || !result.StdoutTruncated || !result.StderrTruncated {
		t.Errorf("result = %#v", result)
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

package command

import (
	"context"
	"os"
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

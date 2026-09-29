package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCustomICTDoesNotReceiveParentEnvironment(t *testing.T) {
	const sentinel = "ict-api-key-sentinel"
	directory := t.TempDir()
	helper := filepath.Join(directory, "ict")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\nprintf '%s' \"${IC_API_KEY-unset}\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IC_API_KEY", sentinel)

	result, err := runICT(context.Background(), helper, 1024, io.Discard, io.Discard, nil, "version")
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "unset" {
		t.Fatalf("custom ICT read parent environment: %q", result.Stdout)
	}
}

func TestTrustedICTRunnerReceivesExplicitParentEnvironment(t *testing.T) {
	const sentinel = "ict-api-key-sentinel"
	t.Setenv("IC_API_KEY", sentinel)

	runner := ictRunner(trustedICTExecutable, 1024, io.Discard, io.Discard, nil)
	if runner.Env == nil {
		t.Fatal("trusted ICT runner has no explicit environment")
	}
	for _, entry := range runner.Env {
		if entry == "IC_API_KEY="+sentinel {
			return
		}
	}
	t.Fatal("trusted ICT runner did not receive parent environment")
}

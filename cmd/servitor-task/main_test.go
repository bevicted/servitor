package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
)

func TestRunDestroyUsesFrozenContextAndProducesValidatedReport(t *testing.T) {
	directory := t.TempDir()
	backendFile := filepath.Join(directory, "backend.json")
	recoveryFile := filepath.Join(directory, "recovery.json")
	resultFile := filepath.Join(directory, "result.json")
	reportFile := filepath.Join(directory, "report.json")
	if err := writeJSON(backendFile, backendConfig{Version: 1, Bucket: "bucket", Key: "key", Region: "us-south", Endpoint: "https://s3.example.invalid"}); err != nil {
		t.Fatal(err)
	}
	recovery := servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"}
	if err := writeJSON(recoveryFile, recovery); err != nil {
		t.Fatal(err)
	}
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\n[ \"$1\" = destroy ] && [ \"$2\" = destroy-a ] || exit 2\n[ -f \"$4\" ] || exit 3\nprintf '%s' '{\"version\":1,\"operation\":\"destroy\"}' > \"$8\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "frozen"}
	if err := runDestroy(context.Background(), "uid", "destroy-a", options, backendFile, recoveryFile, resultFile, reportFile, ict); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(reportFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.DecodeReport(data, "uid", "destroy-a"); err != nil {
		t.Fatalf("destroy report is not controller-valid: %v", err)
	}
	var context ictContext
	data, err = os.ReadFile(filepath.Join(directory, "context.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &context); err != nil || context.StateID != "destroy-a" || context.Recovery.TFVarsSHA256 != "digest" {
		t.Fatalf("destroy context = %+v, err=%v", context, err)
	}
}

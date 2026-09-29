package v1alpha1

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCleanupRecoveryStatusDeepCopyAndCRDSchema(t *testing.T) {
	completed := metav1.Now()
	cluster := &ServitorCluster{Status: ServitorClusterStatus{CleanupRecovery: &CleanupRecoveryStatus{
		Attempt:                2,
		ExecutionImage:         "registry.example/servitor-task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		OperationID:            "cleanup-recovery-id",
		RequestResourceVersion: "42",
		StartedAt:              completed,
		State:                  CleanupRecoveryFailed,
		CompletedAt:            &completed,
		History: []CleanupRecoveryAttempt{{
			ExecutionImage: "registry.example/servitor-task@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			OperationID:    "cleanup-recovery-first",
			Outcome:        CleanupRecoveryFailed,
			Timestamp:      completed,
		}},
	}}}
	copy := cluster.DeepCopy()
	copy.Status.CleanupRecovery.CompletedAt.Time = copy.Status.CleanupRecovery.CompletedAt.Time.AddDate(0, 0, 1)
	copy.Status.CleanupRecovery.History[0].OperationID = "changed"
	if cluster.Status.CleanupRecovery.CompletedAt.Equal(copy.Status.CleanupRecovery.CompletedAt) || cluster.Status.CleanupRecovery.History[0].OperationID == copy.Status.CleanupRecovery.History[0].OperationID {
		t.Fatal("DeepCopy() aliases cleanup recovery state")
	}

	contents, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "servitor.bevicted.github.io_servitorclusters.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(contents, &document); err != nil {
		t.Fatal(err)
	}
	versions := yamlList(t, yamlMap(t, document["spec"])["versions"])
	for _, value := range versions {
		version := yamlMap(t, value)
		if version["name"] != "v1alpha1" {
			continue
		}
		properties := yamlMap(t, yamlMap(t, yamlMap(t, version["schema"])["openAPIV3Schema"])["properties"])
		recovery := yamlMap(t, yamlMap(t, yamlMap(t, properties["status"])["properties"])["cleanupRecovery"])
		if recovery["type"] != "object" {
			t.Fatalf("cleanup recovery schema = %#v", recovery)
		}
		fields := yamlMap(t, recovery["properties"])
		attempt := yamlMap(t, fields["attempt"])
		image := yamlMap(t, fields["executionImage"])
		requestResourceVersion := yamlMap(t, fields["requestResourceVersion"])
		state := yamlMap(t, fields["state"])
		history := yamlMap(t, fields["history"])
		if attempt["maximum"] != 2 || image["pattern"] != "^.+@sha256:[a-f0-9]{64}$" || requestResourceVersion["maxLength"] != 128 || state["type"] != "string" || history["maxItems"] != 1 {
			t.Fatalf("cleanup recovery fields = %#v", fields)
		}
		return
	}
	t.Fatal("v1alpha1 CRD version not found")
}

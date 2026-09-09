package v1alpha1

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestServitorClusterSpecRequiresWholeHourInitialLease(t *testing.T) {
	spec := ServitorClusterSpec{
		Slack: SlackIdentity{OwnerID: "U1", ChannelID: "C1", ThreadTimestamp: "1.2"},
		Lifecycle: LifecyclePolicy{
			InitialLeaseSeconds: 3601,
			RetrySeconds:        []int64{60},
		},
	}
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "whole number of hours") {
		t.Fatalf("Validate() = %v, want whole-hour error", err)
	}
}

func TestServitorClusterDeepCopyPreservesLeaseStatus(t *testing.T) {
	expiry := metav1.Now()
	cluster := &ServitorCluster{Status: ServitorClusterStatus{
		LeaseExpiresAt: &expiry,
		LeaseExtension: &LeaseExtensionStatus{
			RequestedExpiry: expiry,
			PreviousExpiry:  &expiry,
			NewExpiry:       &expiry,
			Outcome:         ExtensionOutcomeApplied,
		},
	}}
	copy := cluster.DeepCopy()
	copy.Status.LeaseExpiresAt.Time = copy.Status.LeaseExpiresAt.Time.AddDate(0, 0, 1)
	copy.Status.LeaseExtension.NewExpiry.Time = copy.Status.LeaseExtension.NewExpiry.Time.AddDate(0, 0, 1)
	if cluster.Status.LeaseExpiresAt.Equal(copy.Status.LeaseExpiresAt) || cluster.Status.LeaseExtension.NewExpiry.Equal(copy.Status.LeaseExtension.NewExpiry) {
		t.Fatal("DeepCopy() aliases lease timestamp pointers")
	}
}

func TestServitorClusterDeepCopyDoesNotAliasSummaryActions(t *testing.T) {
	cluster := &ServitorCluster{Status: ServitorClusterStatus{
		Review: &ReviewSummary{Resources: []SummaryResource{{Actions: []string{"create"}}}},
		Ready:  &ReadySummary{Resources: []SummaryResource{{Actions: []string{"read"}}}},
	}}
	copy := cluster.DeepCopy()
	copy.Status.Review.Resources[0].Actions[0] = "delete"
	copy.Status.Ready.Resources[0].Actions[0] = "update"
	if cluster.Status.Review.Resources[0].Actions[0] != "create" || cluster.Status.Ready.Resources[0].Actions[0] != "read" {
		t.Fatalf("DeepCopy() aliases summary actions: %+v", cluster.Status)
	}
}

func TestServitorClusterCRDReadyResourcesIncludesActions(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "servitor.bevicted.github.io_servitorclusters.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	document := make(map[string]any)
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
		ready := yamlMap(t, properties["status"])
		ready = yamlMap(t, yamlMap(t, ready["properties"])["ready"])
		resources := yamlMap(t, yamlMap(t, ready["properties"])["resources"])
		actions := yamlMap(t, yamlMap(t, yamlMap(t, resources["items"])["properties"])["actions"])

		if actions["type"] != "array" || actions["maxItems"] != 2 {
			t.Fatalf("ready resource actions schema = %#v, want bounded array", actions)
		}
		if items := yamlMap(t, actions["items"]); items["type"] != "string" {
			t.Fatalf("ready resource actions items schema = %#v, want string", items)
		}
		return
	}
	t.Fatal("v1alpha1 CRD version not found")
}

func yamlMap(t *testing.T, value any) map[string]any {
	t.Helper()
	mapping, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("YAML value = %#v, want mapping", value)
	}
	return mapping
}

func yamlList(t *testing.T, value any) []any {
	t.Helper()
	list, ok := value.([]any)
	if !ok {
		t.Fatalf("YAML value = %#v, want sequence", value)
	}
	return list
}

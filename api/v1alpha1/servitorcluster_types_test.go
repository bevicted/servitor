package v1alpha1

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
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

func TestRecoveryValuesSatelliteRequiresOpenShift(t *testing.T) {
	values := RecoveryValues{
		ClusterName: "satellite-cluster", ResourceGroupName: "Default", Region: "us-south",
		ClusterMode: "satellite", Platform: "kubernetes", KubeVersion: "1.31", WorkerCount: 3,
		SatelliteZones: []string{"us-south-1", "us-south-2", "us-south-3"},
	}
	if err := values.validate(); err == nil {
		t.Fatal("Satellite recovery accepted Kubernetes")
	}
	values.Platform = "openshift"
	values.KubeVersion = "4.22_openshift"
	if err := values.validate(); err != nil {
		t.Fatalf("Satellite recovery rejected OpenShift: %v", err)
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

func TestCRDPrunesRemovedPlatformAndStrictTypedValidationRejectsIt(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "servitor.bevicted.github.io_servitorclusters.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(contents, &document); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := json.Unmarshal(encoded, &crd); err != nil {
		t.Fatal(err)
	}
	var schemaProps apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(crd.Spec.Versions[0].Schema.OpenAPIV3Schema, &schemaProps, nil); err != nil {
		t.Fatal(err)
	}
	structural, err := structuralschema.NewStructural(&schemaProps)
	if err != nil {
		t.Fatal(err)
	}
	object := map[string]any{
		"apiVersion": "servitor.bevicted.github.io/v1alpha1", "kind": "ServitorCluster",
		"metadata": map[string]any{"name": "example"},
		"spec": map[string]any{
			"slack":       map[string]any{"ownerID": "U1", "channelID": "C1", "threadTimestamp": "1.2"},
			"userOptions": map[string]any{"platform": "kubernetes", "version": "4.22"},
			"lifecycle":   map[string]any{"initialLeaseSeconds": int64(3600), "retrySeconds": []any{int64(60)}},
		},
	}
	var strict ServitorCluster
	if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructuredWithValidation(object, &strict, true); err == nil || !strings.Contains(err.Error(), `unknown field "spec.userOptions.platform"`) {
		t.Fatalf("strict direct CR conversion = %v, want removed platform field error", err)
	}
	unknown := pruning.PruneWithOptions(object, structural, true, structuralschema.UnknownFieldPathOptions{TrackUnknownFieldPaths: true})
	if !strings.Contains(strings.Join(unknown, ","), "spec.userOptions.platform") {
		t.Fatalf("structural pruning did not report userOptions.platform: %v", unknown)
	}
	if _, found := object["spec"].(map[string]any)["userOptions"].(map[string]any)["platform"]; found {
		t.Fatal("pruned user platform remained in the object")
	}
	if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructuredWithValidation(object, &strict, true); err != nil {
		t.Fatalf("pruned CR did not decode: %v", err)
	}
}

func TestExistingResolvedNameAndRecoveryContextRoundTrip(t *testing.T) {
	cluster := ServitorCluster{Status: ServitorClusterStatus{
		ResolvedOptions: &ResolvedOptions{Platform: "openshift", ClusterName: "servitor-existing"},
		Recovery:        &RecoveryMetadata{Values: RecoveryValues{ClusterName: "servitor-existing"}},
	}}
	data, err := json.Marshal(cluster)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ServitorCluster
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Status.ResolvedOptions.Platform != "openshift" || decoded.Status.ResolvedOptions.ClusterName != "servitor-existing" || decoded.Status.Recovery.Values.ClusterName != "servitor-existing" {
		t.Fatalf("serialized cleanup identity changed: %+v", decoded.Status)
	}
}

func yamlList(t *testing.T, value any) []any {
	t.Helper()
	list, ok := value.([]any)
	if !ok {
		t.Fatalf("YAML value = %#v, want sequence", value)
	}
	return list
}

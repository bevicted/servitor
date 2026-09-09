package rbac_test

import (
	"os"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestControllerRoleAllowsInventoryStateDeletion(t *testing.T) {
	data, err := os.ReadFile("role.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var role struct {
		Rules []struct {
			APIGroups []string `yaml:"apiGroups"`
			Resources []string `yaml:"resources"`
			Verbs     []string `yaml:"verbs"`
		} `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatal(err)
	}
	for _, rule := range role.Rules {
		if contains(rule.APIGroups, "") && contains(rule.Resources, "configmaps") && contains(rule.Verbs, "delete") {
			return
		}
	}
	t.Fatal("controller Role does not allow deleting inventory state ConfigMaps")
}

func contains(values []string, expected string) bool {
	return slices.Contains(values, expected)
}

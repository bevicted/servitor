package rbac_test

import (
	"os"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

type controllerRole struct {
	Rules []struct {
		APIGroups []string `yaml:"apiGroups"`
		Resources []string `yaml:"resources"`
		Verbs     []string `yaml:"verbs"`
	} `yaml:"rules"`
}

func TestControllerRoleAllowsInventoryStateDeletion(t *testing.T) {
	role := loadControllerRole(t)
	if !role.allows("", "configmaps", "delete") {
		t.Fatal("controller Role does not allow deleting inventory state ConfigMaps")
	}
}

func TestControllerRoleAllowsCachedPublisherResources(t *testing.T) {
	role := loadControllerRole(t)
	for _, resource := range []struct{ group, resource string }{{"", "serviceaccounts"}, {"rbac.authorization.k8s.io", "roles"}, {"rbac.authorization.k8s.io", "rolebindings"}} {
		for _, verb := range []string{"get", "list", "watch", "create", "delete"} {
			if !role.allows(resource.group, resource.resource, verb) {
				t.Errorf("controller Role does not allow %s %s in %q", verb, resource.resource, resource.group)
			}
		}
	}
	for _, verb := range []string{"list", "watch"} {
		if role.allows("", "secrets", verb) {
			t.Errorf("controller Role must not cache Secrets: allows %s", verb)
		}
	}
}

func loadControllerRole(t *testing.T) controllerRole {
	t.Helper()
	data, err := os.ReadFile("role.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var role controllerRole
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatal(err)
	}
	return role
}

func (role controllerRole) allows(group, resource, verb string) bool {
	for _, rule := range role.Rules {
		if contains(rule.APIGroups, group) && contains(rule.Resources, resource) && contains(rule.Verbs, verb) {
			return true
		}
	}
	return false
}

func contains(values []string, expected string) bool {
	return slices.Contains(values, expected)
}

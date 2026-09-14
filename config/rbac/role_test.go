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

func TestControllerRoleAllowsTerminalTaskRunDeletion(t *testing.T) {
	role := loadControllerRole(t)
	if !role.allows("tekton.dev", "taskruns", "delete") {
		t.Fatal("controller Role does not allow deleting terminal TaskRuns")
	}
	if role.allows("tekton.dev", "taskruns", "create") {
		t.Fatal("controller Role must not create TaskRuns directly")
	}
}

func TestControllerRoleUsesDirectPublisherLookups(t *testing.T) {
	role := loadControllerRole(t)
	for _, resource := range []struct{ group, resource string }{{"", "secrets"}, {"", "serviceaccounts"}, {"rbac.authorization.k8s.io", "roles"}, {"rbac.authorization.k8s.io", "rolebindings"}} {
		for _, verb := range []string{"get", "create", "delete"} {
			if !role.allows(resource.group, resource.resource, verb) {
				t.Errorf("controller Role does not allow %s %s in %q", verb, resource.resource, resource.group)
			}
		}
		for _, verb := range []string{"list", "watch"} {
			if role.allows(resource.group, resource.resource, verb) {
				t.Errorf("controller Role must not cache %s: allows %s", resource.resource, verb)
			}
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

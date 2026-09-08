// Package terraformview extracts only Slack-safe Terraform metadata.
package terraformview

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

type Resource struct {
	Address, Mode, Type, Name string
	ID, DisplayName           string
	Actions                   []string
	Reused                    bool
}

// Role returns the safe, provider-neutral label used in Slack presentations.
func (r Resource) Role() string {
	if role, found := resourceRoles[r.Type]; found {
		return role
	}
	return "Resource"
}

type Plan struct{ Resources []Resource }
type State struct{ Resources []Resource }

const maxReportableResources = 256

type document struct {
	FormatVersion   string   `json:"format_version"`
	ResourceChanges []change `json:"resource_changes"`
	Values          struct {
		RootModule module `json:"root_module"`
	} `json:"values"`
}
type change struct {
	Address, Mode, Type, Name string
	Change                    struct {
		Actions        []string        `json:"actions"`
		After          json.RawMessage `json:"after"`
		AfterSensitive json.RawMessage `json:"after_sensitive"`
	} `json:"change"`
}
type module struct {
	Resources    []value  `json:"resources"`
	ChildModules []module `json:"child_modules"`
}
type value struct {
	Address, Mode, Type, Name string
	Values                    json.RawMessage `json:"values"`
}

// ParsePlan accepts supported Terraform JSON and exposes no values or sensitive fields.
func ParsePlan(data []byte) (Plan, error) {
	var d document
	if err := decode(data, &d); err != nil {
		return Plan{}, err
	}
	if err := validateFormatVersion(d.FormatVersion); err != nil {
		return Plan{}, fmt.Errorf("Terraform plan JSON: %w", err)
	}
	if len(d.ResourceChanges) > maxReportableResources {
		return Plan{}, fmt.Errorf("Terraform plan contains too many resource changes")
	}
	resources := make([]Resource, 0, len(d.ResourceChanges))
	for _, c := range d.ResourceChanges {
		if c.Address == "" || c.Type == "" || c.Name == "" || !reportableType(c.Type) || !validMode(c.Mode) || !validActions(c.Change.Actions) {
			return Plan{}, fmt.Errorf("unsupported Terraform resource change")
		}
		if sensitiveIdentity(c.Change.AfterSensitive) {
			return Plan{}, fmt.Errorf("Terraform plan contains sensitive resource identity")
		}
		id, displayName, err := safeIdentity(c.Change.After)
		if err != nil {
			return Plan{}, fmt.Errorf("Terraform plan resource identity: %w", err)
		}
		resources = append(resources, Resource{Address: c.Address, Mode: c.Mode, Type: c.Type, Name: c.Name, ID: id, DisplayName: displayName, Actions: append([]string(nil), c.Change.Actions...)})
	}
	sortResources(resources)
	return Plan{Resources: resources}, nil
}

// ParseState reads only resource identity metadata. Data resources are marked reused.
func ParseState(data []byte) (State, error) {
	var d document
	if err := decode(data, &d); err != nil {
		return State{}, err
	}
	if err := validateFormatVersion(d.FormatVersion); err != nil {
		return State{}, fmt.Errorf("Terraform state JSON: %w", err)
	}
	var resources []Resource
	if err := collect(&resources, d.Values.RootModule); err != nil {
		return State{}, err
	}
	sortResources(resources)
	return State{Resources: resources}, nil
}
func collect(out *[]Resource, m module) error {
	for _, r := range m.Resources {
		if r.Address == "" || r.Type == "" || r.Name == "" || !reportableType(r.Type) {
			continue
		}
		id, displayName, err := safeIdentity(r.Values)
		if err != nil {
			continue
		}
		if len(*out) == maxReportableResources {
			return fmt.Errorf("Terraform state contains too many reportable resources")
		}
		*out = append(*out, Resource{Address: r.Address, Mode: r.Mode, Type: r.Type, Name: r.Name, ID: id, DisplayName: displayName, Reused: r.Mode == "data"})
	}
	for _, child := range m.ChildModules {
		if err := collect(out, child); err != nil {
			return err
		}
	}
	return nil
}

var resourceRoles = map[string]string{
	"ibm_resource_group":                      "Resource Group",
	"ibm_container_cluster":                   "Cluster",
	"ibm_container_vpc_cluster":               "Cluster",
	"ibm_is_vpc":                              "VPC",
	"ibm_is_subnet":                           "Subnet",
	"ibm_is_public_gateway":                   "Public Gateway",
	"ibm_is_public_gateways":                  "Public Gateway",
	"ibm_is_subnet_public_gateway_attachment": "Gateway Attachment",
	"ibm_satellite_attach_host_script":        "Host Attachment",
	"ibm_satellite_cluster":                   "Cluster",
	"ibm_satellite_host":                      "Satellite Host",
	"ibm_satellite_location":                  "Satellite Location",
}

func reportableType(resourceType string) bool {
	_, found := resourceRoles[resourceType]
	return found
}

func validMode(mode string) bool { return mode == "managed" || mode == "data" }

func validActions(actions []string) bool {
	if len(actions) == 0 || len(actions) > 2 {
		return false
	}
	for _, action := range actions {
		switch action {
		case "create", "update", "delete", "read", "no-op":
		default:
			return false
		}
	}
	return true
}

// sensitiveIdentity rejects plans that mark metadata Servitor may display as
// sensitive. Other sensitive Terraform fields are not read or exposed.
func sensitiveIdentity(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return true
	}
	for _, key := range []string{"id", "name"} {
		rawValue, found := values[key]
		if !found || bytes.Equal(rawValue, []byte("null")) {
			continue
		}
		var sensitive bool
		if err := json.Unmarshal(rawValue, &sensitive); err != nil || sensitive {
			return true
		}
	}
	return false
}

func safeIdentity(raw json.RawMessage) (string, string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", "", nil
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return "", "", fmt.Errorf("decode identity object")
	}
	read := func(key string) (string, error) {
		rawValue, found := values[key]
		if !found || bytes.Equal(rawValue, []byte("null")) {
			return "", nil
		}
		var value string
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return "", fmt.Errorf("decode %s", key)
		}
		return value, nil
	}
	id, err := read("id")
	if err != nil {
		return "", "", err
	}
	name, err := read("name")
	if err != nil {
		return "", "", err
	}
	return id, name, nil
}
func sortResources(resources []Resource) {
	sort.Slice(resources, func(i, j int) bool { return resources[i].Address < resources[j].Address })
}
func validateFormatVersion(version string) error {
	switch version {
	case "1.0", "1.1", "1.2":
		return nil
	case "":
		return fmt.Errorf("no format_version")
	default:
		return fmt.Errorf("unsupported format_version %q", version)
	}
}

func decode(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("decode Terraform JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return fmt.Errorf("decode Terraform JSON: trailing JSON")
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode Terraform JSON: %w", err)
	}
	return nil
}

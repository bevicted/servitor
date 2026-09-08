package command

import (
	"fmt"
	"strings"
	"unicode"
)

// CreateDefaults are operator-controlled values supplied to every create.
type CreateDefaults struct {
	Version, Target, Provider, ResourceGroup, Zone, VPCID string
	OpenShiftFlavor, KubernetesFlavor                     string
}

// CreateRequest is the validated, shell-free provisioning option vector.
type CreateRequest struct {
	Target, Platform, Version, Provider, ResourceGroup      string
	WorkerShape, WorkerCount                                string
	Location, Zone, Datacenter                              string
	SatelliteZones                                          []string
	VPCID, PublicVLANID, PrivateVLANID, SatelliteLocationID string
	SubnetIDs, PublicGatewayIDs                             []string
	ReuseVPC, ReuseSubnet, ReuseGateway                     bool
	Name                                                    string
	Args                                                    []string
}

func normalizedLocation(provider, zone, datacenter string, satelliteZones []string) string {
	switch provider {
	case "vpc-gen2":
		if index := strings.LastIndex(zone, "-"); index > 0 {
			return zone[:index] + "/" + zone
		}
		return zone
	case "classic":
		return datacenter
	case "satellite":
		return strings.Join(satelliteZones, ", ")
	default:
		return ""
	}
}

var createFlags = map[string]bool{
	"--target": true, "--provider": true, "--platform": true, "--version": true,
	"--resource-group": true, "--zone": true, "--flavor": true, "--vpc-id": true,
	"--subnet-id": true, "--public-gateway-id": true, "--datacenter": true,
	"--machine-type": true, "--public-vlan-id": true, "--private-vlan-id": true,
	"--satellite-zone": true, "--satellite-managed-from": true, "--satellite-location-id": true,
	"--satellite-host-image": true, "--satellite-host-profile": true,
	"--satellite-ssh-key-id": true, "--satellite-worker-instance-id": true,
	"--satellite-worker-operating-system": true, "--worker-count": true, "--name": true,
}
var repeatableCreateFlags = map[string]bool{"--subnet-id": true, "--public-gateway-id": true, "--satellite-zone": true, "--satellite-worker-instance-id": true}
var forbiddenCreateFlags = map[string]bool{"--config": true, "--owner": true, "--prefix": true, "--auto-approve": true, "--confirm-stdin": true, "--satellite-ssh-public-key": true}

// ExplicitCreateOptions contains only user-supplied, safe options. It never includes defaults.
type ExplicitCreateOptions struct{ values map[string][]string }

// Values returns a copy of the explicitly supplied safe flag values.
func (o ExplicitCreateOptions) Values() map[string][]string {
	values := make(map[string][]string, len(o.values))
	for flag, supplied := range o.values {
		values[flag] = append([]string(nil), supplied...)
	}
	return values
}

// ParseCreateOptions validates the safe CLI grammar without applying operator defaults.
func ParseCreateOptions(text string) (ExplicitCreateOptions, error) {
	words, err := splitWords(text)
	if err != nil {
		return ExplicitCreateOptions{}, err
	}
	if len(words) == 0 || words[0] != "create" {
		return ExplicitCreateOptions{}, fmt.Errorf("command must start with create")
	}
	seen, values := map[string]bool{}, map[string][]string{}
	for i := 1; i < len(words); {
		flag, value, joined := strings.Cut(words[i], "=")
		if forbiddenCreateFlags[flag] {
			return ExplicitCreateOptions{}, fmt.Errorf("%s is not permitted", flag)
		}
		if !createFlags[flag] {
			return ExplicitCreateOptions{}, fmt.Errorf("unknown create flag %q", flag)
		}
		if !joined {
			if i+1 == len(words) || strings.HasPrefix(words[i+1], "--") {
				return ExplicitCreateOptions{}, fmt.Errorf("%s requires a value", flag)
			}
			value, i = words[i+1], i+2
		} else {
			if value == "" {
				return ExplicitCreateOptions{}, fmt.Errorf("%s requires a value", flag)
			}
			i++
		}
		if seen[flag] && !repeatableCreateFlags[flag] {
			return ExplicitCreateOptions{}, fmt.Errorf("%s may only be supplied once", flag)
		}
		seen[flag] = true
		values[flag] = append(values[flag], value)
	}
	return ExplicitCreateOptions{values: values}, nil
}

// ParseCreate remains the compatibility helper for callers that immediately resolve defaults.
func ParseCreate(text string, defaults CreateDefaults) (CreateRequest, error) {
	options, err := ParseCreateOptions(text)
	if err != nil {
		return CreateRequest{}, err
	}
	return ResolveCreateOptions(options, defaults)
}

// ResolveCreateOptions overlays startup defaults once onto explicit options.
func ResolveCreateOptions(options ExplicitCreateOptions, defaults CreateDefaults) (CreateRequest, error) {
	values := make(map[string][]string, len(options.values))
	for flag, supplied := range options.values {
		values[flag] = append([]string(nil), supplied...)
	}
	version := one(values, "--version")
	if version == "" {
		version = defaults.Version
	}
	if version == "" {
		return CreateRequest{}, fmt.Errorf("create requires --version VERSION")
	}
	inferredPlatform, err := InferPlatform(version)
	if err != nil {
		return CreateRequest{}, fmt.Errorf("infer create platform: %w", err)
	}
	values["--version"] = []string{version}
	platform := one(values, "--platform")
	if platform == "" {
		platform = inferredPlatform
	}
	if platform != "openshift" && platform != "kubernetes" {
		return CreateRequest{}, fmt.Errorf("invalid platform %q", platform)
	}
	if platform != inferredPlatform {
		return CreateRequest{}, fmt.Errorf("platform %q does not match version %q", platform, version)
	}
	values["--platform"] = []string{platform}
	setDefault := func(flag, value string) {
		if one(values, flag) == "" {
			values[flag] = []string{value}
		}
	}
	setDefault("--target", defaults.Target)
	setDefault("--provider", defaults.Provider)
	setDefault("--resource-group", defaults.ResourceGroup)
	if one(values, "--provider") == "vpc-gen2" {
		setDefault("--zone", defaults.Zone)
		setDefault("--vpc-id", defaults.VPCID)
		if platform == "openshift" {
			setDefault("--flavor", defaults.OpenShiftFlavor)
		} else {
			setDefault("--flavor", defaults.KubernetesFlavor)
		}
	}
	ordered := []string{"--target", "--provider", "--platform", "--version", "--resource-group", "--zone", "--flavor", "--vpc-id", "--subnet-id", "--public-gateway-id", "--datacenter", "--machine-type", "--public-vlan-id", "--private-vlan-id", "--satellite-zone", "--satellite-managed-from", "--satellite-location-id", "--satellite-host-image", "--satellite-host-profile", "--satellite-ssh-key-id", "--satellite-worker-instance-id", "--satellite-worker-operating-system", "--worker-count", "--name"}
	args := make([]string, 0, len(values)*2+12)
	for _, flag := range ordered {
		for _, value := range values[flag] {
			args = append(args, flag, value)
		}
	}
	return CreateRequest{
		Target:              one(values, "--target"),
		Platform:            platform,
		Version:             version,
		Provider:            one(values, "--provider"),
		ResourceGroup:       one(values, "--resource-group"),
		WorkerShape:         first(one(values, "--flavor"), one(values, "--machine-type"), one(values, "--satellite-host-profile")),
		WorkerCount:         one(values, "--worker-count"),
		Zone:                one(values, "--zone"),
		Datacenter:          one(values, "--datacenter"),
		SatelliteZones:      append([]string(nil), values["--satellite-zone"]...),
		VPCID:               one(values, "--vpc-id"),
		PublicVLANID:        one(values, "--public-vlan-id"),
		PrivateVLANID:       one(values, "--private-vlan-id"),
		SatelliteLocationID: one(values, "--satellite-location-id"),
		SubnetIDs:           append([]string(nil), values["--subnet-id"]...),
		PublicGatewayIDs:    append([]string(nil), values["--public-gateway-id"]...),
		ReuseVPC:            one(values, "--vpc-id") != "",
		ReuseSubnet:         len(values["--subnet-id"]) != 0,
		ReuseGateway:        len(values["--public-gateway-id"]) != 0,
		Name:                one(values, "--name"),
		Location:            normalizedLocation(one(values, "--provider"), one(values, "--zone"), one(values, "--datacenter"), values["--satellite-zone"]),
		Args:                args,
	}, nil
}

func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// InferPlatform validates a supported version prefix and returns its platform.
func InferPlatform(version string) (platform string, err error) {
	if strings.HasPrefix(version, "4.") {
		return "openshift", nil
	}
	if strings.HasPrefix(version, "1.") {
		return "kubernetes", nil
	}
	return "", fmt.Errorf("cannot infer platform from version %q", version)
}

func one(values map[string][]string, flag string) string {
	if len(values[flag]) == 0 {
		return ""
	}
	return values[flag][0]
}

func splitWords(text string) ([]string, error) {
	var words []string
	var word strings.Builder
	var quote rune
	escaped := false
	flush := func() {
		if word.Len() > 0 {
			words = append(words, word.String())
			word.Reset()
		}
	}
	for _, r := range text {
		if escaped {
			word.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			} else {
				word.WriteRune(r)
			}
			continue
		}
		if r == '\'' || r == '"' {
			quote = r
			continue
		}
		if strings.ContainsRune(";|&<>$`", r) {
			return nil, fmt.Errorf("shell operators are not allowed")
		}
		if unicode.IsSpace(r) {
			flush()
			continue
		}
		word.WriteRune(r)
	}
	if escaped || quote != 0 {
		return nil, fmt.Errorf("unterminated quoted argument")
	}
	flush()
	return words, nil
}

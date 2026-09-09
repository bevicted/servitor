package command

import (
	"fmt"

	"github.com/bevicted/servitor/internal/inventory"
)

// ResolveBareSelectors promotes configured target and provider shorthand before
// catalog-dependent values are matched. Defaults remain omitted from options.
func ResolveBareSelectors(options ExplicitCreateOptions, defaults CreateDefaults, targets map[string]inventory.TargetConfig) (ExplicitCreateOptions, inventory.TargetConfig, error) {
	values := options.Values()
	bare := append([]string(nil), options.bare...)
	set := func(flag, value string) error {
		if one(values, flag) != "" {
			return fmt.Errorf("%s may only be supplied once", flag)
		}
		values[flag] = []string{value}
		return nil
	}

	for index := 0; index < len(bare); {
		if _, ok := targets[bare[index]]; !ok {
			index++
			continue
		}
		if err := set("--target", bare[index]); err != nil {
			return ExplicitCreateOptions{}, inventory.TargetConfig{}, err
		}
		bare = append(bare[:index], bare[index+1:]...)
	}
	target := one(values, "--target")
	if target == "" {
		target = defaults.Target
	}
	targetConfig, ok := targets[target]
	if !ok {
		return ExplicitCreateOptions{}, inventory.TargetConfig{}, fmt.Errorf("target is not configured; use target=value")
	}
	for index := 0; index < len(bare); {
		if !contains(targetConfig.Providers, bare[index]) {
			index++
			continue
		}
		if err := set("--provider", bare[index]); err != nil {
			return ExplicitCreateOptions{}, inventory.TargetConfig{}, err
		}
		bare = append(bare[:index], bare[index+1:]...)
	}
	provider := one(values, "--provider")
	if provider == "" {
		provider = defaults.Provider
	}
	if !contains(targetConfig.Providers, provider) {
		return ExplicitCreateOptions{}, inventory.TargetConfig{}, fmt.Errorf("provider is not configured for the selected target; use provider=value")
	}
	return ExplicitCreateOptions{values: values, bare: bare}, targetConfig, nil
}

// MatchBareCreateOptions promotes uniquely recognized common catalog values.
// It expects selectors to be resolved first and never applies defaults to the
// returned explicit options.
func MatchBareCreateOptions(options ExplicitCreateOptions, defaults CreateDefaults, catalog inventory.Catalog) (ExplicitCreateOptions, error) {
	if len(options.bare) == 0 {
		return options, nil
	}
	values := options.Values()
	provider := one(values, "--provider")
	if provider == "" {
		provider = defaults.Provider
	}
	if !contains(catalog.Providers, provider) {
		return ExplicitCreateOptions{}, fmt.Errorf("provider is not configured for the selected target; use provider=value")
	}

	zone, datacenter := one(values, "--zone"), one(values, "--datacenter")
	for _, value := range options.bare {
		switch provider {
		case "vpc-gen2":
			if hasLocation(catalog.VPCLocations, value) {
				if zone != "" && zone != value {
					return ExplicitCreateOptions{}, fmt.Errorf("--zone may only be supplied once")
				}
				zone = value
			}
		case "classic":
			if hasLocation(catalog.ClassicLocations, value) {
				if datacenter != "" && datacenter != value {
					return ExplicitCreateOptions{}, fmt.Errorf("--datacenter may only be supplied once")
				}
				datacenter = value
			}
		}
	}
	if zone == "" {
		zone = defaults.Zone
	}

	for _, value := range options.bare {
		roles := matchingRoles(catalog, provider, zone, datacenter, value)
		if len(roles) == 0 {
			return ExplicitCreateOptions{}, fmt.Errorf("unknown shorthand value; use an explicit key such as resource-group=value")
		}
		if len(roles) != 1 {
			return ExplicitCreateOptions{}, fmt.Errorf("ambiguous shorthand value; use an explicit key such as resource-group=value")
		}
		flag := roles[0]
		if one(values, flag) != "" {
			return ExplicitCreateOptions{}, fmt.Errorf("%s may only be supplied once", flag)
		}
		values[flag] = []string{value}
	}
	return ExplicitCreateOptions{values: values}, nil
}

func matchingRoles(catalog inventory.Catalog, provider, zone, datacenter, value string) []string {
	roles := make([]string, 0, 3)
	for _, group := range catalog.ResourceGroups {
		if group == value {
			roles = append(roles, "--resource-group")
		}
	}
	switch provider {
	case "vpc-gen2":
		for _, location := range catalog.VPCLocations {
			if location.Name == value {
				roles = append(roles, "--zone")
			}
			if location.Name == zone {
				for _, flavor := range location.Flavors {
					if flavor == value {
						roles = append(roles, "--flavor")
					}
				}
			}
		}
	case "classic":
		for _, location := range catalog.ClassicLocations {
			if location.Name == value {
				roles = append(roles, "--datacenter")
			}
			if location.Name == datacenter {
				for _, machineType := range location.Flavors {
					if machineType == value {
						roles = append(roles, "--machine-type")
					}
				}
			}
		}
	case "satellite":
		for _, profile := range catalog.SatelliteProfile {
			if profile.Name == value {
				roles = append(roles, "--satellite-host-profile")
			}
		}
	}
	return roles
}

func hasLocation(locations []inventory.Location, value string) bool {
	for _, location := range locations {
		if location.Name == value {
			return true
		}
	}
	return false
}

func contains(values []string, value string) bool {
	for _, current := range values {
		if current == value {
			return true
		}
	}
	return false
}

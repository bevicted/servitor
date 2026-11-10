package command

import (
	"errors"
	"fmt"
	"strings"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
)

// ResolutionDefaults are the operator-controlled inputs shared by create
// previews and allocation snapshots.
type ResolutionDefaults struct {
	Defaults                          servitorv1alpha1.ResolvedOptions
	NetworkBindings                   map[string]servitorv1alpha1.FrozenNetwork
	OpenShiftFlavor, KubernetesFlavor string
}

// ResolveRequestOptions applies the local defaults and network policy that an
// allocation snapshot freezes. It does not create backend or lifecycle state.
func ResolveRequestOptions(supplied servitorv1alpha1.UserOptions, settings ResolutionDefaults) (servitorv1alpha1.ResolvedOptions, error) {
	resolved := settings.Defaults
	resolved.Platform = ""
	resolved.Flavor = ""
	overlayUserOptions(&resolved.UserOptions, supplied)
	if resolved.Provider == "satellite" {
		return servitorv1alpha1.ResolvedOptions{}, errors.New("Satellite provisioning is not supported")
	}
	if resolved.PrivateOnly && resolved.Provider != "vpc-gen2" {
		return servitorv1alpha1.ResolvedOptions{}, errors.New("private-only is supported only for VPC Gen 2")
	}
	platform, err := InferPlatform(resolved.Version)
	if err != nil {
		return servitorv1alpha1.ResolvedOptions{}, fmt.Errorf("infer platform: %w", err)
	}
	resolved.Platform = platform
	if resolved.Headlamp && (platform != "kubernetes" || (resolved.Provider != "vpc-gen2" && resolved.Provider != "classic")) {
		return servitorv1alpha1.ResolvedOptions{}, errors.New("headlamp requires Kubernetes on VPC Gen 2 or Classic")
	}
	if resolved.Provider == "vpc-gen2" {
		network, found := settings.NetworkBindings[resolved.Target]
		if !found {
			return servitorv1alpha1.ResolvedOptions{}, errors.New("no configured existing network binding for target")
		}
		if network.VPCRegion == "" {
			if separator := strings.LastIndex(network.Zone, "-"); separator > 0 {
				network.VPCRegion = network.Zone[:separator]
			}
		}
		if network.AuthPolicy != nil {
			policy := *network.AuthPolicy
			if err := policy.Validate(); err != nil {
				return servitorv1alpha1.ResolvedOptions{}, fmt.Errorf("invalid frozen auth policy: %w", err)
			}
			network.AuthPolicy = &policy
		}
		if !validFrozenNetwork(network) {
			return servitorv1alpha1.ResolvedOptions{}, errors.New("no configured existing network binding for target")
		}
		if resolved.PrivateOnly && network.AuthPolicy == nil {
			return servitorv1alpha1.ResolvedOptions{}, errors.New("private-only requires configured VPN authentication policy")
		}
		if resolved.Zone != "" && resolved.Zone != network.Zone {
			return servitorv1alpha1.ResolvedOptions{}, errors.New("requested zone is not configured for the selected network binding")
		}
		resolved.Zone = network.Zone
		resolved.Network = network
	}
	if resolved.Provider == "vpc-gen2" && resolved.Flavor == "" {
		if resolved.Platform == "openshift" {
			resolved.Flavor = settings.OpenShiftFlavor
		} else {
			resolved.Flavor = settings.KubernetesFlavor
		}
	}
	if resolved.Provider == "classic" {
		resolved.Network = servitorv1alpha1.FrozenNetwork{}
	}
	return resolved, nil
}

func validFrozenNetwork(network servitorv1alpha1.FrozenNetwork) bool {
	return network.BindingID != "" && network.AccountID != "" && network.VPCID != "" && network.SubnetID != "" && network.PublicGatewayID != "" && network.Zone != ""
}

func overlayUserOptions(dst *servitorv1alpha1.UserOptions, supplied servitorv1alpha1.UserOptions) {
	if supplied.Target != "" {
		dst.Target = supplied.Target
	}
	if supplied.Provider != "" {
		dst.Provider = supplied.Provider
	}
	if supplied.Version != "" {
		dst.Version = supplied.Version
	}
	if supplied.Headlamp {
		dst.Headlamp = true
	}
	if supplied.PrivateOnly {
		dst.PrivateOnly = true
	}
	if supplied.Zone != "" {
		dst.Zone = supplied.Zone
	}
	if supplied.Flavor != "" {
		dst.Flavor = supplied.Flavor
	}
	if supplied.Datacenter != "" {
		dst.Datacenter = supplied.Datacenter
	}
	if supplied.MachineType != "" {
		dst.MachineType = supplied.MachineType
	}
	if supplied.PublicVLANID != "" {
		dst.PublicVLANID = supplied.PublicVLANID
	}
	if supplied.PrivateVLANID != "" {
		dst.PrivateVLANID = supplied.PrivateVLANID
	}
	if supplied.SatelliteManagedFrom != "" {
		dst.SatelliteManagedFrom = supplied.SatelliteManagedFrom
	}
	if supplied.SatelliteLocationID != "" {
		dst.SatelliteLocationID = supplied.SatelliteLocationID
	}
	if supplied.SatelliteHostImage != "" {
		dst.SatelliteHostImage = supplied.SatelliteHostImage
	}
	if supplied.SatelliteHostProfile != "" {
		dst.SatelliteHostProfile = supplied.SatelliteHostProfile
	}
	if supplied.SatelliteSSHKeyID != "" {
		dst.SatelliteSSHKeyID = supplied.SatelliteSSHKeyID
	}
	if supplied.SatelliteWorkerOperatingSystem != "" {
		dst.SatelliteWorkerOperatingSystem = supplied.SatelliteWorkerOperatingSystem
	}
	if supplied.WorkerCount != 0 {
		dst.WorkerCount = supplied.WorkerCount
	}
	if supplied.SatelliteZones != nil {
		dst.SatelliteZones = append([]string(nil), supplied.SatelliteZones...)
	}
	if supplied.SatelliteWorkerInstanceIDs != nil {
		dst.SatelliteWorkerInstanceIDs = append([]string(nil), supplied.SatelliteWorkerInstanceIDs...)
	}
}

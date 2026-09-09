package main

import (
	"testing"

	"github.com/bevicted/servitor/internal/config"
)

func TestControllerConfigCarriesAllStartupDefaultInputs(t *testing.T) {
	settings, err := controllerConfig(config.Config{Defaults: config.DefaultsConfig{
		Version: "4.22", Target: "target", Provider: "vpc-gen2", ResourceGroup: "Default", Zone: "us-south-1", VPCID: "vpc",
		OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if settings.Defaults.Version != "4.22" || settings.Defaults.Target != "target" || settings.Defaults.Provider != "vpc-gen2" || settings.Defaults.ResourceGroup != "Default" || settings.Defaults.Zone != "us-south-1" || settings.Defaults.VPCID != "vpc" || settings.OpenShiftFlavor != "bx2.4x16" || settings.KubernetesFlavor != "bx2.2x8" {
		t.Fatalf("controller startup defaults = %+v", settings)
	}
}

func TestControllerConfigRejectsUnsupportedDefaultVersion(t *testing.T) {
	if _, err := controllerConfig(config.Config{Defaults: config.DefaultsConfig{Version: "unsupported"}}); err == nil {
		t.Fatal("controller config accepted unsupported default version")
	}
}

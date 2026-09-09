package main

import (
	"testing"
	"time"

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

func TestInventoryControllerConfigCarriesValidatedPolicy(t *testing.T) {
	settings := inventoryControllerConfig(config.Config{
		Namespace: "servitor", ICT: config.ICTConfig{TargetConfigMap: "servitor-ict-config", TargetConfigKey: "config.yaml"},
		Images: config.ImagesConfig{Execution: "registry.example.invalid/task@sha256:deadbeef"}, Secrets: config.SecretsConfig{IBM: "servitor-ibm"},
		Inventory: config.InventoryConfig{RefreshInterval: 2 * time.Hour, MaximumAge: 48 * time.Hour},
	})
	if settings.RefreshInterval != 2*time.Hour || settings.MaximumAge != 48*time.Hour || settings.TaskConfig.COSSecret != "" || settings.TaskConfig.IBMSecret != "servitor-ibm" {
		t.Fatalf("inventory settings = %+v", settings)
	}
}

func TestControllerConfigRejectsUnsupportedDefaultVersion(t *testing.T) {
	if _, err := controllerConfig(config.Config{Defaults: config.DefaultsConfig{Version: "unsupported"}}); err == nil {
		t.Fatal("controller config accepted unsupported default version")
	}
}

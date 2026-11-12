package main

import (
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/config"
	"github.com/bevicted/servitor/internal/controller"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestControllerConfigCarriesAllStartupDefaultInputs(t *testing.T) {
	settings, err := controllerConfig(config.Config{Defaults: config.DefaultsConfig{
		Version: "4.22", Target: "target", Provider: "vpc-gen2", ResourceGroup: "Default",
		OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8",
	}, Network: config.NetworkConfig{Bindings: map[string]config.NetworkBinding{"binding": {AccountID: "account", VPCID: "vpc", SubnetID: "subnet", PublicGatewayID: "gateway", Zone: "us-south-1"}}, TargetBindings: map[string]string{"target": "binding"}}, Auth: config.AuthConfig{PublicTargets: []string{"target"}}})
	if err != nil {
		t.Fatal(err)
	}
	if settings.Defaults.Version != "4.22" || settings.Defaults.Target != "target" || settings.Defaults.Provider != "vpc-gen2" || settings.Defaults.ResourceGroup != "Default" || settings.NetworkBindings["target"].VPCID != "vpc" || settings.NetworkBindings["target"].Zone != "us-south-1" || settings.OpenShiftFlavor != "bx2.4x16" || settings.KubernetesFlavor != "bx2.2x8" || len(settings.PublicAuthTargets) != 1 || settings.PublicAuthTargets[0] != "target" {
		t.Fatalf("controller startup defaults = %+v", settings)
	}
}

func TestControllerSchemeRegistersRBACResources(t *testing.T) {
	scheme, err := controller.NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	role, err := scheme.New(rbacv1.SchemeGroupVersion.WithKind("Role"))
	if err != nil {
		t.Fatalf("resolve Role GVK: %v", err)
	}
	if _, ok := role.(*rbacv1.Role); !ok {
		t.Errorf("Role GVK resolved to %T", role)
	}
	binding, err := scheme.New(rbacv1.SchemeGroupVersion.WithKind("RoleBinding"))
	if err != nil {
		t.Fatalf("resolve RoleBinding GVK: %v", err)
	}
	if _, ok := binding.(*rbacv1.RoleBinding); !ok {
		t.Errorf("RoleBinding GVK resolved to %T", binding)
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

func TestNewSlackBotUsesDirectAPIReader(t *testing.T) {
	cached := fake.NewClientBuilder().Build()
	reader := fake.NewClientBuilder().Build()
	resolution := controller.Config{Defaults: servitorv1alpha1.ResolvedOptions{ResourceGroup: "Default"}, NetworkBindings: map[string]servitorv1alpha1.FrozenNetwork{"target": {Zone: "us-south-1"}}, OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"}
	bot := newSlackBot(config.Config{Namespace: "servitor", Slack: config.SlackConfig{ChannelID: "C1", MaxAllocationsPerUser: 7}}, resolution, cached, reader, nil, nil)
	direct, ok := bot.Client.(allocationClient)
	if !ok || direct.Client != cached || direct.AllocationReader() != reader || bot.Namespace != "servitor" || bot.ChannelID != "C1" || bot.MaxAllocationsPerUser != 7 || bot.Resolution.Defaults.ResourceGroup != "Default" || bot.Resolution.NetworkBindings["target"].Zone != "us-south-1" || bot.Resolution.OpenShiftFlavor != "bx2.4x16" || bot.Resolution.KubernetesFlavor != "bx2.2x8" {
		t.Fatalf("Slack bot wiring = %+v", bot)
	}
}

func TestControllerConfigRejectsUnsupportedDefaultVersion(t *testing.T) {
	if _, err := controllerConfig(config.Config{Defaults: config.DefaultsConfig{Version: "unsupported"}}); err == nil {
		t.Fatal("controller config accepted unsupported default version")
	}
}

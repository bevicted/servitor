package command

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

var testCreateDefaults = CreateDefaults{
	Version:          "4.22",
	Target:           "synthetic-target",
	Provider:         "vpc-gen2",
	ResourceGroup:    "Default",
	Zone:             "us-south-1",
	VPCID:            "synthetic-vpc-id",
	OpenShiftFlavor:  "bx2.4x16",
	KubernetesFlavor: "bx2.2x8",
}

func TestParseExtendAcceptsOnlyWholeHourGrammar(t *testing.T) {
	for _, test := range []struct {
		text string
		want time.Duration
	}{
		{"extend", 0}, {"extend 1", time.Hour}, {"extend 24", 24 * time.Hour}, {"extend 2h", 2 * time.Hour},
	} {
		got, err := ParseExtend(test.text)
		if err != nil || got != test.want {
			t.Fatalf("ParseExtend(%q) = %s, %v; want %s, nil", test.text, got, err, test.want)
		}
	}
	for _, text := range []string{"extend 0", "extend -1", "extend +1", "extend 1.5", "extend 1H", "extend 1m", "extend 25", "extend 1h extra", "extendh"} {
		if _, err := ParseExtend(text); err == nil {
			t.Fatalf("ParseExtend(%q) succeeded", text)
		}
	}
}

func TestParseCreateAcceptsEverySafeFlag(t *testing.T) {
	request, err := ParseCreate(`create --target test --provider vpc-gen2 --platform openshift --version 4.22 --resource-group "Platform Team" --zone us-south-3 --flavor custom --vpc-id vpc-id --subnet-id subnet-one --subnet-id subnet-two --public-gateway-id gateway-one --public-gateway-id gateway-two --datacenter dal10 --machine-type b3c.4x16 --public-vlan-id public-vlan --private-vlan-id private-vlan --satellite-zone us-south-1 --satellite-zone us-south-2 --satellite-managed-from managed-from --satellite-location-id location-id --satellite-host-image image-id --satellite-host-profile bx2-4x16 --satellite-ssh-key-id ssh-key --satellite-worker-instance-id worker-one --satellite-worker-instance-id worker-two --satellite-worker-operating-system RHCOS --worker-count 3 --name 'team cluster'`, testCreateDefaults)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"--target", "test", "--provider", "vpc-gen2", "--platform", "openshift", "--version", "4.22",
		"--resource-group", "Platform Team", "--zone", "us-south-3", "--flavor", "custom", "--vpc-id", "vpc-id",
		"--subnet-id", "subnet-one", "--subnet-id", "subnet-two", "--public-gateway-id", "gateway-one", "--public-gateway-id", "gateway-two",
		"--datacenter", "dal10", "--machine-type", "b3c.4x16", "--public-vlan-id", "public-vlan", "--private-vlan-id", "private-vlan",
		"--satellite-zone", "us-south-1", "--satellite-zone", "us-south-2", "--satellite-managed-from", "managed-from",
		"--satellite-location-id", "location-id", "--satellite-host-image", "image-id", "--satellite-host-profile", "bx2-4x16",
		"--satellite-ssh-key-id", "ssh-key", "--satellite-worker-instance-id", "worker-one", "--satellite-worker-instance-id", "worker-two",
		"--satellite-worker-operating-system", "RHCOS", "--worker-count", "3", "--name", "team cluster",
	}
	if request.Platform != "openshift" || request.Version != "4.22" || !reflect.DeepEqual(request.Args, want) {
		t.Fatalf("request = %+v\nwant args %#v", request, want)
	}
}

func TestParseCreateAppliesAndOverridesConfiguredDefaults(t *testing.T) {
	tests := []struct {
		name, text, platform string
		want                 []string
	}{
		{
			name: "configured version", text: "create", platform: "openshift",
			want: []string{"--target", "synthetic-target", "--provider", "vpc-gen2", "--platform", "openshift", "--version", "4.22", "--resource-group", "Default", "--zone", "us-south-1", "--flavor", "bx2.4x16", "--vpc-id", "synthetic-vpc-id"},
		},
		{
			name: "OpenShift defaults", text: "create --version 4.22", platform: "openshift",
			want: []string{"--target", "synthetic-target", "--provider", "vpc-gen2", "--platform", "openshift", "--version", "4.22", "--resource-group", "Default", "--zone", "us-south-1", "--flavor", "bx2.4x16", "--vpc-id", "synthetic-vpc-id"},
		},
		{
			name: "Kubernetes defaults", text: "create --version 1.31", platform: "kubernetes",
			want: []string{"--target", "synthetic-target", "--provider", "vpc-gen2", "--platform", "kubernetes", "--version", "1.31", "--resource-group", "Default", "--zone", "us-south-1", "--flavor", "bx2.2x8", "--vpc-id", "synthetic-vpc-id"},
		},
		{
			name: "OpenShift overrides", text: "create --version=4.22 --target target --provider classic --platform openshift --resource-group group --zone zone --vpc-id vpc --flavor flavor", platform: "openshift",
			want: []string{"--target", "target", "--provider", "classic", "--platform", "openshift", "--version", "4.22", "--resource-group", "group", "--zone", "zone", "--flavor", "flavor", "--vpc-id", "vpc"},
		},
		{
			name: "Kubernetes flavor override", text: "create --version 1.31 --platform kubernetes --flavor kubernetes-flavor", platform: "kubernetes",
			want: []string{"--target", "synthetic-target", "--provider", "vpc-gen2", "--platform", "kubernetes", "--version", "1.31", "--resource-group", "Default", "--zone", "us-south-1", "--flavor", "kubernetes-flavor", "--vpc-id", "synthetic-vpc-id"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := ParseCreate(test.text, testCreateDefaults)
			if err != nil {
				t.Fatal(err)
			}
			if request.Platform != test.platform || !reflect.DeepEqual(request.Args, test.want) {
				t.Fatalf("request = %+v\nwant args %#v", request, test.want)
			}
		})
	}
}

func TestParseCreateExposesNormalizedPresentationFields(t *testing.T) {
	request, err := ParseCreate("create --version 1.36 --worker-count 2 --subnet-id subnet --public-gateway-id gateway --name team", testCreateDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if request.Target != "synthetic-target" || request.Platform != "kubernetes" || request.Version != "1.36" || request.Provider != "vpc-gen2" || request.ResourceGroup != "Default" || request.WorkerShape != "bx2.2x8" || request.WorkerCount != "2" || request.Location != "us-south/us-south-1" || request.Name != "team" {
		t.Fatalf("normalized request = %+v", request)
	}
	if !request.ReuseVPC || !request.ReuseSubnet || !request.ReuseGateway || request.VPCID != "synthetic-vpc-id" || !reflect.DeepEqual(request.SubnetIDs, []string{"subnet"}) || !reflect.DeepEqual(request.PublicGatewayIDs, []string{"gateway"}) {
		t.Fatalf("network choices = %+v", request)
	}
}

func TestParseCreateRejectsEveryProhibitedOrAmbiguousInput(t *testing.T) {
	tests := []struct {
		name, text string
	}{
		{"state ID positional input", "create slack-user --version 4.22"},
		{"positional version", "create 4.22"},
		{"unknown flag", "create --version 4.22 --unknown value"},
		{"config", "create --version 4.22 --config /secret"},
		{"owner", "create --version 4.22 --owner user"},
		{"prefix", "create --version 4.22 --prefix user"},
		{"auto approve", "create --version 4.22 --auto-approve true"},
		{"confirm stdin", "create --version 4.22 --confirm-stdin true"},
		{"SSH public key path", "create --version 4.22 --satellite-ssh-public-key /secret"},
		{"uninferable version", "create --version 5.1"},
		{"mismatched platform", "create --version 4.22 --platform kubernetes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := ParseCreate(test.text, testCreateDefaults)
			if err == nil {
				t.Fatal("error = nil")
			}
			if request.Version != "" || request.Platform != "" || len(request.Args) != 0 {
				t.Fatalf("request = %+v, want empty request on error", request)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error exposed value: %v", err)
			}
		})
	}
}

func TestParseCreateRejectsDuplicateAndMissingSingletonValues(t *testing.T) {
	singletons := []struct{ flag, value string }{
		{"--target", "target"}, {"--provider", "vpc-gen2"}, {"--platform", "openshift"}, {"--version", "4.22"},
		{"--resource-group", "group"}, {"--zone", "zone"}, {"--flavor", "flavor"}, {"--vpc-id", "vpc"},
		{"--datacenter", "datacenter"}, {"--machine-type", "machine"}, {"--public-vlan-id", "public"}, {"--private-vlan-id", "private"},
		{"--satellite-managed-from", "managed"}, {"--satellite-location-id", "location"}, {"--satellite-host-image", "image"},
		{"--satellite-host-profile", "profile"}, {"--satellite-ssh-key-id", "key"}, {"--satellite-worker-operating-system", "os"},
		{"--worker-count", "3"}, {"--name", "cluster"},
	}
	for _, test := range singletons {
		t.Run("duplicate "+test.flag, func(t *testing.T) {
			text := "create --version 4.22 " + test.flag + " " + test.value + " " + test.flag + " " + test.value
			request, err := ParseCreate(text, testCreateDefaults)
			if err == nil {
				t.Fatal("error = nil")
			}
			if len(request.Args) != 0 {
				t.Fatalf("request = %+v, want no arguments on error", request)
			}
		})
		t.Run("missing "+test.flag, func(t *testing.T) {
			text := "create --version 4.22 " + test.flag
			if test.flag == "--version" {
				text = "create --version"
			}
			request, err := ParseCreate(text, testCreateDefaults)
			if err == nil {
				t.Fatal("error = nil")
			}
			if len(request.Args) != 0 {
				t.Fatalf("request = %+v, want no arguments on error", request)
			}
		})
	}
}

func TestParseCreateParsesQuotedAndEscapedValuesAndRejectsShellSyntax(t *testing.T) {
	tests := []struct {
		name, text, wantName string
		wantErr              bool
	}{
		{"single quoted", "create --version 4.22 --name 'team cluster'", "team cluster", false},
		{"double quoted escaped quote", `create --version 4.22 --name "team \"cluster\""`, `team "cluster"`, false},
		{"escaped whitespace", `create --version 4.22 --name team\ cluster`, "team cluster", false},
		{"unterminated quote", "create --version 4.22 --name 'team cluster", "", true},
		{"unterminated escape", `create --version 4.22 --name team\`, "", true},
		{"semicolon", "create --version 4.22; destroy", "", true},
		{"pipe", "create --version 4.22 | destroy", "", true},
		{"ampersand", "create --version 4.22 & destroy", "", true},
		{"input redirect", "create --version 4.22 < input", "", true},
		{"output redirect", "create --version 4.22 > output", "", true},
		{"variable", "create --version $VERSION", "", true},
		{"backtick", "create --version `version`", "", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := ParseCreate(test.text, testCreateDefaults)
			if test.wantErr {
				if err == nil {
					t.Fatal("error = nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := request.Args[len(request.Args)-1]; got != test.wantName {
				t.Fatalf("name = %q, want %q", got, test.wantName)
			}
		})
	}
}

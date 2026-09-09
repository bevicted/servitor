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

func TestExtensionTargetUsesSnapshotDefaultAndKeepsUTC(t *testing.T) {
	expiry := time.Date(2026, 9, 8, 16, 0, 0, 0, time.FixedZone("CDT", -5*60*60))
	for _, test := range []struct {
		name      string
		increment time.Duration
		want      time.Time
	}{
		{name: "bare command", increment: 0, want: expiry.Add(4 * time.Hour)},
		{name: "number", increment: 2 * time.Hour, want: expiry.Add(2 * time.Hour)},
		{name: "number with h suffix", increment: 3 * time.Hour, want: expiry.Add(3 * time.Hour)},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ExtensionTarget(expiry, test.increment, 4*time.Hour)
			if err != nil || !got.Equal(test.want) || got.Location() != time.UTC {
				t.Fatalf("ExtensionTarget() = %s, %v; want %s UTC", got, err, test.want.UTC())
			}
		})
	}
	for _, increment := range []time.Duration{30 * time.Minute, 25 * time.Hour} {
		if _, err := ExtensionTarget(expiry, increment, 4*time.Hour); err == nil {
			t.Fatalf("ExtensionTarget accepted %s", increment)
		}
	}
}

func TestParseCreateAcceptsEverySafeFlag(t *testing.T) {
	request, err := ParseCreate(`create --target test --provider vpc-gen2 --version 4.22 --resource-group "Platform Team" --zone us-south-3 --flavor custom --vpc-id vpc-id --subnet-id subnet-one --subnet-id subnet-two --public-gateway-id gateway-one --public-gateway-id gateway-two --datacenter dal10 --machine-type b3c.4x16 --public-vlan-id public-vlan --private-vlan-id private-vlan --satellite-zone us-south-1 --satellite-zone us-south-2 --satellite-managed-from managed-from --satellite-location-id location-id --satellite-host-image image-id --satellite-host-profile bx2-4x16 --satellite-ssh-key-id ssh-key --satellite-worker-instance-id worker-one --satellite-worker-instance-id worker-two --satellite-worker-operating-system RHCOS --worker-count 3`, testCreateDefaults)
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
		"--satellite-worker-operating-system", "RHCOS", "--worker-count", "3",
	}
	if request.Platform != "openshift" || request.Version != "4.22" || !reflect.DeepEqual(request.Args, want) {
		t.Fatalf("request = %+v\nwant args %#v", request, want)
	}
}

func TestParseCreateOptionsNormalizesMixedAssignments(t *testing.T) {
	want := map[string][]string{
		"--target":         {"synthetic-target"},
		"--provider":       {"vpc-gen2"},
		"--version":        {"4.22"},
		"--resource-group": {`Platform "Team"=Core`},
		"--worker-count":   {"3"},
		"--subnet-id":      {"subnet-one", "subnet-two"},
	}
	for _, text := range []string{
		`create target=synthetic-target provider=vpc-gen2 version=4.22 resource-group="Platform \"Team\"=Core" worker-count=3 subnet-id=subnet-one subnet-id=subnet-two`,
		`create --target=synthetic-target --provider=vpc-gen2 --version=4.22 --resource-group="Platform \"Team\"=Core" --worker-count=3 --subnet-id=subnet-one --subnet-id=subnet-two`,
		`create --target synthetic-target provider=vpc-gen2 --version 4.22 resource-group="Platform \"Team\"=Core" --worker-count=3 subnet-id=subnet-one --subnet-id subnet-two`,
	} {
		options, err := ParseCreateOptions(text)
		if err != nil {
			t.Fatalf("ParseCreateOptions(%q): %v", text, err)
		}
		if got := options.Values(); !reflect.DeepEqual(got, want) {
			t.Fatalf("ParseCreateOptions(%q) = %#v\nwant %#v", text, got, want)
		}
	}
}

func TestParseCreateAssignmentsRetainOneQuotedArgvValue(t *testing.T) {
	request, err := ParseCreate(`create target=synthetic-target resource-group="Platform Team=Core" --version 4.22`, testCreateDefaults)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < len(request.Args); index += 2 {
		if request.Args[index] == "--resource-group" {
			if request.Args[index+1] != "Platform Team=Core" {
				t.Fatalf("resource-group argv value = %q", request.Args[index+1])
			}
			return
		}
	}
	t.Fatalf("resource-group was not captured in argv: %#v", request.Args)
}

func TestParseCreateAssignmentErrors(t *testing.T) {
	tests := []struct {
		text, want string
	}{
		{"create unknown=value", `unknown create flag "--unknown"`},
		{"create config=value", "--config is not permitted"},
		{"create target=", "--target requires a value"},
		{"create target=one --target two", "--target may only be supplied once"},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			if _, err := ParseCreateOptions(test.text); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ParseCreateOptions(%q) error = %v, want %q", test.text, err, test.want)
			}
		})
	}
}

func TestParseCreateRejectsInvalidWorkerCount(t *testing.T) {
	for _, text := range []string{
		"create worker-count=not-a-number",
		"create --worker-count 999999999999999999999999999999",
		"create --worker-count=0",
		"create worker-count=101",
	} {
		t.Run(text, func(t *testing.T) {
			if _, err := ParseCreateOptions(text); err == nil || err.Error() != "--worker-count must be an integer from 1 through 100" {
				t.Fatalf("ParseCreateOptions(%q) error = %v", text, err)
			}
		})
	}
	for _, text := range []string{"create worker-count=1", "create --worker-count 100"} {
		options, err := ParseCreateOptions(text)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := options.WorkerCount(); err != nil {
			t.Fatalf("WorkerCount(%q): %v", text, err)
		}
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
			name: "OpenShift overrides", text: "create --version=4.22 --target target --provider classic --resource-group group --zone zone --vpc-id vpc --flavor flavor", platform: "openshift",
			want: []string{"--target", "target", "--provider", "classic", "--platform", "openshift", "--version", "4.22", "--resource-group", "group", "--zone", "zone", "--flavor", "flavor", "--vpc-id", "vpc"},
		},
		{
			name: "Kubernetes flavor override", text: "create --version 1.31 --flavor kubernetes-flavor", platform: "kubernetes",
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
	request, err := ParseCreate("create --version 1.36 --worker-count 2 --subnet-id subnet --public-gateway-id gateway", testCreateDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if request.Target != "synthetic-target" || request.Platform != "kubernetes" || request.Version != "1.36" || request.Provider != "vpc-gen2" || request.ResourceGroup != "Default" || request.WorkerShape != "bx2.2x8" || request.WorkerCount != "2" || request.Location != "us-south/us-south-1" {
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
		{"name", "create --version 4.22 --name caller-selected"},
		{"confirm stdin", "create --version 4.22 --confirm-stdin true"},
		{"SSH public key path", "create --version 4.22 --satellite-ssh-public-key /secret"},
		{"uninferable version", "create --version 5.1"},
		{"removed platform", "create --version 4.22 --platform kubernetes"},
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
		{"--target", "target"}, {"--provider", "vpc-gen2"}, {"--version", "4.22"},
		{"--resource-group", "group"}, {"--zone", "zone"}, {"--flavor", "flavor"}, {"--vpc-id", "vpc"},
		{"--datacenter", "datacenter"}, {"--machine-type", "machine"}, {"--public-vlan-id", "public"}, {"--private-vlan-id", "private"},
		{"--satellite-managed-from", "managed"}, {"--satellite-location-id", "location"}, {"--satellite-host-image", "image"},
		{"--satellite-host-profile", "profile"}, {"--satellite-ssh-key-id", "key"}, {"--satellite-worker-operating-system", "os"},
		{"--worker-count", "3"},
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
		name, text, wantResourceGroup string
		wantErr                       bool
	}{
		{"single quoted", "create --version 4.22 --resource-group 'Platform Team'", "Platform Team", false},
		{"double quoted escaped quote", `create --version 4.22 --resource-group "Platform \"Team\""`, `Platform "Team"`, false},
		{"escaped whitespace", `create --version 4.22 --resource-group Platform\ Team`, "Platform Team", false},
		{"unterminated quote", "create --version 4.22 --resource-group 'Platform Team", "", true},
		{"unterminated escape", `create --version 4.22 --resource-group Platform\`, "", true},
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
			options, err := ParseCreateOptions(test.text)
			if test.wantErr {
				if err == nil {
					t.Fatal("error = nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := one(options.values, "--resource-group"); got != test.wantResourceGroup {
				t.Fatalf("resource group = %q, want %q", got, test.wantResourceGroup)
			}
		})
	}
}

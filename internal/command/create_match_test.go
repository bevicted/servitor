package command

import (
	"reflect"
	"strings"
	"testing"

	"github.com/bevicted/servitor/internal/inventory"
)

var matchDefaults = CreateDefaults{Target: "target-a", Provider: "vpc-gen2", Zone: "us-south-1"}

var matchCatalog = inventory.Catalog{
	Version: inventory.CatalogVersion, Target: "target-a", Providers: []string{"vpc-gen2", "classic", "satellite"},
	Versions: []inventory.Version{{Name: "4.22_openshift", Platform: "openshift", Default: true, Supported: true}}, ResourceGroups: []string{"Platform Team", "shared"},
	VPCLocations:     []inventory.Location{{Name: "us-south-1", Flavors: []string{"bx2.4x16", "shared"}}, {Name: "us-east-1", Flavors: []string{"cx2.2x8"}}},
	ClassicLocations: []inventory.Location{{Name: "dal10", Flavors: []string{"b3c.4x16"}}}, SatelliteProfile: []inventory.Profile{{Region: "us-south", Name: "bx2-4x16"}},
}

func TestBareValuesResolveSelectorsBeforeOrderIndependentLocationMatching(t *testing.T) {
	options, err := ParseCreateOptions("create bx2.4x16 Platform\\ Team vpc-gen2 us-south-1 target-a version=4.22")
	if err != nil {
		t.Fatal(err)
	}
	options, _, err = ResolveBareSelectors(options, matchDefaults, map[string]inventory.TargetConfig{"target-a": {Providers: []string{"vpc-gen2", "classic", "satellite"}}})
	if err != nil {
		t.Fatal(err)
	}
	options, err = MatchBareCreateOptions(options, matchDefaults, matchCatalog)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"--target": {"target-a"}, "--provider": {"vpc-gen2"}, "--version": {"4.22"}, "--resource-group": {"Platform Team"}, "--zone": {"us-south-1"}, "--flavor": {"bx2.4x16"}}
	if got := options.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("matched values = %#v, want %#v", got, want)
	}
}

func TestBareValuesRequireOneExactRoleAndCurrentInventory(t *testing.T) {
	targets := map[string]inventory.TargetConfig{"target-a": {Providers: []string{"vpc-gen2", "classic", "satellite"}}}
	for _, test := range []struct{ text, want string }{
		{"create group", "unknown shorthand"},
		{"create shared", "ambiguous shorthand"},
		{"create target-a target-a", "target may only be supplied once"},
		{"create provider=classic vpc-gen2", "provider may only be supplied once"},
	} {
		t.Run(test.text, func(t *testing.T) {
			options, err := ParseCreateOptions(test.text)
			if err == nil {
				options, _, err = ResolveBareSelectors(options, matchDefaults, targets)
			}
			if err == nil {
				_, err = MatchBareCreateOptions(options, matchDefaults, matchCatalog)
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("%q error = %v, want %q", test.text, err, test.want)
			}
		})
	}
}

func TestBareSatelliteProfileMatchesSelectedRegion(t *testing.T) {
	catalog := matchCatalog
	catalog.SatelliteProfile = []inventory.Profile{{Region: "us-south", Name: "south-profile"}, {Region: "us-east", Name: "east-profile"}}
	for _, test := range []struct {
		text    string
		profile string
		wantErr string
	}{
		{"create provider=satellite satellite-zone=us-south-1 south-profile", "south-profile", ""},
		{"create provider=satellite satellite-zone=us-south-1 east-profile", "", "unknown shorthand"},
	} {
		t.Run(test.text, func(t *testing.T) {
			options, err := ParseCreateOptions(test.text)
			if err != nil {
				t.Fatal(err)
			}
			matched, err := MatchBareCreateOptions(options, matchDefaults, catalog)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("match error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || one(matched.Values(), "--satellite-host-profile") != test.profile {
				t.Fatalf("matched = %#v, err = %v", matched.Values(), err)
			}
		})
	}
}

func TestBareReservedAliasesRequireExplicitKey(t *testing.T) {
	options, err := ParseCreateOptions("create roks")
	if err != nil || one(options.values, "--version") != "default_openshift" || len(options.BareValues()) != 0 {
		t.Fatalf("roks parsed as %#v, %v", options, err)
	}
	options, err = ParseCreateOptions("create resource-group=roks")
	if err != nil || one(options.values, "--resource-group") != "roks" {
		t.Fatalf("keyed reserved value parsed as %#v, %v", options, err)
	}
}

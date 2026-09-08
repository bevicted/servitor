package terraformview

import (
	"reflect"
	"strings"
	"testing"
)

func TestParsePlanExposesOnlyResourceMetadata(t *testing.T) {
	plan, err := ParsePlan([]byte(`{
		"format_version":"1.2",
		"resource_changes":[
			{
				"address":"module.cluster.module.workers.ibm_container_vpc_cluster.main",
				"mode":"managed", "type":"ibm_container_vpc_cluster", "name":"main",
				"change":{"actions":["create"], "before":{"token":"secret"}, "after":{"id":"secret-id","name":"secret-name"}, "after_unknown":{"id":true}, "after_sensitive":{"token":false}}
			},
			{
				"address":"data.ibm_is_vpc.shared", "mode":"data", "type":"ibm_is_vpc", "name":"shared",
				"change":{"actions":["read"], "after":{"id":"secret-id"}, "after_unknown":{"id":true}, "after_sensitive":{"id":false}}
			}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []Resource{
		{Address: "data.ibm_is_vpc.shared", Mode: "data", Type: "ibm_is_vpc", Name: "shared", ID: "secret-id", Actions: []string{"read"}},
		{Address: "module.cluster.module.workers.ibm_container_vpc_cluster.main", Mode: "managed", Type: "ibm_container_vpc_cluster", Name: "main", ID: "secret-id", DisplayName: "secret-name", Actions: []string{"create"}},
	}
	if !reflect.DeepEqual(plan.Resources, want) {
		t.Fatalf("resources = %#v\nwant %#v", plan.Resources, want)
	}
}

func TestParsePlanIgnoresUnreportedSensitiveMetadata(t *testing.T) {
	plan, err := ParsePlan([]byte(`{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"],"after":{"name":"cluster","token":"secret"},"after_sensitive":{"token":true}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Resources; !reflect.DeepEqual(got, []Resource{{Address: "ibm_container_vpc_cluster.cluster", Mode: "managed", Type: "ibm_container_vpc_cluster", Name: "cluster", DisplayName: "cluster", Actions: []string{"create"}}}) {
		t.Fatalf("resources = %#v", got)
	}
}

func TestParsePlanRejectsSensitiveIdentityAndUnsupportedMetadataWithoutLeakingValues(t *testing.T) {
	for _, contents := range []string{
		`{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["create"],"after":{"name":"secret"},"after_sensitive":{"name":true}}}]}`,
		`{"format_version":"1.2","resource_changes":[{"address":"ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","change":{"actions":["replace"],"after":[]}}]}`,
		`{"format_version":"1.2","resource_changes":[{"address":"ibm_unapproved.secret","mode":"managed","type":"ibm_unapproved","name":"secret","change":{"actions":["create"],"after":{"name":"secret"}}}]}`,
	} {
		plan, err := ParsePlan([]byte(contents))
		if err == nil || len(plan.Resources) != 0 {
			t.Fatalf("unsafe plan accepted: plan=%#v err=%v", plan, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("error leaked input: %v", err)
		}
	}
}

func TestParsePlanAcceptsResourceGroupIdentityWithoutUnknownValues(t *testing.T) {
	plan, err := ParsePlan([]byte(`{
		"format_version":"1.2",
		"resource_changes":[{
			"address":"data.ibm_resource_group.default", "mode":"data", "type":"ibm_resource_group", "name":"default",
			"change":{"actions":["read"], "after":{"id":"group-id", "name":"Default", "account_id":"account-secret", "tags":["private-tag"]}, "after_sensitive":{"account_id":false, "tags":false}}
		}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []Resource{{Address: "data.ibm_resource_group.default", Mode: "data", Type: "ibm_resource_group", Name: "default", ID: "group-id", DisplayName: "Default", Actions: []string{"read"}}}
	if !reflect.DeepEqual(plan.Resources, want) {
		t.Fatalf("resources = %#v\nwant %#v", plan.Resources, want)
	}
}

func TestParseStateCollectsNestedClusterAndReusedNetworkingFixtures(t *testing.T) {
	state, err := ParseState([]byte(`{
		"format_version":"1.2",
		"values":{"root_module":{
			"resources":[
				{"address":"data.ibm_is_vpc.shared","mode":"data","type":"ibm_is_vpc","name":"shared","values":{"id":"vpc-id","name":"shared-vpc","sensitive":"secret"}},
				{"address":"data.ibm_is_subnet.shared","mode":"data","type":"ibm_is_subnet","name":"shared","values":{"id":"subnet-id","name":"shared-subnet"}},
				{"address":"data.ibm_is_public_gateways.shared","mode":"data","type":"ibm_is_public_gateways","name":"shared","values":{"id":"gateway-id","name":"shared-gateway"}}
			],
			"child_modules":[{"resources":[
				{"address":"module.vpc.ibm_container_vpc_cluster.cluster","mode":"managed","type":"ibm_container_vpc_cluster","name":"cluster","values":{"id":"vpc-cluster-id","name":"vpc-cluster"}}
			],"child_modules":[{"resources":[
				{"address":"module.vpc.module.classic.ibm_container_cluster.cluster","mode":"managed","type":"ibm_container_cluster","name":"cluster","values":{"id":"classic-cluster-id","name":"classic-cluster"}},
				{"address":"module.vpc.module.satellite.ibm_satellite_cluster.cluster","mode":"managed","type":"ibm_satellite_cluster","name":"cluster","values":{"id":"satellite-cluster-id","name":"satellite-cluster"}},
				{"address":"module.vpc.module.satellite.ibm_satellite_host.control_plane","mode":"managed","type":"ibm_satellite_host","name":"control_plane","values":{"id":null,"name":"sensitive-host","token":"secret"}}
			]}]}]
		}}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	want := []Resource{
		{Address: "data.ibm_is_public_gateways.shared", Mode: "data", Type: "ibm_is_public_gateways", Name: "shared", ID: "gateway-id", DisplayName: "shared-gateway", Reused: true},
		{Address: "data.ibm_is_subnet.shared", Mode: "data", Type: "ibm_is_subnet", Name: "shared", ID: "subnet-id", DisplayName: "shared-subnet", Reused: true},
		{Address: "data.ibm_is_vpc.shared", Mode: "data", Type: "ibm_is_vpc", Name: "shared", ID: "vpc-id", DisplayName: "shared-vpc", Reused: true},
		{Address: "module.vpc.ibm_container_vpc_cluster.cluster", Mode: "managed", Type: "ibm_container_vpc_cluster", Name: "cluster", ID: "vpc-cluster-id", DisplayName: "vpc-cluster"},
		{Address: "module.vpc.module.classic.ibm_container_cluster.cluster", Mode: "managed", Type: "ibm_container_cluster", Name: "cluster", ID: "classic-cluster-id", DisplayName: "classic-cluster"},
		{Address: "module.vpc.module.satellite.ibm_satellite_cluster.cluster", Mode: "managed", Type: "ibm_satellite_cluster", Name: "cluster", ID: "satellite-cluster-id", DisplayName: "satellite-cluster"},
		{Address: "module.vpc.module.satellite.ibm_satellite_host.control_plane", Mode: "managed", Type: "ibm_satellite_host", Name: "control_plane", DisplayName: "sensitive-host"},
	}
	if !reflect.DeepEqual(state.Resources, want) {
		t.Fatalf("resources = %#v\nwant %#v", state.Resources, want)
	}
}

func TestTerraformViewRejectsUnsupportedDocuments(t *testing.T) {
	for _, input := range []string{
		`{"format_version":"1.2","resource_changes":[`,
		`{"resource_changes":[]}`,
		`{"format_version":"0.1","resource_changes":[]}`,
		`{"format_version":"2.0","resource_changes":[]}`,
	} {
		plan, err := ParsePlan([]byte(input))
		if err == nil {
			t.Fatalf("unsupported plan accepted: %s", input)
		}
		if len(plan.Resources) != 0 {
			t.Fatalf("plan = %#v, want no resources on error", plan)
		}
		state, err := ParseState([]byte(input))
		if err == nil {
			t.Fatalf("unsupported state accepted: %s", input)
		}
		if len(state.Resources) != 0 {
			t.Fatalf("state = %#v, want no resources on error", state)
		}
	}
}

func TestTerraformViewAcceptsSupportedFormatVersions(t *testing.T) {
	for _, version := range []string{"1.0", "1.1", "1.2"} {
		t.Run(version, func(t *testing.T) {
			plan := []byte(`{"format_version":"` + version + `","resource_changes":[]}`)
			state := []byte(`{"format_version":"` + version + `","values":{"root_module":{}}}`)
			parsedPlan, err := ParsePlan(plan)
			if err != nil {
				t.Errorf("plan format version %q rejected: %v", version, err)
			} else if len(parsedPlan.Resources) != 0 {
				t.Errorf("plan format version %q resources = %#v, want none", version, parsedPlan.Resources)
			}
			parsedState, err := ParseState(state)
			if err != nil {
				t.Errorf("state format version %q rejected: %v", version, err)
			} else if len(parsedState.Resources) != 0 {
				t.Errorf("state format version %q resources = %#v, want none", version, parsedState.Resources)
			}
		})
	}
}

package inventory

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiscoverUsesConfiguredBasesAndCompleteScopedCatalog(t *testing.T) {
	var requests []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		if r.URL.Path != "/iam/base/identity/token" && r.Header.Get("Authorization") != "Bearer synthetic-token" {
			t.Fatalf("authorization for %s = %q", r.URL.Path, r.Header.Get("Authorization"))
		}
		switch r.URL.Path {
		case "/iam/base/identity/token":
			body, _ := io.ReadAll(r.Body)
			if got := string(body); !strings.Contains(got, "apikey=synthetic-key") || !strings.Contains(got, "grant_type=urn%3Aibm%3Aparams%3Aoauth%3Agrant-type%3Aapikey") {
				t.Fatalf("IAM form = %q", body)
			}
			_, _ = w.Write([]byte(`{"access_token":"synthetic-token"}`))
		case "/iam/base/identity/userinfo":
			_, _ = w.Write([]byte(`{"account_id":"account-1"}`))
		case "/rm/private/v2/resource_groups":
			if r.URL.Query().Get("account_id") != "account-1" || r.URL.Query().Get("limit") != "100" {
				t.Fatalf("resource group scope = %q", r.URL.RawQuery)
			}
			if r.URL.Query().Get("start") == "next" {
				_, _ = w.Write([]byte(`{"resources":[{"name":"Group B","state":"ACTIVE"}]}`))
			} else {
				_, _ = w.Write([]byte(`{"resources":[{"name":"Group A","state":"ACTIVE"},{"name":"deleted","state":"DELETED"}],"next":{"href":"https://not-a-configured-host.invalid/v2/resource_groups?account_id=account-1&start=next"}}`))
			}
		case "/containers/global/v1/versions":
			_, _ = w.Write([]byte(`{"openshift":[{"major":4,"minor":22,"default":true}],"kubernetes":[{"major":1,"minor":31,"default":true,"end_of_service":"2030-01-01"}]}`))
		case "/containers/global/v2/vpc/getZones":
			if r.URL.Query().Get("provider") != "vpc-gen2" || r.URL.Query().Get("showFlavors") != "false" {
				t.Fatalf("VPC zone scope = %q", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`[{"name":"us-south-1"}]`))
		case "/containers/global/v2/getFlavors":
			if r.URL.Query().Get("provider") != "vpc-gen2" || r.URL.Query().Get("zone") != "us-south-1" {
				t.Fatalf("VPC flavor scope = %q", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`[{"name":"bx2.4x16"}]`))
		case "/containers/global/v1/zones":
			_, _ = w.Write([]byte(`[{"name":"dal10","type":"classic"},{"name":"us-south-1","type":"vpc"}]`))
		case "/containers/global/v1/datacenters/dal10/machine-types":
			_, _ = w.Write([]byte(`[{"name":"b3c.4x16"}]`))
		case "/vpc/regional/instance/profiles":
			if r.URL.Query().Get("version") != vpcAPIVersion || r.URL.Query().Get("generation") != "2" {
				t.Fatalf("VPC profile contract = %q", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(`{"profiles":[{"name":"bx2-4x16"}]}`))
		default:
			t.Fatalf("unexpected request: %s", r.URL.RequestURI())
		}
	}))
	defer server.Close()
	config := Config{Version: CatalogVersion, Targets: map[string]TargetConfig{"target-a": {Providers: []string{"vpc-gen2", "classic", "satellite"}, DefaultRegion: "us-south", Endpoints: Endpoints{IAM: server.URL + "/iam/base", ResourceManagement: server.URL + "/rm/private", ContainerService: server.URL + "/containers/global", VPC: server.URL + "/vpc/regional"}}}}
	catalog, err := Discover(context.Background(), config, "target-a", "synthetic-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := catalog.ResourceGroups, []string{"Group A", "Group B"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("resource groups = %v, want %v", got, want)
	}
	if len(catalog.Versions) != 2 || catalog.Versions[0].Name != "1.31" || !catalog.Versions[0].Supported || catalog.Versions[0].EndOfService != "2030-01-01" || catalog.Versions[1].Name != "4.22_openshift" || !catalog.Versions[1].Default || !catalog.Versions[1].Supported || len(catalog.VPCLocations) != 1 || len(catalog.ClassicLocations) != 1 || len(catalog.SatelliteProfile) != 1 {
		t.Fatalf("catalog did not retain scoped metadata: %+v", catalog)
	}
	for _, request := range requests {
		if strings.Contains(request, "not-a-configured-host") {
			t.Fatalf("used pagination host rather than configured base: %q", request)
		}
	}
}

func TestSatelliteProfilesStopWhenFinalPageOmitsNext(t *testing.T) {
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/vpc/private/instance/profiles" || r.URL.Query().Get("version") != vpcAPIVersion || r.URL.Query().Get("generation") != "2" {
			t.Fatalf("VPC profile request = %s", r.URL.RequestURI())
		}
		switch requests {
		case 1:
			_, _ = w.Write([]byte(`{"profiles":[{"name":"bx2-4x16"}],"next":{"href":"https://untrusted.invalid/instance/profiles?version=2026-08-04&generation=2&start=next"}}`))
		case 2:
			_, _ = w.Write([]byte(`{"profiles":[{"name":"cx2-4x8"}]}`))
		default:
			http.Error(w, "unexpected repeat request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	profiles, err := (discovery{client: server.Client(), target: TargetConfig{DefaultRegion: "region-a", Endpoints: Endpoints{VPC: server.URL + "/vpc/private"}}}).satelliteProfiles(context.Background(), "synthetic-token")
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(profiles) != 2 || profiles[0].Name != "bx2-4x16" || profiles[1].Name != "cx2-4x8" {
		t.Fatalf("profiles = %+v after %d requests", profiles, requests)
	}
}

func TestSatelliteProfilesUseConfiguredDefaultRegionalBase(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("version") != vpcAPIVersion || r.URL.Query().Get("generation") != "2" || r.URL.Path != "/vpc/us-east/instance/profiles" {
			t.Fatalf("VPC profile request = %s", r.URL.RequestURI())
		}
		_, _ = w.Write([]byte(`{"profiles":[{"name":"east-profile"}]}`))
	}))
	defer server.Close()

	target := TargetConfig{DefaultRegion: "us-east", Endpoints: Endpoints{VPC: server.URL + "/vpc/{region}"}}
	profiles, err := (discovery{client: server.Client(), target: target}).satelliteProfiles(context.Background(), "synthetic-token")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := profiles, []Profile{{Region: "us-east", Name: "east-profile"}}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("regional profiles = %+v, want %+v", got, want)
	}
}

func TestAuthenticationRedirectsFailWithoutFollowingAPIKey(t *testing.T) {
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("redirect target received IAM credentials")
			}))
			defer redirectTarget.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/iam/private/identity/token" {
					t.Fatalf("IAM request = %s %s", r.Method, r.URL.RequestURI())
				}
				body, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(body), "apikey=synthetic-key") {
					t.Fatalf("IAM form = %q", body)
				}
				w.Header().Set("Location", redirectTarget.URL+"/identity/token")
				w.WriteHeader(status)
			}))
			defer server.Close()

			_, err := Discover(context.Background(), Config{Version: CatalogVersion, Targets: map[string]TargetConfig{"target-a": {Providers: []string{"vpc-gen2"}, Endpoints: Endpoints{IAM: server.URL + "/iam/private", ResourceManagement: server.URL + "/rm", ContainerService: server.URL + "/containers"}}}}, "target-a", "synthetic-key", server.Client())
			if err == nil || err.Error() != "inventory authentication failed" {
				t.Fatalf("authentication redirect error = %v", err)
			}
		})
	}
}

func TestServiceRedirectsFailWithoutFollowingBearerToken(t *testing.T) {
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("redirect target received service credentials")
			}))
			defer redirectTarget.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/service/private/catalog" || r.Header.Get("Authorization") != "Bearer synthetic-token" {
					t.Fatalf("service request = %s with authorization %q", r.URL.RequestURI(), r.Header.Get("Authorization"))
				}
				w.Header().Set("Location", redirectTarget.URL+"/catalog")
				w.WriteHeader(status)
			}))
			defer server.Close()

			var output struct{}
			err := (discovery{client: server.Client()}).requestBase(context.Background(), server.URL+"/service/private", http.MethodGet, "catalog", nil, "synthetic-token", &output)
			if err == nil || err.Error() != "inventory service request failed" {
				t.Fatalf("service redirect error = %v", err)
			}
		})
	}
}

func TestDiscoverFailsClosedForMalformedAndOversizedResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/iam/identity/token":
			_, _ = w.Write([]byte(`{"access_token":"secret-token"}`))
		case "/iam/identity/userinfo":
			_, _ = w.Write([]byte(`{"account_id":"account-1"}`))
		case "/rm/v2/resource_groups":
			_, _ = w.Write([]byte(strings.Repeat("x", MaxResponseBytes+1)))
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	config := Config{Version: CatalogVersion, Targets: map[string]TargetConfig{"target-a": {Providers: []string{"vpc-gen2"}, Endpoints: Endpoints{IAM: server.URL + "/iam", ResourceManagement: server.URL + "/rm", ContainerService: server.URL + "/containers"}}}}
	_, err := Discover(context.Background(), config, "target-a", "synthetic-key", server.Client())
	if err == nil || strings.Contains(err.Error(), "secret-token") || strings.Contains(err.Error(), server.URL) {
		t.Fatalf("unsafe oversized response error: %v", err)
	}
}

func TestLoadConfigEnforcesPipelineTargetIdentity(t *testing.T) {
	configFor := func(target string) []byte {
		return []byte("version: 1\ntargets:\n  \"" + target + "\":\n    providers: [vpc-gen2]\n    default_region: us-south\n    endpoints:\n      iam: https://iam.example.invalid\n      container_service: https://containers.example.invalid\n      global_tagging: https://tagging.example.invalid\n      resource_management: https://rm.example.invalid\n      resource_controller: https://controller.example.invalid\n      vpc: \"https://vpc.{region}.example.invalid\"\n")
	}
	valid := strings.Repeat("a", 63)
	if _, err := LoadConfig(configFor(valid)); err != nil {
		t.Fatalf("rejected valid target identity: %v", err)
	}
	for _, target := range []string{"Target", "1target", "target_name", strings.Repeat("a", 64), "-target", "target-"} {
		if _, err := LoadConfig(configFor(target)); err == nil {
			t.Fatalf("accepted target identity %q that cannot label an inventory PipelineRun", target)
		}
	}
}

func TestLoadConfigAcceptsICTSchemaAndRevisionsEveryField(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "ict-config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(data)
	if err != nil {
		t.Fatalf("ICT schema fixture was rejected: %v", err)
	}
	target := config.Targets["synthetic-target"]
	if target.DefaultRegion != "us-south" || target.Endpoints.IAM != "https://iam.example.invalid/private" || target.Endpoints.ContainerService != "https://containers.example.invalid/global" || target.Endpoints.ResourceManagement != "https://resource-manager.example.invalid/v2" {
		t.Fatalf("ICT lower-snake endpoints or default region were not retained: %+v", target)
	}
	baseline, err := Revision(target)
	if err != nil {
		t.Fatal(err)
	}
	changedRegion := target
	changedRegion.DefaultRegion = "eu-gb"
	changedEndpoint := target
	changedEndpoint.Endpoints.IAM = "https://replacement.example.invalid/private"
	for name, candidate := range map[string]TargetConfig{"default region": changedRegion, "endpoint": changedEndpoint} {
		revision, err := Revision(candidate)
		if err != nil || revision == baseline {
			t.Fatalf("%s revision = %q, %v; want a changed revision", name, revision, err)
		}
	}
}

func TestLoadConfigRejectsInvalidICTSchema(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "ict-config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range [][]byte{
		[]byte(strings.Replace(string(data), "    default_region: us-south\n", "", 1)),
		[]byte(strings.Replace(string(data), "      global_tagging: https://tagging.example.invalid\n", "", 1)),
		[]byte(strings.Replace(string(data), "      iam:", "      IAM:", 1)),
		[]byte(string(data) + "unknown: true\n"),
		[]byte(strings.Replace(string(data), "      vpc: https://vpc.{region}.example.invalid/private", "      vpc: https://vpc.example.invalid/private", 1)),
	} {
		if _, err := LoadConfig(invalid); err == nil {
			t.Fatalf("accepted invalid ICT configuration: %s", invalid)
		}
	}
}

func TestLoadConfigRejectsUnknownAndUnconfiguredEndpoints(t *testing.T) {
	for _, data := range []string{
		"version: 1\ntargets: {}\n",
		"version: 1\ntargets:\n  target-a:\n    providers: [vpc-gen2]\n    endpoints: {IAM: https://iam.example.invalid, ResourceManagement: https://rm.example.invalid, ContainerService: https://containers.example.invalid}\n    unexpected: true\n",
		"version: 1\ntargets:\n  target-a:\n    providers: [vpc-gen2]\n    endpoints: {IAM: https://user:password@example.invalid, ResourceManagement: https://rm.example.invalid, ContainerService: https://containers.example.invalid}\n",
	} {
		if _, err := LoadConfig([]byte(data)); err == nil {
			t.Fatalf("accepted invalid configuration: %s", data)
		}
	}
	base, _ := url.Parse("https://private.example.invalid/global")
	if base.Path != "/global" {
		t.Fatal("synthetic base was malformed")
	}
}

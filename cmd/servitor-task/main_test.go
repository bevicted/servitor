package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/controller"
	"github.com/bevicted/servitor/internal/inventory"
	"github.com/bevicted/servitor/internal/pipeline"
	"github.com/bevicted/servitor/internal/slackbot"
	"github.com/bevicted/servitor/internal/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func planningValidationFixture(t *testing.T, directory string) (string, string) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/iam/identity/token":
			_, _ = w.Write([]byte(`{"access_token":"synthetic-token"}`))
		case "/iam/identity/userinfo":
			_, _ = w.Write([]byte(`{"account_id":"account-1"}`))
		case "/rm/v2/resource_groups":
			_, _ = w.Write([]byte(`{"resources":[{"name":"New Group","state":"ACTIVE"}]}`))
		case "/containers/v1/versions":
			_, _ = w.Write([]byte(`{"openshift":[{"major":4,"minor":22,"default":true}],"kubernetes":[{"major":1,"minor":31,"default":true}]}`))
		case "/containers/v2/vpc/getZones":
			_, _ = w.Write([]byte(`[{"name":"us-south-1"}]`))
		case "/containers/v2/getFlavors":
			_, _ = w.Write([]byte(`[{"name":"bx2.4x16"}]`))
		default:
			t.Fatalf("unexpected planning validation request %s", r.URL.RequestURI())
		}
	}))
	t.Cleanup(server.Close)
	previousTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previousTransport })
	configPath := filepath.Join(directory, "planning-config.yaml")
	config := `version: 1
targets:
  target:
    providers: [vpc-gen2]
    default_region: us-south
    endpoints:
      iam: ` + server.URL + `/iam
      container_service: ` + server.URL + `/containers
      global_tagging: https://tagging.example.invalid
      resource_management: ` + server.URL + `/rm
      resource_controller: https://resource-controller.example.invalid
      vpc: https://vpc.{region}.example.invalid
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath, "synthetic-key"
}

type planningSlackResponder struct{}

func (planningSlackResponder) Reply(context.Context, slackbot.Response) error { return nil }

func TestValidatePlanOptionsScopesSatelliteProfileToSelectedRegion(t *testing.T) {
	var profileRequests []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/iam/identity/token":
			_, _ = w.Write([]byte(`{"access_token":"synthetic-token"}`))
		case "/iam/identity/userinfo":
			_, _ = w.Write([]byte(`{"account_id":"account-1"}`))
		case "/rm/v2/resource_groups":
			_, _ = w.Write([]byte(`{"resources":[{"name":"Group","state":"ACTIVE"}]}`))
		case "/containers/v1/versions":
			_, _ = w.Write([]byte(`{"openshift":[{"major":4,"minor":22,"default":true}]}`))
		case "/vpc/us-south/instance/profiles":
			profileRequests = append(profileRequests, r.URL.Path)
			_, _ = w.Write([]byte(`{"profiles":[{"name":"south-profile"}]}`))
		case "/vpc/us-east/instance/profiles":
			profileRequests = append(profileRequests, r.URL.Path)
			_, _ = w.Write([]byte(`{"profiles":[{"name":"east-profile"}]}`))
		default:
			t.Fatalf("unexpected regional planning request: %s", r.URL.RequestURI())
		}
	}))
	defer server.Close()
	previousTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previousTransport })

	configPath := filepath.Join(t.TempDir(), "satellite-config.yaml")
	config := `version: 1
targets:
  target:
    providers: [satellite]
    default_region: us-south
    endpoints:
      iam: ` + server.URL + `/iam
      container_service: ` + server.URL + `/containers
      global_tagging: https://tagging.example.invalid
      resource_management: ` + server.URL + `/rm
      resource_controller: https://resource-controller.example.invalid
      vpc: ` + server.URL + `/vpc/{region}
      satellite: https://satellite.example.invalid
      satellite_config: https://satellite-config.example.invalid
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "satellite", Version: "4.22", SatelliteZones: []string{"us-south-1", "us-south-2", "us-south-3"}, SatelliteHostProfile: "east-profile"}}
	_, rejection, err := validatePlanOptions(context.Background(), configPath, "synthetic-key", options)
	if err != nil || rejection == nil || rejection.OptionKey != "satellite-host-profile" {
		t.Fatalf("cross-region profile validation = %+v, err=%v", rejection, err)
	}
	if got, want := profileRequests, []string{"/vpc/us-south/instance/profiles"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("regional profile trace = %v, want %v", got, want)
	}
}

func TestKeyedCreateWithoutCatalogReachesFreshPlanningPreflight(t *testing.T) {
	directory := t.TempDir()
	planningConfig, apiKey := planningValidationFixture(t, directory)
	configData, err := os.ReadFile(planningConfig)
	if err != nil {
		t.Fatal(err)
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := servitorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}).WithObjects(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "targets", Namespace: "servitor"}, Data: map[string]string{"config.yaml": string(configData)}}).Build()
	defaults := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22", ResourceGroup: "Default", Zone: "us-south-1"}, Platform: "openshift"}
	bot := slackbot.Bot{ChannelID: "C1", SelfUserID: "BOT", Namespace: "servitor", Client: kube, Events: state.NewEventStore(kube, "servitor"), Defaults: command.CreateDefaults{Target: "target", Provider: "vpc-gen2", Version: "4.22", Zone: "us-south-1"}, InventoryConfigMap: "targets", InventoryConfigKey: "config.yaml", InventoryMaximumAge: time.Hour, Lease: time.Hour, RetryIntervals: []time.Duration{time.Minute}, Responder: planningSlackResponder{}}

	if err := bot.Handle(context.Background(), slackbot.Envelope{ID: "bare", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create New\\ Group", Timestamp: "1"}}); err != nil {
		t.Fatal(err)
	}
	clusters := &servitorv1alpha1.ServitorClusterList{}
	if err := kube.List(context.Background(), clusters); err != nil || len(clusters.Items) != 0 {
		t.Fatalf("bare create clusters=%+v, err=%v", clusters.Items, err)
	}

	if err := bot.Handle(context.Background(), slackbot.Envelope{ID: "keyed", Message: slackbot.Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: "<@BOT> create resource-group=New\\ Group", Timestamp: "2"}}); err != nil {
		t.Fatal(err)
	}
	if err := kube.List(context.Background(), clusters); err != nil || len(clusters.Items) != 1 || clusters.Items[0].Spec.UserOptions.ResourceGroup != "New Group" {
		t.Fatalf("keyed create clusters=%+v, err=%v", clusters.Items, err)
	}

	reconciler := &controller.Reconciler{Client: kube, Scheme: scheme, Config: controller.Config{Namespace: "servitor", Defaults: defaults, OpenShiftFlavor: "bx2.4x16"}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: clusters.Items[0].Namespace, Name: clusters.Items[0].Name}}
	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(context.Background(), request.NamespacedName, &clusters.Items[0]); err != nil {
		t.Fatal(err)
	}
	if clusters.Items[0].Status.ResolvedOptions == nil || clusters.Items[0].Status.ResolvedOptions.ResourceGroup != "New Group" {
		t.Fatalf("controller resolved options=%+v", clusters.Items[0].Status.ResolvedOptions)
	}

	ictTrace := filepath.Join(directory, "ict.trace")
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\nprintf invoked > \"$ICT_TRACE_FILE\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ICT_TRACE_FILE", ictTrace)
	err = runPlan(context.Background(), "uid", "plan-keyed", *clusters.Items[0].Status.ResolvedOptions, filepath.Join(directory, "backend.json"), filepath.Join(directory, "result.json"), filepath.Join(directory, "report.json"), ict, filepath.Join(directory, "terraform"), planningConfig, apiKey)
	if err == nil {
		t.Fatal("runPlan unexpectedly succeeded with a failing synthetic ICT")
	}
	if _, err := os.Stat(ictTrace); err != nil {
		t.Fatalf("fresh planning preflight did not reach ICT for keyed value: %v", err)
	}
}

func TestRunPlanUsesInitializedWorkspaceAndSanitizesOversizedPlan(t *testing.T) {
	directory := t.TempDir()
	workspace := filepath.Join(directory, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, ".cluster"), 0o700); err != nil {
		t.Fatal(err)
	}
	backendFile := filepath.Join(directory, "backend.json")
	resultFile := filepath.Join(directory, "result.json")
	reportFile := filepath.Join(directory, "report.json")
	planResultFile := filepath.Join(directory, "plan-result.json")
	planShowFile := filepath.Join(directory, "plan-show.json")
	recovery := servitorv1alpha1.RecoveryMetadata{
		Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Endpoints: map[string]string{
			"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
		},
		Values: servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"},
	}
	result := ictPlanResult{Version: 1, StateID: "plan-a", PlanPath: filepath.Join(workspace, ".cluster", "create.tfplan"), Values: recovery.Values}
	result.Recovery.Version = recovery.Version
	result.Recovery.Target = recovery.Target
	result.Recovery.Endpoints = recovery.Endpoints
	result.Recovery.Values = recovery.Values
	result.Recovery.TFVarsSHA256 = recovery.TFVarsSHA256
	if err := writeJSON(planResultFile, result); err != nil {
		t.Fatal(err)
	}
	planShow := []byte(`{"format_version":"1.2","resource_changes":[],"ignored":"` + strings.Repeat("x", pipeline.MaxReportBytes) + `"}`)
	if len(planShow) <= pipeline.MaxReportBytes {
		t.Fatal("Terraform plan fixture is not oversized")
	}
	if err := os.WriteFile(planShowFile, planShow, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLAN_RESULT", planResultFile)
	t.Setenv("PLAN_SHOW", planShowFile)
	t.Setenv("TRACE_FILE", filepath.Join(directory, "terraform.args"))
	t.Setenv("ICT_TRACE_FILE", filepath.Join(directory, "ict.args"))
	t.Setenv("WORKSPACE", workspace)
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ICT_TRACE_FILE\"\n[ \"$1\" = plan ] && [ \"$2\" = plan-a ] || exit 2\ncp \"$PLAN_RESULT\" \"$6\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	terraform := filepath.Join(directory, "terraform")
	if err := os.WriteFile(terraform, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$TRACE_FILE\"\n[ \"$1\" = \"-chdir=$WORKSPACE\" ] && [ \"$2\" = show ] && [ \"$3\" = -json ] && [ \"$4\" = .cluster/create.tfplan ] || exit 3\ncat \"$PLAN_SHOW\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	planningConfig, apiKey := planningValidationFixture(t, directory)
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "default_openshift", ResourceGroup: "New Group"}, Platform: "openshift"}
	if err := runPlan(context.Background(), "uid", "plan-a", options, backendFile, resultFile, reportFile, ict, terraform, planningConfig, apiKey); err != nil {
		t.Fatal(err)
	}
	trace, err := os.ReadFile(filepath.Join(directory, "terraform.args"))
	if err != nil {
		t.Fatal(err)
	}
	wantTrace := "-chdir=" + workspace + "\nshow\n-json\n.cluster/create.tfplan\n"
	if got := string(trace); got != wantTrace {
		t.Fatalf("terraform command = %q, want %q", got, wantTrace)
	}
	ictTrace, err := os.ReadFile(filepath.Join(directory, "ict.args"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(ictTrace); !strings.Contains(got, "--prefix\nservitor\n") || !strings.Contains(got, "--provider\nvpc-gen2\n") || !strings.Contains(got, "--platform\nopenshift\n") || !strings.Contains(got, "--version\n4.22_openshift\n") || strings.Contains(got, "--name\n") || strings.Contains(got, "--owner\n") {
		t.Fatalf("ICT plan arguments = %q, want derived OpenShift platform, fixed provider, and generated-name prefix", got)
	}
	report, err := os.ReadFile(reportFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(report) > pipeline.MaxReportBytes {
		t.Fatalf("sanitized report length = %d, limit = %d", len(report), pipeline.MaxReportBytes)
	}
	decoded, err := pipeline.DecodeReport(report, "uid", "plan-a")
	if err != nil {
		t.Fatalf("plan report is not controller-valid: %v", err)
	}
	if decoded.ResolvedOptions.ClusterName != recovery.Values.ClusterName || decoded.ResolvedOptions.Region != recovery.Values.Region || decoded.ResolvedOptions.Version != recovery.Values.KubeVersion || decoded.ResolvedOptions.WorkerCount != recovery.Values.WorkerCount || decoded.ResolvedOptions.Zone != recovery.Values.Zone || decoded.ResolvedOptions.Flavor != recovery.Values.Flavor {
		t.Fatalf("plan report did not adopt ICT-normalized options: %+v", decoded.ResolvedOptions)
	}
	if strings.Contains(string(report), workspace) {
		t.Fatal("plan workspace was exposed in the report")
	}
}

func TestRunPlanRejectsInconsistentRecoveryValues(t *testing.T) {
	values := servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"}
	tests := []struct {
		name     string
		recovery servitorv1alpha1.RecoveryValues
		want     string
	}{
		{name: "platform does not match recovery version", recovery: servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "kubernetes", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"}, want: "ICT produced invalid recovery values"},
		{name: "recovery version diverges from planning result", recovery: servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "kubernetes", KubeVersion: "1.31", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"}, want: "ICT recovery values are inconsistent with planning result"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			workspace := filepath.Join(directory, "workspace")
			if err := os.MkdirAll(filepath.Join(workspace, ".cluster"), 0o700); err != nil {
				t.Fatal(err)
			}
			planResultFile := filepath.Join(directory, "plan-result.json")
			recovery := servitorv1alpha1.RecoveryMetadata{
				Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
				Endpoints: map[string]string{
					"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
				},
				Values: test.recovery,
			}
			if err := recovery.Validate(); err != nil {
				t.Fatalf("recovery fixture must otherwise be valid: %v", err)
			}
			result := ictPlanResult{Version: 1, StateID: "plan-a", PlanPath: filepath.Join(workspace, ".cluster", "create.tfplan"), Values: values}
			result.Recovery.Version = recovery.Version
			result.Recovery.Target = recovery.Target
			result.Recovery.Endpoints = recovery.Endpoints
			result.Recovery.Values = recovery.Values
			result.Recovery.TFVarsSHA256 = recovery.TFVarsSHA256
			if err := writeJSON(planResultFile, result); err != nil {
				t.Fatal(err)
			}
			planShowFile := filepath.Join(directory, "plan-show.json")
			if err := os.WriteFile(planShowFile, []byte(`{"format_version":"1.2","resource_changes":[]}`), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PLAN_RESULT", planResultFile)
			t.Setenv("PLAN_SHOW", planShowFile)
			ict := filepath.Join(directory, "ict")
			if err := os.WriteFile(ict, []byte("#!/bin/sh\ncp \"$PLAN_RESULT\" \"$6\"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			terraform := filepath.Join(directory, "terraform")
			if err := os.WriteFile(terraform, []byte("#!/bin/sh\ncat \"$PLAN_SHOW\"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			reportFile := filepath.Join(directory, "report.json")
			planningConfig, apiKey := planningValidationFixture(t, directory)
			err := runPlan(context.Background(), "uid", "plan-a", servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22"}, Platform: "openshift"}, filepath.Join(directory, "backend.json"), filepath.Join(directory, "result.json"), reportFile, ict, terraform, planningConfig, apiKey)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("runPlan error = %v, want %q", err, test.want)
			}
			if _, err := os.Stat(reportFile); !os.IsNotExist(err) {
				t.Fatalf("inconsistent recovery values wrote report: %v", err)
			}
		})
	}
}

func TestRunPlanRejectsInvalidSelectionBeforeICTAndRedactsDiscoveryFailure(t *testing.T) {
	directory := t.TempDir()
	configPath, apiKey := planningValidationFixture(t, directory)
	ictTrace := filepath.Join(directory, "ict.trace")
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\nprintf invoked > \"$ICT_TRACE_FILE\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ICT_TRACE_FILE", ictTrace)
	reportPath := filepath.Join(directory, "rejection.json")
	if err := runPlan(context.Background(), "uid", "plan-rejected", servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.99"}}, filepath.Join(directory, "backend.json"), filepath.Join(directory, "result.json"), reportPath, ict, filepath.Join(directory, "terraform"), configPath, apiKey); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ictTrace); !os.IsNotExist(err) {
		t.Fatalf("invalid preflight invoked ICT: %v", err)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	report, err := pipeline.DecodeReport(data, "uid", "plan-rejected")
	if err != nil || report.PlanRejection == nil || report.PlanRejection.ReasonCode != "version_not_supported" || report.PlanRejection.OptionKey != "version" {
		t.Fatalf("invalid preflight report = %+v, err=%v", report, err)
	}

	failure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "synthetic private service detail", http.StatusBadGateway)
	}))
	defer failure.Close()
	previousFailureTransport := http.DefaultTransport
	http.DefaultTransport = failure.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previousFailureTransport })
	failureConfig := filepath.Join(directory, "failure-config.yaml")
	contents := `version: 1
targets:
  target:
    providers: [vpc-gen2]
    default_region: us-south
    endpoints:
      iam: ` + failure.URL + `/iam
      container_service: ` + failure.URL + `/containers
      global_tagging: https://tagging.example.invalid
      resource_management: ` + failure.URL + `/rm
      resource_controller: https://resource-controller.example.invalid
      vpc: https://vpc.{region}.example.invalid
`
	if err := os.WriteFile(failureConfig, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	err = runPlan(context.Background(), "uid", "plan-failed", servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22"}}, filepath.Join(directory, "backend.json"), filepath.Join(directory, "result.json"), filepath.Join(directory, "failed-report.json"), ict, filepath.Join(directory, "terraform"), failureConfig, "synthetic-secret")
	if err == nil || !strings.Contains(err.Error(), "inventory discovery failed") || strings.Contains(err.Error(), failure.URL) || strings.Contains(err.Error(), "synthetic private service detail") || strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatalf("planning service failure leaked or became an input rejection: %v", err)
	}
	if _, err := os.Stat(ictTrace); !os.IsNotExist(err) {
		t.Fatalf("service failure invoked ICT: %v", err)
	}
}

func TestSupportedPlanVersionRequiresOneApplicableCloudDefault(t *testing.T) {
	versions := []inventory.Version{
		{Name: "4.16_openshift", Platform: "openshift", Default: false, Supported: true},
		{Name: "4.17_openshift", Platform: "openshift", Default: true, Supported: true},
		{Name: "1.34", Platform: "kubernetes", Default: true, Supported: true},
	}
	for _, requested := range []string{"default_openshift", "4.17", "4.17.9"} {
		version, platform, ok := supportedPlanVersion(versions, requested)
		if !ok || version != "4.17_openshift" || platform != "openshift" {
			t.Fatalf("supportedPlanVersion(%q) = %q, %q, %v", requested, version, platform, ok)
		}
	}
	for _, versions := range [][]inventory.Version{
		nil,
		{{Name: "4.17_openshift", Platform: "openshift", Default: true, Supported: true}, {Name: "4.18_openshift", Platform: "openshift", Default: true, Supported: true}},
		{{Name: "4.17_openshift", Platform: "openshift", Default: true, Supported: false}},
	} {
		if _, _, ok := supportedPlanVersion(versions, "default_openshift"); ok {
			t.Fatalf("supportedPlanVersion(%#v, cloud default) unexpectedly succeeded", versions)
		}
	}
}

func TestResolvedOptionsFromValuesAdoptsAllProviderValues(t *testing.T) {
	tests := []struct {
		name     string
		values   servitorv1alpha1.RecoveryValues
		platform string
		want     servitorv1alpha1.UserOptions
	}{
		{
			name:     "vpc",
			values:   servitorv1alpha1.RecoveryValues{ClusterName: "vpc-cluster", ResourceGroupName: "group", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}},
			platform: "openshift",
			want:     servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "4.22_openshift", ResourceGroup: "group", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}},
		},
		{
			name:     "classic",
			values:   servitorv1alpha1.RecoveryValues{ClusterName: "classic-cluster", ResourceGroupName: "group", Region: "us-south", ClusterMode: "classic", Platform: "kubernetes", KubeVersion: "1.31", WorkerCount: 3, Datacenter: "dal10", MachineType: "bx2.4x16", PublicVLANID: "123", PrivateVLANID: "456"},
			platform: "kubernetes",
			want:     servitorv1alpha1.UserOptions{Target: "target", Provider: "classic", Version: "1.31", ResourceGroup: "group", WorkerCount: 3, Datacenter: "dal10", MachineType: "bx2.4x16", PublicVLANID: "123", PrivateVLANID: "456"},
		},
		{
			name:     "satellite",
			values:   servitorv1alpha1.RecoveryValues{ClusterName: "satellite-cluster", ResourceGroupName: "group", Region: "us-south", ClusterMode: "satellite", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 3, VPCID: "vpc", SubnetIDs: []string{"subnet-a", "subnet-b", "subnet-c"}, PublicGatewayIDs: []string{"gateway-a", "gateway-b", "gateway-c"}, SatelliteZones: []string{"us-south-1", "us-south-2", "us-south-3"}, SatelliteManagedFrom: "us-south", SatelliteLocationID: "location", SatelliteHostImage: "image", SatelliteHostProfile: "bx2-4x16", SatelliteSSHKeyID: "key", SatelliteWorkerInstanceIDs: []string{"worker-a", "worker-b", "worker-c"}, SatelliteWorkerOperatingSystem: "RHCOS"},
			platform: "openshift",
			want:     servitorv1alpha1.UserOptions{Target: "target", Provider: "satellite", Version: "4.22_openshift", ResourceGroup: "group", WorkerCount: 3, VPCID: "vpc", SubnetIDs: []string{"subnet-a", "subnet-b", "subnet-c"}, PublicGatewayIDs: []string{"gateway-a", "gateway-b", "gateway-c"}, SatelliteZones: []string{"us-south-1", "us-south-2", "us-south-3"}, SatelliteManagedFrom: "us-south", SatelliteLocationID: "location", SatelliteHostImage: "image", SatelliteHostProfile: "bx2-4x16", SatelliteSSHKeyID: "key", SatelliteWorkerInstanceIDs: []string{"worker-a", "worker-b", "worker-c"}, SatelliteWorkerOperatingSystem: "RHCOS"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolvedOptionsFromValues(servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", VPCID: "stale-vpc", MachineType: "stale-machine"}}, test.values)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(resolved.UserOptions, test.want) || resolved.Platform != test.platform || resolved.ClusterName != test.values.ClusterName || resolved.Region != test.values.Region {
				t.Fatalf("resolved = %+v, want options %+v", resolved, test.want)
			}
		})
	}
}

func TestOptionArgsOmitsVPCIDForClassic(t *testing.T) {
	args := optionArgs(servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "classic", Version: "1.31", Datacenter: "dal10", MachineType: "bx2.4x16", PublicVLANID: "123", PrivateVLANID: "456"}, Platform: "kubernetes"})
	for _, arg := range args {
		if arg == "--vpc-id" {
			t.Fatalf("Classic argv included --vpc-id: %q", args)
		}
	}
}

func TestRunPlanPassesKubernetesPlatformToICT(t *testing.T) {
	directory := t.TempDir()
	workspace := filepath.Join(directory, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, ".cluster"), 0o700); err != nil {
		t.Fatal(err)
	}
	resultFile := filepath.Join(directory, "result.json")
	planResultFile := filepath.Join(directory, "plan-result.json")
	planShowFile := filepath.Join(directory, "plan-show.json")
	values := servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "kubernetes", KubeVersion: "1.31", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"}
	recovery := servitorv1alpha1.RecoveryMetadata{
		Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Endpoints: map[string]string{
			"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
		},
		Values: values,
	}
	result := ictPlanResult{Version: 1, StateID: "plan-kubernetes", PlanPath: filepath.Join(workspace, ".cluster", "create.tfplan"), Values: values}
	result.Recovery.Version = recovery.Version
	result.Recovery.Target = recovery.Target
	result.Recovery.Endpoints = recovery.Endpoints
	result.Recovery.Values = recovery.Values
	result.Recovery.TFVarsSHA256 = recovery.TFVarsSHA256
	if err := writeJSON(planResultFile, result); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planShowFile, []byte(`{"format_version":"1.2","resource_changes":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLAN_RESULT", planResultFile)
	t.Setenv("PLAN_SHOW", planShowFile)
	t.Setenv("ICT_TRACE_FILE", filepath.Join(directory, "ict.args"))
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ICT_TRACE_FILE\"\ncp \"$PLAN_RESULT\" \"$6\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	terraform := filepath.Join(directory, "terraform")
	if err := os.WriteFile(terraform, []byte("#!/bin/sh\ncat \"$PLAN_SHOW\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	planningConfig, apiKey := planningValidationFixture(t, directory)
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Target: "target", Provider: "vpc-gen2", Version: "1.31"}, Platform: "kubernetes"}
	if err := runPlan(context.Background(), "uid", "plan-kubernetes", options, filepath.Join(directory, "backend.json"), resultFile, filepath.Join(directory, "report.json"), ict, terraform, planningConfig, apiKey); err != nil {
		t.Fatal(err)
	}
	trace, err := os.ReadFile(filepath.Join(directory, "ict.args"))
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{"--provider\nvpc-gen2", "--platform\nkubernetes", "--version\n1.31"} {
		if !strings.Contains(string(trace), wanted) {
			t.Fatalf("ICT plan arguments = %q, want %q", trace, wanted)
		}
	}
}

func TestRunInventoryWritesIsolatedValidatedReport(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/iam/identity/token":
			_, _ = w.Write([]byte(`{"access_token":"synthetic-token"}`))
		case "/iam/identity/userinfo":
			_, _ = w.Write([]byte(`{"account_id":"account-1"}`))
		case "/rm/v2/resource_groups":
			_, _ = w.Write([]byte(`{"resources":[{"name":"Default","state":"ACTIVE"}]}`))
		case "/containers/v1/versions":
			_, _ = w.Write([]byte(`{"openshift":[{"major":4,"minor":22,"default":true}]}`))
		case "/containers/v2/vpc/getZones":
			_, _ = w.Write([]byte(`[{"name":"us-south-1"}]`))
		case "/containers/v2/getFlavors":
			_, _ = w.Write([]byte(`[{"name":"bx2.4x16"}]`))
		default:
			t.Fatalf("unexpected synthetic inventory request %s", r.URL.RequestURI())
		}
	}))
	defer server.Close()
	previousTransport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previousTransport })
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.yaml")
	reportPath := filepath.Join(directory, "report.json")
	config := `version: 1
targets:
  target-a:
    providers: [vpc-gen2]
    default_region: us-south
    endpoints:
      iam: ` + server.URL + `/iam
      container_service: ` + server.URL + `/containers
      global_tagging: https://tagging.example.invalid
      resource_management: ` + server.URL + `/rm
      resource_controller: https://resource-controller.example.invalid
      vpc: https://vpc.{region}.example.invalid
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runInventory(context.Background(), configPath, "target-a", "inventory-a", "revision-a", reportPath, "synthetic-key"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.DecodeInventoryReport(data, "target-a", "inventory-a", "revision-a"); err != nil {
		t.Fatalf("inventory report is not controller-valid: %v", err)
	}
	if strings.Contains(string(data), "synthetic-key") || strings.Contains(string(data), server.URL) {
		t.Fatalf("inventory report disclosed a credential or endpoint: %s", data)
	}
}

func TestEmitValidatedInventoryReportRejectsUnknownFields(t *testing.T) {
	directory := t.TempDir()
	reportPath := filepath.Join(directory, "report.json")
	data, err := json.Marshal(pipeline.InventoryReport{
		Version:  1,
		Target:   "target-a",
		RunID:    "inventory-a",
		Revision: "revision-a",
		Catalog: inventory.Catalog{
			Version:        inventory.CatalogVersion,
			Target:         "target-a",
			Providers:      []string{"vpc-gen2"},
			Versions:       []inventory.Version{{Name: "4.22_openshift", Platform: "openshift", Default: true, Supported: true}},
			ResourceGroups: []string{"Default"},
			VPCLocations:   []inventory.Location{{Name: "us-south-1", Flavors: []string{"bx2.4x16"}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	data = append(data[:len(data)-1], []byte(",\"endpoint\":\"https://credentialed.example.invalid\"}\n")...)
	if err := os.WriteFile(reportPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := emitValidatedInventoryReport(reportPath, "target-a", "inventory-a", "revision-a"); err == nil || strings.Contains(err.Error(), "credentialed.example.invalid") {
		t.Fatalf("unknown endpoint field error = %v", err)
	}
}

func TestRunDestroyUsesFrozenContextAndProducesValidatedReport(t *testing.T) {
	directory := t.TempDir()
	backendFile := filepath.Join(directory, "backend.json")
	recoveryFile := filepath.Join(directory, "recovery.json")
	resultFile := filepath.Join(directory, "result.json")
	reportFile := filepath.Join(directory, "report.json")
	if err := writeJSON(backendFile, backendConfig{Version: 1, Bucket: "bucket", Key: "key", Region: "us-south", Endpoint: "https://s3.example.invalid"}); err != nil {
		t.Fatal(err)
	}
	recovery := servitorv1alpha1.RecoveryMetadata{
		Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Endpoints: map[string]string{
			"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
		},
		Values: servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"},
	}
	if err := writeJSON(recoveryFile, recovery); err != nil {
		t.Fatal(err)
	}
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\n[ \"$1\" = destroy ] && [ \"$2\" = destroy-a ] || exit 2\n[ -f \"$4\" ] || exit 3\nprintf '%s' '{\"version\":1,\"operation\":\"destroy\"}' > \"$8\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "frozen"}
	if err := runDestroy(context.Background(), "uid", "destroy-a", options, backendFile, recoveryFile, resultFile, reportFile, ict); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(reportFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.DecodeReport(data, "uid", "destroy-a"); err != nil {
		t.Fatalf("destroy report is not controller-valid: %v", err)
	}
	var context ictContext
	data, err = os.ReadFile(filepath.Join(directory, "context.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &context); err != nil || context.StateID != "destroy-a" || context.Recovery.TFVarsSHA256 != recovery.TFVarsSHA256 || context.Values.ClusterName != recovery.Values.ClusterName {
		t.Fatalf("destroy context = %+v, err=%v", context, err)
	}
}

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/config"
	"github.com/bevicted/servitor/internal/controller"
	"github.com/bevicted/servitor/internal/pipeline"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type integrationReportLogs struct{ data []byte }

type connectedEndpointMetadata struct {
	PublicURL  string
	PrivateURL string
}

func (m connectedEndpointMetadata) authMode() string {
	if m.PublicURL != "" {
		return "public"
	}
	if m.PrivateURL != "" {
		return "vpn"
	}
	return "unsupported"
}

func (m connectedEndpointMetadata) selectedURL() string {
	if m.PublicURL != "" {
		return m.PublicURL
	}
	return m.PrivateURL
}

func (l integrationReportLogs) ReadContainerLog(context.Context, string, string, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.data)), nil
}

func TestAuthBundleConnectedFlow(t *testing.T) {
	task := filepath.Join(t.TempDir(), "servitor-task")
	build := exec.Command("go", "build", "-o", task, "../servitor-task")
	if err := build.Run(); err != nil {
		t.Fatalf("build servitor task: %v", err)
	}
	for _, test := range []struct {
		name      string
		endpoints connectedEndpointMetadata
		wantMode  string
		failure   bool
	}{
		{name: "public-first", endpoints: connectedEndpointMetadata{PublicURL: "https://api.public.example.invalid", PrivateURL: "https://api.private.example.invalid"}, wantMode: "public"},
		{name: "private-only", endpoints: connectedEndpointMetadata{PrivateURL: "https://api.private.example.invalid"}, wantMode: "vpn"},
		{name: "publication failure", endpoints: connectedEndpointMetadata{PrivateURL: "https://api.private.example.invalid"}, wantMode: "vpn", failure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runConnectedAuthBundleFlow(t, task, test.endpoints, test.wantMode, test.failure)
		})
	}
}

func runConnectedAuthBundleFlow(t *testing.T, task string, endpoints connectedEndpointMetadata, wantMode string, publicationFailure bool) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	operator := connectedAuthConfig()
	if err := operator.Validate(); err != nil {
		t.Fatal(err)
	}
	settings, err := controllerConfig(operator)
	if err != nil {
		t.Fatal(err)
	}
	scheme, err := controller.NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	cluster := &servitorv1alpha1.ServitorCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "allocation", Namespace: "servitor", UID: "allocation-uid", Generation: 1, Finalizers: []string{servitorv1alpha1.CleanupFinalizer}},
		Spec: servitorv1alpha1.ServitorClusterSpec{
			Slack:     servitorv1alpha1.SlackIdentity{OwnerID: "U012AB3CD", ChannelID: "C123", ThreadTimestamp: "1.2"},
			Lifecycle: servitorv1alpha1.LifecyclePolicy{InitialLeaseSeconds: 3600, RetrySeconds: []int64{60}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&servitorv1alpha1.ServitorCluster{}, &tektonv1.PipelineRun{}, &tektonv1.TaskRun{}).WithObjects(cluster).Build()
	reconciler := &controller.Reconciler{Client: kube, DirectReader: kube, Scheme: scheme, Config: settings, Now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stored := connectedCluster(t, kube, request)
	frozen := *stored.Status.ResolvedOptions
	// Planning normally supplies the final cluster name before approval.
	frozen.ClusterName = "allocation"
	stored.Status.ResolvedOptions = &frozen
	if frozen.Network.AuthPolicy == nil {
		t.Fatal("configured private policy was not frozen")
	}
	changed := settings.NetworkBindings["production"]
	changed.AuthPolicy.VPNServerID = "changed-after-snapshot"
	settings.NetworkBindings["production"] = changed
	reconciler.Config = settings
	if frozen.Network.AuthPolicy.VPNServerID == "changed-after-snapshot" {
		t.Fatal("configuration change rewrote the frozen policy")
	}
	stored.Status.Recovery = connectedRecovery(frozen)
	stored.Status.Phase = servitorv1alpha1.PhaseAwaitingApproval
	stored.Status.Operation = &servitorv1alpha1.OperationReference{ID: "plan-allocation", Kind: "plan", Adopted: true}
	stored.Status.ReviewDeadline = &metav1.Time{Time: now.Add(time.Hour)}
	stored.Status.ReviewGeneration = stored.Generation
	if err := kube.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	stored = connectedCluster(t, kube, request)
	stored.Spec.Lifecycle.Approval = "approved"
	stored.Generation++
	if err := kube.Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stored = connectedCluster(t, kube, request)
	if stored.Status.Operation == nil || stored.Status.Operation.Kind != "apply" {
		t.Fatalf("approval did not start apply: %+v", stored.Status.Operation)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stored = connectedCluster(t, kube, request)
	run := &tektonv1.PipelineRun{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: stored.Status.Operation.PipelineRunName}, run); err != nil {
		t.Fatal(err)
	}
	parameters := make(map[string]string, len(run.Spec.Params))
	for _, parameter := range run.Spec.Params {
		parameters[parameter.Name] = parameter.Value.StringVal
	}
	var taskOptions servitorv1alpha1.ResolvedOptions
	if err := json.Unmarshal([]byte(parameters["resolved-options"]), &taskOptions); err != nil || !reflect.DeepEqual(taskOptions, frozen) {
		t.Fatal("frozen options did not survive the PipelineRun handoff")
	}
	if parameters["auth-eligible"] != "true" || parameters["auth-secret"] != pipeline.AuthResourceName(string(stored.UID)) {
		t.Fatal("apply task did not receive publication permission")
	}

	secret := &corev1.Secret{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "servitor", Name: parameters["auth-secret"]}, secret); err != nil {
		t.Fatal(err)
	}
	workdir := t.TempDir()
	reportPath := filepath.Join(workdir, "report.json")
	writeConnectedTaskInputs(t, workdir, frozen, *stored.Status.Recovery, stored.Status.Backend, stored.Status.Operation.ID)
	ict, terraform, bundle, mode, expiry := writeConnectedICTFixture(t, workdir, endpoints, now)
	if mode != wantMode {
		t.Fatalf("endpoint metadata selected mode %q, want %q", mode, wantMode)
	}
	taskArgs := []string{
		"-cluster-uid", string(stored.UID), "-operation-id", stored.Status.Operation.ID, "-operation-kind", "apply",
		"-resolved-options", filepath.Join(workdir, "options.json"), "-backend-config", filepath.Join(workdir, "backend.json"),
		"-recovery", filepath.Join(workdir, "recovery.json"), "-ict-result", filepath.Join(workdir, "result.json"), "-report", reportPath,
		"-ict", ict, "-terraform", terraform, "-auth-eligible=true", "-auth-manifest", filepath.Join(workdir, "auth", "manifest.json"), "-auth-output-dir", filepath.Join(workdir, "auth"),
	}
	if err := exec.Command(task, taskArgs...).Run(); err != nil {
		t.Fatalf("run task ICT fixture: %v", err)
	}
	contextData, err := os.ReadFile(filepath.Join(workdir, "ict-context.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ictContext struct {
		Recovery struct {
			Values servitorv1alpha1.RecoveryValues `json:"values"`
		} `json:"recovery"`
	}
	if err := json.Unmarshal(contextData, &ictContext); err != nil || !reflect.DeepEqual(ictContext.Recovery.Values.AuthPolicy, frozen.Network.AuthPolicy) {
		t.Fatal("ICT fixture did not receive the frozen auth policy")
	}

	publisher, tokenPath, caPath := connectedPublisherServer(t, secret.DeepCopy(), bundle, publicationFailure)
	defer publisher.Close()
	host, port, err := net.SplitHostPort(strings.TrimPrefix(publisher.URL, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	publish := exec.Command(task,
		"-publish-auth", "-cluster-uid", string(stored.UID), "-operation-id", stored.Status.Operation.ID,
		"-publish-auth-secret", secret.Name, "-auth-manifest", filepath.Join(workdir, "auth", "manifest.json"), "-auth-output-dir", filepath.Join(workdir, "auth"),
		"-report", reportPath, "-publisher-namespace", "servitor", "-publisher-token", tokenPath, "-publisher-ca", caPath,
	)
	publish.Env = append(os.Environ(), "KUBERNETES_SERVICE_HOST="+host, "KUBERNETES_SERVICE_PORT="+port)
	if err := publish.Run(); err != nil {
		t.Fatalf("publish task result: %v", err)
	}
	storedSecret := connectedPublishedSecret(t, publisher)
	if publicationFailure {
		if len(storedSecret.Data) != 0 {
			t.Fatal("failed publication left Secret data")
		}
	} else if !sameConnectedBundle(storedSecret.Data, bundle) {
		t.Fatal("published Secret did not receive the complete bundle")
	}
	reportData, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	report, err := pipeline.DecodeReport(reportData, string(stored.UID), stored.Status.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if publicationFailure {
		if report.Auth == nil || report.Auth.Availability != "unavailable" || report.Auth.Mode != "" || report.Auth.Expiry != "" {
			t.Fatal("failed publication did not preserve the bounded unavailable result")
		}
	} else if report.Auth == nil || report.Auth.Availability != "available" || report.Auth.Mode != wantMode || (wantMode == "vpn" && report.Auth.Expiry != expiry) {
		t.Fatal("published report did not contain the safe endpoint-selected result")
	}

	run.Status.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionSucceeded, Status: corev1.ConditionTrue}}
	run.Status.ChildReferences = []tektonv1.ChildStatusReference{{TypeMeta: runtime.TypeMeta{Kind: "TaskRun"}, Name: "apply-task", PipelineTaskName: "operation"}}
	if err := kube.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	taskRun := &tektonv1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: "apply-task", Namespace: "servitor"}, Status: tektonv1.TaskRunStatus{TaskRunStatusFields: tektonv1.TaskRunStatusFields{PodName: "apply-pod", Steps: []tektonv1.StepState{{Name: pipeline.ReportContainerName, Container: "step-report"}}}}}
	if err := kube.Create(context.Background(), taskRun); err != nil {
		t.Fatal(err)
	}
	reconciler.Logs = integrationReportLogs{data: reportData}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stored = connectedCluster(t, kube, request)
	if stored.Status.Phase != servitorv1alpha1.PhaseReady || stored.Status.Ready == nil || stored.Status.Auth == nil {
		t.Fatal("infrastructure readiness was coupled to auth publication")
	}
	if publicationFailure && stored.Status.Auth.Availability != "unavailable" {
		t.Fatal("failed publication was not adopted as unavailable")
	}
	if !publicationFailure && (stored.Status.Auth.Mode != wantMode || stored.Status.Auth.Expiry != report.Auth.Expiry) {
		t.Fatal("controller adopted inconsistent auth metadata")
	}
}

func connectedAuthConfig() config.Config {
	binding := config.NetworkBinding{AccountID: "account", VPCID: "vpc", VPCRegion: "us-south", SubnetID: "subnet", PublicGatewayID: "gateway", Zone: "us-south-1", VPNServerID: "vpn", SecretsManagerID: "secrets", SecretsManagerRegion: "us-south", SecretGroupID: "group", CertificateTemplate: "template", Issuer: "issuer", TTL: "2h"}
	return config.Config{
		Namespace: "servitor", Slack: config.SlackConfig{ChannelID: "C123"},
		Defaults:  config.DefaultsConfig{Version: "4.22", Target: "production", Provider: "vpc-gen2", ResourceGroup: "Default", OpenShiftFlavor: "bx2.4x16", KubernetesFlavor: "bx2.2x8"},
		Network:   config.NetworkConfig{Bindings: map[string]config.NetworkBinding{"existing": binding}, TargetBindings: map[string]string{"production": "existing"}},
		Lifecycle: config.LifecycleConfig{ConfirmationTimeout: time.Minute, Lease: time.Hour, RetryIntervals: []time.Duration{time.Minute}}, Auth: config.AuthConfig{},
		ICT:     config.ICTConfig{TargetConfigMap: "ict-config", TargetConfigKey: "config.yaml"},
		COS:     config.COSConfig{Bucket: "bucket", Region: "us-south", Endpoint: "https://s3.example.invalid", KeyPrefix: "servitor", SkipCredentialsValidation: true, SkipMetadataAPICheck: true, SkipRegionValidation: true, SkipRequestingAccountID: true, ForcePathStyle: true},
		Images:  config.ImagesConfig{Execution: "registry.example.invalid/task@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Secrets: config.SecretsConfig{Slack: "slack", COS: "cos", IBM: "ibm"},
	}
}

func connectedCluster(t *testing.T, kube client.Client, request ctrl.Request) *servitorv1alpha1.ServitorCluster {
	t.Helper()
	cluster := &servitorv1alpha1.ServitorCluster{}
	if err := kube.Get(context.Background(), request.NamespacedName, cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}

func connectedRecovery(options servitorv1alpha1.ResolvedOptions) *servitorv1alpha1.RecoveryMetadata {
	return &servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "production", TFVarsSHA256: strings.Repeat("a", 64), Endpoints: map[string]string{
		"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
	}, Values: servitorv1alpha1.RecoveryValues{ClusterName: options.ClusterName, ResourceGroupName: options.ResourceGroup, Region: "us-south", ClusterMode: "vpc", Platform: options.Platform, KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: options.Network.Zone, Flavor: options.Flavor, AccountID: options.Network.AccountID, VPCRegion: options.Network.VPCRegion, VPCID: options.Network.VPCID, SubnetIDs: []string{options.Network.SubnetID}, PublicGatewayIDs: []string{options.Network.PublicGatewayID}, AuthPolicy: options.Network.AuthPolicy}}
}

func writeConnectedTaskInputs(t *testing.T, directory string, options servitorv1alpha1.ResolvedOptions, recovery servitorv1alpha1.RecoveryMetadata, backend *servitorv1alpha1.BackendIdentity, operation string) {
	t.Helper()
	for path, value := range map[string]any{
		"options.json": options, "recovery.json": recovery,
		"backend.json": map[string]any{"version": backend.Version, "bucket": backend.Bucket, "key": backend.Key, "region": backend.Region, "endpoint": backend.Endpoint, "skip_credentials_validation": backend.SkipCredentialsValidation, "skip_metadata_api_check": backend.SkipMetadataAPICheck, "skip_region_validation": backend.SkipRegionValidation, "skip_requesting_account_id": backend.SkipRequestingAccountID, "force_path_style": backend.ForcePathStyle},
		"fresh.json":   map[string]any{"version": 1, "state_id": operation, "plan_path": filepath.Join(directory, "workspace", ".cluster", "create.tfplan"), "values": recovery.Values, "recovery": map[string]any{"version": recovery.Version, "target": recovery.Target, "endpoints": recovery.Endpoints, "values": recovery.Values, "tfvars_sha256": recovery.TFVarsSHA256}},
	} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, path), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(directory, "workspace", ".cluster"), 0o700); err != nil {
		t.Fatal(err)
	}
}

func writeConnectedICTFixture(t *testing.T, directory string, endpoints connectedEndpointMetadata, now time.Time) (string, string, map[string][]byte, string, string) {
	t.Helper()
	mode := endpoints.authMode()
	bundle, expiry := connectedBundle(t, endpoints, now)
	source := filepath.Join(directory, "fixture-auth")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, contents := range bundle {
		if err := os.WriteFile(filepath.Join(source, name), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	artifacts := []map[string]string{{"name": "kubeconfig.yaml"}}
	if mode == "vpn" {
		artifacts = append(artifacts, map[string]string{"name": "client.ovpn"})
	}
	manifest, err := json.Marshal(map[string]any{"version": 1, "availability": "available", "mode": mode, "expiry": expiry, "artifacts": artifacts})
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(directory, "fixture-manifest.json")
	if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	contextPath := filepath.Join(directory, "ict-context.json")
	ict := filepath.Join(directory, "ict")
	script := "#!/bin/sh\nset -eu\ncase \"$1\" in\nreview) cp \"$4\" \"$CONTEXT_TRACE\"; cp \"$FRESH_RESULT\" \"$8\" ;;\napply) printf '%s' '{\"version\":1,\"operation\":\"apply\",\"workspace\":\"'\"$WORKSPACE\"'\"}' > \"$8\"; mkdir -p \"$AUTH_OUTPUT\"; cp \"$AUTH_SOURCE\"/* \"$AUTH_OUTPUT\"/; cp \"$FIXTURE_MANIFEST\" \"$AUTH_MANIFEST\" ;;\n*) exit 2 ;;\nesac\n"
	if err := os.WriteFile(ict, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	terraform := filepath.Join(directory, "terraform")
	if err := os.WriteFile(terraform, []byte("#!/bin/sh\nif [ \"$#\" -gt 3 ]; then printf '%s' '{\"format_version\":\"1.2\",\"resource_changes\":[]}' ; else printf '%s' '{\"format_version\":\"1.2\",\"values\":{\"root_module\":{}}}' ; fi\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AUTH_SOURCE", source)
	t.Setenv("AUTH_OUTPUT", filepath.Join(directory, "auth"))
	t.Setenv("AUTH_MANIFEST", filepath.Join(directory, "auth", "manifest.json"))
	t.Setenv("FIXTURE_MANIFEST", manifestPath)
	t.Setenv("FRESH_RESULT", filepath.Join(directory, "fresh.json"))
	t.Setenv("CONTEXT_TRACE", contextPath)
	t.Setenv("WORKSPACE", filepath.Join(directory, "workspace"))
	return ict, terraform, bundle, mode, expiry
}

func connectedBundle(t *testing.T, endpoints connectedEndpointMetadata, now time.Time) (map[string][]byte, string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "synthetic root"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	clientTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "synthetic client"}, NotBefore: now.Add(-time.Minute), NotAfter: expires, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	clientPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{CurrentContext: "synthetic", Clusters: map[string]*clientcmdapi.Cluster{"synthetic": {Server: endpoints.selectedURL(), CertificateAuthorityData: caPEM}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"synthetic": {ClientCertificateData: clientPEM, ClientKeyData: keyPEM}}, Contexts: map[string]*clientcmdapi.Context{"synthetic": {Cluster: "synthetic", AuthInfo: "synthetic"}}})
	if err != nil {
		t.Fatal(err)
	}
	bundle := map[string][]byte{"kubeconfig.yaml": kubeconfig}
	if endpoints.authMode() == "vpn" {
		bundle["client.ovpn"] = []byte("client\nremote vpn.example.invalid 443 udp\n<ca>\n" + string(caPEM) + "</ca>\n<cert>\n" + string(clientPEM) + "</cert>\n<key>\n" + string(keyPEM) + "</key>\n")
		return bundle, expires.Format(time.RFC3339)
	}
	return bundle, ""
}

func connectedPublisherServer(t *testing.T, initial *corev1.Secret, expected map[string][]byte, failUpdate bool) (*httptest.Server, string, string) {
	t.Helper()
	var lock sync.Mutex
	secret := initial.DeepCopy()
	protoScheme := runtime.NewScheme()
	if err := corev1.AddToScheme(protoScheme); err != nil {
		t.Fatal(err)
	}
	proto := protobuf.NewSerializer(protoScheme, protoScheme)
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		if request.URL.Path != "/api/v1/namespaces/servitor/secrets/"+secret.Name {
			http.NotFound(response, request)
			return
		}
		switch request.Method {
		case http.MethodGet:
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(secret)
		case http.MethodPut:
			if failUpdate {
				response.WriteHeader(http.StatusInternalServerError)
				return
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			update := &corev1.Secret{}
			if strings.Contains(request.Header.Get("Content-Type"), "protobuf") {
				_, _, err = proto.Decode(body, nil, update)
			} else {
				err = json.Unmarshal(body, update)
			}
			if err != nil || !sameConnectedBundle(update.Data, expected) {
				response.WriteHeader(http.StatusUnprocessableEntity)
				return
			}
			secret = update.DeepCopy()
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(secret)
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	certificate := server.Certificate()
	caPath := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("synthetic-publisher-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return server, tokenPath, caPath
}

func connectedPublishedSecret(t *testing.T, server *httptest.Server) *corev1.Secret {
	t.Helper()
	response, err := server.Client().Get(server.URL + "/api/v1/namespaces/servitor/secrets/" + pipeline.AuthResourceName("allocation-uid"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("read publisher Secret: %s", response.Status)
	}
	secret := &corev1.Secret{}
	if err := json.NewDecoder(response.Body).Decode(secret); err != nil {
		t.Fatal(err)
	}
	return secret
}

func sameConnectedBundle(actual, expected map[string][]byte) bool {
	if len(actual) != len(expected) {
		return false
	}
	for name, expectedContents := range expected {
		actualContents, found := actual[name]
		if !found || sha256.Sum256(actualContents) != sha256.Sum256(expectedContents) {
			return false
		}
	}
	return true
}

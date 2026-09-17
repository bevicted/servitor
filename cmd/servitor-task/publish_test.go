package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestPublicationKubeconfigAcceptsOnlyCompleteKubeconfigArtifact(t *testing.T) {
	directory := t.TempDir()
	manifestPath := filepath.Join(directory, "manifest.json")
	outputDir := filepath.Join(directory, "auth")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := authManifest{Version: 1, Availability: "available", Mode: "public"}
	manifest.Artifacts = append(manifest.Artifacts, struct {
		Name string `json:"name"`
	}{Name: "kubeconfig.yaml"})
	contents := testKubeconfigBytes(t, testKubeconfig(t))
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "kubeconfig.yaml"), contents, 0o600); err != nil {
		t.Fatal(err)
	}
	published, err := publicationKubeconfig(manifestPath, outputDir)
	if err != nil || string(published) != string(contents) {
		t.Fatalf("publication artifact = %q, %v", published, err)
	}
	manifest.Artifacts = append(manifest.Artifacts, struct {
		Name string `json:"name"`
	}{Name: "client.ovpn"})
	data, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := publicationKubeconfig(manifestPath, outputDir); err == nil {
		t.Fatal("accepted an extra auth artifact")
	}
}

func TestPublicationBundleRejectsUnsafeOrInconsistentVPNProfile(t *testing.T) {
	directory := t.TempDir()
	outputDir := filepath.Join(directory, "auth")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := testKubeconfig(t)
	certificate := config.AuthInfos["admin"].ClientCertificateData
	key := config.AuthInfos["admin"].ClientKeyData
	ca := config.Clusters["example"].CertificateAuthorityData
	block, _ := pem.Decode(certificate)
	parsed, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	manifest := authManifest{Version: 1, Availability: "available", Mode: "vpn", Expiry: parsed.NotAfter.UTC().Format(time.RFC3339)}
	manifest.Artifacts = append(manifest.Artifacts, struct {
		Name string `json:"name"`
	}{Name: "kubeconfig.yaml"}, struct {
		Name string `json:"name"`
	}{Name: "client.ovpn"})
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(directory, "manifest.json")
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "kubeconfig.yaml"), testKubeconfigBytes(t, config), 0o600); err != nil {
		t.Fatal(err)
	}
	profile := "client\nremote vpn.example.invalid 443 udp\n<ca>\n" + string(ca) + "</ca>\n<cert>\n" + string(certificate) + "</cert>\n<key>\n" + string(key) + "</key>\n"
	if err := os.WriteFile(filepath.Join(outputDir, "client.ovpn"), []byte(profile), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, status, err := publicationBundle(manifestPath, outputDir); err != nil || status.Mode != "vpn" {
		t.Fatalf("valid VPN bundle = %+v, %v", status, err)
	}
	for _, test := range []struct {
		name    string
		profile string
		expiry  string
	}{
		{"unsafe directive", "plugin evil.so\n" + profile, manifest.Expiry},
		{"incorrect expiry", profile, time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest.Expiry = test.expiry
			data, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(outputDir, "client.ovpn"), []byte(test.profile), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := publicationBundle(manifestPath, outputDir); err == nil {
				t.Fatal("accepted unsafe or inconsistent VPN bundle")
			}
		})
	}
}

func TestPublicationBundleRejectsPartialExtraAndOversizedArtifacts(t *testing.T) {
	directory, _, _, _, _ := publicationFixture(t, "vpn")
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string, string)
	}{
		{"partial", func(t *testing.T, _ string, output string) {
			if err := os.Remove(filepath.Join(output, publishedVPNKey)); err != nil {
				t.Fatal(err)
			}
		}},
		{"extra", func(t *testing.T, _ string, output string) {
			if err := os.WriteFile(filepath.Join(output, "extra"), []byte("synthetic"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"oversized", func(t *testing.T, _ string, output string) {
			if err := os.WriteFile(filepath.Join(output, publishedVPNKey), make([]byte, maxPublishedFileBytes+1), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			caseDirectory := t.TempDir()
			caseManifest, caseOutput := filepath.Join(caseDirectory, "manifest.json"), filepath.Join(caseDirectory, "auth")
			if err := os.Mkdir(caseOutput, 0o700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"manifest.json", filepath.Join("auth", publishedKubeconfigKey), filepath.Join("auth", publishedVPNKey)} {
				contents, err := os.ReadFile(filepath.Join(directory, name))
				if err != nil {
					t.Fatal(err)
				}
				destination := filepath.Join(caseDirectory, name)
				if err := os.WriteFile(destination, contents, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			test.mutate(t, caseManifest, caseOutput)
			if _, _, err := publicationBundle(caseManifest, caseOutput); err == nil {
				t.Fatal("accepted invalid auth artifact set")
			}
		})
	}
}

func TestPublishAuthBundleStoresExactCompleteBundleAndMarksSafeReport(t *testing.T) {
	for _, mode := range []string{"public", "vpn"} {
		t.Run(mode, func(t *testing.T) {
			_, manifestPath, outputDir, bundle, status := publicationFixture(t, mode)
			reportPath := publicationReport(t, "uid", "apply-uid")
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: "apply-uid"}}}
			clientset := k8sfake.NewSimpleClientset(secret)
			clientset.ClearActions()
			if !publishAuthBundle(context.Background(), clientset.CoreV1().Secrets("ns"), "auth", "uid", "apply-uid", bundle, status, reportPath) {
				t.Fatal("publish rejected valid fixture")
			}
			stored, err := clientset.CoreV1().Secrets("ns").Get(context.Background(), "auth", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(stored.Data, bundle) {
				t.Fatalf("stored bundle differs from source: %#v", stored.Data)
			}
			report, err := readJSON[pipeline.Report](reportPath)
			if err != nil || !reflect.DeepEqual(report.Auth, &status) {
				t.Fatalf("safe published report = %#v, %v", report.Auth, err)
			}
			for _, action := range clientset.Actions() {
				if action.GetVerb() == "create" {
					t.Fatalf("publisher attempted Secret creation: %#v", action)
				}
			}
			if _, _, err := publicationBundle(manifestPath, outputDir); err != nil {
				t.Fatalf("fixture became unreadable: %v", err)
			}
		})
	}
}

func TestPublishAuthBundleRejectsStaleMissingAndCancelledSecrets(t *testing.T) {
	_, _, _, bundle, status := publicationFixture(t, "public")
	for _, test := range []struct {
		name      string
		secret    *corev1.Secret
		cancelled bool
	}{
		{"wrong uid", &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "other"}, Annotations: map[string]string{publisherOperationKey: "apply-uid"}}}, false},
		{"wrong operation", &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: "other"}}}, false},
		{"deleted", nil, false},
		{"cancelled", &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: "apply-uid"}}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			reportPath := publicationReport(t, "uid", "apply-uid")
			objects := []corev1.Secret{}
			if test.secret != nil {
				objects = append(objects, *test.secret)
			}
			clientset := k8sfake.NewSimpleClientset()
			for index := range objects {
				if _, err := clientset.CoreV1().Secrets("ns").Create(context.Background(), &objects[index], metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			clientset.ClearActions()
			ctx := context.Background()
			if test.cancelled {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			if publishAuthBundle(ctx, clientset.CoreV1().Secrets("ns"), "auth", "uid", "apply-uid", bundle, status, reportPath) {
				t.Fatal("published stale, missing, or cancelled Secret")
			}
			report, err := readJSON[pipeline.Report](reportPath)
			if err != nil || report.Auth == nil || report.Auth.Availability != "unavailable" {
				t.Fatalf("failed publication changed safe report: %#v, %v", report.Auth, err)
			}
			for _, action := range clientset.Actions() {
				if action.GetVerb() == "update" || action.GetVerb() == "create" {
					t.Fatalf("rejected publisher mutated Secret: %#v", action)
				}
			}
		})
	}
}

func TestPublishAuthBundleCannotRecreateSecretDeletedDuringPublication(t *testing.T) {
	_, _, _, bundle, status := publicationFixture(t, "public")
	reportPath := publicationReport(t, "uid", "apply-uid")
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: "apply-uid"}}}
	clientset := k8sfake.NewSimpleClientset(secret)
	clientset.PrependReactor("update", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if err := clientset.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("secrets"), "ns", "auth"); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	if publishAuthBundle(context.Background(), clientset.CoreV1().Secrets("ns"), "auth", "uid", "apply-uid", bundle, status, reportPath) {
		t.Fatal("publisher reported success after cleanup deleted its Secret")
	}
	if _, err := clientset.CoreV1().Secrets("ns").Get(context.Background(), "auth", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("late publisher recreated cleanup Secret: %v", err)
	}
	report, err := readJSON[pipeline.Report](reportPath)
	if err != nil || report.Auth == nil || report.Auth.Availability != "unavailable" {
		t.Fatalf("cleanup race changed safe report: %#v, %v", report.Auth, err)
	}
}

func publicationFixture(t *testing.T, mode string) (string, string, string, map[string][]byte, servitorv1alpha1.AuthStatus) {
	t.Helper()
	directory, outputDir := t.TempDir(), ""
	outputDir = filepath.Join(directory, "auth")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := testKubeconfig(t)
	bundle := map[string][]byte{publishedKubeconfigKey: testKubeconfigBytes(t, config)}
	status := servitorv1alpha1.AuthStatus{Availability: "available", Mode: mode}
	artifacts := []string{publishedKubeconfigKey}
	if mode == "vpn" {
		certificate, key, ca := config.AuthInfos["admin"].ClientCertificateData, config.AuthInfos["admin"].ClientKeyData, config.Clusters["example"].CertificateAuthorityData
		block, _ := pem.Decode(certificate)
		parsed, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		status.Expiry = parsed.NotAfter.UTC().Format(time.RFC3339)
		bundle[publishedVPNKey] = []byte("client\nremote vpn.example.invalid 443 udp\n<ca>\n" + string(ca) + "</ca>\n<cert>\n" + string(certificate) + "</cert>\n<key>\n" + string(key) + "</key>\n")
		artifacts = append(artifacts, publishedVPNKey)
	}
	for name, contents := range bundle {
		if err := os.WriteFile(filepath.Join(outputDir, name), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest := authManifest{Version: 1, Availability: "available", Mode: mode, Expiry: status.Expiry}
	for _, name := range artifacts {
		manifest.Artifacts = append(manifest.Artifacts, struct {
			Name string `json:"name"`
		}{Name: name})
	}
	contents, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(directory, "manifest.json")
	if err := os.WriteFile(manifestPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	return directory, manifestPath, outputDir, bundle, status
}

func publicationReport(t *testing.T, uid, operation string) string {
	t.Helper()
	values := servitorv1alpha1.RecoveryValues{ClusterName: "fixture", ResourceGroupName: "group", Region: "us-south", ClusterMode: "vpc", Platform: "kubernetes", KubeVersion: "1.31", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.2x8", AccountID: "account", VPCRegion: "us-south", VPCID: "vpc", SubnetIDs: []string{"subnet"}, PublicGatewayIDs: []string{"gateway"}}
	recovery := servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Values: values, Endpoints: map[string]string{"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid"}}
	report := pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "1.31"}, ClusterName: "fixture"}, Recovery: recovery, Auth: &servitorv1alpha1.AuthStatus{Availability: "unavailable"}}
	if err := report.Validate(uid, operation); err != nil {
		t.Fatalf("publication report fixture is invalid: %v", err)
	}
	path := filepath.Join(t.TempDir(), "report.json")
	if err := writeJSON(path, report); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPublishedKubeconfigRejectsMalformedContentContainingRequiredSubstrings(t *testing.T) {
	contents := []byte("certificate-authority-data: Y2E=\nclient-certificate-data: Y2VydA==\nclient-key-data: a2V5\n")
	if err := validatePublishedKubeconfig(contents); err == nil {
		t.Fatal("accepted malformed content containing required substrings")
	}
}

func TestPublishedKubeconfigRejectsCredentialReferences(t *testing.T) {
	config := testKubeconfig(t)
	config.AuthInfos["admin"].Exec = &clientcmdapi.ExecConfig{Command: "helper"}
	if err := validatePublishedKubeconfig(testKubeconfigBytes(t, config)); err == nil {
		t.Fatal("accepted an exec credential reference")
	}
}

func testKubeconfig(t *testing.T) *clientcmdapi.Config {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKeyDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	config := clientcmdapi.NewConfig()
	config.Clusters["example"] = &clientcmdapi.Cluster{
		Server:                   "https://api.example.test:6443",
		CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
	}
	config.AuthInfos["admin"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		ClientKeyData:         pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKeyDER}),
	}
	config.Contexts["admin@example"] = &clientcmdapi.Context{Cluster: "example", AuthInfo: "admin"}
	config.CurrentContext = "admin@example"
	return config
}

func testKubeconfigBytes(t *testing.T, config *clientcmdapi.Config) []byte {
	t.Helper()
	contents, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

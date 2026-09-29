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
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestPublicationCleanupUsesValidatedCertificateOwnershipAndRetainsOnlyPendingReference(t *testing.T) {
	certificate := &servitorv1alpha1.AuthCertificateReference{ID: "certificate-123", AllocationUID: "uid", AttemptID: "apply-uid"}
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}
	if got := publicationCertificate(servitorv1alpha1.AuthStatus{Availability: "available", Mode: "vpn", Certificate: certificate}, options, "uid", "apply-uid"); !reflect.DeepEqual(got, certificate) {
		t.Fatalf("validated certificate = %#v", got)
	}
	if got := publicationCertificate(servitorv1alpha1.AuthStatus{Availability: "available", Mode: "vpn", Certificate: certificate}, options, "other", "apply-uid"); got != nil {
		t.Fatalf("accepted mismatched certificate ownership: %#v", got)
	}
	for _, test := range []struct {
		name, outcome, wantReason string
	}{
		{name: "cleaned including not found", outcome: "cleaned", wantReason: "publisher-unavailable"},
		{name: "pending", outcome: "pending", wantReason: "publisher-unavailable"},
		{name: "unknown", outcome: "unknown", wantReason: "publisher-unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			trace := filepath.Join(directory, "trace")
			ict := filepath.Join(directory, "ict")
			script := fmt.Sprintf("#!/bin/sh\nset -eu\n[ \"$1\" = auth-cleanup ] || exit 2\nprintf '%%s' \"$@\" > %q\nprintf '%%s' '{\"version\":1,\"operation\":\"auth-cleanup\",\"auth_cleanup\":%q,\"cleanup_reason\":\"list\",\"cleanup_stage\":\"metadata-list\"}' > \"$6\"\n", trace, test.outcome)
			if err := os.WriteFile(ict, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			reportPath := publicationReport(t, "uid", "apply-uid")
			contextPath := filepath.Join(directory, "auth-context.json")
			if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := unavailableAfterPublicationFailure(reportPath, "uid", "apply-uid", "publisher-unavailable", ict, contextPath, certificate); err != nil {
				t.Fatal(err)
			}
			report, err := readJSON[pipeline.Report](reportPath)
			if err != nil || report.Auth == nil || report.Auth.Reason != test.wantReason {
				t.Fatalf("publication cleanup report = %#v, %v", report.Auth, err)
			}
			if test.outcome != "cleaned" && (report.Auth.CleanupOutcome != "pending" || report.Auth.CleanupReason != "list" || report.Auth.CleanupStage != "metadata-list" || !reflect.DeepEqual(report.Auth.Certificate, certificate)) {
				t.Fatalf("pending cleanup lost bounded observability: %#v", report.Auth)
			}
			if test.outcome == "cleaned" && (report.Auth.CleanupOutcome != "cleaned" || report.Auth.CleanupReason != "" || report.Auth.CleanupStage != "" || report.Auth.Certificate != nil) {
				t.Fatalf("successful cleanup retained state: %#v", report.Auth)
			}
			args, err := os.ReadFile(trace)
			if err != nil || !strings.Contains(string(args), "--certificate-idcertificate-123") || !strings.Contains(string(args), "--certificate-allocation-uiduid") || !strings.Contains(string(args), "--certificate-attempt-idapply-uid") {
				t.Fatalf("cleanup command did not receive exact ownership: %q, %v", args, err)
			}
		})
	}
}

func TestAuthRetryPublicationPreservesHistoricalPendingCertificate(t *testing.T) {
	directory := t.TempDir()
	currentAttempt := "auth-retry-current"
	historical := servitorv1alpha1.AuthCertificateReference{ID: "certificate-previous", AllocationUID: "uid", AttemptID: "apply-previous"}
	manifestPath := filepath.Join(directory, "manifest.json")
	if err := writeJSON(manifestPath, authManifest{
		Version: 1, Availability: "unavailable", Reason: "certificate-cleanup-pending", CleanupOutcome: "pending", CleanupReason: "transport",
		Certificate: &authCertificate{ID: historical.ID, AllocationUID: historical.AllocationUID, AttemptID: historical.AttemptID},
	}); err != nil {
		t.Fatal(err)
	}
	contextPath := filepath.Join(directory, "auth-context.json")
	if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	ict := filepath.Join(directory, "ict")
	script := `#!/bin/sh
set -eu
[ "$1" = auth-cleanup ]
printf '%s' '{"version":1,"operation":"auth-cleanup","auth_cleanup":"pending","cleanup_reason":"transport","certificate":{"id":"certificate-previous","allocation_uid":"uid","attempt_id":"apply-previous"}}' > "$6"
`
	if err := os.WriteFile(ict, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	reportPath := publicationReport(t, "uid", "auth-retry-operation")
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}
	if err := publishPublicAuthWithAttempt(true, options, "auth", "uid", "auth-retry-operation", currentAttempt, manifestPath, filepath.Join(directory, "auth"), reportPath, "ns", filepath.Join(directory, "token"), filepath.Join(directory, "ca"), ict, contextPath); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	report, err := pipeline.DecodeReport(data, "uid", "auth-retry-operation")
	if err != nil || report.Auth == nil || report.Auth.AttemptID != currentAttempt || report.Auth.CleanupOutcome != "pending" || !reflect.DeepEqual(report.Auth.Certificate, &historical) {
		t.Fatalf("historical pending certificate was not retained for recovery: %#v, %v", report.Auth, err)
	}
}

func TestHistoricalPendingCertificateIsRejectedOutsideValidatedPendingManifest(t *testing.T) {
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}
	historical := &servitorv1alpha1.AuthCertificateReference{ID: "certificate-previous", AllocationUID: "uid", AttemptID: "apply-previous"}
	pending := servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "certificate-cleanup-pending", CleanupOutcome: "pending", Certificate: historical}
	if got := publicationCertificate(pending, options, "uid", "auth-retry-current"); got != nil {
		t.Fatalf("accepted historical certificate as current issuance: %#v", got)
	}
	if got := historicalPendingPublicationCertificate(pending, options, "uid"); !reflect.DeepEqual(got, historical) {
		t.Fatalf("did not retain bounded historical pending certificate: %#v", got)
	}
	for _, test := range []struct {
		name        string
		certificate servitorv1alpha1.AuthCertificateReference
	}{
		{name: "wrong allocation", certificate: servitorv1alpha1.AuthCertificateReference{ID: "certificate-previous", AllocationUID: "other", AttemptID: "apply-previous"}},
		{name: "missing attempt", certificate: servitorv1alpha1.AuthCertificateReference{ID: "certificate-previous", AllocationUID: "uid"}},
		{name: "oversized ID", certificate: servitorv1alpha1.AuthCertificateReference{ID: strings.Repeat("x", 257), AllocationUID: "uid", AttemptID: "apply-previous"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := pending
			status.Certificate = &test.certificate
			if got := historicalPendingPublicationCertificate(status, options, "uid"); got != nil {
				t.Fatalf("accepted invalid historical certificate: %#v", got)
			}
		})
	}
}

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

func TestPublicationRejectsUnsafeCleanupStageWithoutEchoingIt(t *testing.T) {
	directory := t.TempDir()
	leak := "metadata-get:https://malicious.example.invalid/token-secret"
	manifest := authManifest{Version: 1, Availability: "unavailable", Reason: "certificate-cleanup-pending", CleanupOutcome: "pending", CleanupReason: "transport", CleanupStage: leak, Certificate: &authCertificate{AllocationUID: "uid", AttemptID: "attempt"}}
	manifestPath := filepath.Join(directory, "manifest.json")
	if err := writeJSON(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}
	_, _, err := publicationBundle(manifestPath, filepath.Join(directory, "auth"))
	if err == nil || strings.Contains(err.Error(), leak) {
		t.Fatalf("unsafe cleanup stage error = %v", err)
	}
}

func TestPublicationValidationPredicatesAreClosedAndValueFree(t *testing.T) {
	const marker = "private-value-must-not-escape"
	for _, test := range []struct {
		name    string
		want    string
		prepare func(*testing.T, string, string, string)
	}{
		{name: "manifest", want: "auth-manifest-invalid", prepare: func(t *testing.T, manifest, _ string, _ string) {
			if err := os.WriteFile(manifest, []byte(marker), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "artifact layout", want: "auth-artifact-layout-invalid", prepare: func(t *testing.T, _ string, output string, _ string) {
			if err := os.WriteFile(filepath.Join(output, "extra"), []byte(marker), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "kubeconfig", want: "auth-kubeconfig-invalid", prepare: func(t *testing.T, _ string, output string, _ string) {
			if err := os.WriteFile(filepath.Join(output, publishedKubeconfigKey), []byte(marker), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "VPN profile", want: "auth-vpn-profile-invalid", prepare: func(t *testing.T, _ string, output string, _ string) {
			profile, err := os.ReadFile(filepath.Join(output, publishedVPNKey))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(output, publishedVPNKey), append([]byte("plugin "+marker+"\n"), profile...), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "VPN profile trust", want: "auth-vpn-profile-trust-invalid", prepare: func(t *testing.T, _ string, output string, _ string) {
			profile, err := os.ReadFile(filepath.Join(output, publishedVPNKey))
			if err != nil {
				t.Fatal(err)
			}
			profile = []byte(strings.Replace(string(profile), "<ca>\n-----BEGIN CERTIFICATE-----", "<ca>\n-----BEGIN "+marker+"-----", 1))
			if err := os.WriteFile(filepath.Join(output, publishedVPNKey), profile, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "VPN certificate", want: "auth-vpn-certificate-invalid", prepare: func(t *testing.T, _ string, output string, _ string) {
			profile, err := os.ReadFile(filepath.Join(output, publishedVPNKey))
			if err != nil {
				t.Fatal(err)
			}
			profile = []byte(strings.Replace(string(profile), "<key>\n-----BEGIN PRIVATE KEY-----", "<key>\n-----BEGIN "+marker+"-----", 1))
			if err := os.WriteFile(filepath.Join(output, publishedVPNKey), profile, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "VPN expiry", want: "auth-vpn-expiry-mismatch", prepare: func(t *testing.T, manifest, _ string, _ string) {
			value, err := readJSON[authManifest](manifest)
			if err != nil {
				t.Fatal(err)
			}
			value.Expiry = time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
			if err := writeJSON(manifest, value); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, manifest, output, _, _ := publicationFixture(t, "vpn")
			test.prepare(t, manifest, output, directory)
			_, _, err := publicationBundle(manifest, output)
			if err == nil || publicationFailureReason(err) != test.want || strings.Contains(err.Error(), marker) {
				t.Fatalf("publication failure = %q, want %q without %q", err, test.want, marker)
			}
		})
	}
}

func TestPublicationModeIsBoundToFrozenEndpointPolicy(t *testing.T) {
	for _, test := range []struct {
		name    string
		options servitorv1alpha1.ResolvedOptions
		mode    string
		valid   bool
	}{
		{name: "private VPC VPN", options: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}, mode: "vpn", valid: true},
		{name: "private VPC public", options: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}, mode: "public"},
		{name: "public VPC public", options: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2"}}, mode: "public", valid: true},
		{name: "public VPC VPN", options: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2"}}, mode: "vpn"},
		{name: "Satellite", options: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "satellite"}}, mode: "public"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := publicationModeMatches(test.options, test.mode); got != test.valid {
				t.Fatalf("publicationModeMatches(%+v, %q) = %t, want %t", test.options, test.mode, got, test.valid)
			}
		})
	}
}

func TestPublicationModeMismatchCleansValidatedCertificate(t *testing.T) {
	directory := t.TempDir()
	manifestPath, outputDir := filepath.Join(directory, "manifest.json"), filepath.Join(directory, "auth")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatal(err)
	}
	certificate := authCertificate{ID: "certificate-123", AllocationUID: "uid", AttemptID: "apply-uid"}
	manifest := authManifest{Version: 1, Availability: "available", Mode: "public", Certificate: &certificate}
	manifest.Artifacts = append(manifest.Artifacts, struct {
		Name string `json:"name"`
	}{Name: publishedKubeconfigKey})
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, publishedKubeconfigKey), testKubeconfigBytes(t, testKubeconfig(t)), 0o600); err != nil {
		t.Fatal(err)
	}
	trace, ict := filepath.Join(directory, "trace"), filepath.Join(directory, "ict")
	script := fmt.Sprintf("#!/bin/sh\nset -eu\nprintf '%%s' \"$@\" > %q\nprintf '%%s' '{\"version\":1,\"operation\":\"auth-cleanup\",\"auth_cleanup\":\"cleaned\"}' > \"$6\"\n", trace)
	if err := os.WriteFile(ict, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	contextPath := filepath.Join(directory, "auth-context.json")
	if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	reportPath := publicationReport(t, "uid", "apply-uid")
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}
	if err := publishPublicAuth(true, options, "secret", "uid", "apply-uid", manifestPath, outputDir, reportPath, "ns", filepath.Join(directory, "token"), filepath.Join(directory, "ca"), ict, contextPath); err != nil {
		t.Fatal(err)
	}
	report, err := readJSON[pipeline.Report](reportPath)
	if err != nil || report.Auth == nil || report.Auth.Reason != "auth-manifest-invalid" || report.Auth.Certificate != nil {
		t.Fatalf("mode mismatch report = %#v, %v", report.Auth, err)
	}
	args, err := os.ReadFile(trace)
	if err != nil || !strings.Contains(string(args), "--certificate-idcertificate-123") || !strings.Contains(string(args), "--certificate-allocation-uiduid") || !strings.Contains(string(args), "--certificate-attempt-idapply-uid") {
		t.Fatalf("mode mismatch did not reconcile certificate ownership: %q, %v", args, err)
	}
}

func TestPublicationVersionInvalidManifestCleansParsedCertificate(t *testing.T) {
	directory := t.TempDir()
	manifestPath, outputDir := filepath.Join(directory, "manifest.json"), filepath.Join(directory, "auth")
	if err := os.Mkdir(outputDir, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := authManifest{Version: 2, Certificate: &authCertificate{ID: "certificate-123", AllocationUID: "uid", AttemptID: "apply-uid"}}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	trace, ict := filepath.Join(directory, "trace"), filepath.Join(directory, "ict")
	script := fmt.Sprintf("#!/bin/sh\nset -eu\nprintf '%%s' \"$@\" > %q\nprintf '%%s' '{\"version\":1,\"operation\":\"auth-cleanup\",\"auth_cleanup\":\"cleaned\"}' > \"$6\"\n", trace)
	if err := os.WriteFile(ict, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	contextPath := filepath.Join(directory, "auth-context.json")
	if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	reportPath := publicationReport(t, "uid", "apply-uid")
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}
	if err := publishPublicAuth(true, options, "secret", "uid", "apply-uid", manifestPath, outputDir, reportPath, "ns", filepath.Join(directory, "token"), filepath.Join(directory, "ca"), ict, contextPath); err != nil {
		t.Fatal(err)
	}
	report, err := readJSON[pipeline.Report](reportPath)
	if err != nil || report.Auth == nil || report.Auth.Reason != "auth-manifest-invalid" || report.Auth.Certificate != nil {
		t.Fatalf("version invalid report = %#v, %v", report.Auth, err)
	}
	args, err := os.ReadFile(trace)
	if err != nil || !strings.Contains(string(args), "--certificate-idcertificate-123") || !strings.Contains(string(args), "--certificate-allocation-uiduid") || !strings.Contains(string(args), "--certificate-attempt-idapply-uid") {
		t.Fatalf("version invalid manifest did not reconcile certificate ownership: %q, %v", args, err)
	}
}

func TestAuthRetryPublicationFenceRejectsAdvancedSpecBeforeSecretAccessAndCleansCertificate(t *testing.T) {
	directory, manifestPath, outputDir, _, _ := publicationFixture(t, "vpn")
	operation := "auth-retry-current"
	certificate := authCertificate{ID: "certificate-123", AllocationUID: "uid", AttemptID: operation}
	manifest, err := readJSON[authManifest](manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Certificate = &certificate
	if err := writeJSON(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}

	tokenPath, caPath := filepath.Join(directory, "token"), filepath.Join(directory, "ca.crt")
	if err := os.WriteFile(tokenPath, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, []byte("ca\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")

	trace, ict := filepath.Join(directory, "cleanup.trace"), filepath.Join(directory, "ict")
	script := fmt.Sprintf("#!/bin/sh\nset -eu\n[ \"$1\" = auth-cleanup ]\nprintf '%%s' \"$@\" > %q\nprintf '%%s' '{\"version\":1,\"operation\":\"auth-cleanup\",\"auth_cleanup\":\"cleaned\"}' > \"$6\"\n", trace)
	if err := os.WriteFile(ict, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	contextPath := filepath.Join(directory, "auth-context.json")
	if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: operation}}}
	clientset := k8sfake.NewSimpleClientset(secret)
	clientset.ClearActions()
	previousClient := newPublisherClient
	newPublisherClient = func(*rest.Config) (kubernetes.Interface, error) { return clientset, nil }
	t.Cleanup(func() { newPublisherClient = previousClient })
	fence := authRetryFence{Namespace: "ns", Name: "cluster", UID: "uid", Operation: operation, AttemptID: operation, RequestTimestamp: "1710000000.000001", TokenPath: tokenPath, CAPath: caPath}
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"lifecycle": map[string]any{"authRetryRequestTimestamp": "1710000001.000001"}},
		"status": map[string]any{
			"phase":          "Ready",
			"leaseExpiresAt": time.Now().Add(time.Hour).Format(time.RFC3339),
			"operation":      map[string]any{"id": operation, "kind": "auth-retry"},
			"authRetry":      map[string]any{"requestTimestamp": fence.RequestTimestamp, "attemptID": operation, "outcome": "Pending"},
		},
	}}
	cluster.SetUID("uid")
	previousFence := checkAuthRetryFence
	checkAuthRetryFence = func(_ context.Context, actual authRetryFence) error {
		if authRetryFenceMatches(cluster, actual, time.Now()) {
			return nil
		}
		return errors.New("newer auth retry request")
	}
	t.Cleanup(func() { checkAuthRetryFence = previousFence })

	reportPath := publicationReport(t, "uid", operation)
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}
	if err := publishPublicAuth(true, options, "auth", "uid", operation, manifestPath, outputDir, reportPath, "ns", tokenPath, caPath, ict, contextPath, fence); err != nil {
		t.Fatal(err)
	}

	for _, action := range clientset.Actions() {
		if action.GetVerb() == "get" || action.GetVerb() == "update" || action.GetVerb() == "create" {
			t.Fatalf("fenced publisher accessed delivery Secret: %#v", action)
		}
	}
	cleanupArgs, err := os.ReadFile(trace)
	if err != nil || !strings.Contains(string(cleanupArgs), "--certificate-idcertificate-123") {
		t.Fatalf("fenced publisher did not clean issued certificate: %q, %v", cleanupArgs, err)
	}
	report, err := readJSON[pipeline.Report](reportPath)
	if err != nil || report.Auth == nil || report.Auth.Availability != "unavailable" || report.Auth.Reason != "auth-retry-fenced" || report.Auth.Certificate != nil {
		t.Fatalf("late available result was not rejected: %#v, %v", report.Auth, err)
	}
}

func TestAuthRetryPublicationFenceRechecksBeforeSecretUpdate(t *testing.T) {
	_, _, _, bundle, status := publicationFixture(t, "public")
	operation := "auth-retry-current"
	reportPath := publicationReport(t, "uid", operation)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: operation}}}
	clientset := k8sfake.NewSimpleClientset(secret)
	clientset.ClearActions()

	previousFence := checkAuthRetryFence
	checks := 0
	checkAuthRetryFence = func(context.Context, authRetryFence) error {
		checks++
		if checks == 2 {
			return errors.New("cleanup started")
		}
		return nil
	}
	t.Cleanup(func() { checkAuthRetryFence = previousFence })
	fence := authRetryFence{Namespace: "ns", Name: "cluster", UID: "uid", Operation: operation, AttemptID: operation, RequestTimestamp: "1710000000.000001", TokenPath: "/token", CAPath: "/ca"}
	if err := publishAuthBundle(context.Background(), clientset.CoreV1().Secrets("ns"), "auth", "uid", operation, bundle, status, reportPath, fence); !errors.Is(err, errAuthRetryFenced) {
		t.Fatalf("post-read fence failure = %v, want fenced", err)
	}
	if checks != 2 {
		t.Fatalf("live fence checks = %d, want get and update checks", checks)
	}
	for _, action := range clientset.Actions() {
		if action.GetVerb() == "update" || action.GetVerb() == "create" {
			t.Fatalf("publisher updated delivery Secret after fence changed: %#v", action)
		}
	}
}

func TestPublicationReportFailureRevokesSecretAndRetainsOnlyPendingCertificate(t *testing.T) {
	for _, test := range []struct {
		name, cleanup string
		pending       bool
	}{
		{name: "cleanup succeeds", cleanup: "cleaned"},
		{name: "cleanup pending", cleanup: "pending", pending: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, manifestPath, outputDir, _, _ := publicationFixture(t, "vpn")
			certificate := authCertificate{ID: "certificate-123", AllocationUID: "uid", AttemptID: "apply-uid"}
			manifest, err := readJSON[authManifest](manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			manifest.Certificate = &certificate
			if err := writeJSON(manifestPath, manifest); err != nil {
				t.Fatal(err)
			}
			contextPath := filepath.Join(directory, "auth-context.json")
			if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
			ict := filepath.Join(directory, "ict")
			script := fmt.Sprintf("#!/bin/sh\nset -eu\n[ \"$1\" = auth-cleanup ]\nprintf '%%s' '{\"version\":1,\"operation\":\"auth-cleanup\",\"auth_cleanup\":%q}' > \"$6\"\n", test.cleanup)
			if err := os.WriteFile(ict, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			tokenPath, caPath := filepath.Join(directory, "token"), filepath.Join(directory, "ca.crt")
			if err := os.WriteFile(tokenPath, []byte("token\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(caPath, []byte("ca\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
			t.Setenv("KUBERNETES_SERVICE_PORT", "443")
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: "apply-uid"}}}
			clientset := k8sfake.NewSimpleClientset(secret)
			previousClient := newPublisherClient
			newPublisherClient = func(*rest.Config) (kubernetes.Interface, error) { return clientset, nil }
			t.Cleanup(func() { newPublisherClient = previousClient })
			reportPath := publicationReport(t, "uid", "apply-uid")
			previousWriter := writePublishedReport
			writePublishedReport = func(path string, report pipeline.Report) error {
				if err := writeReport(path, report); err != nil {
					return err
				}
				return errors.New("synthetic report fsync failure after rename")
			}
			t.Cleanup(func() { writePublishedReport = previousWriter })

			if err := publishPublicAuth(true, servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}, "auth", "uid", "apply-uid", manifestPath, outputDir, reportPath, "ns", tokenPath, caPath, ict, contextPath); err != nil {
				t.Fatalf("persist-then-error report was not compensated: %v", err)
			}
			stored, err := clientset.CoreV1().Secrets("ns").Get(context.Background(), "auth", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(stored.Data) != 0 {
				t.Fatalf("report failure left deliverable Secret data: %#v", stored.Data)
			}
			report, err := readJSON[pipeline.Report](reportPath)
			if err != nil || report.Auth == nil || report.Auth.Availability != "unavailable" || report.Auth.Reason != "publisher-unavailable" {
				t.Fatalf("persist-then-error final report = %#v, %v", report.Auth, err)
			}
			encoded := stored.Annotations[publisherPendingCertificateKey]
			if !test.pending {
				if encoded != "" {
					t.Fatalf("successful cleanup retained certificate journal: %q", encoded)
				}
				return
			}
			var pending servitorv1alpha1.AuthCertificateReference
			if err := json.Unmarshal([]byte(encoded), &pending); err != nil || pending.ID != certificate.ID || pending.AllocationUID != certificate.AllocationUID || pending.AttemptID != certificate.AttemptID {
				t.Fatalf("pending certificate recovery journal = %q, %#v, %v", encoded, pending, err)
			}
		})
	}
}

func TestCompensatePublishedReportOnlyReplacesCurrentAvailableAttempt(t *testing.T) {
	attempt := "apply-uid"
	unavailable := servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "publisher-unavailable", CleanupOutcome: "not-required", AttemptID: attempt}
	for _, test := range []struct {
		name    string
		initial *servitorv1alpha1.AuthStatus
		wantErr bool
	}{
		{name: "unauthenticated report"},
		{name: "matching available attempt", initial: &servitorv1alpha1.AuthStatus{Availability: "available", Mode: "public", AttemptID: attempt}},
		{name: "other available attempt", initial: &servitorv1alpha1.AuthStatus{Availability: "available", Mode: "public", AttemptID: "other-attempt"}, wantErr: true},
		{name: "unavailable terminal report", initial: &servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: "publisher-unavailable", CleanupOutcome: "not-required", AttemptID: attempt}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := publicationReport(t, "uid", attempt)
			if test.initial != nil {
				if err := markPublished(path, "uid", attempt, *test.initial); err != nil {
					t.Fatal(err)
				}
			}
			err := compensatePublishedReport(path, "uid", attempt, attempt, unavailable)
			if (err != nil) != test.wantErr {
				t.Fatalf("compensation error = %v, want error=%t", err, test.wantErr)
			}
			report, readErr := readJSON[pipeline.Report](path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if test.wantErr && !reflect.DeepEqual(report.Auth, test.initial) {
				t.Fatalf("rejected compensation changed report auth: %#v", report.Auth)
			}
			if !test.wantErr && !reflect.DeepEqual(report.Auth, &unavailable) {
				t.Fatalf("compensation report auth = %#v", report.Auth)
			}
		})
	}

	path := publicationReport(t, "other-uid", attempt)
	if err := compensatePublishedReport(path, "uid", attempt, attempt, unavailable); err == nil {
		t.Fatal("compensated a report from another allocation")
	}
}

func TestAmbiguousSecretUpdateRevokesBundleAndPreservesPendingCertificateJournal(t *testing.T) {
	directory, manifestPath, outputDir, _, _ := publicationFixture(t, "vpn")
	certificate := authCertificate{ID: "certificate-123", AllocationUID: "uid", AttemptID: "apply-uid"}
	manifest, err := readJSON[authManifest](manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Certificate = &certificate
	if err := writeJSON(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}
	contextPath := filepath.Join(directory, "auth-context.json")
	if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\nset -eu\nprintf '%s' '{\"version\":1,\"operation\":\"auth-cleanup\",\"auth_cleanup\":\"pending\",\"cleanup_reason\":\"transport\",\"cleanup_stage\":\"revoke\"}' > \"$6\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	tokenPath, caPath := filepath.Join(directory, "token"), filepath.Join(directory, "ca.crt")
	if err := os.WriteFile(tokenPath, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, []byte("ca\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: "apply-uid"}}}
	clientset := k8sfake.NewSimpleClientset(secret)
	updates := 0
	clientset.PrependReactor("update", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates != 1 {
			return false, nil, nil
		}
		if err := clientset.Tracker().Update(corev1.SchemeGroupVersion.WithResource("secrets"), action.(k8stesting.UpdateAction).GetObject(), "ns"); err != nil {
			t.Fatal(err)
		}
		return true, nil, errors.New("synthetic timeout after Secret update")
	})
	previousClient := newPublisherClient
	newPublisherClient = func(*rest.Config) (kubernetes.Interface, error) { return clientset, nil }
	t.Cleanup(func() { newPublisherClient = previousClient })

	reportPath := publicationReport(t, "uid", "apply-uid")
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}
	if err := publishPublicAuth(true, options, "auth", "uid", "apply-uid", manifestPath, outputDir, reportPath, "ns", tokenPath, caPath, ict, contextPath); err != nil {
		t.Fatalf("ambiguous update was not compensated: %v", err)
	}
	stored, err := clientset.CoreV1().Secrets("ns").Get(context.Background(), "auth", metav1.GetOptions{})
	if err != nil || len(stored.Data) != 0 {
		t.Fatalf("ambiguous update left deliverable Secret data: secret=%#v err=%v", stored, err)
	}
	var pending servitorv1alpha1.AuthCertificateReference
	encoded := stored.Annotations[publisherPendingCertificateKey]
	if err := json.Unmarshal([]byte(encoded), &pending); err != nil || pending.ID != certificate.ID || pending.AllocationUID != certificate.AllocationUID || pending.AttemptID != certificate.AttemptID {
		t.Fatalf("pending certificate recovery journal = %q, %#v, %v", encoded, pending, err)
	}
	report, err := readJSON[pipeline.Report](reportPath)
	if err != nil || report.Auth == nil || report.Auth.Availability != "unavailable" || report.Auth.Reason != "publisher-unavailable" || report.Auth.CleanupOutcome != "pending" || !reflect.DeepEqual(report.Auth.Certificate, &pending) {
		t.Fatalf("ambiguous update report = %#v, %v", report.Auth, err)
	}
}

func TestAmbiguousUncommittedSecretUpdatePublishesUnavailableReportWhenRevocationFails(t *testing.T) {
	directory, manifestPath, outputDir, _, _ := publicationFixture(t, "vpn")
	certificate := authCertificate{ID: "certificate-123", AllocationUID: "uid", AttemptID: "apply-uid"}
	manifest, err := readJSON[authManifest](manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Certificate = &certificate
	if err := writeJSON(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}
	contextPath := filepath.Join(directory, "auth-context.json")
	if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\nset -eu\nprintf '%s' '{\"version\":1,\"operation\":\"auth-cleanup\",\"auth_cleanup\":\"pending\",\"cleanup_reason\":\"transport\",\"cleanup_stage\":\"revoke\"}' > \"$6\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	tokenPath, caPath := filepath.Join(directory, "token"), filepath.Join(directory, "ca.crt")
	if err := os.WriteFile(tokenPath, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, []byte("ca\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: "apply-uid"}}}
	clientset := k8sfake.NewSimpleClientset(secret)
	updates := 0
	clientset.PrependReactor("update", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		return true, nil, errors.New("synthetic Secret update failure")
	})
	previousClient := newPublisherClient
	newPublisherClient = func(*rest.Config) (kubernetes.Interface, error) { return clientset, nil }
	t.Cleanup(func() { newPublisherClient = previousClient })

	reportPath := publicationReport(t, "uid", "apply-uid")
	options := servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}
	if err := publishPublicAuth(true, options, "auth", "uid", "apply-uid", manifestPath, outputDir, reportPath, "ns", tokenPath, caPath, ict, contextPath); err != nil {
		t.Fatalf("uncommitted update compensation did not publish unavailable report: %v", err)
	}
	if updates != 3 {
		t.Fatalf("Secret updates = %d, want initial publication and two failed revocations", updates)
	}
	report, err := readJSON[pipeline.Report](reportPath)
	if err != nil || report.Validate("uid", "apply-uid") != nil || report.Auth == nil || report.Auth.Availability != "unavailable" || report.Auth.CleanupOutcome != "pending" || report.Auth.Certificate == nil || report.Auth.Certificate.ID != certificate.ID || report.Auth.Certificate.AllocationUID != certificate.AllocationUID || report.Auth.Certificate.AttemptID != certificate.AttemptID {
		t.Fatalf("compensation report = %#v, %v", report.Auth, err)
	}
}

func TestCompensationReportFailureJoinsRedactedRevocationErrors(t *testing.T) {
	const marker = "private-value-must-not-escape"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: "attempt"}}}
	clientset := k8sfake.NewSimpleClientset(secret)
	before, after := errors.New(marker+"-before"), errors.New(marker+"-after")
	updates := 0
	clientset.PrependReactor("update", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, before
		}
		return true, nil, after
	})

	err := compensatePublishedAuthBundle(context.Background(), clientset.CoreV1().Secrets("ns"), "auth", "uid", "attempt", filepath.Join(t.TempDir(), "missing-report.json"), "operation", "publisher-unavailable", "", "", nil)
	if err == nil {
		t.Fatal("compensation succeeded without a report")
	}
	if !errors.Is(err, before) || !errors.Is(err, after) {
		t.Fatalf("compensation error did not join both revocation failures: %v", err)
	}
	if strings.Contains(err.Error(), marker) || !strings.Contains(err.Error(), "before certificate cleanup") || !strings.Contains(err.Error(), "after certificate cleanup") {
		t.Fatalf("compensation error leaked a Secret value or omitted revocation detail: %v", err)
	}
}

func TestAuthRetryFenceAfterSecretUpdateRevokesBundle(t *testing.T) {
	directory, manifestPath, outputDir, _, _ := publicationFixture(t, "vpn")
	certificate := authCertificate{ID: "certificate-123", AllocationUID: "uid", AttemptID: "auth-retry-current"}
	manifest, err := readJSON[authManifest](manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Certificate = &certificate
	if err := writeJSON(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}
	contextPath := filepath.Join(directory, "auth-context.json")
	if err := os.WriteFile(contextPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	ict := filepath.Join(directory, "ict")
	if err := os.WriteFile(ict, []byte("#!/bin/sh\nset -eu\nprintf '%s' '{\"version\":1,\"operation\":\"auth-cleanup\",\"auth_cleanup\":\"cleaned\"}' > \"$6\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	tokenPath, caPath := filepath.Join(directory, "token"), filepath.Join(directory, "ca.crt")
	if err := os.WriteFile(tokenPath, []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, []byte("ca\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	operation := "auth-retry-current"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "auth", Namespace: "ns", Labels: map[string]string{publisherUIDLabel: "uid"}, Annotations: map[string]string{publisherOperationKey: operation}}}
	clientset := k8sfake.NewSimpleClientset(secret)
	previousClient := newPublisherClient
	newPublisherClient = func(*rest.Config) (kubernetes.Interface, error) { return clientset, nil }
	t.Cleanup(func() { newPublisherClient = previousClient })
	previousFence := checkAuthRetryFence
	checks := 0
	checkAuthRetryFence = func(context.Context, authRetryFence) error {
		checks++
		if checks == 3 {
			return errors.New("cleanup started")
		}
		return nil
	}
	t.Cleanup(func() { checkAuthRetryFence = previousFence })
	fence := authRetryFence{Namespace: "ns", Name: "cluster", UID: "uid", Operation: operation, AttemptID: operation, RequestTimestamp: "1710000000.000001", TokenPath: tokenPath, CAPath: caPath}
	reportPath := publicationReport(t, "uid", operation)
	if err := publishPublicAuth(true, servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", PrivateOnly: true}}, "auth", "uid", operation, manifestPath, outputDir, reportPath, "ns", tokenPath, caPath, ict, contextPath, fence); err != nil {
		t.Fatal(err)
	}
	stored, err := clientset.CoreV1().Secrets("ns").Get(context.Background(), "auth", metav1.GetOptions{})
	if err != nil || len(stored.Data) != 0 || stored.Annotations[publisherPendingCertificateKey] != "" {
		t.Fatalf("late fence left delivery state: secret=%#v err=%v", stored, err)
	}
	report, err := readJSON[pipeline.Report](reportPath)
	if err != nil || report.Auth == nil || report.Auth.Reason != "auth-retry-fenced" || checks != 3 {
		t.Fatalf("post-update fence report = %#v, checks=%d, err=%v", report.Auth, checks, err)
	}
}

func TestPublicationBundlePassesOnlyKnownVPNCertificateSubreasons(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")
	for _, test := range []struct {
		name, reason string
		valid        bool
	}{
		{"known", "vpn-certificate-chain-verify", true},
		{"missing", "", false},
		{"unknown payload", "vpn-certificate-chain-verify:private-value", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(authManifest{Version: 1, Availability: "unavailable", Reason: test.reason})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
				t.Fatal(err)
			}
			bundle, status, err := publicationBundle(manifestPath, t.TempDir())
			if !test.valid {
				if err == nil || strings.Contains(err.Error(), "private-value") {
					t.Fatalf("unsafe publication result = %#v, bundle = %#v, err = %v", status, bundle, err)
				}
				return
			}
			if err != nil || bundle != nil || !reflect.DeepEqual(status, servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: test.reason}) {
				t.Fatalf("publication status = %#v, bundle = %#v, err = %v", status, bundle, err)
			}
			reportPath := publicationReport(t, "uid", "apply-uid")
			if err := markPublished(reportPath, "uid", "apply-uid", status); err != nil {
				t.Fatal(err)
			}
			report, err := readJSON[pipeline.Report](reportPath)
			if err != nil || report.Auth == nil || report.Auth.Reason != test.reason || strings.Contains(string(mustRead(t, reportPath)), "private-value") {
				t.Fatalf("published report = %#v, %v", report.Auth, err)
			}
		})
	}
}

func TestReplaceReportFileRetainsPreviousValidReportOnRenameFailure(t *testing.T) {
	path := publicationReport(t, "uid", "apply-uid")
	before := mustRead(t, path)
	if err := replaceReportFileWithRename(path, []byte(`{"corrupt":true}\n`), func(string, string) error {
		return errors.New("synthetic rename failure")
	}); err == nil {
		t.Fatal("accepted a failed report replacement")
	}
	after := mustRead(t, path)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("write failure replaced report: got %q, want %q", after, before)
	}
	report, err := pipeline.DecodeReport(after, "uid", "apply-uid")
	if err != nil || report.Auth != nil {
		t.Fatalf("write failure left corrupt report: %#v, %v", report.Auth, err)
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
	profile := "client\nremote vpn.example.invalid 443 udp\n<ca>\n" + string(ca) + "</ca>\n<cert>\n" + string(testVPNCertificatePEM(t, config)) + "</cert>\n<key>\n" + string(key) + "</key>\n"
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

func TestPublishedVPNClientPresentationChainRejectsUntrustedOrderAndProfileTrust(t *testing.T) {
	config := testKubeconfig(t)
	leaf := string(config.AuthInfos["admin"].ClientCertificateData)
	key := string(config.AuthInfos["admin"].ClientKeyData)
	trust := string(config.Clusters["example"].CertificateAuthorityData)
	root := publishedCertificatePEM(t, trust, 0)
	intermediate := publishedCertificatePEM(t, trust, 1)
	presentation := string(testVPNCertificatePEM(t, config))
	block, _ := pem.Decode([]byte(leaf))
	parsed, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	profile := func(ca, presentation string) []byte {
		return []byte("client\nremote vpn.example.invalid 443 udp\n<ca>\n" + ca + "</ca>\n<cert>\n" + presentation + "</cert>\n<key>\n" + key + "</key>\n")
	}
	if err := validatePublishedVPN(profile(trust, presentation), parsed.NotAfter.UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("rejected intermediate-terminated client chain: %v", err)
	}
	for _, test := range []struct {
		name, ca, presentation string
	}{
		{"out-of-order client chain", trust, leaf + root + intermediate},
		{"unrelated client chain", trust, leaf + publishedCertificatePEM(t, string(testKubeconfig(t).Clusters["example"].CertificateAuthorityData), 0)},
		{"broken client chain", trust, leaf + intermediate + publishedCertificatePEM(t, string(testKubeconfig(t).Clusters["example"].CertificateAuthorityData), 0)},
		{"intermediate-only profile trust", intermediate, presentation},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validatePublishedVPN(profile(test.ca, test.presentation), parsed.NotAfter.UTC().Format(time.RFC3339)); err == nil {
				t.Fatal("accepted invalid VPN trust separation")
			}
		})
	}
}

func TestPublishedVPNRejectsCertificateWithServerAuth(t *testing.T) {
	config := testKubeconfig(t, x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth)
	certificate := config.AuthInfos["admin"].ClientCertificateData
	key := config.AuthInfos["admin"].ClientKeyData
	ca := config.Clusters["example"].CertificateAuthorityData
	block, _ := pem.Decode(certificate)
	parsed, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	profile := "client\nremote vpn.example.invalid 443 udp\n<ca>\n" + string(ca) + "</ca>\n<cert>\n" + string(testVPNCertificatePEM(t, config)) + "</cert>\n<key>\n" + string(key) + "</key>\n"
	if err := validatePublishedVPN([]byte(profile), parsed.NotAfter.UTC().Format(time.RFC3339)); err == nil {
		t.Fatal("accepted a VPN certificate with server authentication usage")
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
			if err := publishAuthBundle(context.Background(), clientset.CoreV1().Secrets("ns"), "auth", "uid", "apply-uid", bundle, status, reportPath); err != nil {
				t.Fatalf("publish rejected valid fixture: %v", err)
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
			if err := publishAuthBundle(ctx, clientset.CoreV1().Secrets("ns"), "auth", "uid", "apply-uid", bundle, status, reportPath); err == nil {
				t.Fatal("published stale, missing, or cancelled Secret")
			}
			report, err := readJSON[pipeline.Report](reportPath)
			if err != nil || report.Auth != nil || report.Validate("uid", "apply-uid") != nil {
				t.Fatalf("failed publication changed valid infrastructure report: %#v, %v", report.Auth, err)
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
	if err := publishAuthBundle(context.Background(), clientset.CoreV1().Secrets("ns"), "auth", "uid", "apply-uid", bundle, status, reportPath); err == nil {
		t.Fatal("publisher reported success after cleanup deleted its Secret")
	}
	if _, err := clientset.CoreV1().Secrets("ns").Get(context.Background(), "auth", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("late publisher recreated cleanup Secret: %v", err)
	}
	report, err := readJSON[pipeline.Report](reportPath)
	if err != nil || report.Auth != nil || report.Validate("uid", "apply-uid") != nil {
		t.Fatalf("cleanup race changed valid infrastructure report: %#v, %v", report.Auth, err)
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
		bundle[publishedVPNKey] = []byte("client\nremote vpn.example.invalid 443 udp\n<ca>\n" + string(ca) + "</ca>\n<cert>\n" + string(testVPNCertificatePEM(t, config)) + "</cert>\n<key>\n" + string(key) + "</key>\n")
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
	report := pipeline.Report{Version: 1, ClusterUID: uid, OperationID: operation, ResolvedOptions: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "1.31"}, ClusterName: "fixture"}, Recovery: recovery}
	path := filepath.Join(t.TempDir(), "report.json")
	if err := writeReport(path, report); err != nil {
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

func TestPublishedKubeconfigCertificateValidation(t *testing.T) {
	tests := []struct {
		name    string
		usages  []x509.ExtKeyUsage
		mutate  func(*clientcmdapi.Config)
		accepts bool
	}{
		{name: "allows client and server authentication", usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, accepts: true},
		{name: "rejects missing client authentication", usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{name: "rejects client certificate chain", mutate: func(config *clientcmdapi.Config) {
			config.AuthInfos["admin"].ClientCertificateData = append(config.AuthInfos["admin"].ClientCertificateData, []byte(publishedCertificatePEM(t, string(config.Clusters["example"].CertificateAuthorityData), 0))...)
		}},
		{name: "rejects mismatched private key", mutate: func(config *clientcmdapi.Config) {
			config.AuthInfos["admin"].ClientKeyData = testKubeconfig(t).AuthInfos["admin"].ClientKeyData
		}},
		{name: "rejects untrusted certificate chain", mutate: func(config *clientcmdapi.Config) {
			config.Clusters["example"].CertificateAuthorityData = testKubeconfig(t).Clusters["example"].CertificateAuthorityData
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testKubeconfig(t, test.usages...)
			if test.mutate != nil {
				test.mutate(config)
			}
			if accepted := validatePublishedKubeconfig(testKubeconfigBytes(t, config)) == nil; accepted != test.accepts {
				t.Fatalf("kubeconfig accepted = %t, want %t", accepted, test.accepts)
			}
		})
	}
}

func testKubeconfig(t *testing.T, usages ...x509.ExtKeyUsage) *clientcmdapi.Config {
	t.Helper()
	if len(usages) == 0 {
		usages = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
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
	intermediateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	intermediateTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "test-intermediate"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	intermediateDER, err := x509.CreateCertificate(rand.Reader, intermediateTemplate, caTemplate, &intermediateKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, intermediateTemplate, &clientKey.PublicKey, intermediateKey)
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
		CertificateAuthorityData: append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: intermediateDER})...),
	}
	config.AuthInfos["admin"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		ClientKeyData:         pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKeyDER}),
	}
	config.Contexts["admin@example"] = &clientcmdapi.Context{Cluster: "example", AuthInfo: "admin"}
	config.CurrentContext = "admin@example"
	return config
}

func testVPNCertificatePEM(t *testing.T, config *clientcmdapi.Config) []byte {
	t.Helper()
	leaf := config.AuthInfos["admin"].ClientCertificateData
	intermediate := publishedCertificatePEM(t, string(config.Clusters["example"].CertificateAuthorityData), 1)
	return append(append([]byte(nil), leaf...), intermediate...)
}

func publishedCertificatePEM(t *testing.T, value string, index int) string {
	t.Helper()
	for rest := []byte(value); ; {
		block, remaining := pem.Decode(rest)
		if block == nil {
			t.Fatal("invalid certificate fixture")
		}
		if index == 0 {
			return string(pem.EncodeToMemory(block))
		}
		index--
		rest = remaining
	}
}

func testKubeconfigBytes(t *testing.T, config *clientcmdapi.Config) []byte {
	t.Helper()
	contents, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

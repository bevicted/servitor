package main

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	publisherTimeout               = time.Minute
	publisherUIDLabel              = "servitor.bevicted.github.io/auth-uid"
	publisherOperationKey          = "servitor.bevicted.github.io/auth-operation"
	publisherPendingCertificateKey = "servitor.bevicted.github.io/auth-pending-certificate"
	publishedKubeconfigKey         = "kubeconfig.yaml"
	publishedVPNKey                = "client.ovpn"
	maxPublishedBundleBytes        = 2 << 20
	maxPublishedFileBytes          = 1 << 20
)

type publicationValidationError uint8

type publicationCommitError struct {
	cause error
}

func (err *publicationCommitError) Error() string {
	return err.cause.Error()
}

func (err *publicationCommitError) Unwrap() error {
	return err.cause
}

type publicationRevocationError struct {
	phase string
	cause error
}

func (err *publicationRevocationError) Error() string {
	return "auth Secret revocation " + err.phase + " failed"
}

func (err *publicationRevocationError) Unwrap() error {
	return err.cause
}

const (
	publicationManifestInvalid publicationValidationError = iota + 1
	publicationArtifactLayoutInvalid
	publicationKubeconfigInvalid
	publicationVPNProfileInvalid
	publicationVPNProfileTrustInvalid
	publicationVPNCertificateInvalid
	publicationVPNExpiryMismatch
)

func (publicationValidationError) Error() string {
	return "publication validation failed"
}

func (err publicationValidationError) reason() string {
	switch err {
	case publicationManifestInvalid:
		return "auth-manifest-invalid"
	case publicationArtifactLayoutInvalid:
		return "auth-artifact-layout-invalid"
	case publicationKubeconfigInvalid:
		return "auth-kubeconfig-invalid"
	case publicationVPNProfileInvalid:
		return "auth-vpn-profile-invalid"
	case publicationVPNProfileTrustInvalid:
		return "auth-vpn-profile-trust-invalid"
	case publicationVPNCertificateInvalid:
		return "auth-vpn-certificate-invalid"
	case publicationVPNExpiryMismatch:
		return "auth-vpn-expiry-mismatch"
	default:
		return "auth-manifest-invalid"
	}
}

func publicationFailureReason(err error) string {
	var validation publicationValidationError
	if errors.As(err, &validation) {
		return validation.reason()
	}
	return publicationManifestInvalid.reason()
}

// publishPublicAuth writes an auth result only after it has a valid terminal
// outcome. Unknown publication failures fail the TaskRun instead of emitting a
// malformed report that could be adopted as Ready.
func publishPublicAuth(authEligible bool, options servitorv1alpha1.ResolvedOptions, secretName, uid, operation, manifestPath, outputDir, reportPath, namespace, tokenPath, caPath, ictPath, cleanupContextPath string, fences ...authRetryFence) error {
	return publishPublicAuthWithAttempt(authEligible, options, secretName, uid, operation, operation, manifestPath, outputDir, reportPath, namespace, tokenPath, caPath, ictPath, cleanupContextPath, fences...)
}

func publishPublicAuthWithAttempt(authEligible bool, options servitorv1alpha1.ResolvedOptions, secretName, uid, operation, authAttemptID, manifestPath, outputDir, reportPath, namespace, tokenPath, caPath, ictPath, cleanupContextPath string, fences ...authRetryFence) error {
	if !authEligible {
		return nil
	}
	if !validPublicationInputs(secretName, uid, operation, authAttemptID, manifestPath, outputDir, reportPath, namespace, tokenPath, caPath) {
		return errors.New("auth publication inputs are invalid")
	}
	bundle, status, err := publicationBundle(manifestPath, outputDir)
	status.AttemptID = authAttemptID
	certificate := publicationCertificate(status, options, uid, authAttemptID)
	// A valid unavailable manifest may carry an unresolved certificate from a
	// prior attempt. Validation failures must still clean only this attempt.
	if err == nil && certificate == nil {
		certificate = historicalPendingPublicationCertificate(status, options, uid)
	}
	status.Certificate = certificate
	fence, fenceRequired, fenceErr := publicationFence(fences)
	if fenceErr != nil || fenceRequired && (fence.UID != uid || fence.Operation != operation || fence.AttemptID != authAttemptID) {
		return unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, authAttemptID, "auth-retry-fenced", ictPath, cleanupContextPath, certificate)
	}
	if err != nil {
		return unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, authAttemptID, publicationFailureReason(err), ictPath, cleanupContextPath, certificate)
	}
	if status.Availability == "available" && !publicationModeMatches(options, status.Mode) {
		return unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, authAttemptID, publicationManifestInvalid.reason(), ictPath, cleanupContextPath, certificate)
	}
	if status.Availability == "unavailable" {
		if certificate != nil {
			return unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, authAttemptID, status.Reason, ictPath, cleanupContextPath, certificate)
		}
		return markPublished(reportPath, uid, operation, status)
	}
	config, err := publisherConfig(tokenPath, caPath)
	if err != nil {
		return unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, authAttemptID, "publisher-unavailable", ictPath, cleanupContextPath, certificate)
	}
	ctx, cancel := context.WithTimeout(context.Background(), publisherTimeout)
	defer cancel()
	clientset, err := newPublisherClient(config)
	if err != nil {
		return unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, authAttemptID, "publisher-unavailable", ictPath, cleanupContextPath, certificate)
	}
	var publishFences []authRetryFence
	if fenceRequired {
		publishFences = append(publishFences, fence)
	}
	secrets := clientset.CoreV1().Secrets(namespace)
	if err := publishAuthBundle(ctx, secrets, secretName, uid, operation, bundle, status, reportPath, publishFences...); err != nil {
		reason := "publisher-unavailable"
		if errors.Is(err, errAuthRetryFenced) {
			reason = "auth-retry-fenced"
		}
		var committed *publicationCommitError
		if errors.As(err, &committed) {
			return compensatePublishedAuthBundle(ctx, secrets, secretName, uid, authAttemptID, reportPath, operation, reason, ictPath, cleanupContextPath, certificate)
		}
		return unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, authAttemptID, reason, ictPath, cleanupContextPath, certificate)
	}
	return nil
}

// unavailableAfterPublicationFailure never leaves an issued private certificate
// behind after a failed Secret publication. ICT owns the provider interaction;
// its bounded result is the only cleanup success signal.
func unavailableAfterPublicationFailure(reportPath, uid, operation, reason, ictPath, contextPath string, certificate *servitorv1alpha1.AuthCertificateReference) error {
	return unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, operation, reason, ictPath, contextPath, certificate)
}

func unavailableAfterPublicationFailureWithAttempt(reportPath, uid, operation, authAttemptID, reason, ictPath, contextPath string, certificate *servitorv1alpha1.AuthCertificateReference) error {
	return markPublished(reportPath, uid, operation, unavailablePublicationStatus(operation, authAttemptID, reason, ictPath, contextPath, certificate))
}

func unavailablePublicationStatus(operation, authAttemptID, reason, ictPath, contextPath string, certificate *servitorv1alpha1.AuthCertificateReference) servitorv1alpha1.AuthStatus {
	status := servitorv1alpha1.AuthStatus{Availability: "unavailable", Reason: reason, CleanupOutcome: "not-required", AttemptID: authAttemptID}
	if certificate == nil {
		return status
	}
	cleaned, cleanupReason, cleanupStage, pending := cleanupPublishedCertificate(ictPath, contextPath, operation, *certificate)
	if cleaned {
		status.CleanupOutcome = "cleaned"
		return status
	}
	status.CleanupOutcome = "pending"
	status.CleanupReason = cleanupReason
	status.CleanupStage = cleanupStage
	status.Certificate = pending
	return status
}

// compensatePublishedAuthBundle revokes delivered bytes before cleanup, while
// retaining only the bounded certificate ownership reference for recovery.
func compensatePublishedAuthBundle(ctx context.Context, secrets corev1client.SecretInterface, secretName, uid, authAttemptID, reportPath, operation, reason, ictPath, contextPath string, certificate *servitorv1alpha1.AuthCertificateReference) error {
	beforeCleanup := revokePublishedAuthBundle(ctx, secrets, secretName, uid, authAttemptID, certificate)
	status := unavailablePublicationStatus(operation, authAttemptID, reason, ictPath, contextPath, certificate)
	afterCleanup := revokePublishedAuthBundle(ctx, secrets, secretName, uid, authAttemptID, status.Certificate)
	if reportErr := compensatePublishedReport(reportPath, uid, operation, authAttemptID, status); reportErr != nil {
		return errors.Join(
			reportErr,
			publicationRevocationFailure("before certificate cleanup", beforeCleanup),
			publicationRevocationFailure("after certificate cleanup", afterCleanup),
		)
	}
	return nil
}

func publicationRevocationFailure(phase string, cause error) error {
	if cause == nil {
		return nil
	}
	return &publicationRevocationError{phase: phase, cause: cause}
}

// revokePublishedAuthBundle never mutates a replacement attempt. Its annotation
// contains only the bounded non-secret certificate ownership reference.
func revokePublishedAuthBundle(ctx context.Context, secrets corev1client.SecretInterface, secretName, uid, authAttemptID string, certificate *servitorv1alpha1.AuthCertificateReference) error {
	secret, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read auth Secret for revocation: %w", err)
	}
	if secret.Labels[publisherUIDLabel] != uid || secret.Annotations[publisherOperationKey] != authAttemptID {
		return errors.New("auth Secret is not bound to the active operation")
	}
	secret.Data = nil
	if secret.Annotations == nil {
		secret.Annotations = make(map[string]string)
	}
	if certificate == nil {
		delete(secret.Annotations, publisherPendingCertificateKey)
	} else {
		encoded, err := json.Marshal(certificate)
		if err != nil {
			return fmt.Errorf("encode pending certificate reference: %w", err)
		}
		secret.Annotations[publisherPendingCertificateKey] = string(encoded)
	}
	if _, err := secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("revoke published auth Secret: %w", err)
	}
	return nil
}

// publicationCertificate validates current-attempt ownership separately from
// artifact mode so a mode mismatch or malformed bundle cannot strand a private
// certificate.
func publicationCertificate(status servitorv1alpha1.AuthStatus, options servitorv1alpha1.ResolvedOptions, uid, authAttemptID string) *servitorv1alpha1.AuthCertificateReference {
	certificate := status.Certificate
	if !validPublicationCertificateReference(certificate, options, uid, status) || certificate.AttemptID != authAttemptID {
		return nil
	}
	return certificate
}

// historicalPendingPublicationCertificate is limited to a fully valid pending
// cleanup manifest. A preclean retry may still be reconciling a certificate
// issued by a prior attempt for this allocation.
func historicalPendingPublicationCertificate(status servitorv1alpha1.AuthStatus, options servitorv1alpha1.ResolvedOptions, uid string) *servitorv1alpha1.AuthCertificateReference {
	certificate := status.Certificate
	if status.Availability != "unavailable" || status.CleanupOutcome != "pending" || !validPublicationCertificateReference(certificate, options, uid, status) {
		return nil
	}
	return certificate
}

func validPublicationCertificateReference(certificate *servitorv1alpha1.AuthCertificateReference, options servitorv1alpha1.ResolvedOptions, uid string, status servitorv1alpha1.AuthStatus) bool {
	return options.Provider == "vpc-gen2" && options.PrivateOnly && certificate != nil && certificate.AllocationUID == uid && len(certificate.AllocationUID) <= 128 && certificate.AttemptID != "" && len(certificate.AttemptID) <= 128 && len(certificate.ID) <= 256 && (certificate.ID != "" || status.Reason == "certificate-cleanup-pending" || status.CleanupOutcome == "pending")
}

func cleanupPublishedCertificate(ictPath, contextPath, operation string, certificate servitorv1alpha1.AuthCertificateReference) (bool, string, string, *servitorv1alpha1.AuthCertificateReference) {
	if ictPath == "" || !filepath.IsAbs(contextPath) {
		return false, "transport", "", &certificate
	}
	resultPath := filepath.Join(filepath.Dir(contextPath), "auth-publication-cleanup.json")
	args := []string{"auth-cleanup", operation, "--context-file", contextPath, "--result-file", resultPath}
	if certificate.ID != "" {
		args = append(args, "--certificate-id", certificate.ID)
	}
	args = append(args, "--certificate-allocation-uid", certificate.AllocationUID, "--certificate-attempt-id", certificate.AttemptID)
	ctx, cancel := context.WithTimeout(context.Background(), publisherTimeout)
	defer cancel()
	_, err := runICT(ctx, ictPath, 64*1024, io.Discard, io.Discard, nil, args...)
	if err != nil {
		return false, "transport", "", &certificate
	}
	result, err := readJSON[ictOperationResult](resultPath)
	if err == nil && result.Version == 1 && result.Operation == "auth-cleanup" && authCleanupSucceeded(result) {
		return true, "", "", nil
	}
	if err == nil && result.Certificate != nil && result.Certificate.AllocationUID == certificate.AllocationUID && result.Certificate.AttemptID == certificate.AttemptID && len(result.Certificate.ID) <= 256 {
		return false, cleanupFailureReason(result, nil, nil), cleanupFailureStage(result), &servitorv1alpha1.AuthCertificateReference{ID: result.Certificate.ID, AllocationUID: result.Certificate.AllocationUID, AttemptID: result.Certificate.AttemptID}
	}
	return false, cleanupFailureReason(result, nil, err), cleanupFailureStage(result), &certificate
}

var (
	errAuthRetryFenced = errors.New("auth retry is fenced")
	newPublisherClient = func(config *rest.Config) (kubernetes.Interface, error) {
		return kubernetes.NewForConfig(config)
	}
	writePublishedReport = writeReport
)

func publicationFence(fences []authRetryFence) (authRetryFence, bool, error) {
	if len(fences) == 0 {
		return authRetryFence{}, false, nil
	}
	if len(fences) != 1 || !fences[0].valid() {
		return authRetryFence{}, true, errors.New("auth retry fence inputs are invalid")
	}
	return fences[0], true, nil
}

// publishAuthBundle is the single checked update path. The publisher can only
// update a pre-created, UID and auth-attempt-bound Secret; it never creates one.
// A retry checks the live allocation fence immediately before each delivery
// Secret access so cleanup and newer requests win over publication.
func publishAuthBundle(ctx context.Context, secrets corev1client.SecretInterface, secretName, uid, operation string, bundle map[string][]byte, status servitorv1alpha1.AuthStatus, reportPath string, fences ...authRetryFence) error {
	fence, fenceRequired, err := publicationFence(fences)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fenceRequired {
		if err := checkAuthRetryFence(ctx, fence); err != nil {
			return fmt.Errorf("%w: %v", errAuthRetryFenced, err)
		}
	}
	secret, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read auth Secret: %w", err)
	}
	attemptID := status.AttemptID
	if attemptID == "" { // pre-auth-attempt manifests used the operation identity.
		attemptID = operation
	}
	if secret.Labels[publisherUIDLabel] != uid || secret.Annotations[publisherOperationKey] != attemptID {
		return errors.New("auth Secret is not bound to the active operation")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fenceRequired {
		if err := checkAuthRetryFence(ctx, fence); err != nil {
			return fmt.Errorf("%w: %v", errAuthRetryFenced, err)
		}
	}
	secret.Data = bundle
	if certificate := status.Certificate; certificate != nil {
		encoded, err := json.Marshal(certificate)
		if err != nil {
			return err
		}
		if secret.Annotations == nil {
			secret.Annotations = make(map[string]string)
		}
		secret.Annotations[publisherPendingCertificateKey] = string(encoded)
	}
	if _, err := secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		// Kubernetes may persist an update before a timeout or transport error
		// reaches the publisher. Compensate this ambiguous outcome as a commit.
		return &publicationCommitError{cause: fmt.Errorf("publish auth Secret: %w", err)}
	}
	if fenceRequired {
		if err := checkAuthRetryFence(ctx, fence); err != nil {
			return &publicationCommitError{cause: fmt.Errorf("%w: %v", errAuthRetryFenced, err)}
		}
	}
	if err := markPublished(reportPath, uid, operation, status); err != nil {
		return &publicationCommitError{cause: err}
	}
	return nil
}

// publicationModeMatches rejects a worker-provided mode that is inconsistent
// with the allocation endpoint policy frozen in the operation parameter.
func publicationModeMatches(options servitorv1alpha1.ResolvedOptions, mode string) bool {
	if options.Provider == "satellite" {
		return false
	}
	if options.Provider == "vpc-gen2" && options.PrivateOnly {
		return mode == "vpn"
	}
	return mode == "public"
}

func validPublicationInputs(values ...string) bool {
	for _, value := range values {
		if value == "" {
			return false
		}
	}
	for _, index := range []int{4, 5, 6, 8, 9} {
		if !filepath.IsAbs(values[index]) {
			return false
		}
	}
	return true
}

func publisherConfig(tokenPath, caPath string) (*rest.Config, error) {
	token, err := os.ReadFile(tokenPath)
	if err != nil || len(strings.TrimSpace(string(token))) == 0 {
		return nil, errors.New("publisher token is unavailable")
	}
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("Kubernetes service is unavailable")
	}
	if info, err := os.Stat(caPath); err != nil || info.Size() == 0 {
		return nil, errors.New("publisher CA is unavailable")
	}
	return &rest.Config{Host: "https://" + host + ":" + port, BearerToken: strings.TrimSpace(string(token)), TLSClientConfig: rest.TLSClientConfig{CAFile: caPath}}, nil
}

func publicationKubeconfig(manifestPath, outputDir string) ([]byte, error) {
	bundle, status, err := publicationBundle(manifestPath, outputDir)
	if err != nil || status.Mode != "public" {
		return nil, errors.New("invalid public auth bundle")
	}
	return bundle[publishedKubeconfigKey], nil
}

func validManifestCleanup(outcome, reason, stage string) bool {
	if outcome == "" {
		return reason == "" && stage == "" // manifests from before cleanup observability
	}
	return validAuthCleanup(outcome, reason, stage)
}

func publicationBundle(manifestPath, outputDir string) (map[string][]byte, servitorv1alpha1.AuthStatus, error) {
	manifest, err := readJSON[authManifest](manifestPath)
	if err != nil {
		return nil, servitorv1alpha1.AuthStatus{}, publicationManifestInvalid
	}
	// Preserve a parsed reference even when the rest of the manifest is invalid;
	// cleanup ownership must not depend on a worker-reported artifact mode.
	status := servitorv1alpha1.AuthStatus{Availability: manifest.Availability, Mode: manifest.Mode, Expiry: manifest.Expiry, Reason: manifest.Reason, CleanupOutcome: manifest.CleanupOutcome, CleanupReason: manifest.CleanupReason, CleanupStage: manifest.CleanupStage}
	if manifest.Certificate != nil {
		status.Certificate = &servitorv1alpha1.AuthCertificateReference{ID: manifest.Certificate.ID, AllocationUID: manifest.Certificate.AllocationUID, AttemptID: manifest.Certificate.AttemptID}
	}
	for _, reference := range manifest.ClearedCertificateReferences {
		status.ClearedCertificateReferences = append(status.ClearedCertificateReferences, servitorv1alpha1.AuthCertificateReference{ID: reference.ID, AllocationUID: reference.AllocationUID, AttemptID: reference.AttemptID})
	}
	if manifest.Version != 1 {
		return nil, status, publicationManifestInvalid
	}
	if manifest.Availability == "unavailable" {
		if manifest.Mode != "" || manifest.Expiry != "" || len(manifest.Artifacts) != 0 || !servitorv1alpha1.ValidAuthFailureReason(manifest.Reason) || !validManifestCleanup(manifest.CleanupOutcome, manifest.CleanupReason, manifest.CleanupStage) {
			return nil, status, publicationManifestInvalid
		}
		status.Mode, status.Expiry = "", ""
		return nil, status, nil
	}
	if manifest.Availability != "available" || manifest.Reason != "" || !validManifestCleanup(manifest.CleanupOutcome, manifest.CleanupReason, manifest.CleanupStage) {
		return nil, status, publicationManifestInvalid
	}
	expected := []string{publishedKubeconfigKey}
	switch manifest.Mode {
	case "public":
		if manifest.Expiry != "" {
			return nil, status, publicationManifestInvalid
		}
	case "vpn":
		if _, err := time.Parse(time.RFC3339, manifest.Expiry); err != nil {
			return nil, status, publicationManifestInvalid
		}
		expected = append(expected, publishedVPNKey)
	default:
		return nil, status, publicationManifestInvalid
	}
	if len(manifest.Artifacts) != len(expected) {
		return nil, status, publicationArtifactLayoutInvalid
	}
	allowed := make(map[string]bool, len(expected)+1)
	for index, name := range expected {
		if manifest.Artifacts[index].Name != name {
			return nil, status, publicationArtifactLayoutInvalid
		}
		allowed[name] = true
	}
	if filepath.Dir(manifestPath) == outputDir {
		allowed[filepath.Base(manifestPath)] = true
	}
	entries, err := os.ReadDir(outputDir)
	if err != nil || len(entries) != len(allowed) {
		return nil, status, publicationArtifactLayoutInvalid
	}
	bundle := make(map[string][]byte, len(expected))
	total := int64(0)
	for _, entry := range entries {
		if !allowed[entry.Name()] || !entry.Type().IsRegular() {
			return nil, status, publicationArtifactLayoutInvalid
		}
	}
	for _, name := range expected {
		path := filepath.Join(outputDir, name)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxPublishedFileBytes {
			return nil, status, publicationArtifactLayoutInvalid
		}
		total += info.Size()
		if total > maxPublishedBundleBytes {
			return nil, status, publicationArtifactLayoutInvalid
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, status, publicationArtifactLayoutInvalid
		}
		bundle[name] = contents
	}
	if validatePublishedKubeconfig(bundle[publishedKubeconfigKey]) != nil {
		return nil, status, publicationKubeconfigInvalid
	}
	if status.Mode == "vpn" {
		if err := validatePublishedVPN(bundle[publishedVPNKey], status.Expiry); err != nil {
			return nil, status, err
		}
	}
	return bundle, status, nil
}

func validatePublishedVPN(contents []byte, reportedExpiry string) error {
	if len(contents) == 0 || len(contents) > maxPublishedFileBytes {
		return publicationVPNProfileInvalid
	}
	blocks, directives, err := publishedVPNBlocks(string(contents))
	if err != nil || validatePublishedVPNDirectives(directives) != nil {
		return publicationVPNProfileInvalid
	}
	if err := validatePublishedProfileTrust(blocks["ca"]); err != nil {
		return publicationVPNProfileTrustInvalid
	}
	expiry, err := validatePublishedVPNCertificate(blocks["cert"], blocks["key"])
	if err != nil {
		return publicationVPNCertificateInvalid
	}
	if reportedExpiry != expiry.Format(time.RFC3339) {
		return publicationVPNExpiryMismatch
	}
	return nil
}

func publishedVPNBlocks(profile string) (map[string]string, string, error) {
	allowed := map[string]bool{"ca": true, "cert": true, "key": true, "tls-auth": true, "tls-crypt": true, "tls-crypt-v2": true}
	blocks := make(map[string]string)
	var directives, content strings.Builder
	current := ""
	for _, line := range strings.Split(profile, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "<") {
			if !strings.HasSuffix(trimmed, ">") {
				return nil, "", errors.New("malformed inline block")
			}
			name := strings.ToLower(strings.TrimSuffix(strings.TrimPrefix(trimmed, "<"), ">"))
			closing := strings.HasPrefix(name, "/")
			if closing {
				name = strings.TrimPrefix(name, "/")
			}
			if name == "" || strings.Trim(name, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
				return nil, "", errors.New("malformed inline block")
			}
			if closing {
				if current != name || strings.TrimSpace(content.String()) == "" {
					return nil, "", errors.New("malformed inline block")
				}
				blocks[name] = strings.TrimSpace(content.String())
				current = ""
				content.Reset()
				continue
			}
			if current != "" || !allowed[name] || blocks[name] != "" {
				return nil, "", errors.New("unsafe inline block")
			}
			current = name
			continue
		}
		if current != "" {
			content.WriteString(line)
			content.WriteByte('\n')
		} else {
			directives.WriteString(line)
			directives.WriteByte('\n')
		}
	}
	if current != "" || blocks["ca"] == "" || blocks["cert"] == "" || blocks["key"] == "" {
		return nil, "", errors.New("incomplete VPN profile")
	}
	return blocks, directives.String(), nil
}

func validatePublishedVPNDirectives(profile string) error {
	remote := false
	for _, line := range strings.Split(profile, "\n") {
		fields := strings.Fields(strings.TrimSpace(strings.SplitN(strings.SplitN(line, "#", 2)[0], ";", 2)[0]))
		if len(fields) == 0 {
			continue
		}
		switch strings.ToLower(fields[0]) {
		case "remote":
			if len(fields) < 2 || len(fields) > 4 || !validPublishedVPNHost(fields[1]) || len(fields) >= 3 && !validPublishedVPNPort(fields[2]) || len(fields) == 4 && !validPublishedVPNProtocol(fields[3]) {
				return errors.New("invalid remote")
			}
			remote = true
		case "auth-user-pass", "http-proxy-user-pass", "askpass", "script-security", "dns-updown", "up", "down", "route-up", "route-pre-down", "ipchange", "tls-verify", "auth-user-pass-verify", "client-connect", "client-disconnect", "learn-address", "client-crresponse", "plugin", "management", "pkcs12", "cert", "key", "ca", "secret", "tls-auth", "tls-crypt", "tls-crypt-v2":
			return errors.New("unsafe directive")
		case "http-proxy", "socks-proxy":
			return errors.New("proxy directive")
		}
	}
	if !remote {
		return errors.New("missing remote")
	}
	return nil
}

func validPublishedVPNHost(host string) bool {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	if net.ParseIP(host) != nil {
		return true
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if character != '-' && (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') && (character < '0' || character > '9') {
				return false
			}
		}
	}
	return true
}

func validPublishedVPNPort(value string) bool {
	port, err := strconv.ParseUint(value, 10, 16)
	return err == nil && port > 0
}

func validPublishedVPNProtocol(value string) bool {
	switch strings.ToLower(value) {
	case "udp", "udp4", "udp6", "tcp", "tcp4", "tcp6", "tcp-client", "tcp4-client", "tcp6-client":
		return true
	default:
		return false
	}
}

func validatePublishedProfileTrust(trustPEM string) error {
	trust, err := publishedCertificates(trustPEM)
	if err != nil || len(trust) == 0 {
		return errors.New("invalid trust")
	}
	now := time.Now()
	roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
	rootCount := 0
	for _, authority := range trust {
		if !authority.IsCA || now.Before(authority.NotBefore) || !now.Before(authority.NotAfter) || authority.KeyUsage&x509.KeyUsageCertSign == 0 {
			return errors.New("invalid trust authority")
		}
		if authority.CheckSignatureFrom(authority) == nil {
			roots.AddCert(authority)
			rootCount++
		} else {
			intermediates.AddCert(authority)
		}
	}
	if rootCount == 0 {
		return errors.New("missing trust root")
	}
	for _, authority := range trust {
		if authority.CheckSignatureFrom(authority) != nil {
			if _, err := authority.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
				return errors.New("invalid trust chain")
			}
		}
	}
	return nil
}

func validatePublishedVPNCertificate(certificatePEM, keyPEM string) (time.Time, error) {
	certificates, err := publishedCertificates(certificatePEM)
	if err != nil || len(certificates) < 2 {
		return time.Time{}, errors.New("invalid certificate")
	}
	certificate := certificates[0]
	now := time.Now()
	if certificate.IsCA || now.Before(certificate.NotBefore) || !now.Before(certificate.NotAfter) || certificate.KeyUsage&x509.KeyUsageDigitalSignature == 0 || !containsPublishedUsage(certificate.ExtKeyUsage, x509.ExtKeyUsageClientAuth) || containsPublishedUsage(certificate.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		return time.Time{}, errors.New("invalid certificate usage")
	}
	key, err := publishedPrivateKey(keyPEM)
	if err != nil {
		return time.Time{}, errors.New("invalid certificate key")
	}
	if !publishedPublicKeysEqual(key.Public(), certificate.PublicKey) {
		return time.Time{}, errors.New("invalid certificate key")
	}
	chain := certificates[1:]
	for _, authority := range chain {
		if !authority.IsCA || now.Before(authority.NotBefore) || !now.Before(authority.NotAfter) || authority.KeyUsage&x509.KeyUsageCertSign == 0 {
			return time.Time{}, errors.New("invalid certificate chain authority")
		}
	}
	if certificate.CheckSignatureFrom(chain[0]) != nil {
		return time.Time{}, errors.New("invalid certificate chain")
	}
	for index := 0; index+1 < len(chain); index++ {
		if chain[index].CheckSignatureFrom(chain[index+1]) != nil {
			return time.Time{}, errors.New("invalid certificate chain")
		}
	}
	roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
	for index, authority := range chain {
		if index == len(chain)-1 {
			roots.AddCert(authority)
		} else {
			intermediates.AddCert(authority)
		}
	}
	if _, err := certificate.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return time.Time{}, errors.New("invalid certificate chain")
	}
	return certificate.NotAfter.UTC(), nil
}

func validatePublishedCertificate(certificatePEM, keyPEM, trustPEM string) (time.Time, error) {
	certificates, err := publishedCertificates(certificatePEM)
	if err != nil || len(certificates) != 1 {
		return time.Time{}, errors.New("invalid certificate")
	}
	certificate := certificates[0]
	now := time.Now()
	if certificate.IsCA || now.Before(certificate.NotBefore) || !now.Before(certificate.NotAfter) || certificate.KeyUsage&x509.KeyUsageDigitalSignature == 0 || !containsPublishedUsage(certificate.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return time.Time{}, errors.New("invalid certificate usage")
	}
	key, err := publishedPrivateKey(keyPEM)
	if err != nil || !publishedPublicKeysEqual(key.Public(), certificate.PublicKey) {
		return time.Time{}, errors.New("invalid certificate key")
	}
	trust, err := publishedCertificates(trustPEM)
	if err != nil || len(trust) == 0 {
		return time.Time{}, errors.New("invalid trust")
	}
	roots, intermediates := x509.NewCertPool(), x509.NewCertPool()
	for _, authority := range trust {
		if authority.IsCA && authority.KeyUsage&x509.KeyUsageCertSign != 0 {
			if authority.CheckSignatureFrom(authority) == nil {
				roots.AddCert(authority)
			} else {
				intermediates.AddCert(authority)
			}
		}
	}
	if len(roots.Subjects()) == 0 {
		return time.Time{}, errors.New("missing trust root")
	}
	if _, err := certificate.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return time.Time{}, errors.New("invalid certificate chain")
	}
	return certificate.NotAfter.UTC(), nil
}

func publishedCertificates(value string) ([]*x509.Certificate, error) {
	var certificates []*x509.Certificate
	for rest := []byte(value); ; {
		block, remaining := pem.Decode(rest)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, errors.New("invalid certificate PEM")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		certificates = append(certificates, certificate)
		rest = remaining
		if len(strings.TrimSpace(string(rest))) == 0 {
			return certificates, nil
		}
	}
}

func publishedPrivateKey(value string) (crypto.Signer, error) {
	block, rest := pem.Decode([]byte(value))
	if block == nil || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, errors.New("invalid private key PEM")
	}
	var key any
	var err error
	switch block.Type {
	case "PRIVATE KEY":
		key, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(block.Bytes)
	default:
		return nil, errors.New("invalid private key PEM")
	}
	if err != nil {
		return nil, err
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, errors.New("unsupported private key")
	}
	return signer, nil
}

func containsPublishedUsage(usages []x509.ExtKeyUsage, wanted x509.ExtKeyUsage) bool {
	for _, usage := range usages {
		if usage == wanted {
			return true
		}
	}
	return false
}

func publishedPublicKeysEqual(left, right crypto.PublicKey) bool {
	comparable, ok := left.(interface{ Equal(crypto.PublicKey) bool })
	return ok && comparable.Equal(right)
}

func validatePublishedKubeconfig(contents []byte) error {
	config, err := clientcmd.Load(contents)
	if err != nil {
		return publicationKubeconfigInvalid
	}
	if config.CurrentContext == "" {
		return publicationKubeconfigInvalid
	}
	context, ok := config.Contexts[config.CurrentContext]
	if !ok || context.Cluster == "" || context.AuthInfo == "" {
		return publicationKubeconfigInvalid
	}
	if len(config.Clusters) == 0 || len(config.AuthInfos) == 0 || len(config.Contexts) == 0 {
		return publicationKubeconfigInvalid
	}
	for _, cluster := range config.Clusters {
		endpoint, err := url.ParseRequestURI(cluster.Server)
		if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" {
			return publicationKubeconfigInvalid
		}
		if cluster.CertificateAuthority != "" || cluster.InsecureSkipTLSVerify || len(cluster.CertificateAuthorityData) == 0 {
			return publicationKubeconfigInvalid
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cluster.CertificateAuthorityData) {
			return publicationKubeconfigInvalid
		}
	}
	for _, authInfo := range config.AuthInfos {
		if authInfo.Token != "" || authInfo.TokenFile != "" || authInfo.ClientCertificate != "" || authInfo.ClientKey != "" || authInfo.Exec != nil || authInfo.AuthProvider != nil || len(authInfo.ClientCertificateData) == 0 || len(authInfo.ClientKeyData) == 0 {
			return publicationKubeconfigInvalid
		}
		if _, err := tls.X509KeyPair(authInfo.ClientCertificateData, authInfo.ClientKeyData); err != nil {
			return publicationKubeconfigInvalid
		}
		cluster := config.Clusters[context.Cluster]
		if _, err := validatePublishedCertificate(string(authInfo.ClientCertificateData), string(authInfo.ClientKeyData), string(cluster.CertificateAuthorityData)); err != nil {
			return publicationKubeconfigInvalid
		}
	}
	for _, kubeContext := range config.Contexts {
		if kubeContext.Cluster == "" || kubeContext.AuthInfo == "" || config.Clusters[kubeContext.Cluster] == nil || config.AuthInfos[kubeContext.AuthInfo] == nil {
			return publicationKubeconfigInvalid
		}
	}
	return nil
}

// markPublished atomically replaces a valid unauthenticated report with one
// terminal auth status. It never writes a pending auth status.
func markPublished(path, uid, operation string, status servitorv1alpha1.AuthStatus) error {
	report, err := readJSON[pipeline.Report](path)
	if err != nil {
		return fmt.Errorf("read infrastructure report: %w", err)
	}
	if report.ClusterUID != uid || report.OperationID != operation || report.Auth != nil || report.Validate(uid, operation) != nil {
		return errors.New("infrastructure report is not a valid unauthenticated active-operation report")
	}
	report.Auth = &status
	if err := report.Validate(uid, operation); err != nil {
		return fmt.Errorf("terminal auth report is invalid: %w", err)
	}
	if err := writePublishedReport(path, report); err != nil {
		return fmt.Errorf("persist terminal auth report: %w", err)
	}
	return nil
}

// compensatePublishedReport replaces only this attempt's unauthenticated or
// available terminal report after an ambiguous persistence failure.
func compensatePublishedReport(path, uid, operation, attempt string, status servitorv1alpha1.AuthStatus) error {
	if status.Availability != "unavailable" || status.AttemptID != attempt {
		return errors.New("compensation auth status does not match the active attempt")
	}
	report, err := readJSON[pipeline.Report](path)
	if err != nil {
		return fmt.Errorf("read terminal auth report for compensation: %w", err)
	}
	if report.Version != 1 || report.ClusterUID != uid || report.OperationID != operation || report.Validate(uid, operation) != nil {
		return errors.New("terminal auth report does not match the active operation")
	}
	if report.Auth != nil && (report.Auth.Availability != "available" || report.Auth.AttemptID != attempt) {
		return errors.New("terminal auth report does not match the active attempt")
	}
	report.Auth = &status
	if err := report.Validate(uid, operation); err != nil {
		return fmt.Errorf("compensation auth report is invalid: %w", err)
	}
	if err := writeReport(path, report); err != nil {
		return fmt.Errorf("persist compensation auth report: %w", err)
	}
	return nil
}

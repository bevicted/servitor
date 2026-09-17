package main

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
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
	publisherTimeout        = time.Minute
	publisherUIDLabel       = "servitor.bevicted.github.io/auth-uid"
	publisherOperationKey   = "servitor.bevicted.github.io/auth-operation"
	publishedKubeconfigKey  = "kubeconfig.yaml"
	publishedVPNKey         = "client.ovpn"
	maxPublishedBundleBytes = 2 << 20
	maxPublishedFileBytes   = 1 << 20
)

// publishPublicAuth is deliberately best effort. Infrastructure success already
// exists in reportPath, and a publication failure must not fail the TaskRun.
func publishPublicAuth(secretName, uid, operation, manifestPath, outputDir, reportPath, namespace, tokenPath, caPath string) {
	if !validPublicationInputs(secretName, uid, operation, manifestPath, outputDir, reportPath, namespace, tokenPath, caPath) {
		return
	}
	bundle, status, err := publicationBundle(manifestPath, outputDir)
	if err != nil {
		return
	}
	config, err := publisherConfig(tokenPath, caPath)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), publisherTimeout)
	defer cancel()
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return
	}
	publishAuthBundle(ctx, clientset.CoreV1().Secrets(namespace), secretName, uid, operation, bundle, status, reportPath)
}

// publishAuthBundle is the single checked update path. The publisher can only
// update a pre-created, UID and operation-bound Secret; it never creates one.
func publishAuthBundle(ctx context.Context, secrets corev1client.SecretInterface, secretName, uid, operation string, bundle map[string][]byte, status servitorv1alpha1.AuthStatus, reportPath string) bool {
	if ctx.Err() != nil {
		return false
	}
	secret, err := secrets.Get(ctx, secretName, metav1.GetOptions{})
	if err != nil || secret.Labels[publisherUIDLabel] != uid || secret.Annotations[publisherOperationKey] != operation {
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	secret.Data = bundle
	if _, err := secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		return false
	}
	markPublished(reportPath, uid, operation, status)
	return true
}

func validPublicationInputs(values ...string) bool {
	for _, value := range values {
		if value == "" {
			return false
		}
	}
	for _, index := range []int{3, 4, 5, 7, 8} {
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

func publicationBundle(manifestPath, outputDir string) (map[string][]byte, servitorv1alpha1.AuthStatus, error) {
	manifest, err := readJSON[authManifest](manifestPath)
	if err != nil || manifest.Version != 1 || manifest.Availability != "available" {
		return nil, servitorv1alpha1.AuthStatus{}, errors.New("invalid auth manifest")
	}
	status := servitorv1alpha1.AuthStatus{Availability: "available", Mode: manifest.Mode, Expiry: manifest.Expiry}
	expected := []string{publishedKubeconfigKey}
	switch manifest.Mode {
	case "public":
		if manifest.Expiry != "" {
			return nil, status, errors.New("public auth has expiry")
		}
	case "vpn":
		if _, err := time.Parse(time.RFC3339, manifest.Expiry); err != nil {
			return nil, status, errors.New("VPN auth has invalid expiry")
		}
		expected = append(expected, publishedVPNKey)
	default:
		return nil, status, errors.New("invalid auth mode")
	}
	if len(manifest.Artifacts) != len(expected) {
		return nil, status, errors.New("auth manifest is incomplete")
	}
	allowed := make(map[string]bool, len(expected)+1)
	for index, name := range expected {
		if manifest.Artifacts[index].Name != name {
			return nil, status, errors.New("auth manifest artifacts are invalid")
		}
		allowed[name] = true
	}
	if filepath.Dir(manifestPath) == outputDir {
		allowed[filepath.Base(manifestPath)] = true
	}
	entries, err := os.ReadDir(outputDir)
	if err != nil || len(entries) != len(allowed) {
		return nil, status, errors.New("auth output is incomplete or contains extra artifacts")
	}
	bundle := make(map[string][]byte, len(expected))
	total := int64(0)
	for _, entry := range entries {
		if !allowed[entry.Name()] || !entry.Type().IsRegular() {
			return nil, status, errors.New("auth output contains an invalid artifact")
		}
	}
	for _, name := range expected {
		path := filepath.Join(outputDir, name)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxPublishedFileBytes {
			return nil, status, errors.New("invalid auth artifact")
		}
		total += info.Size()
		if total > maxPublishedBundleBytes {
			return nil, status, errors.New("auth bundle exceeds size limit")
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, status, errors.New("read auth artifact")
		}
		bundle[name] = contents
	}
	if validatePublishedKubeconfig(bundle[publishedKubeconfigKey]) != nil || (status.Mode == "vpn" && validatePublishedVPN(bundle[publishedVPNKey], status.Expiry) != nil) {
		return nil, status, errors.New("invalid auth bundle contents")
	}
	return bundle, status, nil
}

func validatePublishedVPN(contents []byte, reportedExpiry string) error {
	if len(contents) == 0 || len(contents) > maxPublishedFileBytes {
		return errors.New("invalid VPN profile size")
	}
	blocks, directives, err := publishedVPNBlocks(string(contents))
	if err != nil || validatePublishedVPNDirectives(directives) != nil {
		return errors.New("unsafe VPN profile")
	}
	expiry, err := validatePublishedCertificate(blocks["cert"], blocks["key"], blocks["ca"])
	if err != nil || reportedExpiry != expiry.Format(time.RFC3339) {
		return errors.New("invalid VPN certificate")
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

func validatePublishedCertificate(certificatePEM, keyPEM, trustPEM string) (time.Time, error) {
	certificates, err := publishedCertificates(certificatePEM)
	if err != nil || len(certificates) == 0 {
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
	for _, authority := range append(trust, certificates[1:]...) {
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
		return fmt.Errorf("decode kubeconfig: %w", err)
	}
	if config.CurrentContext == "" {
		return errors.New("kubeconfig has no current context")
	}
	context, ok := config.Contexts[config.CurrentContext]
	if !ok || context.Cluster == "" || context.AuthInfo == "" {
		return errors.New("kubeconfig has an incomplete current context")
	}
	if len(config.Clusters) == 0 || len(config.AuthInfos) == 0 || len(config.Contexts) == 0 {
		return errors.New("kubeconfig is incomplete")
	}
	for name, cluster := range config.Clusters {
		endpoint, err := url.ParseRequestURI(cluster.Server)
		if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" {
			return fmt.Errorf("cluster %q has an invalid API endpoint", name)
		}
		if cluster.CertificateAuthority != "" || cluster.InsecureSkipTLSVerify || len(cluster.CertificateAuthorityData) == 0 {
			return fmt.Errorf("cluster %q is not self-contained", name)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cluster.CertificateAuthorityData) {
			return fmt.Errorf("cluster %q has invalid certificate authority data", name)
		}
	}
	for name, authInfo := range config.AuthInfos {
		if authInfo.Token != "" || authInfo.TokenFile != "" || authInfo.ClientCertificate != "" || authInfo.ClientKey != "" || authInfo.Exec != nil || authInfo.AuthProvider != nil || len(authInfo.ClientCertificateData) == 0 || len(authInfo.ClientKeyData) == 0 {
			return fmt.Errorf("user %q is not self-contained certificate authentication", name)
		}
		if _, err := tls.X509KeyPair(authInfo.ClientCertificateData, authInfo.ClientKeyData); err != nil {
			return fmt.Errorf("user %q has invalid certificate authentication data", name)
		}
		cluster := config.Clusters[context.Cluster]
		if _, err := validatePublishedCertificate(string(authInfo.ClientCertificateData), string(authInfo.ClientKeyData), string(cluster.CertificateAuthorityData)); err != nil {
			return fmt.Errorf("user %q has invalid certificate authentication data", name)
		}
	}
	for name, kubeContext := range config.Contexts {
		if kubeContext.Cluster == "" || kubeContext.AuthInfo == "" || config.Clusters[kubeContext.Cluster] == nil || config.AuthInfos[kubeContext.AuthInfo] == nil {
			return fmt.Errorf("context %q has unresolved authentication", name)
		}
	}
	return nil
}

func markPublished(path, uid, operation string, status servitorv1alpha1.AuthStatus) {
	report, err := readJSON[pipeline.Report](path)
	if err != nil || report.Validate(uid, operation) != nil || report.Auth == nil || report.Auth.Availability != "unavailable" {
		return
	}
	report.Auth = &status
	_ = writeReport(path, report)
}

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/pipeline"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	publisherTimeout       = time.Minute
	publisherUIDLabel      = "servitor.bevicted.github.io/auth-uid"
	publisherOperationKey  = "servitor.bevicted.github.io/auth-operation"
	publishedKubeconfigKey = "kubeconfig.yaml"
)

// publishPublicAuth is deliberately best effort. Infrastructure success already
// exists in reportPath, and a publication failure must not fail the TaskRun.
func publishPublicAuth(secretName, uid, operation, manifestPath, outputDir, reportPath, namespace, tokenPath, caPath string) {
	if !validPublicationInputs(secretName, uid, operation, manifestPath, outputDir, reportPath, namespace, tokenPath, caPath) {
		return
	}
	contents, err := publicationKubeconfig(manifestPath, outputDir)
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
	secret, err := clientset.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil || secret.Labels[publisherUIDLabel] != uid || secret.Annotations[publisherOperationKey] != operation {
		return
	}
	if secret.Data == nil {
		secret.Data = make(map[string][]byte, 1)
	}
	secret.Data[publishedKubeconfigKey] = contents
	if _, err := clientset.CoreV1().Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		return
	}
	markPublished(reportPath, uid, operation)
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
	manifest, err := readJSON[authManifest](manifestPath)
	if err != nil || manifest.Version != 1 || manifest.Availability != "available" || len(manifest.Artifacts) != 1 || manifest.Artifacts[0].Name != publishedKubeconfigKey {
		return nil, errors.New("invalid public auth manifest")
	}
	entries, err := os.ReadDir(outputDir)
	allowed := map[string]bool{publishedKubeconfigKey: true}
	if filepath.Dir(manifestPath) == outputDir {
		allowed[filepath.Base(manifestPath)] = true
	}
	if err != nil || len(entries) != len(allowed) {
		return nil, errors.New("public auth output is incomplete or contains extra artifacts")
	}
	for _, entry := range entries {
		if !allowed[entry.Name()] || !entry.Type().IsRegular() {
			return nil, errors.New("public auth output contains an invalid artifact")
		}
	}
	path := filepath.Join(outputDir, publishedKubeconfigKey)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() == 0 || info.Size() > 1024*1024 {
		return nil, errors.New("invalid public auth artifact")
	}
	contents, err := os.ReadFile(path)
	if err != nil || validatePublishedKubeconfig(contents) != nil {
		return nil, errors.New("invalid public kubeconfig")
	}
	return contents, nil
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
	}
	for name, kubeContext := range config.Contexts {
		if kubeContext.Cluster == "" || kubeContext.AuthInfo == "" || config.Clusters[kubeContext.Cluster] == nil || config.AuthInfos[kubeContext.AuthInfo] == nil {
			return fmt.Errorf("context %q has unresolved authentication", name)
		}
	}
	return nil
}

func markPublished(path, uid, operation string) {
	report, err := readJSON[pipeline.Report](path)
	if err != nil || report.Validate(uid, operation) != nil || report.PublicAuth == nil || report.PublicAuth.Availability != "unavailable" {
		return
	}
	report.PublicAuth = &servitorv1alpha1.PublicAuthStatus{Availability: "available"}
	_ = writeReport(path, report)
}

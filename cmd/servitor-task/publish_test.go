package main

import (
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
	"testing"
	"time"

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

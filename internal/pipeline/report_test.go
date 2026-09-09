package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
)

type testLogs struct {
	data []byte
	err  error
}

func (l testLogs) ReadContainerLog(context.Context, string, string, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.data)), l.err
}

func validReport(t *testing.T) []byte {
	t.Helper()
	data, err := json.Marshal(Report{Version: 1, ClusterUID: "uid", OperationID: "plan-a", ResolvedOptions: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "cluster"}, Recovery: validRecovery()})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func validRecovery() servitorv1alpha1.RecoveryMetadata {
	return servitorv1alpha1.RecoveryMetadata{
		Version: 1, Target: "target", TFVarsSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Endpoints: map[string]string{
			"IAM": "https://iam.example.invalid", "ContainerService": "https://containers.example.invalid", "GlobalTagging": "https://tagging.example.invalid", "ResourceManagement": "https://management.example.invalid", "ResourceController": "https://controller.example.invalid", "VPC": "https://vpc.example.invalid",
		},
		Values: servitorv1alpha1.RecoveryValues{ClusterName: "cluster", ResourceGroupName: "Default", Region: "us-south", ClusterMode: "vpc", Platform: "openshift", KubeVersion: "4.22_openshift", WorkerCount: 2, Zone: "us-south-1", Flavor: "bx2.4x16"},
	}
}

func TestReadReportAcceptsOnlyExactBoundedIdentity(t *testing.T) {
	if _, err := ReadReport(context.Background(), testLogs{data: validReport(t)}, "ns", "pod", "step-report", "uid", "plan-a"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		data      []byte
		container string
	}{
		{"extra JSON", append(validReport(t), []byte("{}")...), "step-report"},
		{"wrong container", validReport(t), ""},
		{"oversized", bytes.Repeat([]byte("x"), MaxReportBytes+1), "step-report"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ReadReport(context.Background(), testLogs{data: test.data}, "ns", "pod", test.container, "uid", "plan-a"); err == nil {
				t.Fatal("accepted invalid report")
			}
		})
	}
	stale := validReport(t)
	stale = bytes.Replace(stale, []byte(`"uid"`), []byte(`"other"`), 1)
	if _, err := ReadReport(context.Background(), testLogs{data: stale}, "ns", "pod", "step-report", "uid", "plan-a"); err == nil {
		t.Fatal("accepted stale report")
	}
}

func TestDecodeReportRejectsUnsafeRecoveryMetadata(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*servitorv1alpha1.RecoveryMetadata)
	}{
		{"endpoint credentials", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Endpoints["IAM"] = "https://user:password@example.invalid"
		}},
		{"credential marker in endpoint user info", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Endpoints["IAM"] = "https://password@example.invalid"
		}},
		{"credential marker in endpoint host", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Endpoints["IAM"] = "https://password.example.invalid"
		}},
		{"credential marker in endpoint path", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Endpoints["IAM"] = "https://iam.example.invalid/password=secret"
		}},
		{"encoded credential marker in endpoint path", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Endpoints["IAM"] = "https://iam.example.invalid/p%61ssword=value"
		}},
		{"endpoint query", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Endpoints["IAM"] = "https://iam.example.invalid?api_key=value"
		}},
		{"credential marker in endpoint fragment", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Endpoints["IAM"] = "https://iam.example.invalid#password=value"
		}},
		{"unknown endpoint", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Endpoints["Token"] = "https://token.example.invalid"
		}},
		{"credential-like value", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Values.ResourceGroupName = "Bearer token=value"
		}},
		{"non-canonical value list", func(recovery *servitorv1alpha1.RecoveryMetadata) {
			recovery.Values.SubnetIDs = []string{"subnet-b", "subnet-a"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			recovery := validRecovery()
			test.mutate(&recovery)
			data, err := json.Marshal(Report{Version: 1, ClusterUID: "uid", OperationID: "plan-a", ResolvedOptions: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "cluster"}, Recovery: recovery})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeReport(data, "uid", "plan-a"); err == nil {
				t.Fatal("accepted unsafe recovery metadata")
			}
		})
	}
}

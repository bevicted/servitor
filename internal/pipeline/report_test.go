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
	data, err := json.Marshal(Report{Version: 1, ClusterUID: "uid", OperationID: "plan-a", ResolvedOptions: servitorv1alpha1.ResolvedOptions{UserOptions: servitorv1alpha1.UserOptions{Provider: "vpc-gen2", Version: "4.22"}, ClusterName: "cluster"}, Recovery: servitorv1alpha1.RecoveryMetadata{Version: 1, Target: "target", TFVarsSHA256: "digest"}})
	if err != nil {
		t.Fatal(err)
	}
	return data
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

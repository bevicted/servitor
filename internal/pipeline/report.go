// Package pipeline owns the bounded, controller-validated operation report protocol.
package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
)

const MaxReportBytes = 32 * 1024
const ReportContainerName = "report"

var errReadReportLog = errors.New("read report log")

// Report is the only structured output emitted by a Tekton operation.
type Report struct {
	Version         int                               `json:"version"`
	ClusterUID      string                            `json:"clusterUID"`
	OperationID     string                            `json:"operationID"`
	ResolvedOptions servitorv1alpha1.ResolvedOptions  `json:"resolvedOptions"`
	Recovery        servitorv1alpha1.RecoveryMetadata `json:"recovery"`
	Review          servitorv1alpha1.ReviewSummary    `json:"review,omitempty"`
	Ready           servitorv1alpha1.ReadySummary     `json:"ready,omitempty"`
}

func (r Report) Validate(expectedUID, expectedOperation string) error {
	if r.Version != 1 || r.ClusterUID != expectedUID || r.OperationID != expectedOperation {
		return errors.New("report identity does not match the active operation")
	}
	if err := validateSummary(r.Review.Resources); err != nil {
		return err
	}
	if err := validateSummary(r.Ready.Resources); err != nil {
		return err
	}
	if r.Recovery.Version != 1 || strings.TrimSpace(r.Recovery.Target) == "" || strings.TrimSpace(r.Recovery.TFVarsSHA256) == "" {
		return errors.New("report has incomplete recovery metadata")
	}
	if r.ResolvedOptions.ClusterName == "" || r.ResolvedOptions.Provider == "" || r.ResolvedOptions.Version == "" {
		return errors.New("report has incomplete resolved options")
	}
	return nil
}

func validateSummary(resources []servitorv1alpha1.SummaryResource) error {
	if len(resources) > 256 {
		return errors.New("report has too many resources")
	}
	for _, resource := range resources {
		if len(resource.Role) > 64 || len(resource.ID) > 256 || len(resource.Name) > 256 || len(resource.Actions) > 2 {
			return errors.New("report has invalid resource metadata")
		}
		for _, value := range append([]string{resource.Role, resource.ID, resource.Name}, resource.Actions...) {
			if strings.ContainsAny(value, "\x00\r\n") {
				return errors.New("report has unsafe resource metadata")
			}
		}
	}
	return nil
}

// DecodeReport accepts exactly one complete bounded JSON document.
func DecodeReport(data []byte, expectedUID, expectedOperation string) (Report, error) {
	if len(data) == 0 || len(data) > MaxReportBytes {
		return Report{}, errors.New("report length is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var report Report
	if err := decoder.Decode(&report); err != nil {
		return Report{}, fmt.Errorf("decode report: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Report{}, errors.New("report must contain exactly one JSON document")
	}
	if err := report.Validate(expectedUID, expectedOperation); err != nil {
		return Report{}, err
	}
	return report, nil
}

// LogReader reads one named Pod container log. It deliberately cannot read arbitrary logs.
type LogReader interface {
	ReadContainerLog(context.Context, string, string, string) (io.ReadCloser, error)
}

// ReadReport reads and validates a report log with a hard byte limit.
func ReadReport(ctx context.Context, reader LogReader, namespace, podName, container, uid, operation string) (Report, error) {
	if podName == "" || container == "" {
		return Report{}, errors.New("report Pod or container is invalid")
	}
	stream, err := reader.ReadContainerLog(ctx, namespace, podName, container)
	if err != nil {
		return Report{}, fmt.Errorf("%w: %w", errReadReportLog, err)
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, MaxReportBytes+1))
	if err != nil {
		return Report{}, fmt.Errorf("%w: %w", errReadReportLog, err)
	}
	return DecodeReport(data, uid, operation)
}

// IsLogReadError reports whether the report could not be read from the Pod log API.
func IsLogReadError(err error) bool { return errors.Is(err, errReadReportLog) }

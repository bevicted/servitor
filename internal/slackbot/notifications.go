package slackbot

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/lifecycle"
	"github.com/bevicted/servitor/internal/state"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusNotifier turns controller-owned status transitions into sanitized Slack
// notices. A receipt is claimed before delivery and released when delivery
// fails; Slack delivery remains non-transactional across a process crash.
type StatusNotifier struct {
	Client    client.Client
	Namespace string
	Responder Responder
	Receipts  *state.EventStore
	Interval  time.Duration
	Clock     func() time.Time
}

func (n *StatusNotifier) NeedLeaderElection() bool { return true }
func (n *StatusNotifier) Start(ctx context.Context) error {
	interval := n.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := n.notify(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func (n *StatusNotifier) notify(ctx context.Context) error {
	if n.Client == nil || n.Responder == nil || n.Receipts == nil || n.Namespace == "" {
		return fmt.Errorf("Slack status notifier is not configured")
	}
	var clusters servitorv1alpha1.ServitorClusterList
	if err := n.Client.List(ctx, &clusters, client.InNamespace(n.Namespace)); err != nil {
		return err
	}
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		for _, notice := range statusNoticesAt(cluster, n.now()) {
			if notice.reviewDeadline != nil && !n.now().Before(notice.reviewDeadline.Time) {
				continue
			}
			claimed, err := n.Receipts.Claim(ctx, notice.id)
			if err != nil {
				return err
			}
			if !claimed {
				if !notice.cleanupCause {
					continue
				}
				delivered, err := n.Receipts.Delivered(ctx, notice.id)
				if err != nil {
					return err
				}
				if delivered {
					continue
				}
				// A command may have claimed the cause before replying. Do not let
				// a later completion overtake that reply.
				break
			}
			if err := n.Responder.Reply(ctx, Response{Channel: cluster.Spec.Slack.ChannelID, ThreadTimestamp: cluster.Spec.Slack.ThreadTimestamp, Text: notice.text}); err != nil {
				if releaseErr := n.Receipts.Release(ctx, notice.id); releaseErr != nil {
					return releaseErr
				}
				// Later notices may describe completion. Do not let one overtake a
				// failed cleanup cause delivery in the same thread.
				break
			}
			if err := n.Receipts.MarkDelivered(ctx, notice.id); err != nil {
				return err
			}
		}
	}
	return nil
}

type statusNotice struct {
	id             string
	text           string
	cleanupCause   bool
	reviewDeadline *metav1.Time
}

func (n *StatusNotifier) now() time.Time {
	if n.Clock != nil {
		return n.Clock().UTC()
	}
	return time.Now().UTC()
}

func statusNotices(cluster *servitorv1alpha1.ServitorCluster) []statusNotice {
	return statusNoticesAt(cluster, time.Now().UTC())
}

func statusNoticesAt(cluster *servitorv1alpha1.ServitorCluster, now time.Time) []statusNotice {
	if cluster.Spec.Slack.ChannelID == "" || cluster.Spec.Slack.ThreadTimestamp == "" {
		return nil
	}
	uid := clusterNoticeUID(cluster)
	phase := cluster.Status.Phase
	var texts []string
	switch phase {
	case servitorv1alpha1.PhaseAwaitingApproval:
		if cluster.Status.ReviewDeadline != nil && now.Before(cluster.Status.ReviewDeadline.Time) {
			texts = reviewNoticeTexts(cluster.Status.ResolvedOptions, cluster.Status.Review, cluster.Status.ReviewDeadline.Time)
		}
	case servitorv1alpha1.PhaseReady:
		expiry := time.Time{}
		if cluster.Status.LeaseExpiresAt != nil {
			expiry = cluster.Status.LeaseExpiresAt.Time
		}
		texts = readyNoticeTexts(cluster.Status.Ready, expiry, time.Now().UTC())
	case servitorv1alpha1.PhaseCleanupComplete:
		texts = []string{"Cleanup complete."}
	case servitorv1alpha1.PhaseUnresolved:
		if cluster.Status.Cleanup != nil {
			texts = []string{"Cleanup is unresolved. An administrator must inspect the allocation CR status and private cluster logs."}
		} else {
			texts = []string{"The operation is unresolved. An administrator must inspect the allocation CR status and private cluster logs."}
		}
	}
	notices := make([]statusNotice, 0, len(texts)+2)
	if cleanup := cluster.Status.Cleanup; cleanup != nil {
		notices = append(notices, statusNotice{id: cleanupCauseNoticeID(uid, cleanup.Reason), text: cleanupCauseText(cleanup.Reason, cluster.Status.PlanRejection), cleanupCause: true})
	}
	notices = append(notices, phaseNotices(uid, phase, texts)...)
	if phase == servitorv1alpha1.PhaseAwaitingApproval && cluster.Status.ReviewDeadline != nil {
		for index := range notices {
			notices[index].reviewDeadline = cluster.Status.ReviewDeadline
		}
	}
	if cleanup := cluster.Status.Cleanup; cleanup != nil && cleanup.NextRetryAt != nil && phase == servitorv1alpha1.PhaseCleanupPending {
		notices = append(notices, statusNotice{id: fmt.Sprintf("cleanup-retry:%s:%d", uid, cleanup.RetryCount), text: fmt.Sprintf("Cleanup retry %d is scheduled for %s.", cleanup.RetryCount, cleanup.NextRetryAt.Time.UTC().Format("2006-01-02 15:04:05 UTC"))})
	}
	if extension := cluster.Status.LeaseExtension; extension != nil {
		if text := extensionOutcomeText(extension, now); text != "" {
			notices = append(notices, statusNotice{id: extensionNoticeID(uid, extension), text: text})
		}
	}
	return notices
}

func clusterNoticeUID(cluster *servitorv1alpha1.ServitorCluster) string {
	if cluster.UID != "" {
		return string(cluster.UID)
	}
	return cluster.Namespace + "/" + cluster.Name
}

func cleanupCauseNoticeID(uid string, reason servitorv1alpha1.CleanupReason) string {
	return fmt.Sprintf("cleanup-cause:%s:%s", uid, reason)
}

func cleanupCauseText(reason servitorv1alpha1.CleanupReason, rejection *servitorv1alpha1.PlanRejection) string {
	if rejection != nil {
		return planRejectionText(*rejection) + "\nCleaning up..."
	}
	switch reason {
	case servitorv1alpha1.CleanupReasonRejected:
		return "Plan rejected.\nCleaning up..."
	case servitorv1alpha1.CleanupReasonReviewExpired:
		return "Plan auto rejected due to missed approval deadline.\nCleaning up..."
	case servitorv1alpha1.CleanupReasonPlanningFailed:
		return "Planning failed. Cleaning up..."
	case servitorv1alpha1.CleanupReasonApplyFailed:
		return "Apply failed. Cleaning up..."
	case servitorv1alpha1.CleanupReasonLeaseExpired:
		return "Lease expired. Cleaning up..."
	default:
		return "Cleaning up..."
	}
}

func planRejectionText(rejection servitorv1alpha1.PlanRejection) string {
	key := "`" + rejection.OptionKey + "`"
	switch rejection.ReasonCode {
	case "target_not_configured":
		return "Cannot plan this request: " + key + " is not configured. Correct " + key + " and create a new request."
	case "provider_not_supported":
		return "Cannot plan this request: " + key + " is not supported for the selected target. Correct " + key + " and create a new request."
	case "version_not_supported":
		return "Cannot plan this request: " + key + " is not supported. Correct " + key + " and create a new request."
	default:
		return "Cannot plan this request: " + key + " is not currently available. Correct " + key + " and create a new request."
	}
}

func phaseNotices(uid, phase string, texts []string) []statusNotice {
	notices := make([]statusNotice, 0, len(texts))
	for index, text := range texts {
		notices = append(notices, statusNotice{id: fmt.Sprintf("phase:%s:%s:%d", uid, phase, index), text: text})
	}
	return notices
}

func reviewNoticeTexts(options *servitorv1alpha1.ResolvedOptions, review *servitorv1alpha1.ReviewSummary, deadline time.Time) []string {
	rows := make([][]string, 0)
	if review != nil {
		rows = make([][]string, 0, len(review.Resources))
		for _, resource := range review.Resources {
			rows = append(rows, []string{resource.Role, strings.Join(resource.Actions, "/")})
		}
	}
	create, change, destroy := actionTotals(rows)
	texts := statusTableChunks("Cluster request", nil, reviewConfigRows(options), "")
	deadlineText := deadline.UTC().Format("2006-01-02 15:04:05.999999999 UTC")
	return append(texts, statusTableChunks(fmt.Sprintf("Plan ready for review.\nPlan: %d create, %d change, %d destroy\nPlanned resources:", create, change, destroy), []string{"Resource", "Action"}, rows, "\nReply with exact `yes` in this thread before "+deadlineText+" to approve or `no` to reject the configuration.")...)
}

func reviewConfigRows(options *servitorv1alpha1.ResolvedOptions) [][]string {
	if options == nil {
		return [][]string{{"Configuration:", "-"}}
	}
	location := options.Region
	switch options.Provider {
	case "vpc-gen2":
		if options.Zone != "" {
			location += "/" + options.Zone
		}
	case "classic":
		location = options.Datacenter
	case "satellite":
		location = strings.Join(options.SatelliteZones, ", ")
	}
	shape := options.Flavor
	if shape == "" {
		shape = options.MachineType
	}
	if shape == "" {
		shape = options.SatelliteHostProfile
	}
	worker := shape
	if options.WorkerCount > 0 && shape != "" {
		worker = fmt.Sprintf("%d x %s", options.WorkerCount, shape)
	}
	return [][]string{
		{"Name:", options.ClusterName},
		{"Target:", options.Target},
		{"Platform:", platformLabel(options.Platform) + " " + options.Version},
		{"Provider:", providerLabel(options.Provider)},
		{"Location:", location},
		{"Resource group:", options.ResourceGroup},
		{"Worker:", worker},
		{"Network:", networkDescription(options)},
	}
}

func platformLabel(platform string) string {
	if platform == "openshift" {
		return "OpenShift"
	}
	if platform == "kubernetes" {
		return "Kubernetes"
	}
	return "Platform"
}

func providerLabel(provider string) string {
	switch provider {
	case "vpc-gen2":
		return "VPC Gen 2"
	case "classic":
		return "Classic"
	case "satellite":
		return "Satellite"
	default:
		return "Provider"
	}
}

func networkDescription(options *servitorv1alpha1.ResolvedOptions) string {
	switch options.Provider {
	case "classic":
		if options.PublicVLANID != "" || options.PrivateVLANID != "" {
			return "reuse Classic VLANs"
		}
		return "Classic networking"
	case "satellite":
		if options.SatelliteLocationID != "" {
			return "reuse Satellite location " + options.SatelliteLocationID
		}
		return "Satellite networking"
	default:
		parts := make([]string, 0, 3)
		if options.VPCID != "" {
			parts = append(parts, "reuse VPC "+options.VPCID)
		} else {
			parts = append(parts, "create VPC")
		}
		if len(options.SubnetIDs) > 0 {
			parts = append(parts, "reuse subnet")
		} else {
			parts = append(parts, "create subnet")
		}
		if len(options.PublicGatewayIDs) > 0 {
			parts = append(parts, "reuse gateway")
		} else {
			parts = append(parts, "create gateway")
		}
		return strings.Join(parts, "; ")
	}
}

func actionTotals(rows [][]string) (create, change, destroy int) {
	for _, row := range rows {
		for _, action := range strings.Split(row[1], "/") {
			switch action {
			case "create":
				create++
			case "update":
				change++
			case "delete":
				destroy++
			}
		}
	}
	return create, change, destroy
}

func extensionNoticeID(uid string, extension *servitorv1alpha1.LeaseExtensionStatus) string {
	return "extension:" + uid + ":" + extension.RequestedExpiry.UTC().Format(time.RFC3339Nano)
}

func extensionOutcomeText(extension *servitorv1alpha1.LeaseExtensionStatus, now time.Time) string {
	switch extension.Outcome {
	case servitorv1alpha1.ExtensionOutcomeApplied:
		if extension.PreviousExpiry == nil || extension.NewExpiry == nil {
			return ""
		}
		return extensionNoticeText(extension, now)
	case servitorv1alpha1.ExtensionOutcomeNotReady:
		return "Lease extension was not applied because the cluster is not ready. Extend is available when your cluster is ready."
	case servitorv1alpha1.ExtensionOutcomeExpired:
		return "Lease extension was not applied because the lease expired. Cleanup will begin."
	case servitorv1alpha1.ExtensionOutcomeInvalid:
		return "Lease extension was not applied because the requested duration is invalid. Use extend [N[h]] when your cluster is ready."
	default:
		return ""
	}
}

func extensionNoticeText(extension *servitorv1alpha1.LeaseExtensionStatus, now time.Time) string {
	return fmt.Sprintf("Lease extended.\n\n```\nPrevious expiry: %s\nNew expiry:      %s\nAdded:           %s\n```\nRemaining lease time is capped at 24 hours.", lifecycle.FormatLeaseExpiry(extension.PreviousExpiry.Time, now), lifecycle.FormatLeaseExpiry(extension.NewExpiry.Time, now), formatAddedDuration(time.Duration(extension.AddedSeconds)*time.Second))
}

func formatAddedDuration(added time.Duration) string {
	switch {
	case added <= 0:
		return "-"
	case added < time.Hour:
		return "<1h"
	case added%time.Hour == 0:
		return fmt.Sprintf("%dh", added/time.Hour)
	default:
		return fmt.Sprintf("~%dh", added.Round(time.Hour)/time.Hour)
	}
}

func readyNoticeTexts(ready *servitorv1alpha1.ReadySummary, expiry, now time.Time) []string {
	created, reused := make([][]string, 0), make([][]string, 0)
	if ready != nil {
		for _, resource := range ready.Resources {
			row := []string{resource.Role, resource.Name, resource.ID}
			if resource.Reused {
				reused = append(reused, row)
			} else {
				created = append(created, row)
			}
		}
	}
	header := []string{"Resource", "Name", "ID"}
	texts := statusTableChunks("Your cluster is ready.\n\nCreated", header, created, "")
	conclusion := "\nThis lease will expire at " + lifecycle.FormatLeaseExpiry(expiry, now) + ".\nReply with `done` in this thread or send `@servitor done` in the configured channel to free up your resources sooner."
	return append(texts, statusTableChunks("Reused", header, reused, conclusion)...)
}

func statusTableChunks(title string, header []string, rows [][]string, conclusion string) []string {
	if len(rows) == 0 {
		return []string{title + "\n" + statusTable(header, nil) + conclusion}
	}
	chunks := make([]string, 0, 1)
	current := make([][]string, 0, len(rows))
	for _, row := range rows {
		candidate := append(append([][]string(nil), current...), row)
		text := title + "\n" + statusTable(header, candidate)
		if len(current) > 0 && len(text)+len(conclusion) > maxSlackMessage {
			chunks = append(chunks, title+"\n"+statusTable(header, current))
			current = current[:0]
		}
		current = append(current, row)
	}
	return append(chunks, title+"\n"+statusTable(header, current)+conclusion)
}

func statusTable(header []string, rows [][]string) string {
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 4, 2, ' ', 0)
	if len(header) > 0 {
		writeListRow(writer, header)
	}
	for _, row := range rows {
		safeRow := make([]string, len(row))
		for index, cell := range row {
			safeRow[index] = listSafeCell(cell)
		}
		writeListRow(writer, safeRow)
	}
	_ = writer.Flush()
	return "```\n" + strings.TrimSuffix(buffer.String(), "\n") + "\n```"
}

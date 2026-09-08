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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StatusNotifier turns controller-owned status transitions into sanitized Slack
// notices. A receipt is written after delivery, so a crash may duplicate a
// notice but never suppresses an unsent one or changes cloud intent.
type StatusNotifier struct {
	Client    client.Client
	Namespace string
	Responder Responder
	Receipts  *state.EventStore
	Interval  time.Duration
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
		for _, notice := range statusNotices(cluster) {
			seen, err := n.Receipts.Seen(ctx, notice.id)
			if err != nil {
				return err
			}
			if seen {
				continue
			}
			if err := n.Responder.Reply(ctx, Response{Channel: cluster.Spec.Slack.ChannelID, ThreadTimestamp: cluster.Spec.Slack.ThreadTimestamp, Text: notice.text}); err != nil {
				continue
			}
			if _, err := n.Receipts.Claim(ctx, notice.id); err != nil {
				return err
			}
		}
	}
	return nil
}

type statusNotice struct{ id, text string }

func statusNotices(cluster *servitorv1alpha1.ServitorCluster) []statusNotice {
	if cluster.Spec.Slack.ChannelID == "" || cluster.Spec.Slack.ThreadTimestamp == "" {
		return nil
	}
	uid := string(cluster.UID)
	if uid == "" {
		uid = cluster.Namespace + "/" + cluster.Name
	}
	phase := cluster.Status.Phase
	var texts []string
	switch phase {
	case servitorv1alpha1.PhasePlanning:
		texts = []string{"Planning..."}
	case servitorv1alpha1.PhaseAwaitingApproval:
		texts = reviewNoticeTexts(cluster.Status.Review)
	case servitorv1alpha1.PhaseApplying:
		texts = []string{"Applying the approved configuration..."}
	case servitorv1alpha1.PhaseReady:
		expiry := time.Time{}
		if cluster.Status.LeaseExpiresAt != nil {
			expiry = cluster.Status.LeaseExpiresAt.Time
		}
		texts = readyNoticeTexts(cluster.Status.Ready, expiry, time.Now().UTC())
	case servitorv1alpha1.PhaseCleanupPending:
		texts = []string{"Cleaning up..."}
	case servitorv1alpha1.PhaseCleanupComplete:
		texts = []string{"Cleanup complete."}
	case servitorv1alpha1.PhaseUnresolved:
		texts = []string{"Cleanup is unresolved. An administrator must inspect the allocation CR status and private cluster logs."}
	}
	notices := phaseNotices(uid, phase, texts)
	if cleanup := cluster.Status.Cleanup; cleanup != nil && cleanup.RetryCount > 0 {
		notices = append(notices, statusNotice{id: fmt.Sprintf("cleanup-retry:%s:%d", uid, cleanup.RetryCount), text: fmt.Sprintf("Cleanup retry %d is scheduled.", cleanup.RetryCount)})
	}
	if extension := cluster.Status.LeaseExtension; extension != nil && extension.Outcome == servitorv1alpha1.ExtensionOutcomeApplied && extension.NewExpiry != nil {
		notices = append(notices, statusNotice{id: "extension:" + uid + ":" + extension.RequestedExpiry.UTC().Format(time.RFC3339Nano), text: "Lease extended. New expiry: " + lifecycle.FormatLeaseExpiry(extension.NewExpiry.Time, time.Now().UTC()) + "."})
	}
	return notices
}

func phaseNotices(uid, phase string, texts []string) []statusNotice {
	notices := make([]statusNotice, 0, len(texts))
	for index, text := range texts {
		notices = append(notices, statusNotice{id: fmt.Sprintf("phase:%s:%s:%d", uid, phase, index), text: text})
	}
	return notices
}

func reviewNoticeTexts(review *servitorv1alpha1.ReviewSummary) []string {
	rows := make([][]string, 0)
	if review != nil {
		rows = make([][]string, 0, len(review.Resources))
		for _, resource := range review.Resources {
			rows = append(rows, []string{resource.Role, strings.Join(resource.Actions, "/")})
		}
	}
	return statusTableChunks("Plan ready for review.\nPlanned resources:", []string{"Resource", "Action"}, rows, "\nReply with exact `yes` in this thread to approve or `no` to reject the configuration.")
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
	writeListRow(writer, header)
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

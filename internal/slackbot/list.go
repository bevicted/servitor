package slackbot

import (
	"bytes"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	servitorv1alpha1 "github.com/bevicted/servitor/api/v1alpha1"
	"github.com/bevicted/servitor/internal/lifecycle"
)

const (
	listSafeCellLimit = 160
	listLegend        = "`*` marks your allocations."
)

var listStatuses = map[string]string{
	servitorv1alpha1.PhasePending: "planning", servitorv1alpha1.PhasePlanning: "planning", servitorv1alpha1.PhaseAwaitingApproval: "review", servitorv1alpha1.PhaseApplying: "applying", servitorv1alpha1.PhaseReady: "ready", servitorv1alpha1.PhaseCleanupPending: "cleanup in progress", servitorv1alpha1.PhaseCleanupComplete: "cleanup complete", servitorv1alpha1.PhaseUnresolved: "unresolved",
}

type clusterListRow struct {
	marker, cluster, status, location, expires string
	owner                                      string
	updated                                    time.Time
	hasExpiry                                  bool
}

// clusterListMessages renders only status data held by namespaced CRs. Slack
// owner identities remain a caller marker and are never resolved or displayed.
func clusterListMessages(clusters []servitorv1alpha1.ServitorCluster, caller string, now time.Time) []string {
	owners := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		owners = append(owners, cluster.Spec.Slack.OwnerID)
	}
	rows := make([]clusterListRow, 0, len(clusters))
	for _, cluster := range clusters {
		expiry := time.Time{}
		if cluster.Status.LeaseExpiresAt != nil {
			expiry = cluster.Status.LeaseExpiresAt.Time
		}
		options := cluster.Status.ResolvedOptions
		name, location := "", ""
		if options != nil {
			name = options.ClusterName
			location = options.Region
		}
		rows = append(rows, clusterListRow{marker: listMarker(cluster.Spec.Slack.OwnerID, caller), cluster: listClusterCell(name, owners), status: listStatusCell(cluster.Status.Phase), location: listLocationCell(location, owners), expires: listExpiry(expiry, now), owner: cluster.Spec.Slack.OwnerID, updated: cluster.CreationTimestamp.Time, hasExpiry: !expiry.IsZero()})
	}
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		if left.hasExpiry != right.hasExpiry {
			return left.hasExpiry
		}
		if left.expires != right.expires {
			return left.expires < right.expires
		}
		if left.status != right.status {
			return left.status < right.status
		}
		if !left.updated.Equal(right.updated) {
			return left.updated.Before(right.updated)
		}
		return left.owner < right.owner
	})
	chunks := make([]string, 0, len(rows)/8+1)
	current := make([]clusterListRow, 0, len(rows))
	for _, row := range rows {
		candidate := append(append([]clusterListRow(nil), current...), row)
		if len(current) > 0 && len(renderClusterList(candidate)) > maxSlackMessage {
			chunks = append(chunks, renderClusterList(current))
			current = current[:0]
		}
		current = append(current, row)
	}
	return append(chunks, renderClusterList(current))
}
func renderClusterList(rows []clusterListRow) string {
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 4, 2, ' ', 0)
	writeListRow(writer, []string{"", "cluster", "state", "location", "expires"})
	for _, row := range rows {
		writeListRow(writer, []string{row.marker, row.cluster, row.status, row.location, row.expires})
	}
	_ = writer.Flush()
	return listLegend + "\n```\n" + strings.TrimSuffix(buffer.String(), "\n") + "\n```"
}
func writeListRow(writer *tabwriter.Writer, row []string) {
	for i, value := range row {
		if i != 0 {
			_, _ = writer.Write([]byte{'\t'})
		}
		_, _ = writer.Write([]byte(value))
	}
	_, _ = writer.Write([]byte{'\n'})
}
func listMarker(owner, caller string) string {
	if owner == caller {
		return "*"
	}
	return " "
}
func listClusterCell(value string, owners []string) string {
	value = listSafeCell(value)
	if value == "-" || strings.ToLower(value) != value || strings.ContainsAny(value, "/._") || containsPrivateID(value, owners) {
		return "-"
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-') {
			return "-"
		}
	}
	return value
}
func listLocationCell(value string, owners []string) string {
	value = listSafeCell(value)
	if value == "-" || strings.HasPrefix(value, "/") || strings.Contains(value, "..") || strings.Contains(value, ".") || containsPrivateID(value, owners) {
		return "-"
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' || character == '/' || character == ',') {
			return "-"
		}
	}
	return value
}
func listStatusCell(value string) string {
	if status := listStatuses[value]; status != "" {
		return status
	}
	return "-"
}
func listExpiry(value, now time.Time) string {
	if value.IsZero() {
		return "never"
	}
	return lifecycle.FormatLeaseExpiry(value, now)
}
func listSafeCell(value string) string {
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) || character == '`' || character == '@' || character == '<' || character == '>' {
			return ' '
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return "-"
	}
	if len([]rune(value)) > listSafeCellLimit {
		return string([]rune(value)[:listSafeCellLimit-3]) + "..."
	}
	return value
}
func containsPrivateID(value string, owners []string) bool {
	for _, owner := range owners {
		if owner != "" && strings.Contains(value, owner) {
			return true
		}
	}
	return false
}

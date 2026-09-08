package slackbot

import (
	"bytes"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/bevicted/servitor/internal/lifecycle"
	"github.com/bevicted/servitor/internal/state"
)

const listSafeCellLimit = 160

var listStatuses = map[string]bool{
	"review": true, "applying": true, "ready": true, "cleanup": true, "unresolved": true,
}

type lifecycleListRow struct {
	marker, cluster, status, location, expires string
	record                                     state.LifecycleRecord
}

func lifecycleListMessages(records []state.LifecycleRecord, caller string, now time.Time) []string {
	userIDs := make([]string, 0, len(records))
	for _, record := range records {
		userIDs = append(userIDs, record.UserID)
	}
	rows := make([]lifecycleListRow, 0, len(records))
	for _, record := range records {
		rows = append(rows, lifecycleListRow{
			marker:   listMarker(record.UserID, caller),
			cluster:  listClusterCell(record.ClusterName, userIDs),
			status:   listStatusCell(record.Status),
			location: listLocationCell(record.Location, userIDs),
			expires:  listExpiry(record.LeaseExpiresAt, now),
			record:   record,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i].record, rows[j].record
		leftExpires, rightExpires := !left.LeaseExpiresAt.IsZero(), !right.LeaseExpiresAt.IsZero()
		if leftExpires != rightExpires {
			return leftExpires
		}
		if leftExpires && !left.LeaseExpiresAt.Equal(right.LeaseExpiresAt) {
			return left.LeaseExpiresAt.Before(right.LeaseExpiresAt)
		}
		if left.Status != right.Status {
			return left.Status < right.Status
		}
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.Before(right.UpdatedAt)
		}
		return left.UserID < right.UserID
	})

	chunks := make([]string, 0, len(rows)/8+1)
	current := make([]lifecycleListRow, 0, len(rows))
	for _, row := range rows {
		candidate := append(append([]lifecycleListRow(nil), current...), row)
		if len(current) > 0 && len(renderLifecycleList(candidate)) > maxSlackMessage {
			chunks = append(chunks, renderLifecycleList(current))
			current = current[:0]
		}
		current = append(current, row)
	}
	return append(chunks, renderLifecycleList(current))
}

func renderLifecycleList(rows []lifecycleListRow) string {
	var buffer bytes.Buffer
	writer := tabwriter.NewWriter(&buffer, 0, 4, 2, ' ', 0)
	writeLifecycleListRow(writer, []string{"", "cluster", "state", "location", "expires"})
	for _, row := range rows {
		writeLifecycleListRow(writer, []string{row.marker, row.cluster, row.status, row.location, row.expires})
	}
	_ = writer.Flush()
	return "```\n" + strings.TrimSuffix(buffer.String(), "\n") + "\n```"
}

func writeLifecycleListRow(writer *tabwriter.Writer, row []string) {
	for index, value := range row {
		if index != 0 {
			_, _ = writer.Write([]byte{'\t'})
		}
		_, _ = writer.Write([]byte(value))
	}
	_, _ = writer.Write([]byte{'\n'})
}

func listMarker(userID, caller string) string {
	if userID == caller {
		return "*"
	}
	return " "
}

func listClusterCell(value string, userIDs []string) string {
	value = listSafeCell(value)
	if value == "-" || strings.ToLower(value) != value || strings.ContainsAny(value, "/._") || containsPrivateID(value, userIDs) {
		return "-"
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-') {
			return "-"
		}
	}
	return value
}

func listLocationCell(value string, userIDs []string) string {
	value = listSafeCell(value)
	if value == "-" || strings.HasPrefix(value, "/") || strings.Contains(value, "..") || strings.Contains(value, ".") || containsPrivateID(value, userIDs) {
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
	if !listStatuses[value] {
		return "-"
	}
	return value
}

func listExpiry(value, now time.Time) string {
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

func containsPrivateID(value string, userIDs []string) bool {
	for _, userID := range userIDs {
		if userID != "" && strings.Contains(value, userID) {
			return true
		}
	}
	return false
}

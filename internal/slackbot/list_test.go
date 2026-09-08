package slackbot

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bevicted/servitor/internal/state"
)

func TestLifecycleListRendersSortedSafeInventory(t *testing.T) {
	expires := time.Date(2026, 9, 7, 15, 29, 13, 0, time.UTC)
	records := []state.LifecycleRecord{
		{UserID: "Ucaller", Status: "ready", ClusterName: "servitor-later", Location: "us-south/us-south-1", LeaseExpiresAt: expires.Add(time.Hour), UpdatedAt: expires},
		{UserID: "Upeer", Status: "ready", ClusterName: "servitor-earlier", Location: "us-east/us-east-1", LeaseExpiresAt: expires, UpdatedAt: expires},
		{UserID: "Uthree", Status: "applying", ClusterName: "servitor-applying", Location: "us-south/us-south-2", UpdatedAt: expires.Add(2 * time.Hour)},
		{UserID: "Ufour", Status: "cleanup", UpdatedAt: expires.Add(time.Hour)},
		{UserID: "Ufive", Status: "review", UpdatedAt: expires},
		{UserID: "Usix", Status: "unresolved", UpdatedAt: expires},
	}

	now := expires.Add(-31 * time.Minute)
	messages := lifecycleListMessages(records, "Ucaller", now)
	if len(messages) != 1 {
		t.Fatalf("messages = %d, want one", len(messages))
	}
	text := messages[0]
	for _, want := range []string{"cluster", "state", "location", "expires", "servitor-earlier", "servitor-later", "servitor-applying", "2026-09-07 15:29:13 UTC (~1h)", "2026-09-07 16:29:13 UTC (~2h)"} {
		if !strings.Contains(text, want) {
			t.Errorf("list does not contain %q:\n%s", want, text)
		}
	}
	var expiryColumns []int
	for _, line := range strings.Split(text, "\n") {
		if column := strings.Index(line, "2026-09-07"); column >= 0 {
			expiryColumns = append(expiryColumns, column)
		}
	}
	if len(expiryColumns) != 2 || expiryColumns[0] != expiryColumns[1] {
		t.Errorf("expiry cells are not aligned: columns=%v\n%s", expiryColumns, text)
	}
	for _, forbidden := range []string{"Ucaller", "Upeer", "Uthree", "Ufour", "Ufive", "Usix"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("list leaked Slack ID %q:\n%s", forbidden, text)
		}
	}
	if strings.Count(text, "*") != 1 {
		t.Errorf("caller marker count = %d, want one:\n%s", strings.Count(text, "*"), text)
	}
	ordered := []string{"servitor-earlier", "servitor-later", "servitor-applying", "cleanup", "review", "unresolved"}
	previous := -1
	for _, value := range ordered {
		position := strings.Index(text, value)
		if position <= previous {
			t.Errorf("list order is not expiry then state/update order for %q:\n%s", value, text)
		}
		previous = position
	}
}

func TestLifecycleListEqualExpiryUsesStableStateUpdateOrder(t *testing.T) {
	expires := time.Date(2026, 9, 7, 15, 29, 13, 0, time.UTC)
	messages := lifecycleListMessages([]state.LifecycleRecord{
		{UserID: "Ulater", Status: "ready", ClusterName: "servitor-later-update", LeaseExpiresAt: expires, UpdatedAt: expires.Add(time.Hour)},
		{UserID: "Uearlier", Status: "ready", ClusterName: "servitor-earlier-update", LeaseExpiresAt: expires, UpdatedAt: expires},
	}, "Ucaller", expires.Add(-31*time.Minute))
	text := strings.Join(messages, "\n")
	if strings.Index(text, "servitor-earlier-update") > strings.Index(text, "servitor-later-update") {
		t.Fatalf("equal expiry rows were not ordered by update time:\n%s", text)
	}
}

func TestLifecycleListSanitizesUnsafeCellsAndPrivateValues(t *testing.T) {
	records := []state.LifecycleRecord{{
		UserID:      "Ucaller",
		Status:      "ready",
		ClusterName: "Ucaller /private/ict/module.cluster\n<@Upeer>```",
		Location:    "/private/ict/Upeer\tmodule.cluster",
	}}
	text := strings.Join(lifecycleListMessages(records, "Ucaller", time.Time{}), "\n")
	for _, forbidden := range []string{"Ucaller", "Upeer", "/private", "module.cluster", "<@", "```\nUcaller"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("list leaked unsafe value %q:\n%s", forbidden, text)
		}
	}
	var row []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "*") {
			row = strings.Fields(line)
			break
		}
	}
	if got, want := strings.Join(row, ","), "*,-,ready,-,-"; got != want {
		t.Errorf("unsafe cells were not replaced with unavailable values: got %q, want %q", got, want)
	}
}

func TestLifecycleListSplitsAtRowsWithRepeatedHeaders(t *testing.T) {
	records := make([]state.LifecycleRecord, 0, 100)
	for index := 0; index < 100; index++ {
		records = append(records, state.LifecycleRecord{
			UserID:         fmt.Sprintf("U%03d", index),
			Status:         "ready",
			ClusterName:    fmt.Sprintf("servitor-%03d", index),
			Location:       "us-south/us-south-1",
			LeaseExpiresAt: time.Date(2026, 9, 7, 4, 0, 0, 0, time.UTC),
			UpdatedAt:      time.Date(2026, 9, 7, 0, index, 0, 0, time.UTC),
		})
	}
	messages := lifecycleListMessages(records, "U042", time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC))
	if len(messages) < 2 {
		t.Fatalf("messages = %d, want multiple bounded messages", len(messages))
	}
	joined := strings.Join(messages, "\n")
	if strings.Count(joined, "*") != 1 {
		t.Errorf("caller marker count = %d, want one", strings.Count(joined, "*"))
	}
	for index, message := range messages {
		if len(message) > maxSlackMessage || !utf8.ValidString(message) || strings.Count(message, "```") != 2 || !strings.Contains(message, "cluster") || !strings.Contains(message, "2026-09-07 04:00:00 UTC (~4h)") {
			t.Errorf("chunk %d is not a bounded table with a repeated header:\n%s", index, message)
		}
	}
	for index := range records {
		cluster := fmt.Sprintf("servitor-%03d", index)
		if strings.Count(joined, cluster) != 1 {
			t.Errorf("row %q was split or duplicated", cluster)
		}
	}
}

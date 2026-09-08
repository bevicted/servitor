package slackbot

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFixtureAcknowledgementFailurePreventsDelivery(t *testing.T) {
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "fixture.json")
	outputPath := filepath.Join(directory, "result.json")
	contents, err := json.Marshal(SocketModeFixture{
		SelfUserID:              "B1",
		AcknowledgementFailures: 1,
		Events: []FixtureEvent{{
			ID:      "Ev1",
			Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "help"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	transport, err := NewFixtureSocketMode(inputPath, outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Run(context.Background(), Bot{}.Handle); err == nil || !strings.Contains(err.Error(), "acknowledge Slack envelope: fixture Slack acknowledgement failure") {
		t.Fatalf("Run error = %v, want acknowledgement failure", err)
	}
	if got := len(transport.result.Acknowledgements); got != 0 {
		t.Errorf("successful acknowledgements = %d, want 0", got)
	}
}

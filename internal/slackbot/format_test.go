package slackbot

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBoundedMessagesPreservesUTF8AndBalancedFences(t *testing.T) {
	input := "Table\n```\n" + strings.Repeat("row 😀\n", 800) + "```\n"
	chunks := boundedMessages(input)
	if len(chunks) < 2 {
		t.Fatalf("chunks = %d, want split output", len(chunks))
	}
	for index, chunk := range chunks {
		if len(chunk) > maxSlackMessage || !utf8.ValidString(chunk) || strings.Count(chunk, "```")%2 != 0 {
			t.Fatalf("chunk %d is not bounded UTF-8 with balanced fences", index)
		}
	}
}

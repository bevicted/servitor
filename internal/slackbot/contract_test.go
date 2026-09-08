package slackbot

import (
	"context"
	"strings"
	"testing"
)

func TestConfiguredRootAndThreadRouting(t *testing.T) {
	bot, responses := botForTest(t)
	for _, text := range []string{"help", "<@OTHER> help", "<@BOT>create"} {
		if err := bot.Handle(context.Background(), Envelope{ID: text, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: text, Timestamp: "1"}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 0 {
		t.Fatalf("unmentioned responses=%+v", responses.responses)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "mentioned", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> help", Timestamp: "1"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || !strings.Contains(responses.responses[0].Text, "Commands") {
		t.Fatalf("responses=%+v", responses.responses)
	}
}

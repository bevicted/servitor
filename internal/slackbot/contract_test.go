package slackbot

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/lifecycle"
)

func TestConfiguredRootMentionAndExactCommandContract(t *testing.T) {
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", SelfUserID: "B1", Events: &memoryEvents{}, ICT: &fakeICT{}, Responder: responses}
	for index, text := range []string{"help", "<@B2> help", "help <@B1>", "<@B1>create", "<@B1> <@B1> help"} {
		if err := bot.Handle(context.Background(), Envelope{ID: "silent-" + string(rune('a'+index)), Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: text, Timestamp: "1"}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 1 || !strings.HasPrefix(responses.responses[0].Text, "Command unknown.") {
		t.Fatalf("responses = %+v, want only duplicate-mention unknown response", responses.responses)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "mentioned-help", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: " \t<@B1>\nhelp  ", Timestamp: "2"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 2 || !strings.Contains(responses.responses[1].Text, "Servitor provisions") {
		t.Fatalf("mentioned help = %+v", responses.responses)
	}
	for range 2 {
		if err := bot.Handle(context.Background(), Envelope{ID: "duplicate", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> help", Timestamp: "3"}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 3 {
		t.Fatalf("duplicate event produced responses = %+v", responses.responses)
	}
	for _, text := range []string{"<@B1>", "<@B1> createfoo"} {
		if err := bot.Handle(context.Background(), Envelope{ID: text, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: text, Timestamp: "4"}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, response := range responses.responses[3:] {
		if !strings.HasPrefix(response.Text, "Command unknown.\n\n") {
			t.Fatalf("unknown command response = %q", response.Text)
		}
	}
}

func TestDMUsesExactFirstCommandToken(t *testing.T) {
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", Events: &memoryEvents{}, ICT: &fakeICT{}, Responder: responses}
	for _, text := range []string{"create", "createfoo"} {
		if err := bot.Handle(context.Background(), Envelope{ID: "dm-" + text, Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: text}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 2 || !strings.HasPrefix(responses.responses[0].Text, "Command rejected.") || !strings.HasPrefix(responses.responses[1].Text, "Command unknown.\n\n") {
		t.Fatalf("DM responses = %+v", responses.responses)
	}
}

func TestThreadBoundaryAndMentionedThreadCommands(t *testing.T) {
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", SelfUserID: "B1", Events: &memoryEvents{}, ICT: &fakeICT{}, Lifecycle: &fakeLifecycle{}, Creator: &fakeCreator{}, Responder: responses}
	for _, text := range []string{"yes", "no", "done", "destroy", "ordinary text"} {
		if err := bot.Handle(context.Background(), Envelope{ID: "unrelated-" + text, Message: Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: text, Timestamp: "reply", ThreadTimestamp: "other"}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 0 {
		t.Fatalf("unrelated thread produced responses = %+v", responses.responses)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "thread-help", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> help done", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || responses.responses[0].ThreadTimestamp != "root" || !strings.Contains(responses.responses[0].Text, "`done`") {
		t.Fatalf("mentioned thread help = %+v", responses.responses)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "thread-create", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> create", Timestamp: "reply", ThreadTimestamp: "root"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 2 || !strings.HasPrefix(responses.responses[1].Text, "Command rejected.") {
		t.Fatalf("thread create response = %+v", responses.responses)
	}
}

func TestExtendUsesLifecycleAuthorizationAndOriginalThread(t *testing.T) {
	responses := &memoryResponder{}
	manager := &fakeLifecycle{}
	bot := Bot{ChannelID: "C1", SelfUserID: "B1", Events: &memoryEvents{}, ICT: &fakeICT{}, Lifecycle: manager, Responder: responses}
	for _, message := range []Message{
		{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@B1> extend 2h", Timestamp: "root"},
		{Channel: "C1", ChannelType: "channel", User: "U1", Text: "extend", Timestamp: "reply", ThreadTimestamp: "123.456"},
		{Channel: "C1", ChannelType: "channel", User: "U2", Text: "extend 1", Timestamp: "reply", ThreadTimestamp: "other"},
	} {
		if err := bot.Handle(context.Background(), Envelope{ID: message.Timestamp + message.Text, Message: message}); err != nil {
			t.Fatal(err)
		}
	}
	if len(manager.extendRequests) != 2 || manager.extendValues[0] != 2*time.Hour || manager.extendValues[1] != 0 {
		t.Fatalf("extensions = requests:%+v values:%v", manager.extendRequests, manager.extendValues)
	}
	if len(responses.responses) != 2 {
		t.Fatalf("responses = %+v", responses.responses)
	}
	for _, response := range responses.responses {
		if response.ThreadTimestamp != "123.456" || response.Text != "Lease extended." {
			t.Fatalf("extension response = %+v", response)
		}
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "invalid", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "extend 0", Timestamp: "reply", ThreadTimestamp: "123.456"}}); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[len(responses.responses)-1]; !strings.HasPrefix(got.Text, "Command rejected.") || got.ThreadTimestamp != "123.456" {
		t.Fatalf("invalid extension response = %+v", got)
	}
}

func TestMentionedThreadCleanupRequiresPersistedOwnership(t *testing.T) {
	responses := &memoryResponder{}
	manager := &fakeLifecycle{}
	bot := Bot{ChannelID: "C1", SelfUserID: "B1", Events: &memoryEvents{}, ICT: &fakeICT{}, Lifecycle: manager, Creator: &fakeCreator{}, Responder: responses}
	for _, command := range []string{"done", "destroy"} {
		envelope := Envelope{ID: "mentioned-thread-" + command, Message: Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: "<@B1> " + command, Timestamp: "reply", ThreadTimestamp: "other"}}
		if err := bot.Handle(context.Background(), envelope); err != nil {
			t.Fatal(err)
		}
	}
	if len(manager.requests) != 0 || len(responses.responses) != 0 {
		t.Fatalf("unowned mentioned thread cleanup = requests %+v, responses %+v", manager.requests, responses.responses)
	}
}

type outcomeCreator struct {
	fakeCreator
	outcome lifecycle.ConfirmationOutcome
}

func (c *outcomeCreator) ConfirmResult(context.Context, lifecycle.Request, string, lifecycle.Notifier) (lifecycle.ConfirmationOutcome, error) {
	return c.outcome, nil
}

func TestApplyingConfirmationReportsProgressOnlyInOwnerThread(t *testing.T) {
	responses := &memoryResponder{}
	creator := &outcomeCreator{outcome: lifecycle.ConfirmationApplying}
	bot := Bot{ChannelID: "C1", SelfUserID: "B1", Events: &memoryEvents{}, ICT: &fakeICT{}, Creator: creator, Responder: responses}
	if err := bot.Handle(context.Background(), Envelope{ID: "applying", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "123.456"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || responses.responses[0].Text != "Creation is already in progress." || responses.responses[0].ThreadTimestamp != "123.456" {
		t.Fatalf("applying response = %+v", responses.responses)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "non-yes", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "no", Timestamp: "reply", ThreadTimestamp: "123.456"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 {
		t.Fatalf("non-yes applying reply produced response: %+v", responses.responses)
	}
}

func TestHelpIsSpecificAndDestroyRemainsUnadvertised(t *testing.T) {
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", SelfUserID: "B1", Events: &memoryEvents{}, ICT: &fakeICT{}, Defaults: command.CreateDefaults{Version: "1.36", Target: "synthetic-target", Provider: "vpc-gen2"}, Responder: responses}
	for _, text := range []string{"<@B1> help", "<@B1> help create", "<@B1> help done", "<@B1> help list", "<@B1> help destroy"} {
		if err := bot.Handle(context.Background(), Envelope{ID: text, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: text, Timestamp: text}}); err != nil {
			t.Fatal(err)
		}
	}
	texts := make([]string, len(responses.responses))
	for index, response := range responses.responses {
		texts[index] = response.Text
	}
	all := strings.Join(texts, "\n")
	if strings.Contains(responses.responses[0].Text, "destroy") || !strings.Contains(all, "VPC Gen 2") || !strings.Contains(all, "version 1.36") || !strings.Contains(all, "`list` shows available cluster state. Use it in a DM or as `@servitor list` in the configured channel.") || !strings.HasPrefix(responses.responses[len(responses.responses)-1].Text, "Command unknown.") {
		t.Fatalf("help responses = %q", all)
	}
}

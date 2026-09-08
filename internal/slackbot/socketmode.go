package slackbot

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

// Transport is the Socket Mode boundary used by the process wiring.
type Transport interface {
	SelfUserID(context.Context) (string, error)
	Reply(context.Context, Response) error
	Run(context.Context, func(context.Context, Envelope) error) error
}

// SocketMode runs a Slack Socket Mode connection and adapts it to Bot envelopes.
type SocketMode struct {
	client *socketmode.Client
}

// NewSocketMode creates a Socket Mode adapter from environment-supplied credentials.
func NewSocketMode(botToken, appToken string) *SocketMode {
	api := slack.New(botToken, slack.OptionAppLevelToken(appToken))
	return &SocketMode{client: socketmode.New(api)}
}

// SelfUserID identifies the authenticated bot user.
func (s *SocketMode) SelfUserID(ctx context.Context) (string, error) {
	auth, err := s.client.AuthTestContext(ctx)
	if err != nil {
		return "", fmt.Errorf("authenticate Slack bot: %w", err)
	}
	return auth.UserID, nil
}

// Reply posts a message, preserving a supplied lifecycle thread timestamp.
func (s *SocketMode) Reply(ctx context.Context, response Response) error {
	options := []slack.MsgOption{slack.MsgOptionText(response.Text, false)}
	if response.ThreadTimestamp != "" {
		options = append(options, slack.MsgOptionTS(response.ThreadTimestamp))
	}
	_, _, err := s.client.PostMessageContext(ctx, response.Channel, options...)
	return err
}

// Run delivers Events API message events until context cancellation or connection failure.
func (s *SocketMode) Run(ctx context.Context, handler func(context.Context, Envelope) error) error {
	errors := make(chan error, 1)
	go func() { errors <- s.client.RunContext(ctx) }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errors:
			return err
		case event, ok := <-s.client.Events:
			if !ok {
				return fmt.Errorf("Socket Mode event stream closed")
			}
			if event.Type != socketmode.EventTypeEventsAPI {
				continue
			}
			envelope, ok := s.envelope(event)
			if !ok {
				if event.Request != nil {
					if err := s.client.AckCtx(ctx, event.Request.EnvelopeID, nil); err != nil {
						return fmt.Errorf("acknowledge unsupported Slack event: %w", err)
					}
				}
				continue
			}
			if err := handler(ctx, envelope); err != nil {
				return err
			}
		}
	}
}

func (s *SocketMode) envelope(event socketmode.Event) (Envelope, bool) {
	apiEvent, ok := event.Data.(slackevents.EventsAPIEvent)
	if !ok || event.Request == nil || apiEvent.Type != slackevents.CallbackEvent {
		return Envelope{}, false
	}
	message, ok := apiEvent.InnerEvent.Data.(*slackevents.MessageEvent)
	if !ok {
		return Envelope{}, false
	}
	var callback struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(event.Request.Payload, &callback); err != nil || callback.EventID == "" {
		return Envelope{}, false
	}
	request := *event.Request
	return Envelope{
		ID:          callback.EventID,
		Message:     Message{Channel: message.Channel, ChannelType: message.ChannelType, User: message.User, Text: message.Text, Timestamp: message.TimeStamp, ThreadTimestamp: message.ThreadTimeStamp, Subtype: message.SubType, BotID: message.BotID},
		Acknowledge: func(ctx context.Context) error { return s.client.AckCtx(ctx, request.EnvelopeID, nil) },
	}, true
}

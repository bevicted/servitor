package slackbot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FixtureEvent is one fake Socket Mode envelope used by the binary smoke test.
type FixtureEvent struct {
	ID      string       `json:"id"`
	Message Message      `json:"message"`
	WaitFor *FixtureWait `json:"wait_for,omitempty"`
}

// FixtureWait waits for a test-controlled file before delivering the next event.
type FixtureWait struct {
	Path             string `json:"path,omitempty"`
	Contains         string `json:"contains,omitempty"`
	ResponseContains string `json:"response_contains,omitempty"`
}

// SocketModeFixture describes a finite fake Socket Mode session.
type SocketModeFixture struct {
	SelfUserID              string         `json:"self_user_id"`
	Events                  []FixtureEvent `json:"events"`
	ReplyFailures           int            `json:"reply_failures,omitempty"`
	ReplyFailureContains    string         `json:"reply_failure_contains,omitempty"`
	AcknowledgementFailures int            `json:"acknowledgement_failures,omitempty"`
	Hold                    bool           `json:"hold,omitempty"`
	HoldPath                string         `json:"hold_path,omitempty"`
}

// FixtureResult records the fake transport's acknowledgements and responses.
type FixtureResult struct {
	Acknowledgements []string   `json:"acknowledgements"`
	Responses        []Response `json:"responses"`
}

// FixtureSocketMode is a finite file-backed transport for binary smoke tests.
type FixtureSocketMode struct {
	fixture SocketModeFixture
	output  string

	mu                      sync.Mutex
	result                  FixtureResult
	replyFailures           int
	acknowledgementFailures int
}

// NewFixtureSocketMode loads a finite Socket Mode test fixture.
func NewFixtureSocketMode(inputPath, outputPath string) (*FixtureSocketMode, error) {
	contents, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, fmt.Errorf("read Socket Mode fixture: %w", err)
	}
	var fixture SocketModeFixture
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		return nil, fmt.Errorf("decode Socket Mode fixture: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode Socket Mode fixture: contains more than one JSON value")
		}
		return nil, fmt.Errorf("decode Socket Mode fixture: %w", err)
	}
	if fixture.SelfUserID == "" {
		return nil, fmt.Errorf("Socket Mode fixture: self_user_id is required")
	}
	for index, event := range fixture.Events {
		if event.ID == "" {
			return nil, fmt.Errorf("Socket Mode fixture: events[%d].id is required", index)
		}
		if event.WaitFor != nil && event.WaitFor.Path == "" && event.WaitFor.ResponseContains == "" {
			return nil, fmt.Errorf("Socket Mode fixture: events[%d].wait_for requires path or response_contains", index)
		}
	}
	if fixture.ReplyFailures < 0 {
		return nil, errors.New("Socket Mode fixture: reply_failures must not be negative")
	}
	if fixture.AcknowledgementFailures < 0 {
		return nil, errors.New("Socket Mode fixture: acknowledgement_failures must not be negative")
	}
	if fixture.Hold && fixture.HoldPath == "" {
		return nil, errors.New("Socket Mode fixture: hold_path is required when hold is enabled")
	}
	return &FixtureSocketMode{
		fixture:                 fixture,
		output:                  outputPath,
		replyFailures:           fixture.ReplyFailures,
		acknowledgementFailures: fixture.AcknowledgementFailures,
	}, nil
}

// SelfUserID returns the fake authenticated Slack user.
func (s *FixtureSocketMode) SelfUserID(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return s.fixture.SelfUserID, nil
}

// Reply records a fake Slack response.
func (s *FixtureSocketMode) Reply(ctx context.Context, response Response) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.replyFailures > 0 {
		s.replyFailures--
		s.mu.Unlock()
		return errors.New("fixture Slack reply failure")
	}
	if s.fixture.ReplyFailureContains != "" && strings.Contains(response.Text, s.fixture.ReplyFailureContains) {
		s.mu.Unlock()
		return errors.New("fixture Slack reply failure")
	}
	s.result.Responses = append(s.result.Responses, response)
	s.mu.Unlock()
	return s.writeResult()
}

// Run delivers each fixture event and writes the observed result before returning.
func (s *FixtureSocketMode) Run(ctx context.Context, handler func(context.Context, Envelope) error) error {
	for _, event := range s.fixture.Events {
		if err := ctx.Err(); err != nil {
			return err
		}
		event := event
		envelope := Envelope{
			ID:      event.ID,
			Message: event.Message,
			Acknowledge: func(context.Context) error {
				s.mu.Lock()
				defer s.mu.Unlock()
				if s.acknowledgementFailures > 0 {
					s.acknowledgementFailures--
					return errors.New("fixture Slack acknowledgement failure")
				}
				s.result.Acknowledgements = append(s.result.Acknowledgements, event.ID)
				return nil
			},
		}
		if err := handler(ctx, envelope); err != nil {
			return err
		}
		if event.WaitFor != nil {
			if err := s.waitFor(ctx, *event.WaitFor); err != nil {
				return err
			}
		}
	}
	if s.fixture.Hold {
		if err := os.WriteFile(s.fixture.HoldPath, nil, 0o600); err != nil {
			return fmt.Errorf("write Socket Mode fixture hold marker: %w", err)
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return s.writeResult()
}

func (s *FixtureSocketMode) waitFor(ctx context.Context, wait FixtureWait) error {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		fileReady := wait.Path == ""
		if !fileReady {
			contents, err := os.ReadFile(wait.Path)
			fileReady = err == nil && strings.Contains(string(contents), wait.Contains)
		}
		s.mu.Lock()
		responses := append([]Response(nil), s.result.Responses...)
		s.mu.Unlock()
		responseReady := wait.ResponseContains == ""
		for _, response := range responses {
			responseReady = responseReady || strings.Contains(response.Text, wait.ResponseContains)
		}
		if fileReady && responseReady {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("Socket Mode fixture: wait_for %q timed out", wait.Path)
		case <-ticker.C:
		}
	}
}

func (s *FixtureSocketMode) writeResult() error {
	s.mu.Lock()
	contents, err := json.Marshal(s.result)
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("encode Socket Mode fixture result: %w", err)
	}
	directory := filepath.Dir(s.output)
	temporary, err := os.CreateTemp(directory, ".servitor-socket-mode-")
	if err != nil {
		return fmt.Errorf("create Socket Mode fixture result: %w", err)
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("protect Socket Mode fixture result: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("write Socket Mode fixture result: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Socket Mode fixture result: %w", err)
	}
	if err := os.Rename(name, s.output); err != nil {
		return fmt.Errorf("replace Socket Mode fixture result: %w", err)
	}
	if err := os.Chmod(s.output, 0o600); err != nil {
		return fmt.Errorf("protect Socket Mode fixture result: %w", err)
	}
	return nil
}

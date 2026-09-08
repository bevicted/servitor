package slackbot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/admission"
	"github.com/bevicted/servitor/internal/diagnostics"
	"github.com/bevicted/servitor/internal/lifecycle"
	"github.com/bevicted/servitor/internal/state"
)

type memoryEvents struct{ ids map[string]bool }

func (s *memoryEvents) Claim(id string) (bool, error) {
	if s.ids == nil {
		s.ids = map[string]bool{}
	}
	if s.ids[id] {
		return false, nil
	}
	s.ids[id] = true
	return true, nil
}

type fakeICT struct {
	output       string
	err          error
	calls        int
	stateID      string
	diagnosticID string
	inventory    state.WorkspaceInventory
}

func (f *fakeICT) WorkspaceInventoryDiagnostic(_ context.Context, stateID, diagnosticID string) (state.WorkspaceInventory, error) {
	f.calls++
	f.stateID = stateID
	f.diagnosticID = diagnosticID
	return f.inventory, f.err
}

func (f *fakeICT) ListDiagnostic(_ context.Context, stateID, diagnosticID string) (string, error) {
	f.calls++
	f.stateID = stateID
	f.diagnosticID = diagnosticID
	return f.output, f.err
}

type memoryResponder struct{ responses []Response }

func (r *memoryResponder) Reply(_ context.Context, response Response) error {
	r.responses = append(r.responses, response)
	return nil
}

type fakeLifecycle struct {
	requests       []lifecycle.Request
	extendRequests []lifecycle.Request
	extendValues   []time.Duration
	err            error
	extendErr      error
}

type fakeCreator struct {
	confirmations  []string
	requests       []lifecycle.Request
	startErr       error
	confirmErr     error
	confirmOutcome lifecycle.ConfirmationOutcome
}

type pausingCreator struct {
	fakeCreator
	mu       sync.Mutex
	declines int
	done     chan<- struct{}
}

func (f *pausingCreator) DeclinePendingReviews(lifecycle.Notifier) {
	f.mu.Lock()
	f.declines++
	f.mu.Unlock()
	if f.done != nil {
		f.done <- struct{}{}
	}
}

func (f *pausingCreator) declineCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.declines
}

type blockingDeclineCreator struct {
	fakeCreator
	started chan<- struct{}
	release <-chan struct{}
}

func (f *blockingDeclineCreator) DeclinePendingReviews(lifecycle.Notifier) {
	f.started <- struct{}{}
	<-f.release
}

type signalingResponder struct{ sent chan<- Response }

func (r signalingResponder) Reply(_ context.Context, response Response) error {
	r.sent <- response
	return nil
}

func (f *fakeCreator) Start(context.Context, lifecycle.Request, string, lifecycle.Notifier) error {
	return f.startErr
}

func (f *fakeCreator) ConfirmResult(_ context.Context, request lifecycle.Request, text string, _ lifecycle.Notifier) (lifecycle.ConfirmationOutcome, error) {
	f.requests = append(f.requests, request)
	f.confirmations = append(f.confirmations, text)
	return f.confirmOutcome, f.confirmErr
}
func (f *fakeCreator) Cancel(context.Context, lifecycle.Request, lifecycle.Notifier) bool {
	return false
}

func (f *fakeLifecycle) OwnsThread(request lifecycle.Request) bool {
	return request.UserID == "U1" && request.Channel == "C1" && request.ThreadTimestamp == "123.456"
}

func (f *fakeLifecycle) RequestDestroy(_ context.Context, request lifecycle.Request, notify lifecycle.Notifier) error {
	f.requests = append(f.requests, request)
	if err := notify(context.Background(), lifecycle.Notice{Channel: request.Channel, ThreadTimestamp: request.ThreadTimestamp, Text: "Cleanup complete."}); err != nil {
		return err
	}
	return f.err
}
func (*fakeLifecycle) ScheduleLease(lifecycle.Request, state.LifecycleRecord, lifecycle.Notifier) {}
func (f *fakeLifecycle) Extend(_ context.Context, request lifecycle.Request, increment time.Duration, notify lifecycle.Notifier) (lifecycle.ExtensionResult, error) {
	f.extendRequests = append(f.extendRequests, request)
	f.extendValues = append(f.extendValues, increment)
	if f.extendErr != nil {
		return lifecycle.ExtensionResult{}, f.extendErr
	}
	return lifecycle.ExtensionResult{}, notify(context.Background(), lifecycle.Notice{Channel: "C1", ThreadTimestamp: "123.456", Text: "Lease extended."})
}

func TestCommandLogsUseValidatedCategoriesAndAdmissions(t *testing.T) {
	const rawCommand = "xoxb-secret /private/state"
	for _, test := range []struct {
		name      string
		message   Message
		creator   lifecycle.CreateHandler
		lifecycle lifecycleHandler
		category  string
		admission string
		result    string
	}{
		{name: "DM unknown", message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: rawCommand}, category: "unknown", admission: "rejected"},
		{name: "DM restricted", message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "destroy"}, category: "destroy", admission: "rejected"},
		{name: "mentioned unknown", message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> " + rawCommand}, category: "unknown", admission: "rejected"},
		{name: "thread confirmation without lifecycle", message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "123.456"}, creator: &fakeCreator{confirmOutcome: lifecycle.ConfirmationNoLifecycle}, category: "yes", admission: "rejected", result: "no-lifecycle"},
		{name: "thread confirmation wrong thread", message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "no", Timestamp: "reply", ThreadTimestamp: "123.456"}, creator: &fakeCreator{confirmOutcome: lifecycle.ConfirmationWrongThread}, category: "no", admission: "rejected", result: "wrong-thread"},
		{name: "thread confirmation before review", message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "123.456"}, creator: &fakeCreator{confirmOutcome: lifecycle.ConfirmationPreReview}, category: "yes", admission: "rejected", result: "pre-review"},
		{name: "thread confirmation approval", message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "123.456"}, creator: &fakeCreator{confirmOutcome: lifecycle.ConfirmationApproved}, category: "yes", admission: "accepted", result: "approved"},
		{name: "thread confirmation rejection", message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "no", Timestamp: "reply", ThreadTimestamp: "123.456"}, creator: &fakeCreator{confirmOutcome: lifecycle.ConfirmationRejected}, category: "no", admission: "accepted", result: "rejected"},
		{name: "thread cleanup", message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "done", Timestamp: "reply", ThreadTimestamp: "123.456"}, lifecycle: &fakeLifecycle{}, category: "done", admission: "accepted"},
		{name: "foreign thread cleanup", message: Message{Channel: "C1", ChannelType: "channel", User: "U2", Text: "destroy", Timestamp: "reply", ThreadTimestamp: "123.456"}, lifecycle: &fakeLifecycle{}, category: "destroy", admission: "rejected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs []string
			bot := Bot{
				ChannelID:  "C1",
				SelfUserID: "BOT",
				Events:     &memoryEvents{},
				Creator:    test.creator,
				Lifecycle:  test.lifecycle,
				Responder:  &memoryResponder{},
				Logf: func(format string, args ...any) {
					logs = append(logs, fmt.Sprintf(format, args...))
				},
			}
			if err := bot.Handle(context.Background(), Envelope{ID: test.name, Message: test.message}); err != nil {
				t.Fatal(err)
			}
			got := strings.Join(logs, "\n")
			for _, forbidden := range []string{rawCommand, "xoxb-secret", "/private/state"} {
				if strings.Contains(got, forbidden) {
					t.Fatalf("command log leaked raw command text %q: %q", forbidden, got)
				}
			}
			wants := []string{`event_id="` + test.name + `"`, `category="` + test.category + `"`, "admission=" + test.admission, "mode=accepting"}
			if test.result != "" {
				wants = append(wants, `result="`+test.result+`"`)
			}
			for _, want := range wants {
				if !strings.Contains(got, want) {
					t.Fatalf("command log = %q, want %q", got, want)
				}
			}
		})
	}
}

func TestMaintainerAdmissionCommandsStayPrivateAndPauseBlocksModifyingCommands(t *testing.T) {
	control, err := admission.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reviewDone := control.Activity("review")
	defer reviewDone()
	responses := &memoryResponder{}
	declined := make(chan struct{}, 1)
	creator := &pausingCreator{done: declined}
	bot := Bot{
		ChannelID: "C1", SelfUserID: "BOT", MaintainerID: "UMaintainer", Admission: control,
		Events: &memoryEvents{}, ICT: &fakeICT{}, Creator: creator, Responder: responses,
	}
	events := []Envelope{
		{ID: "maintainer-help", Message: Message{Channel: "DM", ChannelType: "im", User: "UMaintainer", Text: "help"}},
		{ID: "ordinary-pause", Message: Message{Channel: "DM", ChannelType: "im", User: "U1", Text: "pause"}},
		{ID: "maintainer-pause", Message: Message{Channel: "DM", ChannelType: "im", User: "UMaintainer", Text: "pause"}},
		{ID: "blocked-create", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create", Timestamp: "1"}},
		{ID: "allowed-help", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> help", Timestamp: "2"}},
		{ID: "maintainer-unpause", Message: Message{Channel: "DM", ChannelType: "im", User: "UMaintainer", Text: "unpause"}},
	}
	for _, envelope := range events {
		if err := bot.Handle(context.Background(), envelope); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != len(events)+1 {
		t.Fatalf("responses = %+v", responses.responses)
	}
	if !strings.Contains(responses.responses[1].Text, "Maintainer DM commands") || !strings.Contains(responses.responses[1].Text, "pause") {
		t.Fatalf("maintainer help = %q", responses.responses[1].Text)
	}
	if strings.Contains(responses.responses[2].Text, "pause") || strings.Contains(responses.responses[2].Text, "Maintainer") {
		t.Fatalf("ordinary user learned maintainer controls: %q", responses.responses[2].Text)
	}
	if !strings.Contains(responses.responses[3].Text, "Mode: paused") || !strings.Contains(responses.responses[3].Text, "Pending reviews: 1") {
		t.Fatalf("pause status = %q", responses.responses[3].Text)
	}
	if !strings.Contains(responses.responses[4].Text, "Only help and list") {
		t.Fatalf("paused create = %q", responses.responses[4].Text)
	}
	if !strings.Contains(responses.responses[5].Text, "Commands") {
		t.Fatalf("paused help = %q", responses.responses[5].Text)
	}
	if !strings.Contains(responses.responses[6].Text, "Mode: accepting") || !control.Accepting() {
		t.Fatalf("unpause status = %q accepting=%v", responses.responses[6].Text, control.Accepting())
	}
	select {
	case <-declined:
	case <-time.After(time.Second):
		t.Fatal("pending review decline did not start")
	}
	if creator.declineCount() != 1 {
		t.Fatalf("pending review declines = %d, want 1", creator.declineCount())
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "non-bare-pause", Message: Message{Channel: "DM", ChannelType: "im", User: "UMaintainer", Text: "pause now"}}); err != nil {
		t.Fatal(err)
	}
	last := responses.responses[len(responses.responses)-1].Text
	if !control.Accepting() || strings.Contains(last, "Maintainer") {
		t.Fatalf("non-bare pause changed admission or exposed controls: accepting=%v response=%q", control.Accepting(), last)
	}
}

func TestMaintainerStopReturnsImmediatelyThenDrainsOnce(t *testing.T) {
	control, err := admission.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	applyDone := control.Activity("apply")
	responses := &memoryResponder{}
	stopped := make(chan lifecycle.Request, 1)
	bot := Bot{
		ChannelID: "C1", SelfUserID: "BOT", MaintainerID: "UMaintainer", Admission: control,
		Events: &memoryEvents{}, Responder: responses,
		GracefulStop: func(request lifecycle.Request, drained <-chan struct{}) {
			<-drained
			stopped <- request
		},
	}
	stop := Envelope{ID: "stop", Message: Message{Channel: "DM", ChannelType: "im", User: "UMaintainer", Text: "stop"}}
	if err := bot.Handle(context.Background(), stop); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[0].Text; !strings.Contains(got, "Mode: draining-to-stop") || !strings.Contains(got, "Active applies: 1") {
		t.Fatalf("immediate stop response = %q", got)
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "status", Message: Message{Channel: "DM", ChannelType: "im", User: "UMaintainer", Text: "status"}}); err != nil {
		t.Fatal(err)
	}
	if got := responses.responses[1].Text; !strings.Contains(got, "Mode: draining-to-stop") {
		t.Fatalf("status during drain = %q", got)
	}
	if err := bot.Handle(context.Background(), stop); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-stopped:
		t.Fatalf("graceful stop started before apply drained: %+v", request)
	default:
	}
	applyDone()
	select {
	case request := <-stopped:
		if request.UserID != "UMaintainer" || request.Channel != "DM" {
			t.Fatalf("graceful stop request = %+v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("graceful stop did not run after drain")
	}
	select {
	case <-stopped:
		t.Fatal("repeated stop started another drain")
	default:
	}
}

func TestPausedModifierMatrixRejectsUserCommandsWhileHelpAndListRemainAvailable(t *testing.T) {
	control, err := admission.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := control.Pause(); err != nil || !changed {
		t.Fatalf("pause changed=%v err=%v", changed, err)
	}
	store, inventory := lifecycleListStore(t)
	manager := &fakeLifecycle{}
	creator := &fakeCreator{}
	responses := &memoryResponder{}
	bot := Bot{
		ChannelID: "C1", SelfUserID: "BOT", Admission: control, Events: &memoryEvents{},
		ICT: &fakeICT{inventory: inventory}, Lifecycles: store, Lifecycle: manager, Creator: creator, Responder: responses,
	}
	for _, test := range []struct {
		name, text, thread string
	}{
		{name: "create", text: "<@BOT> create"},
		{name: "yes", text: "yes", thread: "123.456"},
		{name: "done", text: "done", thread: "123.456"},
		{name: "destroy", text: "destroy", thread: "123.456"},
		{name: "extend", text: "extend 1h", thread: "123.456"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := bot.Handle(context.Background(), Envelope{ID: test.name, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: test.text, Timestamp: "reply", ThreadTimestamp: test.thread}}); err != nil {
				t.Fatal(err)
			}
			response := responses.responses[len(responses.responses)-1]
			if !strings.Contains(response.Text, "Only help and list are available") {
				t.Fatalf("paused %s response = %q", test.name, response.Text)
			}
		})
	}
	if len(creator.confirmations) != 0 || len(manager.requests) != 0 || len(manager.extendRequests) != 0 {
		t.Fatalf("paused commands reached lifecycle handlers: confirmations=%v cleanup=%v extensions=%v", creator.confirmations, manager.requests, manager.extendRequests)
	}
	for _, test := range []struct{ name, text string }{
		{name: "help", text: "<@BOT> help"},
		{name: "list", text: "<@BOT> list"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := bot.Handle(context.Background(), Envelope{ID: "allowed-" + test.name, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: test.text, Timestamp: test.name}}); err != nil {
				t.Fatal(err)
			}
		})
	}
	if got := responses.responses[len(responses.responses)-2].Text; !strings.Contains(got, "Commands") {
		t.Fatalf("paused help response = %q", got)
	}
	if got := responses.responses[len(responses.responses)-1].Text; !strings.Contains(got, "cluster") {
		t.Fatalf("paused list response = %q", got)
	}
}

func TestPauseRepliesWithoutWaitingForPendingReviewDecline(t *testing.T) {
	control, err := admission.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	declineStarted := make(chan struct{})
	releaseDecline := make(chan struct{})
	responseSent := make(chan Response)
	bot := Bot{
		MaintainerID: "UMaintainer",
		Admission:    control,
		Events:       &memoryEvents{},
		Creator:      &blockingDeclineCreator{started: declineStarted, release: releaseDecline},
		Responder:    signalingResponder{sent: responseSent},
	}
	done := make(chan error, 1)
	go func() {
		done <- bot.Handle(context.Background(), Envelope{
			ID:      "pause",
			Message: Message{Channel: "DM", ChannelType: "im", User: "UMaintainer", Text: "pause"},
		})
	}()
	response := <-responseSent
	if !strings.Contains(response.Text, "Mode: paused") {
		t.Fatalf("pause response = %q", response.Text)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pause waited for pending review decline")
	}
	select {
	case <-declineStarted:
	case <-time.After(time.Second):
		t.Fatal("pending review decline did not start")
	}
	close(releaseDecline)
}

func TestDMListUsesWorkspaceLocalRecordsAndDuplicateIsIgnored(t *testing.T) {
	records := []state.LifecycleRecord{
		{UserID: "U1", Channel: "C1", ThreadTimestamp: "1", Status: "ready", ClusterName: "servitor-one", Location: "us-south/us-south-1", LeaseExpiresAt: time.Date(2026, 9, 7, 15, 29, 13, 0, time.UTC), UpdatedAt: time.Date(2026, 9, 7, 11, 0, 0, 0, time.UTC)},
		{UserID: "U2", Channel: "C1", ThreadTimestamp: "2", Status: "applying", ClusterName: "servitor-two", Location: "us-east/us-east-1", UpdatedAt: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)},
	}
	store, inventory := lifecycleListStore(t, records...)
	ict := &fakeICT{inventory: inventory}
	responses := &memoryResponder{}
	now := time.Date(2026, 9, 7, 11, 29, 13, 0, time.UTC)
	bot := Bot{ChannelID: "C1", Events: &memoryEvents{}, ICT: ict, Lifecycles: store, Responder: responses, Clock: func() time.Time { return now }}
	acks := 0
	envelope := Envelope{ID: "Ev1", Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "list", Timestamp: "1"}, Acknowledge: func(context.Context) error { acks++; return nil }}
	if err := bot.Handle(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if err := bot.Handle(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if acks != 2 || ict.calls != 1 || len(responses.responses) != 1 {
		t.Fatalf("acks=%d ICT calls=%d replies=%d, want 2, 1, 1", acks, ict.calls, len(responses.responses))
	}
	response := responses.responses[0]
	if response.ThreadTimestamp != "" || response.Channel != "D1" || !strings.Contains(response.Text, "*  servitor-one") || !strings.Contains(response.Text, "servitor-two") || !strings.Contains(response.Text, "2026-09-07 15:29:13 UTC (~4h)") || strings.Contains(response.Text, "U1") || strings.Contains(response.Text, "U2") {
		t.Errorf("response = %+v, want a safe lifecycle table", response)
	}
	if ict.stateID != "U1" || !diagnostics.ValidID(ict.diagnosticID) {
		t.Errorf("list diagnostic = (%q, %q), want caller state and opaque ID", ict.stateID, ict.diagnosticID)
	}
}

func TestDMListFailureUsesSafeDiagnosticReference(t *testing.T) {
	store, _ := lifecycleListStore(t)
	ict := &fakeICT{err: errors.New("xoxb-secret /private/state {terraform_values}")}
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", Events: &memoryEvents{}, ICT: ict, Lifecycles: store, Responder: responses}

	if err := bot.Handle(context.Background(), Envelope{ID: "Ev1", Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "list"}}); err != nil {
		t.Fatal(err)
	}
	if ict.calls != 1 || ict.stateID != "U1" || !diagnostics.ValidID(ict.diagnosticID) {
		t.Fatalf("list diagnostic = calls %d, (%q, %q)", ict.calls, ict.stateID, ict.diagnosticID)
	}
	if len(responses.responses) != 1 {
		t.Fatalf("responses = %+v, want one safe list error", responses.responses)
	}
	text := responses.responses[0].Text
	for _, forbidden := range []string{"xoxb-secret", "/private/state", "terraform_values"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Slack response leaked %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, "No cleanup is running; this requires maintainer attention.") || !strings.Contains(text, "Diagnostic ID: `"+ict.diagnosticID+"`.") {
		t.Fatalf("Slack response did not preserve safe correlation: %s", text)
	}
}

func TestEmptyDMListRendersOnlyTableHeaders(t *testing.T) {
	store, inventory := lifecycleListStore(t)
	ict := &fakeICT{inventory: inventory}
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", Events: &memoryEvents{}, ICT: ict, Lifecycles: store, Responder: responses}
	if err := bot.Handle(context.Background(), Envelope{ID: "Ev1", Message: Message{Channel: "D1", ChannelType: "im", Text: "list"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 || strings.Count(responses.responses[0].Text, "```") != 2 || !strings.Contains(responses.responses[0].Text, "cluster") || strings.Contains(responses.responses[0].Text, "There are no") {
		t.Errorf("response = %+v, want only the table header", responses.responses)
	}
}

func TestListUsesSameInventoryInDMAndConfiguredChannel(t *testing.T) {
	record := state.LifecycleRecord{UserID: "U1", Channel: "C1", ThreadTimestamp: "1", Status: "ready", ClusterName: "servitor-one", Location: "us-south/us-south-1", UpdatedAt: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)}
	store, inventory := lifecycleListStore(t, record)
	ict := &fakeICT{inventory: inventory}
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Events: &memoryEvents{}, ICT: ict, Lifecycles: store, Responder: responses}
	for _, envelope := range []Envelope{
		{ID: "dm-list", Message: Message{Channel: "D1", ChannelType: "im", User: "U1", Text: "list", Timestamp: "1"}},
		{ID: "channel-list", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> list", Timestamp: "2"}},
	} {
		if err := bot.Handle(context.Background(), envelope); err != nil {
			t.Fatal(err)
		}
	}
	if len(responses.responses) != 2 || responses.responses[0].Text != responses.responses[1].Text || responses.responses[0].ThreadTimestamp != "" || responses.responses[1].ThreadTimestamp != "2" || ict.calls != 2 {
		t.Fatalf("list responses = %+v, ICT calls = %d; want identical DM/channel inventories", responses.responses, ict.calls)
	}
}

func TestLifecycleCommandsNeverInvokeICTOutsideConfiguredChannel(t *testing.T) {
	for _, test := range []struct {
		name        string
		channel     string
		channelType string
		command     string
		thread      string
	}{
		{name: "DM", channel: "D1", channelType: "im", command: "create"},
		{name: "other channel", channel: "C2", channelType: "channel", command: "destroy", thread: "1"},
		{name: "configured channel", channel: "C1", channelType: "channel", command: "done", thread: "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ict := &fakeICT{}
			responses := &memoryResponder{}
			bot := Bot{ChannelID: "C1", Events: &memoryEvents{}, ICT: ict, Responder: responses}
			if err := bot.Handle(context.Background(), Envelope{ID: test.name, Message: Message{Channel: test.channel, ChannelType: test.channelType, Text: test.command, Timestamp: "1"}}); err != nil {
				t.Fatal(err)
			}
			if ict.calls != 0 {
				t.Errorf("ICT calls = %d, want 0", ict.calls)
			}
			if test.channelType != "im" && len(responses.responses) != 0 {
				t.Errorf("responses = %+v, want silent outside mentioned configured commands", responses.responses)
			}
		})
	}
}

func TestConfirmationThreadNonliteralRepliesAreIgnored(t *testing.T) {
	creator := &fakeCreator{}
	responses := &memoryResponder{}
	bot := Bot{ChannelID: "C1", Events: &memoryEvents{}, ICT: &fakeICT{}, Creator: creator, Responder: responses}
	for _, text := range []string{"yes please", "Yes", "maybe", "help", "create --version 4.22", "no", "yes"} {
		envelope := Envelope{ID: "Ev-thread-" + text, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: text, Timestamp: "reply", ThreadTimestamp: "123.456"}}
		if err := bot.Handle(context.Background(), envelope); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := strings.Join(creator.confirmations, ","), "no,yes"; got != want {
		t.Errorf("confirmation replies = %q, want %q", got, want)
	}
	for _, request := range creator.requests {
		want := lifecycleRequest("U1")
		want.RawThread = true
		if request != want {
			t.Errorf("confirmation request = %+v, want caller-owned raw thread", request)
		}
	}
	if len(responses.responses) != 0 {
		t.Errorf("confirmation replies produced responses: %+v", responses.responses)
	}
}

type safeCreateError struct{}

type safeApprovalError struct{}

func (safeApprovalError) Error() string { return "xoxb-secret /private/state {terraform_values}" }
func (safeApprovalError) UserNotice() string {
	return "Unable to record create approval. Cleanup is running. Diagnostic ID: `111111111111111111111111`."
}

func (safeCreateError) Error() string { return "xoxb-secret /private/state {terraform_values}" }
func (safeCreateError) UserNotice() string {
	return "Unable to verify existing ICT state. No cleanup is running; this requires maintainer attention. Diagnostic ID: `111111111111111111111111`."
}

func TestApprovalPersistenceNoticeUsesSafeCorrelatedError(t *testing.T) {
	responses := &memoryResponder{}
	bot := Bot{
		ChannelID: "C1",
		Events:    &memoryEvents{},
		ICT:       &fakeICT{},
		Creator:   &fakeCreator{confirmErr: safeApprovalError{}},
		Responder: responses,
	}
	envelope := Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "yes", Timestamp: "reply", ThreadTimestamp: "123.456"}}
	if err := bot.Handle(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 {
		t.Fatalf("responses = %+v, want one safe confirmation error", responses.responses)
	}
	response := responses.responses[0]
	if response.Channel != "C1" || response.ThreadTimestamp != "123.456" {
		t.Fatalf("response = %+v, want same-thread confirmation error", response)
	}
	for _, forbidden := range []string{"xoxb-secret", "/private/state", "terraform_values"} {
		if strings.Contains(response.Text, forbidden) {
			t.Fatalf("Slack response leaked %q: %s", forbidden, response.Text)
		}
	}
	if !strings.Contains(response.Text, "Cleanup is running.") || !strings.Contains(response.Text, "Diagnostic ID: `111111111111111111111111`.") {
		t.Fatalf("Slack response did not preserve safe correlation: %s", response.Text)
	}
}

func TestCreatePreflightNoticeUsesSafeCorrelatedError(t *testing.T) {
	responses := &memoryResponder{}
	bot := Bot{
		ChannelID:  "C1",
		SelfUserID: "BOT",
		Events:     &memoryEvents{},
		ICT:        &fakeICT{},
		Creator:    &fakeCreator{startErr: safeCreateError{}},
		Responder:  responses,
	}
	if err := bot.Handle(context.Background(), Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> create --version 4.22", Timestamp: "123.456"}}); err != nil {
		t.Fatal(err)
	}
	if len(responses.responses) != 1 {
		t.Fatalf("responses = %+v, want one safe create error", responses.responses)
	}
	text := responses.responses[0].Text
	for _, forbidden := range []string{"xoxb-secret", "/private/state", "terraform_values"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Slack response leaked %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, "Diagnostic ID: `111111111111111111111111`.") || !strings.Contains(text, "No cleanup is running") {
		t.Fatalf("Slack response did not preserve safe correlation: %s", text)
	}
}

func TestConfiguredChannelDestroyAndDoneUseCallerAndThread(t *testing.T) {
	for _, command := range []string{"destroy", "done"} {
		t.Run(command, func(t *testing.T) {
			manager := &fakeLifecycle{}
			responses := &memoryResponder{}
			bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Events: &memoryEvents{}, ICT: &fakeICT{}, Lifecycle: manager, Responder: responses}
			envelope := Envelope{ID: "Ev-" + command, Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> " + command, Timestamp: "123.456"}}
			if err := bot.Handle(context.Background(), envelope); err != nil {
				t.Fatal(err)
			}
			if len(manager.requests) != 1 || manager.requests[0] != (lifecycleRequest("U1")) {
				t.Fatalf("lifecycle requests = %+v, want caller-owned request", manager.requests)
			}
			if len(responses.responses) != 2 || responses.responses[0].Text != "Command accepted.\nCleaning up..." || responses.responses[0].ThreadTimestamp != "123.456" || responses.responses[0].Channel != "C1" {
				t.Fatalf("responses = %+v, want immediate configured-channel acceptance and lifecycle result", responses.responses)
			}
		})
	}
}

func TestDestroyErrorAfterSafeNoticeDoesNotStopHandler(t *testing.T) {
	responses := &memoryResponder{}
	bot := Bot{
		ChannelID:  "C1",
		SelfUserID: "BOT",
		Events:     &memoryEvents{},
		ICT:        &fakeICT{},
		Lifecycle:  &fakeLifecycle{err: safeCreateError{}},
		Responder:  responses,
	}
	envelope := Envelope{ID: "Ev1", Message: Message{Channel: "C1", ChannelType: "channel", User: "U1", Text: "<@BOT> destroy", Timestamp: "123.456"}}
	if err := bot.Handle(context.Background(), envelope); err != nil {
		t.Fatalf("Handle returned lifecycle error: %v", err)
	}
	if len(responses.responses) != 2 {
		t.Fatalf("responses = %+v, want immediate acceptance and lifecycle notice", responses.responses)
	}
	response := responses.responses[1]
	if responses.responses[0].Text != "Command accepted.\nCleaning up..." || response.Channel != "C1" || response.ThreadTimestamp != "123.456" || response.Text != "Cleanup complete." {
		t.Fatalf("responses = %+v, want safe same-thread cleanup feedback", responses.responses)
	}
}

func lifecycleRequest(userID string) lifecycle.Request {
	return lifecycle.Request{UserID: userID, Channel: "C1", ThreadTimestamp: "123.456"}
}

func lifecycleListStore(t *testing.T, records ...state.LifecycleRecord) (*state.LifecycleStore, state.WorkspaceInventory) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "ict")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	inventory := state.WorkspaceInventory{Version: 1, StateRoot: root}
	ids := []string{"default"}
	for _, record := range records {
		ids = append(ids, record.UserID)
	}
	for _, id := range ids {
		path := filepath.Join(root, id)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		inventory.Workspaces = append(inventory.Workspaces, state.WorkspaceLocation{ID: id, Path: path})
	}
	store, err := state.OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(inventory); err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := store.Put(record); err != nil {
			t.Fatal(err)
		}
	}
	return store, inventory
}

func TestSelfAndSubtypeMessagesAreIgnored(t *testing.T) {
	for _, message := range []Message{
		{Channel: "D1", ChannelType: "im", User: "BOT", Text: "list"},
		{Channel: "D1", ChannelType: "im", User: "U1", Text: "list", Subtype: "message_changed"},
		{Channel: "D1", ChannelType: "im", User: "U1", Text: "list", BotID: "B1"},
	} {
		ict := &fakeICT{}
		responses := &memoryResponder{}
		bot := Bot{ChannelID: "C1", SelfUserID: "BOT", Events: &memoryEvents{}, ICT: ict, Responder: responses}
		if err := bot.Handle(context.Background(), Envelope{ID: message.User + message.Subtype + message.BotID, Message: message}); err != nil {
			t.Fatal(err)
		}
		if ict.calls != 0 || len(responses.responses) != 0 {
			t.Errorf("message %+v was not ignored", message)
		}
	}
}

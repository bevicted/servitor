// Package slackbot implements the Slack command boundary.
package slackbot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/bevicted/servitor/internal/admission"
	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/diagnostics"
	"github.com/bevicted/servitor/internal/lifecycle"
	"github.com/bevicted/servitor/internal/state"
)

const maxSlackMessage = 3000

type EventStore interface{ Claim(string) (bool, error) }
type ListClient interface {
	WorkspaceInventoryDiagnostic(context.Context, string, string) (state.WorkspaceInventory, error)
}
type Responder interface {
	Reply(context.Context, Response) error
}
type lifecycleHandler interface {
	lifecycle.Destroyer
	OwnsThread(lifecycle.Request) bool
	Extend(context.Context, lifecycle.Request, time.Duration, lifecycle.Notifier) (lifecycle.ExtensionResult, error)
}
type Message struct{ Channel, ChannelType, User, Text, Timestamp, ThreadTimestamp, Subtype, BotID string }
type Envelope struct {
	ID          string
	Message     Message
	Acknowledge func(context.Context) error
}
type Response struct{ Channel, Text, ThreadTimestamp string }

// Bot routes messages while preserving the configured-channel authorization boundary.
type Bot struct {
	ChannelID, SelfUserID string
	MaintainerID          string
	Admission             *admission.Control
	Events                EventStore
	ICT                   ListClient
	Lifecycles            *state.LifecycleStore
	Lifecycle             lifecycleHandler
	Creator               lifecycle.CreateHandler
	Defaults              command.CreateDefaults
	Responder             Responder
	GracefulStop          func(lifecycle.Request, <-chan struct{})
	Clock                 func() time.Time
	Logf                  func(string, ...any)
}

func (b Bot) Handle(ctx context.Context, envelope Envelope) error {
	if envelope.Acknowledge != nil {
		if err := envelope.Acknowledge(ctx); err != nil {
			return fmt.Errorf("acknowledge Slack envelope: %w", err)
		}
	}
	if envelope.ID != "" {
		claimed, err := b.Events.Claim(envelope.ID)
		if err != nil {
			return fmt.Errorf("record Slack event: %w", err)
		}
		if !claimed {
			return nil
		}
	}
	message := envelope.Message
	if message.Subtype != "" || message.BotID != "" || (b.SelfUserID != "" && message.User == b.SelfUserID) {
		return nil
	}

	isDM := message.ChannelType == "im"
	thread := message.Timestamp
	if isDM {
		thread = ""
	} else if message.ThreadTimestamp != "" {
		thread = message.ThreadTimestamp
	}
	reply := func(text string) error {
		return b.Responder.Reply(ctx, Response{Channel: message.Channel, Text: text, ThreadTimestamp: thread})
	}
	respond := func(text string) {
		if err := reply(text); err != nil {
			b.logf("deliver Slack reply: %v", err)
		}
	}
	notify := func(noticeCtx context.Context, notice lifecycle.Notice) error {
		err := b.Responder.Reply(noticeCtx, Response{Channel: notice.Channel, ThreadTimestamp: notice.ThreadTimestamp, Text: notice.Text})
		if err != nil {
			b.logf("deliver lifecycle Slack reply: %v", err)
		}
		return err
	}

	if isDM {
		category := commandCategory(message.Text)
		b.logCommand(envelope, message, thread, category, b.dmCommandAccepted(message.User, message.Text, category))
		b.handleDM(ctx, message, reply, respond, notify)
		return nil
	}
	if message.Channel != b.ChannelID {
		return nil
	}

	raw := message.Text
	if message.ThreadTimestamp != "" {
		request := lifecycle.Request{UserID: message.User, Channel: message.Channel, ThreadTimestamp: thread, RawThread: true}
		if modifyingCommand(firstToken(raw)) && !b.admissionOpen() {
			if b.ownsThread(request) {
				b.logCommand(envelope, message, thread, firstToken(raw), false)
				respond(modeRejection(b.admissionMode()))
			}
			return nil
		}
		if firstToken(raw) == "extend" {
			b.logCommand(envelope, message, thread, "extend", b.Lifecycle != nil && b.Lifecycle.OwnsThread(request))
			b.handleExtension(ctx, request, raw, notify, respond)
			return nil
		}
		switch raw {
		case "yes", "no":
			outcome := b.handleConfirmation(ctx, request, raw, notify, respond)
			b.logConfirmation(envelope, message, thread, raw, outcome)
			return nil
		case "done", "destroy":
			b.logCommand(envelope, message, thread, raw, b.Lifecycle != nil && b.Lifecycle.OwnsThread(request))
			b.handleThreadCleanup(ctx, request, notify, respond)
			return nil
		}
	}

	text, mentioned := channelCommand(message.Text, b.SelfUserID)
	if !mentioned {
		return nil
	}
	request := lifecycle.Request{UserID: message.User, Channel: message.Channel, ThreadTimestamp: thread, RawThread: message.ThreadTimestamp != ""}
	category := commandCategory(text)
	b.logCommand(envelope, message, thread, category, b.channelCommandAccepted(category, request))
	b.handleChannelRoot(ctx, message, request, text, reply, respond, notify)
	return nil
}

func (b Bot) handleDM(ctx context.Context, message Message, reply func(string) error, respond func(string), notify lifecycle.Notifier) {
	commandText := strings.TrimSpace(message.Text)
	if message.User == b.MaintainerID && len(strings.Fields(commandText)) == 1 {
		switch firstToken(commandText) {
		case "status":
			respond(maintainerStatus(b.admissionStatus()))
			return
		case "pause":
			if b.Admission == nil {
				respond(rejectedText("Admission control is unavailable."))
				return
			}
			status, changed, err := b.Admission.Pause()
			if err != nil {
				b.logf("pause admission: %v", err)
				respond(rejectedText("Unable to pause admission. The operator must inspect the private server logs."))
				return
			}
			respond(maintainerStatus(status))
			if changed {
				if creator, ok := b.Creator.(interface{ DeclinePendingReviews(lifecycle.Notifier) }); ok {
					go creator.DeclinePendingReviews(notify)
				}
			}
			return
		case "unpause":
			if b.Admission == nil {
				respond(rejectedText("Admission control is unavailable."))
				return
			}
			status, _, err := b.Admission.Unpause()
			if err != nil {
				b.logf("unpause admission: %v", err)
				respond(rejectedText("Unable to unpause admission. The operator must inspect the private server logs."))
				return
			}
			respond(maintainerStatus(status))
			return
		case "stop":
			if b.Admission == nil || b.GracefulStop == nil {
				respond(rejectedText("Graceful stop is unavailable. The operator must inspect the private server logs."))
				return
			}
			status, changed, drained, err := b.Admission.Stop()
			if err != nil {
				b.logf("start graceful stop: %v", err)
				respond(rejectedText("Unable to start graceful stop. The operator must inspect the private server logs."))
				return
			}
			respond(maintainerStatus(status))
			if changed {
				if creator, ok := b.Creator.(interface{ DeclinePendingReviews(lifecycle.Notifier) }); ok {
					go creator.DeclinePendingReviews(notify)
				}
				go b.GracefulStop(lifecycle.Request{UserID: message.User, Channel: message.Channel}, drained)
			}
			return
		}
	}
	switch firstToken(commandText) {
	case "help":
		b.respondHelp(commandText, message.User == b.MaintainerID, respond)
	case "list":
		b.list(ctx, message.User, respond)
	case "":
		respond(unknownText())
	case "create", "done", "destroy", "extend":
		respond(rejectedText("That command is available only in the configured channel."))
	case "status", "pause", "unpause", "stop":
		b.respondHelp("help", false, respond)
	default:
		respond(unknownText())
	}
	_ = reply
}

func (b Bot) handleExtension(ctx context.Context, request lifecycle.Request, text string, notify lifecycle.Notifier, respond func(string)) {
	if b.Lifecycle == nil || (request.RawThread && !b.Lifecycle.OwnsThread(request)) {
		return
	}
	increment, err := command.ParseExtend(text)
	if err != nil {
		respond(rejectedText(err.Error()))
		return
	}
	if _, err := b.Lifecycle.Extend(ctx, request, increment, notify); err != nil {
		respond(rejectedText(err.Error()))
	}
}

func (b Bot) handleThreadCleanup(ctx context.Context, request lifecycle.Request, notify lifecycle.Notifier, respond func(string)) {
	if b.Lifecycle == nil || !b.Lifecycle.OwnsThread(request) {
		return
	}
	if b.Creator != nil && b.Creator.Cancel(ctx, request, notify) {
		respond("Command accepted.\nCleaning up...")
		return
	}
	respond("Command accepted.\nCleaning up...")
	if err := b.Lifecycle.RequestDestroy(ctx, request, notify); err != nil {
		b.logf("start cleanup: %v", err)
	}
}

func (b Bot) handleChannelRoot(ctx context.Context, message Message, request lifecycle.Request, text string, reply func(string) error, respond func(string), notify lifecycle.Notifier) {
	if modifyingCommand(firstToken(text)) && !b.admissionOpen() {
		respond(modeRejection(b.admissionMode()))
		return
	}
	switch firstToken(text) {
	case "help":
		b.respondHelp(text, false, respond)
	case "list":
		b.list(ctx, message.User, respond)
	case "create":
		if message.ThreadTimestamp != "" {
			respond(rejectedText("Create must be started from the configured channel root."))
			return
		}
		if b.Creator == nil {
			respond(rejectedText("Create is unavailable. The operator must inspect the private server logs."))
			return
		}
		if err := b.Creator.Start(ctx, request, text, notify); err != nil {
			var noticeError lifecycle.UserNoticeError
			if errors.As(err, &noticeError) {
				respond(rejectedText(noticeError.UserNotice()))
				return
			}
			var deliveryError *lifecycle.AcceptanceDeliveryError
			if errors.As(err, &deliveryError) {
				// A failed acceptance reply is deliberately silent: Start has not invoked ICT.
				b.logf("start create: %v", err)
				return
			}
			respond(rejectedText(err.Error()))
		}
	case "extend":
		b.handleExtension(ctx, request, text, notify, respond)
	case "done", "destroy":
		if request.RawThread {
			b.handleThreadCleanup(ctx, request, notify, respond)
			return
		}
		if b.Creator != nil && b.Creator.Cancel(ctx, request, notify) {
			respond("Command accepted.\nCleaning up...")
			return
		}
		if b.Lifecycle == nil {
			respond(rejectedText("Cleanup is unavailable. The operator must inspect the private server logs."))
			return
		}
		respond("Command accepted.\nCleaning up...")
		if err := b.Lifecycle.RequestDestroy(ctx, request, notify); err != nil {
			b.logf("start cleanup: %v", err)
		}
	default:
		respond(unknownText())
	}
	_ = reply
}

func (b Bot) handleConfirmation(ctx context.Context, request lifecycle.Request, text string, notify lifecycle.Notifier, respond func(string)) lifecycle.ConfirmationOutcome {
	if b.Creator == nil {
		return lifecycle.ConfirmationNoLifecycle
	}
	outcome, err := b.Creator.ConfirmResult(ctx, request, text, notify)
	if err != nil {
		var noticeError lifecycle.UserNoticeError
		if errors.As(err, &noticeError) {
			respond(rejectedText(noticeError.UserNotice()))
			return outcome
		}
		b.logf("confirm lifecycle: %v", err)
		return outcome
	}
	if outcome == lifecycle.ConfirmationApplying && text == "yes" {
		respond("Creation is already in progress.")
	}
	return outcome
}

func (b Bot) list(ctx context.Context, user string, respond func(string)) {
	if b.ICT == nil || b.Lifecycles == nil {
		respond(rejectedText("List is unavailable. The operator must inspect the private server logs."))
		return
	}
	diagnosticID, err := diagnostics.NewID()
	if err != nil {
		respond(rejectedText("Unable to prepare private diagnostics. No ICT operation was started; this requires maintainer attention."))
		return
	}
	if pathProvider, ok := b.ICT.(interface{ DiagnosticPath(string) (string, error) }); ok {
		if path, err := pathProvider.DiagnosticPath(diagnosticID); err != nil {
			b.logf("list diagnostic path failed diagnostic_id=%q: %v", diagnosticID, err)
		} else {
			b.logf("list state_id=%q diagnostic_id=%q diagnostic_path=%q started", user, diagnosticID, path)
		}
	} else {
		b.logf("list diagnostic_id=%q state_id=%q started", diagnosticID, user)
	}
	defer func() {
		resolver, ok := b.ICT.(interface{ ResolveDiagnostic(string) error })
		if !ok {
			return
		}
		if err := resolver.ResolveDiagnostic(diagnosticID); err != nil {
			b.logf("list diagnostic resolution failed diagnostic_id=%q: %v", diagnosticID, err)
		}
	}()
	inventory, err := b.ICT.WorkspaceInventoryDiagnostic(ctx, user, diagnosticID)
	if err != nil {
		respond(rejectedText("Unable to list clusters. No cleanup is running; this requires maintainer attention. Diagnostic ID: " + lifecycle.DiagnosticReference(diagnosticID) + "."))
		return
	}
	if err := b.Lifecycles.Refresh(inventory); err != nil {
		b.logf("refresh lifecycle inventory: %v", err)
		respond(rejectedText("Unable to list clusters. No cleanup is running; this requires maintainer attention. Diagnostic ID: " + lifecycle.DiagnosticReference(diagnosticID) + "."))
		return
	}
	now := b.now()
	for _, message := range lifecycleListMessages(b.Lifecycles.Records(), user, now) {
		respond(message)
	}
}

func (b Bot) now() time.Time {
	if b.Clock != nil {
		return b.Clock()
	}
	return time.Now()
}

func (b Bot) respondHelp(text string, maintainer bool, respond func(string)) {
	words := strings.Fields(text)
	if len(words) > 2 {
		respond(unknownText())
		return
	}
	topic := ""
	if len(words) == 2 {
		topic = words[1]
	}
	var messages []string
	switch topic {
	case "":
		messages = helpOverview()
		if maintainer {
			messages = append(messages, "Maintainer DM commands\n```\n  status                show admission mode and activity counts\n  pause                 pause new modifying commands\n  unpause               resume modifying commands\n  stop                  drain active work and stop Servitor\n```")
		}
	case "create":
		messages = createHelp(b.Defaults)
	case "done":
		messages = []string{"`done` releases your resources. Use it in your lifecycle thread, or as `@servitor done` in the configured channel."}
	case "extend":
		messages = []string{"`extend [N[h]]` extends your ready lease by the configured duration or by 1 through 24 whole hours. Use it in your lifecycle thread or as `@servitor extend` in the configured channel."}
	case "list":
		messages = []string{"`list` shows available cluster state. Use it in a DM or as `@servitor list` in the configured channel."}
	default:
		respond(unknownText())
		return
	}
	for _, message := range messages {
		for _, chunk := range boundedMessages(message) {
			respond(chunk)
		}
	}
}

func channelCommand(text, self string) (string, bool) {
	if self == "" {
		return "", false
	}
	text = trimASCIIWhitespace(text)
	mention := "<@" + self + ">"
	if !strings.HasPrefix(text, mention) {
		return "", false
	}
	rest := strings.TrimPrefix(text, mention)
	if rest != "" && !isASCIIWhitespace(rune(rest[0])) {
		return "", false
	}
	return trimASCIIWhitespace(rest), true
}

func trimASCIIWhitespace(value string) string {
	return strings.TrimFunc(value, isASCIIWhitespace)
}

func isASCIIWhitespace(character rune) bool {
	return character == ' ' || character == '\t' || character == '\n' || character == '\r' || character == '\f' || character == '\v'
}

func firstToken(text string) string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return ""
	}
	return words[0]
}

func commandCategory(text string) string {
	switch command := firstToken(text); command {
	case "help", "list", "create", "done", "destroy", "extend", "yes", "no", "status", "pause", "unpause", "stop":
		return command
	default:
		return "unknown"
	}
}

func (b Bot) dmCommandAccepted(user, text, category string) bool {
	if user == b.MaintainerID && len(strings.Fields(text)) == 1 && (category == "status" || category == "pause" || category == "unpause" || category == "stop") {
		return true
	}
	return category == "help" || category == "list"
}

func (b Bot) channelCommandAccepted(category string, request lifecycle.Request) bool {
	if modifyingCommand(category) && !b.admissionOpen() {
		return false
	}
	switch category {
	case "help", "list":
		return true
	case "create":
		return !request.RawThread && b.Creator != nil
	case "done", "destroy":
		if request.RawThread {
			return b.Lifecycle != nil && b.Lifecycle.OwnsThread(request)
		}
		return b.Creator != nil || b.Lifecycle != nil
	case "extend":
		return b.Lifecycle != nil && (!request.RawThread || b.Lifecycle.OwnsThread(request))
	default:
		return false
	}
}

func (b Bot) logConfirmation(envelope Envelope, message Message, thread, category string, outcome lifecycle.ConfirmationOutcome) {
	b.logCommandResult(envelope, message, thread, category, confirmationAdmitted(outcome), confirmationResult(outcome))
}

func confirmationAdmitted(outcome lifecycle.ConfirmationOutcome) bool {
	return outcome == lifecycle.ConfirmationApproved || outcome == lifecycle.ConfirmationRejected
}

func confirmationResult(outcome lifecycle.ConfirmationOutcome) string {
	switch outcome {
	case lifecycle.ConfirmationNoLifecycle, lifecycle.ConfirmationWrongThread, lifecycle.ConfirmationPreReview,
		lifecycle.ConfirmationReview, lifecycle.ConfirmationApplying, lifecycle.ConfirmationReady,
		lifecycle.ConfirmationCleanup, lifecycle.ConfirmationUnresolved, lifecycle.ConfirmationApproved,
		lifecycle.ConfirmationRejected:
		return string(outcome)
	default:
		return "unknown"
	}
}

func (b Bot) logCommand(envelope Envelope, message Message, thread, category string, accepted bool) {
	b.logCommandResult(envelope, message, thread, category, accepted, "")
}

func (b Bot) logCommandResult(envelope Envelope, message Message, thread, category string, accepted bool, result string) {
	admission := "rejected"
	if accepted {
		admission = "accepted"
	}
	mode := b.admissionMode()
	if result == "" {
		b.logf("command event_id=%q user_id=%q channel_id=%q thread_id=%q category=%q admission=%s mode=%s", envelope.ID, message.User, message.Channel, thread, category, admission, mode)
		return
	}
	b.logf("command event_id=%q user_id=%q channel_id=%q thread_id=%q category=%q admission=%s result=%q mode=%s", envelope.ID, message.User, message.Channel, thread, category, admission, result, mode)
}

func (b Bot) admissionOpen() bool {
	return b.Admission == nil || b.Admission.Accepting()
}

func (b Bot) admissionStatus() admission.Status {
	if b.Admission == nil {
		return admission.Status{Mode: admission.Accepting}
	}
	return b.Admission.Status()
}

func (b Bot) admissionMode() admission.Mode { return b.admissionStatus().Mode }

func (b Bot) ownsThread(request lifecycle.Request) bool {
	if b.Lifecycle != nil && b.Lifecycle.OwnsThread(request) {
		return true
	}
	if b.Lifecycles == nil {
		return false
	}
	record, found := b.Lifecycles.Get(request.UserID)
	return found && record.Channel == request.Channel && record.ThreadTimestamp == request.ThreadTimestamp
}

func modifyingCommand(command string) bool {
	switch command {
	case "create", "yes", "no", "done", "destroy", "extend":
		return true
	default:
		return false
	}
}

func modeRejection(mode admission.Mode) string {
	return rejectedText("Servitor is " + string(mode) + ". Only help and list are available while admission is " + string(mode) + ".")
}

func maintainerStatus(status admission.Status) string {
	return fmt.Sprintf("Mode: %s\nPending reviews: %d\nActive applies: %d\nActive cleanup: %d", status.Mode, status.Reviews, status.Applies, status.Cleanup)
}

func unknownText() string {
	return "Command unknown.\n\n" + helpOverview()[0]
}

func rejectedText(reason string) string {
	return "Command rejected.\n\n" + reason
}

func helpOverview() []string {
	return []string{"Servitor provisions one temporary IBM Cloud cluster per Slack user.\n\nCommands\n```\nDM\n  help [command]          print help\n  list                    list clusters\n\nConfigured channel\n  @servitor help [command]  print help\n  @servitor create [flags]  provision a new cluster\n  @servitor done            release your resources\n  @servitor extend [N[h]]   extend your lease\n  @servitor list            list clusters\n\nLifecycle thread\n  yes                       approve the cluster plan\n  no                        reject the cluster plan\n  done                      release your resources\n  extend [N[h]]             extend your lease\n```"}
}

func createHelp(defaults command.CreateDefaults) []string {
	defaultVersion := safeHelpCell(defaults.Version)
	defaultTarget := safeHelpCell(defaults.Target)
	defaultProvider := safeHelpCell(defaults.Provider)
	return []string{
		"`create` starts planning from the configured channel root. Defaults: version " + defaultVersion + ", target " + defaultTarget + ", provider " + defaultProvider + ".\n\nExamples\n```\n@servitor create\n@servitor create --version 1.36\n@servitor create --version 4.22 --provider classic --datacenter dal10\n```\nReview the plan in its thread, then reply with exact `yes` or `no` within five minutes.",
		"Safe create flags\n```\nCommon\n  --target --provider --platform --version --resource-group --name --worker-count\n\nVPC Gen 2\n  --zone --flavor --vpc-id --subnet-id --public-gateway-id\n\nClassic\n  --datacenter --machine-type --public-vlan-id --private-vlan-id\n\nSatellite\n  --satellite-zone --satellite-managed-from --satellite-location-id\n  --satellite-host-image --satellite-host-profile --satellite-ssh-key-id\n  --satellite-worker-instance-id --satellite-worker-operating-system\n```",
	}
}

func safeHelpCell(value string) string {
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) || character == '`' || character == '<' || character == '>' || character == '@' {
			return ' '
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return "-"
	}
	if len([]rune(value)) > 160 {
		return string([]rune(value)[:157]) + "..."
	}
	return value
}

func boundedMessages(text string) []string {
	if len(text) <= maxSlackMessage {
		return []string{text}
	}
	lines := strings.SplitAfter(text, "\n")
	chunks := make([]string, 0, len(lines)/2+1)
	var current strings.Builder
	inFence := false
	flush := func() {
		if current.Len() == 0 {
			return
		}
		if inFence {
			current.WriteString("```\n")
		}
		chunks = append(chunks, current.String())
		current.Reset()
		if inFence {
			current.WriteString("```\n")
		}
	}
	for _, line := range lines {
		if len(line) > maxSlackMessage-8 {
			flush()
			for len(line) > maxSlackMessage-8 {
				chunk, rest := utf8Prefix(line, maxSlackMessage-8)
				if inFence {
					chunks = append(chunks, "```\n"+chunk+"\n```")
				} else {
					chunks = append(chunks, chunk)
				}
				line = rest
			}
		}
		lineFence := strings.Count(line, "```")%2 != 0
		reserve := 0
		if inFence != lineFence {
			reserve = 4
		}
		if current.Len() > 0 && current.Len()+len(line)+reserve > maxSlackMessage {
			flush()
		}
		current.WriteString(line)
		if lineFence {
			inFence = !inFence
		}
	}
	if current.Len() > 0 {
		if inFence {
			current.WriteString("```\n")
		}
		chunks = append(chunks, current.String())
	}
	return chunks
}

func utf8Prefix(value string, limit int) (string, string) {
	end := 0
	for _, character := range value {
		size := utf8.RuneLen(character)
		if end+size > limit {
			break
		}
		end += size
	}
	return value[:end], value[end:]
}

func (b Bot) logf(format string, args ...any) {
	if b.Logf != nil {
		b.Logf(format, args...)
		return
	}
	log.Printf("slackbot: "+format, args...)
}

// Package lifecycle owns per-user ICT destruction and bounded cleanup retries.
package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bevicted/servitor/internal/diagnostics"
	"github.com/bevicted/servitor/internal/state"
)

const (
	statusCleanup    = "cleanup"
	statusResolved   = "resolved"
	statusUnresolved = "unresolved"
)

// ICTClient is the restricted destructive ICT boundary. Lifecycle operations
// always use a private diagnostic writer.
type ICTClient interface {
	WorkspaceInventoryDiagnostic(context.Context, string, string) (state.WorkspaceInventory, error)
	DestroyDiagnostic(context.Context, string, string) error
}

// Clock makes retry timing deterministic in tests.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                             { return time.Now() }
func (realClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

// Request identifies the caller-owned lifecycle and its Slack thread.
type Request struct {
	UserID          string
	Channel         string
	ThreadTimestamp string
	RawThread       bool
}

// Notice is a concise lifecycle message for the initiating Slack thread.
type Notice struct {
	Channel         string
	ThreadTimestamp string
	Text            string
}

// Notifier delivers lifecycle notices without coupling this package to Slack.
type Notifier func(context.Context, Notice) error

// Destroyer starts caller-owned cleanup and owns ready-lease scheduling.
type Destroyer interface {
	RequestDestroy(context.Context, Request, Notifier) error
	ScheduleLease(Request, state.LifecycleRecord, Notifier)
}

// ExtensionResult describes a durably persisted lease extension.
type ExtensionResult struct {
	OldExpiry time.Time
	NewExpiry time.Time
	Added     time.Duration
	Clamped   bool
}

// Manager serializes cleanup per user and records every transition before retrying.
type Manager struct {
	ICT               ICTClient
	Store             *state.LifecycleStore
	Clock             Clock
	RetryIntervals    []time.Duration
	MaintainerID      string
	Diagnostics       *diagnostics.Logger
	Logf              func(string, ...any)
	OnCleanupActivity func(bool)
	Lease             time.Duration

	mu          sync.Mutex
	running     map[string]bool
	leases      map[string]uint64
	transitions map[string]*sync.Mutex
}

// NewManager builds a cleanup manager with the approved retry schedule.
func NewManager(client ICTClient, store *state.LifecycleStore, retryIntervals []time.Duration, maintainerID string) *Manager {
	return &Manager{ICT: client, Store: store, RetryIntervals: append([]time.Duration(nil), retryIntervals...), MaintainerID: maintainerID, running: make(map[string]bool), leases: make(map[string]uint64), transitions: make(map[string]*sync.Mutex)}
}

// Reconcile restores all valid persisted lifecycles from one ICT workspace snapshot.
// It never derives a command path from a record; state validates each ID before exposing it.
func (m *Manager) Reconcile(ctx context.Context, notify Notifier) error {
	if m.ICT == nil || m.Store == nil {
		return fmt.Errorf("reconcile: manager is not configured")
	}
	diagnosticID, err := diagnostics.NewID()
	if err != nil {
		return fmt.Errorf("create reconciliation diagnostic ID: %w", err)
	}
	defer m.resolveDiagnostic(diagnosticID, "reconciliation")
	m.logDiagnostic("reconciliation", diagnosticID, "reconciliation started")
	workspaces, err := m.workspaces(ctx, "reconciliation", diagnosticID)
	if err != nil {
		return fmt.Errorf("reconcile ICT workspaces: %w", err)
	}
	records := m.Store.Records()
	known := make(map[string]bool, len(records))
	for _, record := range records {
		known[record.UserID] = true
		request := requestFor(record)
		present := workspaces[record.UserID]
		switch record.Status {
		case statusReview, statusApplying:
			if present {
				m.resumeCleanup(request, record, notify, true)
			} else {
				m.markUnresolved(record, request, notify, "ICT workspace disappeared while creation was incomplete")
			}
		case statusReady:
			if !present {
				m.markUnresolved(record, request, notify, "ICT workspace disappeared while resources may still exist")
				continue
			}
			if !record.LeaseExpiresAt.After(m.now()) {
				m.resumeCleanup(request, record, notify, true)
			} else {
				m.ScheduleLease(request, record, notify)
			}
		case statusCleanup:
			if !present {
				m.resolveAfterAbsentWorkspace(record, request, notify)
				continue
			}
			m.resumeCleanup(request, record, notify, false)
		case statusUnresolved:
			// A final failed cleanup remains blocked until a maintainer investigates it.
		}
	}
	for _, workspace := range m.Store.CorruptWorkspaceIDs() {
		m.logf("workspace-local lifecycle state for %q is corrupt; retaining private investigation trail", workspace)
	}
	for workspace := range workspaces {
		if known[workspace] {
			continue
		}
		if stateID(workspace) {
			m.logf("record-less Servitor workspace %q found after restart; starting conservative cleanup", workspace)
			m.cleanupRecordlessWorkspace(workspace)
			continue
		}
		m.logf("unknown ICT workspace %q retained for maintainer review", workspace)
	}
	return nil
}

// OwnsThread reports whether request is the persisted lifecycle owner's thread.
func (m *Manager) OwnsThread(request Request) bool {
	record, found := m.Store.Get(request.UserID)
	return found && record.Channel == request.Channel && record.ThreadTimestamp == request.ThreadTimestamp
}

// RequestDestroy durably admits cleanup, then returns before any ICT operation.
// ICT work deliberately uses a background context so Slack cancellation cannot stop cleanup.
func (m *Manager) RequestDestroy(_ context.Context, request Request, notify Notifier) error {
	if request.UserID == "" || request.Channel == "" || request.ThreadTimestamp == "" {
		return fmt.Errorf("start cleanup: incomplete Slack request")
	}
	if m.ICT == nil || m.Store == nil {
		return fmt.Errorf("start cleanup: manager is not configured")
	}
	transition := m.transition(request.UserID)
	transition.Lock()
	defer transition.Unlock()
	return m.requestDestroy(request, notify)
}

func (m *Manager) requestDestroy(request Request, notify Notifier) error {
	prior, hadPrior := m.Store.Get(request.UserID)
	if request.RawThread && (!hadPrior || prior.Channel != request.Channel || prior.ThreadTimestamp != request.ThreadTimestamp) {
		return nil
	}
	notificationRequest := request
	if hadPrior {
		notificationRequest = requestFor(prior)
	}
	if hadPrior && prior.Status == statusUnresolved {
		m.notice(notify, notificationRequest, m.unresolvedTextFor(prior))
		return nil
	}
	if !m.claimCleanup(request.UserID) {
		m.notice(notify, notificationRequest, "Cleanup is already in progress.")
		return nil
	}

	record := state.LifecycleRecord{UserID: request.UserID, Channel: notificationRequest.Channel, ThreadTimestamp: notificationRequest.ThreadTimestamp, Status: statusCleanup, UpdatedAt: m.now()}
	if hadPrior {
		record = prior
		record.Status = statusCleanup
		record.RetryCount = 0
		record.NextRetryAt = time.Time{}
		record.LeaseExpiresAt = time.Time{}
		record.LastError = ""
		record.UpdatedAt = m.now()
	}
	if hadPrior && validDiagnosticID(prior.DiagnosticRef) {
		record.DiagnosticRef = prior.DiagnosticRef
	} else {
		id, err := diagnostics.NewID()
		if err != nil {
			m.releaseCleanup(request.UserID)
			return err
		}
		record.DiagnosticRef = id
	}
	m.logDiagnostic(request.UserID, record.DiagnosticRef, "lifecycle transition=cleanup")
	m.event(record, "cleanup lifecycle admitted")
	if err := m.Store.Put(record); err != nil {
		m.notice(notify, notificationRequest, "Cleanup did not start because Servitor could not persist its lifecycle. Cleanup requires maintainer attention."+m.diagnosticSuffix(record))
		m.releaseCleanup(request.UserID)
		return fmt.Errorf("persist cleanup admission: %w", err)
	}
	go m.startCleanup(request.UserID, notificationRequest, record, notify)
	return nil
}

func (m *Manager) startCleanup(userID string, request Request, record state.LifecycleRecord, notify Notifier) {
	m.event(record, "cleanup preflight started")
	workspaces, err := m.workspaces(context.Background(), userID, record.DiagnosticRef)
	if err != nil {
		m.event(record, "cleanup preflight failed: "+err.Error())
		m.finalizeAdmissionFailure(record, request, notify, "Unable to check existing ICT state. Cleanup requires maintainer attention.")
		return
	}
	if !workspaces[userID] {
		m.resolveDiagnostic(record.DiagnosticRef, userID)
		m.Store.RemoveAbsentWorkspace(userID)
		m.notice(notify, request, "There is nothing to destroy.")
		m.releaseCleanup(userID)
		return
	}
	if current, found := m.Store.Get(userID); found {
		record = current
	}
	m.notice(notify, request, "Starting cleanup.")
	m.attempt(request, record, notify)
}

func (m *Manager) finalizeAdmissionFailure(record state.LifecycleRecord, request Request, notify Notifier, text string) {
	record.Status = statusUnresolved
	record.LastError = "cleanup preflight failed"
	record.NextRetryAt = time.Time{}
	record.UpdatedAt = m.now()
	if err := m.Store.Put(record); err != nil {
		m.logf("persist unresolved cleanup for %q: %v", record.UserID, err)
	}
	m.notice(notify, request, text+m.diagnosticSuffix(record))
	m.releaseCleanup(record.UserID)
}

func (m *Manager) resumeCleanup(request Request, record state.LifecycleRecord, notify Notifier, immediate bool) {
	if !m.claimCleanup(request.UserID) {
		return
	}
	if !validDiagnosticID(record.DiagnosticRef) {
		id, err := diagnostics.NewID()
		if err != nil {
			m.logf("create cleanup diagnostic ID for %q: %v", request.UserID, err)
			m.releaseCleanup(request.UserID)
			return
		}
		record.DiagnosticRef = id
	}
	record.Status = statusCleanup
	record.LeaseExpiresAt = time.Time{}
	record.UpdatedAt = m.now()
	if err := m.Store.Put(record); err != nil {
		m.event(record, "resume cleanup persistence failed: "+err.Error())
		m.logf("persist resumed cleanup for %q: %v", request.UserID, err)
		m.notice(notify, request, "Cleanup could not resume after restart because Servitor could not persist its lifecycle. Cleanup requires maintainer attention."+m.diagnosticSuffix(record))
		m.releaseCleanup(request.UserID)
		return
	}
	m.notice(notify, request, "Starting cleanup after restart.")
	if immediate || record.NextRetryAt.IsZero() || !record.NextRetryAt.After(m.now()) {
		m.attempt(request, record, notify)
		return
	}
	m.scheduleRetry(request, notify, record.NextRetryAt)
}

// ScheduleLease replaces the caller's existing ready-lease timer. It admits
// only the latest persisted ready deadline while serializing with extensions.
func (m *Manager) ScheduleLease(request Request, record state.LifecycleRecord, notify Notifier) {
	transition := m.transition(request.UserID)
	transition.Lock()
	defer transition.Unlock()
	m.scheduleLease(request, record, notify)
}

// scheduleLease requires the caller to hold the user's transition lock.
func (m *Manager) scheduleLease(request Request, record state.LifecycleRecord, notify Notifier) {
	if m.Store == nil || record.Status != statusReady || record.LeaseExpiresAt.IsZero() {
		return
	}
	current, found := m.Store.Get(request.UserID)
	if !found || current.Status != statusReady || !current.LeaseExpiresAt.Equal(record.LeaseExpiresAt) {
		return
	}
	m.mu.Lock()
	if m.running[request.UserID] {
		m.mu.Unlock()
		return
	}
	generation := m.leases[request.UserID] + 1
	m.leases[request.UserID] = generation
	m.mu.Unlock()

	delay := record.LeaseExpiresAt.Sub(m.now())
	var expired <-chan time.Time
	if delay > 0 {
		expired = m.clock().After(delay)
	}
	go func() {
		if expired != nil {
			<-expired
		}
		transition := m.transition(request.UserID)
		transition.Lock()
		defer transition.Unlock()
		m.mu.Lock()
		currentGeneration := m.leases[request.UserID]
		m.mu.Unlock()
		if currentGeneration != generation {
			return
		}
		current, found := m.Store.Get(request.UserID)
		if !found || current.Status != statusReady || !current.LeaseExpiresAt.Equal(record.LeaseExpiresAt) {
			return
		}
		if err := m.requestDestroy(requestFor(current), notify); err == nil {
			m.notice(notify, requestFor(current), "Lease expired; starting cleanup.")
		}
	}()
}

// Extend advances a ready owner's persisted lease, retaining a rolling 24-hour
// maximum remaining duration. Persistence precedes timer replacement and notice.
func (m *Manager) Extend(_ context.Context, request Request, increment time.Duration, notify Notifier) (ExtensionResult, error) {
	if m.Store == nil {
		return ExtensionResult{}, fmt.Errorf("lease extension is unavailable")
	}
	if increment == 0 {
		increment = m.Lease
	}
	if increment < time.Hour || increment > 24*time.Hour || increment%time.Hour != 0 {
		return ExtensionResult{}, fmt.Errorf("lease extension is unavailable")
	}
	transition := m.transition(request.UserID)
	transition.Lock()
	defer transition.Unlock()
	record, found := m.Store.Get(request.UserID)
	if !found || record.Channel != request.Channel {
		return ExtensionResult{}, fmt.Errorf("no ready lifecycle is available to extend")
	}
	if request.RawThread && record.ThreadTimestamp != request.ThreadTimestamp {
		return ExtensionResult{}, fmt.Errorf("no ready lifecycle is available to extend")
	}
	if record.Status != statusReady || !record.LeaseExpiresAt.After(m.now()) {
		return ExtensionResult{}, fmt.Errorf("only an unexpired ready lifecycle can be extended")
	}
	maximum := m.now().Add(24 * time.Hour)
	if !record.LeaseExpiresAt.Before(maximum) {
		return ExtensionResult{}, fmt.Errorf("the lease already has the maximum 24 hours remaining")
	}
	requested := record.LeaseExpiresAt.Add(increment)
	newExpiry := requested
	clamped := false
	if requested.After(maximum) {
		newExpiry = maximum
		clamped = true
	}
	updated := record
	updated.LeaseExpiresAt = newExpiry
	updated.UpdatedAt = m.now()
	if err := m.Store.Put(updated); err != nil {
		return ExtensionResult{}, fmt.Errorf("could not persist the lease extension")
	}
	m.scheduleLease(requestFor(updated), updated, notify)
	result := ExtensionResult{OldExpiry: record.LeaseExpiresAt, NewExpiry: newExpiry, Added: newExpiry.Sub(record.LeaseExpiresAt), Clamped: clamped}
	m.notice(notify, requestFor(updated), extensionSuccessText(result, m.now()))
	return result, nil
}

func (m *Manager) attempt(request Request, record state.LifecycleRecord, notify Notifier) {
	attempt := record.RetryCount + 1
	if record.RetryCount == 0 {
		m.notice(notify, request, "Cleanup attempt 1 started.")
	} else {
		m.notice(notify, request, fmt.Sprintf("Cleanup retry %d of %d started.", record.RetryCount, len(m.RetryIntervals)))
	}

	m.event(record, fmt.Sprintf("cleanup attempt %d started", attempt))
	err := m.ICT.DestroyDiagnostic(context.Background(), request.UserID, record.DiagnosticRef)
	if err == nil {
		var workspaces map[string]bool
		workspaces, err = m.workspaces(context.Background(), request.UserID, record.DiagnosticRef)
		if err == nil && workspaces[request.UserID] {
			err = fmt.Errorf("ict still lists this state after destroy")
		}
	}
	if err == nil {
		// ICT removed the workspace, including its lifecycle record. Do not recreate
		// a resolved record in an absent workspace.
		m.event(record, "cleanup resolved; workspace absent")
		m.resolveDiagnostic(record.DiagnosticRef, record.UserID)
		m.Store.RemoveAbsentWorkspace(record.UserID)
		m.notice(notify, request, "Cleanup complete.")
		m.releaseCleanup(request.UserID)
		return
	}

	record.LastError = err.Error()
	m.event(record, "cleanup attempt failed: "+err.Error())
	record.UpdatedAt = m.now()
	if record.RetryCount >= len(m.RetryIntervals) {
		record.Status = statusUnresolved
		record.NextRetryAt = time.Time{}
		if saveErr := m.Store.Put(record); saveErr != nil {
			m.notice(notify, request, "Cleanup remains unresolved. The operator must inspect the private server logs."+m.diagnosticSuffix(record))
			m.releaseCleanup(request.UserID)
			return
		}
		m.notice(notify, request, m.unresolvedTextFor(record))
		m.releaseCleanup(request.UserID)
		return
	}

	delay := m.RetryIntervals[record.RetryCount]
	record.RetryCount++
	record.NextRetryAt = m.now().Add(delay)
	if saveErr := m.Store.Put(record); saveErr != nil {
		m.notice(notify, request, "Cleanup remains blocked because Servitor could not persist retry state. The operator must inspect the private server logs."+m.diagnosticSuffix(record))
		m.releaseCleanup(request.UserID)
		return
	}
	m.notice(notify, request, fmt.Sprintf("Cleanup attempt %d failed. Retrying in %s.%s", attempt, delay, m.diagnosticSuffix(record)))
	m.scheduleRetry(request, notify, record.NextRetryAt)
}

func (m *Manager) scheduleRetry(request Request, notify Notifier, at time.Time) {
	go func() {
		delay := at.Sub(m.now())
		if delay > 0 {
			<-m.clock().After(delay)
		}
		m.mu.Lock()
		running := m.running[request.UserID]
		m.mu.Unlock()
		if !running {
			return
		}
		record, found := m.Store.Get(request.UserID)
		if !found || record.Status != statusCleanup || !record.NextRetryAt.Equal(at) {
			return
		}
		m.attempt(request, record, notify)
	}()
}

func (m *Manager) resolveAfterAbsentWorkspace(record state.LifecycleRecord, request Request, notify Notifier) {
	m.event(record, "workspace absent; lifecycle resolved without record rewrite")
	m.resolveDiagnostic(record.DiagnosticRef, record.UserID)
	m.Store.RemoveAbsentWorkspace(record.UserID)
	m.notice(notify, request, "Cleanup complete.")
}

func (m *Manager) markUnresolved(record state.LifecycleRecord, request Request, notify Notifier, reason string) {
	if record.Status == statusUnresolved {
		return
	}
	record.Status = statusUnresolved
	record.LastError = reason
	record.NextRetryAt = time.Time{}
	record.LeaseExpiresAt = time.Time{}
	record.UpdatedAt = m.now()
	if err := m.Store.Put(record); err != nil {
		m.logf("persist unresolved lifecycle for %q: %v", request.UserID, err)
		return
	}
	m.notice(notify, request, m.unresolvedTextFor(record))
}

func (m *Manager) workspaces(ctx context.Context, lifecycleStateID, diagnosticID string) (map[string]bool, error) {
	if !validDiagnosticID(diagnosticID) {
		return nil, fmt.Errorf("invalid diagnostic ID")
	}
	inventory, err := m.ICT.WorkspaceInventoryDiagnostic(ctx, lifecycleStateID, diagnosticID)
	if err != nil {
		return nil, err
	}
	if err := m.Store.Refresh(inventory); err != nil {
		return nil, fmt.Errorf("refresh workspace-local lifecycle state: %w", err)
	}
	workspaces := make(map[string]bool, len(inventory.Workspaces))
	for _, workspace := range inventory.Workspaces {
		workspaces[workspace.ID] = true
	}
	return workspaces, nil
}

// cleanupRecordlessWorkspace is intentionally record-free: a Slack-ID workspace
// without Servitor metadata may be an interrupted create, but its notification
// route and remote state are unknown. Private diagnostics are the evidence trail.
func (m *Manager) cleanupRecordlessWorkspace(userID string) {
	if !m.claimCleanup(userID) {
		return
	}
	defer m.releaseCleanup(userID)
	id, err := diagnostics.NewID()
	if err != nil {
		m.logf("create record-less cleanup diagnostic for %q: %v", userID, err)
		return
	}
	if err := m.ICT.DestroyDiagnostic(context.Background(), userID, id); err != nil {
		m.logf("record-less workspace cleanup for %q failed: %v (diagnostic %s)", userID, err, id)
		return
	}
	workspaces, err := m.workspaces(context.Background(), userID, id)
	if err != nil {
		m.logf("verify record-less workspace cleanup for %q: %v (diagnostic %s)", userID, err, id)
		return
	}
	if workspaces[userID] {
		m.logf("record-less workspace %q remains after cleanup (diagnostic %s)", userID, id)
		return
	}
	m.resolveDiagnostic(id, userID)
	m.logf("record-less workspace %q removed after restart reconciliation (diagnostic %s)", userID, id)
}

func stateID(value string) bool {
	if len(value) < 2 || len(value) > 128 || (value[0] != 'U' && value[0] != 'W') {
		return false
	}
	for _, character := range value[1:] {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') {
			continue
		}
		return false
	}
	return true
}

func requestFor(record state.LifecycleRecord) Request {
	return Request{UserID: record.UserID, Channel: record.Channel, ThreadTimestamp: record.ThreadTimestamp}
}

func (m *Manager) transition(userID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.transitions == nil {
		m.transitions = make(map[string]*sync.Mutex)
	}
	transition := m.transitions[userID]
	if transition == nil {
		transition = &sync.Mutex{}
		m.transitions[userID] = transition
	}
	return transition
}

func (m *Manager) claimCleanup(userID string) bool {
	m.mu.Lock()
	if m.running[userID] {
		m.mu.Unlock()
		return false
	}
	delete(m.leases, userID)
	m.running[userID] = true
	m.mu.Unlock()
	if m.OnCleanupActivity != nil {
		m.OnCleanupActivity(true)
	}
	return true
}

func (m *Manager) releaseCleanup(userID string) {
	m.mu.Lock()
	if !m.running[userID] {
		m.mu.Unlock()
		return
	}
	delete(m.running, userID)
	m.mu.Unlock()
	if m.OnCleanupActivity != nil {
		m.OnCleanupActivity(false)
	}
}

func (m *Manager) unresolvedTextFor(record state.LifecycleRecord) string {
	text := "Cleanup remains unresolved."
	if m.MaintainerID == "" {
		text += " The operator must investigate with `ict list` and `ict destroy SLACK_USER_ID` on the private server."
	} else {
		text += " <@" + m.MaintainerID + "> must investigate with `ict list` and `ict destroy SLACK_USER_ID` on the private server."
	}
	if validDiagnosticID(record.DiagnosticRef) {
		text += " Diagnostic ID: " + DiagnosticReference(record.DiagnosticRef) + "."
	}
	return text
}

func validDiagnosticID(id string) bool {
	if len(id) != 24 {
		return false
	}
	for _, character := range id {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func DiagnosticReference(id string) string {
	return "`" + id + "`"
}

func (m *Manager) diagnosticSuffix(record state.LifecycleRecord) string {
	if validDiagnosticID(record.DiagnosticRef) {
		return " Diagnostic ID: " + DiagnosticReference(record.DiagnosticRef)
	}
	return ""
}

func (m *Manager) event(record state.LifecycleRecord, message string) {
	if m.Diagnostics == nil || !validDiagnosticID(record.DiagnosticRef) {
		return
	}
	writer, err := m.Diagnostics.Writer(record.DiagnosticRef, record.UserID)
	if err == nil {
		writer.Event(message)
	}
}

func (m *Manager) notice(notify Notifier, request Request, text string) {
	if notify != nil {
		_ = notify(context.Background(), Notice{Channel: request.Channel, ThreadTimestamp: request.ThreadTimestamp, Text: text})
	}
}

func (m *Manager) resolveDiagnostic(id, stateID string) {
	if m.Diagnostics == nil || !diagnostics.ValidID(id) {
		return
	}
	if err := m.Diagnostics.Resolve(id); err != nil {
		m.logf("diagnostic resolution failed state_id=%q diagnostic_id=%q: %v", stateID, id, err)
		return
	}
	m.logDiagnostic(stateID, id, "diagnostic resolved")
}

func (m *Manager) logDiagnostic(stateID, id, message string) {
	if m.Diagnostics == nil || !diagnostics.ValidID(id) {
		return
	}
	path, err := m.Diagnostics.Path(id)
	if err != nil {
		m.logf("diagnostic path failed state_id=%q diagnostic_id=%q: %v", stateID, id, err)
		return
	}
	m.logf("lifecycle state_id=%q diagnostic_id=%q diagnostic_path=%q %s", stateID, id, path, message)
}

func (m *Manager) logf(format string, args ...any) {
	if m.Logf != nil {
		m.Logf(format, args...)
	}
}

func (m *Manager) now() time.Time { return m.clock().Now() }

func (m *Manager) clock() Clock {
	if m.Clock != nil {
		return m.Clock
	}
	return realClock{}
}

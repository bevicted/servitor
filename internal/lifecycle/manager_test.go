package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/admission"
	"github.com/bevicted/servitor/internal/diagnostics"
	"github.com/bevicted/servitor/internal/state"
)

type fakeICT struct {
	mu                        sync.Mutex
	states                    map[string]bool
	listErr                   error
	destroyErrors             []error
	destroyCalls              []string
	listCalls                 int
	listDiagnosticCalls       []diagnosticListCall
	destroyStarted            chan struct{}
	releaseDestroy            <-chan struct{}
	inventoryStarted          chan struct{}
	releaseInventory          <-chan struct{}
	preserveState             bool
	rejectCanceledListContext bool
	inventory                 state.WorkspaceInventory
}

type diagnosticListCall struct {
	stateID      string
	diagnosticID string
}

func (f *fakeICT) List(ctx context.Context) (string, error) {
	return f.list(ctx, diagnosticListCall{})
}

func (f *fakeICT) ListDiagnostic(ctx context.Context, stateID, diagnosticID string) (string, error) {
	return f.list(ctx, diagnosticListCall{stateID: stateID, diagnosticID: diagnosticID})
}

func (f *fakeICT) WorkspaceInventoryDiagnostic(ctx context.Context, stateID, diagnosticID string) (state.WorkspaceInventory, error) {
	if f.inventoryStarted != nil {
		select {
		case f.inventoryStarted <- struct{}{}:
		default:
		}
	}
	if f.releaseInventory != nil {
		<-f.releaseInventory
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	if f.rejectCanceledListContext && ctx.Err() != nil {
		return state.WorkspaceInventory{}, ctx.Err()
	}
	f.listDiagnosticCalls = append(f.listDiagnosticCalls, diagnosticListCall{stateID: stateID, diagnosticID: diagnosticID})
	if f.listErr != nil {
		return state.WorkspaceInventory{}, f.listErr
	}
	workspaces := make([]state.WorkspaceLocation, 0, len(f.states))
	for _, workspace := range f.inventory.Workspaces {
		if f.states[workspace.ID] {
			workspaces = append(workspaces, workspace)
		}
	}
	return state.WorkspaceInventory{Version: f.inventory.Version, StateRoot: f.inventory.StateRoot, Workspaces: workspaces}, nil
}

func (f *fakeICT) list(ctx context.Context, call diagnosticListCall) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	if f.rejectCanceledListContext && ctx.Err() != nil {
		return "", ctx.Err()
	}
	if call.diagnosticID != "" {
		f.listDiagnosticCalls = append(f.listDiagnosticCalls, call)
	}
	if f.listErr != nil {
		return "", f.listErr
	}
	var output strings.Builder
	for userID, present := range f.states {
		if present {
			output.WriteString(userID)
			output.WriteByte('\n')
		}
	}
	return output.String(), nil
}

func (f *fakeICT) Destroy(_ context.Context, userID string) error {
	if f.destroyStarted != nil {
		select {
		case f.destroyStarted <- struct{}{}:
		default:
		}
	}
	if f.releaseDestroy != nil {
		<-f.releaseDestroy
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroyCalls = append(f.destroyCalls, userID)
	if len(f.destroyErrors) != 0 {
		err := f.destroyErrors[0]
		f.destroyErrors = f.destroyErrors[1:]
		if err != nil {
			return err
		}
	}
	if !f.preserveState {
		delete(f.states, userID)
	}
	return nil
}

func (f *fakeICT) DestroyDiagnostic(ctx context.Context, userID, _ string) error {
	return f.Destroy(ctx, userID)
}

func (f *fakeICT) HasState(_ context.Context, userID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states[userID], nil
}

func (f *fakeICT) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.destroyCalls...)
}

func (f *fakeICT) lists() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

func (f *fakeICT) diagnosticLists() []diagnosticListCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]diagnosticListCall(nil), f.listDiagnosticCalls...)
}

type fakeTimer struct {
	delay time.Duration
	ch    chan time.Time
}

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) After(delay time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.timers = append(c.timers, fakeTimer{delay: delay, ch: ch})
	return ch
}
func (c *fakeClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}
func (c *fakeClock) advance(t *testing.T, delay time.Duration) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		if len(c.timers) != 0 {
			timer := c.timers[0]
			c.timers = c.timers[1:]
			if timer.delay != delay {
				c.mu.Unlock()
				t.Fatalf("scheduled retry = %s, want %s", timer.delay, delay)
			}
			c.now = c.now.Add(delay)
			c.mu.Unlock()
			timer.ch <- c.now
			return
		}
		c.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("retry timer was not scheduled")
		}
		time.Sleep(time.Millisecond)
	}
}

type notices struct {
	mu    sync.Mutex
	items []Notice
}

func (n *notices) send(_ context.Context, notice Notice) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.items = append(n.items, notice)
	return nil
}
func (n *notices) texts() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	texts := make([]string, len(n.items))
	for index, notice := range n.items {
		texts[index] = notice.Text
	}
	return texts
}

func TestExtendPersistsBeforeReplacingTimerAndHonorsRollingCap(t *testing.T) {
	manager, client, store, clock, sent := newManager(t, []error{nil})
	manager.Lease = 4 * time.Hour
	record := lifecycleRecord("U1", statusReady)
	record.LeaseExpiresAt = clock.now.Add(4 * time.Hour)
	putRecord(t, store, record)
	manager.ScheduleLease(request("U1"), record, sent.send)

	result, err := manager.Extend(context.Background(), request("U1"), 0, sent.send)
	if err != nil || !result.NewExpiry.Equal(clock.now.Add(8*time.Hour)) || result.Added != 4*time.Hour || result.Clamped {
		t.Fatalf("bare extension = %+v, %v", result, err)
	}
	assertNotices(t, sent.texts(), "Lease extended.\n\n```\nPrevious expiry: 2026-09-04 16:00:00 UTC (~4h)\nNew expiry:      2026-09-04 20:00:00 UTC (~8h)\nAdded:           4h\n```\nRemaining lease time is capped at 24 hours.")
	result, err = manager.Extend(context.Background(), request("U1"), 24*time.Hour, sent.send)
	if err != nil || !result.NewExpiry.Equal(clock.now.Add(24*time.Hour)) || result.Added != 16*time.Hour || !result.Clamped {
		t.Fatalf("clamped extension = %+v, %v", result, err)
	}
	assertNotices(t, sent.texts(), "Lease extended.\n\n```\nPrevious expiry: 2026-09-04 16:00:00 UTC (~4h)\nNew expiry:      2026-09-04 20:00:00 UTC (~8h)\nAdded:           4h\n```\nRemaining lease time is capped at 24 hours.", "Lease extended.\n\n```\nPrevious expiry: 2026-09-04 20:00:00 UTC (~8h)\nNew expiry:      2026-09-05 12:00:00 UTC (~24h)\nAdded:           16h\n```\nRemaining lease time is capped at 24 hours.")
	if _, err := manager.Extend(context.Background(), request("U1"), time.Hour, sent.send); err == nil {
		t.Fatal("extension at rolling cap succeeded")
	}
	stored, found := store.Get("U1")
	if !found || !stored.LeaseExpiresAt.Equal(clock.now.Add(24*time.Hour)) {
		t.Fatalf("stored expiry = %+v, found=%v", stored, found)
	}

	// The initial timer fires first but has an older generation and cannot clean up.
	clock.advance(t, 4*time.Hour)
	time.Sleep(10 * time.Millisecond)
	if got := client.calls(); len(got) != 0 {
		t.Fatalf("stale timer started cleanup: %v", got)
	}

	workspace, ok := store.WorkspacePath("U1")
	if !ok {
		t.Fatal("workspace absent")
	}
	if err := os.Rename(workspace, workspace+"-missing"); err != nil {
		t.Fatal(err)
	}
	prior := stored
	if _, err := manager.Extend(context.Background(), request("U1"), time.Hour, sent.send); err == nil {
		t.Fatal("extension with persistence failure succeeded")
	}
	after, found := store.Get("U1")
	if !found || !after.LeaseExpiresAt.Equal(prior.LeaseExpiresAt) {
		t.Fatalf("persistence failure changed deadline: %+v, found=%v", after, found)
	}
}

func TestLeaseScheduleRejectsStaleCreateDeadlineAfterExtension(t *testing.T) {
	manager, client, store, clock, sent := newManager(t, []error{nil})
	manager.Lease = 4 * time.Hour
	created := lifecycleRecord("U1", statusReady)
	created.LeaseExpiresAt = clock.now.Add(4 * time.Hour)
	putRecord(t, store, created)

	result, err := manager.Extend(context.Background(), request("U1"), 0, sent.send)
	if err != nil || !result.NewExpiry.Equal(clock.now.Add(8*time.Hour)) {
		t.Fatalf("extension = %+v, %v", result, err)
	}
	// Simulate create finishing its post-persistence scheduling after the
	// extension. It must not supersede the newly persisted deadline.
	manager.ScheduleLease(request("U1"), created, sent.send)
	if got := clock.timerCount(); got != 1 {
		t.Fatalf("effective lease timers = %d, want 1", got)
	}

	clock.advance(t, 8*time.Hour)
	waitForCalls(t, client, 1)
}

func TestLeaseExpiryAdmissionPreventsConcurrentExtension(t *testing.T) {
	manager, client, store, clock, _ := newManager(t, []error{nil})
	releaseInventory := make(chan struct{})
	var releaseInventoryOnce sync.Once
	client.releaseInventory = releaseInventory
	t.Cleanup(func() { releaseInventoryOnce.Do(func() { close(releaseInventory) }) })

	expiryNotified := make(chan struct{}, 1)
	releaseExpiryNotice := make(chan struct{})
	notify := func(_ context.Context, notice Notice) error {
		if notice.Text == "Lease expired; starting cleanup." {
			expiryNotified <- struct{}{}
			<-releaseExpiryNotice
		}
		return nil
	}
	record := lifecycleRecord("U1", statusReady)
	record.LeaseExpiresAt = clock.now.Add(time.Hour)
	putRecord(t, store, record)
	manager.ScheduleLease(request("U1"), record, notify)

	clock.advance(t, time.Hour)
	select {
	case <-expiryNotified:
	case <-time.After(time.Second):
		t.Fatal("lease expiry was not admitted")
	}
	admitted, found := store.Get("U1")
	if !found || admitted.Status != statusCleanup {
		t.Fatalf("expiry admission = %+v, found=%v", admitted, found)
	}

	extensionDone := make(chan error, 1)
	go func() {
		_, err := manager.Extend(context.Background(), request("U1"), time.Hour, notify)
		extensionDone <- err
	}()
	select {
	case err := <-extensionDone:
		t.Fatalf("extension completed while expiry admission held the transition: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseExpiryNotice)
	if err := <-extensionDone; err == nil {
		t.Fatal("extension overwrote admitted cleanup")
	}
	after, found := store.Get("U1")
	if !found || after.Status != statusCleanup {
		t.Fatalf("extension changed admitted cleanup: %+v, found=%v", after, found)
	}

	releaseInventoryOnce.Do(func() { close(releaseInventory) })
	waitForCalls(t, client, 1)
}

func TestLeaseExpiryContinuesWhileAdmissionIsPaused(t *testing.T) {
	control, err := admission.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := control.Pause(); err != nil || !changed {
		t.Fatalf("pause changed=%v err=%v", changed, err)
	}
	manager, client, store, clock, sent := newManager(t, []error{nil})
	record := lifecycleRecord("U1", statusReady)
	record.LeaseExpiresAt = clock.now.Add(time.Hour)
	putRecord(t, store, record)
	manager.ScheduleLease(request("U1"), record, sent.send)

	clock.advance(t, time.Hour)
	waitForNotice(t, sent, "Lease expired; starting cleanup.")
	waitForCalls(t, client, 1)
	if got := control.Status().Mode; got != admission.Paused {
		t.Fatalf("admission mode = %q, want paused", got)
	}
}

func TestDestroySuccessConfirmsStateAbsentBeforeResolving(t *testing.T) {
	manager, client, store, _, sent := newManager(t, []error{nil})
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	waitForNotice(t, sent, "Cleanup complete.")
	if got := client.calls(); len(got) != 1 || got[0] != "U1" {
		t.Fatalf("destroy calls = %v, want [U1]", got)
	}
	if _, found := store.Get("U1"); found {
		t.Fatal("lifecycle record survived successful workspace removal")
	}
	assertNotices(t, sent.texts(), "Starting cleanup.", "Cleanup attempt 1 started.", "Cleanup complete.")
}

func TestCleanupPreflightOutlivesCanceledRequestContext(t *testing.T) {
	manager, client, store, _, sent := newManager(t, []error{nil})
	var activity []bool
	var activityMu sync.Mutex
	manager.OnCleanupActivity = func(active bool) {
		activityMu.Lock()
		defer activityMu.Unlock()
		activity = append(activity, active)
	}
	client.rejectCanceledListContext = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := manager.RequestDestroy(ctx, request("U1"), sent.send); err != nil {
		t.Fatalf("RequestDestroy with canceled request context: %v", err)
	}
	waitForNotice(t, sent, "Cleanup complete.")
	if got := client.calls(); len(got) != 1 || got[0] != "U1" {
		t.Fatalf("destroy calls = %v, want [U1]", got)
	}
	if _, found := store.Get("U1"); found {
		t.Fatal("lifecycle record survived successful workspace removal")
	}
	activityMu.Lock()
	defer activityMu.Unlock()
	if len(activity) != 2 || !activity[0] || activity[1] {
		t.Fatalf("cleanup activity = %v, want [true false]", activity)
	}
}

func TestDestroyRetriesAtApprovedIntervalsThenEscalates(t *testing.T) {
	manager, client, store, clock, sent := newManager(t, []error{errors.New("first"), errors.New("second"), errors.New("third"), errors.New("fourth")})
	var activity []bool
	var activityMu sync.Mutex
	manager.OnCleanupActivity = func(active bool) {
		activityMu.Lock()
		defer activityMu.Unlock()
		activity = append(activity, active)
	}
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, client, 1)
	for _, delay := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute} {
		clock.advance(t, delay)
		waitForCalls(t, client, len(client.calls())+1)
	}
	record, found := store.Get("U1")
	if !found || record.Status != statusUnresolved || record.RetryCount != 3 || record.NextRetryAt != (time.Time{}) || record.LastError != "fourth" {
		t.Fatalf("record = %+v, found=%v; want final unresolved record", record, found)
	}
	texts := strings.Join(sent.texts(), "\n")
	for _, want := range []string{"Retrying in 1m0s.", "Retrying in 5m0s.", "Retrying in 15m0s.", "Cleanup retry 1 of 3 started.", "Cleanup retry 2 of 3 started.", "Cleanup retry 3 of 3 started.", "<@U-maintainer>"} {
		if !strings.Contains(texts, want) {
			t.Errorf("notices missing %q:\n%s", want, texts)
		}
	}
	activityMu.Lock()
	defer activityMu.Unlock()
	if len(activity) != 2 || !activity[0] || activity[1] {
		t.Fatalf("cleanup activity = %v, want [true false]", activity)
	}
}

func TestCleanupActivityRemainsActiveThroughRetrySuccess(t *testing.T) {
	manager, client, _, clock, sent := newManager(t, []error{errors.New("first"), nil})
	var activity []bool
	var activityMu sync.Mutex
	manager.OnCleanupActivity = func(active bool) {
		activityMu.Lock()
		defer activityMu.Unlock()
		activity = append(activity, active)
	}

	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, client, 1)
	clock.advance(t, time.Minute)
	waitForNotice(t, sent, "Cleanup complete.")

	activityMu.Lock()
	defer activityMu.Unlock()
	if len(activity) != 2 || !activity[0] || activity[1] {
		t.Fatalf("cleanup activity = %v, want [true false]", activity)
	}
}

func TestCleanupPreflightErrorUsesDiagnosticAndSafeSlackNotice(t *testing.T) {
	manager, client, _, _, sent := newManager(t, nil)
	client.listErr = errors.New("xoxb-secret /private/state {terraform_values}")
	logger, err := diagnostics.Open(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	manager.Diagnostics = logger

	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatalf("RequestDestroy returned an admission error: %v", err)
	}
	waitForNotice(t, sent, "Unable to check existing ICT state.")
	calls := client.diagnosticLists()
	if len(calls) != 1 || calls[0].stateID != "U1" || !diagnostics.ValidID(calls[0].diagnosticID) {
		t.Fatalf("diagnostic list calls = %+v, want one call for U1 with a valid ID", calls)
	}
	writer, err := logger.Writer(calls[0].diagnosticID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	segments, err := writer.Segments()
	if err != nil || len(segments) == 0 {
		t.Fatalf("private diagnostic segments = %v, %v", segments, err)
	}
	text := strings.Join(sent.texts(), "\n")
	for _, forbidden := range []string{"xoxb-secret", "/private/state", "terraform_values"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Slack notice leaked %q: %s", forbidden, text)
		}
	}
	for _, want := range []string{"Cleanup requires maintainer attention.", "Diagnostic ID: `" + calls[0].diagnosticID + "`"} {
		if !strings.Contains(text, want) {
			t.Fatalf("Slack notices missing %q: %s", want, text)
		}
	}
}

func TestCleanupPersistenceFailureUsesDiagnosticAndSafeSlackNotice(t *testing.T) {
	manager, client, _, _, sent := newManager(t, nil)
	var activityMu sync.Mutex
	var activity []bool
	manager.OnCleanupActivity = func(active bool) {
		activityMu.Lock()
		defer activityMu.Unlock()
		activity = append(activity, active)
	}
	workspace := client.inventory.Workspaces[0].Path
	if err := os.Chmod(workspace, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(workspace, 0o700) })

	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err == nil {
		t.Fatal("RequestDestroy succeeded despite lifecycle persistence failure")
	}
	waitForNotice(t, sent, "Cleanup did not start")
	text := strings.Join(sent.texts(), "\n")
	for _, forbidden := range []string{"/private/state", "terraform_values", "xoxb-secret"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Slack notice leaked %q: %s", forbidden, text)
		}
	}
	if calls := client.diagnosticLists(); len(calls) != 0 {
		t.Fatalf("diagnostic list calls = %+v, want none before durable admission", calls)
	}
	for _, want := range []string{"Cleanup did not start", "Cleanup requires maintainer attention.", "Diagnostic ID: `"} {
		if !strings.Contains(text, want) {
			t.Fatalf("Slack notice missing %q: %s", want, text)
		}
	}
	_, diagnosticReference, found := strings.Cut(text, "Diagnostic ID: `")
	if !found {
		t.Fatalf("Slack notice omitted code-formatted diagnostic ID: %s", text)
	}
	id, _, found := strings.Cut(diagnosticReference, "`")
	if !found || !diagnostics.ValidID(id) {
		t.Fatalf("diagnostic ID = %q", id)
	}
	if calls := client.calls(); len(calls) != 0 {
		t.Fatalf("destroy calls = %v, want none after lifecycle persistence failure", calls)
	}
	activityMu.Lock()
	defer activityMu.Unlock()
	if len(activity) != 2 || !activity[0] || activity[1] {
		t.Fatalf("cleanup activity = %v, want [true false]", activity)
	}
}

func TestCleanupErrorIsSanitizedAndReferencesDiagnosticID(t *testing.T) {
	manager, _, store, _, sent := newManager(t, []error{errors.New("xoxb-secret /private/state {terraform_values}")})
	logger, err := diagnostics.Open(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	manager.Diagnostics = logger
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	waitForNotice(t, sent, "Retrying in 1m0s.")
	record, found := store.Get("U1")
	if !found || !diagnostics.ValidID(record.DiagnosticRef) {
		t.Fatalf("record diagnostic reference = %q, found=%v", record.DiagnosticRef, found)
	}
	text := strings.Join(sent.texts(), "\n")
	for _, forbidden := range []string{"xoxb-secret", "/private/state", "terraform_values"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("Slack notice leaked %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, "Diagnostic ID: `"+record.DiagnosticRef+"`") {
		t.Fatalf("Slack notice omitted diagnostic ID: %s", text)
	}
}

func TestCleanupAdmissionPersistsBeforeInventory(t *testing.T) {
	manager, client, store, _, sent := newManager(t, []error{nil})
	client.inventoryStarted = make(chan struct{}, 1)
	release := make(chan struct{})
	client.releaseInventory = release

	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.inventoryStarted:
	case <-time.After(time.Second):
		t.Fatal("background inventory did not start")
	}
	record, found := store.Get("U1")
	if !found || record.Status != statusCleanup {
		t.Fatalf("cleanup admission record = %+v, found=%v", record, found)
	}
	if calls := client.calls(); len(calls) != 0 {
		t.Fatalf("destroy calls = %v, want none before inventory returns", calls)
	}
	close(release)
	waitForNotice(t, sent, "Cleanup complete.")
}

func TestDuplicateCleanupConvergesOnOneICTProcess(t *testing.T) {
	manager, client, _, _, sent := newManager(t, []error{nil})
	client.destroyStarted = make(chan struct{}, 1)
	release := make(chan struct{})
	client.releaseDestroy = release
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.destroyStarted:
	case <-time.After(time.Second):
		t.Fatal("first destroy did not start")
	}
	if record, found := manager.Store.Get("U1"); !found || record.Status != statusCleanup {
		t.Fatalf("cleanup admission record = %+v, found=%v", record, found)
	}
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitForNotice(t, sent, "Cleanup complete.")
	if got := client.calls(); len(got) != 1 {
		t.Fatalf("destroy calls = %v, want exactly one", got)
	}
	if !strings.Contains(strings.Join(sent.texts(), "\n"), "Cleanup is already in progress.") {
		t.Error("duplicate request did not receive an in-progress notice")
	}
}

func TestDifferentUsersCanDestroyConcurrently(t *testing.T) {
	manager, client, store, _, sent := newManager(t, []error{nil, nil})
	addFakeWorkspace(t, client, "U2")
	if err := store.Refresh(client.inventory); err != nil {
		t.Fatal(err)
	}
	client.destroyStarted = make(chan struct{}, 2)
	release := make(chan struct{})
	client.releaseDestroy = release
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	if err := manager.RequestDestroy(context.Background(), request("U2"), sent.send); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-client.destroyStarted:
		case <-time.After(time.Second):
			t.Fatal("destroy operations did not run concurrently")
		}
	}
	close(release)
	waitForCalls(t, client, 2)
	got := strings.Join(client.calls(), ",")
	if got != "U1,U2" && got != "U2,U1" {
		t.Errorf("destroy calls = %q, want one call per user", got)
	}
}

func TestThreadCleanupStaysInPersistedLifecycleThread(t *testing.T) {
	manager, client, store, _, sent := newManager(t, []error{nil})
	if err := manager.RequestDestroy(context.Background(), Request{UserID: "U2", Channel: "C1", ThreadTimestamp: "unrelated", RawThread: true}, sent.send); err != nil {
		t.Fatal(err)
	}
	if got := sent.texts(); len(got) != 0 {
		t.Fatalf("record-less raw thread notices = %q, want none", got)
	}
	if got := client.calls(); len(got) != 0 {
		t.Fatalf("record-less raw thread destroy calls = %v, want none", got)
	}
	if _, found := store.Get("U2"); found {
		t.Fatal("record-less raw thread persisted lifecycle ownership")
	}

	original := Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "original"}
	if err := store.Put(state.LifecycleRecord{UserID: "U1", Channel: original.Channel, ThreadTimestamp: original.ThreadTimestamp, Status: statusReady, DiagnosticRef: "111111111111111111111111", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := manager.RequestDestroy(context.Background(), Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "unrelated", RawThread: true}, sent.send); err != nil {
		t.Fatal(err)
	}
	if got := sent.texts(); len(got) != 0 {
		t.Fatalf("unrelated raw thread notices = %q, want none", got)
	}
	if got := client.calls(); len(got) != 0 {
		t.Fatalf("unrelated raw thread destroy calls = %v, want none", got)
	}

	if err := manager.RequestDestroy(context.Background(), Request{UserID: "U1", Channel: "C1", ThreadTimestamp: "root"}, sent.send); err != nil {
		t.Fatal(err)
	}
	waitForNotice(t, sent, "Cleanup complete.")
	for _, notice := range sent.items {
		if notice.Channel != original.Channel || notice.ThreadTimestamp != original.ThreadTimestamp {
			t.Fatalf("notice = %+v, want persisted lifecycle route", notice)
		}
	}
}

func TestCleanupActivityBalancesAdmissionFailureAndSuccess(t *testing.T) {
	manager, client, store, _, sent := newManager(t, []error{nil})
	var mu sync.Mutex
	var activity []bool
	manager.OnCleanupActivity = func(active bool) {
		mu.Lock()
		defer mu.Unlock()
		activity = append(activity, active)
	}
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	waitForNotice(t, sent, "Cleanup complete.")
	client.listErr = errors.New("inventory unavailable")
	client.states["U1"] = true
	if err := store.Refresh(client.inventory); err != nil {
		t.Fatal(err)
	}
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	waitForNotice(t, sent, "Unable to check existing ICT state.")
	mu.Lock()
	defer mu.Unlock()
	want := []bool{true, false, true, false}
	if len(activity) != len(want) {
		t.Fatalf("cleanup activity = %v, want %v", activity, want)
	}
	for index := range want {
		if activity[index] != want[index] {
			t.Fatalf("cleanup activity = %v, want %v", activity, want)
		}
	}
}

func TestDestroyWithoutStateDoesNotFabricateSuccess(t *testing.T) {
	manager, client, store, _, sent := newManager(t, nil)
	client.states = map[string]bool{}
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	waitForNotice(t, sent, "There is nothing to destroy.")
	if got := client.calls(); len(got) != 0 {
		t.Errorf("destroy calls = %v, want none", got)
	}
	if _, found := store.Get("U1"); found {
		t.Error("no-state request unexpectedly persisted lifecycle ownership")
	}
	assertNotices(t, sent.texts(), "There is nothing to destroy.")
}

func TestNominalDestroyRetainsOwnershipWhenICTStillListsState(t *testing.T) {
	manager, client, store, _, sent := newManager(t, []error{nil})
	client.preserveState = true
	if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
		t.Fatal(err)
	}
	waitForNotice(t, sent, "Retrying in 1m0s.")
	record, found := store.Get("U1")
	if !found || record.Status != statusCleanup || record.LastError != "ict still lists this state after destroy" {
		t.Fatalf("record = %+v, found=%v; want retained cleanup ownership", record, found)
	}
	if !strings.Contains(strings.Join(sent.texts(), "\n"), "Retrying in 1m0s.") {
		t.Error("state still listed after nominal destroy did not schedule a retry")
	}
}

func newManager(t *testing.T, errors []error) (*Manager, *fakeICT, *state.LifecycleStore, *fakeClock, *notices) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "ict")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	root = canonicalRoot
	workspaces := make([]state.WorkspaceLocation, 0, 2)
	for _, id := range []string{"U1", "U2"} {
		path := filepath.Join(root, id)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		workspaces = append(workspaces, state.WorkspaceLocation{ID: id, Path: path})
	}
	store, err := state.OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeICT{states: map[string]bool{"U1": true}, destroyErrors: errors, inventory: state.WorkspaceInventory{Version: 1, StateRoot: root, Workspaces: workspaces[:1]}}
	if err := store.Refresh(client.inventory); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	manager := NewManager(client, store, []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}, "U-maintainer")
	manager.Clock = clock
	return manager, client, store, clock, &notices{}
}

func addFakeWorkspace(t *testing.T, client *fakeICT, id string) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	path := filepath.Join(client.inventory.StateRoot, id)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	client.states[id] = true
	client.inventory.Workspaces = append(client.inventory.Workspaces, state.WorkspaceLocation{ID: id, Path: path})
}

func request(userID string) Request {
	return Request{UserID: userID, Channel: "C1", ThreadTimestamp: "123.456"}
}

func assertNotices(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("notices = %q, want %q", got, want)
	}
}

func waitForNotice(t *testing.T, sent *notices, text string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		for _, notice := range sent.texts() {
			if strings.Contains(notice, text) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("notices = %q, want one containing %q", sent.texts(), text)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCalls(t *testing.T, client *fakeICT, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(client.calls()) < want {
		if time.Now().After(deadline) {
			t.Fatalf("destroy calls = %v, want at least %d", client.calls(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

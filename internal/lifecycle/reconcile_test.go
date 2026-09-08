package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bevicted/servitor/internal/diagnostics"
	"github.com/bevicted/servitor/internal/state"
)

func TestReconcileCleansInterruptedCreationAndRestoresLease(t *testing.T) {
	t.Run("interrupted review is destroyed", func(t *testing.T) {
		manager, client, store, _, sent := newManager(t, []error{nil})
		putRecord(t, store, lifecycleRecord("U1", statusReview))
		if err := manager.Reconcile(context.Background(), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 1 || got[0] != "U1" {
			t.Fatalf("destroy calls = %v, want [U1]", got)
		}
		assertStatus(t, store, "U1", statusResolved)
		if got := strings.Join(sent.texts(), "\n"); !strings.Contains(got, "Starting cleanup after restart.") || !strings.Contains(got, "Cleanup complete.") {
			t.Fatalf("notices = %q", got)
		}
	})

	t.Run("future lease keeps original deadline", func(t *testing.T) {
		manager, client, store, clock, sent := newManager(t, []error{nil})
		record := lifecycleRecord("U1", statusReady)
		record.LeaseExpiresAt = clock.now.Add(time.Hour)
		putRecord(t, store, record)
		if err := manager.Reconcile(context.Background(), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 0 {
			t.Fatalf("destroy calls before deadline = %v", got)
		}
		clock.advance(t, time.Hour)
		waitForCalls(t, client, 1)
		assertStatus(t, store, "U1", statusResolved)
	})

	t.Run("overdue lease is destroyed immediately", func(t *testing.T) {
		manager, client, store, clock, sent := newManager(t, []error{nil})
		record := lifecycleRecord("U1", statusReady)
		record.LeaseExpiresAt = clock.now.Add(-time.Second)
		putRecord(t, store, record)
		if err := manager.Reconcile(context.Background(), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 1 {
			t.Fatalf("destroy calls = %v, want one immediate destroy", got)
		}
		assertStatus(t, store, "U1", statusResolved)
	})
}

func TestReconcileRestoresExtendedLeaseAndExpiresOverdueExtension(t *testing.T) {
	manager, client, store, clock, sent := newManager(t, []error{nil})
	record := lifecycleRecord("U1", statusReady)
	record.LeaseExpiresAt = clock.now.Add(20 * time.Hour)
	putRecord(t, store, record)
	if err := manager.Reconcile(context.Background(), sent.send); err != nil {
		t.Fatal(err)
	}
	clock.advance(t, 20*time.Hour)
	waitForCalls(t, client, 1)
	assertStatus(t, store, "U1", statusResolved)
}

func TestReconcileCoversReservationApplyAndPostApplyCrashPoints(t *testing.T) {
	t.Run("before workspace reservation leaves no ownership", func(t *testing.T) {
		manager, client, store, _, sent := newManager(t, nil)
		client.states = map[string]bool{}
		if err := manager.Reconcile(context.Background(), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 0 {
			t.Fatalf("destroy calls = %v, want none", got)
		}
		if records := store.Records(); len(records) != 0 {
			t.Fatalf("records = %+v, want none", records)
		}
		if notices := sent.texts(); len(notices) != 0 {
			t.Fatalf("notices = %q, want none", notices)
		}
	})

	t.Run("retained interrupted apply is destroyed", func(t *testing.T) {
		manager, client, store, _, sent := newManager(t, []error{nil})
		putRecord(t, store, lifecycleRecord("U1", statusApplying))
		if err := manager.Reconcile(context.Background(), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 1 || got[0] != "U1" {
			t.Fatalf("destroy calls = %v, want [U1]", got)
		}
		assertStatus(t, store, "U1", statusResolved)
	})

	t.Run("immediately after apply retains original future deadline", func(t *testing.T) {
		manager, client, store, clock, sent := newManager(t, nil)
		record := lifecycleRecord("U1", statusReady)
		record.LeaseExpiresAt = clock.now.Add(time.Hour)
		putRecord(t, store, record)
		if err := manager.Reconcile(context.Background(), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 0 {
			t.Fatalf("destroy calls = %v, want none before the persisted deadline", got)
		}
		stored, found := store.Get("U1")
		if !found || !stored.LeaseExpiresAt.Equal(record.LeaseExpiresAt) {
			t.Fatalf("record = %+v, found=%v; want original deadline %s", stored, found, record.LeaseExpiresAt)
		}
	})
}

func TestReconcileResumesCleanupWithoutRestartingFinalFailure(t *testing.T) {
	t.Run("future retry retains persisted attempt and time", func(t *testing.T) {
		manager, client, store, clock, sent := newManager(t, []error{errors.New("second")})
		record := lifecycleRecord("U1", statusCleanup)
		record.RetryCount = 1
		record.NextRetryAt = clock.now.Add(5 * time.Minute)
		putRecord(t, store, record)
		if err := manager.Reconcile(context.Background(), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 0 {
			t.Fatalf("destroy calls before retry time = %v", got)
		}
		clock.advance(t, 5*time.Minute)
		waitForCalls(t, client, 1)
		updated, _ := store.Get("U1")
		if updated.Status != statusCleanup || updated.RetryCount != 2 || !updated.NextRetryAt.Equal(clock.now.Add(5*time.Minute)) {
			t.Fatalf("resumed record = %+v", updated)
		}
	})

	t.Run("final failed cleanup remains blocked", func(t *testing.T) {
		manager, client, store, _, sent := newManager(t, []error{nil})
		record := lifecycleRecord("U1", statusUnresolved)
		record.RetryCount = 3
		record.LastError = "final failure"
		putRecord(t, store, record)
		if err := manager.Reconcile(context.Background(), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 0 {
			t.Fatalf("final failed cleanup restarted: %v", got)
		}
		if err := manager.RequestDestroy(context.Background(), request("U1"), sent.send); err != nil {
			t.Fatal(err)
		}
		if got := client.calls(); len(got) != 0 {
			t.Fatalf("user request restarted final failed cleanup: %v", got)
		}
		assertStatus(t, store, "U1", statusUnresolved)
	})
}

func TestReconcileResumePersistenceFailureNotifiesThreadWithDiagnosticID(t *testing.T) {
	manager, client, store, _, sent := newManager(t, nil)
	putRecord(t, store, lifecycleRecord("U1", statusApplying))
	workspace := client.inventory.Workspaces[0].Path
	if err := os.Chmod(workspace, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(workspace, 0o700) })

	if err := manager.Reconcile(context.Background(), sent.send); err != nil {
		t.Fatal(err)
	}
	sent.mu.Lock()
	defer sent.mu.Unlock()
	if len(sent.items) != 1 {
		t.Fatalf("notices = %+v, want one persistence failure notice", sent.items)
	}
	notice := sent.items[0]
	if notice.Channel != "C1" || notice.ThreadTimestamp != "123.456" {
		t.Fatalf("notice route = %+v, want initiating thread", notice)
	}
	for _, want := range []string{"Cleanup could not resume after restart", "Cleanup requires maintainer attention.", "Diagnostic ID: `"} {
		if !strings.Contains(notice.Text, want) {
			t.Fatalf("notice missing %q: %s", want, notice.Text)
		}
	}
	id, _, found := strings.Cut(strings.Split(notice.Text, "Diagnostic ID: `")[1], "`")
	if !found || !diagnostics.ValidID(id) {
		t.Fatalf("diagnostic ID = %q", id)
	}
	if calls := client.calls(); len(calls) != 0 {
		t.Fatalf("destroy calls = %v, want none after persistence failure", calls)
	}
}

func TestReconcileRestoresEachPersistedRetry(t *testing.T) {
	for retryCount, delay := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute} {
		t.Run(fmt.Sprintf("retry-%d", retryCount+1), func(t *testing.T) {
			manager, client, store, clock, sent := newManager(t, []error{nil})
			record := lifecycleRecord("U1", statusCleanup)
			record.RetryCount = retryCount + 1
			record.NextRetryAt = clock.now.Add(delay)
			putRecord(t, store, record)
			if err := manager.Reconcile(context.Background(), sent.send); err != nil {
				t.Fatal(err)
			}
			clock.advance(t, delay)
			waitForCalls(t, client, 1)
			assertStatus(t, store, "U1", statusResolved)
		})
	}
}

func TestReconcileMissingAndUnknownWorkspacesAreConservative(t *testing.T) {
	manager, client, store, _, sent := newManager(t, nil)
	addFakeWorkspace(t, client, "default")
	client.states = map[string]bool{"default": true}
	var logs []string
	manager.Logf = func(format string, args ...any) { logs = append(logs, format) }
	putRecord(t, store, lifecycleRecord("U1", statusApplying))
	if err := manager.Reconcile(context.Background(), sent.send); err != nil {
		t.Fatal(err)
	}
	if records := store.Records(); len(records) != 0 {
		t.Fatalf("records = %+v, want no fabricated remote-state ownership", records)
	}
	if got := client.calls(); len(got) != 0 {
		t.Fatalf("destroy calls = %v, want unknown workspace preserved", got)
	}
	if client.lists() != 1 {
		t.Fatalf("initial ICT list snapshots = %d, want 1", client.lists())
	}
	if notices := sent.texts(); len(notices) != 0 {
		t.Fatalf("notices = %q, want no fabricated remote-state result", notices)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "unknown ICT workspace") {
		t.Fatalf("logs = %q, want unknown workspace review", logs)
	}
}

func TestReconcileUsesPrivateDiagnosticList(t *testing.T) {
	manager, client, store, _, sent := newManager(t, nil)
	putRecord(t, store, lifecycleRecord("U1", statusUnresolved))
	if err := manager.Reconcile(context.Background(), sent.send); err != nil {
		t.Fatal(err)
	}
	calls := client.diagnosticLists()
	if len(calls) != 1 || calls[0].stateID != "reconciliation" || !validDiagnosticID(calls[0].diagnosticID) {
		t.Fatalf("reconciliation diagnostic list calls = %+v", calls)
	}
}

func TestReconcileAbsentWorkspaceDoesNotFabricateLifecycleState(t *testing.T) {
	manager, client, store, _, sent := newManager(t, nil)
	addFakeWorkspace(t, client, "U2")
	if err := store.Refresh(client.inventory); err != nil {
		t.Fatal(err)
	}
	putRecord(t, store, lifecycleRecord("U1", statusCleanup))
	putRecord(t, store, lifecycleRecord("U2", statusReady))
	client.states = map[string]bool{}
	if err := manager.Reconcile(context.Background(), sent.send); err != nil {
		t.Fatal(err)
	}
	if records := store.Records(); len(records) != 0 {
		t.Fatalf("records = %+v, want no remote-state absence fabrication", records)
	}
	if got := client.calls(); len(got) != 0 {
		t.Fatalf("destroy calls = %v, want no absent workspace destroy", got)
	}
	if notices := sent.texts(); len(notices) != 0 {
		t.Fatalf("notices = %q, want no absence notice", notices)
	}
}

func lifecycleRecord(userID, status string) state.LifecycleRecord {
	return state.LifecycleRecord{UserID: userID, Channel: "C1", ThreadTimestamp: "123.456", Status: status, DiagnosticRef: "private server logs", UpdatedAt: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
}

func putRecord(t *testing.T, store *state.LifecycleStore, record state.LifecycleRecord) {
	t.Helper()
	if err := store.Put(record); err != nil {
		t.Fatal(err)
	}
}

func assertStatus(t *testing.T, store *state.LifecycleStore, userID, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		record, found := store.Get(userID)
		if want == statusResolved && !found {
			return
		}
		if found && record.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("record for %s = %+v, found=%v; want status %q", userID, record, found, want)
		}
		time.Sleep(time.Millisecond)
	}
}

package state

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func workspaceInventory(t *testing.T, ids ...string) WorkspaceInventory {
	t.Helper()
	root := filepath.Join(t.TempDir(), "ict")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	inventory := WorkspaceInventory{Version: 1, StateRoot: canonicalRoot}
	for _, id := range ids {
		path := filepath.Join(canonicalRoot, id)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		inventory.Workspaces = append(inventory.Workspaces, WorkspaceLocation{ID: id, Path: path})
	}
	return inventory
}

func lifecycleRecordForTest(id string) LifecycleRecord {
	return LifecycleRecord{UserID: id, Channel: "C1", ThreadTimestamp: "123.456", Status: "cleanup", RetryCount: 2, NextRetryAt: time.Date(2026, 9, 4, 12, 6, 0, 0, time.UTC), LeaseExpiresAt: time.Date(2026, 9, 4, 16, 0, 0, 0, time.UTC), DiagnosticRef: "private server logs", ClusterName: "servitor-test", Location: "us-south/us-south-1", UpdatedAt: time.Date(2026, 9, 4, 12, 1, 0, 0, time.UTC)}
}

func TestLifecycleRecordsPersistIndependentlyInValidatedWorkspaces(t *testing.T) {
	inventory := workspaceInventory(t, "U1", "U2")
	store, err := OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(inventory); err != nil {
		t.Fatal(err)
	}
	first, second := lifecycleRecordForTest("U1"), lifecycleRecordForTest("U2")
	if err := store.Put(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(second); err != nil {
		t.Fatal(err)
	}
	store, err = OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(inventory); err != nil {
		t.Fatal(err)
	}
	if got, found := store.Get("U1"); !found || got != first {
		t.Fatalf("U1 record = %+v, found=%v; want %+v", got, found, first)
	}
	for _, workspace := range inventory.Workspaces {
		info, err := os.Stat(filepath.Join(workspace.Path, lifecycleFilename))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("record mode = %o, want 600", info.Mode().Perm())
		}
	}
	if matches, err := filepath.Glob(filepath.Join(inventory.StateRoot, "lifecycles.json")); err != nil || len(matches) != 0 {
		t.Fatalf("aggregate lifecycle file = %v, %v", matches, err)
	}
}

func TestLifecycleStorePersistsConcurrentWorkspaceRecords(t *testing.T) {
	inventory := workspaceInventory(t, "U1", "U2")
	store, err := OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(inventory); err != nil {
		t.Fatal(err)
	}

	records := []LifecycleRecord{lifecycleRecordForTest("U1"), lifecycleRecordForTest("U2")}
	start := make(chan struct{})
	errs := make(chan error, len(records))
	var writes sync.WaitGroup
	for _, record := range records {
		writes.Add(1)
		go func(record LifecycleRecord) {
			defer writes.Done()
			<-start
			errs <- store.Put(record)
		}(record)
	}
	close(start)
	writes.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	reopened, err := OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Refresh(inventory); err != nil {
		t.Fatal(err)
	}
	for _, want := range records {
		if got, found := reopened.Get(want.UserID); !found || got != want {
			t.Fatalf("record for %s = %+v, found=%v; want %+v", want.UserID, got, found, want)
		}
	}
}

func TestWorkspaceInventoryRejectsUnsafeRootsAndPaths(t *testing.T) {
	inventory := workspaceInventory(t, "U1")
	for _, test := range []struct {
		name string
		edit func(*WorkspaceInventory)
	}{
		{"unsupported version", func(i *WorkspaceInventory) { i.Version = 2 }},
		{"relative root", func(i *WorkspaceInventory) { i.StateRoot = "relative" }},
		{"path mismatch", func(i *WorkspaceInventory) { i.Workspaces[0].Path = filepath.Join(i.StateRoot, "other") }},
		{"duplicate ID", func(i *WorkspaceInventory) { i.Workspaces = append(i.Workspaces, i.Workspaces[0]) }},
		{"invalid Slack record ID", func(i *WorkspaceInventory) { i.Workspaces[0].ID = "../U1" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := inventory
			candidate.Workspaces = append([]WorkspaceLocation(nil), inventory.Workspaces...)
			test.edit(&candidate)
			if _, err := validateWorkspaceInventory(candidate); err == nil {
				t.Fatal("validateWorkspaceInventory succeeded")
			}
		})
	}
}

func TestLifecycleStoreAtomicWriteFailureKeepsCachedRecord(t *testing.T) {
	inventory := workspaceInventory(t, "U1")
	store, err := OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(inventory); err != nil {
		t.Fatal(err)
	}
	record := lifecycleRecordForTest("U1")
	if err := store.Put(record); err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(inventory.Workspaces[0].Path, lifecycleFilename)
	if err := os.Remove(recordPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(recordPath, 0o700); err != nil {
		t.Fatal(err)
	}
	updated := record
	updated.Status = "ready"
	if err := store.Put(updated); err == nil {
		t.Fatal("Put succeeded with a non-replaceable record path")
	}
	if got, found := store.Get("U1"); !found || got != record {
		t.Fatalf("cached record = %+v, found=%v; want %+v", got, found, record)
	}
}

func TestLifecycleStoreDoesNotWriteWithoutAnInventoryWorkspace(t *testing.T) {
	store, err := OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(lifecycleRecordForTest("U1")); err == nil {
		t.Fatal("Put succeeded without an ICT workspace")
	}
}

func TestLifecycleStoreReportsCorruptWorkspaceState(t *testing.T) {
	inventory := workspaceInventory(t, "U1")
	if err := os.WriteFile(filepath.Join(inventory.Workspaces[0].Path, lifecycleFilename), []byte(`{"user_id":"U1","unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := OpenLifecycleStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(inventory); err != nil {
		t.Fatal(err)
	}
	if got := store.CorruptWorkspaceIDs(); len(got) != 1 || got[0] != "U1" {
		t.Fatalf("corrupt workspaces = %q", got)
	}
}

package state

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEventClaimsPersistAcrossReopen(t *testing.T) {
	directory := t.TempDir()
	store, err := OpenEventStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim("Ev1")
	if err != nil || !claimed {
		t.Fatalf("first Claim() = (%v, %v), want (true, nil)", claimed, err)
	}
	store, err = OpenEventStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err = store.Claim("Ev1")
	if err != nil || claimed {
		t.Fatalf("reopened Claim() = (%v, %v), want (false, nil)", claimed, err)
	}
	info, err := os.Stat(filepath.Join(directory, "events.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("events.json mode = %o, want 600", info.Mode().Perm())
	}
}

func TestEventStoreRejectsEmptyID(t *testing.T) {
	store, err := OpenEventStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(""); err == nil {
		t.Error("Claim(empty) error = nil, want error")
	}
}

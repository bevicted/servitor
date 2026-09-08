// Package state persists Servitor metadata that must survive Socket Mode redelivery.
package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// EventStore records handled Slack event IDs in a private atomically-replaced file.
type EventStore struct {
	mu   sync.Mutex
	path string
	ids  map[string]struct{}
}

type eventFile struct {
	IDs []string `json:"ids"`
}

// OpenEventStore loads an existing event ID set or creates it on first use.
func OpenEventStore(directory string) (*EventStore, error) {
	store := &EventStore{path: filepath.Join(directory, "events.json"), ids: make(map[string]struct{})}
	contents, err := os.ReadFile(store.path)
	if os.IsNotExist(err) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read event store: %w", err)
	}
	var disk eventFile
	if err := json.Unmarshal(contents, &disk); err != nil {
		return nil, fmt.Errorf("decode event store: %w", err)
	}
	for _, id := range disk.IDs {
		if id != "" {
			store.ids[id] = struct{}{}
		}
	}
	return store, nil
}

// Claim returns false for an already handled event. New IDs are persisted before it returns.
func (s *EventStore) Claim(id string) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("claim event: empty ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.ids[id]; exists {
		return false, nil
	}
	s.ids[id] = struct{}{}
	if err := s.saveLocked(); err != nil {
		delete(s.ids, id)
		return false, err
	}
	return true, nil
}

func (s *EventStore) saveLocked() error {
	ids := make([]string, 0, len(s.ids))
	for id := range s.ids {
		ids = append(ids, id)
	}
	contents, err := json.Marshal(eventFile{IDs: ids})
	if err != nil {
		return fmt.Errorf("encode event store: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".events-")
	if err != nil {
		return fmt.Errorf("create event store: %w", err)
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("protect event store: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("write event store: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close event store: %w", err)
	}
	if err := os.Rename(name, s.path); err != nil {
		return fmt.Errorf("replace event store: %w", err)
	}
	return os.Chmod(s.path, 0o600)
}

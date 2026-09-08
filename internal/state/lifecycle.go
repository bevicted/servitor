package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

const lifecycleFilename = ".servitor-lifecycle.json"

// LifecycleRecord is the private, durable cleanup ownership for one Slack user.
type LifecycleRecord struct {
	UserID          string    `json:"user_id"`
	Channel         string    `json:"channel"`
	ThreadTimestamp string    `json:"thread_timestamp"`
	Status          string    `json:"status"`
	RetryCount      int       `json:"retry_count"`
	NextRetryAt     time.Time `json:"next_retry_at,omitempty"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
	DiagnosticRef   string    `json:"diagnostic_ref,omitempty"`
	ClusterName     string    `json:"cluster_name,omitempty"`
	Location        string    `json:"location,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// WorkspaceInventory is ICT's private, versioned workspace contract.
type WorkspaceInventory struct {
	Version    int                 `json:"version"`
	StateRoot  string              `json:"state_root"`
	Workspaces []WorkspaceLocation `json:"workspaces"`
}

type WorkspaceLocation struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// DecodeWorkspaceInventory strictly decodes and validates ICT's workspace inventory.
func DecodeWorkspaceInventory(contents []byte) (WorkspaceInventory, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var inventory WorkspaceInventory
	if err := decoder.Decode(&inventory); err != nil {
		return WorkspaceInventory{}, fmt.Errorf("decode ICT workspace inventory: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return WorkspaceInventory{}, fmt.Errorf("decode ICT workspace inventory: trailing data")
		}
		return WorkspaceInventory{}, fmt.Errorf("decode ICT workspace inventory: trailing data: %w", err)
	}
	return validateWorkspaceInventory(inventory)
}

func validateWorkspaceInventory(inventory WorkspaceInventory) (WorkspaceInventory, error) {
	if inventory.Version != 1 {
		return WorkspaceInventory{}, fmt.Errorf("unsupported ICT workspace inventory version %d", inventory.Version)
	}
	if !filepath.IsAbs(inventory.StateRoot) || filepath.Clean(inventory.StateRoot) != inventory.StateRoot {
		return WorkspaceInventory{}, fmt.Errorf("ICT state root is not canonical and absolute")
	}
	resolvedRoot, err := filepath.EvalSymlinks(inventory.StateRoot)
	if err == nil && resolvedRoot != inventory.StateRoot {
		return WorkspaceInventory{}, fmt.Errorf("ICT state root is not canonical")
	}
	if err != nil && !os.IsNotExist(err) {
		return WorkspaceInventory{}, fmt.Errorf("resolve ICT state root: %w", err)
	}
	if err == nil {
		info, statErr := os.Stat(inventory.StateRoot)
		if statErr != nil || !info.IsDir() {
			return WorkspaceInventory{}, fmt.Errorf("ICT state root is not a directory")
		}
	} else {
		parent, parentErr := filepath.EvalSymlinks(filepath.Dir(inventory.StateRoot))
		if parentErr != nil || filepath.Join(parent, filepath.Base(inventory.StateRoot)) != inventory.StateRoot {
			return WorkspaceInventory{}, fmt.Errorf("ICT state root is not canonical")
		}
	}
	seen := make(map[string]struct{}, len(inventory.Workspaces))
	for index, workspace := range inventory.Workspaces {
		if !validWorkspaceID(workspace.ID) {
			return WorkspaceInventory{}, fmt.Errorf("ICT workspace %d has an invalid ID", index)
		}
		if _, exists := seen[workspace.ID]; exists {
			return WorkspaceInventory{}, fmt.Errorf("ICT workspace inventory has duplicate ID %q", workspace.ID)
		}
		seen[workspace.ID] = struct{}{}
		wantPath := filepath.Join(inventory.StateRoot, workspace.ID)
		if !filepath.IsAbs(workspace.Path) || filepath.Clean(workspace.Path) != wantPath || workspace.Path != wantPath {
			return WorkspaceInventory{}, fmt.Errorf("ICT workspace %q is outside its state root", workspace.ID)
		}
		entry, err := os.Lstat(workspace.Path)
		if err != nil || !entry.IsDir() || entry.Mode()&os.ModeSymlink != 0 {
			return WorkspaceInventory{}, fmt.Errorf("ICT workspace %q is not a private directory", workspace.ID)
		}
		resolved, err := filepath.EvalSymlinks(workspace.Path)
		if err != nil || resolved != workspace.Path {
			return WorkspaceInventory{}, fmt.Errorf("ICT workspace %q escapes its state root", workspace.ID)
		}
	}
	return inventory, nil
}

var lifecycleUserID = regexp.MustCompile(`^[UW][A-Za-z0-9]{1,127}$`)

var lifecycleStatuses = map[string]bool{
	"review": true, "applying": true, "ready": true, "cleanup": true, "unresolved": true,
}

func validWorkspaceID(value string) bool {
	return len(value) > 0 && len(value) <= 128 && regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`).MatchString(value)
}

// LifecycleStore keeps only records discovered from the current ICT inventory.
type LifecycleStore struct {
	mu         sync.Mutex
	workspaces map[string]string
	records    map[string]LifecycleRecord
	corrupt    []string
}

// OpenLifecycleStore creates an empty workspace-local lifecycle store. Lifecycle
// data is never read from an aggregate Servitor file.
func OpenLifecycleStore(_ ...string) (*LifecycleStore, error) {
	return &LifecycleStore{workspaces: make(map[string]string), records: make(map[string]LifecycleRecord)}, nil
}

// Refresh replaces the inventory snapshot and reads each workspace-local record.
// Missing records are normal. Corrupt records are retained only as private errors.
func (s *LifecycleStore) Refresh(inventory WorkspaceInventory) error {
	inventory, err := validateWorkspaceInventory(inventory)
	if err != nil {
		return err
	}
	// Keep the snapshot replacement and any concurrent Put ordered. Otherwise a
	// refresh that observed a newly reserved workspace before its record was
	// written could erase that in-memory record after the write completed.
	s.mu.Lock()
	defer s.mu.Unlock()
	workspaces := make(map[string]string, len(inventory.Workspaces))
	records := make(map[string]LifecycleRecord)
	corrupt := make([]string, 0)
	for _, workspace := range inventory.Workspaces {
		workspaces[workspace.ID] = workspace.Path
		recordPath := filepath.Join(workspace.Path, lifecycleFilename)
		entry, err := os.Lstat(recordPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil || !entry.Mode().IsRegular() || entry.Mode()&os.ModeSymlink != 0 {
			corrupt = append(corrupt, workspace.ID)
			continue
		}
		contents, err := os.ReadFile(recordPath)
		if err != nil {
			corrupt = append(corrupt, workspace.ID)
			continue
		}
		var record LifecycleRecord
		decoder := json.NewDecoder(bytes.NewReader(contents))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil || decoder.Decode(&struct{}{}) != io.EOF || record.UserID != workspace.ID || !validLifecycleRecord(record) {
			corrupt = append(corrupt, workspace.ID)
			continue
		}
		records[record.UserID] = record
	}
	s.workspaces, s.records, s.corrupt = workspaces, records, corrupt
	return nil
}

// Records returns valid records in a stable order.
func (s *LifecycleStore) Records() []LifecycleRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]LifecycleRecord, 0, len(s.records))
	for _, record := range s.records {
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].UserID < records[j].UserID })
	return records
}

func (s *LifecycleStore) Get(userID string) (LifecycleRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[userID]
	return record, ok
}

// WorkspacePath returns only a path supplied by the validated ICT inventory.
func (s *LifecycleStore) WorkspacePath(userID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path, ok := s.workspaces[userID]
	return path, ok
}

// CorruptWorkspaceIDs reports workspace-local state that needs private investigation.
func (s *LifecycleStore) CorruptWorkspaceIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.corrupt...)
}

// Put atomically replaces the private record in its validated workspace.
func (s *LifecycleStore) Put(record LifecycleRecord) error {
	if !validLifecycleRecord(record) {
		return fmt.Errorf("save lifecycle: invalid lifecycle record")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	workspace, exists := s.workspaces[record.UserID]
	if !exists {
		return fmt.Errorf("save lifecycle: ICT workspace is absent")
	}
	if err := atomicWrite(filepath.Join(workspace, lifecycleFilename), record); err != nil {
		return err
	}
	s.records[record.UserID] = record
	return nil
}

// RemoveAbsentWorkspace forgets a lifecycle after ICT has confirmed its
// workspace is absent.
func (s *LifecycleStore) RemoveAbsentWorkspace(userID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, userID)
	delete(s.workspaces, userID)
}

func validLifecycleRecord(record LifecycleRecord) bool {
	return lifecycleUserID.MatchString(record.UserID) && record.Channel != "" && record.ThreadTimestamp != "" && lifecycleStatuses[record.Status] && record.RetryCount >= 0
}

func atomicWrite(path string, record LifecycleRecord) error {
	contents, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode lifecycle record: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".servitor-lifecycle-")
	if err != nil {
		return fmt.Errorf("create lifecycle record: %w", err)
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("protect lifecycle record: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("write lifecycle record: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close lifecycle record: %w", err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("replace lifecycle record: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("protect lifecycle record: %w", err)
	}
	return nil
}

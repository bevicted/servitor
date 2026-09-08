package diagnostics

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	firstID  = "111111111111111111111111"
	secondID = "222222222222222222222222"
	thirdID  = "333333333333333333333333"
)

func TestWriterRotatesAndKeepsConcurrentLifecycleStreamsSeparate(t *testing.T) {
	logger, err := Open(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	logger.now = func() time.Time { return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) }
	first, err := logger.Writer(firstID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := logger.Writer(secondID, "U2")
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for _, writer := range []*Writer{first, second} {
		writer := writer
		group.Add(1)
		go func() {
			defer group.Done()
			for range 5 {
				writer.Event(strings.Repeat(writer.stateID, 30))
			}
		}()
	}
	group.Wait()
	for _, writer := range []*Writer{first, second} {
		segments, err := writer.Segments()
		if err != nil || len(segments) < 2 {
			t.Fatalf("segments = %v, %v; want rotation", segments, err)
		}
		for _, segment := range segments {
			info, err := os.Stat(segment)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() > 80 || info.Mode().Perm() != 0o600 {
				t.Fatalf("segment %s = mode %o size %d", segment, info.Mode().Perm(), info.Size())
			}
			contents, _ := os.ReadFile(segment)
			other := "U1"
			if writer.stateID == "U1" {
				other = "U2"
			}
			if strings.Contains(string(contents), "state="+other) {
				t.Fatalf("segment %s mixed lifecycle output", segment)
			}
		}
	}
}

func TestWriterRotatesWhenAnExistingSegmentExceedsLoweredLimit(t *testing.T) {
	root := t.TempDir()
	logger, err := Open(root, 4096)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := logger.Writer(firstID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	writer.Event(strings.Repeat("x", 128))

	lowered, err := Open(root, 80)
	if err != nil {
		t.Fatal(err)
	}
	writer, err = lowered.Writer(firstID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	writer.Event("follow-up")
	segments, err := writer.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) < 2 {
		t.Fatalf("segments = %v, want rotation after lowering limit", segments)
	}
	info, err := os.Stat(segments[1])
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 80 {
		t.Fatalf("new segment size = %d, want at most 80", info.Size())
	}
}

func TestRedactArgv(t *testing.T) {
	argv := RedactArgv([]string{"ict", "create", "U1", "--config", "/synthetic/private/config.yaml", "--token=xoxb-synthetic-token", "xapp-synthetic-token", "--version", "4.22"})
	joined := strings.Join(argv, " ")
	for _, secret := range []string{"/synthetic/private/config.yaml", "xoxb-synthetic-token", "xapp-synthetic-token"} {
		if strings.Contains(joined, secret) {
			t.Fatalf("redacted argv leaked %q: %s", secret, joined)
		}
	}
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
	ch  chan time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *testClock) After(time.Duration) <-chan time.Time { return c.ch }
func TestStartRetentionRunsAtStartupAndPeriodically(t *testing.T) {
	root := t.TempDir()
	logger, err := Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{firstID, secondID} {
		writer, err := logger.Writer(id, "U1")
		if err != nil {
			t.Fatal(err)
		}
		writer.Event("created")
	}
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := &testClock{now: now, ch: make(chan time.Time, 1)}
	refs := []Reference{{ID: firstID, Resolved: true, Updated: now.Add(-31 * 24 * time.Hour)}, {ID: secondID, Resolved: true, Updated: now}}
	context, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := logger.StartRetention(context, clock, 30*24*time.Hour, time.Hour, func() []Reference { return refs }); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, firstID)); !os.IsNotExist(err) {
		t.Fatalf("startup retention did not remove expired logs: %v", err)
	}
	refs[1].Updated = now.Add(-31 * 24 * time.Hour)
	clock.ch <- now.Add(time.Hour)
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(filepath.Join(root, secondID)); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic retention did not remove expired logs")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRetainClosesAndEvictsDeletedWriter(t *testing.T) {
	root := t.TempDir()
	logger, err := Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := logger.Writer(firstID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	writer.Event("created")
	if err := logger.Retain([]Reference{{ID: firstID, Resolved: true, Updated: time.Now().Add(-31 * 24 * time.Hour)}}, time.Now(), 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if writer.file != nil || !writer.retired {
		t.Fatalf("writer after retention = file %v, retired %v", writer.file, writer.retired)
	}
	if _, found := logger.writers[firstID]; found {
		t.Fatal("retained writer remains cached")
	}
	writer.Event("must not recreate deleted logs")
	if _, err := os.Stat(filepath.Join(root, firstID)); !os.IsNotExist(err) {
		t.Fatalf("stale writer recreated deleted logs: %v", err)
	}
}

func TestPathRejectsInvalidIDsAndReturnsAbsolutePrivateDirectory(t *testing.T) {
	root := t.TempDir()
	logger, err := Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	path, err := logger.Path(firstID)
	if err != nil || !filepath.IsAbs(path) || path != filepath.Join(root, firstID) {
		t.Fatalf("Path = %q, %v", path, err)
	}
	if _, err := logger.Path("../" + firstID); err == nil {
		t.Fatal("Path accepted an invalid diagnostic ID")
	}
}

func TestWriterFiltersSplitANSIAndFramesRecordsAcrossRotation(t *testing.T) {
	logger, err := Open(t.TempDir(), 200)
	if err != nil {
		t.Fatal(err)
	}
	logger.now = func() time.Time { return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) }
	writer, err := logger.Writer(firstID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range [][]byte{[]byte("plain \x1b[3"), []byte("1mred\x1b[0"), []byte("m\nnext\rline")} {
		if _, err := writer.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	writer.Event(strings.Repeat("x", 80))
	segments, err := writer.Segments()
	if err != nil || len(segments) < 2 {
		t.Fatalf("segments = %v, %v; want rotation", segments, err)
	}
	for _, segment := range segments {
		contents, err := os.ReadFile(segment)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(string(contents), "\n") || strings.Contains(string(contents), "\\x1b") {
			t.Fatalf("unframed ANSI output in %s: %q", segment, contents)
		}
	}
	all := strings.Builder{}
	for _, segment := range segments {
		contents, _ := os.ReadFile(segment)
		all.Write(contents)
	}
	if !strings.Contains(all.String(), "kind=output plain red\n") || !strings.Contains(all.String(), "kind=output nextline\n") {
		t.Fatalf("filtered output = %q", all.String())
	}
}

func TestWriterPreservesLongJSONLineSplitAcrossWritesAndRotation(t *testing.T) {
	logger, err := Open(t.TempDir(), 200)
	if err != nil {
		t.Fatal(err)
	}
	logger.now = func() time.Time { return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) }
	writer, err := logger.Writer(firstID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	writer.Event(strings.Repeat("b", 80))
	payload := `{"values":{"remote":"` + strings.Repeat("x", 256) + `"}}`
	split := strings.Index(payload, "remote") + 2
	if _, err := writer.Write([]byte(payload[:split])); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte(payload[split:])); err != nil {
		t.Fatal(err)
	}
	writer.Flush()
	writer.Event("after")

	segments, err := writer.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 3 {
		t.Fatalf("segments = %v, want rotation before and after long output", segments)
	}
	for _, segment := range segments {
		contents, err := os.ReadFile(segment)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(string(contents), "\n") {
			t.Fatalf("segment %s is not newline-terminated", segment)
		}
		line := strings.TrimSuffix(string(contents), "\n")
		if index := strings.Index(line, "kind=output "); index >= 0 {
			output := line[index+len("kind=output "):]
			if output != payload {
				t.Fatalf("output = %q, want complete payload", output)
			}
			var parsed map[string]any
			if err := json.Unmarshal([]byte(output), &parsed); err != nil {
				t.Fatalf("parse output JSON: %v", err)
			}
		}
	}
}

func TestResolvedMarkerExpiresRecordlessDiagnosticsOnly(t *testing.T) {
	root := t.TempDir()
	logger, err := Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	logger.now = func() time.Time { return now.Add(-31 * 24 * time.Hour) }
	resolved, err := logger.Writer(firstID, "list")
	if err != nil {
		t.Fatal(err)
	}
	resolved.Event("listed")
	if err := logger.Resolve(firstID); err != nil {
		t.Fatal(err)
	}
	active, err := logger.Writer(secondID, "U1")
	if err != nil {
		t.Fatal(err)
	}
	active.Event("active")
	if err := logger.Retain([]Reference{{ID: secondID, Resolved: false, Updated: now.Add(-90 * 24 * time.Hour)}}, now, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, firstID)); !os.IsNotExist(err) {
		t.Fatalf("resolved recordless diagnostic remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, secondID)); err != nil {
		t.Fatalf("active diagnostic removed: %v", err)
	}
}

func TestRetainOnlyRemovesExpiredResolvedLogs(t *testing.T) {
	root := t.TempDir()
	logger, err := Open(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{firstID, secondID, thirdID} {
		writer, err := logger.Writer(id, "U1")
		if err != nil {
			t.Fatal(err)
		}
		writer.Event("created")
	}
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	err = logger.Retain([]Reference{
		{ID: firstID, Resolved: true, Updated: now.Add(-31 * 24 * time.Hour)},
		// An active or final failed-cleanup record is never eligible for removal.
		{ID: secondID, Resolved: false, Updated: now.Add(-90 * 24 * time.Hour)},
		{ID: thirdID, Resolved: true, Updated: now.Add(-29 * 24 * time.Hour)},
	}, now, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, firstID)); !os.IsNotExist(err) {
		t.Fatalf("expired resolved logs remain: %v", err)
	}
	for _, id := range []string{secondID, thirdID} {
		if _, err := os.Stat(filepath.Join(root, id)); err != nil {
			t.Fatalf("retention removed protected log %s: %v", id, err)
		}
	}
}

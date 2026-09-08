// Package diagnostics writes private, bounded lifecycle diagnostics.
package diagnostics

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var idPattern = regexp.MustCompile(`^[a-f0-9]{24}$`)

const resolvedFilename = ".resolved"

// Logger owns a private root containing one directory for each diagnostic ID.
type Logger struct {
	root    string
	max     int64
	now     func() time.Time
	mu      sync.Mutex
	writers map[string]*Writer
}

// Open creates or verifies a private diagnostic root.
func Open(root string, maxSize int64) (*Logger, error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("diagnostic log size must be positive")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create diagnostic root: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("protect diagnostic root: %w", err)
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve diagnostic root: %w", err)
	}
	return &Logger{root: absolute, max: maxSize, now: time.Now, writers: make(map[string]*Writer)}, nil
}

// ValidID reports whether a value is an opaque diagnostic reference.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// NewID returns an opaque stable reference safe to show in Slack.
func NewID() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate diagnostic ID: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// Path returns the absolute private directory for a valid diagnostic ID. It is
// for local main-log correlation only and must never be sent to Slack.
func (l *Logger) Path(id string) (string, error) {
	if l == nil {
		return "", fmt.Errorf("diagnostics are not configured")
	}
	if !ValidID(id) {
		return "", fmt.Errorf("invalid diagnostic ID")
	}
	return filepath.Join(l.root, id), nil
}

// Writer opens the private stream for one lifecycle. The ID is intentionally
// opaque, while stateID is recorded only inside its private log.
func (l *Logger) Writer(id, stateID string) (*Writer, error) {
	if l == nil {
		return nil, nil
	}
	directory, err := l.Path(id)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if writer := l.writers[id]; writer != nil {
		if writer.stateID != stateID {
			return nil, fmt.Errorf("diagnostic ID belongs to another state")
		}
		return writer, nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create diagnostic directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("protect diagnostic directory: %w", err)
	}
	writer := &Writer{logger: l, id: id, stateID: stateID, directory: directory}
	l.writers[id] = writer
	return writer, nil
}

// Resolve durably marks a non-lifecycle diagnostic as eligible for retention.
func (l *Logger) Resolve(id string) error {
	directory, err := l.Path(id)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create resolved diagnostic directory: %w", err)
	}
	marker := filepath.Join(directory, resolvedFilename)
	temporary, err := os.CreateTemp(directory, ".resolved-")
	if err != nil {
		return fmt.Errorf("create resolved marker: %w", err)
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect resolved marker: %w", err)
	}
	if _, err := io.WriteString(temporary, l.now().UTC().Format(time.RFC3339Nano)+"\n"); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write resolved marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close resolved marker: %w", err)
	}
	if err := os.Rename(name, marker); err != nil {
		return fmt.Errorf("replace resolved marker: %w", err)
	}
	return nil
}

// Writer serializes a single lifecycle stream and rotates before a segment
// grows beyond Logger's configured limit.
type Writer struct {
	logger    *Logger
	id        string
	stateID   string
	directory string
	mu        sync.Mutex
	segment   int
	file      *os.File
	size      int64
	retired   bool
	ansi      ansiFilter
	output    string
}

// ID is the safe external diagnostic reference.
func (w *Writer) ID() string           { return w.id }
func (w *Writer) Event(message string) { w.write("event", message) }
func (w *Writer) Error(err error) {
	if err != nil {
		w.write("error", err.Error())
	}
}
func (w *Writer) Command(argv ...string) { w.write("argv", strings.Join(RedactArgv(argv), " ")) }

// Write records private subprocess output after removing terminal controls.
// It frames output only at source line boundaries, so arbitrary pipe writes
// cannot insert diagnostic metadata into a logical line.
func (w *Writer) Write(data []byte) (int, error) {
	if w == nil {
		return len(data), nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writeOutputLocked(w.ansi.filter(data))
	return len(data), nil
}

// Flush writes a final unterminated subprocess line. Command runners call it
// after both output streams have been drained.
func (w *Writer) Flush() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushOutputLocked()
}

func (w *Writer) write(kind, message string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushOutputLocked()
	w.writeRecordLocked(kind, message, true)
}

func (w *Writer) writeOutputLocked(message string) {
	for len(message) > 0 {
		index := strings.IndexByte(message, '\n')
		if index < 0 {
			w.output += message
			return
		}
		w.output += message[:index]
		w.writeRecordLocked("output", w.output, false)
		w.output = ""
		message = message[index+1:]
	}
}

func (w *Writer) flushOutputLocked() {
	if w.output == "" {
		return
	}
	w.writeRecordLocked("output", w.output, false)
	w.output = ""
}

func (w *Writer) writeRecordLocked(kind, message string, bounded bool) {
	prefix := w.logger.now().UTC().Format(time.RFC3339Nano) + " id=" + w.id + " state=" + w.stateID + " kind=" + kind + " "
	record := prefix + message + "\n"
	if bounded && int64(len(record)) > w.logger.max {
		available := int(w.logger.max) - len(prefix) - 1
		if available < 0 {
			return
		}
		record = prefix + utf8Prefix(message, available) + "\n"
	}
	if w.retired {
		return
	}
	if w.file == nil {
		if err := w.openLocked(); err != nil {
			return
		}
	}
	// Never split a record across rotation. A single output line can exceed
	// max so it remains complete and reconstructable in one segment.
	if w.size > 0 && w.size+int64(len(record)) > w.logger.max {
		if err := w.rotateLocked(); err != nil {
			return
		}
	}
	if _, err := io.WriteString(w.file, record); err != nil {
		return
	}
	w.size += int64(len(record))
}

func utf8Prefix(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && limit < len(value) && (value[limit]&0xc0) == 0x80 {
		limit--
	}
	return value[:limit]
}

type ansiFilter struct{ state byte }

func (f *ansiFilter) filter(data []byte) string {
	var output strings.Builder
	for _, b := range data {
		switch f.state {
		case 1: // ESC
			if b == '[' {
				f.state = 2
			} else {
				f.state = 0
			}
			continue
		case 2: // CSI ends at a final byte.
			if b >= 0x40 && b <= 0x7e {
				f.state = 0
			}
			continue
		}
		if b == 0x1b {
			f.state = 1
			continue
		}
		if (b < 0x20 && b != '\n' && b != '\t') || b == 0x7f {
			continue
		}
		output.WriteByte(b)
	}
	return output.String()
}

func (w *Writer) openLocked() error {
	entries, err := os.ReadDir(w.directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var segment int
		if _, err := fmt.Sscanf(entry.Name(), "%06d.log", &segment); err == nil && segment > w.segment {
			w.segment = segment
		}
	}
	if w.segment == 0 {
		w.segment = 1
	}
	file, err := os.OpenFile(filepath.Join(w.directory, fmt.Sprintf("%06d.log", w.segment)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	w.file, w.size = file, info.Size()
	return nil
}
func (w *Writer) rotateLocked() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	w.file, w.size, w.segment = nil, 0, w.segment+1
	return w.openLocked()
}
func (w *Writer) retireLocked() error {
	w.retired = true
	w.size = 0
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// RedactArgv returns an argv vector suitable for private logs.
func RedactArgv(argv []string) []string {
	redacted := append([]string(nil), argv...)
	for i, value := range redacted {
		lower := strings.ToLower(value)
		if strings.HasPrefix(lower, "--") && (strings.Contains(lower, "token") || strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "config") || strings.Contains(lower, "ssh-key")) {
			if strings.Contains(value, "=") {
				redacted[i] = value[:strings.Index(value, "=")+1] + "[REDACTED]"
			} else if i+1 < len(redacted) {
				redacted[i+1] = "[REDACTED]"
			}
		}
		if strings.Contains(lower, "xoxb-") || strings.Contains(lower, "xapp-") || strings.Contains(lower, "token=") || strings.Contains(lower, "password=") {
			redacted[i] = "[REDACTED]"
		}
	}
	return redacted
}

type Reference struct {
	ID       string
	Resolved bool
	Updated  time.Time
}
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}
type realClock struct{}

func (realClock) Now() time.Time                             { return time.Now() }
func (realClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

func (l *Logger) StartRetention(ctx context.Context, clock Clock, retention, interval time.Duration, references func() []Reference) error {
	if clock == nil {
		clock = realClock{}
	}
	if interval <= 0 {
		return fmt.Errorf("retention interval must be positive")
	}
	if err := l.Retain(references(), clock.Now(), retention); err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-clock.After(interval):
				_ = l.Retain(references(), clock.Now(), retention)
			}
		}
	}()
	return nil
}

// Retain removes lifecycle-resolved records by their record timestamp and
// recordless diagnostics by their durable resolved-marker timestamp.
func (l *Logger) Retain(references []Reference, now time.Time, retention time.Duration) error {
	if l == nil || retention <= 0 {
		return nil
	}
	byID := make(map[string]Reference, len(references))
	for _, reference := range references {
		byID[reference.ID] = reference
	}
	entries, err := os.ReadDir(l.root)
	if err != nil {
		return fmt.Errorf("read diagnostic root: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || !ValidID(entry.Name()) {
			continue
		}
		reference, found := byID[entry.Name()]
		if !found {
			marker, markerErr := resolvedAt(filepath.Join(l.root, entry.Name(), resolvedFilename))
			if markerErr != nil {
				continue
			}
			reference, found = Reference{Resolved: true, Updated: marker}, true
		}
		if !found || !reference.Resolved || reference.Updated.Add(retention).After(now) {
			continue
		}
		l.mu.Lock()
		if writer := l.writers[entry.Name()]; writer != nil {
			writer.mu.Lock()
			err := writer.retireLocked()
			writer.mu.Unlock()
			delete(l.writers, entry.Name())
			if err != nil {
				l.mu.Unlock()
				return fmt.Errorf("close resolved diagnostic logs: %w", err)
			}
		}
		if err := os.RemoveAll(filepath.Join(l.root, entry.Name())); err != nil {
			l.mu.Unlock()
			return fmt.Errorf("remove resolved diagnostic logs: %w", err)
		}
		l.mu.Unlock()
	}
	return nil
}
func resolvedAt(path string) (time.Time, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data)))
}
func (w *Writer) Segments() ([]string, error) {
	entries, err := os.ReadDir(w.directory)
	if err != nil {
		return nil, err
	}
	segments := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			segments = append(segments, filepath.Join(w.directory, entry.Name()))
		}
	}
	sort.Strings(segments)
	return segments, nil
}

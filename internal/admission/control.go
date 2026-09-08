// Package admission controls user-initiated lifecycle admission.
package admission

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const filename = "admission.json"

type Mode string

const (
	Accepting      Mode = "accepting"
	Paused         Mode = "paused"
	DrainingToStop Mode = "draining-to-stop"
)

type Status struct {
	Mode    Mode
	Reviews int
	Applies int
	Cleanup int
}

type persisted struct {
	Mode Mode `json:"mode"`
}

// Control serializes admission changes, activity accounting, and paused-state
// persistence. It deliberately does not own lifecycle work.
type Control struct {
	mu             sync.Mutex
	path           string
	mode           Mode
	reviews        int
	applies        int
	cleanup        int
	generation     uint64
	drained        chan struct{}
	drainClosed    bool
	drainWasPaused bool
}

// Open restores the durable paused mode. Missing state means accepting.
func Open(stateDirectory string) (*Control, error) {
	if stateDirectory == "" {
		return nil, fmt.Errorf("admission state directory is required")
	}
	control := &Control{path: filepath.Join(stateDirectory, filename), mode: Accepting}
	contents, err := os.ReadFile(control.path)
	if os.IsNotExist(err) {
		return control, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read admission state: %w", err)
	}
	var saved persisted
	if err := json.Unmarshal(contents, &saved); err != nil {
		return nil, fmt.Errorf("decode admission state: %w", err)
	}
	if saved.Mode != Paused {
		return nil, fmt.Errorf("decode admission state: invalid mode")
	}
	control.mode = Paused
	return control, nil
}

func (c *Control) Accepting() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode == Accepting
}

// Admit atomically checks admission and runs a state-changing operation. Pause
// cannot complete between the check and operation, making the callback's
// completion the admission linearization point.
func (c *Control) Admit(operation func() error) (admitted bool, generation uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != Accepting {
		return false, c.generation, nil
	}
	return true, c.generation, operation()
}

// Generation changes for each pause transition, including a pause followed by
// an immediate unpause. Lifecycle creation uses it to decline a review that
// raced with a pause before it became visible to the review registry.
func (c *Control) Generation() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *Control) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status()
}

// Pause persists the admission boundary before returning. Repeating it is a
// no-op, while draining is rejected so stop remains irreversible.
func (c *Control) Pause() (status Status, changed bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode == DrainingToStop {
		return c.status(), false, fmt.Errorf("admission is draining to stop")
	}
	if c.mode == Paused {
		return c.status(), false, nil
	}
	if err := c.write(Paused); err != nil {
		return c.status(), false, err
	}
	c.mode = Paused
	c.generation++
	return c.status(), true, nil
}

// Unpause removes the durable pause marker. Repeating it is a no-op. Draining
// is irreversible for the lifetime of this process.
func (c *Control) Unpause() (status Status, changed bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode == DrainingToStop {
		return c.status(), false, fmt.Errorf("admission is draining to stop")
	}
	if c.mode == Accepting {
		return c.status(), false, nil
	}
	if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
		return c.status(), false, fmt.Errorf("remove admission state: %w", err)
	}
	c.mode = Accepting
	return c.status(), true, nil
}

// Stop atomically closes admission and returns a channel closed after every
// active review, apply, and cleanup chain reaches a terminal state. Its pause
// marker makes a crash during draining recover as paused.
func (c *Control) Stop() (status Status, changed bool, drained <-chan struct{}, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode == DrainingToStop {
		return c.status(), false, c.drained, nil
	}
	wasPaused := c.mode == Paused
	if !wasPaused {
		if err := c.write(Paused); err != nil {
			return c.status(), false, nil, err
		}
	}
	c.mode = DrainingToStop
	c.generation++
	c.drainWasPaused = wasPaused
	c.drained = make(chan struct{})
	c.drainClosed = false
	c.closeDrainIfIdle()
	return c.status(), true, c.drained, nil
}

// CompleteStop clears the transient pause marker after a normal graceful
// drain. A pause that predated stop remains durable.
func (c *Control) CompleteStop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != DrainingToStop {
		return nil
	}
	if c.drainWasPaused {
		c.mode = Paused
		return nil
	}
	if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove completed stop state: %w", err)
	}
	c.mode = Accepting
	return nil
}

// Activity returns an idempotent completion callback for active review, apply,
// or cleanup work. Ready leases intentionally have no activity callback.
func (c *Control) Activity(kind string) func() {
	c.mu.Lock()
	switch kind {
	case "review":
		c.reviews++
	case "apply":
		c.applies++
	case "cleanup":
		c.cleanup++
	default:
		c.mu.Unlock()
		return func() {}
	}
	c.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			switch kind {
			case "review":
				c.reviews--
			case "apply":
				c.applies--
			case "cleanup":
				c.cleanup--
			}
			c.closeDrainIfIdle()
			c.mu.Unlock()
		})
	}
}

func (c *Control) status() Status {
	return Status{Mode: c.mode, Reviews: c.reviews, Applies: c.applies, Cleanup: c.cleanup}
}

func (c *Control) closeDrainIfIdle() {
	if c.drained != nil && !c.drainClosed && c.mode == DrainingToStop && c.reviews == 0 && c.applies == 0 && c.cleanup == 0 {
		close(c.drained)
		c.drainClosed = true
	}
}

func (c *Control) write(mode Mode) error {
	contents, err := json.Marshal(persisted{Mode: mode})
	if err != nil {
		return fmt.Errorf("encode admission state: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(c.path), ".servitor-admission-")
	if err != nil {
		return fmt.Errorf("create admission state: %w", err)
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return fmt.Errorf("protect admission state: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return fmt.Errorf("write admission state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close admission state: %w", err)
	}
	if err := os.Rename(name, c.path); err != nil {
		return fmt.Errorf("replace admission state: %w", err)
	}
	return nil
}

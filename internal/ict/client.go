// Package ict provides the constrained ICT command boundary used by Servitor.
package ict

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bevicted/servitor/internal/command"
	"github.com/bevicted/servitor/internal/diagnostics"
	"github.com/bevicted/servitor/internal/state"
)

// Client runs constrained ICT commands.
type Client struct {
	Path        string
	Runner      command.Runner
	Log         io.Writer
	Diagnostics *diagnostics.Logger
	// Logf receives only safe command-boundary summaries, never argv or output.
	Logf func(string, ...any)
	// InventorySource is a test seam for an already-validated ICT inventory.
	// Production clients leave it nil and always invoke `ict list --output json`.
	InventorySource func(context.Context) (state.WorkspaceInventory, error)
}

func (c Client) List(ctx context.Context) (string, error) {
	return c.list(ctx, c.Runner)
}

// ListDiagnostic keeps a lifecycle preflight's private output with that lifecycle.
func (c Client) ListDiagnostic(ctx context.Context, stateID, diagnosticID string) (string, error) {
	writer, err := c.diagnosticWriter(stateID, diagnosticID)
	if err != nil {
		return "", fmt.Errorf("open diagnostic log: %w", err)
	}
	if writer == nil {
		return "", fmt.Errorf("open diagnostic log: diagnostics are not configured")
	}
	runner := c.Runner
	runner.Log = writer
	return c.list(ctx, runner)
}

func (c Client) list(ctx context.Context, runner command.Runner) (string, error) {
	result, err := runner.Run(ctx, c.Path, "list")
	if err != nil {
		return "", fmt.Errorf("ict list: %w", err)
	}
	return result.Stdout, nil
}
func (c Client) Destroy(ctx context.Context, stateID string) error {
	if _, err := c.Runner.Run(ctx, c.Path, "destroy", stateID); err != nil {
		return fmt.Errorf("ict destroy: %w", err)
	}
	return nil
}

// DestroyDiagnostic keeps one cleanup command's private output with its lifecycle.
func (c Client) DestroyDiagnostic(ctx context.Context, stateID, diagnosticID string) error {
	writer, err := c.diagnosticWriter(stateID, diagnosticID)
	if err != nil {
		return fmt.Errorf("open diagnostic log: %w", err)
	}
	if writer == nil {
		return fmt.Errorf("open diagnostic log: diagnostics are not configured")
	}
	runner := c.Runner
	runner.Log = writer
	c.logProcess("destroy", stateID, diagnosticID, "started")
	if _, err := runner.Run(ctx, c.Path, "destroy", stateID); err != nil {
		c.logProcess("destroy", stateID, diagnosticID, "failed")
		return fmt.Errorf("ict destroy: %w", err)
	}
	c.logProcess("destroy", stateID, diagnosticID, "completed")
	return nil
}

// WorkspaceInventory invokes ICT's private machine-readable inventory contract.
func (c Client) WorkspaceInventory(ctx context.Context) (state.WorkspaceInventory, error) {
	if c.InventorySource != nil {
		return c.InventorySource(ctx)
	}
	return c.workspaceInventory(ctx, c.Runner)
}

// WorkspaceInventoryDiagnostic keeps private inventory output with a lifecycle.
func (c Client) WorkspaceInventoryDiagnostic(ctx context.Context, stateID, diagnosticID string) (state.WorkspaceInventory, error) {
	if c.InventorySource != nil {
		return c.InventorySource(ctx)
	}
	writer, err := c.diagnosticWriter(stateID, diagnosticID)
	if err != nil {
		return state.WorkspaceInventory{}, fmt.Errorf("open diagnostic log: %w", err)
	}
	if writer == nil {
		return state.WorkspaceInventory{}, fmt.Errorf("open diagnostic log: diagnostics are not configured")
	}
	runner := c.Runner
	runner.Log = writer
	c.logProcess("list", stateID, diagnosticID, "started")
	inventory, err := c.workspaceInventory(ctx, runner)
	if err != nil {
		c.logProcess("list", stateID, diagnosticID, "failed")
		return state.WorkspaceInventory{}, err
	}
	c.logProcess("list", stateID, diagnosticID, "completed")
	return inventory, nil
}

func (c Client) workspaceInventory(ctx context.Context, runner command.Runner) (state.WorkspaceInventory, error) {
	result, err := runner.Run(ctx, c.Path, "list", "--output", "json")
	if err != nil {
		return state.WorkspaceInventory{}, fmt.Errorf("ict list --output json: %w", err)
	}
	inventory, err := state.DecodeWorkspaceInventory([]byte(result.Stdout))
	if err != nil {
		return state.WorkspaceInventory{}, fmt.Errorf("ict workspace inventory: %w", err)
	}
	return inventory, nil
}

func (c Client) HasState(ctx context.Context, stateID string) (bool, error) {
	inventory, err := c.WorkspaceInventory(ctx)
	if err != nil {
		return false, err
	}
	for _, workspace := range inventory.Workspaces {
		if workspace.ID == stateID {
			return true, nil
		}
	}
	return false, nil
}

// CreateProcess holds the single ICT process from planning through apply.
type CreateProcess struct {
	process  *os.Process
	stdin    *os.File
	finished chan struct{}
	err      error
	output   *boundedOutput
	once     sync.Once
}

// StartCreate runs `ict create` only with internal state/config/prefix/confirmation arguments.
func (c Client) StartCreate(stateID, configPath string, args []string) (*CreateProcess, error) {
	if !filepath.IsAbs(c.Path) {
		return nil, fmt.Errorf("ICT path must be absolute")
	}
	argv := []string{c.Path, "create", stateID, "--config", configPath, "--prefix", "servitor", "--confirm-stdin"}
	argv = append(argv, args...)
	diagnosticID := ""
	if writer, ok := c.Log.(*diagnostics.Writer); ok {
		diagnosticID = writer.ID()
	}
	c.logProcess("create", stateID, diagnosticID, "started")
	if writer, ok := c.Log.(*diagnostics.Writer); ok {
		writer.Command(argv...)
		writer.Event("create process started")
	}
	stdinReader, stdin, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("open ict stdin: %w", err)
	}
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		stdinReader.Close()
		stdin.Close()
		return nil, fmt.Errorf("open ict stdout: %w", err)
	}
	stderr, stderrWriter, err := os.Pipe()
	if err != nil {
		stdinReader.Close()
		stdin.Close()
		stdout.Close()
		stdoutWriter.Close()
		return nil, fmt.Errorf("open ict stderr: %w", err)
	}
	process, err := os.StartProcess(c.Path, argv, &os.ProcAttr{Files: []*os.File{stdinReader, stdoutWriter, stderrWriter}})
	stdinReader.Close()
	if err != nil {
		c.logProcess("create", stateID, diagnosticID, "failed")
		if writer, ok := c.Log.(*diagnostics.Writer); ok {
			writer.Error(fmt.Errorf("start ict create: %w", err))
		}
		stdin.Close()
		stdout.Close()
		stdoutWriter.Close()
		stderr.Close()
		stderrWriter.Close()
		return nil, fmt.Errorf("start ict create: %w", err)
	}
	out := &boundedOutput{limit: 64 * 1024, log: c.Log}
	p := &CreateProcess{process: process, stdin: stdin, finished: make(chan struct{}), output: out}
	stdoutDone := copyTo(out, stdout)
	stderrDone := copyTo(out, stderr)
	go func() {
		state, waitErr := process.Wait()
		stdoutWriter.Close()
		stderrWriter.Close()
		<-stdoutDone
		<-stderrDone
		if writer, ok := c.Log.(*diagnostics.Writer); ok {
			writer.Flush()
		}
		if waitErr != nil {
			p.err = fmt.Errorf("ict create: %w", waitErr)
		} else if !state.Success() {
			p.err = fmt.Errorf("ict create: exit status %s", state)
		}
		if p.err != nil {
			if writer, ok := c.Log.(*diagnostics.Writer); ok {
				writer.Error(p.err)
			}
			c.logProcess("create", stateID, diagnosticID, "failed")
		} else {
			if writer, ok := c.Log.(*diagnostics.Writer); ok {
				writer.Event("create process completed")
			}
			c.logProcess("create", stateID, diagnosticID, "completed")
		}
		close(p.finished)
	}()
	return p, nil
}
func (p *CreateProcess) Wait() error           { <-p.finished; return p.err }
func (p *CreateProcess) Done() <-chan struct{} { return p.finished }

// Prompted closes when ICT writes its literal approval prompt.
func (p *CreateProcess) Prompted() <-chan struct{} { return p.output.Prompted() }
func (p *CreateProcess) Output() string            { return p.output.String() }
func (p *CreateProcess) Confirm(approved bool) error {
	answer := "no\n"
	if approved {
		answer = "yes\n"
	}
	_, err := io.WriteString(p.stdin, answer)
	if closeErr := p.stdin.Close(); err == nil {
		err = closeErr
	}
	return err
}

// Interrupt sends one graceful signal and never kills the process.
func (p *CreateProcess) Interrupt() error {
	var err error
	p.once.Do(func() { err = p.process.Signal(os.Interrupt) })
	return err
}
func copyTo(destination io.Writer, source *os.File) <-chan struct{} {
	done := make(chan struct{})
	go func() { _, _ = io.Copy(destination, source); _ = source.Close(); close(done) }()
	return done
}

// NewClient supplies bounded output capture and private detailed command logging.
func NewClient(path string, log io.Writer) Client {
	return Client{Path: path, Runner: command.Runner{MaxOutput: 64 * 1024, Log: log}, Log: log}
}

// WithDiagnostic returns an isolated client whose create stream is tied to one lifecycle.
// It never falls back to the shared service log when the private writer is unavailable.
func (c Client) WithDiagnostic(id, stateID string) (Client, error) {
	writer, err := c.diagnosticWriter(stateID, id)
	if err != nil {
		return c, fmt.Errorf("open diagnostic log: %w", err)
	}
	if writer == nil {
		return c, fmt.Errorf("open diagnostic log: diagnostics are not configured")
	}
	c.Log = writer
	c.Runner.Log = writer
	return c, nil
}

func (c Client) logProcess(operation, stateID, diagnosticID, result string) {
	if c.Logf != nil {
		c.Logf("subprocess operation=%q state_id=%q diagnostic_id=%q result=%q", operation, stateID, diagnosticID, result)
	}
}

func (c Client) diagnosticWriter(stateID, id string) (*diagnostics.Writer, error) {
	if id == "" {
		return nil, fmt.Errorf("diagnostic ID is required")
	}
	return c.Diagnostics.Writer(id, stateID)
}

// DiagnosticPath returns a private diagnostic path for main-log correlation.
func (c Client) DiagnosticPath(id string) (string, error) {
	if c.Diagnostics == nil {
		return "", fmt.Errorf("diagnostics are not configured")
	}
	return c.Diagnostics.Path(id)
}

// ResolveDiagnostic marks a recordless command diagnostic for private retention.
func (c Client) ResolveDiagnostic(id string) error {
	if c.Diagnostics == nil {
		return fmt.Errorf("diagnostics are not configured")
	}
	return c.Diagnostics.Resolve(id)
}

const approvalPrompt = "Do you want to perform these actions?"

type boundedOutput struct {
	mu        sync.Mutex
	data      bytes.Buffer
	limit     int
	truncated bool
	log       io.Writer
	prompted  chan struct{}
	prompt    bool
	tail      string
}

func (o *boundedOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	written := len(p)
	o.detectPrompt(p)
	if o.log != nil {
		_, _ = o.log.Write(p)
	}
	remaining := o.limit - o.data.Len()
	if remaining <= 0 {
		o.truncated = true
		return written, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		o.truncated = true
	}
	_, _ = o.data.Write(p)
	return written, nil
}

// Prompted closes when the live output contains ICT's approval prompt.
func (o *boundedOutput) Prompted() <-chan struct{} {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.prompted == nil {
		o.prompted = make(chan struct{})
		if o.prompt {
			close(o.prompted)
		}
	}
	return o.prompted
}

func (o *boundedOutput) detectPrompt(p []byte) {
	if o.prompt {
		return
	}
	output := o.tail + string(p)
	if strings.Contains(output, approvalPrompt) {
		o.prompt = true
		if o.prompted == nil {
			o.prompted = make(chan struct{})
		}
		close(o.prompted)
		return
	}
	if len(output) >= len(approvalPrompt) {
		o.tail = output[len(output)-len(approvalPrompt)+1:]
	} else {
		o.tail = output
	}
}
func (o *boundedOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	value := o.data.String()
	if o.truncated {
		value += "\n[output truncated]"
	}
	return value
}

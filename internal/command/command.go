// Package command runs fixed executable argv vectors without a shell.
package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

// Runner executes commands and preserves bounded output. Stdout and Stderr
// optionally stream subprocess output verbatim; Log receives sanitized failure diagnostics.
type Runner struct {
	MaxOutput int
	Stdout    io.Writer
	Stderr    io.Writer
	Log       io.Writer
}

// Result contains bounded command output.
type Result struct {
	Stdout          string
	Stderr          string
	StdoutTruncated bool
	StderrTruncated bool
}

// Run executes name and args directly, never through a shell.
func (r Runner) Run(ctx context.Context, name string, args ...string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	executable, err := resolveExecutable(name)
	if err != nil {
		return Result{}, fmt.Errorf("resolve command: %w", err)
	}
	limit := r.MaxOutput
	if limit <= 0 {
		limit = 64 * 1024
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return Result{}, fmt.Errorf("create stdout pipe: %w", err)
	}
	defer closeFile(stdoutReader)
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		closeFile(stdoutWriter)
		return Result{}, fmt.Errorf("create stderr pipe: %w", err)
	}
	defer closeFile(stderrReader)

	process, err := os.StartProcess(executable, append([]string{name}, args...), &os.ProcAttr{Files: []*os.File{os.Stdin, stdoutWriter, stderrWriter}})
	if err != nil {
		closeFile(stdoutWriter)
		closeFile(stderrWriter)
		return Result{}, fmt.Errorf("start command: %w", err)
	}
	closeFile(stdoutWriter)
	closeFile(stderrWriter)
	var stdout, stderr boundedBuffer
	stdout.limit = limit
	stderr.limit = limit
	stdoutDone := copyOutput(&capturingWriter{capture: &stdout, stream: r.Stdout}, stdoutReader)
	stderrDone := copyOutput(&capturingWriter{capture: &stderr, stream: r.Stderr}, stderrReader)
	waitDone := make(chan error, 1)
	go func() {
		state, waitErr := process.Wait()
		if waitErr != nil {
			waitDone <- waitErr
			return
		}
		if !state.Success() {
			waitDone <- fmt.Errorf("exit status %s", state)
			return
		}
		waitDone <- nil
	}()
	select {
	case err = <-waitDone:
	case <-ctx.Done():
		_ = process.Kill()
		<-waitDone
		err = ctx.Err()
	}
	<-stdoutDone
	<-stderrDone
	result := Result{
		Stdout:          stdout.String(),
		Stderr:          stderr.String(),
		StdoutTruncated: stdout.truncated,
		StderrTruncated: stderr.truncated,
	}
	if err != nil {
		if r.Log != nil {
			log.New(r.Log, "command: ", log.LstdFlags|log.LUTC).Printf("%q failed: %v", executable, err)
		}
		return result, fmt.Errorf("command failed")
	}
	return result, nil
}

func resolveExecutable(name string) (string, error) {
	if strings.ContainsRune(name, os.PathSeparator) {
		return name, nil
	}
	executable, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("look up executable: %w", err)
	}
	return executable, nil
}

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Printf("close command pipe: %v", err)
	}
}

func copyOutput(destination io.Writer, source *os.File) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(destination, source)
		done <- err
	}()
	return done
}

type capturingWriter struct {
	capture *boundedBuffer
	stream  io.Writer
}

func (w *capturingWriter) Write(contents []byte) (int, error) {
	captured, err := w.capture.Write(contents)
	if err != nil {
		return captured, err
	}
	if w.stream != nil {
		streamed, streamErr := w.stream.Write(contents)
		if streamErr != nil || streamed != len(contents) {
			w.stream = nil
		}
	}
	return len(contents), nil
}

type boundedBuffer struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(contents []byte) (int, error) {
	written := len(contents)
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		if len(contents) > remaining {
			contents = contents[:remaining]
			b.truncated = true
		}
		_, _ = b.buffer.Write(contents)
	} else {
		b.truncated = true
	}
	return written, nil
}

func (b *boundedBuffer) String() string {
	if b.truncated {
		return b.buffer.String() + "\n[output truncated]"
	}
	return b.buffer.String()
}

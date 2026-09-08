// Package command runs fixed executable argv vectors without a shell.
package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/bevicted/servitor/internal/diagnostics"
)

// Runner executes commands and preserves bounded diagnostic output for private logs.
type Runner struct {
	MaxOutput int
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
	if writer, ok := r.Log.(*diagnostics.Writer); ok {
		writer.Command(append([]string{name}, args...)...)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	limit := r.MaxOutput
	if limit <= 0 {
		limit = 64 * 1024
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return Result{}, fmt.Errorf("create stdout pipe: %w", err)
	}
	defer stdoutReader.Close()
	defer stdoutWriter.Close()
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		return Result{}, fmt.Errorf("create stderr pipe: %w", err)
	}
	defer stderrReader.Close()
	defer stderrWriter.Close()

	process, err := os.StartProcess(name, append([]string{name}, args...), &os.ProcAttr{Files: []*os.File{os.Stdin, stdoutWriter, stderrWriter}})
	if err != nil {
		startErr := fmt.Errorf("start command: %w", err)
		if writer, ok := r.Log.(*diagnostics.Writer); ok {
			writer.Error(startErr)
		}
		return Result{}, startErr
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	var stdout, stderr boundedBuffer
	stdout.limit = limit
	stderr.limit = limit
	stdoutDestination, stderrDestination := io.Writer(&stdout), io.Writer(&stderr)
	if r.Log != nil {
		stdoutDestination = io.MultiWriter(stdoutDestination, r.Log)
		stderrDestination = io.MultiWriter(stderrDestination, r.Log)
	}
	stdoutDone := copyOutput(stdoutDestination, stdoutReader)
	stderrDone := copyOutput(stderrDestination, stderrReader)
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
	if writer, ok := r.Log.(*diagnostics.Writer); ok {
		writer.Flush()
	}
	result := Result{
		Stdout:          stdout.String(),
		Stderr:          stderr.String(),
		StdoutTruncated: stdout.truncated,
		StderrTruncated: stderr.truncated,
	}
	if err != nil {
		if r.Log != nil {
			argv := diagnostics.RedactArgv(append([]string{name}, args...))
			log.New(r.Log, "command: ", log.LstdFlags|log.LUTC).Printf("%q failed: %v\nstdout:\n%s\nstderr:\n%s", argv, err, result.Stdout, result.Stderr)
		}
		return result, fmt.Errorf("command failed")
	}
	return result, nil
}

func copyOutput(destination io.Writer, source *os.File) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(destination, source)
		done <- err
	}()
	return done
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

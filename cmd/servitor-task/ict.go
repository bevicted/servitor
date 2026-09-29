package main

import (
	"context"
	"io"
	"os"

	"github.com/bevicted/servitor/internal/command"
)

const trustedICTExecutable = "/usr/local/bin/ict"

// ictRunner is the only command runner permitted to pass task credentials to
// ICT. The task image is digest-pinned and owns this executable; custom ICT
// paths explicitly receive an empty environment while other children inherit.
func ictRunner(ictPath string, maxOutput int, stdout, stderr, logs io.Writer) command.Runner {
	runner := command.Runner{
		MaxOutput: maxOutput,
		Stdout:    stdout,
		Stderr:    stderr,
		Log:       logs,
		Env:       []string{},
	}
	if ictPath == trustedICTExecutable {
		runner.Env = os.Environ()
	}
	return runner
}

func runICT(ctx context.Context, ictPath string, maxOutput int, stdout, stderr, logs io.Writer, args ...string) (command.Result, error) {
	return ictRunner(ictPath, maxOutput, stdout, stderr, logs).Run(ctx, ictPath, args...)
}

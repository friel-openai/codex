// Package gitrepo provides the bounded system-Git operations needed by
// layerctl. It owns command construction and error reporting, not Frodex
// projection policy.
package gitrepo

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Repository runs Git commands against one shared repository.
type Repository struct {
	Root string
}

// Discover resolves the repository containing directory.
func Discover(ctx context.Context, directory string) (*Repository, error) {
	root, err := output(ctx, directory, "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, err
	}
	return &Repository{Root: root}, nil
}

// Output runs Git in directory and returns trimmed standard output.
func (r *Repository) Output(ctx context.Context, directory string, args ...string) (string, error) {
	output, err := r.Bytes(ctx, directory, args...)
	return strings.TrimSpace(string(output)), err
}

// Run runs Git in directory and discards standard output after successful
// completion. Failures retain both output streams in the returned error.
func (r *Repository) Run(ctx context.Context, directory string, args ...string) error {
	_, err := r.Bytes(ctx, directory, args...)
	return err
}

// Bytes runs Git in directory without altering standard output bytes.
func (r *Repository) Bytes(ctx context.Context, directory string, args ...string) ([]byte, error) {
	return commandOutput(ctx, directory, Invocation{Arguments: args})
}

// Invoke runs Git with input and environment needed by commands that construct
// objects without exposing temporary process files to their callers.
func (r *Repository) Invoke(ctx context.Context, directory string, invocation Invocation) ([]byte, error) {
	return commandOutput(ctx, directory, invocation)
}

// Invocation describes one system-Git process. Environment entries override
// inherited values with the same name.
type Invocation struct {
	Arguments   []string
	Stdin       []byte
	Environment []string
}

func output(ctx context.Context, directory string, args ...string) (string, error) {
	output, err := commandOutput(ctx, directory, Invocation{Arguments: args})
	return strings.TrimSpace(string(output)), err
}

func commandOutput(ctx context.Context, directory string, invocation Invocation) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", invocation.Arguments...)
	command.Dir = directory
	command.Stdin = bytes.NewReader(invocation.Stdin)
	if len(invocation.Environment) > 0 {
		command.Env = append(os.Environ(), invocation.Environment...)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if detail == "" {
			return nil, fmt.Errorf("git %s: %w", strings.Join(invocation.Arguments, " "), err)
		}
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(invocation.Arguments, " "), err, detail)
	}
	return stdout.Bytes(), nil
}

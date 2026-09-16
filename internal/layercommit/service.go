// Package layercommit translates between accepted Git commits and canonical
// Frodex layer definitions. A layer owns an exact commit message and an
// optional binary-safe tree diff; callers own ordering and lifecycle policy.
package layercommit

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/friel-openai/codex/layerctl/internal/definition"
	"github.com/friel-openai/codex/layerctl/internal/gitrepo"
)

// Service captures, applies, and verifies generated layer commits.
type Service struct {
	Git *gitrepo.Repository // required
}

// Captured is the canonical representation of one generated commit. Patch is
// empty when Before and After have the same tree.
type Captured struct {
	Message []byte
	Patch   []byte
}

// CaptureRequest selects the predecessor and accepted endpoint for one layer.
type CaptureRequest struct {
	Before string
	After  string
}

// Capture returns After's exact commit message and the tree delta from Before
// to After. Git plumbing owns the diff semantics and binary representation.
func (s *Service) Capture(ctx context.Context, req CaptureRequest) (Captured, error) {
	message, err := commitMessage(s.Git, ctx, s.Git.Root, req.After)
	if err != nil {
		return Captured{}, err
	}
	patch, err := s.Git.Bytes(
		ctx,
		s.Git.Root,
		"-c", "diff.suppressBlankEmpty=true",
		"diff-tree",
		"--patch",
		"--binary",
		"--full-index",
		"--no-commit-id",
		"-r",
		"--no-renames",
		"--no-ext-diff",
		"--no-textconv",
		"--no-color",
		"--diff-algorithm=myers",
		"--src-prefix=a/",
		"--dst-prefix=b/",
		req.Before,
		req.After,
	)
	if err != nil {
		return Captured{}, fmt.Errorf("diff layer trees: %w", err)
	}
	return Captured{Message: message, Patch: patch}, nil
}

// Apply applies unit's optional tree diff and creates its generated commit.
func (s *Service) Apply(ctx context.Context, worktree string, unit definition.Unit) error {
	if unit.PatchPath != "" {
		if err := s.Git.Run(
			ctx,
			worktree,
			"-c", "rerere.enabled=false",
			"apply",
			"--3way",
			"--index",
			"--whitespace=nowarn",
			"--",
			unit.PatchPath,
		); err != nil {
			return err
		}
	}
	return s.Commit(ctx, worktree, unit)
}

// Commit records the staged tree using unit's exact commit message. It is also
// used after an operator resolves a conflicted three-way application.
func (s *Service) Commit(ctx context.Context, worktree string, unit definition.Unit) error {
	return s.Git.Run(
		ctx,
		worktree,
		"commit",
		"--allow-empty",
		"--no-verify",
		"--no-gpg-sign",
		"--cleanup=verbatim",
		"--file", unit.MessagePath,
	)
}

// HasConflicts reports whether worktree contains unresolved index entries.
func (s *Service) HasConflicts(ctx context.Context, worktree string) (bool, error) {
	output, err := s.Git.Bytes(ctx, worktree, "ls-files", "--unmerged")
	if err != nil {
		return false, fmt.Errorf("inspect unresolved layer paths: %w", err)
	}
	return len(output) != 0, nil
}

// Message reads the exact generated commit message from unit.
func (s *Service) Message(unit definition.Unit) ([]byte, error) {
	message, err := os.ReadFile(unit.MessagePath)
	if err != nil {
		return nil, fmt.Errorf("read layer message %q: %w", unit.MessagePath, err)
	}
	return message, nil
}

// Matches applies unit from before in a disposable worktree and reports
// whether the generated tree and message equal after.
func (s *Service) Matches(ctx context.Context, unit definition.Unit, before, after string) (bool, error) {
	worktree, err := os.MkdirTemp("", "layerctl-layercommit-")
	if err != nil {
		return false, fmt.Errorf("create verification worktree path: %w", err)
	}
	if err := os.Remove(worktree); err != nil {
		return false, fmt.Errorf("prepare verification worktree path %q: %w", worktree, err)
	}
	if err := s.Git.Run(ctx, s.Git.Root, "worktree", "add", "--detach", worktree, before); err != nil {
		return false, fmt.Errorf("create verification worktree: %w", err)
	}
	defer func() {
		_ = s.Git.Run(context.Background(), s.Git.Root, "worktree", "remove", "--force", worktree)
	}()

	if err := s.Apply(ctx, worktree, unit); err != nil {
		return false, fmt.Errorf("apply verification layer: %w", err)
	}
	gotTree, err := s.Git.Output(ctx, worktree, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return false, fmt.Errorf("resolve generated tree: %w", err)
	}
	wantTree, err := s.Git.Output(ctx, s.Git.Root, "rev-parse", after+"^{tree}")
	if err != nil {
		return false, fmt.Errorf("resolve accepted tree: %w", err)
	}
	gotMessage, err := commitMessage(s.Git, ctx, worktree, "HEAD")
	if err != nil {
		return false, err
	}
	wantMessage, err := commitMessage(s.Git, ctx, s.Git.Root, after)
	if err != nil {
		return false, err
	}
	return gotTree == wantTree && bytes.Equal(gotMessage, wantMessage), nil
}

func commitMessage(git *gitrepo.Repository, ctx context.Context, directory, commit string) ([]byte, error) {
	object, err := git.Bytes(ctx, directory, "cat-file", "commit", commit)
	if err != nil {
		return nil, fmt.Errorf("read commit %q: %w", commit, err)
	}
	_, message, ok := bytes.Cut(object, []byte("\n\n"))
	if !ok || len(message) == 0 {
		return nil, fmt.Errorf("commit %q has no message", commit)
	}
	return message, nil
}

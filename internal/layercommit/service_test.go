package layercommit_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/friel-openai/codex/layerctl/internal/definition"
	"github.com/friel-openai/codex/layerctl/internal/gitrepo"
	"github.com/friel-openai/codex/layerctl/internal/layercommit"
)

func TestServiceCaptureAndApplyRoundTripAcceptedEndpoint(t *testing.T) {
	root, before := newRepository(t)
	writeFile(t, filepath.Join(root, "text.txt"), []byte("intermediate\n"), 0o644)
	writeFile(t, filepath.Join(root, "binary.bin"), []byte{0, 1, 2, 255}, 0o644)
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "intermediate edit")

	writeFile(t, filepath.Join(root, "text.txt"), []byte("accepted\n"), 0o644)
	writeFile(t, filepath.Join(root, "script.sh"), []byte("#!/bin/sh\n"), 0o755)
	if err := os.Remove(filepath.Join(root, "deleted.txt")); err != nil {
		t.Fatalf("Remove(deleted.txt) error = %v", err)
	}
	if err := os.Remove(filepath.Join(root, "replace")); err != nil {
		t.Fatalf("Remove(replace) error = %v", err)
	}
	writeFile(t, filepath.Join(root, "replace", "child.txt"), []byte("directory\n"), 0o644)
	if err := os.Symlink("text.txt", filepath.Join(root, "link")); err != nil {
		t.Fatalf("Symlink(link) error = %v", err)
	}
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "Capture endpoint", "-m", "Preserve the complete accepted tree.\n\n---\n\nIncluding separators.")
	after := gitOutput(t, root, "rev-parse", "HEAD")

	git, err := gitrepo.Discover(t.Context(), root)
	if err != nil {
		t.Fatalf("gitrepo.Discover() error = %v", err)
	}
	service := &layercommit.Service{Git: git}
	captured, err := service.Capture(t.Context(), layercommit.CaptureRequest{Before: before, After: after})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if !bytes.Contains(captured.Patch, []byte("GIT binary patch")) {
		t.Fatal("Capture() patch does not contain the binary delta")
	}
	unit := writeLayer(t, t.TempDir(), "0001-feature", captured)
	matches, err := service.Matches(t.Context(), unit, before, after)
	if err != nil {
		t.Fatalf("Matches() error = %v", err)
	}
	if !matches {
		t.Fatal("Matches() = false, want true")
	}
}

func TestServiceCaptureAndApplyMessageOnlyCommit(t *testing.T) {
	root, before := newRepository(t)
	gitRun(t, root, "commit", "--allow-empty", "-m", "Message only")
	after := gitOutput(t, root, "rev-parse", "HEAD")
	git, err := gitrepo.Discover(t.Context(), root)
	if err != nil {
		t.Fatalf("gitrepo.Discover() error = %v", err)
	}
	service := &layercommit.Service{Git: git}
	captured, err := service.Capture(t.Context(), layercommit.CaptureRequest{Before: before, After: after})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if len(captured.Patch) != 0 {
		t.Fatalf("Capture().Patch = %q, want empty", captured.Patch)
	}
	unit := writeLayer(t, t.TempDir(), "0001-message", captured)
	if unit.PatchPath != "" {
		t.Fatalf("unit.PatchPath = %q, want empty", unit.PatchPath)
	}
	matches, err := service.Matches(t.Context(), unit, before, after)
	if err != nil || !matches {
		t.Fatalf("Matches() = %v, %v; want true, nil", matches, err)
	}
}

func TestServiceApplyLeavesConflictsForResolution(t *testing.T) {
	root, before := newRepository(t)
	writeFile(t, filepath.Join(root, "text.txt"), []byte("layer\n"), 0o644)
	gitRun(t, root, "add", "text.txt")
	gitRun(t, root, "commit", "-m", "Layer change")
	after := gitOutput(t, root, "rev-parse", "HEAD")
	git, err := gitrepo.Discover(t.Context(), root)
	if err != nil {
		t.Fatalf("gitrepo.Discover() error = %v", err)
	}
	service := &layercommit.Service{Git: git}
	captured, err := service.Capture(t.Context(), layercommit.CaptureRequest{Before: before, After: after})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	unit := writeLayer(t, t.TempDir(), "0001-feature", captured)

	gitRun(t, root, "switch", "--detach", before)
	writeFile(t, filepath.Join(root, "text.txt"), []byte("upstream\n"), 0o644)
	gitRun(t, root, "add", "text.txt")
	gitRun(t, root, "commit", "-m", "Upstream change")
	if err := service.Apply(t.Context(), root, unit); err == nil {
		t.Fatal("Apply() error = nil, want conflict")
	}
	hasConflicts, err := service.HasConflicts(t.Context(), root)
	if err != nil {
		t.Fatalf("HasConflicts() error = %v", err)
	}
	if !hasConflicts {
		t.Fatal("HasConflicts() = false, want true")
	}
	writeFile(t, filepath.Join(root, "text.txt"), []byte("resolved\n"), 0o644)
	gitRun(t, root, "add", "-A")
	if err := service.Commit(t.Context(), root, unit); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if got := gitOutput(t, root, "show", "-s", "--format=%B", "HEAD"); got != "Layer change" {
		t.Fatalf("resolved message = %q, want %q", got, "Layer change")
	}
}

func newRepository(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	gitRun(t, root, "init", "--initial-branch=main")
	gitRun(t, root, "config", "user.name", "Layerctl Test")
	gitRun(t, root, "config", "user.email", "layerctl@example.com")
	writeFile(t, filepath.Join(root, "text.txt"), []byte("base\n"), 0o644)
	writeFile(t, filepath.Join(root, "deleted.txt"), []byte("delete\n"), 0o644)
	writeFile(t, filepath.Join(root, "replace"), []byte("file\n"), 0o644)
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "base")
	return root, gitOutput(t, root, "rev-parse", "HEAD")
}

func writeLayer(t *testing.T, root, id string, captured layercommit.Captured) definition.Unit {
	t.Helper()
	directory := filepath.Join(root, id)
	messagePath := filepath.Join(directory, "COMMIT_EDITMSG")
	writeFile(t, messagePath, captured.Message, 0o644)
	unit := definition.Unit{ID: id, Directory: directory, MessagePath: messagePath}
	if len(captured.Patch) > 0 {
		unit.PatchPath = filepath.Join(directory, "patch")
		writeFile(t, unit.PatchPath, captured.Patch, 0o644)
	}
	return unit
}

func writeFile(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func gitRun(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.CommandContext(t.Context(), "git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v error = %v\n%s", args, err, output)
	}
}

func gitOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.CommandContext(t.Context(), "git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v error = %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

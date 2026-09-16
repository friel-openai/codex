package upstream_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/friel-openai/codex/layerctl/internal/definition"
	"github.com/friel-openai/codex/layerctl/internal/gitrepo"
	"github.com/friel-openai/codex/layerctl/internal/layer"
	"github.com/friel-openai/codex/layerctl/internal/layercommit"
	"github.com/friel-openai/codex/layerctl/internal/projection"
	"github.com/friel-openai/codex/layerctl/internal/upstream"
)

func TestAdvanceConflictRefreshContinueAndAbort(t *testing.T) {
	root := newCanonicalRepository(t)
	upstreamService, _, projections := newServices(t, root)
	cleanWorktree := filepath.Join(t.TempDir(), "clean")

	path, err := upstreamService.Advance(t.Context(), upstream.AdvanceRequest{
		Tag:          "rust-v1.1.0",
		WorktreePath: cleanWorktree,
	})
	if err != nil {
		t.Fatalf("Advance(clean) error = %v", err)
	}
	if path != cleanWorktree {
		t.Fatalf("Advance(clean) path = %q, want %q", path, cleanWorktree)
	}
	assertUpstreamTag(t, root, "rust-v1.1.0")
	assertFile(t, filepath.Join(cleanWorktree, "base.txt"), "feature\n")
	if err := projections.Delete(t.Context(), "upstream-advance"); err != nil {
		t.Fatalf("Delete(clean projection) error = %v", err)
	}

	upstreamService, _, _ = newServices(t, root)
	abortWorktree := filepath.Join(t.TempDir(), "abort")
	if _, err := upstreamService.Advance(t.Context(), upstream.AdvanceRequest{
		Tag:          "rust-v1.2.0",
		WorktreePath: abortWorktree,
	}); err == nil || !strings.Contains(err.Error(), `unit "0001-feature" requires resolution`) {
		t.Fatalf("Advance(conflict) error = %v", err)
	}
	if err := upstreamService.Abort(t.Context()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if _, err := os.Stat(abortWorktree); !os.IsNotExist(err) {
		t.Fatalf("aborted worktree Stat() error = %v, want not exist", err)
	}

	upstreamService, _, _ = newServices(t, root)
	resolveWorktree := filepath.Join(t.TempDir(), "resolve")
	if _, err := upstreamService.Advance(t.Context(), upstream.AdvanceRequest{
		Tag:          "rust-v1.2.0",
		WorktreePath: resolveWorktree,
	}); err == nil {
		t.Fatal("Advance(conflict retry) error = nil")
	}
	writeFile(t, filepath.Join(resolveWorktree, "base.txt"), "resolved feature\n")
	path, err = upstreamService.Continue(t.Context())
	if err == nil || !strings.Contains(err.Error(), "layerctl layer refresh 0001-feature") {
		t.Fatalf("Continue(resolved) path = %q, error = %v", path, err)
	}

	_, layerService, _ := newServices(t, root)
	if err := layerService.Refresh(t.Context(), "0001-feature", "upstream-advance"); err != nil {
		t.Fatalf("Refresh(feature) error = %v", err)
	}
	upstreamService, _, projections = newServices(t, root)
	if _, err := upstreamService.Continue(t.Context()); err != nil {
		t.Fatalf("Continue(refreshed) error = %v", err)
	}
	assertUpstreamTag(t, root, "rust-v1.2.0")
	assertFile(t, filepath.Join(resolveWorktree, "base.txt"), "resolved feature\n")
	if err := upstreamService.Check(t.Context()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if err := projections.Delete(t.Context(), "upstream-advance"); err != nil {
		t.Fatalf("Delete(resolved projection) error = %v", err)
	}

	if _, err := upstreamService.Advance(t.Context(), upstream.AdvanceRequest{Tag: "main"}); err == nil {
		t.Fatal("Advance(invalid tag) error = nil")
	}
}

func TestContinueRecoversAfterResolvedLayerWasCommitted(t *testing.T) {
	root := newCanonicalRepository(t)
	upstreamService, _, projections := newServices(t, root)
	resolveWorktree := filepath.Join(t.TempDir(), "resolve")
	if _, err := upstreamService.Advance(t.Context(), upstream.AdvanceRequest{
		Tag:          "rust-v1.2.0",
		WorktreePath: resolveWorktree,
	}); err == nil || !strings.Contains(err.Error(), `unit "0001-feature" requires resolution`) {
		t.Fatalf("Advance(conflict) error = %v", err)
	}

	writeFile(t, filepath.Join(resolveWorktree, "base.txt"), "resolved after interruption\n")
	gitRun(t, resolveWorktree, "add", "-A")
	gitRun(t, resolveWorktree, "commit", "--no-verify", "--no-gpg-sign", "--cleanup=verbatim", "--file", filepath.Join(root, "layers", "0001-feature", "COMMIT_EDITMSG"))

	path, err := upstreamService.Continue(t.Context())
	if err == nil || !strings.Contains(err.Error(), "layerctl layer refresh 0001-feature") {
		t.Fatalf("Continue(recover committed resolution) path = %q, error = %v", path, err)
	}
	_, layerService, _ := newServices(t, root)
	if err := layerService.Refresh(t.Context(), "0001-feature", "upstream-advance"); err != nil {
		t.Fatalf("Refresh(feature) error = %v", err)
	}
	upstreamService, _, projections = newServices(t, root)
	if _, err := upstreamService.Continue(t.Context()); err != nil {
		t.Fatalf("Continue(refreshed) error = %v", err)
	}
	assertUpstreamTag(t, root, "rust-v1.2.0")
	assertFile(t, filepath.Join(resolveWorktree, "base.txt"), "resolved after interruption\n")
	if err := projections.Delete(t.Context(), "upstream-advance"); err != nil {
		t.Fatalf("Delete(resolved projection) error = %v", err)
	}
}

func newCanonicalRepository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "repository")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", root, err)
	}
	gitRun(t, root, "init", "--initial-branch=upstream")
	gitRun(t, root, "config", "user.name", "Layerctl Test")
	gitRun(t, root, "config", "user.email", "layerctl@example.com")
	writeFile(t, filepath.Join(root, "base.txt"), "base\n")
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "upstream 1.0")
	v1Commit := gitOutput(t, root, "rev-parse", "HEAD")
	gitRun(t, root, "tag", "rust-v1.0.0")
	writeFile(t, filepath.Join(root, "upstream.txt"), "compatible\n")
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "upstream 1.1")
	gitRun(t, root, "tag", "rust-v1.1.0")
	writeFile(t, filepath.Join(root, "base.txt"), "upstream conflict\n")
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "upstream 1.2")
	gitRun(t, root, "tag", "rust-v1.2.0")

	gitRun(t, root, "switch", "--detach", v1Commit)
	writeFile(t, filepath.Join(root, "foundation.txt"), "foundation\n")
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "frodex: foundation")
	foundationLayer := captureLayer(t, root, "HEAD^", "HEAD")
	writeFile(t, filepath.Join(root, "base.txt"), "feature\n")
	writeFile(t, filepath.Join(root, "feature.txt"), "feature\n")
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "frodex: feature")
	featureLayer := captureLayer(t, root, "HEAD^", "HEAD")
	gitRun(t, root, "switch", "--orphan", "frodex-next")

	writeFile(t, filepath.Join(root, "upstream.json"), fmt.Sprintf("{\n  \"tag\": \"rust-v1.0.0\",\n  \"commit\": %q\n}\n", v1Commit))
	writeLayerDefinition(t, root, "0000-foundation", foundationLayer)
	writeLayerDefinition(t, root, "0001-feature", featureLayer)
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-m", "canonical")
	return root
}

func newServices(t *testing.T, root string) (*upstream.Service, *layer.Service, *projection.Service) {
	t.Helper()
	canonical, err := definition.Load(root)
	if err != nil {
		t.Fatalf("definition.Load() error = %v", err)
	}
	git, err := gitrepo.Discover(t.Context(), root)
	if err != nil {
		t.Fatalf("gitrepo.Discover() error = %v", err)
	}
	projections := &projection.Service{
		Definition: canonical,
		Git:        git,
		Log:        log.New(&bytes.Buffer{}, "", 0),
	}
	return &upstream.Service{Definition: canonical, Git: git, Projection: projections},
		&layer.Service{Definition: canonical, Git: git, Projection: projections},
		projections
}

func assertUpstreamTag(t *testing.T, root, want string) {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(root, "upstream.json"))
	if err != nil {
		t.Fatalf("ReadFile(upstream.json) error = %v", err)
	}
	var got definition.Upstream
	if err := json.Unmarshal(content, &got); err != nil {
		t.Fatalf("Unmarshal(upstream.json) error = %v", err)
	}
	if got.Tag != want {
		t.Fatalf("upstream tag = %q, want %q", got.Tag, want)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if got := string(content); got != want {
		t.Fatalf("ReadFile(%q) = %q, want %q", path, got, want)
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

func captureLayer(t *testing.T, directory, before, after string) layercommit.Captured {
	t.Helper()
	git, err := gitrepo.Discover(t.Context(), directory)
	if err != nil {
		t.Fatalf("gitrepo.Discover() error = %v", err)
	}
	layers := &layercommit.Service{Git: git}
	captured, err := layers.Capture(t.Context(), layercommit.CaptureRequest{
		Before: before,
		After:  after,
	})
	if err != nil {
		t.Fatalf("Capture(%s, %s) error = %v", before, after, err)
	}
	return captured
}

func writeLayerDefinition(t *testing.T, root, id string, captured layercommit.Captured) {
	t.Helper()
	directory := filepath.Join(root, "layers", id)
	writeFile(t, filepath.Join(directory, "COMMIT_EDITMSG"), string(captured.Message))
	if len(captured.Patch) > 0 {
		writeFile(t, filepath.Join(directory, "patch"), string(captured.Patch))
	}
}

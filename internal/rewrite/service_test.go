package rewrite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/friel-openai/codex/layerctl/internal/definition"
	"github.com/friel-openai/codex/layerctl/internal/gitrepo"
	"github.com/friel-openai/codex/layerctl/internal/layercommit"
)

func TestMovePartialTextHunks(t *testing.T) {
	for _, direction := range []string{"forward", "backward"} {
		t.Run(direction, func(t *testing.T) {
			from, to := "0001-source", "0003-target"
			layers := []testLayer{
				{id: definition.FoundationLayerID, message: "Foundation\n"},
				{id: from, message: "Source\n\nKeep exact whitespace.  \n", edit: editTextHunks},
				{id: "0002-independent", message: "Independent\n", edit: func(root string) {
					writeTestFile(t, filepath.Join(root, "independent.txt"), []byte("independent\n"), 0o644)
				}},
				{id: to, message: "Target\n"},
			}
			if direction == "backward" {
				from, to = "0003-source", "0001-target"
				layers[1], layers[3] = testLayer{id: to, message: "Target\n"}, testLayer{id: from, message: "Source\n\nKeep exact whitespace.  \n", edit: editTextHunks}
			}
			fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, layers)
			hunks, err := fixture.service.Hunks(from)
			if err != nil || len(hunks) != 3 {
				t.Fatalf("Hunks(%q) = %d hunks, %v; want 3, nil", from, len(hunks), err)
			}
			// Every selected hunk names the complete original postimage in index.
			// Attribution checks detect accidentally importing that whole postimage.
			selected := []string{hunks[2].ID, hunks[0].ID}
			result, err := fixture.service.Move(t.Context(), MoveRequest{From: from, To: to, Hunks: selected})
			if err != nil {
				t.Fatalf("Move(%s) error = %v", direction, err)
			}
			fixture.assertResult(t, result, []string{layers[0].id, layers[1].id, layers[2].id, layers[3].id})
			if result.Move == nil || result.Move.From != from || result.Move.To != to || !slices.Equal(result.Move.Hunks, []string{hunks[0].ID, hunks[2].ID}) {
				t.Fatalf("Move report = %+v, want source, target, and canonical selector order", result.Move)
			}
			commits := fixture.project(t)
			first := from
			want := bytes.ReplaceAll(textLines(), []byte("line 30\n"), []byte("changed 30\n"))
			if direction == "backward" {
				first = to
				want = bytes.ReplaceAll(textLines(), []byte("line 05\n"), []byte("changed 05\n"))
				want = bytes.ReplaceAll(want, []byte("line 55\n"), []byte("changed 55\n"))
			}
			if got := gitTestBytes(t, fixture.gitRoot, "show", commits[first]+":text.txt"); !bytes.Equal(got, want) {
				t.Fatalf("text after %q = %q, want %q", first, got, want)
			}
			fixture.assertMessages(t)
			fixture.assertClean(t)
		})
	}
}

func TestMovePartialTextFromFreshUpstreamRepository(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-source", message: "Source\n", edit: editTextHunks},
		{id: "0002-target", message: "Target\n"},
	})
	originalHead := gitTestOutput(t, fixture.gitRoot, "rev-parse", "HEAD")
	fresh := t.TempDir()
	gitTestRun(t, fresh, "init", "--initial-branch=main")
	gitTestRun(t, fresh, "config", "user.name", "Layerctl Test")
	gitTestRun(t, fresh, "config", "user.email", "layerctl@example.com")
	gitTestRun(t, fresh, "fetch", "--no-tags", fixture.gitRoot, "+refs/tags/rust-v-test:refs/tags/rust-v-test")
	gitTestRun(t, fresh, "switch", "--detach", "refs/tags/rust-v-test")
	command := exec.CommandContext(t.Context(), "git", "cat-file", "-e", originalHead+"^{commit}")
	command.Dir = fresh
	if err := command.Run(); err == nil {
		t.Fatal("fresh repository unexpectedly contains the original projection commit")
	}
	git, err := gitrepo.Discover(t.Context(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	fixture.gitRoot, fixture.service.Git = fresh, git
	fixture.refs = gitTestOutput(t, fresh, "show-ref")
	hunks, err := fixture.service.Hunks("0001-source")
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.service.Move(t.Context(), MoveRequest{From: "0001-source", To: "0002-target", Hunks: []string{hunks[1].ID}})
	if err != nil {
		t.Fatalf("Move() without projection history error = %v", err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-source", "0002-target"})
	commits := fixture.project(t)
	want := bytes.ReplaceAll(textLines(), []byte("line 05\n"), []byte("changed 05\n"))
	want = bytes.ReplaceAll(want, []byte("line 55\n"), []byte("changed 55\n"))
	if got := gitTestBytes(t, fresh, "show", commits["0001-source"]+":text.txt"); !bytes.Equal(got, want) {
		t.Fatalf("source text = %q, want unselected first and third hunks only", got)
	}
	fixture.assertClean(t)
}

func TestMoveQuotedPathAndNoFinalNewline(t *testing.T) {
	name := "quoted\t\"path.txt"
	fixture := newRewriteFixture(t, func(root string) {
		writeTestFile(t, filepath.Join(root, name), bytes.TrimSuffix(textLines(), []byte{'\n'}), 0o644)
	}, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-source", message: "Source\n", edit: func(root string) {
			data := bytes.ReplaceAll(textLines(), []byte("line 05\n"), []byte("changed 05\n"))
			data = bytes.ReplaceAll(data, []byte("line 60\n"), []byte("changed 60\n"))
			writeTestFile(t, filepath.Join(root, name), bytes.TrimSuffix(data, []byte{'\n'}), 0o644)
		}},
		{id: "0002-target", message: "Target\n"},
	})
	hunks, err := fixture.service.Hunks("0001-source")
	if err != nil || len(hunks) != 2 {
		t.Fatalf("Hunks() = %d hunks, %v; want two quoted-path hunks", len(hunks), err)
	}
	result, err := fixture.service.Move(t.Context(), MoveRequest{From: "0001-source", To: "0002-target", Hunks: []string{hunks[1].ID}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-source", "0002-target"})
	commits := fixture.project(t)
	want := bytes.TrimSuffix(bytes.ReplaceAll(textLines(), []byte("line 05\n"), []byte("changed 05\n")), []byte{'\n'})
	if got := gitTestBytes(t, fixture.gitRoot, "show", commits["0001-source"]+":"+name); !bytes.Equal(got, want) {
		t.Fatalf("quoted-path source text = %q, want %q", got, want)
	}
	fixture.assertClean(t)
}

func TestMovePartialTextWithShiftedLinesAndSameFileTarget(t *testing.T) {
	for _, direction := range []string{"forward", "backward"} {
		t.Run(direction, func(t *testing.T) {
			from, to := "0001-source", "0002-target"
			source := testLayer{id: from, message: "Source\n", edit: func(root string) {
				path := filepath.Join(root, "text.txt")
				data := bytes.ReplaceAll(readTestFile(t, path), []byte("line 05\n"), []byte("changed 05\ninserted one\ninserted two\n"))
				data = bytes.ReplaceAll(data, []byte("line 55\n"), []byte("changed 55\n"))
				writeTestFile(t, path, data, 0o644)
			}}
			target := testLayer{id: to, message: "Target\n", edit: func(root string) {
				path := filepath.Join(root, "text.txt")
				writeTestFile(t, path, bytes.ReplaceAll(readTestFile(t, path), []byte("line 30\n"), []byte("target 30\n")), 0o644)
			}}
			layers := []testLayer{{id: definition.FoundationLayerID, message: "Foundation\n"}, source, target}
			if direction == "backward" {
				from, to = "0002-source", "0001-target"
				source.id, target.id = from, to
				layers[1], layers[2] = target, source
			}
			fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, layers)
			hunks, err := fixture.service.Hunks(from)
			if err != nil || len(hunks) != 2 {
				t.Fatalf("Hunks() = %d hunks, %v; want two line-shifting hunks", len(hunks), err)
			}
			result, err := fixture.service.Move(t.Context(), MoveRequest{From: from, To: to, Hunks: []string{hunks[1].ID}})
			if err != nil {
				t.Fatalf("Move(%s) error = %v", direction, err)
			}
			fixture.assertResult(t, result, []string{layers[0].id, layers[1].id, layers[2].id})
			commits := fixture.project(t)
			first := from
			want := bytes.ReplaceAll(textLines(), []byte("line 05\n"), []byte("changed 05\ninserted one\ninserted two\n"))
			if direction == "backward" {
				first = to
				want = bytes.ReplaceAll(textLines(), []byte("line 30\n"), []byte("target 30\n"))
				want = bytes.ReplaceAll(want, []byte("line 55\n"), []byte("changed 55\n"))
			}
			if got := gitTestBytes(t, fixture.gitRoot, "show", commits[first]+":text.txt"); !bytes.Equal(got, want) {
				t.Fatalf("text after %q = %q, want %q", first, got, want)
			}
			fixture.assertClean(t)
		})
	}
}

func TestMoveAtomicFileChanges(t *testing.T) {
	cases := []struct {
		name string
		base []byte
		mode os.FileMode
		edit func(*testing.T, string)
	}{
		{name: "binary", base: []byte{0, 1, 2, 255}, mode: 0o644, edit: func(t *testing.T, path string) { writeTestFile(t, path, []byte{0, 9, 8, 7, 255}, 0o644) }},
		{name: "new", edit: func(t *testing.T, path string) { writeTestFile(t, path, textLines(), 0o644) }},
		{name: "delete", base: textLines(), mode: 0o644, edit: func(t *testing.T, path string) { removeTestPath(t, path) }},
		{name: "mode-and-text", base: textLines(), mode: 0o644, edit: func(t *testing.T, path string) {
			writeTestFile(t, path, []byte("executable replacement\n"), 0o644)
			if err := os.Chmod(path, 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "mode-only", base: []byte("#!/bin/sh\n"), mode: 0o644, edit: func(t *testing.T, path string) {
			if err := os.Chmod(path, 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", edit: func(t *testing.T, path string) {
			if err := os.Symlink("remaining.txt", path); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range cases {
		for _, direction := range []string{"forward", "backward"} {
			t.Run(test.name+"/"+direction, func(t *testing.T) {
				from, to := "0001-source", "0002-target"
				source := testLayer{id: from, message: "Source\n", edit: func(root string) {
					test.edit(t, filepath.Join(root, "atomic"))
					writeTestFile(t, filepath.Join(root, "remaining.txt"), []byte("remaining\n"), 0o644)
				}}
				target := testLayer{id: to, message: "Target\n"}
				layers := []testLayer{{id: definition.FoundationLayerID, message: "Foundation\n"}, source, target}
				if direction == "backward" {
					from, to = "0002-source", "0001-target"
					source.id, target.id = from, to
					layers[1], layers[2] = target, source
				}
				fixture := newRewriteFixture(t, func(root string) {
					if test.base != nil {
						writeTestFile(t, filepath.Join(root, "atomic"), test.base, test.mode)
					}
				}, layers)
				hunks, err := fixture.service.Hunks(from)
				if err != nil || len(hunks) != 2 {
					t.Fatalf("Hunks() = %d hunks, %v; want two atomic files", len(hunks), err)
				}
				var atomic Hunk
				for _, hunk := range hunks {
					if bytes.HasPrefix(hunk.Patch, []byte("diff --git a/atomic b/atomic\n")) {
						atomic = hunk
					}
				}
				if atomic.ID == "" || !strings.HasPrefix(atomic.Header, "diff --git ") {
					t.Fatalf("atomic file selector = %+v, want one complete file patch", atomic)
				}
				result, err := fixture.service.Move(t.Context(), MoveRequest{From: from, To: to, Hunks: []string{atomic.ID}})
				if err != nil {
					t.Fatalf("Move() error = %v", err)
				}
				fixture.assertResult(t, result, []string{layers[0].id, layers[1].id, layers[2].id})
				if patch := fixture.patch(t, from); bytes.Contains(patch, []byte("diff --git a/atomic b/atomic\n")) {
					t.Fatalf("source retains moved atomic file: %s", patch)
				}
				if patch := fixture.patch(t, to); !bytes.Contains(patch, []byte("diff --git a/atomic b/atomic\n")) || bytes.Contains(patch, []byte("diff --git a/remaining.txt b/remaining.txt\n")) {
					t.Fatalf("target patch must contain only atomic file: %s", patch)
				}
				commits := fixture.project(t)
				wantAtomic := gitTestOutput(t, fixture.gitRoot, "ls-tree", "HEAD", "--", "atomic")
				if got := gitTestOutput(t, fixture.gitRoot, "ls-tree", commits[to], "--", "atomic"); got != wantAtomic {
					t.Fatalf("atomic file after target = %q, want accepted mode and object %q", got, wantAtomic)
				}
				fixture.assertMessages(t)
				fixture.assertClean(t)
			})
		}
	}
}

func TestMoveRejectsDependentAndConflictingChanges(t *testing.T) {
	for _, direction := range []string{"forward", "backward"} {
		t.Run(direction, func(t *testing.T) {
			fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("base\n"), 0o644) }, []testLayer{
				{id: definition.FoundationLayerID, message: "Foundation\n"},
				{id: "0001-first", message: "First\n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("first\n"), 0o644) }},
				{id: "0002-dependent", message: "Dependent\n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("dependent\n"), 0o644) }},
				{id: "0003-last", message: "Last\n"},
			})
			before := canonicalFiles(t, fixture.service.Definition.Root)
			req := MoveRequest{From: "0001-first", To: "0003-last", All: true}
			if direction == "backward" {
				req.From, req.To = "0002-dependent", definition.FoundationLayerID
			}
			if _, err := fixture.service.Move(t.Context(), req); err == nil {
				t.Fatal("Move() error = nil, want dependency or conflict rejection")
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			fixture.assertClean(t)
		})
	}
}

func TestMoveDryRunAndMessageOnlyNoop(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(fmt.Sprintf("dry-run-%t", dryRun), func(t *testing.T) {
			fixture := newRewriteFixture(t, nil, []testLayer{
				{id: definition.FoundationLayerID, message: "Foundation\n"},
				{id: "0001-empty", message: "Empty\n\nExact message.  \n"},
				{id: "0002-target", message: "Target\n"},
			})
			before := canonicalFiles(t, fixture.service.Definition.Root)
			result, err := fixture.service.Move(t.Context(), MoveRequest{From: "0001-empty", To: "0002-target", All: true, DryRun: dryRun})
			if err != nil {
				t.Fatal(err)
			}
			fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-empty", "0002-target"})
			if result.DryRun != dryRun || result.Move == nil || len(result.Move.Hunks) != 0 {
				t.Fatalf("Move result = %+v, want matching dry-run and empty selectors", result)
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			fixture.assertClean(t)
		})
	}
	t.Run("partial-dry-run", func(t *testing.T) {
		fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
			{id: definition.FoundationLayerID, message: "Foundation\n"},
			{id: "0001-source", message: "Source\n", edit: editTextHunks},
			{id: "0002-target", message: "Target\n"},
		})
		before := canonicalFiles(t, fixture.service.Definition.Root)
		hunks, err := fixture.service.Hunks("0001-source")
		if err != nil {
			t.Fatal(err)
		}
		result, err := fixture.service.Move(t.Context(), MoveRequest{From: "0001-source", To: "0002-target", Hunks: []string{hunks[0].ID}, DryRun: true})
		if err != nil {
			t.Fatal(err)
		}
		fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-source", "0002-target"})
		assertCanonicalFiles(t, fixture.service.Definition.Root, before)
		fixture.assertClean(t)
	})
}

func TestMoveRejectsInvalidSelectors(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-source", message: "Source\n", edit: editTextHunks},
		{id: "0002-target", message: "Target\n"},
	})
	hunks, err := fixture.service.Hunks("0001-source")
	if err != nil {
		t.Fatal(err)
	}
	before := canonicalFiles(t, fixture.service.Definition.Root)
	for _, req := range []MoveRequest{
		{From: "0001-source", To: "0001-source", All: true},
		{From: "0001-source", To: "0002-target"},
		{From: "0001-source", To: "0002-target", All: true, Hunks: []string{hunks[0].ID}},
		{From: "0001-source", To: "0002-target", Hunks: []string{hunks[0].ID, hunks[0].ID}},
		{From: "0001-source", To: "0002-target", Hunks: []string{"stale-selector"}},
		{From: "9999-unknown", To: "0002-target", All: true},
		{From: "0001-source", To: "9999-unknown", All: true},
	} {
		if _, err := fixture.service.Move(t.Context(), req); err == nil {
			t.Errorf("Move(%+v) error = nil, want rejection", req)
		}
		assertCanonicalFiles(t, fixture.service.Definition.Root, before)
		fixture.assertClean(t)
	}
	if _, err := fixture.service.Hunks("9999-unknown"); err == nil {
		t.Fatal("Hunks(unknown) error = nil")
	}
}

func TestRegroupNoncontiguousIndependentSources(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(fmt.Sprintf("dry-run-%t", dryRun), func(t *testing.T) {
			fixture := independentRewriteFixture(t)
			before := canonicalFiles(t, fixture.service.Definition.Root)
			groups := []Group{
				{ID: definition.FoundationLayerID, Sources: []string{"0003-three", definition.FoundationLayerID}},
				{ID: "0004-combined", Sources: []string{"0002-two", "0001-one"}},
			}
			result, err := fixture.service.Regroup(t.Context(), RegroupRequest{Groups: groups, DryRun: dryRun})
			if err != nil {
				t.Fatalf("Regroup() error = %v", err)
			}
			fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0004-combined"})
			if !reflect.DeepEqual(result.Groups, groups) || result.DryRun != dryRun || result.Move != nil {
				t.Fatalf("Regroup report = %+v, want original plan and dry-run", result)
			}
			if dryRun {
				assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			} else {
				for _, group := range groups {
					var messages [][]byte
					for _, source := range group.Sources {
						messages = append(messages, fixture.messages[source])
					}
					want := bytes.Join(messages, []byte("\n"))
					if got := readTestFile(t, filepath.Join(fixture.service.Definition.Root, "layers", group.ID, "COMMIT_EDITMSG")); !bytes.Equal(got, want) {
						t.Fatalf("message for %q = %q, want %q", group.ID, got, want)
					}
				}
				commits := fixture.project(t)
				if got := gitTestOutput(t, fixture.gitRoot, "ls-tree", "--name-only", commits[definition.FoundationLayerID]); got != "base.txt\nthree.txt" {
					t.Fatalf("first group files = %q, want base.txt and three.txt", got)
				}
			}
			fixture.assertClean(t)
		})
	}
}

func TestRegroupMessageOverrideAndIdentity(t *testing.T) {
	fixture := independentRewriteFixture(t)
	before := canonicalFiles(t, fixture.service.Definition.Root)
	var identity []Group
	for _, unit := range fixture.service.Definition.Layers {
		identity = append(identity, Group{ID: unit.ID, Sources: []string{unit.ID}})
	}
	if _, err := fixture.service.Regroup(t.Context(), RegroupRequest{Groups: identity}); err != nil {
		t.Fatal(err)
	}
	assertCanonicalFiles(t, fixture.service.Definition.Root, before)
	override := "Combined title\n\nPreserve spaces.  \n\n---\n"
	groups := []Group{{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID, "0001-one", "0002-two", "0003-three"}, Message: override}}
	result, err := fixture.service.Regroup(t.Context(), RegroupRequest{Groups: groups})
	if err != nil {
		t.Fatal(err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID})
	if got := readTestFile(t, fixture.service.Definition.Layers[0].MessagePath); string(got) != override {
		t.Fatalf("override message = %q, want %q", got, override)
	}
	fixture.project(t)
	fixture.assertClean(t)
}

func TestRegroupRejectsInvalidPlans(t *testing.T) {
	fixture := independentRewriteFixture(t)
	before := canonicalFiles(t, fixture.service.Definition.Root)
	valid := []Group{{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID, "0001-one", "0002-two", "0003-three"}}}
	for name, groups := range map[string][]Group{
		"empty":              nil,
		"missing-foundation": {{ID: "0001-other", Sources: valid[0].Sources}},
		"invalid-id":         {{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID}}, {ID: "invalid", Sources: []string{"0001-one", "0002-two", "0003-three"}}},
		"duplicate-id":       {{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID}}, {ID: definition.FoundationLayerID, Sources: []string{"0001-one", "0002-two", "0003-three"}}},
		"unordered":          {{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID}}, {ID: "0009-last", Sources: []string{"0001-one"}}, {ID: "0004-earlier", Sources: []string{"0002-two", "0003-three"}}},
		"empty-sources":      {{ID: definition.FoundationLayerID}},
		"unknown-source":     {{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID, "9999-unknown"}}},
		"duplicate-source":   {{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID, "0001-one", "0001-one", "0002-two", "0003-three"}}},
		"omitted-source":     {{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID, "0001-one", "0002-two"}}},
		"nul-message":        {{ID: definition.FoundationLayerID, Sources: valid[0].Sources, Message: "bad\x00message"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := fixture.service.Regroup(t.Context(), RegroupRequest{Groups: groups}); err == nil {
				t.Fatal("Regroup() error = nil, want invalid-plan rejection")
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			fixture.assertClean(t)
		})
	}
}

func TestRegroupRejectsReorderedDependentSources(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("base\n"), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-first", message: "First\n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("first\n"), 0o644) }},
		{id: "0002-dependent", message: "Dependent\n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("dependent\n"), 0o644) }},
	})
	before := canonicalFiles(t, fixture.service.Definition.Root)
	groups := []Group{{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID, "0002-dependent", "0001-first"}}}
	if _, err := fixture.service.Regroup(t.Context(), RegroupRequest{Groups: groups}); err == nil {
		t.Fatal("Regroup() error = nil, want dependency rejection")
	}
	assertCanonicalFiles(t, fixture.service.Definition.Root, before)
	fixture.assertClean(t)
}

func TestRewriteRejectsStaleDefinitionAndMovedUpstreamTag(t *testing.T) {
	for _, change := range []string{"layers", "upstream", "tag"} {
		t.Run(change, func(t *testing.T) {
			fixture := independentRewriteFixture(t)
			switch change {
			case "layers":
				writeTestFile(t, filepath.Join(fixture.service.Definition.Root, "layers", "0004-new", "COMMIT_EDITMSG"), []byte("New\n"), 0o644)
			case "upstream":
				writeUpstream(t, fixture.service.Definition.Root, definition.Upstream{Tag: "rust-v-other", Commit: fixture.base})
			case "tag":
				gitTestRun(t, fixture.gitRoot, "tag", "--force", fixture.service.Definition.Upstream.Tag, "HEAD")
				fixture.refs = gitTestOutput(t, fixture.gitRoot, "show-ref")
			}
			before := canonicalFiles(t, fixture.service.Definition.Root)
			if change != "tag" {
				if _, err := fixture.service.Hunks("0001-one"); err == nil {
					t.Fatal("Hunks() error = nil, want stale-definition rejection")
				}
			}
			if _, err := fixture.service.Move(t.Context(), MoveRequest{From: "0001-one", To: "0002-two", All: true}); err == nil {
				t.Fatal("Move() error = nil, want changed-input rejection")
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			fixture.assertClean(t)
		})
	}
}

func TestRewriteRejectsSymlinkCanonicalInputs(t *testing.T) {
	for _, name := range []string{"root", "upstream.json", "layers", "message", "patch"} {
		t.Run(name, func(t *testing.T) {
			fixture := independentRewriteFixture(t)
			path := fixture.service.Definition.Root
			switch name {
			case "upstream.json", "layers":
				path = filepath.Join(path, name)
			case "message":
				path = fixture.service.Definition.Layers[1].MessagePath
			case "patch":
				path = fixture.service.Definition.Layers[1].PatchPath
			}
			actual := filepath.Join(t.TempDir(), "actual")
			if err := os.Rename(path, actual); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(actual, path); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.service.Hunks("0001-one"); err == nil {
				t.Fatal("Hunks() error = nil, want symlink rejection")
			}
			if _, err := fixture.service.Move(t.Context(), MoveRequest{From: "0001-one", To: "0002-two", All: true}); err == nil {
				t.Fatal("Move() error = nil, want symlink rejection")
			}
			fixture.assertClean(t)
		})
	}
}

func TestSnapshotDigestRejectsConcurrentCanonicalEdits(t *testing.T) {
	for _, change := range []string{"message", "patch", "mode", "upstream-bytes"} {
		t.Run(change, func(t *testing.T) {
			fixture := independentRewriteFixture(t)
			input, err := fixture.service.freezeInputs()
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(input.root)
			unit := fixture.service.Definition.Layers[1]
			switch change {
			case "message":
				writeTestFile(t, unit.MessagePath, []byte("Concurrent message\n"), 0o644)
			case "patch":
				writeTestFile(t, unit.PatchPath, append(readTestFile(t, unit.PatchPath), '\n'), 0o644)
			case "mode":
				if err := os.Chmod(unit.MessagePath, 0o600); err != nil {
					t.Fatal(err)
				}
			case "upstream-bytes":
				path := filepath.Join(fixture.service.Definition.Root, "upstream.json")
				writeTestFile(t, path, append(readTestFile(t, path), '\n'), 0o644)
			}
			before := canonicalFiles(t, fixture.service.Definition.Root)
			if err := fixture.service.requireUnchanged(input); err == nil {
				t.Fatal("requireUnchanged() error = nil, want changed-byte or mode rejection")
			}
			if err := fixture.service.install(t.Context(), input, input.units); err == nil {
				t.Fatal("install() error = nil, want changed-input rejection")
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			if err := os.RemoveAll(input.root); err != nil {
				t.Fatal(err)
			}
			fixture.assertClean(t)
		})
	}
}

func TestReplayRejectsFinalTreeMismatchAndCleansUp(t *testing.T) {
	fixture := newRewriteFixture(t, nil, []testLayer{{id: definition.FoundationLayerID, message: "Foundation\n"}})
	before := canonicalFiles(t, fixture.service.Definition.Root)
	input, err := fixture.service.freezeInputs()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(input.root)
	_, err = fixture.service.replay(t.Context(), input, false, func(worktree string) ([]frozenUnit, error) {
		predecessor := gitTestOutput(t, worktree, "rev-parse", "HEAD")
		writeTestFile(t, filepath.Join(worktree, "unexpected.txt"), []byte("unexpected\n"), 0o644)
		gitTestRun(t, worktree, "add", "-A")
		unit, err := fixture.service.commitCapture(t.Context(), input.root, worktree, definition.FoundationLayerID, predecessor, []byte("Foundation\n"))
		return []frozenUnit{unit}, err
	}, Result{})
	if err == nil || !strings.Contains(err.Error(), "rewrite changes final tree") {
		t.Fatalf("replay() error = %v, want final-tree mismatch", err)
	}
	assertCanonicalFiles(t, fixture.service.Definition.Root, before)
	if err := os.RemoveAll(input.root); err != nil {
		t.Fatal(err)
	}
	fixture.assertClean(t)
	if err := requireSameTree("same", "same"); err != nil {
		t.Fatal(err)
	}
	if err := requireSameTree("before", "after"); err == nil {
		t.Fatal("requireSameTree() error = nil for different trees")
	}
}

func TestReplayIndependentlyVerifiesSerializedPatches(t *testing.T) {
	fixture := newRewriteFixture(t, nil, []testLayer{{id: definition.FoundationLayerID, message: "Foundation\n"}})
	before := canonicalFiles(t, fixture.service.Definition.Root)
	input, err := fixture.service.freezeInputs()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(input.root)
	_, err = fixture.service.replay(t.Context(), input, false, func(worktree string) ([]frozenUnit, error) {
		predecessor := gitTestOutput(t, worktree, "rev-parse", "HEAD")
		writeTestFile(t, filepath.Join(worktree, "unexpected.txt"), []byte("unexpected\n"), 0o644)
		gitTestRun(t, worktree, "add", "-A")
		unit, captureErr := fixture.service.commitCapture(t.Context(), input.root, worktree, definition.FoundationLayerID, predecessor, []byte("Foundation\n"))
		// Candidate commits reproduce baseline, but the serialized patch does not.
		gitTestRun(t, worktree, "reset", "--hard", predecessor)
		return []frozenUnit{unit}, captureErr
	}, Result{})
	if err == nil || !strings.Contains(err.Error(), "rewrite changes final tree") {
		t.Fatalf("replay() error = %v, want independently replayed patch mismatch", err)
	}
	assertCanonicalFiles(t, fixture.service.Definition.Root, before)
	if err := os.RemoveAll(input.root); err != nil {
		t.Fatal(err)
	}
	fixture.assertClean(t)
}

func TestReplayRejectsConcurrentEditAndCancellationBeforeInstall(t *testing.T) {
	for _, action := range []string{"concurrent-edit", "cancel", "candidate-error"} {
		t.Run(action, func(t *testing.T) {
			fixture := newRewriteFixture(t, nil, []testLayer{{id: definition.FoundationLayerID, message: "Foundation\n"}})
			before := canonicalFiles(t, fixture.service.Definition.Root)
			input, err := fixture.service.freezeInputs()
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(input.root)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			candidateErr := errors.New("candidate failed")
			_, err = fixture.service.replay(ctx, input, false, func(worktree string) ([]frozenUnit, error) {
				if action == "candidate-error" {
					return nil, candidateErr
				}
				predecessor := gitTestOutput(t, worktree, "rev-parse", "HEAD")
				unit, captureErr := fixture.service.commitCapture(t.Context(), input.root, worktree, definition.FoundationLayerID, predecessor, []byte("Foundation\n"))
				if captureErr != nil {
					return nil, captureErr
				}
				if action == "concurrent-edit" {
					writeTestFile(t, fixture.service.Definition.Layers[0].MessagePath, []byte("External edit\n"), 0o644)
					before = canonicalFiles(t, fixture.service.Definition.Root)
				} else {
					cancel()
				}
				return []frozenUnit{unit}, nil
			}, Result{})
			if err == nil {
				t.Fatal("replay() error = nil, want operation rejection")
			}
			if action == "concurrent-edit" && !strings.Contains(err.Error(), "canonical inputs changed") {
				t.Fatalf("concurrent-edit error = %v, want digest rejection", err)
			}
			if action == "candidate-error" && !errors.Is(err, candidateErr) {
				t.Fatalf("candidate-error = %v, want %v", err, candidateErr)
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			if err := os.RemoveAll(input.root); err != nil {
				t.Fatal(err)
			}
			fixture.assertClean(t)
		})
	}
}

func TestRewriteRejectsCanceledContextAndInvalidService(t *testing.T) {
	fixture := independentRewriteFixture(t)
	before := canonicalFiles(t, fixture.service.Definition.Root)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := fixture.service.Move(ctx, MoveRequest{From: "0001-one", To: "0002-two", All: true}); err == nil {
		t.Fatal("Move(canceled) error = nil")
	}
	assertCanonicalFiles(t, fixture.service.Definition.Root, before)
	fixture.assertClean(t)
	for _, service := range []*Service{{}, {Definition: fixture.service.Definition}, {Git: fixture.service.Git}} {
		if _, err := service.Hunks(definition.FoundationLayerID); err == nil {
			t.Fatal("Hunks() error = nil for missing repository")
		}
	}
}

func TestRewriteBaselineFailureCleansUp(t *testing.T) {
	fixture := independentRewriteFixture(t)
	writeTestFile(t, fixture.service.Definition.Layers[1].PatchPath, []byte("not a Git patch\n"), 0o644)
	before := canonicalFiles(t, fixture.service.Definition.Root)
	groups := []Group{{ID: definition.FoundationLayerID, Sources: []string{definition.FoundationLayerID, "0001-one", "0002-two", "0003-three"}}}
	if _, err := fixture.service.Regroup(t.Context(), RegroupRequest{Groups: groups}); err == nil || !strings.Contains(err.Error(), "replay baseline layer") {
		t.Fatalf("Regroup() error = %v, want baseline replay failure", err)
	}
	assertCanonicalFiles(t, fixture.service.Definition.Root, before)
	fixture.assertClean(t)
}

// testLayer defines one accepted commit before Capture serializes its delta.
type testLayer struct {
	id      string
	message string
	edit    func(string)
}

// rewriteFixture owns an isolated upstream repository and canonical definition.
type rewriteFixture struct {
	service   *Service
	gitRoot   string
	base      string
	finalTree string
	tempRoot  string
	refs      string
	messages  map[string][]byte
}

func newRewriteFixture(t *testing.T, setup func(string), layers []testLayer) *rewriteFixture {
	t.Helper()
	tempRoot := t.TempDir()
	t.Setenv("TMPDIR", tempRoot)
	gitRoot := t.TempDir()
	gitTestRun(t, gitRoot, "init", "--initial-branch=main")
	gitTestRun(t, gitRoot, "config", "user.name", "Layerctl Test")
	gitTestRun(t, gitRoot, "config", "user.email", "layerctl@example.com")
	gitTestRun(t, gitRoot, "config", "core.filemode", "true")
	writeTestFile(t, filepath.Join(gitRoot, "base.txt"), []byte("base\n"), 0o644)
	if setup != nil {
		setup(gitRoot)
	}
	gitTestRun(t, gitRoot, "add", "-A")
	gitTestRun(t, gitRoot, "commit", "--no-gpg-sign", "-m", "Upstream")
	base := gitTestOutput(t, gitRoot, "rev-parse", "HEAD")
	gitTestRun(t, gitRoot, "tag", "rust-v-test", base)
	git, err := gitrepo.Discover(t.Context(), gitRoot)
	if err != nil {
		t.Fatal(err)
	}
	canonicalRoot := t.TempDir()
	writeUpstream(t, canonicalRoot, definition.Upstream{Tag: "rust-v-test", Commit: base})
	capture := &layercommit.Service{Git: git}
	messages := make(map[string][]byte, len(layers))
	for _, layer := range layers {
		before := gitTestOutput(t, gitRoot, "rev-parse", "HEAD")
		if layer.edit != nil {
			layer.edit(gitRoot)
		}
		gitTestRun(t, gitRoot, "add", "-A")
		messagePath := filepath.Join(t.TempDir(), "message")
		writeTestFile(t, messagePath, []byte(layer.message), 0o644)
		gitTestRun(t, gitRoot, "commit", "--allow-empty", "--no-verify", "--no-gpg-sign", "--cleanup=verbatim", "--file", messagePath)
		after := gitTestOutput(t, gitRoot, "rev-parse", "HEAD")
		captured, err := capture.Capture(t.Context(), layercommit.CaptureRequest{Before: before, After: after})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writeUnit(filepath.Join(canonicalRoot, "layers"), layer.id, captured.Message, captured.Patch); err != nil {
			t.Fatal(err)
		}
		messages[layer.id] = captured.Message
	}
	canonical, err := definition.Load(canonicalRoot)
	if err != nil {
		t.Fatal(err)
	}
	return &rewriteFixture{service: &Service{Definition: canonical, Git: git}, gitRoot: gitRoot, base: base,
		finalTree: gitTestOutput(t, gitRoot, "rev-parse", "HEAD^{tree}"), tempRoot: tempRoot,
		refs: gitTestOutput(t, gitRoot, "show-ref"), messages: messages}
}

func independentRewriteFixture(t *testing.T) *rewriteFixture {
	t.Helper()
	layers := []testLayer{{id: definition.FoundationLayerID, message: "Foundation\n\nFoundation body.  \n"}}
	for i, name := range []string{"one", "two", "three"} {
		layers = append(layers, testLayer{id: fmt.Sprintf("%04d-%s", i+1, name), message: name + "\n\nBody.  \n", edit: func(root string) {
			writeTestFile(t, filepath.Join(root, name+".txt"), []byte(name+"\n"), 0o644)
		}})
	}
	return newRewriteFixture(t, nil, layers)
}

func (fixture *rewriteFixture) assertResult(t *testing.T, result Result, after []string) {
	t.Helper()
	if result.BeforeTree != fixture.finalTree || result.AfterTree != fixture.finalTree {
		t.Fatalf("result trees = %q, %q; want %q", result.BeforeTree, result.AfterTree, fixture.finalTree)
	}
	if !slices.Equal(result.AfterLayers, after) {
		t.Fatalf("AfterLayers = %v, want %v", result.AfterLayers, after)
	}
	var before []string
	for id := range fixture.messages {
		before = append(before, id)
	}
	slices.Sort(before)
	if !slices.Equal(result.BeforeLayers, before) {
		t.Fatalf("BeforeLayers = %v, want %v", result.BeforeLayers, before)
	}
}

func (fixture *rewriteFixture) assertMessages(t *testing.T) {
	t.Helper()
	for _, unit := range fixture.service.Definition.Layers {
		if got := readTestFile(t, unit.MessagePath); !bytes.Equal(got, fixture.messages[unit.ID]) {
			t.Fatalf("message for %q = %q, want %q", unit.ID, got, fixture.messages[unit.ID])
		}
	}
}

func (fixture *rewriteFixture) assertClean(t *testing.T) {
	t.Helper()
	if got := gitTestOutput(t, fixture.gitRoot, "show-ref"); got != fixture.refs {
		t.Errorf("rewrite changed Git refs: got %q, want %q", got, fixture.refs)
	}
	worktrees := gitTestOutput(t, fixture.gitRoot, "worktree", "list", "--porcelain")
	if strings.Count(worktrees, "worktree ") != 1 || !strings.HasPrefix(worktrees, "worktree "+fixture.gitRoot+"\n") {
		t.Errorf("rewrite leaked a registered worktree: %s", worktrees)
	}
	entries, err := os.ReadDir(fixture.tempRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "layerctl-rewrite-") {
			t.Errorf("rewrite leaked temporary directory %q", filepath.Join(fixture.tempRoot, entry.Name()))
		}
	}
	entries, err = os.ReadDir(fixture.service.Definition.Root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".layerctl-rewrite-") {
			t.Errorf("rewrite leaked install directory %q", entry.Name())
		}
	}
}

func (fixture *rewriteFixture) patch(t *testing.T, id string) []byte {
	t.Helper()
	for _, unit := range fixture.service.Definition.Layers {
		if unit.ID == id {
			if unit.PatchPath == "" {
				return nil
			}
			return readTestFile(t, unit.PatchPath)
		}
	}
	t.Fatalf("missing layer %q", id)
	return nil
}

func (fixture *rewriteFixture) project(t *testing.T) map[string]string {
	t.Helper()
	canonical, err := definition.Load(fixture.service.Definition.Root)
	if err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(t.TempDir(), "verify")
	gitTestRun(t, fixture.gitRoot, "worktree", "add", "--detach", worktree, fixture.base)
	defer gitTestRun(t, fixture.gitRoot, "worktree", "remove", "--force", worktree)
	service := &layercommit.Service{Git: fixture.service.Git}
	commits := make(map[string]string, len(canonical.Layers))
	for _, unit := range canonical.Layers {
		if err := service.Apply(t.Context(), worktree, unit); err != nil {
			t.Fatalf("independent Apply(%q) error = %v", unit.ID, err)
		}
		commits[unit.ID] = gitTestOutput(t, worktree, "rev-parse", "HEAD")
	}
	if got := gitTestOutput(t, worktree, "rev-parse", "HEAD^{tree}"); got != fixture.finalTree {
		t.Fatalf("independent replay final tree = %q, want %q", got, fixture.finalTree)
	}
	return commits
}

func textLines() []byte {
	var lines strings.Builder
	for i := 1; i <= 60; i++ {
		fmt.Fprintf(&lines, "line %02d\n", i)
	}
	return []byte(lines.String())
}

func editTextHunks(root string) {
	text := textLines()
	for _, line := range []int{5, 30, 55} {
		text = bytes.ReplaceAll(text, []byte(fmt.Sprintf("line %02d\n", line)), []byte(fmt.Sprintf("changed %02d\n", line)))
	}
	if err := os.WriteFile(filepath.Join(root, "text.txt"), text, 0o644); err != nil {
		panic(err)
	}
}

func writeUpstream(t *testing.T, root string, upstream definition.Upstream) {
	t.Helper()
	data, err := json.Marshal(upstream)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(root, "upstream.json"), append(data, '\n'), 0o644)
}

// canonicalFile compares contents and modes, including empty layer directories.
type canonicalFile struct {
	mode fs.FileMode
	data string
}

func canonicalFiles(t *testing.T, root string) map[string]canonicalFile {
	t.Helper()
	files := make(map[string]canonicalFile)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		file := canonicalFile{mode: info.Mode()}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			file.data = string(data)
		}
		files[name] = file
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func assertCanonicalFiles(t *testing.T, root string, want map[string]canonicalFile) {
	t.Helper()
	if got := canonicalFiles(t, root); !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical definition changed: got %v, want %v", got, want)
	}
}

func writeTestFile(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func removeTestPath(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

func gitTestRun(t *testing.T, directory string, args ...string) {
	t.Helper()
	gitTestBytes(t, directory, args...)
}

func gitTestOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	return strings.TrimSpace(string(gitTestBytes(t, directory, args...)))
}

func gitTestBytes(t *testing.T, directory string, args ...string) []byte {
	t.Helper()
	command := exec.CommandContext(t.Context(), "git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v error = %v\n%s", args, err, output)
	}
	return output
}

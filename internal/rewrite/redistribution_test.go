package rewrite

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/friel-openai/codex/layerctl/internal/definition"
)

func TestRedistributeOneSourceToMultipleTargets(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-source", message: "Source\n", edit: editTextHunks},
		{id: "0002-first-target", message: "First target\n"},
		{id: "0003-second-target", message: "Second target\n"},
	})
	hunks := requireLayerHunks(t, fixture, "0001-source", 3)
	moves := []MoveRequest{
		{From: "0001-source", To: "0003-second-target", Hunks: []string{hunks[2].ID}},
		{From: "0001-source", To: "0002-first-target", Hunks: []string{hunks[0].ID}},
	}
	result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves})
	if err != nil {
		t.Fatal(err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-source", "0002-first-target", "0003-second-target"})
	assertRedistributionMoves(t, result, []Move{
		{From: moves[0].From, To: moves[0].To, Hunks: []string{hunks[2].ID}},
		{From: moves[1].From, To: moves[1].To, Hunks: []string{hunks[0].ID}},
	})
	commits := fixture.project(t)
	assertProjectedText(t, fixture, commits["0001-source"], changedTextLines(30))
	assertProjectedText(t, fixture, commits["0002-first-target"], changedTextLines(5, 30))
	assertProjectedText(t, fixture, commits["0003-second-target"], changedTextLines(5, 30, 55))
	fixture.assertMessages(t)
	fixture.assertClean(t)
}

func TestRedistributeMultipleSourcesToOneTarget(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-first", message: "First\n", edit: editTestLine(t, 5)},
		{id: "0002-second", message: "Second\n", edit: editTestLine(t, 55)},
		{id: "0003-target", message: "Target\n", edit: editTestLine(t, 30)},
	})
	first := requireLayerHunks(t, fixture, "0001-first", 1)
	second := requireLayerHunks(t, fixture, "0002-second", 1)
	moves := []MoveRequest{
		{From: "0002-second", To: "0003-target", Hunks: []string{second[0].ID}},
		{From: "0001-first", To: "0003-target", All: true},
	}
	result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves})
	if err != nil {
		t.Fatal(err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-first", "0002-second", "0003-target"})
	assertRedistributionMoves(t, result, []Move{
		{From: moves[0].From, To: moves[0].To, Hunks: []string{second[0].ID}},
		{From: moves[1].From, To: moves[1].To, Hunks: []string{first[0].ID}},
	})
	commits := fixture.project(t)
	assertProjectedText(t, fixture, commits["0001-first"], textLines())
	assertProjectedText(t, fixture, commits["0002-second"], textLines())
	assertProjectedText(t, fixture, commits["0003-target"], changedTextLines(5, 30, 55))
	if len(fixture.patch(t, "0001-first")) != 0 || len(fixture.patch(t, "0002-second")) != 0 {
		t.Fatal("source layers retain changes after moving all their original hunks")
	}
	fixture.assertMessages(t)
	fixture.assertClean(t)
}

func TestRedistributeCombinedForwardBackwardAndReceivingSource(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-first", message: "First\n", edit: editTestLine(t, 5)},
		{id: "0002-middle", message: "Middle\n", edit: editTestLine(t, 30)},
		{id: "0003-last", message: "Last\n", edit: editTestLine(t, 55)},
	})
	first := requireLayerHunks(t, fixture, "0001-first", 1)
	middle := requireLayerHunks(t, fixture, "0002-middle", 1)
	last := requireLayerHunks(t, fixture, "0003-last", 1)
	moves := []MoveRequest{
		{From: "0003-last", To: "0002-middle", Hunks: []string{last[0].ID}},
		{From: "0002-middle", To: definition.FoundationLayerID, Hunks: []string{middle[0].ID}},
		{From: "0001-first", To: "0002-middle", Hunks: []string{first[0].ID}},
	}
	result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves})
	if err != nil {
		t.Fatal(err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-first", "0002-middle", "0003-last"})
	assertRedistributionMoves(t, result, []Move{
		{From: moves[0].From, To: moves[0].To, Hunks: []string{last[0].ID}},
		{From: moves[1].From, To: moves[1].To, Hunks: []string{middle[0].ID}},
		{From: moves[2].From, To: moves[2].To, Hunks: []string{first[0].ID}},
	})
	commits := fixture.project(t)
	assertProjectedText(t, fixture, commits[definition.FoundationLayerID], changedTextLines(30))
	assertProjectedText(t, fixture, commits["0001-first"], changedTextLines(30))
	assertProjectedText(t, fixture, commits["0002-middle"], changedTextLines(5, 30, 55))
	assertProjectedText(t, fixture, commits["0003-last"], changedTextLines(5, 30, 55))
	if len(fixture.patch(t, "0001-first")) != 0 || len(fixture.patch(t, "0003-last")) != 0 {
		t.Fatal("source layers retain forwarded or backward-moved hunks")
	}
	fixture.assertMessages(t)
	fixture.assertClean(t)
}

func TestRedistributeAllSelectsOriginalChangesOnReceivingSources(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-first", message: "First\n", edit: editTestLine(t, 5)},
		{id: "0002-middle", message: "Middle\n", edit: editTestLine(t, 30)},
		{id: "0003-last", message: "Last\n", edit: editTestLine(t, 55)},
	})
	first := requireLayerHunks(t, fixture, "0001-first", 1)
	middle := requireLayerHunks(t, fixture, "0002-middle", 1)
	last := requireLayerHunks(t, fixture, "0003-last", 1)
	moves := []MoveRequest{
		{From: "0001-first", To: "0002-middle", Hunks: []string{first[0].ID}},
		{From: "0002-middle", To: "0003-last", All: true},
		{From: "0003-last", To: "0001-first", All: true},
	}
	result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves})
	if err != nil {
		t.Fatal(err)
	}
	assertRedistributionMoves(t, result, []Move{
		{From: moves[0].From, To: moves[0].To, Hunks: []string{first[0].ID}},
		{From: moves[1].From, To: moves[1].To, Hunks: []string{middle[0].ID}},
		{From: moves[2].From, To: moves[2].To, Hunks: []string{last[0].ID}},
	})
	commits := fixture.project(t)
	assertProjectedText(t, fixture, commits["0001-first"], changedTextLines(55))
	assertProjectedText(t, fixture, commits["0002-middle"], changedTextLines(5, 55))
	assertProjectedText(t, fixture, commits["0003-last"], changedTextLines(5, 30, 55))
	if patch := fixture.patch(t, "0002-middle"); bytes.Contains(patch, []byte("+changed 30\n")) || !bytes.Contains(patch, []byte("+changed 05\n")) {
		t.Fatalf("middle must retain incoming line 5 but move original line 30: %s", patch)
	}
	fixture.assertMessages(t)
	fixture.assertClean(t)
}

func TestRedistributeOneSourceSplitForwardAndBackward(t *testing.T) {
	for _, reversed := range []bool{false, true} {
		t.Run(fmt.Sprintf("reversed-%t", reversed), func(t *testing.T) {
			fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
				{id: definition.FoundationLayerID, message: "Foundation\n"},
				{id: "0001-first-target", message: "First target\n"},
				{id: "0002-source", message: "Source\n", edit: editTextHunks},
				{id: "0003-last-target", message: "Last target\n"},
			})
			hunks := requireLayerHunks(t, fixture, "0002-source", 3)
			moves := []MoveRequest{
				{From: "0002-source", To: "0001-first-target", Hunks: []string{hunks[0].ID}},
				{From: "0002-source", To: "0003-last-target", Hunks: []string{hunks[1].ID}},
			}
			if reversed {
				slices.Reverse(moves)
			}
			result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves})
			if err != nil {
				t.Fatal(err)
			}
			fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-first-target", "0002-source", "0003-last-target"})
			commits := fixture.project(t)
			assertProjectedText(t, fixture, commits["0001-first-target"], changedTextLines(5))
			assertProjectedText(t, fixture, commits["0002-source"], changedTextLines(5, 55))
			assertProjectedText(t, fixture, commits["0003-last-target"], changedTextLines(5, 30, 55))
			fixture.assertMessages(t)
			fixture.assertClean(t)
		})
	}
}

func TestRedistributeSingleMoveCompatibility(t *testing.T) {
	for _, entrypoint := range []string{"Move", "Redistribute"} {
		t.Run(entrypoint, func(t *testing.T) {
			fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
				{id: definition.FoundationLayerID, message: "Foundation\n"},
				{id: "0001-source", message: "Source\n", edit: editTextHunks},
				{id: "0002-target", message: "Target\n"},
			})
			hunks := requireLayerHunks(t, fixture, "0001-source", 3)
			req := MoveRequest{From: "0001-source", To: "0002-target", Hunks: []string{hunks[2].ID, hunks[0].ID}, DryRun: true}
			before := canonicalFiles(t, fixture.service.Definition.Root)
			var result Result
			var err error
			if entrypoint == "Move" {
				result, err = fixture.service.Move(t.Context(), req)
			} else {
				req.DryRun = false
				result, err = fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: []MoveRequest{req}, DryRun: true})
			}
			if err != nil {
				t.Fatal(err)
			}
			want := Move{From: req.From, To: req.To, Hunks: []string{hunks[0].ID, hunks[2].ID}}
			assertRedistributionMoves(t, result, []Move{want})
			if result.Move == nil || !reflect.DeepEqual(*result.Move, want) || !result.DryRun {
				t.Fatalf("single move result = %+v, want legacy Move report and dry-run", result)
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			fixture.assertClean(t)
		})
	}
}

func TestRedistributeRejectsDuplicateOriginalHunkOwnership(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-source", message: "Source\n", edit: editTextHunks},
		{id: "0002-first-target", message: "First\n"},
		{id: "0003-second-target", message: "Second\n"},
	})
	hunks := requireLayerHunks(t, fixture, "0001-source", 3)
	before := canonicalFiles(t, fixture.service.Definition.Root)
	for name, moves := range map[string][]MoveRequest{
		"partial-partial": {
			{From: "0001-source", To: "0002-first-target", Hunks: []string{hunks[0].ID}},
			{From: "0001-source", To: "0003-second-target", Hunks: []string{hunks[0].ID}},
		},
		"all-partial": {
			{From: "0001-source", To: "0002-first-target", All: true},
			{From: "0001-source", To: "0003-second-target", Hunks: []string{hunks[2].ID}},
		},
		"partial-all": {
			{From: "0001-source", To: "0002-first-target", Hunks: []string{hunks[1].ID}},
			{From: "0001-source", To: "0003-second-target", All: true},
		},
		"all-all": {
			{From: "0001-source", To: "0002-first-target", All: true},
			{From: "0001-source", To: "0003-second-target", All: true},
		},
		"same-target": {
			{From: "0001-source", To: "0002-first-target", Hunks: []string{hunks[0].ID}},
			{From: "0001-source", To: "0002-first-target", Hunks: []string{hunks[0].ID}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves}); err == nil {
				t.Fatal("Redistribute() error = nil, want duplicate original hunk rejection")
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			fixture.assertClean(t)
		})
	}
}

func TestRedistributeRejectsInvalidMoveWithoutApplyingOtherMoves(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-source", message: "Source\n", edit: editTextHunks},
		{id: "0002-target", message: "Target\n"},
	})
	hunks := requireLayerHunks(t, fixture, "0001-source", 3)
	before := canonicalFiles(t, fixture.service.Definition.Root)
	valid := MoveRequest{From: "0001-source", To: "0002-target", Hunks: []string{hunks[0].ID}}
	for _, invalid := range []MoveRequest{
		{From: "0001-source", To: "0001-source", Hunks: []string{hunks[1].ID}},
		{From: "9999-unknown", To: "0002-target", All: true},
		{From: "0001-source", To: "9999-unknown", Hunks: []string{hunks[1].ID}},
		{From: "0001-source", To: "0002-target", Hunks: []string{"stale-selector"}},
		{From: "0001-source", To: "0002-target"},
		{From: "0001-source", To: "0002-target", All: true, Hunks: []string{hunks[1].ID}},
		{From: "0001-source", To: "0002-target", Hunks: []string{hunks[1].ID}, DryRun: true},
	} {
		if _, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: []MoveRequest{valid, invalid}}); err == nil {
			t.Errorf("Redistribute() with invalid move %+v error = nil", invalid)
		}
		assertCanonicalFiles(t, fixture.service.Definition.Root, before)
		fixture.assertClean(t)
	}
}

func TestRedistributeDependencyConflictPreservesDefinitions(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "dependent.txt"), []byte("base\n"), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-first", message: "First\n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "dependent.txt"), []byte("first\n"), 0o644) }},
		{id: "0002-dependent", message: "Dependent\n", edit: func(root string) {
			writeTestFile(t, filepath.Join(root, "dependent.txt"), []byte("dependent\n"), 0o644)
		}},
		{id: "0003-independent", message: "Independent\n", edit: func(root string) {
			writeTestFile(t, filepath.Join(root, "independent.txt"), []byte("independent\n"), 0o644)
		}},
		{id: "0004-target", message: "Target\n"},
	})
	before := canonicalFiles(t, fixture.service.Definition.Root)
	hunks, err := fixture.service.Hunks("0002-dependent")
	if err != nil || len(hunks) != 1 {
		t.Fatalf("Hunks() = %d, %v; want one dependent hunk", len(hunks), err)
	}
	moves := []MoveRequest{
		{From: "0003-independent", To: "0004-target", All: true},
		{From: "0002-dependent", To: definition.FoundationLayerID, All: true},
	}
	_, err = fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves})
	if err == nil {
		t.Fatal("Redistribute() error = nil, want dependency conflict")
	}
	for _, context := range []string{moves[1].From, moves[1].To, hunks[0].ID, fmt.Sprintf("%q", hunks[0].Header)} {
		if !strings.Contains(err.Error(), context) {
			t.Errorf("conflict error %q omits assignment context %q", err, context)
		}
	}
	assertCanonicalFiles(t, fixture.service.Definition.Root, before)
	fixture.assertClean(t)
}

func TestRedistributeDryRunAndMessageOnlyNoop(t *testing.T) {
	for _, noop := range []bool{false, true} {
		t.Run(fmt.Sprintf("noop-%t", noop), func(t *testing.T) {
			layers := []testLayer{
				{id: definition.FoundationLayerID, message: "Foundation\n"},
				{id: "0001-source", message: "Source\n"},
				{id: "0002-first-target", message: "First\n"},
				{id: "0003-second-target", message: "Second\n"},
			}
			if !noop {
				layers[1].edit = editTextHunks
			}
			fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), textLines(), 0o644) }, layers)
			before := canonicalFiles(t, fixture.service.Definition.Root)
			moves := []MoveRequest{{From: "0001-source", To: "0002-first-target", All: true}, {From: "0003-second-target", To: definition.FoundationLayerID, All: true}}
			result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves, DryRun: !noop})
			if err != nil {
				t.Fatal(err)
			}
			fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-source", "0002-first-target", "0003-second-target"})
			if result.DryRun != !noop {
				t.Fatalf("DryRun = %t, want %t", result.DryRun, !noop)
			}
			if len(result.Moves) != 2 {
				t.Fatalf("Moves = %v, want two original assignments", result.Moves)
			}
			if noop && (len(result.Moves[0].Hunks) != 0 || len(result.Moves[1].Hunks) != 0) {
				t.Fatalf("message-only assignments select hunks: %v", result.Moves)
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			fixture.assertClean(t)
		})
	}
}

func TestRedistributeRejectsBackwardMoveAlreadyOwnedByTarget(t *testing.T) {
	for _, entrypoint := range []string{"Move", "Redistribute"} {
		t.Run(entrypoint, func(t *testing.T) {
			fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("base\n"), 0o644) }, []testLayer{
				{id: definition.FoundationLayerID, message: "Foundation\n"},
				{id: "0001-target", message: "Target\n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("selected\n"), 0o644) }},
				{id: "0002-revert", message: "Revert\n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("base\n"), 0o644) }},
				{id: "0003-source", message: "Source\n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("selected\n"), 0o644) }},
			})
			hunks := requireLayerHunks(t, fixture, "0003-source", 1)
			before := canonicalFiles(t, fixture.service.Definition.Root)
			req := MoveRequest{From: "0003-source", To: "0001-target", Hunks: []string{hunks[0].ID}}
			var err error
			if entrypoint == "Move" {
				_, err = fixture.service.Move(t.Context(), req)
			} else {
				_, err = fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: []MoveRequest{req}})
			}
			if err == nil {
				t.Fatal("backward move error = nil: target already owns the change, but an intervening layer reverts it")
			}
			assertCanonicalFiles(t, fixture.service.Definition.Root, before)
			fixture.assertClean(t)
		})
	}
}

func TestRedistributeAllowsForwardCancellationAtTarget(t *testing.T) {
	fixture := newRewriteFixture(t, func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("base\n"), 0o644) }, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-source", message: "Source\n\nKeep source message.  \n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("selected\n"), 0o644) }},
		{id: "0002-target", message: "Target\n\nKeep target message.  \n", edit: func(root string) { writeTestFile(t, filepath.Join(root, "text.txt"), []byte("base\n"), 0o644) }},
	})
	hunks := requireLayerHunks(t, fixture, "0001-source", 1)
	result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: []MoveRequest{{From: "0001-source", To: "0002-target", Hunks: []string{hunks[0].ID}}}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-source", "0002-target"})
	assertRedistributionMoves(t, result, []Move{{From: "0001-source", To: "0002-target", Hunks: []string{hunks[0].ID}}})
	if len(fixture.patch(t, "0001-source")) != 0 || len(fixture.patch(t, "0002-target")) != 0 {
		t.Fatal("forwarded change and target's original inverse must cancel into empty source and target patches")
	}
	commits := fixture.project(t)
	for _, id := range []string{"0001-source", "0002-target"} {
		if got := gitTestBytes(t, fixture.gitRoot, "show", commits[id]+":text.txt"); !bytes.Equal(got, []byte("base\n")) {
			t.Fatalf("text after %q = %q, want base after cancellation", id, got)
		}
	}
	fixture.assertMessages(t)
	fixture.assertClean(t)
}

func TestMoveBackwardCorrectionOfTargetCreatedFile(t *testing.T) {
	fixture := newRewriteFixture(t, nil, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-target", message: "Create file\n\nKeep target message.  \n", edit: func(root string) {
			writeTestFile(t, filepath.Join(root, "created.txt"), []byte("initial value\n"), 0o644)
		}},
		{id: "0002-source", message: "Correct file\n\nKeep source message.  \n", edit: func(root string) {
			writeTestFile(t, filepath.Join(root, "created.txt"), []byte("corrected value\n"), 0o644)
		}},
	})
	hunks := requireLayerHunks(t, fixture, "0002-source", 1)
	result, err := fixture.service.Move(t.Context(), MoveRequest{From: "0002-source", To: "0001-target", Hunks: []string{hunks[0].ID}})
	if err != nil {
		t.Fatalf("Move() correcting a file created by the target error = %v", err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-target", "0002-source"})
	assertRedistributionMoves(t, result, []Move{{From: "0002-source", To: "0001-target", Hunks: []string{hunks[0].ID}}})
	if len(fixture.patch(t, "0002-source")) != 0 {
		t.Fatal("source retains the backward-moved file correction")
	}
	if _, err := os.Lstat(filepath.Join(fixture.service.Definition.Root, "layers", "0002-source", "patch")); !os.IsNotExist(err) {
		t.Fatalf("source patch file remains after moving the correction: %v", err)
	}
	if patch := fixture.patch(t, "0001-target"); !bytes.Contains(patch, []byte("new file mode 100644\n")) || !bytes.Contains(patch, []byte("+corrected value\n")) {
		t.Fatalf("target must create the corrected file directly: %s", patch)
	}
	commits := fixture.project(t)
	if got := gitTestOutput(t, fixture.gitRoot, "ls-tree", "--name-only", commits[definition.FoundationLayerID]); got != "base.txt" {
		t.Fatalf("upstream and foundation files = %q, want no created.txt", got)
	}
	if got := gitTestBytes(t, fixture.gitRoot, "show", commits["0001-target"]+":created.txt"); !bytes.Equal(got, []byte("corrected value\n")) {
		t.Fatalf("file after target = %q, want corrected value", got)
	}
	fixture.assertMessages(t)
	fixture.assertClean(t)
}

func TestRedistributeBackwardCreatedFileCorrectionAndForwardAdditionToSameTarget(t *testing.T) {
	fixture := newRewriteFixture(t, nil, []testLayer{
		{id: definition.FoundationLayerID, message: "Foundation\n"},
		{id: "0001-forward-source", message: "Forward source\n", edit: func(root string) {
			writeTestFile(t, filepath.Join(root, "forward.txt"), []byte("forward addition\n"), 0o644)
		}},
		{id: "0002-target", message: "Create files\n\nKeep target message.  \n", edit: func(root string) {
			writeTestFile(t, filepath.Join(root, "created.txt"), []byte("initial value\n"), 0o644)
			writeTestFile(t, filepath.Join(root, "target-only.txt"), []byte("target context\n"), 0o644)
		}},
		{id: "0003-backward-source", message: "Correct file\n\nKeep source message.  \n", edit: func(root string) {
			writeTestFile(t, filepath.Join(root, "created.txt"), []byte("corrected value\n"), 0o644)
		}},
	})
	forward := requireLayerHunks(t, fixture, "0001-forward-source", 1)
	backward := requireLayerHunks(t, fixture, "0003-backward-source", 1)
	moves := []MoveRequest{
		{From: "0003-backward-source", To: "0002-target", Hunks: []string{backward[0].ID}},
		{From: "0001-forward-source", To: "0002-target", Hunks: []string{forward[0].ID}},
	}
	result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: moves})
	if err != nil {
		t.Fatal(err)
	}
	fixture.assertResult(t, result, []string{definition.FoundationLayerID, "0001-forward-source", "0002-target", "0003-backward-source"})
	assertRedistributionMoves(t, result, []Move{
		{From: moves[0].From, To: moves[0].To, Hunks: []string{backward[0].ID}},
		{From: moves[1].From, To: moves[1].To, Hunks: []string{forward[0].ID}},
	})
	if len(fixture.patch(t, "0001-forward-source")) != 0 || len(fixture.patch(t, "0003-backward-source")) != 0 {
		t.Fatal("source layers retain changes assigned to the target")
	}
	for _, id := range []string{"0001-forward-source", "0003-backward-source"} {
		if _, err := os.Lstat(filepath.Join(fixture.service.Definition.Root, "layers", id, "patch")); !os.IsNotExist(err) {
			t.Fatalf("source %q patch file remains after redistribution: %v", id, err)
		}
	}
	commits := fixture.project(t)
	if got := gitTestOutput(t, fixture.gitRoot, "ls-tree", "--name-only", commits["0001-forward-source"]); got != "base.txt" {
		t.Fatalf("files after forward source = %q, want base.txt only", got)
	}
	for name, want := range map[string]string{"created.txt": "corrected value\n", "forward.txt": "forward addition\n", "target-only.txt": "target context\n"} {
		if got := gitTestBytes(t, fixture.gitRoot, "show", commits["0002-target"]+":"+name); string(got) != want {
			t.Fatalf("%s after target = %q, want %q", name, got, want)
		}
	}
	fixture.assertMessages(t)
	fixture.assertClean(t)
}

func TestRedistributeAtomicFileTypeChanges(t *testing.T) {
	for _, conversion := range []string{"regular-to-symlink", "symlink-to-regular"} {
		for _, direction := range []string{"forward", "backward"} {
			t.Run(conversion+"/"+direction, func(t *testing.T) {
				from, to := "0001-source", "0002-target"
				source := testLayer{id: from, message: "Source\n", edit: func(root string) {
					path := filepath.Join(root, "atomic")
					removeTestPath(t, path)
					if conversion == "regular-to-symlink" {
						if err := os.Symlink("base.txt", path); err != nil {
							t.Fatal(err)
						}
					} else {
						writeTestFile(t, path, textLines(), 0o644)
					}
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
					path := filepath.Join(root, "atomic")
					if conversion == "regular-to-symlink" {
						writeTestFile(t, path, textLines(), 0o644)
					} else if err := os.Symlink("base.txt", path); err != nil {
						t.Fatal(err)
					}
				}, layers)
				hunks := requireLayerHunks(t, fixture, from, 2)
				var atomic Hunk
				for _, hunk := range hunks {
					if bytes.HasPrefix(hunk.Patch, []byte("diff --git a/atomic b/atomic\n")) {
						atomic = hunk
					}
				}
				if atomic.ID == "" || bytes.Count(atomic.Patch, []byte("diff --git a/atomic b/atomic\n")) != 2 {
					t.Fatalf("file type-change selector = %+v, want deleted and new file sections together", atomic)
				}
				result, err := fixture.service.Redistribute(t.Context(), RedistributionRequest{Moves: []MoveRequest{{From: from, To: to, Hunks: []string{atomic.ID}}}})
				if err != nil {
					t.Fatal(err)
				}
				fixture.assertResult(t, result, []string{layers[0].id, layers[1].id, layers[2].id})
				if patch := fixture.patch(t, from); bytes.Contains(patch, []byte("diff --git a/atomic b/atomic\n")) {
					t.Fatalf("source retains a partial file conversion: %s", patch)
				}
				commits := fixture.project(t)
				want := gitTestOutput(t, fixture.gitRoot, "ls-tree", "HEAD", "--", "atomic")
				if got := gitTestOutput(t, fixture.gitRoot, "ls-tree", commits[to], "--", "atomic"); got != want {
					t.Fatalf("file after target = %q, want accepted file type and object %q", got, want)
				}
				fixture.assertMessages(t)
				fixture.assertClean(t)
			})
		}
	}
}

func requireLayerHunks(t *testing.T, fixture *rewriteFixture, id string, count int) []Hunk {
	t.Helper()
	hunks, err := fixture.service.Hunks(id)
	if err != nil || len(hunks) != count {
		t.Fatalf("Hunks(%q) = %d hunks, %v; want %d, nil", id, len(hunks), err, count)
	}
	return hunks
}

func assertRedistributionMoves(t *testing.T, result Result, want []Move) {
	t.Helper()
	if len(result.Moves) != len(want) {
		t.Fatalf("Moves = %v, want %v", result.Moves, want)
	}
	for i, move := range result.Moves {
		if move.From != want[i].From || move.To != want[i].To || !slices.Equal(move.Hunks, want[i].Hunks) {
			t.Fatalf("Moves[%d] = %+v, want %+v", i, move, want[i])
		}
	}
	if len(want) != 1 && result.Move != nil {
		t.Fatalf("batch result has legacy single Move report: %+v", result.Move)
	}
}

func assertProjectedText(t *testing.T, fixture *rewriteFixture, commit string, want []byte) {
	t.Helper()
	if got := gitTestBytes(t, fixture.gitRoot, "show", commit+":text.txt"); !bytes.Equal(got, want) {
		t.Fatalf("text after commit %s = %q, want %q", commit, got, want)
	}
}

func changedTextLines(lines ...int) []byte {
	text := textLines()
	for _, line := range lines {
		text = bytes.ReplaceAll(text, []byte(fmt.Sprintf("line %02d\n", line)), []byte(fmt.Sprintf("changed %02d\n", line)))
	}
	return text
}

func editTestLine(t *testing.T, line int) func(string) {
	t.Helper()
	return func(root string) {
		path := filepath.Join(root, "text.txt")
		text := bytes.ReplaceAll(readTestFile(t, path), []byte(fmt.Sprintf("line %02d\n", line)), []byte(fmt.Sprintf("changed %02d\n", line)))
		writeTestFile(t, path, text, os.FileMode(0o644))
	}
}

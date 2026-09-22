// Package rewrite redistributes canonical layer changes without changing the
// final projected tree or publishing projection refs.
package rewrite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/friel-openai/codex/layerctl/internal/definition"
	"github.com/friel-openai/codex/layerctl/internal/gitrepo"
	"github.com/friel-openai/codex/layerctl/internal/layercommit"
)

// Service rewrites a complete canonical stack. Definition and Git are required.
type Service struct {
	Definition *definition.Repository
	Git        *gitrepo.Repository
}

// MoveRequest selects changes from one existing layer for another existing
// layer. All and Hunks are mutually exclusive; selectors identify exact bytes.
type MoveRequest struct {
	From   string   `json:"from"`
	To     string   `json:"to"`
	Hunks  []string `json:"hunks,omitempty"`
	All    bool     `json:"all,omitempty"`
	DryRun bool     `json:"-"`
}

// RedistributionRequest applies one ordered set of moves from a single frozen
// original stack. Only this top-level DryRun controls installation.
type RedistributionRequest struct {
	Moves  []MoveRequest `json:"moves"`
	DryRun bool          `json:"-"`
}

// Group assigns original layers, in Sources order, to one regenerated layer.
// Message overrides the concatenated original messages when nonempty.
type Group struct {
	ID      string   `json:"id"`
	Sources []string `json:"sources"`
	Message string   `json:"message,omitempty"`
}

// RegroupRequest must assign every original layer exactly once. Groups must
// already be in lexical order, beginning with 0000-foundation.
type RegroupRequest struct {
	Groups []Group
	DryRun bool
}

// Move records original selectors assigned to a target during verified replay.
// A forward target's inverse change may cancel them in its captured delta.
type Move struct {
	From  string   `json:"from"`
	To    string   `json:"to"`
	Hunks []string `json:"hunks"`
}

// Result reports verified tree identity and the original-to-regenerated layer
// assignment. DryRun results describe the candidate without installing it.
type Result struct {
	BeforeTree   string   `json:"before_tree"`
	AfterTree    string   `json:"after_tree"`
	BeforeLayers []string `json:"before_layers"`
	AfterLayers  []string `json:"after_layers"`
	Groups       []Group  `json:"groups,omitempty"`
	Move         *Move    `json:"move,omitempty"`  // single-move compatibility; nil for multiple moves
	Moves        []Move   `json:"moves,omitempty"` // assignments in declared request order
	DryRun       bool     `json:"dry_run"`
}

// frozenUnit holds immutable message and patch bytes copied before any replay.
type frozenUnit struct {
	unit    definition.Unit // points only into the operation's temporary directory
	message []byte
	patch   []byte
}

// snapshot retains the input digest used to reject concurrent canonical edits.
type snapshot struct {
	root     string // operation-owned directory, never a user projection
	digest   [sha256.Size]byte
	upstream definition.Upstream
	units    []frozenUnit
}

// selectedMove retains original indices and exact selected bytes throughout
// one batch; candidate recaptures never change another move's selectors.
type selectedMove struct {
	from  int    // original lexical source index
	to    int    // original lexical target index
	hunks []Hunk // exact selections from the frozen source patch
}

// Hunks returns stable selectors from one canonical patch. It does not need
// projection objects and rejects a Service loaded before a structural edit.
func (s *Service) Hunks(id string) ([]Hunk, error) {
	input, err := s.readInputs()
	if err != nil {
		return nil, err
	}
	for _, unit := range input.units {
		if unit.unit.ID == id {
			return parseHunks(unit.patch)
		}
	}
	return nil, fmt.Errorf("unknown layer %q", id)
}

// Move delegates a single assignment to Redistribute. Forward selections
// precede To's original delta and can cancel with its inverse. Backward
// corrections follow To's original delta, which may create their files.
// From's captured delta must no longer own the selected changes.
func (s *Service) Move(ctx context.Context, req MoveRequest) (Result, error) {
	dryRun := req.DryRun
	req.DryRun = false
	return s.Redistribute(ctx, RedistributionRequest{Moves: []MoveRequest{req}, DryRun: dryRun})
}

// Redistribute applies incoming forward selections before each original delta
// and backward corrections after it, preserving declared order within each.
// It removes outgoing forward selections before capturing their source.
// Every selector belongs to exactly one original source and one target.
func (s *Service) Redistribute(ctx context.Context, req RedistributionRequest) (Result, error) {
	if len(req.Moves) == 0 {
		return Result{}, errors.New("redistribution requires one or more moves")
	}
	release, err := s.lockCanonicalInputs()
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = release() }()
	input, err := s.freezeInputs()
	if err != nil {
		return Result{}, err
	}
	defer os.RemoveAll(input.root)
	moves, reports, err := prepareMoves(input.units, req.Moves)
	if err != nil {
		return Result{}, err
	}
	report := Result{Moves: reports}
	if len(reports) == 1 {
		report.Move = &reports[0]
	}
	return s.replay(ctx, input, req.DryRun, func(worktree string) ([]frozenUnit, error) {
		result := make([]frozenUnit, 0, len(input.units))
		for i, original := range input.units {
			before, err := s.head(ctx, worktree)
			if err != nil {
				return nil, err
			}
			for _, move := range moves {
				if i == move.to && move.from < move.to {
					if err := s.applySelected(ctx, worktree, move.hunks, false); err != nil {
						return nil, fmt.Errorf("move changes from %q into %q: %w", input.units[move.from].unit.ID, original.unit.ID, err)
					}
				}
			}
			for _, move := range moves {
				if i == move.from && move.from > move.to {
					if err := s.prepareBackwardSelections(ctx, worktree, original.unit.ID, move.hunks); err != nil {
						return nil, err
					}
				}
			}
			if err := s.applyPatch(ctx, worktree, original.patch, false); err != nil {
				return nil, fmt.Errorf("replay layer %q: %w", original.unit.ID, err)
			}
			for _, move := range moves {
				if i == move.to && move.from > move.to {
					if err := s.applySelected(ctx, worktree, move.hunks, false); err != nil {
						return nil, fmt.Errorf("move changes from %q into %q: %w", input.units[move.from].unit.ID, original.unit.ID, err)
					}
				}
			}
			for _, move := range moves {
				if i == move.from && move.from < move.to {
					if err := s.applySelected(ctx, worktree, move.hunks, true); err != nil {
						return nil, fmt.Errorf("remove changes from %q: %w", original.unit.ID, err)
					}
				}
			}
			unit, err := s.commitCapture(ctx, input.root, worktree, original.unit.ID, before, original.message)
			if err != nil {
				return nil, err
			}
			result = append(result, unit)
		}
		return result, nil
	}, report)
}

func prepareMoves(units []frozenUnit, requested []MoveRequest) ([]selectedMove, []Move, error) {
	indices := make(map[string]int, len(units))
	for i, unit := range units {
		indices[unit.unit.ID] = i
	}
	parsed := make(map[int][]Hunk)
	owned := make(map[[2]string]bool)
	moves := make([]selectedMove, 0, len(requested))
	reports := make([]Move, 0, len(requested))
	for _, req := range requested {
		if req.DryRun {
			return nil, nil, errors.New("set dry-run on the redistribution request, not individual moves")
		}
		if req.From == req.To {
			return nil, nil, errors.New("source and target layers must differ")
		}
		if req.All == (len(req.Hunks) > 0) {
			return nil, nil, errors.New("select either all changes or one or more hunk IDs")
		}
		from, knownFrom := indices[req.From]
		to, knownTo := indices[req.To]
		if !knownFrom || !knownTo {
			return nil, nil, fmt.Errorf("unknown source or target layer %q, %q", req.From, req.To)
		}
		hunks, cached := parsed[from]
		if !cached {
			var err error
			hunks, err = parseHunks(units[from].patch)
			if err != nil {
				return nil, nil, fmt.Errorf("parse source layer %q: %w", req.From, err)
			}
			parsed[from] = hunks
		}
		selected, ids, err := selectHunks(hunks, req.Hunks, req.All)
		if err != nil {
			return nil, nil, err
		}
		for _, id := range ids {
			key := [2]string{req.From, id}
			if owned[key] {
				return nil, nil, fmt.Errorf("hunk %q from layer %q assigned to multiple moves", id, req.From)
			}
			owned[key] = true
		}
		moves = append(moves, selectedMove{from: from, to: to, hunks: selected})
		reports = append(reports, Move{From: req.From, To: req.To, Hunks: ids})
	}
	return moves, reports, nil
}

// Regroup applies each group's original deltas in declared order and captures
// one commit per group. Noncontiguous assignments are accepted only when they
// replay cleanly and reproduce the exact baseline final tree.
func (s *Service) Regroup(ctx context.Context, req RegroupRequest) (Result, error) {
	release, err := s.lockCanonicalInputs()
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = release() }()
	input, err := s.freezeInputs()
	if err != nil {
		return Result{}, err
	}
	defer os.RemoveAll(input.root)
	if err := validateGroups(input.units, req.Groups); err != nil {
		return Result{}, err
	}
	byID := make(map[string]frozenUnit, len(input.units))
	for _, unit := range input.units {
		byID[unit.unit.ID] = unit
	}
	return s.replay(ctx, input, req.DryRun, func(worktree string) ([]frozenUnit, error) {
		result := make([]frozenUnit, 0, len(req.Groups))
		for _, group := range req.Groups {
			before, err := s.head(ctx, worktree)
			if err != nil {
				return nil, err
			}
			messages := make([][]byte, 0, len(group.Sources))
			for _, source := range group.Sources {
				unit := byID[source]
				if err := s.applyPatch(ctx, worktree, unit.patch, false); err != nil {
					return nil, fmt.Errorf("replay source %q in group %q: %w", source, group.ID, err)
				}
				messages = append(messages, unit.message)
			}
			message := bytes.Join(messages, []byte("\n"))
			if group.Message != "" {
				message = []byte(group.Message)
			}
			unit, err := s.commitCapture(ctx, input.root, worktree, group.ID, before, message)
			if err != nil {
				return nil, err
			}
			result = append(result, unit)
		}
		return result, nil
	}, Result{Groups: req.Groups})
}

func (s *Service) lockCanonicalInputs() (func() error, error) {
	if s.Definition == nil || s.Git == nil {
		return nil, errors.New("definition and Git repository are required")
	}
	return acquireRewriteLock(s.Definition.Root)
}

func validateGroups(units []frozenUnit, groups []Group) error {
	if len(groups) == 0 || groups[0].ID != definition.FoundationLayerID {
		return fmt.Errorf("first group must be %q", definition.FoundationLayerID)
	}
	remaining := make(map[string]bool, len(units))
	for _, unit := range units {
		remaining[unit.unit.ID] = true
	}
	for i, group := range groups {
		if err := definition.ValidateLayerID(group.ID); err != nil {
			return err
		}
		if i > 0 && groups[i-1].ID >= group.ID {
			return errors.New("group IDs must be unique and in lexical order")
		}
		if len(group.Sources) == 0 {
			return fmt.Errorf("group %q has no sources", group.ID)
		}
		if strings.ContainsRune(group.Message, '\x00') {
			return fmt.Errorf("group %q message contains NUL", group.ID)
		}
		for _, source := range group.Sources {
			available, known := remaining[source]
			if !known {
				return fmt.Errorf("unknown group source %q", source)
			}
			if !available {
				return fmt.Errorf("duplicate group source %q", source)
			}
			remaining[source] = false
		}
	}
	for _, unit := range units {
		if remaining[unit.unit.ID] {
			return fmt.Errorf("group plan omits layer %q", unit.unit.ID)
		}
	}
	return nil
}

func selectHunks(hunks []Hunk, requested []string, all bool) ([]Hunk, []string, error) {
	known := make(map[string]bool, len(hunks))
	for _, hunk := range hunks {
		known[hunk.ID] = true
	}
	wanted := make(map[string]bool, len(requested))
	for _, id := range requested {
		if wanted[id] {
			return nil, nil, fmt.Errorf("duplicate hunk selector %q", id)
		}
		if !known[id] {
			return nil, nil, fmt.Errorf("unknown or stale hunk selector %q", id)
		}
		wanted[id] = true
	}
	selected := make([]Hunk, 0, len(hunks))
	ids := make([]string, 0, len(hunks))
	for _, hunk := range hunks {
		if all || wanted[hunk.ID] {
			selected = append(selected, hunk)
			ids = append(ids, hunk.ID)
		}
	}
	return selected, ids, nil
}

func (s *Service) readInputs() (*snapshot, error) {
	if s.Definition == nil || s.Git == nil {
		return nil, errors.New("definition and Git repository are required")
	}
	for _, path := range []string{s.Definition.Root, filepath.Join(s.Definition.Root, "upstream.json"), filepath.Join(s.Definition.Root, "layers")} {
		if err := rejectSymlink(path); err != nil {
			return nil, err
		}
	}
	current, err := definition.Load(s.Definition.Root)
	if err != nil {
		return nil, err
	}
	if current.Upstream != s.Definition.Upstream || !slices.Equal(layerIDs(current.Layers), layerIDs(s.Definition.Layers)) {
		return nil, errors.New("canonical definition changed; reload it before rewriting")
	}
	input := &snapshot{upstream: current.Upstream}
	digest := sha256.New()
	upstream, err := os.ReadFile(filepath.Join(current.Root, "upstream.json"))
	if err != nil {
		return nil, err
	}
	if err := hashInput(digest, "upstream.json", filepath.Join(current.Root, "upstream.json"), upstream); err != nil {
		return nil, err
	}
	for _, unit := range current.Layers {
		if err := rejectSymlink(unit.Directory); err != nil {
			return nil, err
		}
		if err := rejectSymlink(unit.MessagePath); err != nil {
			return nil, err
		}
		message, err := os.ReadFile(unit.MessagePath)
		if err != nil {
			return nil, err
		}
		if err := hashInput(digest, unit.ID+"/COMMIT_EDITMSG", unit.MessagePath, message); err != nil {
			return nil, err
		}
		var patch []byte
		if unit.PatchPath != "" {
			if err := rejectSymlink(unit.PatchPath); err != nil {
				return nil, err
			}
			patch, err = os.ReadFile(unit.PatchPath)
			if err != nil {
				return nil, err
			}
			if err := hashInput(digest, unit.ID+"/patch", unit.PatchPath, patch); err != nil {
				return nil, err
			}
		}
		input.units = append(input.units, frozenUnit{unit: unit, message: message, patch: patch})
	}
	copy(input.digest[:], digest.Sum(nil))
	return input, nil
}

func rejectSymlink(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect canonical input %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("canonical input %q must not be a symlink", path)
	}
	return nil
}

func hashInput(digest hash.Hash, name, path string, data []byte) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("canonical input %q must be a regular file", path)
	}
	for _, value := range [][]byte{[]byte(name), []byte(info.Mode().String()), data} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = digest.Write(size[:])
		_, _ = digest.Write(value)
	}
	return nil
}

func (s *Service) freezeInputs() (*snapshot, error) {
	input, err := s.readInputs()
	if err != nil {
		return nil, err
	}
	input.root, err = os.MkdirTemp("", "layerctl-rewrite-")
	if err != nil {
		return nil, err
	}
	for i, unit := range input.units {
		input.units[i].unit, err = writeUnit(filepath.Join(input.root, "original"), unit.unit.ID, unit.message, unit.patch)
		if err != nil {
			_ = os.RemoveAll(input.root)
			return nil, err
		}
	}
	return input, nil
}

func writeUnit(root, id string, message, patch []byte) (definition.Unit, error) {
	directory := filepath.Join(root, id)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return definition.Unit{}, err
	}
	unit := definition.Unit{ID: id, Directory: directory, MessagePath: filepath.Join(directory, "COMMIT_EDITMSG")}
	if err := os.WriteFile(unit.MessagePath, message, 0o644); err != nil {
		return definition.Unit{}, err
	}
	if len(patch) > 0 {
		unit.PatchPath = filepath.Join(directory, "patch")
		if err := os.WriteFile(unit.PatchPath, patch, 0o644); err != nil {
			return definition.Unit{}, err
		}
	}
	return unit, nil
}

func (s *Service) replay(ctx context.Context, input *snapshot, dryRun bool, candidate func(string) ([]frozenUnit, error), result Result) (Result, error) {
	base, err := s.Git.Output(ctx, s.Git.Root, "rev-parse", "--verify", "refs/tags/"+input.upstream.Tag+"^{commit}")
	if err != nil {
		return Result{}, fmt.Errorf("resolve exact upstream tag: %w", err)
	}
	if base != input.upstream.Commit {
		return Result{}, fmt.Errorf("upstream tag resolves to %q, expected %q", base, input.upstream.Commit)
	}
	worktree := filepath.Join(input.root, "worktree")
	if err := s.Git.Run(ctx, s.Git.Root, "worktree", "add", "--detach", worktree, base); err != nil {
		return Result{}, fmt.Errorf("create rewrite worktree: %w", err)
	}
	worktreePresent := true
	defer func() {
		if worktreePresent {
			_ = s.Git.Run(context.Background(), s.Git.Root, "worktree", "remove", "--force", worktree)
		}
	}()
	layers := &layercommit.Service{Git: s.Git}
	for _, unit := range input.units {
		if err := layers.Apply(ctx, worktree, unit.unit); err != nil {
			return Result{}, fmt.Errorf("replay baseline layer %q: %w", unit.unit.ID, err)
		}
	}
	result.BeforeTree, err = s.tree(ctx, worktree)
	if err != nil {
		return Result{}, err
	}
	if err := s.Git.Run(ctx, worktree, "reset", "--hard", base); err != nil {
		return Result{}, err
	}
	generated, err := candidate(worktree)
	if err != nil {
		return Result{}, err
	}
	result.AfterTree, err = s.tree(ctx, worktree)
	if err != nil {
		return Result{}, err
	}
	if err := requireSameTree(result.BeforeTree, result.AfterTree); err != nil {
		return Result{}, err
	}
	// Verify the serialized definitions independently, not just candidate commits.
	if err := s.Git.Run(ctx, worktree, "reset", "--hard", base); err != nil {
		return Result{}, err
	}
	for _, unit := range generated {
		if err := layers.Apply(ctx, worktree, unit.unit); err != nil {
			return Result{}, fmt.Errorf("verify regenerated layer %q: %w", unit.unit.ID, err)
		}
	}
	verified, err := s.tree(ctx, worktree)
	if err != nil {
		return Result{}, err
	}
	if err := requireSameTree(result.BeforeTree, verified); err != nil {
		return Result{}, err
	}
	result.DryRun = dryRun
	for _, unit := range input.units {
		result.BeforeLayers = append(result.BeforeLayers, unit.unit.ID)
	}
	for _, unit := range generated {
		result.AfterLayers = append(result.AfterLayers, unit.unit.ID)
	}
	if err := s.requireUnchanged(input); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := s.Git.Run(context.Background(), s.Git.Root, "worktree", "remove", "--force", worktree); err != nil {
		return Result{}, fmt.Errorf("remove rewrite worktree: %w", err)
	}
	worktreePresent = false
	if !dryRun && !sameUnits(input.units, generated) {
		if err := s.install(ctx, input, generated); err != nil {
			return Result{}, err
		}
	}
	return result, nil
}

func sameUnits(before, after []frozenUnit) bool {
	if len(before) != len(after) {
		return false
	}
	for i := range before {
		if before[i].unit.ID != after[i].unit.ID || !bytes.Equal(before[i].message, after[i].message) || !bytes.Equal(before[i].patch, after[i].patch) {
			return false
		}
	}
	return true
}

func requireSameTree(before, after string) error {
	if before != after {
		return fmt.Errorf("rewrite changes final tree: before %s, after %s", before, after)
	}
	return nil
}

func (s *Service) applyPatch(ctx context.Context, worktree string, patch []byte, reverse bool) error {
	if len(patch) == 0 {
		return nil
	}
	args := []string{"-c", "rerere.enabled=false", "apply", "--3way", "--index", "--whitespace=nowarn"}
	if reverse {
		args = append(args, "--reverse")
	}
	_, err := s.Git.Invoke(ctx, worktree, gitrepo.Invocation{Arguments: args, Stdin: patch})
	return err
}

func (s *Service) applySelected(ctx context.Context, worktree string, selected []Hunk, reverse bool) error {
	// Each partial file patch retains its original blob IDs. Applying selections
	// separately lets Git merge each reconstructed partial postimage in turn.
	for _, hunk := range selected {
		if err := s.applyPatch(ctx, worktree, hunk.Patch, reverse); err != nil {
			return fmt.Errorf("apply selected hunk %q (header %q, reverse %t): %w", hunk.ID, hunk.Header, reverse, err)
		}
	}
	return nil
}

func (s *Service) prepareBackwardSelections(ctx context.Context, worktree, source string, selected []Hunk) error {
	for _, hunk := range selected {
		before, err := s.Git.Output(ctx, worktree, "write-tree")
		if err != nil {
			return err
		}
		// Reversing and reapplying must recover the current index. Otherwise
		// an intervening layer removed this change, and the original source
		// would own it again even if the final projected tree still matched.
		if err := s.applyPatch(ctx, worktree, hunk.Patch, true); err != nil {
			return fmt.Errorf("verify moved hunk %q before source %q: %w", hunk.ID, source, err)
		}
		if err := s.applyPatch(ctx, worktree, hunk.Patch, false); err != nil {
			return fmt.Errorf("verify moved hunk %q before source %q: %w", hunk.ID, source, err)
		}
		after, err := s.Git.Output(ctx, worktree, "write-tree")
		if err != nil {
			return err
		}
		if before != after {
			return fmt.Errorf("moved hunk %q is no longer present before source layer %q", hunk.ID, source)
		}
		// Restore the selected preimage until the complete original delta is
		// replayed. Git cannot apply an already-performed deletion or type
		// conversion reliably; the source commit records no selected delta.
		if err := s.applyPatch(ctx, worktree, hunk.Patch, true); err != nil {
			return fmt.Errorf("restore moved preimage before source %q: %w", source, err)
		}
	}
	return nil
}

func (s *Service) commitCapture(ctx context.Context, root, worktree, id, before string, message []byte) (frozenUnit, error) {
	unit, err := writeUnit(filepath.Join(root, "candidate"), id, message, nil)
	if err != nil {
		return frozenUnit{}, err
	}
	layers := &layercommit.Service{Git: s.Git}
	if err := layers.Commit(ctx, worktree, unit); err != nil {
		return frozenUnit{}, fmt.Errorf("commit regenerated layer %q: %w", id, err)
	}
	after, err := s.head(ctx, worktree)
	if err != nil {
		return frozenUnit{}, err
	}
	captured, err := layers.Capture(ctx, layercommit.CaptureRequest{Before: before, After: after})
	if err != nil {
		return frozenUnit{}, err
	}
	unit, err = writeUnit(filepath.Join(root, "candidate"), id, message, captured.Patch)
	if err != nil {
		return frozenUnit{}, err
	}
	return frozenUnit{unit: unit, message: bytes.Clone(message), patch: captured.Patch}, nil
}

func (s *Service) head(ctx context.Context, worktree string) (string, error) {
	return s.Git.Output(ctx, worktree, "rev-parse", "HEAD")
}

func (s *Service) tree(ctx context.Context, worktree string) (string, error) {
	return s.Git.Output(ctx, worktree, "rev-parse", "HEAD^{tree}")
}

func layerIDs(units []definition.Unit) []string {
	result := make([]string, 0, len(units))
	for _, unit := range units {
		result = append(result, unit.ID)
	}
	return result
}

func (s *Service) requireUnchanged(input *snapshot) error {
	current, err := s.readInputs()
	if err != nil {
		return fmt.Errorf("canonical inputs changed during rewrite: %w", err)
	}
	if current.digest != input.digest {
		return errors.New("canonical inputs changed during rewrite")
	}
	return nil
}

func (s *Service) install(ctx context.Context, input *snapshot, generated []frozenUnit) error {
	stage, err := os.MkdirTemp(s.Definition.Root, ".layerctl-rewrite-stage-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	target := filepath.Join(s.Definition.Root, "layers")
	info, err := os.Lstat(target)
	if err != nil {
		return err
	}
	if err := os.Chmod(stage, info.Mode()); err != nil {
		return err
	}
	for _, unit := range generated {
		if _, err := writeUnit(stage, unit.unit.ID, unit.message, unit.patch); err != nil {
			return err
		}
	}
	backup, err := os.MkdirTemp(s.Definition.Root, ".layerctl-rewrite-backup-")
	if err != nil {
		return err
	}
	backupDisposable := true
	defer func() {
		if backupDisposable {
			_ = os.RemoveAll(backup)
		}
	}()
	if err := os.Remove(backup); err != nil {
		return err
	}
	if err := s.requireUnchanged(input); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	retained, err := replaceLayers(stage, target, backup, os.Rename, func() error {
		if err := s.requireBackupUnchanged(input, backup); err != nil {
			return err
		}
		return ctx.Err()
	})
	backupDisposable = !retained
	if err != nil {
		return err
	}
	units := make([]definition.Unit, 0, len(generated))
	for _, unit := range generated {
		directory := filepath.Join(target, unit.unit.ID)
		installed := definition.Unit{ID: unit.unit.ID, Directory: directory, MessagePath: filepath.Join(directory, "COMMIT_EDITMSG")}
		if len(unit.patch) > 0 {
			installed.PatchPath = filepath.Join(directory, "patch")
		}
		units = append(units, installed)
	}
	s.Definition.Layers = units
	return nil
}

// replaceLayers returns true only when restoration fails and backup still
// holds original inputs. Its caller must retain that directory for recovery.
func replaceLayers(stage, target, backup string, rename func(string, string) error, verify func() error) (bool, error) {
	if err := rename(target, backup); err != nil {
		return false, fmt.Errorf("backup layers: %w", err)
	}
	// Inspect the actual renamed inputs before publishing. A manual edit made
	// after the last pre-rename digest check must be restored, not discarded.
	var err error
	if verify != nil {
		err = verify()
	}
	if err == nil {
		err = rename(stage, target)
	}
	if err != nil {
		if restoreErr := rename(backup, target); restoreErr != nil {
			return true, fmt.Errorf("install layers: %w; restore backup %q: %w", err, backup, restoreErr)
		}
		return false, fmt.Errorf("install layers: %w", err)
	}
	return false, nil
}

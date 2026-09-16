// Package upstream advances canonical Frodex definitions to another exact
// Codex release through one resumable local projection.
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/friel-openai/codex/layerctl/internal/definition"
	"github.com/friel-openai/codex/layerctl/internal/gitrepo"
	"github.com/friel-openai/codex/layerctl/internal/projection"
)

const projectionName = "upstream-advance"

var releaseTagPattern = regexp.MustCompile(`^rust-v[0-9]+\.[0-9]+\.[0-9]+(-(alpha(\.[0-9]+){0,2}|beta(\.[0-9]+)?))?$`)

// Service owns resumable upstream advancement state for one canonical
// repository. Canonical definitions change only through explicit layer refresh
// and the final upstream.json update.
type Service struct {
	Definition *definition.Repository // required
	Git        *gitrepo.Repository    // required
	Projection *projection.Service    // required
}

// AdvanceRequest selects an already available exact Codex tag and optional
// caller-chosen worktree path.
type AdvanceRequest struct {
	Tag          string
	WorktreePath string
}

// Advance starts a new upstream projection and applies units until completion
// or the first conflict requiring operator resolution.
func (s *Service) Advance(ctx context.Context, req AdvanceRequest) (string, error) {
	if !releaseTagPattern.MatchString(req.Tag) {
		return "", fmt.Errorf("invalid Codex release tag %q", req.Tag)
	}
	statePath, err := s.statePath(ctx)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(statePath); err == nil {
		return "", errors.New("an upstream advance is already active; use upstream continue or upstream abort")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect upstream state: %w", err)
	}
	if _, err := s.Projection.Get(ctx, projectionName); err == nil {
		return "", fmt.Errorf("projection %q already exists", projectionName)
	}

	commit, err := s.Git.Output(ctx, s.Git.Root, "rev-parse", req.Tag+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve exact Codex tag %q: %w", req.Tag, err)
	}
	baseRef, headRef := advanceRefs()
	if err := s.Git.Run(ctx, s.Git.Root, "update-ref", baseRef, commit, ""); err != nil {
		return "", fmt.Errorf("create upstream base ref: %w", err)
	}
	if err := s.Git.Run(ctx, s.Git.Root, "update-ref", headRef, commit, ""); err != nil {
		_ = s.Git.Run(context.Background(), s.Git.Root, "update-ref", "-d", baseRef)
		return "", fmt.Errorf("create upstream head ref: %w", err)
	}

	worktreePath := req.WorktreePath
	if worktreePath == "" {
		worktreePath, err = temporaryWorktreePath()
		if err != nil {
			_ = s.Projection.Delete(context.Background(), projectionName)
			return "", err
		}
	}
	worktreePath, err = s.Projection.Checkout(ctx, projection.CheckoutRequest{
		Name:         projectionName,
		WorktreePath: worktreePath,
	})
	if err != nil {
		_ = s.Projection.Delete(context.Background(), projectionName)
		return "", err
	}

	state := advanceState{
		TargetTag:    req.Tag,
		TargetCommit: commit,
		WorktreePath: worktreePath,
	}
	if err := writeState(statePath, state); err != nil {
		_ = s.Projection.Delete(context.Background(), projectionName)
		return "", err
	}
	if err := s.process(ctx, statePath, &state); err != nil {
		return worktreePath, err
	}
	return worktreePath, nil
}

// Continue resumes an active advance after the operator resolves or refreshes
// the current unit.
func (s *Service) Continue(ctx context.Context) (string, error) {
	statePath, err := s.statePath(ctx)
	if err != nil {
		return "", err
	}
	state, err := readState(statePath)
	if err != nil {
		return "", err
	}
	units := s.Definition.Layers
	if state.UnitIndex >= len(units) {
		return state.WorktreePath, s.finish(statePath, state)
	}
	unit := units[state.UnitIndex]

	if state.ApplyingHead != "" {
		return state.WorktreePath, s.continueUnit(ctx, statePath, &state, unit)
	}
	if state.NeedsRefresh {
		matches, err := s.refreshedUnitMatches(ctx, state.WorktreePath, unit)
		if err != nil {
			return state.WorktreePath, err
		}
		if !matches {
			return state.WorktreePath, fmt.Errorf("unit %q is not refreshed; run layerctl layer refresh %s --from %s, then layerctl upstream continue", unit.ID, unit.ID, projectionName)
		}
		state.NeedsRefresh = false
		state.Resolving = false
		state.UnitIndex++
		if err := writeState(statePath, state); err != nil {
			return state.WorktreePath, err
		}
		return state.WorktreePath, s.process(ctx, statePath, &state)
	}
	return state.WorktreePath, s.process(ctx, statePath, &state)
}

// Abort deletes the active projection and its local recovery state.
func (s *Service) Abort(ctx context.Context) error {
	statePath, err := s.statePath(ctx)
	if err != nil {
		return err
	}
	if _, err := readState(statePath); err != nil {
		return err
	}
	if err := s.Projection.Delete(ctx, projectionName); err != nil {
		return err
	}
	if err := os.Remove(statePath); err != nil {
		return fmt.Errorf("remove upstream state: %w", err)
	}
	return nil
}

// Check proves that the current canonical definition materializes completely.
func (s *Service) Check(ctx context.Context) error {
	temporary, err := os.CreateTemp("", "layerctl-check-name-")
	if err != nil {
		return fmt.Errorf("create check identity: %w", err)
	}
	name := "check-" + filepath.Base(temporary.Name())
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close check identity: %w", err)
	}
	_ = os.Remove(temporary.Name())
	if _, err := s.Projection.Create(ctx, projection.CreateRequest{Name: name}); err != nil {
		return err
	}
	if err := s.Projection.Delete(ctx, name); err != nil {
		return fmt.Errorf("delete check projection: %w", err)
	}
	return nil
}

func (s *Service) process(ctx context.Context, statePath string, state *advanceState) error {
	units := s.Definition.Layers
	for state.UnitIndex < len(units) {
		unit := units[state.UnitIndex]
		head, err := s.Git.Output(ctx, state.WorktreePath, "rev-parse", "HEAD")
		if err != nil {
			return fmt.Errorf("resolve advance head: %w", err)
		}
		state.ApplyingHead = head
		state.Resolving = false
		if err := writeState(statePath, *state); err != nil {
			return err
		}
		if err := s.Projection.ApplyUnit(ctx, state.WorktreePath, unit); err != nil {
			hasConflicts, inspectErr := s.Projection.UnitHasConflicts(ctx, state.WorktreePath)
			if inspectErr != nil {
				return errors.Join(err, inspectErr)
			}
			if !hasConflicts {
				state.ApplyingHead = ""
				if writeErr := writeState(statePath, *state); writeErr != nil {
					return errors.Join(err, writeErr)
				}
				return fmt.Errorf("apply unit %q: %w", unit.ID, err)
			}
			state.Resolving = true
			if writeErr := writeState(statePath, *state); writeErr != nil {
				return errors.Join(err, writeErr)
			}
			return fmt.Errorf("unit %q requires resolution in %q: %w; resolve the desired tree, then run layerctl upstream continue", unit.ID, state.WorktreePath, err)
		}
		state.UnitIndex++
		state.ApplyingHead = ""
		if err := writeState(statePath, *state); err != nil {
			return err
		}
	}
	return s.finish(statePath, *state)
}

func (s *Service) continueUnit(
	ctx context.Context,
	statePath string,
	state *advanceState,
	unit definition.Unit,
) error {
	head, err := s.Git.Output(ctx, state.WorktreePath, "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("resolve advance head: %w", err)
	}
	if head != state.ApplyingHead {
		return s.reconcileCompletedUnit(ctx, statePath, state, unit)
	}
	if !state.Resolving {
		state.ApplyingHead = ""
		if err := writeState(statePath, *state); err != nil {
			return err
		}
		return s.process(ctx, statePath, state)
	}
	if err := s.Git.Run(ctx, state.WorktreePath, "add", "-A"); err != nil {
		return fmt.Errorf("stage resolved unit %q: %w", unit.ID, err)
	}
	if err := s.Projection.CommitUnit(ctx, state.WorktreePath, unit); err != nil {
		return fmt.Errorf("commit resolved unit %q: %w", unit.ID, err)
	}
	state.ApplyingHead = ""
	state.NeedsRefresh = true
	if err := writeState(statePath, *state); err != nil {
		return err
	}
	return refreshRequiredError(unit)
}

func (s *Service) reconcileCompletedUnit(
	ctx context.Context,
	statePath string,
	state *advanceState,
	unit definition.Unit,
) error {
	parent, err := s.Git.Output(ctx, state.WorktreePath, "rev-parse", "HEAD^")
	if err != nil {
		return fmt.Errorf("resolve parent of recovered unit %q: %w", unit.ID, err)
	}
	if parent != state.ApplyingHead {
		return fmt.Errorf("unit %q changed advance HEAD unexpectedly; use layerctl upstream abort", unit.ID)
	}

	state.ApplyingHead = ""
	if state.Resolving {
		state.NeedsRefresh = true
		if err := writeState(statePath, *state); err != nil {
			return err
		}
		return refreshRequiredError(unit)
	}
	state.UnitIndex++
	if err := writeState(statePath, *state); err != nil {
		return err
	}
	return s.process(ctx, statePath, state)
}

func refreshRequiredError(unit definition.Unit) error {
	return fmt.Errorf(
		"captured resolved unit %q; run layerctl layer refresh %s --from %s, then layerctl upstream continue",
		unit.ID,
		unit.ID,
		projectionName,
	)
}

func (s *Service) refreshedUnitMatches(ctx context.Context, worktree string, unit definition.Unit) (bool, error) {
	resolvedHead, err := s.Git.Output(ctx, worktree, "rev-parse", "HEAD")
	if err != nil {
		return false, err
	}
	predecessor, err := s.Git.Output(ctx, worktree, "rev-parse", "HEAD^")
	if err != nil {
		return false, err
	}
	temporaryPath, err := temporaryWorktreePath()
	if err != nil {
		return false, err
	}
	if err := s.Git.Run(ctx, s.Git.Root, "worktree", "add", "--detach", temporaryPath, predecessor); err != nil {
		return false, err
	}
	defer func() {
		_ = s.Git.Run(context.Background(), s.Git.Root, "worktree", "remove", "--force", temporaryPath)
	}()
	if err := s.Projection.ApplyUnit(ctx, temporaryPath, unit); err != nil {
		return false, nil
	}
	generatedTree, err := s.Git.Output(ctx, temporaryPath, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return false, err
	}
	resolvedTree, err := s.Git.Output(ctx, s.Git.Root, "rev-parse", resolvedHead+"^{tree}")
	if err != nil {
		return false, err
	}
	return generatedTree == resolvedTree, nil
}

func (s *Service) finish(statePath string, state advanceState) error {
	path := filepath.Join(s.Definition.Root, "upstream.json")
	content, err := json.MarshalIndent(definition.Upstream{Tag: state.TargetTag, Commit: state.TargetCommit}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode upstream definition: %w", err)
	}
	content = append(content, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".layerctl-upstream-")
	if err != nil {
		return fmt.Errorf("create temporary upstream definition: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary upstream definition: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary upstream definition: %w", err)
	}
	if err := replaceFile(temporaryPath, path); err != nil {
		return err
	}
	s.Definition.Upstream = definition.Upstream{Tag: state.TargetTag, Commit: state.TargetCommit}
	if err := os.Remove(statePath); err != nil {
		return fmt.Errorf("remove completed upstream state: %w", err)
	}
	return nil
}

func (s *Service) statePath(ctx context.Context) (string, error) {
	commonDirectory, err := s.Git.Output(ctx, s.Git.Root, "rev-parse", "--git-common-dir")
	if err != nil {
		return "", fmt.Errorf("resolve Git common directory: %w", err)
	}
	if !filepath.IsAbs(commonDirectory) {
		commonDirectory = filepath.Join(s.Git.Root, commonDirectory)
	}
	directory := filepath.Join(commonDirectory, "layerctl")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return "", fmt.Errorf("create layerctl state directory: %w", err)
	}
	return filepath.Join(directory, "upstream.json"), nil
}

type advanceState struct {
	TargetTag    string `json:"targetTag"`
	TargetCommit string `json:"targetCommit"`
	WorktreePath string `json:"worktreePath"`
	UnitIndex    int    `json:"unitIndex"`
	NeedsRefresh bool   `json:"needsRefresh"`
	ApplyingHead string `json:"applyingHead,omitempty"`
	Resolving    bool   `json:"resolving,omitempty"`
}

func writeState(path string, state advanceState) error {
	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode upstream state: %w", err)
	}
	content = append(content, '\n')
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("write upstream state: %w", err)
	}
	return nil
}

func readState(path string) (advanceState, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return advanceState{}, errors.New("no upstream advance is active")
		}
		return advanceState{}, fmt.Errorf("read upstream state: %w", err)
	}
	var state advanceState
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return advanceState{}, fmt.Errorf("decode upstream state: %w", err)
	}
	return state, nil
}

func temporaryWorktreePath() (string, error) {
	path, err := os.MkdirTemp("", "layerctl-upstream-")
	if err != nil {
		return "", fmt.Errorf("create temporary worktree path: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("prepare temporary worktree path %q: %w", path, err)
	}
	return path, nil
}

func advanceRefs() (string, string) {
	prefix := "refs/layerctl/projections/" + projectionName + "/"
	return prefix + "base", prefix + "head"
}

func replaceFile(source, target string) error {
	backup, err := os.CreateTemp(filepath.Dir(target), ".layerctl-upstream-backup-")
	if err != nil {
		return fmt.Errorf("reserve upstream backup: %w", err)
	}
	backupPath := backup.Name()
	if err := backup.Close(); err != nil {
		return fmt.Errorf("close upstream backup: %w", err)
	}
	if err := os.Remove(backupPath); err != nil {
		return fmt.Errorf("prepare upstream backup: %w", err)
	}
	if err := os.Rename(target, backupPath); err != nil {
		return fmt.Errorf("backup upstream definition: %w", err)
	}
	if err := os.Rename(source, target); err != nil {
		_ = os.Rename(backupPath, target)
		return fmt.Errorf("replace upstream definition: %w", err)
	}
	if err := os.Remove(backupPath); err != nil {
		return fmt.Errorf("remove upstream backup: %w", err)
	}
	return nil
}

// Package projection materializes canonical Frodex definitions as editable
// local Git refs and manages the worktrees attached to those refs.
package projection

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/friel-openai/codex/layerctl/internal/definition"
	"github.com/friel-openai/codex/layerctl/internal/gitrepo"
	"github.com/friel-openai/codex/layerctl/internal/layercommit"
)

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Service owns projection creation and lifecycle policy for one canonical
// repository.
type Service struct {
	Definition *definition.Repository // required
	Git        *gitrepo.Repository    // required
	Log        *log.Logger            // required
}

// CreateRequest selects a local projection and an optional terminal layer.
// An empty Through value includes every layer.
type CreateRequest struct {
	Name         string
	WorktreePath string
	Through      string
}

// Create materializes the selected units, then publishes base and head refs at
// the generated tip. It leaves no refs behind when generation fails.
func (s *Service) Create(ctx context.Context, req CreateRequest) (Projection, error) {
	name, err := parseName(req.Name)
	if err != nil {
		return Projection{}, err
	}
	if err := s.requireAbsent(ctx, name); err != nil {
		return Projection{}, err
	}

	units, err := s.selectedUnits(req.Through)
	if err != nil {
		return Projection{}, err
	}
	baseCommit, err := s.resolveUpstream(ctx)
	if err != nil {
		return Projection{}, err
	}

	temporaryPath, err := os.MkdirTemp("", "layerctl-projection-")
	if err != nil {
		return Projection{}, fmt.Errorf("create temporary worktree path: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return Projection{}, fmt.Errorf("prepare temporary worktree path %q: %w", temporaryPath, err)
	}

	worktreeAdded := false
	defer func() {
		if worktreeAdded {
			_ = s.Git.Run(context.Background(), s.Git.Root, "worktree", "remove", "--force", temporaryPath)
		}
		_ = os.RemoveAll(temporaryPath)
	}()

	if err := s.Git.Run(ctx, s.Git.Root, "worktree", "add", "--detach", temporaryPath, baseCommit); err != nil {
		return Projection{}, fmt.Errorf("create generation worktree: %w", err)
	}
	worktreeAdded = true

	for _, unit := range units {
		s.Log.Printf("applying %s", unit.ID)
		if err := s.ApplyUnit(ctx, temporaryPath, unit); err != nil {
			return Projection{}, fmt.Errorf("apply unit %q: %w", unit.ID, err)
		}
	}

	tip, err := s.Git.Output(ctx, temporaryPath, "rev-parse", "HEAD")
	if err != nil {
		return Projection{}, fmt.Errorf("resolve generated tip: %w", err)
	}
	baseRef, headRef := refs(name)
	if err := s.Git.Run(ctx, s.Git.Root, "update-ref", baseRef, tip, ""); err != nil {
		return Projection{}, fmt.Errorf("create base ref: %w", err)
	}
	if err := s.Git.Run(ctx, s.Git.Root, "update-ref", headRef, tip, ""); err != nil {
		_ = s.Git.Run(context.Background(), s.Git.Root, "update-ref", "-d", baseRef, tip)
		return Projection{}, fmt.Errorf("create head ref: %w", err)
	}

	projection := Projection{Name: name, Base: tip, Head: tip}
	if req.WorktreePath != "" {
		path, err := s.Checkout(ctx, CheckoutRequest{Name: name, WorktreePath: req.WorktreePath})
		if err != nil {
			_ = s.Delete(context.Background(), name)
			return Projection{}, err
		}
		projection.WorktreePath = path
	}
	return projection, nil
}

// Projection is the current local state for one name.
type Projection struct {
	Name         string
	Base         string
	Head         string
	WorktreePath string
}

// Get returns the current refs and attached worktree for name.
func (s *Service) Get(ctx context.Context, rawName string) (Projection, error) {
	name, err := parseName(rawName)
	if err != nil {
		return Projection{}, err
	}
	projections, err := s.List(ctx)
	if err != nil {
		return Projection{}, err
	}
	for _, item := range projections {
		if item.Name == name {
			return item, nil
		}
	}
	return Projection{}, fmt.Errorf("projection %q does not exist", name)
}

func (s *Service) selectedUnits(through string) ([]definition.Unit, error) {
	units := make([]definition.Unit, 0, len(s.Definition.Layers))
	for _, layer := range s.Definition.Layers {
		units = append(units, layer)
		if layer.ID == through {
			return units, nil
		}
	}
	if through != "" {
		return nil, fmt.Errorf("unknown terminal layer %q", through)
	}
	return units, nil
}

func (s *Service) resolveUpstream(ctx context.Context) (string, error) {
	commit, err := s.Git.Output(ctx, s.Git.Root, "rev-parse", s.Definition.Upstream.Tag+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve upstream tag %q: %w", s.Definition.Upstream.Tag, err)
	}
	if commit != s.Definition.Upstream.Commit {
		return "", fmt.Errorf("upstream tag %q resolves to %q, expected %q", s.Definition.Upstream.Tag, commit, s.Definition.Upstream.Commit)
	}
	return commit, nil
}

// ApplyUnit applies one definition to worktree and creates its generated commit.
// Callers own the worktree's starting commit and any recovery after failure.
func (s *Service) ApplyUnit(ctx context.Context, worktree string, unit definition.Unit) error {
	layers := &layercommit.Service{Git: s.Git}
	if err := layers.Apply(ctx, worktree, unit); err != nil {
		return fmt.Errorf("apply layer %q: %w", unit.ID, err)
	}
	return nil
}

// CommitUnit records the staged resolution for an interrupted ApplyUnit.
func (s *Service) CommitUnit(ctx context.Context, worktree string, unit definition.Unit) error {
	layers := &layercommit.Service{Git: s.Git}
	return layers.Commit(ctx, worktree, unit)
}

// UnitHasConflicts reports whether ApplyUnit left unresolved index entries.
func (s *Service) UnitHasConflicts(ctx context.Context, worktree string) (bool, error) {
	layers := &layercommit.Service{Git: s.Git}
	return layers.HasConflicts(ctx, worktree)
}

func (s *Service) requireAbsent(ctx context.Context, name string) error {
	baseRef, headRef := refs(name)
	for _, ref := range []string{baseRef, headRef} {
		if _, err := s.Git.Output(ctx, s.Git.Root, "show-ref", "--verify", "--hash", ref); err == nil {
			return fmt.Errorf("projection %q already exists", name)
		}
	}
	return nil
}

// List returns complete and partial local projections sorted by name.
func (s *Service) List(ctx context.Context) ([]Projection, error) {
	output, err := s.Git.Output(ctx, s.Git.Root, "for-each-ref", "--format=%(refname) %(objectname)", "refs/layerctl/projections")
	if err != nil {
		return nil, fmt.Errorf("list projection refs: %w", err)
	}
	byName := make(map[string]*Projection)
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		name, kind, ok := parseProjectionRef(fields[0])
		if !ok {
			continue
		}
		projection := byName[name]
		if projection == nil {
			projection = &Projection{Name: name}
			byName[name] = projection
		}
		if kind == "base" {
			projection.Base = fields[1]
		} else {
			projection.Head = fields[1]
		}
	}

	worktrees, err := s.attachedWorktrees(ctx)
	if err != nil {
		return nil, err
	}
	projections := make([]Projection, 0, len(byName))
	for name, projection := range byName {
		projection.WorktreePath = worktrees[name]
		projections = append(projections, *projection)
	}
	sort.Slice(projections, func(i, j int) bool {
		return projections[i].Name < projections[j].Name
	})
	return projections, nil
}

// CheckoutRequest attaches a new worktree directly to a projection head ref.
type CheckoutRequest struct {
	Name         string
	WorktreePath string
}

// Checkout creates an attached worktree and returns its absolute path.
func (s *Service) Checkout(ctx context.Context, req CheckoutRequest) (string, error) {
	name, err := parseName(req.Name)
	if err != nil {
		return "", err
	}
	if req.WorktreePath == "" {
		return "", errors.New("worktree path is required")
	}
	path, err := filepath.Abs(req.WorktreePath)
	if err != nil {
		return "", fmt.Errorf("resolve worktree path %q: %w", req.WorktreePath, err)
	}
	worktrees, err := s.attachedWorktrees(ctx)
	if err != nil {
		return "", err
	}
	if existing := worktrees[name]; existing != "" {
		return "", fmt.Errorf("projection %q is already checked out at %q", name, existing)
	}
	_, headRef := refs(name)
	if _, err := s.Git.Output(ctx, s.Git.Root, "show-ref", "--verify", "--hash", headRef); err != nil {
		return "", fmt.Errorf("projection %q does not exist", name)
	}
	if err := s.Git.Run(ctx, s.Git.Root, "worktree", "add", "--detach", path, headRef); err != nil {
		return "", fmt.Errorf("create worktree %q: %w", path, err)
	}
	if err := s.Git.Run(ctx, path, "symbolic-ref", "HEAD", headRef); err != nil {
		_ = s.Git.Run(context.Background(), s.Git.Root, "worktree", "remove", "--force", path)
		return "", fmt.Errorf("attach worktree to %q: %w", headRef, err)
	}
	return path, nil
}

// Path returns the worktree currently attached to name.
func (s *Service) Path(ctx context.Context, rawName string) (string, error) {
	name, err := parseName(rawName)
	if err != nil {
		return "", err
	}
	worktrees, err := s.attachedWorktrees(ctx)
	if err != nil {
		return "", err
	}
	path := worktrees[name]
	if path == "" {
		return "", fmt.Errorf("projection %q has no attached worktree", name)
	}
	return path, nil
}

// Delete removes every attached worktree and both refs for name. The explicit
// command is the caller's authorization to discard dirty and uncaptured work.
func (s *Service) Delete(ctx context.Context, rawName string) error {
	name, err := parseName(rawName)
	if err != nil {
		return err
	}
	projections, err := s.List(ctx)
	if err != nil {
		return err
	}
	var target *Projection
	for i := range projections {
		if projections[i].Name == name {
			target = &projections[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("projection %q does not exist", name)
	}
	if target.WorktreePath != "" {
		if err := s.Git.Run(ctx, s.Git.Root, "worktree", "remove", "--force", target.WorktreePath); err != nil {
			return fmt.Errorf("remove worktree %q: %w", target.WorktreePath, err)
		}
	}
	baseRef, headRef := refs(name)
	for _, ref := range []string{headRef, baseRef} {
		if err := s.Git.Run(ctx, s.Git.Root, "update-ref", "-d", ref); err != nil {
			return fmt.Errorf("delete ref %q: %w", ref, err)
		}
	}
	return nil
}

func (s *Service) attachedWorktrees(ctx context.Context) (map[string]string, error) {
	output, err := s.Git.Output(ctx, s.Git.Root, "worktree", "list", "--porcelain")
	if err != nil {
		return nil, fmt.Errorf("list worktrees: %w", err)
	}
	worktrees := make(map[string]string)
	var path string
	for _, line := range strings.Split(output, "\n") {
		switch {
		case strings.HasPrefix(line, "worktree "):
			path = strings.TrimPrefix(line, "worktree ")
		case strings.HasPrefix(line, "branch "):
			name, kind, ok := parseProjectionRef(strings.TrimPrefix(line, "branch "))
			if ok && kind == "head" {
				worktrees[name] = path
			}
		}
	}
	return worktrees, nil
}

func parseName(raw string) (string, error) {
	if !namePattern.MatchString(raw) {
		return "", fmt.Errorf("invalid projection name %q", raw)
	}
	return raw, nil
}

func refs(name string) (string, string) {
	prefix := "refs/layerctl/projections/" + name + "/"
	return prefix + "base", prefix + "head"
}

func parseProjectionRef(ref string) (string, string, bool) {
	const prefix = "refs/layerctl/projections/"
	remainder := strings.TrimPrefix(ref, prefix)
	if remainder == ref {
		return "", "", false
	}
	name, kind, ok := strings.Cut(remainder, "/")
	if !ok || !namePattern.MatchString(name) || (kind != "base" && kind != "head") {
		return "", "", false
	}
	return name, kind, true
}

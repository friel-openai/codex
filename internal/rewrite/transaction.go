package rewrite

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/friel-openai/codex/layerctl/internal/definition"
)

// acquireRewriteLock excludes other rewrites of this canonical worktree, not
// rewrites of independent worktrees sharing the same Git object directory.
func acquireRewriteLock(canonicalRoot string) (func() error, error) {
	if err := rejectSymlink(canonicalRoot); err != nil {
		return nil, err
	}
	path := filepath.Join(canonicalRoot, ".layerctl-rewrite.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("acquire canonical rewrite lock %q: %w", path, err)
	}
	owned, err := file.Stat()
	if err != nil {
		// Without file identity, cleanup cannot distinguish a replacement lock.
		return nil, errors.Join(fmt.Errorf("inspect created rewrite lock %q; lock retained: %w", path, err), file.Close())
	}
	var mutex sync.Mutex
	released := false
	return func() error {
		mutex.Lock()
		defer mutex.Unlock()
		if released {
			return nil
		}
		released = true
		current, err := os.Lstat(path)
		if err != nil {
			return errors.Join(fmt.Errorf("inspect owned rewrite lock %q: %w", path, err), file.Close())
		}
		if !os.SameFile(owned, current) {
			return errors.Join(fmt.Errorf("canonical rewrite lock %q was replaced; replacement retained", path), file.Close())
		}
		return errors.Join(os.Remove(path), file.Close())
	}, nil
}

// requireBackupUnchanged reads the original directory after its rename, so an
// edit made between the last canonical check and rename cannot be discarded by
// deleting the backup after installation. It never reloads the absent layers/.
func (s *Service) requireBackupUnchanged(input *snapshot, backup string) error {
	if s.Definition == nil || input == nil {
		return errors.New("canonical definition and input snapshot are required")
	}
	upstreamPath := filepath.Join(s.Definition.Root, "upstream.json")
	for _, path := range []string{s.Definition.Root, upstreamPath, backup} {
		if err := rejectSymlink(path); err != nil {
			return err
		}
	}
	upstreamBytes, err := readBackupFile(upstreamPath)
	if err != nil {
		return err
	}
	var upstream definition.Upstream
	decoder := json.NewDecoder(bytes.NewReader(upstreamBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&upstream); err != nil {
		return fmt.Errorf("decode canonical upstream: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("unexpected value after object")
		}
		return fmt.Errorf("decode canonical upstream: %w", err)
	}
	if upstream != input.upstream {
		return errors.New("canonical upstream changed during rewrite")
	}
	digest := sha256.New()
	if err := hashInput(digest, "upstream.json", upstreamPath, upstreamBytes); err != nil {
		return err
	}
	entries, err := os.ReadDir(backup)
	if err != nil {
		return err
	}
	if len(entries) != len(input.units) {
		return errors.New("canonical layer entries changed during rewrite")
	}
	for i, entry := range entries {
		id := entry.Name()
		if err := definition.ValidateLayerID(id); err != nil {
			return err
		}
		if !entry.IsDir() || id != input.units[i].unit.ID {
			return fmt.Errorf("canonical layer entry %q changed during rewrite", id)
		}
		directory := filepath.Join(backup, id)
		if err := rejectSymlink(directory); err != nil {
			return err
		}
		files, err := os.ReadDir(directory)
		if err != nil {
			return err
		}
		messageFound := false
		patchFound := false
		for _, entry := range files {
			switch entry.Name() {
			case "COMMIT_EDITMSG":
				messageFound = true
			case "patch":
				patchFound = true
			default:
				return fmt.Errorf("invalid entry %q in original layer %q", entry.Name(), id)
			}
		}
		if !messageFound || patchFound != (input.units[i].unit.PatchPath != "") {
			return fmt.Errorf("canonical files in layer %q changed during rewrite", id)
		}
		for _, name := range []string{"COMMIT_EDITMSG", "patch"} {
			if name == "patch" && !patchFound {
				continue
			}
			path := filepath.Join(directory, name)
			if err := rejectSymlink(path); err != nil {
				return err
			}
			data, err := readBackupFile(path)
			if err != nil {
				return err
			}
			if err := hashInput(digest, id+"/"+name, path, data); err != nil {
				return err
			}
		}
	}
	var actual [sha256.Size]byte
	copy(actual[:], digest.Sum(nil))
	if actual != input.digest {
		return errors.New("canonical input files changed during rewrite")
	}
	return nil
}

func readBackupFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("canonical input %q must be a regular file", path)
	}
	// Reject special files before reading; reading a FIFO could block forever.
	return os.ReadFile(path)
}

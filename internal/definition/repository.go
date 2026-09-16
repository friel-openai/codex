// Package definition reads and validates the canonical Frodex repository.
// It owns the on-disk layer representation, while projection and maintenance
// packages own operations performed with those definitions.
package definition

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var layerIDPattern = regexp.MustCompile(`^[0-9]{4}-[a-z0-9]+(-[a-z0-9]+)*$`)

// FoundationLayerID identifies the required first generated commit.
const FoundationLayerID = "0000-foundation"

// Repository is one complete canonical Frodex definition.
// Layers are ordered lexically by their numbered directory names.
type Repository struct {
	Root     string
	Upstream Upstream
	Layers   []Unit
}

// Upstream identifies the annotated or lightweight release tag and the commit
// it must resolve to. Keeping both values detects a locally moved tag.
type Upstream struct {
	Tag    string `json:"tag"`
	Commit string `json:"commit"`
}

// Unit is one generated commit serialized as a message and optional tree diff.
type Unit struct {
	ID          string
	Directory   string
	MessagePath string
	PatchPath   string
}

// Load reads the canonical repository rooted at root and rejects malformed or
// ambiguous layer definitions before a projection mutates Git state.
func Load(root string) (*Repository, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve repository root %q: %w", root, err)
	}

	upstream, err := loadUpstream(filepath.Join(absRoot, "upstream.json"))
	if err != nil {
		return nil, err
	}

	layers, err := loadLayers(filepath.Join(absRoot, "layers"))
	if err != nil {
		return nil, err
	}
	if len(layers) == 0 || layers[0].ID != FoundationLayerID {
		return nil, fmt.Errorf("first layer must be %q", FoundationLayerID)
	}

	return &Repository{
		Root:     absRoot,
		Upstream: upstream,
		Layers:   layers,
	}, nil
}

func loadUpstream(path string) (Upstream, error) {
	file, err := os.Open(path)
	if err != nil {
		return Upstream{}, fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	var upstream Upstream
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&upstream); err != nil {
		return Upstream{}, fmt.Errorf("decode %q: %w", path, err)
	}
	if err := requireJSONEnd(decoder); err != nil {
		return Upstream{}, fmt.Errorf("decode %q: %w", path, err)
	}
	if upstream.Tag == "" {
		return Upstream{}, errors.New("upstream tag is required")
	}
	if upstream.Commit == "" {
		return Upstream{}, errors.New("upstream commit is required")
	}
	return upstream, nil
}

func requireJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected value after object")
		}
		return err
	}
	return nil
}

func loadLayers(path string) ([]Unit, error) {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return []Unit{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read layer directory %q: %w", path, err)
	}
	layers := make([]Unit, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() {
			return nil, fmt.Errorf("invalid layer entry %q", filepath.Join(path, name))
		}
		if err := ValidateLayerID(name); err != nil {
			return nil, fmt.Errorf("invalid layer entry %q: %w", filepath.Join(path, name), err)
		}
		directory := filepath.Join(path, name)
		unit, err := loadUnit(name, directory)
		if err != nil {
			return nil, err
		}
		layers = append(layers, unit)
	}
	return layers, nil
}

func loadUnit(id, directory string) (Unit, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return Unit{}, fmt.Errorf("read layer %q: %w", id, err)
	}
	unit := Unit{
		ID:          id,
		Directory:   directory,
		MessagePath: filepath.Join(directory, "COMMIT_EDITMSG"),
	}
	for _, entry := range entries {
		path := filepath.Join(directory, entry.Name())
		switch entry.Name() {
		case "COMMIT_EDITMSG":
			if err := requireNonemptyRegularFile(entry, path); err != nil {
				return Unit{}, err
			}
		case "patch":
			if err := requireNonemptyRegularFile(entry, path); err != nil {
				return Unit{}, err
			}
			unit.PatchPath = path
		default:
			return Unit{}, fmt.Errorf("invalid entry %q in layer %q", entry.Name(), id)
		}
	}
	if _, err := os.Stat(unit.MessagePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Unit{}, fmt.Errorf("layer %q is missing COMMIT_EDITMSG", id)
		}
		return Unit{}, fmt.Errorf("inspect layer message %q: %w", unit.MessagePath, err)
	}
	return unit, nil
}

func requireNonemptyRegularFile(entry os.DirEntry, path string) error {
	info, err := entry.Info()
	if err != nil {
		return fmt.Errorf("inspect layer file %q: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return fmt.Errorf("layer file %q must be a nonempty regular file", path)
	}
	return nil
}

// ValidateLayerID rejects names that cannot identify canonical layer directories.
func ValidateLayerID(id string) error {
	if !layerIDPattern.MatchString(id) {
		return fmt.Errorf("invalid layer ID %q", id)
	}
	return nil
}

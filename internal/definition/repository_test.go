package definition_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/friel-openai/codex/layerctl/internal/definition"
)

func TestLoadSortsNumberedLayerDirectories(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "upstream.json"), `{"tag":"rust-v1.2.3","commit":"abc123"}`)
	writeLayer(t, root, "0000-foundation", true)
	writeLayer(t, root, "0002-second", false)
	writeLayer(t, root, "0001-first", true)

	repository, err := definition.Load(root)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	gotLayers := []string{repository.Layers[0].ID, repository.Layers[1].ID, repository.Layers[2].ID}
	if want := []string{"0000-foundation", "0001-first", "0002-second"}; !reflect.DeepEqual(gotLayers, want) {
		t.Fatalf("layer order = %v, want %v", gotLayers, want)
	}
	if got, want := filepath.Base(repository.Layers[2].Directory), "0002-second"; got != want {
		t.Fatalf("last directory = %q, want %q", got, want)
	}
	if repository.Layers[1].PatchPath == "" {
		t.Fatal("first product layer has no patch")
	}
	if repository.Layers[2].PatchPath != "" {
		t.Fatalf("second product layer patch = %q, want none", repository.Layers[2].PatchPath)
	}
}

func TestLoadRejectsUnnumberedLayer(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "upstream.json"), `{"tag":"rust-v1.2.3","commit":"abc123"}`)
	writeLayer(t, root, "0000-foundation", true)
	writeLayer(t, root, "feature", true)

	if _, err := definition.Load(root); err == nil {
		t.Fatal("Load() error = nil, want invalid layer directory error")
	}
}

func TestLoadRejectsMailPatchLayer(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "upstream.json"), `{"tag":"rust-v1.2.3","commit":"abc123"}`)
	writeFile(t, filepath.Join(root, "layers", "0000-foundation.patch"), "foundation\n")

	if _, err := definition.Load(root); err == nil {
		t.Fatal("Load() error = nil, want mail patch layer error")
	}
}

func TestLoadRejectsMalformedLayerContents(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{
			name: "missing message",
			setup: func(t *testing.T, root string) {
				writeFile(t, filepath.Join(root, "layers", "0000-foundation", "patch"), "patch\n")
			},
		},
		{
			name: "empty message",
			setup: func(t *testing.T, root string) {
				writeFile(t, filepath.Join(root, "layers", "0000-foundation", "COMMIT_EDITMSG"), "")
			},
		},
		{
			name: "empty patch",
			setup: func(t *testing.T, root string) {
				writeLayer(t, root, "0000-foundation", false)
				writeFile(t, filepath.Join(root, "layers", "0000-foundation", "patch"), "")
			},
		},
		{
			name: "unknown entry",
			setup: func(t *testing.T, root string) {
				writeLayer(t, root, "0000-foundation", false)
				writeFile(t, filepath.Join(root, "layers", "0000-foundation", "extra"), "extra\n")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, filepath.Join(root, "upstream.json"), `{"tag":"rust-v1.2.3","commit":"abc123"}`)
			test.setup(t, root)
			if _, err := definition.Load(root); err == nil {
				t.Fatal("Load() error = nil, want malformed layer error")
			}
		})
	}
}

func TestLoadAllowsRepositoryWithoutProductLayers(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "upstream.json"), `{"tag":"rust-v1.2.3","commit":"abc123"}`)
	writeLayer(t, root, "0000-foundation", false)

	repository, err := definition.Load(root)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := len(repository.Layers), 1; got != want {
		t.Fatalf("len(Load().Layers) = %d, want %d", got, want)
	}
}

func writeLayer(t *testing.T, root, id string, withPatch bool) {
	t.Helper()
	directory := filepath.Join(root, "layers", id)
	writeFile(t, filepath.Join(directory, "COMMIT_EDITMSG"), id+"\n")
	if withPatch {
		writeFile(t, filepath.Join(directory, "patch"), "patch\n")
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

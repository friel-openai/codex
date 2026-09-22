package rewrite

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/friel-openai/codex/layerctl/internal/definition"
	"github.com/friel-openai/codex/layerctl/internal/gitrepo"
)

func TestRewriteLockOwnership(t *testing.T) {
	root := t.TempDir()
	release, err := acquireRewriteLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireRewriteLock(root); err == nil {
		t.Fatal("another rewrite acquired the same canonical root")
	}
	otherRelease, err := acquireRewriteLock(t.TempDir())
	if err != nil {
		t.Fatalf("independent canonical root blocked: %v", err)
	}
	if err := otherRelease(); err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, ".layerctl-rewrite.lock")); !os.IsNotExist(err) {
		t.Fatalf("released lock still exists: %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("second release: %v", err)
	}
	release, err = acquireRewriteLock(root)
	if err != nil {
		t.Fatalf("released root cannot be acquired: %v", err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
}

func TestRewriteLockRetainsReplacement(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "symlink"}[symlink], func(t *testing.T) {
			root := t.TempDir()
			release, err := acquireRewriteLock(root)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, ".layerctl-rewrite.lock")
			if err := os.Rename(path, path+".original"); err != nil {
				t.Fatal(err)
			}
			if symlink {
				if err := os.Symlink(path+".original", path); err != nil {
					t.Fatal(err)
				}
			} else {
				writeTestFile(t, path, []byte("other owner\n"), 0o600)
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := release(); err == nil {
				t.Fatal("release accepted a replacement lock")
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("replacement lock removed or changed: %v", err)
			}
		})
	}
}

func TestRewriteLockRejectsSymlinkRoot(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(t.TempDir(), "canonical")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireRewriteLock(alias); err == nil {
		t.Fatal("rewrite lock followed a symlink root")
	}
	if _, err := os.Lstat(filepath.Join(root, ".layerctl-rewrite.lock")); !os.IsNotExist(err) {
		t.Fatalf("symlink root rejection created a lock: %v", err)
	}
}

func TestRewriteLockRetainsPreexistingLock(t *testing.T) {
	for _, kind := range []string{"file", "directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, ".layerctl-rewrite.lock")
			switch kind {
			case "file":
				writeTestFile(t, path, []byte("other owner\n"), 0o600)
			case "directory":
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(root, "missing"), path); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := acquireRewriteLock(root); err == nil {
				t.Fatal("preexisting rewrite lock accepted")
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("preexisting rewrite lock removed or changed: %v", err)
			}
		})
	}
}

func TestRequireBackupUnchanged(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, root, backup string)
	}{
		{name: "unchanged"},
		{name: "message bytes", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(backup, "0001-source", "COMMIT_EDITMSG"), []byte("Manual edit\n"), 0o644)
		}},
		{name: "patch bytes", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(backup, "0001-source", "patch"), []byte("Manual patch\n"), 0o644)
		}},
		{name: "upstream semantics", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(root, "upstream.json"), []byte(`{"tag":"other","commit":"abc"}`), 0o644)
		}},
		{name: "upstream bytes", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(root, "upstream.json"), []byte(`{ "tag": "v-test", "commit": "abc" }`), 0o644)
		}},
		{name: "upstream extra JSON", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(root, "upstream.json"), []byte(`{"tag":"v-test","commit":"abc"} {}`), 0o644)
		}},
		{name: "upstream unknown field", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(root, "upstream.json"), []byte(`{"tag":"v-test","commit":"abc","other":true}`), 0o644)
		}},
		{name: "message mode", mutate: func(t *testing.T, root, backup string) {
			if err := os.Chmod(filepath.Join(backup, "0001-source", "COMMIT_EDITMSG"), 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "patch mode", mutate: func(t *testing.T, root, backup string) {
			if err := os.Chmod(filepath.Join(backup, "0001-source", "patch"), 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "upstream mode", mutate: func(t *testing.T, root, backup string) {
			if err := os.Chmod(filepath.Join(root, "upstream.json"), 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "extra unit", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(backup, "0002-extra", "COMMIT_EDITMSG"), []byte("Extra\n"), 0o644)
		}},
		{name: "extra file", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(backup, "0001-source", "extra"), []byte("Extra\n"), 0o644)
		}},
		{name: "missing unit", mutate: func(t *testing.T, root, backup string) {
			if err := os.Rename(filepath.Join(backup, "0001-source"), filepath.Join(root, "source-away")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing message", mutate: func(t *testing.T, root, backup string) {
			if err := os.Remove(filepath.Join(backup, "0001-source", "COMMIT_EDITMSG")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing patch", mutate: func(t *testing.T, root, backup string) {
			if err := os.Remove(filepath.Join(backup, "0001-source", "patch")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "added patch", mutate: func(t *testing.T, root, backup string) {
			writeTestFile(t, filepath.Join(backup, "0000-foundation", "patch"), []byte("Extra\n"), 0o644)
		}},
		{name: "invalid unit ID", mutate: func(t *testing.T, root, backup string) {
			if err := os.Rename(filepath.Join(backup, "0001-source"), filepath.Join(backup, "source")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unit symlink", mutate: func(t *testing.T, root, backup string) {
			replaceWithTestSymlink(t, filepath.Join(backup, "0001-source"))
		}},
		{name: "message symlink", mutate: func(t *testing.T, root, backup string) {
			replaceWithTestSymlink(t, filepath.Join(backup, "0001-source", "COMMIT_EDITMSG"))
		}},
		{name: "patch symlink", mutate: func(t *testing.T, root, backup string) {
			replaceWithTestSymlink(t, filepath.Join(backup, "0001-source", "patch"))
		}},
		{name: "upstream symlink", mutate: func(t *testing.T, root, backup string) {
			replaceWithTestSymlink(t, filepath.Join(root, "upstream.json"))
		}},
		{name: "backup symlink", mutate: func(t *testing.T, root, backup string) {
			replaceWithTestSymlink(t, backup)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			writeTestFile(t, filepath.Join(root, "upstream.json"), []byte("{\"tag\":\"v-test\",\"commit\":\"abc\"}\n"), 0o644)
			writeTestFile(t, filepath.Join(root, "layers", "0000-foundation", "COMMIT_EDITMSG"), []byte("Foundation\n"), 0o644)
			writeTestFile(t, filepath.Join(root, "layers", "0001-source", "COMMIT_EDITMSG"), []byte("Source\n"), 0o644)
			writeTestFile(t, filepath.Join(root, "layers", "0001-source", "patch"), []byte("Patch\n"), 0o644)
			def, err := definition.Load(root)
			if err != nil {
				t.Fatal(err)
			}
			service := &Service{Definition: def, Git: &gitrepo.Repository{}}
			input, err := service.readInputs()
			if err != nil {
				t.Fatal(err)
			}
			backup := filepath.Join(root, "layers-backup")
			if err := os.Rename(filepath.Join(root, "layers"), backup); err != nil {
				t.Fatal(err)
			}
			if test.mutate != nil {
				test.mutate(t, root, backup)
			}
			err = service.requireBackupUnchanged(input, backup)
			if test.mutate == nil && err != nil {
				t.Fatalf("matching backup rejected: %v", err)
			}
			if test.mutate != nil && err == nil {
				t.Fatal("changed backup accepted")
			}
		})
	}
}

func replaceWithTestSymlink(t *testing.T, path string) {
	t.Helper()
	other := filepath.Join(t.TempDir(), filepath.Base(path))
	if err := os.Rename(path, other); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, path); err != nil {
		t.Fatal(err)
	}
}

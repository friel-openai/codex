package rewrite

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplaceLayersFailureAtomicity(t *testing.T) {
	for _, test := range []struct {
		name        string
		failBackup  bool
		failStage   bool
		failRestore bool
	}{
		{name: "success"},
		{name: "backup-error", failBackup: true},
		{name: "install-error-restores-originals", failStage: true},
		{name: "restore-error-retains-backup", failStage: true, failRestore: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			target, stage, backup := filepath.Join(root, "layers"), filepath.Join(root, "stage"), filepath.Join(root, "backup")
			writeTestFile(t, filepath.Join(target, "0000-foundation", "COMMIT_EDITMSG"), []byte("original message\n"), 0o640)
			writeTestFile(t, filepath.Join(stage, "0000-foundation", "COMMIT_EDITMSG"), []byte("regenerated message\n"), 0o644)
			if err := os.Chmod(target, 0o750); err != nil {
				t.Fatal(err)
			}
			original := canonicalFiles(t, target)
			backupErr, stageErr, restoreErr := errors.New("backup failed"), errors.New("install failed"), errors.New("restore failed")
			var calls [][2]string
			rename := func(from, to string) error {
				calls = append(calls, [2]string{from, to})
				switch {
				case from == target && test.failBackup:
					return backupErr
				case from == stage && test.failStage:
					return stageErr
				case from == backup && test.failRestore:
					return restoreErr
				default:
					return os.Rename(from, to)
				}
			}
			retained, err := replaceLayers(stage, target, backup, rename, nil)
			switch {
			case test.failRestore:
				if !retained || !errors.Is(err, stageErr) || !strings.Contains(err.Error(), backup) || !strings.Contains(err.Error(), restoreErr.Error()) {
					t.Fatalf("replaceLayers = %v, %v; want retained backup and both errors", retained, err)
				}
				assertCanonicalFiles(t, backup, original)
				if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("target unexpectedly published after restore failure: %v", err)
				}
			case test.failStage:
				if retained || !errors.Is(err, stageErr) {
					t.Fatalf("replaceLayers = %v, %v; want install error and successful restoration", retained, err)
				}
				assertCanonicalFiles(t, target, original)
				if _, err := os.Lstat(backup); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("restored backup still exists: %v", err)
				}
			case test.failBackup:
				if retained || !errors.Is(err, backupErr) || len(calls) != 1 {
					t.Fatalf("replaceLayers = %v, %v, %v; want no installation attempt", retained, err, calls)
				}
				assertCanonicalFiles(t, target, original)
			default:
				if retained || err != nil || len(calls) != 2 {
					t.Fatalf("replaceLayers = %v, %v, %v; want installed candidate", retained, err, calls)
				}
				assertCanonicalFiles(t, backup, original)
				if got := readTestFile(t, filepath.Join(target, "0000-foundation", "COMMIT_EDITMSG")); string(got) != "regenerated message\n" {
					t.Fatalf("installed message = %q", got)
				}
				if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("installed stage still exists: %v", err)
				}
			}
		})
	}
}

func TestReplaceLayersRejectsEditDuringRename(t *testing.T) {
	fixture := independentRewriteFixture(t)
	input, err := fixture.service.freezeInputs()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(input.root)
	target := filepath.Join(fixture.service.Definition.Root, "layers")
	stage, backup := filepath.Join(fixture.service.Definition.Root, "stage"), filepath.Join(fixture.service.Definition.Root, "backup")
	writeTestFile(t, filepath.Join(stage, "0000-foundation", "COMMIT_EDITMSG"), []byte("regenerated\n"), 0o644)
	changedMessage := []byte("manual edit made after the final digest check\n")
	var calls [][2]string
	rename := func(from, to string) error {
		calls = append(calls, [2]string{from, to})
		if from == target {
			writeTestFile(t, filepath.Join(target, "0000-foundation", "COMMIT_EDITMSG"), changedMessage, 0o644)
		}
		return os.Rename(from, to)
	}
	retained, err := replaceLayers(stage, target, backup, rename, func() error {
		return fixture.service.requireBackupUnchanged(input, backup)
	})
	if retained || err == nil || len(calls) != 2 || calls[1] != [2]string{backup, target} {
		t.Fatalf("replaceLayers = %v, %v, %v; want changed-input rejection and restoration without publication", retained, err, calls)
	}
	if got := readTestFile(t, filepath.Join(target, "0000-foundation", "COMMIT_EDITMSG")); string(got) != string(changedMessage) {
		t.Fatalf("restoration discarded the manual edit: %q", got)
	}
	if got := readTestFile(t, filepath.Join(stage, "0000-foundation", "COMMIT_EDITMSG")); string(got) != "regenerated\n" {
		t.Fatalf("unpublished staged definition changed: %q", got)
	}
}

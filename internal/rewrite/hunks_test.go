package rewrite

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const hunkOldID = "1111111111111111111111111111111111111111"
const hunkNewID = "2222222222222222222222222222222222222222"
const hunkZeroID = "0000000000000000000000000000000000000000"

func hunkTextHeader(path, mode string) string {
	return "diff --git a/" + path + " b/" + path + "\nindex " + hunkOldID + ".." + hunkNewID + " " + mode + "\n--- a/" + path + "\n+++ b/" + path + "\n"
}

func TestParseHunksSplitsTextAndPreservesBytes(t *testing.T) {
	header := hunkTextHeader("file", "100644")
	first := "@@ -1,2 +1,2 @@ first function\n context\n-old\n+new\n"
	second := "@@ -10 +10 @@ second function\n-before\n+after\n"
	hunks, err := parseHunks([]byte(header + first + second))
	if err != nil {
		t.Fatal(err)
	}
	if len(hunks) != 2 {
		t.Fatalf("got %d hunks, want 2", len(hunks))
	}
	for i, body := range []string{first, second} {
		want := []byte(header + body)
		if !bytes.Equal(hunks[i].Patch, want) {
			t.Fatalf("hunk %d patch = %q, want %q", i, hunks[i].Patch, want)
		}
		hash := sha256.Sum256(want)
		if hunks[i].ID != hex.EncodeToString(hash[:]) {
			t.Fatalf("hunk %d ID does not hash its exact file header and content", i)
		}
		if hunks[i].Header != strings.SplitN(body, "\n", 2)[0] {
			t.Fatalf("hunk %d header = %q", i, hunks[i].Header)
		}
	}
	unchanged, err := parseHunks([]byte(header + second))
	if err != nil {
		t.Fatal(err)
	}
	if unchanged[0].ID != hunks[1].ID {
		t.Fatal("hunk ID depends on earlier hunks")
	}
	renamed, err := parseHunks([]byte(hunkTextHeader("other", "100644") + second))
	if err != nil {
		t.Fatal(err)
	}
	if renamed[0].ID == hunks[1].ID {
		t.Fatal("hunk ID does not include its file header")
	}
}

func TestParseHunksAtomicChanges(t *testing.T) {
	body := "@@ -1 +1 @@\n-old\n+new\n@@ -10 +10 @@\n-before\n+after\n"
	cases := map[string]string{
		"mode only":     "diff --git a/file b/file\nold mode 100644\nnew mode 100755\n",
		"mode and text": "diff --git a/file b/file\nold mode 100644\nnew mode 100755\nindex " + hunkOldID + ".." + hunkNewID + "\n--- a/file\n+++ b/file\n" + body,
		"symlink":       hunkTextHeader("file", "120000") + body,
		"submodule":     hunkTextHeader("file", "160000") + "@@ -1 +1 @@\n-Subproject commit " + hunkOldID + "\n+Subproject commit " + hunkNewID + "\n",
		"new text":      "diff --git a/file b/file\nnew file mode 100644\nindex " + hunkZeroID + ".." + hunkNewID + "\n--- /dev/null\n+++ b/file\n@@ -0,0 +1,2 @@\n+first\n+second\n",
		"deleted text":  "diff --git a/file b/file\ndeleted file mode 100644\nindex " + hunkOldID + ".." + hunkZeroID + "\n--- a/file\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-first\n-second\n",
		"new empty":     "diff --git a/file b/file\nnew file mode 100644\nindex " + hunkZeroID + ".." + hunkNewID + "\n",
		"deleted empty": "diff --git a/file b/file\ndeleted file mode 100644\nindex " + hunkOldID + ".." + hunkZeroID + "\n",
		"binary":        "diff --git a/file b/file\nindex " + hunkOldID + ".." + hunkNewID + " 100644\nGIT binary patch\n" + hunkBinaryBlock(t, []byte{0, 1, 2}) + hunkBinaryBlock(t, []byte{0, 3, 4}),
	}
	for name, patch := range cases {
		t.Run(name, func(t *testing.T) {
			hunks, err := parseHunks([]byte(patch))
			if err != nil {
				t.Fatal(err)
			}
			if len(hunks) != 1 || !bytes.Equal(hunks[0].Patch, []byte(patch)) {
				t.Fatalf("atomic patch changed or split: %#v", hunks)
			}
			if hunks[0].Header != "diff --git a/file b/file" {
				t.Fatalf("atomic header = %q", hunks[0].Header)
			}
		})
	}
}

func TestParseHunksMissingFinalNewlinesAndBlankContext(t *testing.T) {
	header := hunkTextHeader("file", "100644")
	for _, body := range []string{
		"@@ -1 +1 @@\n-old\n\\ No newline at end of file\n+new\n\\ No newline at end of file\n",
		"@@ -1,2 +1,2 @@\n\n-old\n+new\n",
		"@@ -1,2 +1,2 @@\n-old\n+new\n last\n\\ No newline at end of file\n",
		"@@ -3,0 +4 @@\n+inserted\n",
		"@@ -4 +3,0 @@\n-deleted\n",
	} {
		hunks, err := parseHunks([]byte(header + body))
		if err != nil {
			t.Fatalf("body %q: %v", body, err)
		}
		if len(hunks) != 1 || string(hunks[0].Patch) != header+body {
			t.Fatalf("body %q was not preserved", body)
		}
	}
}

func TestParseHunksRejectsMalformedPatches(t *testing.T) {
	header := hunkTextHeader("file", "100644")
	valid := header + "@@ -1 +1 @@\n-old\n+new\n"
	binary := "diff --git a/file b/file\nindex " + hunkOldID + ".." + hunkNewID + " 100644\nGIT binary patch\n"
	cases := map[string]string{
		"missing terminal newline": strings.TrimSuffix(valid, "\n"),
		"unexpected preamble":      "commit deadbeef\n" + valid,
		"abbreviated index":        strings.Replace(valid, hunkOldID, "1111111", 1),
		"missing index mode":       strings.Replace(valid, " 100644\n", "\n", 1),
		"unsupported mode":         strings.ReplaceAll(valid, "100644", "100600"),
		"rename":                   strings.Replace(valid, "b/file\nindex", "b/other\nindex", 1),
		"mismatched path":          strings.Replace(valid, "+++ b/file", "+++ b/other", 1),
		"zero old object":          strings.Replace(valid, hunkOldID, hunkZeroID, 1),
		"missing hunk":             header,
		"too many lines":           header + "@@ -1 +1 @@\n-old\n+new\n+extra\n",
		"too few lines":            header + "@@ -1,2 +1 @@\n-old\n+new\n",
		"bad header":               header + "@@ -x +1 @@\n-old\n+new\n",
		"zero line position":       header + "@@ -0 +0 @@\n-old\n+new\n",
		"empty change":             header + "@@ -0,0 +0,0 @@\n",
		"context only":             header + "@@ -1 +1 @@\n same\n",
		"overflow range":           header + "@@ -9223372036854775807,2 +1 @@\n-old\n+new\n",
		"overlap":                  valid + "@@ -1 +1 @@\n-old\n+new\n",
		"unequal unchanged gaps":   valid + "@@ -10 +11 @@\n-old\n+new\n",
		"orphan newline marker":    header + "@@ -1 +1 @@\n\\ No newline at end of file\n-old\n+new\n",
		"duplicate newline marker": valid + "\\ No newline at end of file\n\\ No newline at end of file\n",
		"old after newline marker": header + "@@ -1,2 +1 @@\n-old\n\\ No newline at end of file\n-more\n+new\n",
		"unknown content prefix":   header + "@@ -1 +1 @@\n?old\n+new\n",
		"unknown header":           strings.Replace(valid, "index ", "similarity index ", 1),
		"unpaired mode":            "diff --git a/file b/file\nold mode 100644\n",
		"unchanged mode":           "diff --git a/file b/file\nold mode 100644\nnew mode 100644\n",
		"truncated binary":         binary + hunkBinaryBlock(t, []byte{0, 1}),
		"bad binary base85":        binary + "literal 2\nA?????\n\n" + hunkBinaryBlock(t, []byte{0, 1}),
		"binary size mismatch":     binary + strings.Replace(hunkBinaryBlock(t, []byte{0, 1}), "literal 2", "literal 3", 1) + hunkBinaryBlock(t, []byte{0, 1}),
		"binary trailing content":  binary + hunkBinaryBlock(t, []byte{0, 1}) + hunkBinaryBlock(t, []byte{0, 1}) + "garbage\n",
	}
	for name, patch := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseHunks([]byte(patch)); err == nil {
				t.Fatalf("accepted malformed patch %q", patch)
			}
		})
	}
}

func TestParseHunksEmptyPatch(t *testing.T) {
	hunks, err := parseHunks(nil)
	if err != nil || len(hunks) != 0 {
		t.Fatalf("empty patch = %#v, %v", hunks, err)
	}
}

func TestParseHunksMultipleFilesAndSHA256Index(t *testing.T) {
	first := hunkTextHeader("first", "100755") + "@@ -1 +1 @@\n-old\n+new\n"
	second := hunkTextHeader("second", "100644") + "@@ -1 +1 @@\n-before\n+after\n"
	second = strings.ReplaceAll(second, hunkOldID, strings.Repeat("1", 64))
	second = strings.ReplaceAll(second, hunkNewID, strings.Repeat("2", 64))
	hunks, err := parseHunks([]byte(first + second))
	if err != nil {
		t.Fatal(err)
	}
	if len(hunks) != 2 || string(hunks[0].Patch) != first || string(hunks[1].Patch) != second {
		t.Fatalf("multi-file patches were not preserved: %#v", hunks)
	}
}

func TestParseHunksRejectsNonGitPathEscapes(t *testing.T) {
	for _, path := range []string{`"a/\x66ile"`, `"a/\u0066ile"`, `"a/\777ile"`, `"a/\000ile"`, `a/raw\backslash`} {
		header := "diff --git " + path + " " + strings.Replace(path, "a/", "b/", 1) + "\n"
		if _, err := parseHunks([]byte(header + "old mode 100644\nnew mode 100755\n")); err == nil {
			t.Fatalf("accepted non-Git path %q", path)
		}
	}
}

func TestParseHunksGitEscapedPaths(t *testing.T) {
	for _, path := range []string{"with spaces", "with b/a b/segments", "with\ttab", "with\nnewline", "with\"quote", "with\\backslash", "caf\xc3\xa9", "invalid\xff", "with\rreturn"} {
		t.Run(fmt.Sprintf("%q", path), func(t *testing.T) {
			repo := hunkGitRepo(t)
			hunkWrite(t, repo, path, []byte("old\n"))
			hunkGit(t, repo, "add", "--", path)
			hunkGit(t, repo, "commit", "-m", "old")
			hunkWrite(t, repo, path, []byte("new\n"))
			hunkGit(t, repo, "add", "--", path)
			hunkGit(t, repo, "commit", "-m", "new")
			patch := hunkGitPatch(t, repo)
			hunks, err := parseHunks(patch)
			if err != nil {
				t.Fatalf("Git patch %q: %v", patch, err)
			}
			if len(hunks) != 1 || !bytes.Equal(hunks[0].Patch, patch) {
				t.Fatal("Git escaped path patch was not preserved")
			}
		})
	}
}

func TestParseHunksCanonicalGitChanges(t *testing.T) {
	tests := []struct {
		name                                string
		before, after                       []byte
		added, deleted, executable, symlink bool
	}{
		{name: "two text hunks", before: []byte("old\n" + strings.Repeat("context\n", 20) + "old\n"), after: []byte("new\n" + strings.Repeat("context\n", 20) + "new\n")},
		{name: "blank context", before: []byte("\nold\n"), after: []byte("\nnew\n")},
		{name: "no final newline", before: []byte("old"), after: []byte("new")},
		{name: "new text", added: true, after: []byte("new\n")},
		{name: "deleted text", deleted: true, before: []byte("old\n")},
		{name: "new empty", added: true},
		{name: "deleted empty", deleted: true},
		{name: "mode only", executable: true, before: []byte("same\n"), after: []byte("same\n")},
		{name: "mode and text", executable: true, before: []byte("old\n"), after: []byte("new\n")},
		{name: "symlink", symlink: true, before: []byte("old-target"), after: []byte("new-target")},
		{name: "binary literal", before: []byte{0, 1, 2, 3}, after: []byte{0, 4, 5, 6}},
		{name: "binary delta", before: bytes.Repeat([]byte{0, 1, 2, 3}, 1000), after: append(bytes.Repeat([]byte{0, 1, 2, 3}, 999), 0, 4, 5, 6)},
		{name: "new binary", added: true, after: []byte{0, 4, 5, 6}},
		{name: "deleted binary", deleted: true, before: []byte{0, 1, 2, 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := hunkGitRepo(t)
			if !test.added {
				hunkWrite(t, repo, "file", test.before)
				if test.symlink {
					if err := os.Remove(filepath.Join(repo, "file")); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(string(test.before), filepath.Join(repo, "file")); err != nil {
						t.Fatal(err)
					}
				}
			}
			hunkGit(t, repo, "add", "-A")
			hunkGit(t, repo, "commit", "--allow-empty", "-m", "old")
			if test.deleted || test.symlink {
				if err := os.Remove(filepath.Join(repo, "file")); err != nil {
					t.Fatal(err)
				}
			}
			if !test.deleted {
				if test.symlink {
					if err := os.Symlink(string(test.after), filepath.Join(repo, "file")); err != nil {
						t.Fatal(err)
					}
				} else {
					hunkWrite(t, repo, "file", test.after)
					if test.executable {
						if err := os.Chmod(filepath.Join(repo, "file"), 0755); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			hunkGit(t, repo, "add", "-A")
			hunkGit(t, repo, "commit", "-m", "new")
			patch := hunkGitPatch(t, repo)
			hunks, err := parseHunks(patch)
			if err != nil {
				t.Fatalf("Git patch %q: %v", patch, err)
			}
			want := 1
			if test.name == "two text hunks" {
				want = 2
			}
			if len(hunks) != want {
				t.Fatalf("got %d hunks, want %d: %q", len(hunks), want, patch)
			}
			if want == 1 && !bytes.Equal(hunks[0].Patch, patch) {
				t.Fatal("atomic or single-hunk Git patch was not preserved")
			}
		})
	}
}

func TestParseHunksGitSubmodule(t *testing.T) {
	repo := hunkGitRepo(t)
	hunkGit(t, repo, "commit", "--allow-empty", "-m", "target one")
	old := strings.TrimSpace(string(hunkGit(t, repo, "rev-parse", "HEAD")))
	hunkGit(t, repo, "commit", "--allow-empty", "-m", "target two")
	new := strings.TrimSpace(string(hunkGit(t, repo, "rev-parse", "HEAD")))
	hunkGit(t, repo, "update-index", "--add", "--cacheinfo", "160000,"+old+",module")
	hunkGit(t, repo, "commit", "-m", "old submodule")
	hunkGit(t, repo, "update-index", "--add", "--cacheinfo", "160000,"+new+",module")
	hunkGit(t, repo, "commit", "-m", "new submodule")
	patch := hunkGitPatch(t, repo)
	hunks, err := parseHunks(patch)
	if err != nil {
		t.Fatal(err)
	}
	if len(hunks) != 1 || !bytes.Equal(hunks[0].Patch, patch) {
		t.Fatal("submodule Git patch was not kept atomic")
	}
}

func TestParseHunksGitFileTypeConversionsAreAtomic(t *testing.T) {
	for _, test := range []struct {
		name, oldMode, newMode string
	}{
		{name: "regular to symlink", oldMode: "100644", newMode: "120000"},
		{name: "symlink to regular", oldMode: "120000", newMode: "100644"},
		{name: "executable to symlink", oldMode: "100755", newMode: "120000"},
		{name: "regular to gitlink", oldMode: "100644", newMode: "160000"},
		{name: "gitlink to regular", oldMode: "160000", newMode: "100644"},
		{name: "symlink to gitlink", oldMode: "120000", newMode: "160000"},
		{name: "gitlink to symlink", oldMode: "160000", newMode: "120000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := hunkGitRepo(t)
			hunkGit(t, repo, "commit", "--allow-empty", "-m", "submodule target")
			target := strings.TrimSpace(string(hunkGit(t, repo, "rev-parse", "HEAD")))
			for i, mode := range []string{test.oldMode, test.newMode} {
				object := target
				if mode != "160000" {
					hunkWrite(t, repo, "object", []byte(fmt.Sprintf("version-%d\n", i)))
					object = strings.TrimSpace(string(hunkGit(t, repo, "hash-object", "-w", "--", "object")))
				}
				hunkGit(t, repo, "update-index", "--add", "--cacheinfo", mode+","+object+",file")
				hunkGit(t, repo, "commit", "-m", fmt.Sprintf("version %d", i))
			}
			patch := hunkGitPatch(t, repo)
			if bytes.Count(patch, []byte("diff --git a/file b/file\n")) != 2 {
				t.Fatalf("Git did not emit the expected two conversion sections: %q", patch)
			}
			hunks, err := parseHunks(patch)
			if err != nil {
				t.Fatalf("Git conversion patch %q: %v", patch, err)
			}
			if len(hunks) != 1 || !bytes.Equal(hunks[0].Patch, patch) {
				t.Fatalf("Git conversion was split or changed: %#v", hunks)
			}
			if hunks[0].Header != "diff --git a/file b/file" {
				t.Fatalf("conversion header = %q", hunks[0].Header)
			}
			hash := sha256.Sum256(patch)
			if hunks[0].ID != hex.EncodeToString(hash[:]) {
				t.Fatal("conversion ID does not hash both complete sections")
			}
		})
	}
}

func TestParseHunksRejectsRepeatedPathsExceptFileTypeConversions(t *testing.T) {
	deleted := "diff --git a/file b/file\ndeleted file mode 100644\nindex " + hunkOldID + ".." + hunkZeroID + "\n--- a/file\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n"
	added := "diff --git a/file b/file\nnew file mode 120000\nindex " + hunkZeroID + ".." + hunkNewID + "\n--- /dev/null\n+++ b/file\n@@ -0,0 +1 @@\n+new\n"
	text := hunkTextHeader("file", "100644") + "@@ -1 +1 @@\n-old\n+new\n"
	other := hunkTextHeader("other", "100644") + "@@ -1 +1 @@\n-before\n+after\n"
	for name, patch := range map[string]string{
		"repeated modification":           text + text,
		"same type deletion and addition": deleted + strings.Replace(added, "120000", "100755", 1),
		"addition before deletion":        added + deleted,
		"nonadjacent conversion":          deleted + other + added,
		"third section after conversion":  deleted + added + added,
		"second deletion":                 deleted + deleted,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseHunks([]byte(patch)); err == nil {
				t.Fatalf("accepted repeated-path patch %q", patch)
			}
		})
	}
	hunks, err := parseHunks([]byte(other + deleted + added))
	if err != nil {
		t.Fatal(err)
	}
	if len(hunks) != 2 || string(hunks[0].Patch) != other || string(hunks[1].Patch) != deleted+added {
		t.Fatalf("conversion after another file was not kept atomic: %#v", hunks)
	}
}

func hunkGitRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	hunkGit(t, repo, "init", "-b", "main")
	hunkGit(t, repo, "config", "user.name", "Parser Test")
	hunkGit(t, repo, "config", "user.email", "parser@example.invalid")
	hunkGit(t, repo, "config", "commit.gpgsign", "false")
	return repo
}

func hunkGit(t *testing.T, repo string, args ...string) []byte {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = repo
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return output
}

func hunkGitPatch(t *testing.T, repo string) []byte {
	t.Helper()
	return hunkGit(t, repo, "-c", "core.quotePath=true", "-c", "diff.suppressBlankEmpty=true", "diff-tree", "--no-commit-id", "--binary", "--full-index", "--no-renames", "--no-ext-diff", "--no-textconv", "--no-color", "-p", "HEAD^", "HEAD")
}

func hunkWrite(t *testing.T, repo, path string, content []byte) {
	t.Helper()
	fullPath := filepath.Join(repo, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, content, 0644); err != nil {
		t.Fatal(err)
	}
}

func hunkBinaryBlock(t *testing.T, data []byte) string {
	t.Helper()
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	var encoded strings.Builder
	fmt.Fprintf(&encoded, "literal %d\n", len(data))
	remaining := compressed.Bytes()
	for len(remaining) > 0 {
		count := min(52, len(remaining))
		prefix := byte('A' + count - 1)
		if count > 26 {
			prefix = byte('a' + count - 27)
		}
		encoded.WriteByte(prefix)
		line := remaining[:count]
		for len(line) > 0 {
			var chunk [4]byte
			n := min(4, len(line))
			copy(chunk[:], line[:n])
			value := uint32(chunk[0])<<24 | uint32(chunk[1])<<16 | uint32(chunk[2])<<8 | uint32(chunk[3])
			var digits [5]byte
			for i := 4; i >= 0; i-- {
				digits[i] = binaryAlphabet[value%85]
				value /= 85
			}
			encoded.Write(digits[:])
			line = line[n:]
		}
		encoded.WriteByte('\n')
		remaining = remaining[count:]
	}
	encoded.WriteByte('\n')
	return encoded.String()
}

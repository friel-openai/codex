package rewrite

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Hunk is an independently selectable patch. File metadata and non-regular
// files stay together because their changes cannot be split into text hunks.
type Hunk struct {
	// ID identifies the exact file header and hunk bytes, not its position.
	ID string `json:"id"`
	// Header is the unified hunk header, or the file header for an atomic patch.
	Header string `json:"header"`
	// Patch includes the file header required by git apply.
	Patch []byte `json:"-"`
}

var hunkHeader = regexp.MustCompile(`^@@ -([0-9]+)(?:,([0-9]+))? \+([0-9]+)(?:,([0-9]+))? @@(?: .*)?$`)
var indexHeader = regexp.MustCompile(`^index ([0-9a-f]{40}|[0-9a-f]{64})\.\.([0-9a-f]{40}|[0-9a-f]{64})(?: (100644|100755|120000|160000))?$`)

// parseHunks accepts the full-index, no-renames patch produced by git diff-tree.
// Each file starts with matching diff --git paths, optional new/deleted mode or
// an old/new mode pair, and a full index header. Text follows with matching
// ---/+++ paths and @@ ranges. Content prefixes are space, minus, and plus;
// diff.suppressBlankEmpty also permits bare blank context lines. Range counts
// must match content, and no-newline markers must follow the last content line
// for that file version. Paths use Git's C quoting or unquoted bytes, with an
// optional trailing tab delimiter in ---/+++ headers. Binary content consists
// of forward and reverse literal/delta blocks with base85-encoded zlib data.
// Empty-file creation/deletion and mode-only changes have no content blocks.
// Adjacent same-path deletion/creation sections with different file types form
// one atomic conversion. Other repeated paths are not canonical diff-tree output.
// Bytes are preserved so IDs and git apply operations use the same patch.
func parseHunks(patch []byte) ([]Hunk, error) {
	if len(patch) == 0 {
		return []Hunk{}, nil
	}
	if patch[len(patch)-1] != '\n' {
		return nil, fmt.Errorf("patch does not end with a newline")
	}
	lines := bytes.SplitAfter(patch, []byte{'\n'})
	lines = lines[:len(lines)-1]
	var hunks []Hunk
	seen := make(map[string]bool)
	previousPath, previousDeletedMode := "", ""
	for start := 0; start < len(lines); {
		if !bytes.HasPrefix(lines[start], []byte("diff --git ")) {
			return nil, fmt.Errorf("line %d: expected diff --git header", start+1)
		}
		end := start + 1
		for end < len(lines) && !bytes.HasPrefix(lines[end], []byte("diff --git ")) {
			end++
		}
		path, err := diffPath(patchLine(lines[start]))
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", start+1, err)
		}
		fileHunks, err := parseFileHunks(lines[start:end])
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", start+1, err)
		}
		addedMode, deletedMode := "", ""
		if start+1 < end {
			metadata := patchLine(lines[start+1])
			if strings.HasPrefix(metadata, "new file mode ") {
				addedMode = strings.TrimPrefix(metadata, "new file mode ")
			} else if strings.HasPrefix(metadata, "deleted file mode ") {
				deletedMode = strings.TrimPrefix(metadata, "deleted file mode ")
			}
		}
		if seen[path] {
			if previousPath != path || previousDeletedMode == "" || addedMode == "" || previousDeletedMode[:3] == addedMode[:3] {
				return nil, fmt.Errorf("line %d: repeated path %q is not a file type conversion", start+1, path)
			}
			// Git emits two sections when the file type changes. Both sections
			// must move together; the first three mode digits identify the type.
			previous := hunks[len(hunks)-1]
			combined := append(bytes.Clone(previous.Patch), fileHunks[0].Patch...)
			hunks[len(hunks)-1] = makeHunk(previous.Header, combined)
		} else {
			hunks = append(hunks, fileHunks...)
			seen[path] = true
		}
		previousPath, previousDeletedMode = path, deletedMode
		start = end
	}
	return hunks, nil
}

func patchLine(line []byte) string {
	return string(line[:len(line)-1])
}

func parseFileHunks(lines [][]byte) ([]Hunk, error) {
	path, err := diffPath(patchLine(lines[0]))
	if err != nil {
		return nil, err
	}
	i := 1
	oldMode, newMode := "", ""
	added, deleted := false, false
	if i < len(lines) {
		line := patchLine(lines[i])
		switch {
		case strings.HasPrefix(line, "new file mode "):
			newMode, added = strings.TrimPrefix(line, "new file mode "), true
			i++
		case strings.HasPrefix(line, "deleted file mode "):
			oldMode, deleted = strings.TrimPrefix(line, "deleted file mode "), true
			i++
		case strings.HasPrefix(line, "old mode "):
			oldMode = strings.TrimPrefix(line, "old mode ")
			i++
			if i == len(lines) || !strings.HasPrefix(patchLine(lines[i]), "new mode ") {
				return nil, fmt.Errorf("old mode requires new mode")
			}
			newMode = strings.TrimPrefix(patchLine(lines[i]), "new mode ")
			i++
		}
	}
	for _, mode := range []string{oldMode, newMode} {
		if mode != "" && !validMode(mode) {
			return nil, fmt.Errorf("invalid file mode %q", mode)
		}
	}
	modeChanged := !added && !deleted && oldMode != ""
	if modeChanged && oldMode == newMode {
		return nil, fmt.Errorf("old mode and new mode are equal")
	}
	hasIndex := false
	if i < len(lines) && strings.HasPrefix(patchLine(lines[i]), "index ") {
		match := indexHeader.FindStringSubmatch(patchLine(lines[i]))
		if match == nil || len(match[1]) != len(match[2]) {
			return nil, fmt.Errorf("invalid full-index header")
		}
		if added || deleted || modeChanged {
			if match[3] != "" {
				return nil, fmt.Errorf("index mode conflicts with file mode headers")
			}
		} else {
			if match[3] == "" {
				return nil, fmt.Errorf("index header requires a file mode")
			}
			oldMode, newMode = match[3], match[3]
		}
		oldZero := strings.Trim(match[1], "0") == ""
		newZero := strings.Trim(match[2], "0") == ""
		if oldZero != added || newZero != deleted {
			return nil, fmt.Errorf("index object IDs conflict with file creation or deletion")
		}
		hasIndex = true
		i++
	}
	atomic := added || deleted || modeChanged || oldMode == "120000" || oldMode == "160000"
	if i == len(lines) {
		if (added || deleted) && hasIndex || modeChanged && !hasIndex {
			return []Hunk{makeHunk(patchLine(lines[0]), bytes.Join(lines, nil))}, nil
		}
		return nil, fmt.Errorf("file patch has no content change")
	}
	if !hasIndex {
		return nil, fmt.Errorf("file content requires a full-index header")
	}
	if patchLine(lines[i]) == "GIT binary patch" {
		if err := validateBinary(lines[i+1:]); err != nil {
			return nil, err
		}
		return []Hunk{makeHunk(patchLine(lines[0]), bytes.Join(lines, nil))}, nil
	}
	if i+1 >= len(lines) || !strings.HasPrefix(patchLine(lines[i]), "--- ") || !strings.HasPrefix(patchLine(lines[i+1]), "+++ ") {
		return nil, fmt.Errorf("expected old and new file headers")
	}
	oldPath, err := unquotePath(strings.TrimSuffix(strings.TrimPrefix(patchLine(lines[i]), "--- "), "\t"))
	if err != nil {
		return nil, fmt.Errorf("invalid old file path: %w", err)
	}
	newPath, err := unquotePath(strings.TrimSuffix(strings.TrimPrefix(patchLine(lines[i+1]), "+++ "), "\t"))
	if err != nil {
		return nil, fmt.Errorf("invalid new file path: %w", err)
	}
	wantOld, wantNew := "a/"+path, "b/"+path
	if added {
		wantOld = "/dev/null"
	}
	if deleted {
		wantNew = "/dev/null"
	}
	if oldPath != wantOld || newPath != wantNew {
		return nil, fmt.Errorf("file headers do not match diff --git paths")
	}
	i += 2
	fileHeader := bytes.Join(lines[:i], nil)
	var hunks []Hunk
	previousOld, previousNew := 0, 0
	oldEnded, newEnded := false, false
	for i < len(lines) {
		header := patchLine(lines[i])
		match := hunkHeader.FindStringSubmatch(header)
		if match == nil {
			return nil, fmt.Errorf("invalid unified hunk header %q", header)
		}
		oldStart, oldCount, err := hunkRange(match[1], match[2])
		if err != nil {
			return nil, err
		}
		newStart, newCount, err := hunkRange(match[3], match[4])
		if err != nil {
			return nil, err
		}
		if added && (oldStart != 0 || oldCount != 0) || deleted && (newStart != 0 || newCount != 0) {
			return nil, fmt.Errorf("hunk ranges conflict with file creation or deletion")
		}
		if oldCount == 0 && newCount == 0 || oldStart < previousOld || newStart < previousNew || oldStart-previousOld != newStart-previousNew {
			return nil, fmt.Errorf("hunk ranges overlap or disagree on unchanged lines")
		}
		previousOld, previousNew = oldStart+oldCount, newStart+newCount
		start := i
		i++
		oldLines, newLines, last := 0, 0, byte(0)
		changed := false
		for i < len(lines) && !bytes.HasPrefix(lines[i], []byte("@@ ")) {
			line := patchLine(lines[i])
			if line == "\\ No newline at end of file" {
				if last == 0 {
					return nil, fmt.Errorf("no-newline marker does not follow a content line")
				}
				oldEnded = oldEnded || last == '-' || last == ' '
				newEnded = newEnded || last == '+' || last == ' '
				last = 0
				i++
				continue
			}
			if len(line) != 0 && line[0] != ' ' && line[0] != '-' && line[0] != '+' {
				return nil, fmt.Errorf("invalid unified hunk content %q", line)
			}
			last = ' '
			if len(line) != 0 {
				last = line[0]
			}
			if last != '+' {
				if oldEnded {
					return nil, fmt.Errorf("old content follows its no-newline marker")
				}
				oldLines++
			}
			if last != '-' {
				if newEnded {
					return nil, fmt.Errorf("new content follows its no-newline marker")
				}
				newLines++
			}
			changed = changed || last != ' '
			if oldLines > oldCount || newLines > newCount {
				return nil, fmt.Errorf("hunk content exceeds its declared line counts")
			}
			i++
		}
		if oldLines != oldCount || newLines != newCount || !changed {
			return nil, fmt.Errorf("hunk content does not match its declared change")
		}
		content := append(bytes.Clone(fileHeader), bytes.Join(lines[start:i], nil)...)
		hunks = append(hunks, makeHunk(header, content))
	}
	if len(hunks) == 0 {
		return nil, fmt.Errorf("text file patch has no hunks")
	}
	if atomic {
		return []Hunk{makeHunk(patchLine(lines[0]), bytes.Join(lines, nil))}, nil
	}
	return hunks, nil
}

func makeHunk(header string, patch []byte) Hunk {
	hash := sha256.Sum256(patch)
	return Hunk{ID: hex.EncodeToString(hash[:]), Header: header, Patch: patch}
}

func validMode(mode string) bool {
	return mode == "100644" || mode == "100755" || mode == "120000" || mode == "160000"
}

func hunkRange(startText, countText string) (int, int, error) {
	start, err := strconv.Atoi(startText)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid hunk start %q", startText)
	}
	count := 1
	if countText != "" {
		count, err = strconv.Atoi(countText)
	}
	if err != nil || count > int(^uint(0)>>1)-start || count != 0 && start == 0 {
		return 0, 0, fmt.Errorf("invalid hunk range")
	}
	if count != 0 {
		start--
	}
	return start, count, nil
}

func diffPath(header string) (string, error) {
	paths := strings.TrimPrefix(header, "diff --git ")
	if strings.HasPrefix(paths, `"`) {
		quoted, err := strconv.QuotedPrefix(paths)
		if err != nil || len(paths) <= len(quoted) || paths[len(quoted)] != ' ' {
			return "", fmt.Errorf("invalid quoted diff --git paths")
		}
		oldPath, err := unquotePath(quoted)
		if err != nil {
			return "", err
		}
		newPath, err := unquotePath(paths[len(quoted)+1:])
		if err == nil && strings.HasPrefix(oldPath, "a/") && newPath == "b/"+oldPath[2:] && len(oldPath) > 2 {
			return oldPath[2:], nil
		}
		return "", fmt.Errorf("diff --git paths must match without renames")
	}
	if strings.HasPrefix(paths, "a/") {
		for i := 2; i < len(paths); i++ {
			if strings.HasPrefix(paths[i:], " b/") && paths[i+3:] == paths[2:i] && i > 2 {
				if _, err := unquotePath(paths[:i]); err == nil {
					return paths[2:i], nil
				}
			}
		}
	}
	return "", fmt.Errorf("diff --git paths must match without renames")
}

func unquotePath(path string) (string, error) {
	if strings.HasPrefix(path, `"`) {
		for i := 1; i < len(path)-1; i++ {
			if path[i] != '\\' {
				continue
			}
			i++
			if i >= len(path)-1 {
				return "", fmt.Errorf("invalid Git quoted path %q", path)
			}
			if strings.IndexByte("abtnvfr\\\"", path[i]) >= 0 {
				continue
			}
			if i+2 >= len(path)-1 || path[i] < '0' || path[i] > '3' || path[i+1] < '0' || path[i+1] > '7' || path[i+2] < '0' || path[i+2] > '7' {
				return "", fmt.Errorf("invalid Git path escape in %q", path)
			}
			i += 2
		}
		decoded, err := strconv.Unquote(path)
		if err != nil || strings.IndexByte(decoded, 0) >= 0 {
			return "", fmt.Errorf("invalid Git quoted path %q", path)
		}
		return decoded, nil
	}
	if path == "" || strings.ContainsAny(path, "\t\r\n\x00\\\"") {
		return "", fmt.Errorf("invalid Git path %q", path)
	}
	return path, nil
}

const binaryAlphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~"

func validateBinary(lines [][]byte) error {
	for block := 0; block < 2; block++ {
		if len(lines) == 0 {
			return fmt.Errorf("binary patch requires forward and reverse blocks")
		}
		header := strings.Split(patchLine(lines[0]), " ")
		if len(header) != 2 || header[0] != "literal" && header[0] != "delta" {
			return fmt.Errorf("invalid binary patch block header")
		}
		size, err := strconv.ParseInt(header[1], 10, 64)
		if err != nil || size < 0 || size == int64(^uint64(0)>>1) || strings.HasPrefix(header[1], "+") {
			return fmt.Errorf("invalid binary patch block size")
		}
		lines = lines[1:]
		var compressed []byte
		for len(lines) > 0 && patchLine(lines[0]) != "" {
			line := patchLine(lines[0])
			length := 0
			if line[0] >= 'A' && line[0] <= 'Z' {
				length = int(line[0]-'A') + 1
			} else if line[0] >= 'a' && line[0] <= 'z' {
				length = int(line[0]-'a') + 27
			}
			if length == 0 || len(line)-1 != (length+3)/4*5 {
				return fmt.Errorf("invalid binary patch encoded line length")
			}
			decoded := make([]byte, 0, (length+3)/4*4)
			for i := 1; i < len(line); i += 5 {
				value := uint64(0)
				for _, character := range []byte(line[i : i+5]) {
					digit := strings.IndexByte(binaryAlphabet, character)
					if digit < 0 {
						return fmt.Errorf("invalid binary patch base85 character")
					}
					value = value*85 + uint64(digit)
				}
				if value > 0xffffffff {
					return fmt.Errorf("binary patch base85 value overflows")
				}
				decoded = append(decoded, byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
			}
			compressed = append(compressed, decoded[:length]...)
			lines = lines[1:]
		}
		if len(lines) == 0 || len(compressed) == 0 {
			return fmt.Errorf("binary patch block requires data and a terminating blank line")
		}
		lines = lines[1:]
		input := bytes.NewReader(compressed)
		reader, err := zlib.NewReader(input)
		if err != nil {
			return fmt.Errorf("invalid binary patch compressed data: %w", err)
		}
		actualSize, err := io.Copy(io.Discard, io.LimitReader(reader, size+1))
		closeErr := reader.Close()
		if err != nil || closeErr != nil || actualSize != size || input.Len() != 0 {
			return fmt.Errorf("binary patch compressed data does not match its declared size")
		}
	}
	if len(lines) != 0 {
		return fmt.Errorf("unexpected content after binary patch blocks")
	}
	return nil
}

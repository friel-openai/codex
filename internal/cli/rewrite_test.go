package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/friel-openai/codex/layerctl/internal/rewrite"
)

// fakeRewrite records requests so CLI tests can distinguish parsing from replay.
type fakeRewrite struct {
	move    rewrite.MoveRequest
	regroup rewrite.RegroupRequest
	batch   rewrite.RedistributionRequest
	err     error
}

func (s *fakeRewrite) Hunks(id string) ([]rewrite.Hunk, error) {
	return []rewrite.Hunk{{ID: id, Header: "file header", Patch: []byte("@@ patch\n")}}, s.err
}
func (s *fakeRewrite) Move(_ context.Context, request rewrite.MoveRequest) (rewrite.Result, error) {
	s.move = request
	return rewrite.Result{BeforeTree: "same-tree", AfterTree: "same-tree", DryRun: request.DryRun}, s.err
}
func (s *fakeRewrite) Regroup(_ context.Context, request rewrite.RegroupRequest) (rewrite.Result, error) {
	s.regroup = request
	return rewrite.Result{BeforeTree: "same-tree", AfterTree: "same-tree", DryRun: request.DryRun}, s.err
}

func (s *fakeRewrite) Redistribute(_ context.Context, request rewrite.RedistributionRequest) (rewrite.Result, error) {
	s.batch = request
	return rewrite.Result{BeforeTree: "same-tree", AfterTree: "same-tree", DryRun: request.DryRun}, s.err
}

func TestRewriteRejectsInvalidArgumentsBeforeReplay(t *testing.T) {
	for _, args := range [][]string{
		{"hunks"}, {"hunks", "--json"}, {"hunks", "0001-feature", "extra"},
		{"move"}, {"move", "from", "--all"}, {"move", "from", "to"},
		{"move", "from", "to", "--all", "--hunk", "abc"},
		{"move", "from", "to", "--hunk", ""},
		{"move", "from", "to", "--all", "extra"},
		{"regroup"}, {"regroup", "--plan", ""}, {"regroup", "extra"},
		{"redistribute"}, {"redistribute", "--plan", ""}, {"redistribute", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			err := runRewrite(t.Context(), nil, args, new(bytes.Buffer), new(bytes.Buffer))
			if err == nil {
				t.Fatal("invalid arguments accepted")
			}
		})
	}
}

func TestRewriteMoveParsesRepeatedSelectorsAndReportsTree(t *testing.T) {
	service := new(fakeRewrite)
	var stdout bytes.Buffer
	err := runRewrite(t.Context(), service, []string{"move", "from", "to", "--hunk", "abc", "--hunk", "def", "--dry-run"}, &stdout, new(bytes.Buffer))
	if err != nil {
		t.Fatal(err)
	}
	want := rewrite.MoveRequest{From: "from", To: "to", Hunks: []string{"abc", "def"}, DryRun: true}
	if !reflect.DeepEqual(service.move, want) {
		t.Fatalf("request = %#v, want %#v", service.move, want)
	}
	var result rewrite.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.BeforeTree != result.AfterTree || !result.DryRun {
		t.Fatalf("result = %s, error = %v", stdout.String(), err)
	}
}

func TestRewriteMoveAll(t *testing.T) {
	service := new(fakeRewrite)
	if err := runRewrite(t.Context(), service, []string{"move", "from", "to", "--all"}, new(bytes.Buffer), new(bytes.Buffer)); err != nil {
		t.Fatal(err)
	}
	if !service.move.All || len(service.move.Hunks) != 0 {
		t.Fatalf("request = %#v", service.move)
	}
}

func TestRewriteHunksJSONContainsReadablePatch(t *testing.T) {
	var stdout bytes.Buffer
	if err := runRewrite(t.Context(), new(fakeRewrite), []string{"hunks", "0001-feature", "--json"}, &stdout, new(bytes.Buffer)); err != nil {
		t.Fatal(err)
	}
	var hunks []map[string]string
	if err := json.Unmarshal(stdout.Bytes(), &hunks); err != nil {
		t.Fatal(err)
	}
	if len(hunks) != 1 || hunks[0]["patch"] != "@@ patch\n" || hunks[0]["id"] != "0001-feature" {
		t.Fatalf("hunks = %#v", hunks)
	}
}

func TestRewriteRegroupStrictJSON(t *testing.T) {
	for name, content := range map[string]string{
		"unknown root field":  `{"groups":[],"typo":true}`,
		"unknown group field": `{"groups":[{"id":"0000-foundation","typo":true}]}`,
		"trailing JSON":       `{"groups":[]} {}`,
		"trailing garbage":    `{"groups":[]} invalid`,
		"invalid JSON":        `{`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "plan.json")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := runRewrite(t.Context(), nil, []string{"regroup", "--plan", path}, new(bytes.Buffer), new(bytes.Buffer)); err == nil {
				t.Fatal("invalid JSON accepted")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, []byte(`{"groups":[{"id":"0000-foundation","sources":["0000-foundation"],"message":"Foundation\n"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	service := new(fakeRewrite)
	if err := runRewrite(t.Context(), service, []string{"regroup", "--plan", path, "--dry-run"}, new(bytes.Buffer), new(bytes.Buffer)); err != nil {
		t.Fatal(err)
	}
	if !service.regroup.DryRun || len(service.regroup.Groups) != 1 || service.regroup.Groups[0].Message != "Foundation\n" {
		t.Fatalf("request = %#v", service.regroup)
	}
}

func TestRewriteDoesNotReportSuccessOnServiceFailure(t *testing.T) {
	want := errors.New("final tree differs")
	var stdout bytes.Buffer
	err := runRewrite(t.Context(), &fakeRewrite{err: want}, []string{"move", "from", "to", "--all"}, &stdout, new(bytes.Buffer))
	if !errors.Is(err, want) || stdout.Len() != 0 {
		t.Fatalf("error = %v, stdout = %s", err, stdout.String())
	}
}

func TestRewriteRedistributeParsesPlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "moves.json")
	if err := os.WriteFile(path, []byte(`{"moves":[{"from":"a","to":"b","hunks":["one","two"]},{"from":"c","to":"d","all":true}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	service := new(fakeRewrite)
	if err := runRewrite(t.Context(), service, []string{"redistribute", "--plan", path, "--dry-run"}, new(bytes.Buffer), new(bytes.Buffer)); err != nil {
		t.Fatal(err)
	}
	want := rewrite.RedistributionRequest{Moves: []rewrite.MoveRequest{
		{From: "a", To: "b", Hunks: []string{"one", "two"}},
		{From: "c", To: "d", All: true},
	}, DryRun: true}
	if !reflect.DeepEqual(service.batch, want) {
		t.Fatalf("request = %#v, want %#v", service.batch, want)
	}
}

func TestRewriteRedistributeRejectsUnknownMoveField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "moves.json")
	if err := os.WriteFile(path, []byte(`{"moves":[{"from":"a","to":"b","all":true,"dry_run":true}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runRewrite(t.Context(), nil, []string{"redistribute", "--plan", path}, new(bytes.Buffer), new(bytes.Buffer)); err == nil {
		t.Fatal("per-move unknown field accepted")
	}
}

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/friel-openai/codex/layerctl/internal/rewrite"
)

// rewriteCommands lets CLI tests check parsing without replaying a Git stack.
type rewriteCommands interface {
	Hunks(string) ([]rewrite.Hunk, error)
	Move(context.Context, rewrite.MoveRequest) (rewrite.Result, error)
	Redistribute(context.Context, rewrite.RedistributionRequest) (rewrite.Result, error)
	Regroup(context.Context, rewrite.RegroupRequest) (rewrite.Result, error)
}

// hunkFlags collects exact selectors rather than interpreting patch text.
type hunkFlags []string

func (v *hunkFlags) String() string { return strings.Join(*v, ",") }
func (v *hunkFlags) Set(value string) error {
	if value == "" {
		return errors.New("hunk ID cannot be empty")
	}
	*v = append(*v, value)
	return nil
}

func runRewrite(ctx context.Context, service rewriteCommands, args []string, stdout, stderr io.Writer) error {
	switch args[0] {
	case "hunks":
		if len(args) < 2 || strings.HasPrefix(args[1], "-") {
			return errors.New("usage: layerctl layer hunks ID [--json]")
		}
		flags := flag.NewFlagSet("layer hunks", flag.ContinueOnError)
		flags.SetOutput(stderr)
		asJSON := flags.Bool("json", false, "render selectors and patch text as JSON")
		if err := flags.Parse(args[2:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return errors.New("usage: layerctl layer hunks ID [--json]")
		}
		hunks, err := service.Hunks(args[1])
		if err != nil {
			return err
		}
		if *asJSON {
			// Text remains readable in JSON; raw []byte would be base64 encoded.
			type listedHunk struct {
				ID     string `json:"id"`
				Header string `json:"header"`
				Patch  string `json:"patch"`
			}
			listed := make([]listedHunk, 0, len(hunks))
			for _, h := range hunks {
				listed = append(listed, listedHunk{h.ID, h.Header, string(h.Patch)})
			}
			return renderJSON(stdout, listed)
		}
		for _, h := range hunks {
			if _, err := fmt.Fprintf(stdout, "%s\t%s\n%s", h.ID, h.Header, h.Patch); err != nil {
				return err
			}
		}
		return nil
	case "move":
		const usage = "usage: layerctl layer move FROM TO (--hunk ID ... | --all) [--dry-run]"
		if len(args) < 3 || strings.HasPrefix(args[1], "-") || strings.HasPrefix(args[2], "-") {
			return errors.New(usage)
		}
		flags := flag.NewFlagSet("layer move", flag.ContinueOnError)
		flags.SetOutput(stderr)
		all := flags.Bool("all", false, "move every source delta")
		dryRun := flags.Bool("dry-run", false, "verify without replacing definitions")
		var hunks hunkFlags
		flags.Var(&hunks, "hunk", "select an exact hunk ID; repeat to select more")
		if err := flags.Parse(args[3:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *all == (len(hunks) > 0) {
			return errors.New(usage)
		}
		result, err := service.Move(ctx, rewrite.MoveRequest{
			From: args[1], To: args[2], Hunks: hunks, All: *all, DryRun: *dryRun,
		})
		if err != nil {
			return err
		}
		return renderJSON(stdout, result)
	case "regroup", "redistribute":
		usage := fmt.Sprintf("usage: layerctl layer %s --plan PATH [--dry-run]", args[0])
		flags := flag.NewFlagSet("layer "+args[0], flag.ContinueOnError)
		flags.SetOutput(stderr)
		path := flags.String("plan", "", "read the assignments from PATH")
		dryRun := flags.Bool("dry-run", false, "verify without replacing definitions")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *path == "" {
			return errors.New(usage)
		}
		var result rewrite.Result
		var err error
		if args[0] == "regroup" {
			groups, readErr := readGroups(*path)
			if readErr != nil {
				return readErr
			}
			result, err = service.Regroup(ctx, rewrite.RegroupRequest{Groups: groups, DryRun: *dryRun})
		} else {
			moves, readErr := readMoves(*path)
			if readErr != nil {
				return readErr
			}
			result, err = service.Redistribute(ctx, rewrite.RedistributionRequest{Moves: moves, DryRun: *dryRun})
		}
		if err != nil {
			return err
		}
		return renderJSON(stdout, result)
	default:
		return fmt.Errorf("unknown rewrite command %q", args[0])
	}
}

func readGroups(path string) ([]rewrite.Group, error) {
	var plan struct {
		Groups []rewrite.Group `json:"groups"`
	}
	if err := decodeRewritePlan(path, &plan); err != nil {
		return nil, err
	}
	return plan.Groups, nil
}

func readMoves(path string) ([]rewrite.MoveRequest, error) {
	var plan struct {
		Moves []struct {
			From  string   `json:"from"`
			To    string   `json:"to"`
			Hunks []string `json:"hunks,omitempty"`
			All   bool     `json:"all,omitempty"`
		} `json:"moves"`
	}
	if err := decodeRewritePlan(path, &plan); err != nil {
		return nil, err
	}
	moves := make([]rewrite.MoveRequest, 0, len(plan.Moves))
	for _, move := range plan.Moves {
		moves = append(moves, rewrite.MoveRequest{From: move.From, To: move.To, Hunks: move.Hunks, All: move.All})
	}
	return moves, nil
}

func decodeRewritePlan(path string, plan any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open rewrite plan: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(plan); err != nil {
		return fmt.Errorf("decode rewrite plan: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return fmt.Errorf("decode rewrite plan trailing data: %w", err)
	}
	return nil
}

func renderJSON(stdout io.Writer, value any) error {
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

layerctl
========

`layerctl` materializes, captures, verifies, and advances the canonical
Frodex layers.
Run it from this repository with `go run ./cmd/layerctl`.

Projections
-----------

Named projections use only these local refs:

```text
refs/layerctl/projections/<name>/base
refs/layerctl/projections/<name>/head
```

Projection worktrees are local, caller-selected scratch space.
Do not encode a machine-specific worktree root in repository files or push
projection refs.
`projection delete` is intentionally destructive and deletes the named
projection without checking whether its work was captured.

Read the projected tree's `AGENTS.md` before changing Codex or Frodex source.
Use `layerctl layer add` or `layerctl layer refresh` to capture accepted
projection work instead of hand-editing generated patches.
The hydrated projection commit is the source-review surface;
the canonical layer directory is its generated storage form.

Create a layer
--------------

Create new feature work from the generated predecessor:

```text
go run ./cmd/layerctl projection create <name> \
  --worktree <path> --through <predecessor>
# Edit and commit in <path>.
go run ./cmd/layerctl layer add <NNNN-layer-slug> --from <name>
go run ./cmd/layerctl projection delete <name>
```

Choose the next available four-digit layer ID.

Revise a layer
--------------

Project through the layer, commit the complete desired result, then capture it:

```text
go run ./cmd/layerctl projection create <name> \
  --worktree <path> --through <layer>
# Edit and commit in <path>.
go run ./cmd/layerctl layer refresh <layer> --from <name>
go run ./cmd/layerctl projection delete <name>
```

Capture uses the projection head's complete tree and exact commit message as
the desired layer result. It does not preserve commit authorship metadata.
`git diff-tree` produces the optional binary-safe `patch` file relative to the
generated predecessor.

Redistribute changes without changing source
-------------------------------------------

List the exact hunk selectors in a layer:

```text
go run ./cmd/layerctl layer hunks <layer> --json
```

Move selected hunks to another existing layer, or use `--all` for every delta:

```text
go run ./cmd/layerctl layer move <source> <target> \
  --hunk <selector> --hunk <selector> --dry-run
go run ./cmd/layerctl layer move <source> <target> --all
```

Modified text files permit individual hunks. File creation, deletion, binary
content, permissions, symlinks and submodules are atomic file deltas. Selectors
identify exact patch bytes; list them again after changing a definition.

For several moves, select all IDs from the same unchanged input and replay once:

```json
{
  "moves": [
    {"from": "0003-correction", "to": "0001-feature", "hunks": ["<selector>"]},
    {"from": "0004-correction", "to": "0002-feature", "all": true}
  ]
}
```

```text
go run ./cmd/layerctl layer redistribute --plan <moves.json> --dry-run
go run ./cmd/layerctl layer redistribute --plan <moves.json>
```

Each source hunk can have only one target. Batch redistribution verifies the
entire assignment together; it does not partially install successful moves.
Moving backward applies the target's original delta first, then its absorbed
corrections. Moving forward applies selected changes first, then the target's
original delta. A forward target containing their inverse can cancel both deltas,
leaving no captured change for that pair.

Combine or reorder definitions with a JSON plan:

```json
{
  "groups": [
    {"id": "0000-foundation", "sources": ["0000-foundation"]},
    {
      "id": "0001-feature",
      "sources": ["0001-feature", "0002-feature-correction"],
      "message": "Implement the complete feature\n"
    }
  ]
}
```

```text
go run ./cmd/layerctl layer regroup --plan <plan.json> --dry-run
go run ./cmd/layerctl layer regroup --plan <plan.json>
```

Every original definition must occur exactly once. New IDs must be in lexical
order and begin with `0000-foundation`. Source definitions apply in their declared
order. An omitted message concatenates the original messages.

The rewrite commands first replay the original stack from the pinned upstream, then
replay and recapture the candidate definitions. They reject conflicts, stale
inputs and any difference in the final Git tree hash. Commit hashes can change
because commit parents and messages change. Dependent hunks cannot always move
across intervening changes; a rejected move leaves definitions unchanged.

Successful commands print a JSON receipt containing the before/after tree hashes
and attribution. `--dry-run` verifies without replacing definitions. Installation
stages the complete directory and restores its backup on ordinary errors; this
does not guarantee recovery from process termination or power loss. No rewrite
command updates existing projection refs, publishes commits or changes upstream.

Rewrites of the same canonical worktree use `.layerctl-rewrite.lock`. An abrupt
termination can leave this file behind. Remove a stale lock only after verifying
that no rewrite of that worktree is still running. A failed backup restoration
reports and retains the original directory; do not discard it.

Advance upstream
----------------

Advance only to exact published `rust-v*` tags.
Fetch the selected tag directly from OpenAI before starting:

```text
git fetch --no-tags https://github.com/openai/codex.git \
  +refs/tags/<tag>:refs/tags/<tag>
go run ./cmd/layerctl upstream advance <tag> --worktree <path>
```

`layerctl` resolves the local tag and records its commit in `upstream.json`.
Clean advances update `upstream.json` only after every layer applies
successfully.

When a layer conflicts, resolve its complete desired tree in the reported
`upstream-advance` worktree and run `layerctl upstream continue`.
`layerctl` owns the interrupted three-way patch application, commits the
resolved layer with its canonical message, and directs the required
`layerctl layer refresh <layer> --from upstream-advance` command.
Run `layerctl upstream continue` again; it independently reapplies the
refreshed definition and proceeds only when the generated tree matches.
Use `layerctl upstream abort` to discard both the interrupted layer
and the local advancement state.

After a successful advance, run
`layerctl projection delete upstream-advance` to remove its completed local
worktree and refs before the next advance.

Verification and release
------------------------

Run `go run ./cmd/layerctl check` before handoff or release.

`layerctl` does not publish releases.
Run `tools/release.sh [-b RELEASE_NOTES]` to dispatch the release workflow from
the remote default branch.
The workflow verifies the exact Codex tag and commit in `upstream.json`, creates
a full projection, chooses the next available Frodex release number, pushes
the immutable projection tag, builds each platform from that tag, and creates
the GitHub release.
Release dispatch does not require or accept a caller-provided projection ref or
version.
Never change a published Frodex tag.

Implementation
--------------

Keep the Go dependency count low; the intended baseline is the standard
library plus the system `git` executable.
Organize code by the domain that owns each operation.
Keep command parsing and rendering at the CLI boundary.
Use the standard-library `log` package for diagnostics on standard error, and
reserve standard output for the requested command result.

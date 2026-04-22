# Release and Documentation Coverage Ledger

## 2026-04-22T00:00:00Z - Preregister audit and coverage pass

Intention: verify release-process commits and retention documentation have appropriate checks and durable prevention guidance.

Responsible agent: pending worker.

Start commit: `83d6ddb19a`.

Worktree or branch: pending.

Mutable surface: files named in `plan.md`.

Validator: workflow inspection, focused lint/test checks if code changes occur.

Expected artifacts: coverage table, any doc/test changes, validator output, disposition.

Disposition: pending.

## 2026-04-22T00:00:00Z - Coverage audit results

Responsible agent: release-doc-coverage worker.

Worktree or branch: `/build/frodex-worktrees/test-audit/release-doc-coverage` on `audit/release-doc-coverage`.

### Per-Commit Coverage Checklist

- [x] `c85dc2884d` - Annotate `codex exec --fork` test literals

  - [x] Code paths read:
        `codex-rs/exec/src/lib.rs`,
        `codex-rs/argument-comment-lint`.
  - [x] Behavior protected:
        `codex exec --fork` tests keep exact `/*path*/ None` callsite annotations for opaque positional arguments, so future changes cannot silently reintroduce ambiguous literals around fork parameter construction.
  - [x] Regression/conformance tests:
        `just argument-comment-lint` is the focused repository check for this behavior; the affected `codex-rs/exec/src/lib.rs` test module is also covered by the fork/cache/exec workstream's focused `codex-exec` tests.
  - [x] Boundary reached:
        source-lint boundary. Responses API/item format is not relevant to this commit because it changes only test callsite annotations.
  - [x] Validation command/result:
        `just argument-comment-lint` - passed after rerun with escalated filesystem access for Bazel cache writes.
  - [x] Remaining gap:
        no hard-test gap for the annotation behavior; the meaningful enforcement is the lint itself.

- [x] `52ad779fe0` - Annotate CLI remote and color literals

  - [x] Code paths read:
        `codex-rs/cli/src/main.rs`,
        `codex-rs/argument-comment-lint`.
  - [x] Behavior protected:
        CLI tests keep exact argument comments for `format_exit_messages` color booleans and remote/auth optional literals, preserving readability and satisfying the repository-wide opaque literal lint.
  - [x] Regression/conformance tests:
        `just argument-comment-lint` covers the changed callsites and is also run by Rust CI for `codex-rs/*` changes.
  - [x] Boundary reached:
        source-lint boundary. Responses API/item format is not relevant because this commit changes only test annotation text.
  - [x] Validation command/result:
        `just argument-comment-lint` - passed after rerun with escalated filesystem access for Bazel cache writes.
  - [x] Remaining gap:
        no hard-test gap for the annotation behavior.

- [x] `75318ecaba` - Restore Frodex release workflow

  - [x] Code paths read:
        `.github/workflows/frodex-release.yml`.
  - [x] Behavior protected:
        the Frodex release workflow exists, runs on `frodex-v*` tags and manual dispatch, builds `codex` release archives for supported macOS and Linux targets, uploads artifacts, and publishes a prerelease.
  - [x] Regression/conformance tests:
        static workflow inspection recorded here. Separately, this audit updates `docs/frodex-feature-retention.md` so future stack reconstruction must preserve the workflow asset, tag trigger, target matrix, artifact naming, and prerelease publishing path.
  - [x] Boundary reached:
        GitHub Actions workflow-source boundary. Responses API/item format is not relevant to release packaging.
  - [x] Validation command/result:
        `git diff --check` - passed in worker validation.
  - [x] Remaining gap:
        no local unit test can prove GitHub's hosted release runner will publish a prerelease. A real `frodex-v*` tag workflow run remains the production validator.

- [x] `d952829ad3` - Use Cargo git CLI fetches in Frodex release builds

  - [x] Code paths read:
        `.github/workflows/frodex-release.yml`.
  - [x] Behavior protected:
        release builds set `CARGO_NET_GIT_FETCH_WITH_CLI: "true"` in the build job environment so Cargo dependency git fetches use the git CLI path on release runners.
  - [x] Regression/conformance tests:
        static workflow inspection recorded here. Separately, this audit updates `docs/frodex-feature-retention.md` so future release-stack checks must preserve `CARGO_NET_GIT_FETCH_WITH_CLI=true`.
  - [x] Boundary reached:
        GitHub Actions workflow-source boundary. Responses API/item format is not relevant.
  - [x] Validation command/result:
        `git diff --check` - passed in worker validation.
  - [x] Remaining gap:
        no local unit test executes a GitHub-hosted release job. The workflow source and retention checklist are the durable local protection; a real release run is the production validator.

- [x] `docs/frodex-feature-retention.md` - Preserve non-code release-stack assets
  - [x] Code paths read:
        `docs/frodex-feature-retention.md`,
        `docs/autoplan-frodex-release-test-audit.md`,
        `docs/loop-ledger-frodex-release-test-audit.md`.
  - [x] Behavior protected:
        future stack reconstruction must preserve release workflow assets, argument-comment lint cleanup, prompt files, feature defaults, and release-artifact or runtime verification evidence instead of treating code diffs as the complete feature inventory.
  - [x] Regression/conformance tests:
        documentation checklist added in `docs/frodex-feature-retention.md` under `Release Stack Retention Gates`; the root AutoPlan now requires each future audited commit to have code-read evidence, behavior evidence, test names, boundary level, validation, and remaining-gap disposition.
  - [x] Boundary reached:
        process-documentation boundary. Responses API/item format is only relevant to runtime feature commits and is covered in the other workstream ledgers.
  - [x] Validation command/result:
        `git diff --check` - passed in worker validation.
  - [x] Remaining gap:
        process docs still depend on future agents following them; this audit makes that requirement explicit and reviewable.

Validation:

- `just argument-comment-lint` failed in the default sandbox because Bazel could not create `/home/dev-user/.cache/bazel/...` on the read-only filesystem.
- `just argument-comment-lint` passed after rerunning with filesystem escalation for Bazel cache writes. Bazel reported `Build completed successfully, 12263 total actions`.

No Rust runtime code changed, so no crate-specific tests were required for this workstream.

Remaining risks: this audit is static for the GitHub release workflow. It does not execute a release tag build or create a GitHub prerelease in CI from this worktree.

Disposition: covered; docs updated and validation passed.

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

Audit table:

| Commit | Behavior under audit | Existing coverage | Disposition |
|---|---|---|---|
| `c85dc2884d` | `codex exec --fork` test callsites use exact `/*path*/ None` literal annotation for `thread_fork_params_from_config`. | `just argument-comment-lint` is the repository-wide focused check for opaque literal argument comments. The touched file is under `codex-rs/`, so normal Rust CI also routes it through `argument_comment_lint_prebuilt`. | covered |
| `52ad779fe0` | CLI tests annotate `format_exit_messages` color booleans and remote/auth optional literals with exact parameter names. | `just argument-comment-lint` covers the changed callsites; `rust-ci.yml` runs the prebuilt lint for `codex-rs/*` changes and workflow changes. | covered |
| `75318ecaba` | Frodex release workflow exists, runs on `frodex-v*` tags and manual dispatch, builds `codex` release archives for supported macOS and Linux targets, uploads artifacts, and publishes a prerelease. | Source inspection of `.github/workflows/frodex-release.yml`: trigger includes `frodex-v*` and `workflow_dispatch`; matrix includes `aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu`, and `aarch64-unknown-linux-gnu`; each matrix row stages `frodex-${{ matrix.target }}.tar.gz`; release job downloads merged artifacts and passes `dist/**` to `softprops/action-gh-release`. | covered |
| `d952829ad3` | Release builds use Cargo's git CLI fetch path on release runners. | Source inspection of `.github/workflows/frodex-release.yml`: build job env sets `CARGO_NET_GIT_FETCH_WITH_CLI: "true"` next to `CARGO_PROFILE_RELEASE_LTO`. | covered |
| `docs/frodex-feature-retention.md` | Future stack reconstruction preserves release workflow assets, argument-comment lint cleanup, and non-code feature assets. | Added `Release Stack Retention Gates` naming the workflow asset, release triggers, supported targets, Cargo git CLI fetch mode, argument-comment lint validation, and ledger mapping requirement. Existing prompt-injection RCAs already capture non-code prompt asset retention and release-binary verification expectations. | new-doc-added |

Validation:

- `just argument-comment-lint` failed in the default sandbox because Bazel could not create `/home/dev-user/.cache/bazel/...` on the read-only filesystem.
- `just argument-comment-lint` passed after rerunning with filesystem escalation for Bazel cache writes. Bazel reported `Build completed successfully, 12263 total actions`.

No Rust runtime code changed, so no crate-specific tests were required for this workstream.

Remaining risks: this audit is static for the GitHub release workflow. It does not execute a release tag build or create a GitHub prerelease in CI from this worktree.

Disposition: covered; docs updated and validation passed.

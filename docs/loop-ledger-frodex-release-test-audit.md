# Frodex Release Test Audit Root Loop Ledger

## 2026-04-22T00:00:00Z - Root initialization

Intention: create a durable AutoPlan for auditing every Frodex release-stack commit and adding missing regression or conformance tests.

Responsible agent: root.

Start commit: `83d6ddb19a`.

Worktree: `/build/frodex-worktrees/stack-refresh-20260418/collab-stack-release`.

Mutable surface: `docs/autoplan-frodex-release-test-audit.md`, this root ledger, workstream plans and ledgers, and test/source files later assigned by workstream.

Validator: workstream-specific focused tests plus final reconciliation.

Expected artifacts: commit-to-behavior coverage map, new tests where needed, validator output, final dispositions.

Disposition: advance.

## 2026-04-22T00:05:00Z - Parallel workstreams launched

Intention: divide the release-stack audit by behavior area and keep each branch moving in its own worktree.

Responsible agent: root.

Start commit: `2021d7fef8`.

Worktrees:

- `/build/frodex-worktrees/test-audit/fork-cache-exec` on `audit/fork-cache-exec`, worker Helmholtz.
- `/build/frodex-worktrees/test-audit/watchdog-runtime-tools` on `audit/watchdog-runtime-tools`, worker Volta.
- `/build/frodex-worktrees/test-audit/prompt-config-models` on `audit/prompt-config-models`, worker Franklin.
- `/build/frodex-worktrees/test-audit/tui-agent-surface` on `audit/tui-agent-surface`, worker Ptolemy.
- `/build/frodex-worktrees/test-audit/release-doc-coverage` on `audit/release-doc-coverage`, worker Noether.

Mutable surface: each worker's plan file.

Validator: worker-specific focused Rust tests plus root reconciliation.

Expected artifacts: one branch commit per workstream or a documented no-code-change disposition.

Disposition: advance.

## 2026-04-22T00:20:00Z - Checklist standard tightened

Intention: match the user's explicit requirement that every release-stack commit have a rigorous checklist, with code-read evidence and hard-to-miss regression or conformance coverage.

Responsible agent: root.

Action: updated the root AutoPlan acceptance evidence and sent follow-up instructions to active workstream agents. The required checklist shape is now per commit, with sub-checks for code paths read, behavior protected, test names and paths, boundary level reached, validator command/result, and remaining gap.

Validator: final reconciliation must reject generic summaries that do not satisfy this checklist shape.

Disposition: advance.

## 2026-04-22T17:45:00Z - Worker commits integrated

Intention: bring all workstream audit evidence and new tests into `refresh/20260418/collab-stack-release`.

Responsible agent: root.

Integrated commits:

- `6c54cb9eb8` from Helmholtz: fork/cache/exec request and rollout item tests.
- `190ff4cd6e` from Volta: watchdog runtime/tool tests.
- `33ce5485cd` from Franklin: prompt/config/model request-level tests.
- `b009a0e2b7` from Ptolemy: TUI agent surface tests and snapshots.
- `d5e06a94c8` from Noether: release/doc retention coverage.

Action: cherry-picked all five workstream commits cleanly. Expanded the release/doc ledger after integration so it matches the per-commit sub-checklist standard rather than a compact table.

Validator: pending integrated-tree focused tests and lint checks.

Disposition: advance.

## 2026-04-22T18:10:00Z - Integrated validation complete

Intention: verify the integrated release branch, not only the individual worker branches.

Responsible agent: root.

Focused tests passed:

- `cargo test -p codex-core inherited_thread_state_shapes_first_responses_request`
- `cargo test -p codex-core watchdog_ -- --nocapture`
- `cargo test -p codex-exec fork_option`
- `cargo test -p codex-exec exec_fork_by_id_creates_new_session_with_copied_history`
- `cargo test -p codex-exec prompt_stdin`
- `cargo test -p codex-core custom_model_alias_uses_backing_model_in_responses_request`
- `cargo test -p codex-features agent_watchdog_is_stable_and_enabled_by_default`
- `cargo test -p codex-features agent_prompt_injection_is_stable_and_enabled_by_default`
- `cargo test -p codex-models-manager custom_model_alias_uses_backing_model_metadata_and_request_model`
- `cargo test -p codex-tui chatwidget::tests::app_server`

Lint, formatting, and snapshot checks passed:

- `just fmt`
- `just fix -p codex-core`
- `just fix -p codex-exec`
- `just fix -p codex-tui`
- `just argument-comment-lint`
- `cargo insta pending-snapshots --manifest-path tui/Cargo.toml`
- `git diff --check`

Corrections made during root reconciliation: expanded the release/doc ledger from a compact table to per-commit sub-checklists, and corrected the TUI ledger to name `resume_replay_closed_watchdog_history_cells` as a snapshot assertion inside `resume_replay_does_not_resurrect_closed_watchdog_panel_row`, not as a standalone executable test.

Remaining risks: full workspace `cargo test` was not run. Release workflow validation remains static/source-inspection plus the already completed production release run; this audit did not trigger a new `frodex-v*` release workflow.

Disposition: complete.

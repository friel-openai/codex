# Frodex Rebase 2026-04-24 Workstream Plan

## Goal

Rebuild the Frodex stack on upstream `main` commit `4816b892044084de6ab5a55ea0b5854c330843fd`, preserve current release behavior except for intentionally removing `exclude_from_compaction`, validate focused behavior, and produce a local Frodex binary.

## Material TODO

- [x] Choose upstream base `4816b892044084de6ab5a55ea0b5854c330843fd`.
- [x] Rebuild the stacked Frodex branch on that base.
- [x] Remove `exclude_from_compaction` from the retained Frodex feature set.
- [x] Add compaction remediation for user messages prefixed `Warning: The maximum number of unified exec process`.
- [x] Restore watchdog runtime fixes: watchdog check-in task as `InterAgentCommunication`, watchdog `PendingInit` reported as running, and model-visible `wait_agent` status bootstrap instead of `list_agents`.
- [x] Validate focused Rust tests, scoped fix/lint checks, and release binary build.
- [x] Install local binary at `/build/frodex-rebase/frodex`.

## Validation

Fast checks are focused crate tests for compaction, watchdog runtime, exec watchdog lifetime, tool registry, prompt config, and models-manager custom aliases.

Release acceptance requires a successful `cargo build --release --bin codex`, a copied executable at `/build/frodex-rebase/frodex`, and a `--version` smoke check.

## Notes

The local branch `refresh/20260423/collab-stack-rebase` is the rebuilt stacked branch. Legacy local feature branch refs under `refresh/20260418/*` and `dev/friel/*` were not force-moved during this pass; this workstream records the rebuilt stacked branch and retained behavior validation.

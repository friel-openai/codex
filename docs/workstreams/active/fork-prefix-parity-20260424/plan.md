# Fork Prefix Parity Workstream Plan

## Goal

Maximize shared request-prefix tokens between root agents, forked subagents, and watchdog check-in agents in the rebased Frodex stack, while preserving watchdog-only tool authorization semantics.

## Material TODO

- [x] Build or add tooling that compares raw backend token dumps and reports shared-prefix length plus first divergences.
- [x] Explain the observed root/watchdog deltas from `/home/dev-user/frodex-root-agent-tokens.log` and `/home/dev-user/watchdog-agent-tokens.log`.
- [x] Replace synthetic watchdog `tool_search` bootstrap with an eager `watchdog` namespace in the initial Responses API tool list.
- [x] Remove watchdog lifecycle tools from the deferred/dynamic tool-search source path.
- [x] Keep watchdog tools runtime-authorized only for watchdog check-in threads even though their schemas are eagerly present.
- [x] Align forked-agent developer prompt prefix content, including model override and discoverable tool sections, unless a documented runtime boundary makes parity unsafe.
- [x] Remove Frodex `exclude_from_compaction` behavior and remediate unified-exec process-limit warnings during compaction using the existing user-message prefix filter mechanism.
- [x] Prevent persistent watchdog handles and ephemeral watchdog check-in handlers from appearing in the `/agent` subagent selector.
- [x] Add focused tests for prefix tooling and request/tool-surface parity.
- [x] Run focused validators, formatting, and rebuild `/build/frodex-rebase/frodex`.

## Acceptance Criteria

- A deterministic local command can compare root vs fork/watchdog prompt dumps and report common prefix bytes/chars, shared percentage, and first divergent snippets.
- Watchdog lifecycle tools are registered as eager tools under the `watchdog` namespace for every agent when Frodex watchdogs are enabled, so root and forked handlers share one early tool-schema prefix.
- The watchdog bootstrap no longer injects synthetic `tool_search` calls or outputs for watchdog lifecycle tools.
- Runtime handlers still reject watchdog lifecycle tool calls from non-watchdog threads.
- Actual request-level tests cover root/subagent/watchdog prompt and tool-surface parity near the first divergence.
- Compaction drops user messages whose text starts with `Warning: The maximum number of unified exec process` without relying on the removed `exclude_from_compaction` feature.
- The TUI `/agent` selector never lists watchdog persistent handles or ephemeral watchdog check-in agent IDs.
- The final binary is rebuilt and version-checked; fresh backend token-dump/live watchdog smoke remains the next optional validator after this patch.

## Mutable Surface

Expected code surfaces include `codex-rs/core/src/tools/spec.rs`, `codex-rs/tools/src/tool_registry_plan.rs`, watchdog prompt/bootstrap code in `codex-rs/core/src/agent/control.rs`, prompt assembly/model override code, compaction/history filtering, TUI agent selector filtering, and any new diagnostic tooling under `codex-rs` or `tools`.

## Fast Validators

Use targeted Rust tests for tool registry/spec behavior and watchdog request construction first. Use the token-dump comparison tool against the provided logs after each prompt/tool-surface change.

# TUI Agent Surface Ledger

## 2026-04-22T00:00:00Z - Preregister audit and coverage pass

Intention: verify the TUI-visible contract for subagent/watchdog panel rows and agent-message cells, adding snapshot coverage where gaps remain.

Responsible agent: Codex subagent.

Start commit: `83d6ddb19a`.

Worktree or branch: `/build/frodex-worktrees/test-audit/tui-agent-surface` on `audit/tui-agent-surface`.

Mutable surface: files named in `plan.md`.

Validator: focused `codex-tui` tests and snapshots.

Expected artifacts: coverage table, any new tests/snapshots, validator output, disposition.

Disposition: completed by coverage audit implementation below.

## 2026-04-22T00:00:00Z - Coverage audit implementation

Release-stack commit checklist:

- [x] `677b05bf0f` - Restore TUI subagent status panel.
  - Code paths read: `codex-rs/tui/src/chatwidget.rs` (`on_collab_agent_tool_call`, `refresh_subagent_panel`, render flex insertion), `codex-rs/tui/src/subagent_panel.rs` (`SubagentPanelRegistry` spawn/status/close/rebuild handling), `codex-rs/tui/src/history_cell.rs` (`SubagentStatusCell` rendering), `codex-rs/tui/src/chatwidget/tests/app_server.rs`.
  - Behavior protected: live app-server collab tool-call items create visible subagent/watchdog panel rows; watchdog rows render idle while pending; normal subagents render running; closed watchdog handles disappear from the panel.
  - Regression/conformance tests: existing `subagent_panel_mounts_watchdog_spawn`, existing `watchdog_goodbye_message_closes_subagent_panel_row`, added `subagent_panel_renders_subagent_and_watchdog_rows` in `codex-rs/tui/src/chatwidget/tests/app_server.rs`; snapshots in `codex-rs/tui/src/chatwidget/snapshots/codex_tui__chatwidget__tests__subagent_panel_mounts_watchdog_spawn.snap`, `...__watchdog_goodbye_message_closes_subagent_panel_row.snap`, and added `...__subagent_panel_renders_subagent_and_watchdog_rows.snap`.
  - Responses API/request/item format reach: not directly relevant for this commit; tests enter through app-server `ThreadItem::CollabAgentToolCall` notifications and verify the TUI surface, not `/responses` request bodies.
  - Validation command/result: `cargo test -p codex-tui chatwidget::tests::app_server` passed after accepting intended snapshots (`22 passed, 0 failed`); `cargo insta pending-snapshots --manifest-path tui/Cargo.toml` reported `No pending snapshots`; `just fix -p codex-tui` passed; `just argument-comment-lint` passed.
  - Remaining gap: no end-to-end app-server process test drives a real spawned subagent through the terminal renderer; current hard test is a deterministic TUI adapter snapshot from app-server notification shapes.

- [x] `add4a0d0f8` - Restore subagent inbox transcript cells.
  - Code paths read: `codex-rs/tui/src/app/app_server_adapter.rs` raw response notification mapping, `codex-rs/tui/src/app_server_session.rs` raw event opt-in for thread start, `codex-rs/tui/src/chatwidget.rs` (`on_raw_response_item`, replay duplicate suppression, `inter_agent_message_from_item`), `codex-rs/tui/src/chatwidget/tests/app_server.rs`.
  - Behavior protected: app-server raw response items carrying inter-agent messages render visible TUI history cells; watchdog goodbye messages render as agent-message history cells; close updates render as history cells rather than silently disappearing.
  - Regression/conformance tests: existing `live_app_server_raw_inter_agent_message_renders_agent_message_cell` and existing `watchdog_goodbye_message_closes_subagent_panel_row` in `codex-rs/tui/src/chatwidget/tests/app_server.rs`; snapshots in `codex-rs/tui/src/chatwidget/snapshots/codex_tui__chatwidget__tests__live_app_server_raw_inter_agent_message_renders_agent_message_cell.snap` and `...__watchdog_goodbye_message_inserts_close_history.snap`; added resume history coverage inside `resume_replay_does_not_resurrect_closed_watchdog_panel_row` in `codex-rs/tui/src/chatwidget/tests/app_server.rs` with snapshot `...__resume_replay_closed_watchdog_history_cells.snap`.
  - Responses API/request/item format reach: yes at item-format level for `InterAgentCommunication::to_response_input_item().into()` delivered as `ServerNotification::RawResponseItemCompleted`; no live `/responses` HTTP request body is asserted in TUI tests.
  - Validation command/result: `cargo test -p codex-tui chatwidget::tests::app_server` passed (`22 passed, 0 failed`); `cargo insta pending-snapshots --manifest-path tui/Cargo.toml` reported `No pending snapshots`; `just fix -p codex-tui` passed; `just argument-comment-lint` passed.
  - Remaining gap: no TUI test covers the removed legacy `agent_inbox` function-output route because `f84089fc57` intentionally made the InterAgentCommunication item route authoritative.

- [x] `f84089fc57` - Use inter-agent communication for subagent messages.
  - Code paths read: `codex-rs/core/src/tools/handlers/multi_agents/send_input.rs` (`send_inter_agent_communication` path for subagent send_input), `codex-rs/tui/src/chatwidget.rs` (`inter_agent_message_from_item`, raw item rendering, replay duplicate guard), `codex-rs/tui/src/chatwidget/tests/app_server.rs`, and `codex-rs/core/src/tools/handlers/multi_agents_tests.rs` for existing core-level delivery assertions.
  - Behavior protected: InterAgentCommunication-delivered messages remain visible in TUI history; replayed closed watchdog spawn+close state does not resurrect a closed watchdog panel row on resume.
  - Regression/conformance tests: existing `live_app_server_raw_inter_agent_message_renders_agent_message_cell` and added `resume_replay_does_not_resurrect_closed_watchdog_panel_row` in `codex-rs/tui/src/chatwidget/tests/app_server.rs`; snapshots in `codex-rs/tui/src/chatwidget/snapshots/codex_tui__chatwidget__tests__live_app_server_raw_inter_agent_message_renders_agent_message_cell.snap`, added `...__resume_replay_does_not_resurrect_closed_watchdog_panel_row.snap`, and added `...__resume_replay_closed_watchdog_history_cells.snap`.
  - Responses API/request/item format reach: yes at Responses item format level through `InterAgentCommunication::to_response_input_item()` in the TUI test; core request-level behavior remains covered by existing `codex-rs/core/src/tools/handlers/multi_agents_tests.rs` from the release-stack commit, but this workstream did not add a new core `/responses` request assertion because the mutable surface is TUI.
  - Validation command/result: `cargo test -p codex-tui chatwidget::tests::app_server` passed (`22 passed, 0 failed`); `cargo insta pending-snapshots --manifest-path tui/Cargo.toml` reported `No pending snapshots`; `just fix -p codex-tui` passed; `just argument-comment-lint` passed.
  - Remaining gap: no new core test was added in this workstream; request-level coverage relies on the existing core tests from `f84089fc57` and this workstream adds TUI adapter/item-format coverage only.

Outcome: added focused TUI snapshot coverage for normal subagent panel rows and resume replay of a closed watchdog; accepted intended snapshots; recorded per-commit coverage and residual gaps.

Disposition: advance.

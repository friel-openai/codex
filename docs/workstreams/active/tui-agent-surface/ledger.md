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
  - Code paths read: `codex-rs/tui/src/app/app_server_adapter.rs` raw response notification mapping, `codex-rs/tui/src/app_server_session.rs` raw event opt-in for thread start, `codex-rs/tui/src/chatwidget.rs` (`on_raw_response_item`, replay duplicate suppression, and the then-current `agent_inbox_message_from_item` parser), `codex-rs/tui/src/chatwidget/tests/app_server.rs`.
  - Actual commit behavior: restored TUI transcript rendering for subagent inbox raw response items by mapping app-server `RawResponseItemCompleted` notifications into core raw-response events and rendering parsed inbox/inter-agent payloads as visible history cells. The commit still included compatibility parsing for legacy `[agent_inbox:...]` text and `agent_inbox` function-output payloads; it did not change the core `send_input` emission format.
  - Regression/conformance tests: the commit added live raw-response TUI adapter snapshots for both legacy `agent_inbox` and InterAgentCommunication-shaped items. Second-pass coverage now also asserts app-server raw InterAgentCommunication notification emission in `codex-rs/app-server/src/bespoke_event_handling.rs::tests::test_inter_agent_raw_response_emits_raw_response_item_completed`.
  - Responses API/request/item format reach: yes at item-format/notification level for raw response items delivered as `ServerNotification::RawResponseItemCompleted`; no `/responses` HTTP request body is asserted in TUI tests.
  - Validation command/result: `cargo test -p codex-app-server test_inter_agent_raw_response_emits_raw_response_item_completed` passed; `cargo test -p codex-tui chatwidget::tests::app_server` passed (`22 passed, 0 failed`); `cargo insta pending-snapshots --manifest-path tui/Cargo.toml` reported `No pending snapshots`; `just fix -p codex-app-server` and `just fix -p codex-tui` passed; `just argument-comment-lint` passed.
  - Remaining gap: the legacy `agent_inbox` compatibility route was intentionally removed by `f84089fc57`, so current protection for this commit focuses on the app-server raw-item adapter path rather than preserving that deleted payload format.

- [x] `f84089fc57` - Use inter-agent communication for subagent messages.
  - Code paths read: `codex-rs/core/src/tools/handlers/multi_agents/send_input.rs` (`send_inter_agent_communication` path for subagent `send_input`), `codex-rs/tui/src/chatwidget.rs` (`inter_agent_message_from_item`, raw item rendering, replay duplicate guard), `codex-rs/app-server-protocol/src/protocol/thread_history.rs`, `codex-rs/app-server/tests/suite/v2/thread_resume.rs`, `codex-rs/tui/src/chatwidget/tests/app_server.rs`, and `codex-rs/core/src/tools/handlers/multi_agents_tests.rs` for existing core-level delivery assertions.
  - Actual commit behavior: changed subagent messages to be emitted as structured InterAgentCommunication response items from core and removed the TUI-only legacy `agent_inbox` parser/test route. The user-visible contract is that InterAgentCommunication raw items remain visible live and after app-server resume reconstruction.
  - Regression/conformance tests: existing `live_app_server_raw_inter_agent_message_renders_agent_message_cell`; second-pass `thread_history::tests::reconstructs_inter_agent_raw_response_item_between_watchdog_spawn_and_close`; second-pass `thread_resume_reconstructs_inter_agent_raw_item_and_closed_watchdog`; and strengthened `resume_replay_does_not_resurrect_closed_watchdog_panel_row`, whose input now comes from app-server-protocol rollout reconstruction rather than a hand-built TUI `Turn`.
  - Responses API/request/item format reach: yes at Responses item format level through `InterAgentCommunication::to_response_input_item()`; live app-server notification emission is asserted at the app-server boundary, and resume/read reconstruction is asserted through app-server-protocol plus an app-server `thread/resume` integration test.
  - Validation command/result: `just write-app-server-schema` passed; `cargo test -p codex-app-server-protocol` passed, including schema fixture checks; `cargo test -p codex-app-server thread_resume_reconstructs_inter_agent_raw_item_and_closed_watchdog` passed; `cargo test -p codex-tui chatwidget::tests::app_server` passed (`22 passed, 0 failed`); `cargo insta pending-snapshots --manifest-path tui/Cargo.toml` reported `No pending snapshots`; `just fix -p codex-app-server-protocol`, `just fix -p codex-app-server`, and `just fix -p codex-tui` passed; `just argument-comment-lint` passed.
  - Remaining gap: no new core request-body test was added in this workstream; request-level coverage still relies on the existing core tests from `f84089fc57`.

Outcome: added focused TUI snapshot coverage for normal subagent panel rows and resume replay of a closed watchdog; accepted intended snapshots; recorded per-commit coverage and residual gaps.

Disposition: advance.

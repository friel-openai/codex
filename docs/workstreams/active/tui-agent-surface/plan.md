# TUI Agent Surface Workstream

## Scope

Audit commits `677b05bf0f`, `add4a0d0f8`, and `f84089fc57`.

Expected behaviors:

- The TUI renders a subagent/watchdog panel while handles are open.
- Closed watchdog durable handles disappear from the panel.
- Subagent and watchdog messages appear as history cells, including final `goodbye` and close/update lines.
- InterAgentCommunication-delivered messages are visible in the TUI and not lost compared with the older tool-call route.
- Resume does not incorrectly resurrect closed subagents.

## Mutable Surface

- `codex-rs/tui/src/chatwidget*`
- `codex-rs/tui/src/subagent_panel.rs`
- `codex-rs/tui/src/history_cell.rs`
- `codex-rs/tui/src/app*`
- TUI snapshots.

## Validator

Fastest useful check: focused `cargo test -p codex-tui <snapshot-test-name>`.

Main validator: snapshot tests and app-server event tests that assert visible cells/panel rows from externally delivered events.

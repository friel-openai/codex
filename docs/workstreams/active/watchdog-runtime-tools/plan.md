# Watchdog Runtime and Tooling Workstream

## Scope

Audit commits `0521ec3973`, `d922d305a3`, `d0c9b82cce`, `ee7700ce4d`, `707ac54ebf`, `8476e9427c`, `4e6338d7f9`, `0ab67b8cea`, and `e409995413`.

Expected behaviors:

- Watchdog handles are durable control handles and short-lived check-in agents are distinct.
- Watchdogs stay alive across owner turns and trigger only after the owner first becomes idle.
- Watchdog check-ins are fresh forks of current owner state, not reused stale helper threads.
- Watchdog helper runs avoid repeated MCP startup when a snapshot exists.
- Watchdog helper context includes synthetic `tool_search` results for `watchdog.snooze`, `watchdog.watchdog_self_close`, and compaction tools.
- `watchdog.snooze` delays the next check-in and ends the helper turn.
- `watchdog.watchdog_self_close` sends the final message to the owner, unregisters the durable watchdog handle, and ends future wakeups.
- Goodbye fallback closes the watchdog handle when applicable.

## Mutable Surface

- `codex-rs/core/src/agent/control.rs`
- `codex-rs/core/src/agent/control_tests.rs`
- `codex-rs/core/src/agent/watchdog.rs`
- `codex-rs/core/src/tools/handlers/multi_agents*`
- `codex-rs/core/src/thread_manager*`
- `codex-rs/core/src/session/*`

## Validator

Fastest useful check: focused `cargo test -p codex-core watchdog_ -- --nocapture` subsets.

Main validator: core tests that assert observable owner events, list-agent output, function-call outputs, and Responses/rollout item shape for watchdog helper requests.

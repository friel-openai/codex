# Fork, Cache, and `codex exec --fork` Workstream

## Scope

Audit commits `7d55fd785a`, `b6c04adf99`, `f6320c8885`, and `c03bb5e599`.

Expected behaviors:

- Forked agents preserve parent prompt-cache keys efficiently.
- Forked agents inherit MCP tool snapshots without reinitializing MCP servers when the snapshot is available.
- `codex exec --fork` works without requiring the fork-reference feature, while fork references still work when enabled.
- Prompt text from stdin is preserved when appending to `codex exec` prompts.

## Mutable Surface

- `codex-rs/core/src/session/*`
- `codex-rs/core/src/thread_manager*`
- `codex-rs/core/src/client.rs`
- `codex-rs/core/src/tools/handlers/multi_agents*`
- `codex-rs/tools/*`
- CLI/exec tests if needed.

## Validator

Fastest useful check: focused `cargo test -p codex-core <test-name>` or relevant CLI test target.

Main validator: targeted crate tests that inspect outbound Responses request structure or rollout fork items where these behaviors affect backend request shape.

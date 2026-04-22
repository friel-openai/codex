# Fork, Cache, and `codex exec --fork` Ledger

## 2026-04-22T00:00:00Z - Preregister audit and coverage pass

Intention: map the assigned commits to behavior-level tests and add missing conformance tests, especially for Responses request item shape.

Responsible agent: pending worker.

Start commit: `83d6ddb19a`.

Worktree or branch: pending.

Mutable surface: files named in `plan.md`.

Validator: focused core/CLI tests chosen by the worker.

Expected artifacts: coverage table, any new tests, validator output, disposition.

Disposition: pending.

## 2026-04-22T17:29:57Z - Coverage audit complete

Responsible agent: Codex worker.

Worktree: `/build/frodex-worktrees/test-audit/fork-cache-exec`

Branch: `audit/fork-cache-exec`

Start commit: `83d6ddb19a`

### Per-Commit Coverage Checklist

- [x] `7d55fd785a` - Inherit forked agent prompt cache keys
  - [x] Code paths read:
    `codex-rs/core/src/agent/control.rs`,
    `codex-rs/core/src/inherited_thread_state.rs`,
    `codex-rs/core/src/session/session.rs`,
    `codex-rs/core/src/session/mod.rs`,
    `codex-rs/core/src/client.rs`,
    `codex-rs/core/src/agent/control_tests.rs`,
    `codex-rs/core/src/session/tests.rs`.
  - [x] Behavior protected:
    forked subagent sessions can carry the parent prompt cache key into the model client so the first backend Responses request uses the inherited `prompt_cache_key`, not the child thread id.
  - [x] Regression/conformance tests:
    existing `spawn_agent_can_fork_parent_thread_history_with_sanitized_items` in `codex-rs/core/src/agent/control_tests.rs` verifies the child session inherits the parent prompt-cache key in memory;
    new second-pass `forked_spawn_first_request_uses_parent_cache_key_and_mcp_snapshot` drives the actual `AgentControl::spawn_agent_with_metadata` full-history fork path to the child thread's first mocked Responses request and verifies the inherited `prompt_cache_key`;
    new `inherited_thread_state_shapes_first_responses_request` in `codex-rs/core/src/session/tests.rs` verifies the actual mocked Responses body contains the inherited `prompt_cache_key`.
  - [x] Responses API/request/item-format level:
    yes. The session-level test captures a mocked Responses request with `ResponseMock::single_request().body_json()` and asserts `body["prompt_cache_key"]`; the second-pass spawn-path test proves the same request field after real fork/spawn inheritance.
  - [x] Validation command/result:
    `cargo test -p codex-core inherited_thread_state_shapes_first_responses_request` - passed.
    `just fix -p codex-core` - passed.
    `just fmt` - passed.
    `just argument-comment-lint` - passed.
  - [x] Remaining gap:
    no remaining hard-test gap for the request-format behavior. The existing fork-spawn test remains the higher-level AgentControl inheritance check.

- [x] `b6c04adf99` - Snapshot MCP tools for forked agents
  - [x] Code paths read:
    `codex-rs/core/src/agent/control.rs`,
    `codex-rs/core/src/inherited_thread_state.rs`,
    `codex-rs/core/src/state/service.rs`,
    `codex-rs/core/src/session/session.rs`,
    `codex-rs/core/src/session/turn.rs`,
    `codex-rs/core/src/session/mcp.rs`,
    `codex-rs/core/src/agent/control_tests.rs`,
    `codex-rs/core/src/session/tests.rs`.
  - [x] Behavior protected:
    inherited MCP tool snapshots can provide model-visible MCP tools for a forked session's first turn even when the child session has no live MCP servers configured.
  - [x] Regression/conformance tests:
    existing `spawn_agent_can_fork_parent_thread_history_with_sanitized_items` in `codex-rs/core/src/agent/control_tests.rs` verifies the child receives an MCP snapshot matching the parent's tool names;
    new second-pass `forked_spawn_first_request_uses_parent_cache_key_and_mcp_snapshot` seeds a parent fake stdio MCP tool, drives `AgentControl::spawn_agent_with_metadata` with `FullHistory`, and asserts the child thread's first mocked Responses request exposes `mcp__rmcp__/echo`;
    new `inherited_thread_state_shapes_first_responses_request` in `codex-rs/core/src/session/tests.rs` seeds an inherited `McpToolSnapshot` and asserts the first mocked Responses request exposes `mcp__snapshot__/echo`.
  - [x] Responses API/request/item-format level:
    yes. The session-level test inspects the actual mocked Responses body and uses `namespace_child_tool(&body, "mcp__snapshot__", "echo")`; the second-pass spawn-path test uses the same request-boundary assertion for the actual fork/spawn route with `mcp__rmcp__/echo`.
  - [x] Validation command/result:
    `cargo test -p codex-core inherited_thread_state_shapes_first_responses_request` - passed.
    `just fix -p codex-core` - passed.
    `just fmt` - passed.
    `just argument-comment-lint` - passed.
  - [x] Remaining gap:
    no request-format gap. The test does not launch a real MCP subprocess; that is intentional because the regression boundary is snapshot-to-Responses tool shape, not MCP transport behavior.

- [x] `f6320c8885` - `codex exec --fork`
  - [x] Code paths read:
    `codex-rs/exec/src/cli.rs`,
    `codex-rs/exec/src/main.rs`,
    `codex-rs/exec/src/lib.rs`,
    `codex-rs/exec/src/cli_tests.rs`,
    `codex-rs/exec/src/main_tests.rs`,
    `codex-rs/exec/tests/suite/fork.rs`,
    `codex-rs/core/src/thread_manager.rs`,
    `codex-rs/app-server/src/codex_message_processor.rs`.
  - [x] Behavior protected:
    non-interactive `codex exec --fork <SESSION_ID> PROMPT` sends a `thread/fork` bootstrap, creates a new session, preserves the source relationship, and sends the prompt to the fork rather than mutating the original session.
  - [x] Regression/conformance tests:
    existing `fork_option_parses_prompt` and `fork_option_conflicts_with_subcommands` in `codex-rs/exec/src/cli_tests.rs`;
    existing `top_cli_parses_fork_option_with_root_config` in `codex-rs/exec/src/main_tests.rs`;
    strengthened `exec_fork_by_id_creates_new_session_with_copied_history` in `codex-rs/exec/tests/suite/fork.rs` to assert the exec fork creates a new session with copied history. Fork-reference replay/materialization behavior is attributed to `83d6ddb19a`, not this CLI commit.
  - [x] Responses API/request/item-format level:
    rollout item-format level, not Responses API level. This behavior affects app-server thread/fork bootstrap and copied-history fork shape; fork-reference payload replay/materialization coverage belongs to `83d6ddb19a`.
  - [x] Validation command/result:
    `cargo test -p codex-exec fork_option` - passed.
    `cargo test -p codex-exec exec_fork_by_id_creates_new_session_with_copied_history` - passed.
    `just fix -p codex-exec` - passed.
    `just fmt` - passed.
    `just argument-comment-lint` - passed.
  - [x] Remaining gap:
    no remaining hard-test gap for CLI parsing or rollout fork shape. The test exercises the in-process app-server path rather than mocking the JSON-RPC message directly; that is the right boundary for `codex exec`.

- [x] `c03bb5e599` - Preserve prompt stdin append behavior
  - [x] Code paths read:
    `codex-rs/exec/src/lib.rs`,
    `codex-rs/exec/src/lib_tests.rs`,
    `codex-rs/exec/tests/suite/prompt_stdin.rs`.
  - [x] Behavior protected:
    positional exec prompts preserve piped stdin by appending it inside a `<stdin>` block; empty piped stdin does not alter a positional prompt; `-` and missing prompt continue to read stdin as the primary prompt and reject empty stdin.
  - [x] Regression/conformance tests:
    existing unit tests `prompt_with_stdin_context_wraps_stdin_block` and `prompt_with_stdin_context_preserves_trailing_newline` in `codex-rs/exec/src/lib_tests.rs`;
    existing integration tests in `codex-rs/exec/tests/suite/prompt_stdin.rs`: `exec_appends_piped_stdin_to_prompt_argument`, `exec_ignores_empty_piped_stdin_when_prompt_argument_is_present`, `exec_dash_prompt_reads_stdin_as_the_prompt`, `exec_without_prompt_argument_reads_piped_stdin_as_the_prompt`, `exec_without_prompt_argument_rejects_empty_piped_stdin`, and `exec_dash_prompt_rejects_empty_piped_stdin`.
  - [x] Responses API/request/item-format level:
    yes where relevant. The prompt-stdin integration tests capture mocked Responses requests and assert the user message input text sent to the backend.
  - [x] Validation command/result:
    `cargo test -p codex-exec prompt_stdin` - passed.
    `just fix -p codex-exec` - passed.
    `just fmt` - passed.
    `just argument-comment-lint` - passed.
  - [x] Remaining gap:
    no remaining hard-test gap. Existing request-level tests directly protect the backend-visible prompt text.

### Files Changed

- `codex-rs/core/src/session/tests.rs`
  - Added `inherited_thread_state_shapes_first_responses_request`.
  - Added a test helper path for constructing sessions with explicit inherited thread state.
- `codex-rs/exec/tests/suite/fork.rs`
  - Strengthened exec fork coverage without attributing fork-reference replay/materialization to `f6320c8885`; that behavior belongs to `83d6ddb19a`.

### Validators Run

- `cargo test -p codex-core inherited_thread_state_shapes_first_responses_request` - passed.
- `cargo test -p codex-exec exec_fork_by_id_creates_new_session_with_copied_history` - passed.
- `cargo test -p codex-exec prompt_stdin` - passed.
- `cargo test -p codex-exec fork_option` - passed.
- `just fix -p codex-core` - passed.
- `just fix -p codex-exec` - passed.
- `just fmt` - passed.
- `just argument-comment-lint` - passed.

Note: an initial run of `just fmt` and `cargo fmt` failed before escalation because the assigned `/build` worktree is read-only inside the default sandbox. The same formatting command succeeded with approved escalated write access. An initial draft of `inherited_thread_state_shapes_first_responses_request` attempted websocket transport against the HTTP mock and timed out; the test was corrected to disable websocket support so it exercises the mocked HTTP Responses request boundary.

### Disposition

Complete. All assigned commits are mapped to behavior-level coverage. Missing request/item-format checks were added for inherited prompt cache/MCP snapshot request shape and fork rollout item shape. Focused validators and required lint/format checks passed. No blockers remain.

## 2026-04-22T18:43:34Z - Second-pass request-boundary fixes

- Added `codex-rs/core/src/agent/control_tests.rs::forked_spawn_first_request_uses_parent_cache_key_and_mcp_snapshot`.
- Coverage: drives the actual `AgentControl::spawn_agent_with_metadata` `FullHistory` path from a parent with live MCP tools to the child thread's first mocked Responses request, proving the inherited `prompt_cache_key` and inherited MCP snapshot tool shape at request boundary.
- Validators: `cargo test -p codex-core forked_spawn_first_request_uses_parent_cache_key_and_mcp_snapshot -- --nocapture` passed; `just fix -p codex-core` passed; `just fmt` passed; `just argument-comment-lint` passed.

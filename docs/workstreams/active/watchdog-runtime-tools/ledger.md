# Watchdog Runtime and Tooling Ledger

## 2026-04-22T00:00:00Z - Preregister Audit and Coverage Pass

Intention: prove watchdog lifecycle, helper fork, and deferred watchdog tool behaviors at externally observable boundaries.

Responsible agent: Codex workstream worker.

Start commit: `2021d7fef8` (`Add Frodex release test audit AutoPlan`) on branch `audit/watchdog-runtime-tools`.

Worktree: `/build/frodex-worktrees/test-audit/watchdog-runtime-tools`.

Mutable surface: files named in `plan.md`.

Expected artifacts: commit coverage checklist, new tests where gaps existed, validator output, disposition.

### Coverage Checklist

- [x] `0521ec3973` Add watchdog runtime handles
  - Code paths read: `codex-rs/core/src/agent/watchdog.rs`, `codex-rs/core/src/agent/control.rs`, `codex-rs/core/src/tools/handlers/multi_agents/spawn.rs`, `codex-rs/core/src/tools/handlers/multi_agents/wait.rs`, `codex-rs/core/src/tools/handlers/multi_agents_tests.rs`.
  - Behavior protected: watchdog spawns return durable inert handles, helpers are separate short-lived threads, waiting on a handle is rejected, closing a handle closes any active helper.
  - Tests: `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::spawn_agent_watchdog_role_returns_inert_handle`; `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::wait_agent_rejects_only_watchdog_handles`; `codex-rs/core/src/agent/control_tests.rs::close_watchdog_handle_closes_active_helper_thread`; strengthened `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::watchdog_handle_is_listed_and_close_agent_removes_it`.
  - Responses/request/item format level: not relevant to the inert handle itself; coverage asserts externally visible tool outputs, `list_agents` JSON, `close_agent` JSON, captured `Op::Interrupt`, and `Op::Shutdown`. The strengthened list test registers an active watchdog helper and proves only the durable handle is listed/targetable, then closing that handle shuts down the hidden helper.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture` passed; `cargo test -p codex-core close_agent -- --nocapture` passed.
  - Remaining gap: none for core runtime/tool observability; TUI panel rendering is covered by the separate TUI workstream.

- [x] `d922d305a3` Add watchdog mailbox wakeup fallback
  - Code paths read: `codex-rs/core/src/agent/control.rs`, `codex-rs/core/src/agent/watchdog.rs`, `codex-rs/core/src/agent/control_tests.rs`.
  - Behavior protected: completed helper messages are delivered to the owner through inter-agent communication, helper prompt scaffolding is stripped, empty scaffold-only fallback is ignored.
  - Tests: `codex-rs/core/src/agent/control_tests.rs::send_watchdog_wakeup_queues_mailbox_message_for_root`; `codex-rs/core/src/agent/control_tests.rs::send_watchdog_wakeup_strips_helper_prompt_scaffold`; `codex-rs/core/src/agent/control_tests.rs::send_watchdog_wakeup_ignores_scaffold_without_report`; `codex-rs/core/src/agent/control_tests.rs::watchdog_forwards_completed_helper_without_waiting_for_interval`; new `codex-rs/core/src/agent/control_tests.rs::watchdog_repeated_checkins_use_fresh_helpers_and_current_owner_fork`.
  - Responses/request/item format level: not relevant; fallback is a runtime owner-delivery path. Tests assert captured `Op::InterAgentCommunication` shape, sender `/root/watchdog`, receiver `/root`, `trigger_turn: true`, and sanitized content.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture` passed.
  - Remaining gap: no hard gap for core delivery. The test does not drive a full model turn after the owner receives the queued input; it verifies the submitted runtime operation at the boundary used by the core thread manager.

- [x] `d0c9b82cce` Add watchdog namespace tools
  - Code paths read: `codex-rs/core/src/tools/handlers/multi_agents/watchdog_snooze.rs`, `codex-rs/core/src/tools/handlers/multi_agents/watchdog_self_close.rs`, `codex-rs/core/src/tools/handlers/multi_agents.rs`, `codex-rs/core/src/tools/spec_tests.rs`, `codex-rs/tools/src/agent_tool.rs`, `codex-rs/tools/src/tool_registry_plan.rs`, `codex-rs/tools/src/tool_registry_plan_tests.rs`, `codex-rs/core/src/tools/handlers/multi_agents_tests.rs`.
  - Behavior protected: watchdog-only tools reject non-watchdog callers; `watchdog.snooze` returns clamped delay and ends helper identity; `watchdog.watchdog_self_close` notifies owner, closes the durable handle, emits owner close event, and prevents future handle wakeups.
  - Tests: `codex-rs/core/src/tools/spec_tests.rs::watchdog_tools_register_namespaced_and_flattened_handlers`; `codex-rs/tools/src/tool_registry_plan_tests.rs::agent_watchdog_adds_watchdog_namespace_tools_and_handlers`; `codex-rs/tools/src/tool_registry_plan_tests.rs::agent_watchdog_handlers_do_not_require_collab_tools`; `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::watchdog_snooze_rejects_non_watchdog_thread`; enhanced `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::watchdog_snooze_suppresses_helper_and_clears_active_helper`; new `codex-rs/core/src/agent/control_tests.rs::watchdog_snooze_delays_next_helper_and_resumes_after_delay`; `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::watchdog_self_close_rejects_non_watchdog_thread`; strengthened `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::watchdog_self_close_notifies_owner_and_unregisters_handle`.
  - Responses/request/item format level: function-call output level is covered through handler outputs parsed from `ToolOutput` text JSON; self-close and snooze tests assert JSON fields (`delay_seconds`, `target_thread_id`, `previous_status`) plus owner ops/events, close-event status, and scheduler delay/resume behavior.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture` passed; `cargo test -p codex-tools agent_watchdog -- --nocapture` passed.
  - Remaining gap: no hard gap. The tests use handler/runtime invocation rather than a remote Responses round trip, which is sufficient for the local function-tool and watchdog scheduler contracts.

- [x] `ee7700ce4d` Keep watchdogs alive after owner turns complete
  - Code paths read: `codex-rs/core/src/agent/watchdog.rs`, `codex-rs/core/src/agent/control_tests.rs`.
  - Behavior protected: owner `Completed`, `Interrupted`, and `Errored` statuses are not treated as watchdog termination; only `Shutdown` and `NotFound` terminate the watchdog. Watchdogs continue to spawn helpers and forward helper results after owner turns complete.
  - Tests: `codex-rs/core/src/agent/watchdog.rs::tests::owner_completed_status_does_not_terminate_watchdog`; `codex-rs/core/src/agent/control_tests.rs::watchdog_spawns_helper_after_owner_completes`; `codex-rs/core/src/agent/control_tests.rs::watchdog_forwards_completed_helper_without_waiting_for_interval`; new `codex-rs/core/src/agent/control_tests.rs::watchdog_repeated_checkins_use_fresh_helpers_and_current_owner_fork`.
  - Responses/request/item format level: not directly relevant; coverage asserts externally visible helper spawn `Op::UserInput` and owner `Op::InterAgentCommunication`.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture` passed.
  - Remaining gap: none for core lifecycle. Long-running wall-clock scheduling is represented with a one-second interval to keep the regression deterministic.

- [x] `707ac54ebf` Trigger watchdog after owner first goes idle
  - Code paths read: `codex-rs/core/src/agent/watchdog.rs`, `codex-rs/core/src/agent/control_tests.rs`.
  - Behavior protected: watchdogs do not spawn while the owner is running/pending; first check-in is triggered only after an owner idle event, and subsequent check-ins require the owner to become idle again.
  - Tests: `codex-rs/core/src/agent/control_tests.rs::watchdog_spawns_helper_after_owner_completes`; new `codex-rs/core/src/agent/control_tests.rs::watchdog_repeated_checkins_use_fresh_helpers_and_current_owner_fork`.
  - Responses/request/item format level: not relevant; idle timing is runtime state. Tests assert captured helper spawn operations only after owner `TurnComplete` events.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture` passed.
  - Remaining gap: no hard gap. The test does not use private watchdog timestamps; it uses owner events and observable helper spawn operations.

- [x] `8476e9427c` Fork watchdog check-in helpers
  - Code paths read: `codex-rs/core/src/agent/watchdog.rs`, `codex-rs/core/src/agent/control.rs`, `codex-rs/core/src/session/tests.rs`, `codex-rs/core/src/agent/role.rs`, `codex-rs/core/src/agent/control_tests.rs`.
  - Behavior protected: watchdog helpers are full-history forks of current owner state, use the watchdog role prompt, record fork references rather than copying parent items into helper rollout, and each later check-in uses a fresh helper thread.
  - Tests: enhanced `codex-rs/core/src/agent/control_tests.rs::watchdog_helper_forks_owner_history`; new `codex-rs/core/src/agent/control_tests.rs::watchdog_repeated_checkins_use_fresh_helpers_and_current_owner_fork`; existing fork baseline `codex-rs/core/src/agent/control_tests.rs::spawn_agent_can_fork_parent_thread_history_with_sanitized_items`; role interval tests `codex-rs/core/src/agent/role_tests.rs::watchdog_role_uses_builtin_interval` and `custom_role_can_define_watchdog_interval`.
  - Responses/request/item format level: yes where relevant. `watchdog_helper_forks_owner_history` inspects helper rollout items for `RolloutItem::ForkReference`, developer prompt ordering, synthetic tool/search entries, and absence of copied parent assistant content.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture` passed.
  - Remaining gap: none for core fork behavior. Backend Responses request-body inspection is not necessary here because the durable fork shape is persisted and asserted at rollout-item level.

- [x] `4e6338d7f9` Avoid MCP startup in watchdog helpers
  - Code paths read: `codex-rs/core/src/agent/watchdog.rs`, `codex-rs/core/src/agent/control.rs`, `codex-rs/core/src/session/turn.rs`, `codex-rs/core/src/agent/control_tests.rs`.
  - Behavior protected: watchdog helper config clears live MCP servers while preserving inherited parent MCP tool snapshot for model-visible tools.
  - Tests: enhanced `codex-rs/core/src/agent/control_tests.rs::watchdog_helper_forks_owner_history` asserts inherited `mcp_tool_snapshot` is present and helper `mcp_connection_manager.has_servers()` is false; existing `codex-rs/core/src/agent/control_tests.rs::spawn_agent_can_fork_parent_thread_history_with_sanitized_items` asserts normal forked children inherit MCP tool snapshot contents.
  - Responses/request/item format level: partially relevant. The watchdog helper test asserts rollout/bootstrap item shape for synthetic tools and runtime MCP state, not a live Responses request.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture` passed.
  - Remaining gap: no hard gap. This does not boot a real MCP server; it verifies the core state boundary that prevents helper startup and preserves snapshot-based tool exposure.

- [x] `0ab67b8cea` Preload watchdog helper control tools
  - Code paths read: `codex-rs/core/src/agent/control.rs`, `codex-rs/core/src/tools/handlers/multi_agents/compact_parent_context.rs`, `codex-rs/core/src/tools/handlers/multi_agents/watchdog_snooze.rs`, `codex-rs/core/src/tools/handlers/multi_agents/watchdog_self_close.rs`, `codex-rs/tools/src/tool_registry_plan.rs`, `codex-rs/core/src/agent/control_tests.rs`.
  - Behavior protected: helper startup injects synthetic `tool_search` results for the watchdog namespace and pre-injects a sanitized `list_agents` output with root context.
  - Tests: enhanced `codex-rs/core/src/agent/control_tests.rs::watchdog_helper_forks_owner_history`; `codex-rs/core/src/agent/control_tests.rs::watchdog_boot_list_agents_redacts_non_root_task_messages`; `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::compact_parent_context_submits_compaction_for_idle_parent`; enhanced snooze/self-close tests listed above.
  - Responses/request/item format level: yes. `watchdog_helper_forks_owner_history` now asserts persisted rollout `ToolSearchCall`, `ToolSearchOutput` containing `compact_parent_context`, `watchdog_self_close`, and `snooze`, plus synthetic `FunctionCallOutput` text JSON with `source: pre_injected_agents_list`, `owner_thread_id`, and `/root` agent entry.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture` passed.
  - Remaining gap: none for item-format coverage. The test reads rollout items rather than relying on private bootstrap helpers.

- [x] `e409995413` Close watchdog handle on goodbye fallback
  - Code paths read: `codex-rs/core/src/agent/watchdog.rs`, `codex-rs/core/src/agent/control.rs`, `codex-rs/core/src/agent/control_tests.rs`.
  - Behavior protected: a helper final message of plain `goodbye` closes the durable watchdog handle, emits a close event to the owner, preserves the final message status on the close event, and stops future helper wakeups for that handle.
  - Tests: `codex-rs/core/src/agent/control_tests.rs::watchdog_plain_goodbye_final_message_closes_handle`; `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::watchdog_self_close_notifies_owner_and_unregisters_handle`; new `codex-rs/core/src/tools/handlers/multi_agents_tests.rs::watchdog_handle_is_listed_and_close_agent_removes_it`.
  - Responses/request/item format level: not directly relevant; tests assert owner-visible `CollabCloseEnd` event, `close_agent` result JSON, registry/list observability, and `AgentStatus::NotFound` after close.
  - Validation: `cargo test -p codex-core watchdog_ -- --nocapture`; `cargo test -p codex-core close_agent -- --nocapture` passed.
  - Remaining gap: none for core fallback. UI rendering of close history is covered by the TUI workstream.

### Validators

- `just fmt` from `codex-rs`: passed after running with elevated filesystem access for the `/build` worktree.
- `cargo test -p codex-core watchdog_ -- --nocapture`: passed, 23 tests.
- `cargo test -p codex-core list_agents -- --nocapture`: passed, 4 tests.
- `cargo test -p codex-core close_agent -- --nocapture`: passed, 4 tests.
- `cargo test -p codex-tools agent_watchdog -- --nocapture`: passed, 2 tests.
- `just fix -p codex-core`: passed. Per repo instruction, tests were not rerun after this lint/fix pass.
- `just argument-comment-lint`: passed; Bazel build completed successfully.

### Disposition

Covered with new and strengthened core tests. No remaining hard-test gaps for the assigned watchdog runtime/tools commits inside this workstream. Remaining UI-only close/list rendering validation belongs to the `tui-agent-surface` workstream.

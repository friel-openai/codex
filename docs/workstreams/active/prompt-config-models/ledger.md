# Prompt, Config, and Model Defaults Ledger

## 2026-04-22T00:00:00Z - Preregister audit and coverage pass

Intention: map prompt/config/model default behaviors to tests and add missing request-level prompt injection tests.

Responsible agent: pending worker.

Start commit: `83d6ddb19a`.

Worktree or branch: pending.

Mutable surface: files named in `plan.md`.

Validator: focused `codex-core` and `codex-models-manager` tests.

Expected artifacts: coverage table, any new tests, validator output, disposition.

Disposition: completed by worker in worktree `/build/frodex-worktrees/test-audit/prompt-config-models` on branch `audit/prompt-config-models`.

## 2026-04-22T17:30:14Z - Coverage audit and request-level regression tests

Responsible agent: Codex worker.

Start commit: `83d6ddb19a`.

Commits audited:

- [x] `4430b27c39` - Restore Frodex watchdog defaults and custom models.
  - [x] Code paths read:
    - `codex-rs/features/src/lib.rs`
    - `codex-rs/features/src/tests.rs`
    - `codex-rs/config/src/config_toml.rs`
    - `codex-rs/core/src/config/mod.rs`
    - `codex-rs/core/src/config/config_tests.rs`
    - `codex-rs/core/src/client.rs`
    - `codex-rs/core/src/thread_manager.rs`
    - `codex-rs/models-manager/src/config.rs`
    - `codex-rs/models-manager/src/manager.rs`
    - `codex-rs/models-manager/src/manager_tests.rs`
    - `codex-rs/protocol/src/openai_models.rs`
    - `docs/frodex-feature-retention.md`
  - [x] Behavior protected:
    - Frodex default feature set keeps `agent_watchdog` enabled.
    - Custom model aliases load from config, reject duplicate aliases, preserve configured token limits, resolve backing model metadata, and send the backing request model to Responses.
  - [x] Regression/conformance tests:
    - `codex-rs/features/src/tests.rs::agent_watchdog_is_stable_and_enabled_by_default`
    - `codex-rs/core/src/config/config_tests.rs::custom_models_load_from_config_toml`
    - `codex-rs/core/src/config/config_tests.rs::custom_models_reject_duplicate_aliases`
    - `codex-rs/models-manager/src/manager_tests.rs::custom_model_alias_uses_backing_model_metadata_and_request_model`
    - `codex-rs/core/tests/suite/model_switching.rs::custom_model_alias_uses_backing_model_in_responses_request`
  - [x] Responses API/request/item format reach:
    - Yes for custom model request routing: `custom_model_alias_uses_backing_model_in_responses_request` captures the mocked Responses body and asserts `"model" == "gpt-real-preview"` for alias `frontier-local`.
    - Not relevant for feature default and config parse duplicate checks; those are pre-request configuration invariants.
  - [x] Validation command/result:
    - `cargo test -p codex-features agent_watchdog_is_stable_and_enabled_by_default` - passed.
    - `cargo test -p codex-core custom_models_load_from_config_toml` - passed.
    - `cargo test -p codex-core custom_models_reject_duplicate_aliases` - passed.
    - `cargo test -p codex-models-manager custom_model_alias_uses_backing_model_metadata_and_request_model` - passed.
    - `cargo test -p codex-core custom_model_alias_uses_backing_model_in_responses_request` - passed.
  - [x] Remaining gap:
    - No binary-level `frodex` smoke test was added in this workstream; coverage is config/manager/request-level Rust tests.

- [x] `83d6ddb19a` - Restore Frodex watchdog and prompt behavior.
  - [x] Code paths read:
    - `codex-rs/core/root_agent_prompt.md`
    - `codex-rs/core/root_agent_watchdog_prompt.md`
    - `codex-rs/core/subagent_prompt.md`
    - `codex-rs/core/watchdog_agent_prompt.md`
    - `codex-rs/core/src/session/mod.rs`
    - `codex-rs/core/src/session/tests.rs`
    - `codex-rs/core/src/agent/control.rs`
    - `codex-rs/core/src/agent/control_tests.rs`
    - `codex-rs/core/src/thread_manager.rs`
    - `codex-rs/core/src/thread_manager_tests.rs`
    - `codex-rs/core/src/tools/handlers/multi_agents_tests.rs`
    - `codex-rs/features/src/lib.rs`
    - `codex-rs/features/src/tests.rs`
    - `docs/frodex-feature-retention.md`
  - [x] Behavior protected:
    - `agent_prompt_injection` is stable and enabled by default.
    - Root prompt fallback loads checked-in `root_agent_prompt.md` and adds the watchdog root fragment only when `agent_watchdog` is enabled.
    - `$CODEX_HOME/AGENTS.root.md`, `AGENTS.subagent.md`, and `AGENTS.watchdog.md` override checked-in prompt fallbacks.
    - Role prompts are represented as developer-role conversation items and are not folded into durable `developer_instructions`.
    - Root, normal subagent, and watchdog sessions put role prompts before the task user message in the mocked Responses request.
    - Normal subagents receive `subagent_prompt.md` guidance only; watchdog subagents receive watchdog guidance including `watchdog.snooze` and not regular subagent responsibilities.
    - Watchdog helper request ordering places materialized owner context before the watchdog developer prompt, and the watchdog developer prompt before the watchdog task. Fork-reference replay/materialization behavior is attributed to this commit (`83d6ddb19a`), not `f6320c8885` or `8476e9427c`.
    - Prompt injection does not depend on legacy `Collab`.
  - [x] Regression/conformance tests:
    - `codex-rs/features/src/tests.rs::agent_prompt_injection_is_stable_and_enabled_by_default`
    - `codex-rs/core/src/session/tests.rs::root_agent_prompt_only_includes_watchdog_fragment_when_enabled`
    - `codex-rs/core/src/session/tests.rs::subagent_prompt_is_for_regular_subagents_only`
    - `codex-rs/core/src/session/tests.rs::agent_prompt_loader_prefers_home_overrides`
    - `codex-rs/core/src/session/tests.rs::root_agent_prompt_is_inline_developer_context_not_session_instructions`
    - `codex-rs/core/src/session/tests.rs::agent_prompt_injection_does_not_require_collab_feature`
    - `codex-rs/core/src/agent/control_tests.rs::watchdog_helper_forks_owner_history`
    - `codex-rs/core/src/agent/control_tests.rs::watchdog_helper_first_request_orders_owner_context_prompt_and_task`
    - `codex-rs/core/tests/suite/prompt_config_models.rs::root_subagent_and_watchdog_prompts_are_developer_items_in_responses_requests`
  - [x] Responses API/request/item format reach:
    - Yes: `root_subagent_and_watchdog_prompts_are_developer_items_in_responses_requests` constructs `ThreadManager` instances for `SessionSource::Exec`, normal `SubAgent(ThreadSpawn { agent_role: None })`, and watchdog `SubAgent(ThreadSpawn { agent_role: Some("watchdog") })`, captures mocked Responses requests, and asserts role prompt content is in developer messages before user task messages and absent from top-level `instructions`.
    - Yes for the watchdog-helper path: `watchdog_helper_first_request_orders_owner_context_prompt_and_task` drives watchdog registration and helper spawn to the first mocked Responses request, then asserts materialized owner context precedes the watchdog prompt and the watchdog task follows it.
  - [x] Validation command/result:
    - `cargo test -p codex-features agent_prompt_injection_is_stable_and_enabled_by_default` - passed.
    - `cargo test -p codex-core root_agent_prompt_only_includes_watchdog_fragment_when_enabled` - passed.
    - `cargo test -p codex-core subagent_prompt_is_for_regular_subagents_only` - passed.
    - `cargo test -p codex-core agent_prompt_loader_prefers_home_overrides` - passed.
    - `cargo test -p codex-core root_agent_prompt_is_inline_developer_context_not_session_instructions` - passed.
    - `cargo test -p codex-core agent_prompt_injection_does_not_require_collab_feature` - passed.
    - `cargo test -p codex-core watchdog_helper_forks_owner_history` - passed.
    - `cargo test -p codex-core root_subagent_and_watchdog_prompts_are_developer_items_in_responses_requests` - passed.
  - [x] Remaining gap:
    - The explicit root/subagent/watchdog request-level prompt test covers new thread history. The second-pass watchdog-helper test now covers reconstructed fork history in the first mocked helper Responses request.

Files changed:

- `codex-rs/core/tests/suite/model_switching.rs`
- `codex-rs/core/tests/suite/mod.rs`
- `codex-rs/core/tests/suite/prompt_config_models.rs`
- `docs/workstreams/active/prompt-config-models/ledger.md`

Additional validators:

- `just fmt` - passed.
- `just fix -p codex-core` - passed.
- `just argument-comment-lint` - passed.

Notes:

- A first `cargo test -p codex-core role_prompts_are_ordered_developer_items_in_responses_requests` attempt failed to compile because it tried to mix internal `codex-core` unit-test private APIs with the integration test harness crate types. The request-level prompt coverage was moved to `codex-rs/core/tests/suite/prompt_config_models.rs` and passed there.
- `cargo test -p codex-core custom_models_load_from_config_toml custom_models_reject_duplicate_aliases` was an invalid Cargo invocation because Cargo accepts one test filter; the two filters were rerun separately and both passed.

## 2026-04-22T18:43:34Z - Second-pass request-boundary fixes

- Added `codex-rs/core/src/agent/control_tests.rs::watchdog_helper_first_request_orders_owner_context_prompt_and_task`.
- Coverage: drives watchdog registration, owner completion, helper spawn, and helper turn execution to the helper's first mocked Responses request. The request proves materialized owner context appears before the watchdog developer prompt, and the watchdog task appears after that prompt.
- Attribution correction: fork-reference replay/materialization behavior belongs to `83d6ddb19a`, not `f6320c8885` or `8476e9427c`.
- Validators: `cargo test -p codex-core watchdog_helper_first_request_orders_owner_context_prompt_and_task -- --nocapture` passed; `just fix -p codex-core` passed; `just fmt` passed; `just argument-comment-lint` passed.

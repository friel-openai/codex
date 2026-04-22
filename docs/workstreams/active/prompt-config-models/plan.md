# Prompt, Config, and Model Defaults Workstream

## Scope

Audit commits `4430b27c39` and `83d6ddb19a`, plus the restored checked-in prompt assets and `docs/frodex-feature-retention.md`.

Expected behaviors:

- Frodex binary defaults enable `agent_watchdog` and `agent_prompt_injection`.
- Custom model support remains present and test-covered.
- Root, subagent, and watchdog role prompts load from user overrides or checked-in defaults.
- Prompt injection does not mutate durable `developer_instructions`; role prompts are represented as developer-role conversation items at temporally coherent positions.
- Watchdogs receive only `watchdog_agent_prompt.md`; normal subagents receive `subagent_prompt.md`.

## Mutable Surface

- `codex-rs/core/src/config/*`
- `codex-rs/core/src/agent/role*`
- `codex-rs/core/src/session/*`
- `codex-rs/core/*_prompt.md`
- `codex-rs/models-manager/*`
- `docs/frodex-feature-retention.md`

## Validator

Fastest useful check: focused prompt/config/model tests in `codex-core` and `codex-models-manager`.

Main validator: tests that inspect conversation items sent to mocked Responses requests for role prompt ordering and content.

# Frodex Feature Retention

This document records feature-retention checks for the Frodex branch stack. Use it during rebases, release-branch reconstruction, and release cuts.

## Required Checks For Every Stacked Feature

- Identify the branch or PR that owns the feature.
- Name the user-visible behavior that must survive the rebase.
- Name the config flag, default, or binary override that enables the behavior.
- Name the tests that fail if the behavior is dropped.
- Check for non-Rust assets that are part of the feature, including Markdown prompts, schemas, snapshots, config examples, and generated files.
- Verify the release artifact or runtime behavior, not only unit tests. For the current release workflow, this means the `frodex-*` archives contain the built `codex` binary.

## Release Stack Retention Gates

Apply these gates before cutting any `frodex-v*` tag from a reconstructed stack:

- Treat `.github/workflows/frodex-release.yml` as a retained release asset. A stack audit must confirm the workflow still runs on `frodex-v*` tags and `workflow_dispatch`.
- Confirm the release workflow builds and uploads one archive for each supported target: `aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-gnu`, and `aarch64-unknown-linux-gnu`.
- Confirm release builds set `CARGO_NET_GIT_FETCH_WITH_CLI=true` so Cargo git fetches use the GitHub CLI path on release runners.
- For commits that only satisfy the literal argument-comment convention, run `just argument-comment-lint` or record the exact reason it could not run locally.
- Record the coverage mapping in the active workstream ledger before tagging, including which workflow, lint, unit, snapshot, artifact-level, or runtime check protects each retained behavior.

## RCA: Agent Prompt Injection Dropped During Stack Reconstruction

Date: 2026-04-22.

Feature: root, subagent, and watchdog prompt injection through `AGENTS.root.md`, `AGENTS.subagent.md`, `AGENTS.watchdog.md`, and checked-in default prompt fallbacks.

What broke: the reconstructed release branch kept much of the watchdog runtime but dropped the prompt-injection feature flag, the prompt-loading path, and the checked-in default prompt files. The resulting binary still ran subagents and watchdogs, but the root, subagent, and watchdog role-specific guidance was absent from new sessions.

Why it slipped: the branch audit focused on Rust runtime behavior, model-facing tools, and TUI output. Prompt Markdown files were treated like incidental documentation instead of feature assets. The release checks did not require proving that prompt assets were compiled into the binary or visible in request construction.

Fix: restore `agent_prompt_injection` as a stable default-enabled feature, restore checked-in default prompt files, load user overrides from `$CODEX_HOME/AGENTS.root.md`, `$CODEX_HOME/AGENTS.subagent.md`, and `$CODEX_HOME/AGENTS.watchdog.md`, and add tests that assert root, subagent, and watchdog prompts are injected.

Follow-up correction: prompt injection must not mutate `SessionConfiguration.developer_instructions`. Those instructions are part of the durable request baseline and changing them for forked agents breaks backend prompt/KV caching. Role prompts must be recorded as developer-role conversation items when they become relevant: root prompts in the initial root context, subagent prompts after fork reconstruction and before the child task, and watchdog prompts after the fork reference and before the watchdog check-in task.

Additional prompt recovery: two April 6 watchdog prompt edits diverged. The recovered default now combines the evidence-based supervision guidance from `670b2e4fb77443950bbb8a2b1d0a26430102507f` with the later `watchdog.snooze` guidance from `88145c13a5c4c15093a7ffd6e9a98ec44a22a8da`.

Prevention: every branch checklist must include prompt assets and other non-code feature assets. For prompt features, tests must assert both the default fallback content and user override behavior. Release validation must include an artifact-level smoke test or request-body inspection that proves the role prompt appears in the effective thread instructions.

## RCA: Prompt Injection Incorrectly Depended On Collab

Date: 2026-04-22.

Feature: default root, subagent, and watchdog prompt injection for current agent/watchdog sessions.

What broke: `agent_prompt_injection` was enabled and the prompt assets were compiled into the binary, but `load_agent_role_prompt` returned `None` unless `Feature::Collab` was also enabled. Current Frodex watchdogs and subagents can be created through the newer agent path without the old Collab feature, so watchdog helper rollouts contained the fork reference, synthetic watchdog tool-search bootstrap, and watchdog task user message, but no developer-role watchdog prompt.

Why it slipped: the prompt-injection tests enabled `Collab`, which matched the older branch stack but not the current runtime path. That left the real binary configuration untested: `agent_prompt_injection = true`, `agent_watchdog = true`, and Collab disabled.

Fix: make prompt injection depend on `agent_prompt_injection` itself, not on `Collab`. Add a regression test that disables `Collab`, enables `agent_prompt_injection` and `agent_watchdog`, builds an explicit watchdog `SessionSource`, and asserts the loaded prompt is the watchdog role prompt with `watchdog.snooze` guidance, not the regular subagent prompt.

Prevention: every feature branch that survives a stack rebase needs at least one test that uses the release configuration, not only the historical branch combination. When a feature moves from one tool/runtime path to another, remove stale feature dependencies or add tests that prove the old dependency is still required.

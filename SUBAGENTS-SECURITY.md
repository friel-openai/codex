# Subagent Security, Reliability, and Abuse Review

_Workspace: `/Users/friel/code/codex-workdirs/superb-impl`, branch `subagents/superb-main`._
_Reviewed: Sat Nov 22 2025 (local time)._

## Overview

This review focused on the built-in subagent orchestration system in Codex, specifically:

- Runtime management and lifecycle: `codex-rs/core/src/subagents/manager.rs`
- Tool surface and handler logic: `codex-rs/core/src/tools/handlers/subagent.rs`
- Feature flag gating: `codex-rs/core/src/features.rs` (the `SubagentTools` feature)
- Configuration parsing and limits: `codex-rs/core/src/config/mod.rs` (subagent-related settings)
- Prompting and alignment: `codex-rs/core/subagent_prompt.md`, `core/root_agent_prompt.md`, and the base prompts in `core/*.md`.

The goal was to assess security, robustness, and abuse resistance, including risks such as runaway spawning or awaiting, sandbox bypass or tool misuse, agent misidentification, reasoning-log leakage, and watchdog misuse. No code changes were made; this document only reports findings and suggested mitigations.

Overall, the design already included several good controls:

- Subagent tools are gated behind an explicit `subagent_tools` feature and a `config.include_subagent_tools` flag.
- The maximum number of active subagents per session is configurable with sensible defaults and hard upper bounds.
- Watchdog intervals are bounded with a minimum of 30 seconds and are keyed per `(caller_session_id, target_agent_id)` to avoid unbounded fan-out.
- Subagent logs are bounded in memory (`LOG_CAPACITY`) and log-rendering applies byte and count caps.
- The prompts for both root and subagents strongly encourage focused, scoped work and discourage unnecessary blocking.

The sections below list specific risks and concrete mitigations.

## Identified Risks and Concerns

### 1. Denial-of-service via subagent fan-out and root autosubmit

**Behavior today**

- `Config::from_config` (in `core/src/config/mod.rs`) enforced `DEFAULT_MAX_ACTIVE_SUBAGENTS` (8) and a hard upper bound `MAX_MAX_ACTIVE_SUBAGENTS` (64). Values below the minimum were rejected; values above the maximum were clamped with a warning.
- `SubagentManager::new` created a `Semaphore` with `max_active_subagents` permits and used this to gate spawning and forking.
- The subagent tools (`subagent_spawn`, `subagent_fork`) were only registered when `config.include_subagent_tools` was true, which ultimately depended on the `SubagentTools` feature and config.
- The manager maintained per-session maps of runs, completions, logs, inboxes, and root inbox entries. Root autosubmit could synthesize `subagent_await` calls and outputs into the root session history, and child inboxes could be delivered into child threads either as synthetic awaits or user messages.

**Risk**

- Even with the `max_active_subagents` cap, an adversarial or buggy root agent could:
  - Rapidly churn subagents (spawn, perform minimal work, cancel/prune, and respawn), driving CPU and memory overhead and generating large volumes of events and histories.
  - Combine frequent subagent spawns with autosubmit-enabled inbox draining, causing many synthetic `subagent_await` items to be injected into the root history, which could increase prompt size and degrade performance.
- Within a single subagent, repeated `subagent_await` calls with long timeouts (up to 30 minutes) could tie up server-side resources, especially if many agents held concurrent awaits across a fleet.

**Mitigations**

- Consider adding per-session rate limiting or budgeting for subagent operations (spawns, forks, awaits, and cancels), such as:
  - A maximum spawn/fork rate (e.g., per minute) per root session; excess requests could return a `FunctionCallError::RespondToModel` instructing the model to slow down or consolidate work.
  - A limit on synthetic `subagent_await` injections per turn when autosubmit is enabled, to prevent pathological growth of root histories from a single idle period.
- Consider tracking and exposing per-session resource usage (number of subagents created, average lifespan, total CPU time, etc.) to the root agent via `subagent_list` or a future introspection tool so root prompts can make more informed decisions about pruning and consolidation.
- For multi-tenant deployments, pair these in-process controls with external rate limiting at the API or session layer.

### 2. Watchdog misuse and self-sustaining traffic

**Behavior today**

- `subagent_watchdog` accepted `agent_id`, an optional `interval_s`, optional `message`, and optional `cancel`.
- `resolve_watchdog_interval` (in the handler) enforced:
  - `MIN_WATCHDOG_INTERVAL_SECS = 30` and a default of 300 seconds when the interval was omitted or zero.
- `SubagentManager::start_watchdog`:
  - Keyed watchdogs by `(caller_session_id, target_agent_id)` and canceled any existing watchdog for that pair before starting a new one.
  - Created a background task that slept for `interval_s` and, on wake, sent a message either to the root (agent 0) or to a specific subagent via `send_message`.
  - Stopped the watchdog if `send_message` failed or if metadata for the caller could not be found.

**Risk**

- A root agent or subagent could start a watchdog targeting another agent (or the root itself) with a short interval (30 seconds) and a verbose message, leading to:
  - Persistent background traffic and event generation, even when no useful work was happening.
  - Large accumulated histories in the target agents if they did not promptly consume or prune messages.
- Because the watchdog message was free-form `String`, a malicious agent could attempt to embed sensitive context or misleading instructions into watchdog pings, potentially confusing other agents or leaking information in logs.

**Mitigations**

- Consider adding a per-session cap on the number of active watchdogs, or expose watchdog state in `subagent_list` (e.g., by annotating metadata) so root prompts can more easily discover and cancel unused timers.
- Consider adding a maximum length for watchdog messages (e.g., truncate to a few hundred characters) to reduce the risk of large, repeated payloads.
- Consider adding optional configuration to disable watchdogs entirely or to require explicit user opt-in per session in higher-security deployments.
- Clarify in `root_agent_prompt.md` and `subagent_prompt.md` that watchdog messages are primarily for short status pings and should not be used to transmit sensitive information or long-form context.

### 3. Agent identification and targeting errors

**Behavior today**

- The handler computed `registry_by_agent` and used it to map `AgentId` values to `ConversationId` via `InvocationContext::agent_session` and `require_agent_session`.
- `is_root_session` treated a session as root when no registry entry existed with that `session_id` and `agent_id != ROOT_AGENT_ID`.
- `subagent_send_message` enforced:
  - You could not send an interrupt to agent 0 (root).
  - The root agent could not target agent 0 via `subagent_send_message`; it had to use a normal user message instead.
  - Non-root senders targeting agent 0 had to have associated metadata; otherwise, the call errored.
- `SubagentManager::agent_id_for` and `parent_agent_id` fell back to `ROOT_AGENT_ID` when metadata was missing.

**Risk**

- If registry state became inconsistent (e.g., due to a bug in import-from-rollout or partial cleanup), the `agent_id_for` and `parent_agent_id` fallbacks might attribute messages or completions to the root agent instead of the correct subagent.
- Synthetic `subagent_await` injections relied on matching `agent_id` and `session_id` to find metadata and pending inbox messages; inconsistent metadata could cause:
  - Mislabeling of subagents in logs and UI.
  - Confusing behavior where messages appeared to originate from or target the wrong agent.

**Mitigations**

- Consider tightening the fallbacks in `agent_id_for` and `parent_agent_id`:
  - Instead of silently returning `ROOT_AGENT_ID` when metadata was missing, consider returning an error or logging at `error!` level and skipping the operation.
- Add internal consistency checks (e.g., debug assertions or optional validation functions) that could be invoked in tests or diagnostics to verify that `SubagentRegistry` and the manager’s maps were in sync.
- Expand tests around `import_from_rollout` and synthetic `subagent_await` injection to cover edge cases where lifecycle events were missing, out of order, or partially persisted.

### 4. Reasoning header and log leakage

**Behavior today**

- `SubagentManager` recorded a bounded log of `Event`s per subagent (`LOG_CAPACITY = 200`) in a `VecDeque`, with timestamps stored via `unix_time_millis`.
- `subagent_logs` exposed log snapshots with filtering by `since_ms`, `before_ms`, `limit`, and `max_bytes`, and `render_logs_payload` enforced a default limit (5) and byte cap via `apply_log_window`.
- `classify_activity` and `render_logs_lines` summarized log content into human-readable lines, focusing on high-level activity and the last assistant message and reasoning summary.
- The global prompts and config included options to hide or show agent reasoning, but `subagent_logs` could still expose reasoning deltas and summaries as part of events.

**Risk**

- In deployments where subagents operated on sensitive data or where different subagents represented distinct trust boundaries, `subagent_logs` could leak:
  - Internal reasoning summaries (`Reasoning` items and `ReasoningContentDelta` events).
  - Raw assistant messages or tool results that were not intended to be shared across agents or with certain users.
- The current system assumed shared trust among subagents within a session; there was no fine-grained authorization on who might call `subagent_logs` for which `agent_id`.

**Mitigations**

- Document explicitly that subagents within a session were not isolation boundaries; they shared a trust context, and `subagent_logs` was not filtered for sensitive content.
- For higher-security scenarios, consider adding configuration flags or policy hooks to:
  - Disable `subagent_logs` entirely.
  - Filter or redact reasoning events and certain message types from the logs that were exposed to other agents.
- Ensure that UI surfaces displaying subagent logs clearly indicated that the content could include internal reasoning and intermediate tool outputs, so users understood the exposure.

### 5. Long-await behavior and resource occupancy

**Behavior today**

- `resolve_await_timeout` enforced:
  - A maximum per-call timeout of `MAX_AWAIT_TIMEOUT_SECS = 30 * 60` (30 minutes).
  - A minimum timeout of 300 seconds if a smaller positive value was provided; `None` and zero both mapped to the maximum.
- `SubagentManager::await_completion` used this timeout to wait on completion via a `watch::Receiver`, and it returned an error when the total elapsed time exceeded the timeout.

**Risk**

- A root agent could initiate many concurrent `subagent_await` calls, each holding server-side state for up to 30 minutes. In combination with many sessions, this could lead to high memory usage and potentially exhaustion of async runtime resources, even though each individual await was bounded.

**Mitigations**

- Consider enforcing a per-session cap on concurrently outstanding `subagent_await` operations, returning an error when the limit was exceeded and advising the model to consolidate awaits.
- Consider exposing basic metrics (number of awaits per session, average wait time) via internal telemetry to help detect pathological patterns.
- Optionally tighten the default from 30 minutes to a smaller value in default configurations intended for interactive use, leaving the larger window only for explicitly configured batch modes.

### 6. Prompt-level alignment and watchdog guidance

**Behavior today**

- `subagent_prompt.md` and `root_agent_prompt.md`:
  - Emphasized staying within the scope of assigned work.
  - Recommended using `subagent_await` only when truly necessary and encouraged non-blocking progress.
  - Encouraged use of `subagent_send_message` for concise progress updates and coordination.
  - Provided an example of a long-running supervisor with a watchdog that kept `PLAN.md` up to date and responded to pings.
- The base prompts (`prompt.md`, `gpt_5_1_prompt.md`, `gpt_5_codex_prompt.md`, `gpt-5.1-codex-max_prompt.md`, `review_prompt.md`) provided broader guidance on safe tool use, sandboxing, and avoiding ungrounded actions.

**Risk**

- The example watchdog usage encouraged frequent, long-lived watchdogs for supervision; models might over-apply this pattern, leading to many background timers and status pings even for small tasks.
- The prompts did not explicitly warn against using subagents to circumvent sandbox restrictions or approval policies, relying instead on the underlying sandbox enforcement to prevent misuse.

**Mitigations**

- Consider updating `root_agent_prompt.md` and `subagent_prompt.md` to:
  - Emphasize sparing use of watchdogs and a preference for manual progress reporting on shorter tasks.
  - Call out that subagents inherited the same sandbox and approval constraints as the root and must not be used to evade them.
- Consider adding a brief line reminding agents to keep subagent hierarchies shallow and to prune idle subagents to avoid confusion and resource waste.

### 7. Rollout import and synthetic event reconstruction

**Behavior today**

- `SubagentManager::import_from_rollout` reconstructed metadata and watchdogs from persisted `RolloutItem`s by:
  - Scanning for `SubagentLifecycle` events to rebuild `SubagentMetadata` per `ConversationId`.
  - Tracking `subagent_watchdog` function calls and outputs to rebuild active watchdog state.
- Synthetic `subagent_await` calls and outputs were injected either into the root or child sessions under certain conditions (autosubmit, terminal inbox draining, etc.), using metadata and pending inbox messages.

**Risk**

- If rollout logs were truncated or inconsistent (e.g., missing some `FunctionCallOutput` events, or missing certain lifecycle events), the reconstruction logic might:
  - Leave watchdogs active that no longer corresponded to real agents.
  - Reconstruct partial metadata for subagents (e.g., missing status or reasoning header) and then use it in synthetic `subagent_await` injections.

**Mitigations**

- Consider making `collect_watchdogs_from_rollout` more defensive, for example by:
  - Requiring both a valid `agent_id` and a recognized `action` string, and logging at `warn!` level when unexpected payload shapes were encountered.
- Add tests that simulated truncated or partially corrupted rollouts and verified that import behaved safely (e.g., by dropping inconsistent entries and not starting orphaned watchdogs).

## Suggested Tests and Instrumentation

To strengthen security and reliability, the following additional tests and instrumentation would be valuable:

- **Stress tests for subagent fan-out**
  - Create tests that spawned near the maximum number of subagents, exercised frequent cancels and prunes, and verified that memory and CPU usage remained within expected bounds and that the system recovered cleanly.

- **Watchdog behavior tests**
  - Add tests that started and canceled watchdogs for various `(caller_session_id, target_agent_id)` combinations, ensuring that multiple reconfigurations did not leak tasks and that failed sends reliably stopped watchdogs.
  - Add tests for the minimum interval enforcement and message length truncation if implemented.

- **Import-from-rollout robustness tests**
  - Simulate rollouts with missing or out-of-order events and assert that `import_from_rollout` avoided starting inconsistent watchdogs or creating invalid metadata.

- **Agent identification invariants**
  - Add invariants or property-based tests that generated random subagent trees and verified that `agent_session`, `parent_agent_id`, and synthetic `subagent_await` injections always agreed with registry state.

- **Prompt conformance tests**
  - Create automated prompt checks (or snapshot tests over extracted instructions) to ensure that security-critical guidance (watchdog usage, sandbox adherence, pruning expectations) was present and did not regress in future edits.

- **Telemetry and observability**
  - Add optional metrics (behind feature flags or compilation features) that tracked:
    - Number of active subagents per session.
    - Number of active watchdogs per session.
    - Rates of `subagent_spawn`, `subagent_fork`, `subagent_await`, and `subagent_logs` calls.
  - Use these metrics in staging/production to detect abuse patterns or misconfigured sessions.

## Documentation and Config Recommendations

- Extend `docs/` and `config.md` to document:
  - The `subagent_tools` feature flag and `include_subagent_tools` config, including recommended defaults for different environments (local development vs. shared servers).
  - The semantics and limits of `max_active_subagents`, `subagent_root_inbox_autosubmit`, and `subagent_inbox_inject_before_tools`.
  - The behavior and intended use of `subagent_watchdog`, including guidance to avoid excessive timers and to prefer short status messages.
  - The trust model: subagents within a session shared a trust boundary; `subagent_logs` and synthetic `subagent_await` injections were not designed as isolation boundaries.
- Ensure that user-facing docs and examples highlighted how to safely use subagents without over-spawning, over-watching, or leaking reasoning logs.

## Summary of Main Findings

- The subagent system already enforced important safety constraints: feature gating, bounded active subagents, bounded logs, and guarded watchdog intervals.
- Residual risks remained around denial-of-service via aggressive subagent fan-out and long-running awaits, watchdog misuse leading to self-sustaining traffic, and potential confusion or leakage through logs and synthetic `subagent_await` injections.
- These risks could be mitigated with additional rate limiting, defensive metadata handling, improved tests and metrics, and small prompt and documentation updates that clarified the trust model and safe usage patterns.


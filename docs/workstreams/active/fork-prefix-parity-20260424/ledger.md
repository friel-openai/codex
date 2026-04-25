# Fork Prefix Parity Ledger

## 2026-04-24T09:30:00Z - Workstream Created

Preregistration: investigate and fix root/fork/watchdog prompt-prefix divergence in the rebased Frodex stack. Responsible agent: root. Start commit: `0d096d0677`. Worktree: `/build/frodex-rebase/collab-stack-rebase` on `refresh/20260423/collab-stack-rebase`. Mutable surface: prompt/tool assembly, watchdog deferred tools, tool-search authorization, request-prefix diagnostic tooling, and focused tests. Validators: token-dump comparator on `/home/dev-user/frodex-root-agent-tokens.log` and `/home/dev-user/watchdog-agent-tokens.log`, focused Rust tests, `just fmt`, scoped `just fix -p`, `just argument-comment-lint`, and release binary rebuild.

Disposition: advance

## 2026-04-24T10:11:54Z - Added Compaction and Watchdog Selector Fixes

Preregistration: incorporate new Frodex rebase requirements while preserving the prefix-parity workstream. Responsible agent: root. Start commit: `0d096d0677` plus uncommitted prefix-parity patches. Worktree: `/build/frodex-rebase/collab-stack-rebase` on `refresh/20260423/collab-stack-rebase`. Mutable surface: compaction/history filtering, removal of `exclude_from_compaction`, TUI `/agent` selector filtering, watchdog registry cleanup if needed, and focused tests. Validators: grep for removed feature surface, compaction filtering tests, TUI selector or registry tests, `just fmt`, focused `cargo test`, scoped `just fix -p`, `just argument-comment-lint`, and final release binary rebuild.

Disposition: advance

## 2026-04-24T11:05:00Z - Prefix Parity and Rebuild Outcome

Outcome: implemented prompt-prefix diagnostic tooling, deferred the watchdog namespace, preserved watchdog tool authorization through deferred discovery, inherited app-server client metadata for fork/watchdog prompt parity, kept Collab prompt surface visible at child depth limits, confirmed unified-exec warning compaction filtering without `exclude_from_compaction`, filtered watchdog threads out of `/agent`, and rebuilt `/build/frodex-rebase/frodex`.

Evidence: `tools/prompt-prefix-diff/prompt-prefix-diff.py /home/dev-user/frodex-root-agent-tokens.log /home/dev-user/watchdog-agent-tokens.log` identifies first backend-token divergence at line 500 in the supplied old dumps; focused tests passed for watchdog namespace deferral, inherited request shaping, depth-limit Collab prompt preservation, watchdog selector filtering, and unified-exec compaction filtering; `just fmt` and `just argument-comment-lint` passed; `cargo build --release -p codex-cli` passed; `/build/frodex-rebase/frodex --version` prints `codex-cli 0.0.0`; rebuilt binary sha256 is `6b7678e3897182d0fe8bea15c69e2f94b69f688fe650b9d96f75b373d37b57d1`.

Caveat: full `cargo test -p codex-tui` still has unrelated snapshot drift where the fixture footer model slug changes from `gpt-5.3-codex` to `gpt-5.4`; plain execution without `RUST_MIN_STACK=8388608` also hit an existing stack overflow in one TUI test binary. No `.snap.new` artifacts are retained.

Disposition: advance to stacked-branch validation / watchdog end-to-end run when a live TTY is available.

## 2026-04-24T11:35:00Z - Final Live TTY Validation

Outcome: corrected the first rebuild after backend rejection of namespace-level `defer_loading`. The final implementation serializes `defer_loading` on the watchdog namespace member functions, not on the namespace object itself, which preserves deferred discovery without sending unsupported `tools[].defer_loading` fields. Rebuilt `/build/frodex-rebase/frodex` with SHA256 `98ab2969a73b49ac75ef0531af4338282ef47642e297c24818cc68c1868bf801`.

Evidence: `CODEX_MATERIALIZE_EPHEMERAL_ROLLOUTS=1 /build/frodex-rebase/frodex "$(cat ~/frodex-watchdog-test.md)"` ran under a live TTY. Session artifacts under `~/.codex/sessions/2026/04/24` show an early `watchdog.snooze` call, synthetic `tool_search` output for the deferred `watchdog` namespace with all child functions marked `defer_loading: true`, ping/pong turns `ping 63 (63)` / `pong 46 (109)`, `ping 95 (204)` / `pong 41 (245)`, `ping 62 (307)` / `pong 49 (356)`, then `watchdog.watchdog_self_close` with message `goodbye`. The root turn reported that the watchdog said `goodbye` and that root did not call `close_agent`.

Disposition: done for this workstream; ready for branch review/commit and any broader stacked-branch release steps.

## 2026-04-24T19:20:00Z - Deferred Watchdog Search and Max-Depth Collab Repair

Outcome: completed the follow-up repair for watchdog tool deferral and max-depth multi-agent visibility. Watchdog namespace tools are now registered locally but omitted from model-visible top-level tools; `tool_search` includes the watchdog namespace only for watchdog check-in threads. Non-watchdog `tool_search` cannot discover watchdog tools, and `compact_parent_context`, `snooze`, and `watchdog_self_close` all reject non-watchdog callers. Removed the max-depth `Feature::Collab` disable path so watchdog/forked handlers still see `spawn_agent`, `send_input`, `resume_agent`, `wait_agent`, and `close_agent`; runtime depth checks remain responsible for returning errors when spawning/resuming would exceed depth.

Evidence: `cargo test -p codex-core watchdog` passed 31 watchdog-focused tests; `cargo test -p codex-core depth_limit` passed depth-limit runtime tests; `cargo test -p codex-tools watchdog` passed watchdog registry-plan tests; `just fmt` passed; `just fix -p codex-core` and `just fix -p codex-tools` passed with only the pre-existing `thread_manager.rs` too-many-arguments warning on core; `just argument-comment-lint` passed repo-wide. `cargo build --release -p codex-cli` passed and `/build/frodex-rebase/frodex` was atomically replaced because the old executable was busy. New binary SHA256: `5ddf82941ff868f4f2add6fcd3c94fc25f1c3ed76edc0098a4e168f87df5864d`; `/build/frodex-rebase/frodex --version` prints `codex-cli 0.0.0`.

Disposition: complete for the requested watchdog deferral and multi-agent visibility repair; ready for another live token-dump comparison or watchdog smoke if desired.

## 2026-04-24T19:31:00Z - Live TTY Watchdog Smoke Passed on Final Binary

Outcome: ran the rebuilt `/build/frodex-rebase/frodex` under a live TTY with materialized ephemeral rollouts. The watchdog lifecycle completed successfully: the first check-in snoozed, three subsequent helper check-ins sent ping messages, the root replied with pong messages and status checks, and the fourth watchdog check-in used `watchdog.watchdog_self_close` with message `goodbye`. The root reported: "The watchdog closed itself. I did not close it."

Evidence: command `CODEX_MATERIALIZE_EPHEMERAL_ROLLOUTS=1 /build/frodex-rebase/frodex "$(cat ~/frodex-watchdog-test.md)"` ran to completion. Rollouts under `~/.codex/sessions/2026/04/24` show `synthetic_watchdog_tool_search` returning the deferred `watchdog` namespace with child functions marked `defer_loading: true`; `rollout-2026-04-24T19-13-15-019dc0e9-1947-7f71-a3f8-658e130974ef.jsonl` shows `watchdog.snooze`; helper rollouts sent `ping 12 (12)`, `ping 57 (77)`, and `ping 31 (195)`; the root replied `pong 8 (20)`, `pong 87 (164)`, and `pong 93 (288)`; `rollout-2026-04-24T19-18-23-019dc0ed-cd5c-7c82-b8e9-197c1366bd86.jsonl` shows `watchdog.watchdog_self_close` with `{"message":"goodbye"}`. Current validated binary SHA256 remains `5ddf82941ff868f4f2add6fcd3c94fc25f1c3ed76edc0098a4e168f87df5864d`.

Disposition: complete for requested watchdog deferred-tool, max-depth multi-agent visibility, and live smoke validation work; ready for branch review/commit.

## 2026-04-24T20:05:00Z - Root Watchdog Namespace Prefix Parity Repair

Outcome: addressed the refreshed token-dump delta where watchdog handlers had a top-level `watchdog` namespace before `tool_search` while the root prompt did not. The registered built-in watchdog namespace is now included in model-visible `Prompt.tools` for all agents when `agent_watchdog` is enabled. Authorization remains runtime-enforced: non-watchdog callers still receive watchdog-only errors, and regular `tool_search` still does not discover watchdog tools unless the thread is a watchdog check-in.

Evidence: refreshed dumps showed first divergence at line 684 with root entering `tool_search` while watchdog entered `watchdog`. Removed the model-visible filter in `codex-rs/core/src/tools/router.rs` and updated visibility tests in `codex-rs/core/src/tools/spec_tests.rs`. `cargo test -p codex-core watchdog` passed 31 watchdog-focused tests plus the prompt-config integration filter; `just fmt`, `just fix -p codex-core`, and `just argument-comment-lint` passed, with only the pre-existing `thread_manager.rs` too-many-arguments warning. `cargo build --release -p codex-cli` passed and `/build/frodex-rebase/frodex` was replaced. New binary SHA256: `b9d7601ba63dd4db334d93dd8e1bcad583d092c08995475d994208555aeca7ba`; `/build/frodex-rebase/frodex --version` prints `codex-cli 0.0.0`.

Disposition: complete for the remaining watchdog namespace prefix diff; needs fresh token dumps to confirm byte-level prefix parity in the backend.

## 2026-04-24T20:45:00Z - Watchdog Namespace Restored to Dynamic-Only Surface

Outcome: corrected the prior root-prefix experiment. The built-in `watchdog` namespace is again registered for local dispatch but filtered from model-visible `Prompt.tools` for every agent, including watchdog check-in helpers. Watchdog lifecycle tools surface only through the watchdog-only `tool_search` path; ordinary agents still cannot discover them via `tool_search`, and runtime handlers still reject non-watchdog callers.

Evidence: restored the router model-visible filter for the built-in watchdog namespace and reverted the spec tests to assert registered-but-not-model-visible behavior. `cargo test -p codex-core watchdog` passed 31 watchdog-focused tests and the prompt-config integration filter; `just fmt`, `just fix -p codex-core`, and `just argument-comment-lint` passed, with only the pre-existing `thread_manager.rs` too-many-arguments warning. `cargo build --release -p codex-cli` passed and `/build/frodex-rebase/frodex` was replaced. Binary SHA256: `5ddf82941ff868f4f2add6fcd3c94fc25f1c3ed76edc0098a4e168f87df5864d`; `/build/frodex-rebase/frodex --version` prints `codex-cli 0.0.0`.

Disposition: complete for dynamic-only watchdog tool exposure; fresh backend token dumps should show neither root nor watchdog receives the top-level `watchdog` namespace in the initial tools list, while watchdog helpers still receive synthetic `tool_search` discovery output.

## 2026-04-24T23:25:44Z - Watchdog Member-Tool Deferral Implemented at Prompt Boundary

Outcome: corrected the prior namespace-level filtering approach. The synthetic watchdog `tool_search` injection remains intact. The `watchdog` namespace itself no longer carries namespace-level `defer_loading`; only `compact_parent_context`, `watchdog_self_close`, and `snooze` carry `defer_loading: true`. Prompt assembly now strips any deferred function tool, including namespace member functions, from initial Responses API `tools` payloads. This makes watchdog tools behave like other dynamic deferred tools: registered for local dispatch, discoverable to authorized watchdog handlers through `tool_search`, and absent from the initial tool-definition prefix for both root and watchdog requests.

Evidence: `cargo test -p codex-core watchdog` passed 31 watchdog-focused tests plus the prompt-config integration filter when rerun with port access; `cargo test -p codex-core filter_deferred_tool_spec` passed the new prompt-boundary deferred-tool tests; `cargo test -p codex-tools watchdog` passed watchdog registry-plan tests. `just fmt`, `just fix -p codex-core`, `just fix -p codex-tools`, and `just argument-comment-lint` passed, with only the pre-existing `codex-rs/core/src/thread_manager.rs:929` too-many-arguments warning during core clippy. `cargo build --release -p codex-cli` passed after enabling network for the missing `rusty_v8` archive. `/build/frodex-rebase/frodex` was rebuilt and installed with SHA256 `6ae8383ec76c025adbe37c065a68bc1ab2e0802ae9eb062189f1860b1023ea0d`; `/build/frodex-rebase/frodex --version` prints `codex-cli 0.0.0`.

Disposition: complete for the requested dynamic-deferred watchdog tool behavior; next validator is a fresh backend token dump confirming the top-level watchdog namespace is absent from initial tool lists and present only through synthetic watchdog `tool_search`.

## 2026-04-24T23:36:50Z - Live TTY Watchdog Smoke on Dynamic Deferred Binary

Outcome: ran the rebuilt `/build/frodex-rebase/frodex` under a live TTY with materialized ephemeral rollouts using `CODEX_MATERIALIZE_EPHEMERAL_ROLLOUTS=1 /build/frodex-rebase/frodex -c model_reasoning_effort=low -m gpt-5.4-ultrafast "$(cat ~/frodex-watchdog-test.md)"`. The watchdog path exercised periodic helper wakeups, synthetic watchdog discovery, ping/pong messages, and final `goodbye`. The TUI displayed `Closed Singer [watchdog]` followed by `Agent message: goodbye from /root/watchdog`, then the root explicitly called close after it could not verify self-closure from the handle and reported: `I closed the watchdog. close_agent returned previous status pending_init, so it had not been observed as self-closed when I shut it down.`

Evidence: live TTY output included `ping 1 (1)`, `pong 1 (2)`, `ping 1 (3)`, `pong 1 (4)`, `ping 1 (5)`, `pong 1 (6)`, `Closed Singer [watchdog]`, `Agent message: goodbye from /root/watchdog`, then an explicit close-agent report with previous status `pending_init`. The final binary remained `/build/frodex-rebase/frodex` SHA256 `6ae8383ec76c025adbe37c065a68bc1ab2e0802ae9eb062189f1860b1023ea0d`; TTY session token accounting reported `387,328 cached` input tokens.

Disposition: binary validation passed, and the watchdog exercised deferred synthetic tool discovery plus ping/pong behavior. The smoke exposed a remaining lifecycle-observation caveat: the model/root did not reliably observe the persistent watchdog handle as self-closed and closed it explicitly even after `goodbye` arrived.

## 2026-04-25T01:49:11Z - Duplicate Watchdog Namespace Root Cause Fixed

Outcome: fixed the actual duplicate-source bug behind the refreshed backend token dump. Prompt-boundary filtering was not sufficient because `codex-rs/tools/src/tool_registry_plan.rs` still pushed the built-in `watchdog` namespace as a normal registry-plan spec when `agent_watchdog` was enabled. Watchdog handlers also enable watchdog-only synthetic `tool_search`, so they received the namespace once from the initial Responses API tools payload and once from synthetic discovery. The registry plan now registers watchdog local dispatch handlers without adding a model-visible namespace spec; synthetic watchdog `tool_search` remains the only path that surfaces the namespace.

Evidence: updated `codex-rs/tools/src/tool_registry_plan.rs`, `codex-rs/tools/src/tool_registry_plan_tests.rs`, and `codex-rs/core/src/tools/spec_tests.rs` to assert that `router.specs()` and `router.model_visible_specs()` do not contain `watchdog` before `tool_search`, while watchdog handlers still have dispatch handlers and can discover watchdog tools through synthetic `tool_search`. Validators passed: `git diff --check`, `cargo test -p codex-tools watchdog`, `cargo test -p codex-core watchdog_namespace_is_not_model_visible_before_tool_search`, `cargo test -p codex-core watchdog_handlers_see_collab_tools_and_discover_watchdog_namespace`, `cargo test -p codex-core watchdog`, `cargo test -p codex-core filter_deferred_tool_spec`, `just fmt`, `just fix -p codex-core`, `just fix -p codex-tools`, and `just argument-comment-lint`. `cargo build --release -p codex-cli` rebuilt and installed `/build/frodex-rebase/frodex` with SHA256 `c7b7d9e673cd12c2ad1a580ac83cd49b57572d897247c6491b16137565afa23c`; `/build/frodex-rebase/frodex --version` prints `codex-cli 0.0.0`.

Disposition: complete for the duplicate initial-tool watchdog namespace exposure. The remaining confirmation step is a fresh backend token dump from the rebuilt binary showing the initial Responses API tools payload lacks the top-level `watchdog` namespace and the namespace appears only in the synthetic watchdog `tool_search` result.

## 2026-04-25T02:05:00Z - Preregister Watchdog Deferred-Source Refactor

Preregistration: replace the watchdog-specific boolean plumbing with an explicit deferred tool-search source path analogous to deferred MCP tools. Responsible agent: root. Start commit: current uncommitted worktree on `refresh/20260423/collab-stack-rebase`. Mutable surface: `codex-rs/tools/src/tool_registry_plan_types.rs`, `codex-rs/tools/src/tool_registry_plan.rs`, `codex-rs/core/src/tools/tool_search_entry.rs`, `codex-rs/core/src/tools/spec.rs`, focused tests, and this workstream ledger. Validator: focused `cargo test -p codex-tools watchdog`, focused `cargo test -p codex-core watchdog_namespace_is_not_model_visible_before_tool_search`, focused `cargo test -p codex-core watchdog_handlers_see_collab_tools_and_discover_watchdog_namespace`, `just fmt`, and scoped `just fix` if Rust changes compile.

Disposition: advance

## 2026-04-25T02:33:00Z - Watchdog Modeled as Deferred Tool-Search Source

Outcome: refactored the watchdog discovery path to match the deferred MCP split more closely. The normal registry plan receives a generic `deferred_tool_search_sources` list for `tool_search` source labels, while the runtime `ToolSearchHandler` receives a matching built-in deferred source enum whose `Watchdog` entry materializes the loadable `watchdog` namespace. The initial Responses API tool list still does not include the top-level `watchdog` namespace; the namespace remains present in the synthetic/tool_search loadable output, with all watchdog member tools marked `defer_loading: true`.

Evidence: changed `codex-rs/tools/src/tool_registry_plan_types.rs`, `codex-rs/tools/src/tool_registry_plan.rs`, `codex-rs/core/src/tools/spec.rs`, and `codex-rs/core/src/tools/tool_search_entry.rs`; added `search_tool_registers_for_deferred_tool_search_sources` and kept watchdog discovery tests. Validators passed: `cargo test -p codex-tools deferred_tool_search_sources`, `cargo test -p codex-tools watchdog`, `cargo test -p codex-core watchdog`, `just fmt`, `cargo test -p codex-core filter_deferred_tool_spec`, `just fix -p codex-core`, `just fix -p codex-tools`, `just argument-comment-lint`, and `git diff --check`. `cargo build --release -p codex-cli` rebuilt and installed `/build/frodex-rebase/frodex` with SHA256 `9a3cf614785f8fdebfe67e5a35158a6d58289e7d540531db8ed7b220676cc650`; `/build/frodex-rebase/frodex --version` prints `codex-cli 0.0.0`.

Disposition: complete for MCP-style watchdog deferred discovery. Remaining external confirmation is a fresh backend token dump showing the `tool_search` source label includes `watchdog`, the initial tools payload lacks the top-level namespace, and the synthetic/tool_search output contains the loadable `watchdog` namespace once.

## 2026-04-25T16:40:00Z - Preregister Eager Watchdog Workaround and Backend Source Investigation

Preregistration: bite the bullet and make watchdog lifecycle tools eager initial tools while removing synthetic watchdog `tool_search`, then investigate OpenAI backend source under `/home/dev-user/code/openai` for why forked conversation/session IDs appear to cause backend tool-schema reinjection that differs from a single conversation. Responsible agent: root. Start commit: current uncommitted worktree on `refresh/20260423/collab-stack-rebase`. Mutable surface: watchdog tool construction/registry, watchdog bootstrap history injection, focused tests, AutoPlan ledgers, and read-only backend source analysis. Validator: request/tool-surface tests showing the watchdog namespace is eager exactly once and synthetic watchdog `tool_search` is absent; focused watchdog tests; `just fmt`; scoped `just fix`; release rebuild.

Disposition: advance

## 2026-04-25T18:15:00Z - Eager Watchdog Workaround Built and Backend Hypothesis Supported

Outcome: implemented the requested workaround after the backend token dump continued showing the watchdog namespace twice for forked watchdog handlers. Frodex now registers `watchdog.compact_parent_context`, `watchdog.watchdog_self_close`, and `watchdog.snooze` as eager namespace tools in the normal initial Responses API tool list when `agent_watchdog` is enabled. The synthetic watchdog `tool_search` bootstrap was removed, and obsolete watchdog deferred-source plumbing was removed. Runtime authorization remains enforced in the watchdog handlers, so non-watchdog callers still receive watchdog-only errors even though the schema is eagerly present.

Backend investigation: OpenAI backend source supports the fork/new-conversation hypothesis. `response_creator_service.py` merges client `tool_search_output` payloads into effective tools both from hydrated context and from request inputs (`_build_effective_tools`, `_merge_loaded_tools_from_request_inputs`), and `_merge_loaded_tools_into_effective_tools` activates loaded tools with `defer_loading=false` while de-duplicating namespaces against the current effective tool set. `tool_search_replay_processor.py` also reconstructs function namespaces for persisted `ToolSearchOutput` items. This means Codex's synthetic watchdog tool-search output was not just inert context: on a forked/new response context it could become an additional backend tool-schema source, matching the observed duplicate namespace injection. The eager workaround removes that second source.

Evidence: validators passed: `cargo test -p codex-tools watchdog`, `cargo test -p codex-core watchdog`, `cargo test -p codex-core filter_deferred_tool_spec`, `just fmt`, `just fix -p codex-core`, `just fix -p codex-tools`, `just argument-comment-lint`, and `git diff --check`. `just fix -p codex-core` still reports the pre-existing `core/src/thread_manager.rs:929` too-many-arguments warning; the release build still reports the pre-existing app-server unused constant warning. `cargo build --release -p codex-cli` completed and `/build/frodex-rebase/frodex` was rebuilt with SHA256 `a5b1c6bef8492109e3470a61e0c9b1a34bc0a35a9dd3bba387d22b5e3d94a41a`; `/build/frodex-rebase/frodex --version` prints `codex-cli 0.0.0`.

Disposition: complete for the eager watchdog workaround and backend source investigation. Next external confirmation is a fresh backend token dump showing exactly one eager `watchdog` namespace in both root and watchdog initial tool schemas, with no synthetic watchdog `tool_search` output.

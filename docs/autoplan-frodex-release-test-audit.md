# Frodex Release Test Audit AutoPlan

## Purpose

This AutoPlan controls the audit of the Frodex release branch at `frodex-v0.122.0-alpha.10-frodex.3`. The user-visible goal is that every Frodex behavior restored or changed by the release stack has a regression or conformance test that proves the behavior at the right boundary. For protocol and Responses work, tests must assert request items, rollout items, or externally visible tool output rather than only private helper details.

## Goal

Review every commit unique to `refresh/20260418/collab-stack-release` after the current upstream merge-base and ensure its behavior is either already covered by an appropriate test or receives a new focused test. Missing coverage must be fixed in the branch, validated with targeted Rust tests, and recorded in the relevant workstream ledger.

Unique commits under audit:

1. `7d55fd785a` Inherit forked agent prompt cache keys
2. `b6c04adf99` Snapshot MCP tools for forked agents
3. `0521ec3973` Add watchdog runtime handles
4. `d922d305a3` Add watchdog mailbox wakeup fallback
5. `d0c9b82cce` Add watchdog namespace tools
6. `f6320c8885` feat(exec): add --fork option to codex exec
7. `c85dc2884d` test(exec): annotate fork option literals
8. `52ad779fe0` test(cli): annotate remote and color literals
9. `c03bb5e599` fix(exec): preserve prompt stdin append behavior
10. `75318ecaba` Restore frodex release workflow
11. `d952829ad3` ci: make frodex release git fetches use cli
12. `677b05bf0f` Restore TUI subagent status panel
13. `4430b27c39` Restore Frodex watchdog defaults and custom models
14. `ee7700ce4d` Keep watchdogs alive after owner turns complete
15. `707ac54ebf` Trigger watchdog after owner first goes idle
16. `add4a0d0f8` Restore subagent inbox transcript cells
17. `f84089fc57` Use inter-agent communication for subagent messages
18. `8476e9427c` Fork watchdog check-in helpers
19. `4e6338d7f9` Avoid MCP startup in watchdog helpers
20. `0ab67b8cea` Preload watchdog helper control tools
21. `e409995413` Close watchdog handle on goodbye fallback
22. `83d6ddb19a` Restore Frodex watchdog and prompt behavior

## Evaluation Mode

Deterministic. Evidence is source inspection, focused unit/integration tests, snapshot tests, and where needed structured assertions against mocked Responses API requests or rollout JSON item formats.

## Acceptance Evidence

- Each commit above is mapped to one or more behaviors in a workstream ledger.
- Each commit has a checklist entry, not only a paragraph summary.
- Each commit checklist has sub-checks for:
  - code paths read by an agent,
  - behavior protected,
  - regression or conformance test names and file paths,
  - whether the strongest relevant test reaches the Responses API request, rollout item, app-server event, or TUI snapshot boundary,
  - validator command and result,
  - remaining gap, if any.
- Each behavior is marked `covered`, `new-test-added`, or `not-testable-here` with a concrete reason.
- For any new code changes, targeted crate tests pass.
- For TUI-visible changes, snapshot coverage is present or explicitly justified.
- For Responses API item-format behavior, at least one test inspects the structured outbound request or rollout item representation rather than relying only on helper return values.
- The root ledger records final reconciliation and remaining risks.

## Mutable Surface

Primary source/test areas:

- `codex-rs/core/src/agent/*`
- `codex-rs/core/src/session/*`
- `codex-rs/core/src/tools/handlers/multi_agents*`
- `codex-rs/core/src/thread_manager*`
- `codex-rs/core/src/config*`
- `codex-rs/core/*_prompt.md`
- `codex-rs/tui/src/chatwidget*`
- `codex-rs/tui/src/subagent_panel.rs`
- `codex-rs/tui/src/history_cell.rs`
- `codex-rs/models-manager/*`
- `codex-rs/protocol/*`
- `.github/workflows/frodex-release.yml`
- `docs/frodex-feature-retention.md`

Workers must keep edits inside their assigned workstream unless the root records a scope change.

## Iteration Unit

One iteration is: audit a bounded commit group, identify behavior-level coverage gaps, add or improve tests for the gaps, run the fastest meaningful validator, and append an outcome entry with a disposition.

## Loop Budget

Run all five active workstreams in parallel where possible. The root should reconcile results after each worker returns or blocks. Stop only when all commit groups are mapped and either covered or explicitly out of scope.

## Workstream Board

| Workstream | Status | Responsible agent | Dependency / blocker | Plan | Ledger | Worktree / branch | Next step | Latest disposition |
|---|---|---|---|---|---|---|---|---|
| Fork/cache/exec | complete | Helmholtz (`019db628-d1a4-7650-ba0e-30483538f682`) | none | `docs/workstreams/active/fork-cache-exec/plan.md` | `docs/workstreams/active/fork-cache-exec/ledger.md` | `/build/frodex-worktrees/test-audit/fork-cache-exec` / `audit/fork-cache-exec` | none | Commit checklist and tests integrated in `6c54cb9eb8`; final focused validators passed. |
| Watchdog runtime/tools | complete | Volta (`019db628-d120-7c60-8209-cefb03bd9121`) | none | `docs/workstreams/active/watchdog-runtime-tools/plan.md` | `docs/workstreams/active/watchdog-runtime-tools/ledger.md` | `/build/frodex-worktrees/test-audit/watchdog-runtime-tools` / `audit/watchdog-runtime-tools` | none | Commit checklist and tests integrated in `190ff4cd6e`; final focused validators passed. |
| Prompt/config/models | complete | Franklin (`019db628-d2eb-7720-a4ee-8a77ee52c997`) | none | `docs/workstreams/active/prompt-config-models/plan.md` | `docs/workstreams/active/prompt-config-models/ledger.md` | `/build/frodex-worktrees/test-audit/prompt-config-models` / `audit/prompt-config-models` | none | Commit checklist and tests integrated in `33ce5485cd`; final focused validators passed. |
| TUI agent surface | complete | Ptolemy (`019db628-d478-7543-b7d9-3ea444162ef1`) | none | `docs/workstreams/active/tui-agent-surface/plan.md` | `docs/workstreams/active/tui-agent-surface/ledger.md` | `/build/frodex-worktrees/test-audit/tui-agent-surface` / `audit/tui-agent-surface` | none | Commit checklist and snapshots integrated in `b009a0e2b7`; final focused validators passed. |
| Release/doc coverage | complete | Noether (`019db628-d57f-79f1-85d1-ac0c0ce82428`) | none | `docs/workstreams/active/release-doc-coverage/plan.md` | `docs/workstreams/active/release-doc-coverage/ledger.md` | `/build/frodex-worktrees/test-audit/release-doc-coverage` / `audit/release-doc-coverage` | none | Commit checklist and docs integrated in `d5e06a94c8`; root expanded checklist shape after integration. |

## Pivot Rules

- If two workstreams need to edit the same test helper or module, the root serializes those edits and records the new owner.
- If a worker finds an implementation bug while writing a test, it should fix the bug only if the fix is inside its assigned mutable surface. Otherwise it records a blocker.
- If a full crate test is too slow, first add or run a narrower test, then promote to crate-level validation before final acceptance.

## Stop Conditions

Stop when every unique release commit is accounted for and validators pass, or when a blocker requires user input because it changes the requested release behavior.

## References

- `docs/frodex-feature-retention.md`
- `docs/loop-ledger-frodex-release-test-audit.md`
- Workstream plans and ledgers under `docs/workstreams/active/`

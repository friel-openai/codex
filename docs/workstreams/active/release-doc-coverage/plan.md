# Release and Documentation Coverage Workstream

## Scope

Audit commits `c85dc2884d`, `52ad779fe0`, `75318ecaba`, and `d952829ad3`, and verify `docs/frodex-feature-retention.md` names the retention process well enough to prevent future stacked-release feature loss.

Expected behaviors:

- Literal argument comment lint remains clean for touched exec/CLI tests.
- Frodex release workflow checks out/fetches correctly through the GitHub CLI path.
- Release assets are produced for all supported platforms.
- Feature-retention documentation captures RCA, prevention rules, and verification expectations for stacked branch releases.

## Mutable Surface

- `.github/workflows/frodex-release.yml`
- CLI/exec tests changed by annotation commits
- `docs/frodex-feature-retention.md`
- AutoPlan ledgers and summary tables.

## Validator

Fastest useful check: inspect workflow plus run `just argument-comment-lint` if relevant changes are made.

Main validator: documentation and workflow review, plus focused lint/test checks for any touched code.

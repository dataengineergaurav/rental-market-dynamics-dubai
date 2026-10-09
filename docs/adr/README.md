# Architecture Decision Records

Each ADR captures one decision that was expensive to reverse, with the context, the
alternatives that were rejected, and what the choice cost. Read the **Status** column first —
a superseded ADR is not wrong, it is history.

> **Why the series starts at 06.** ADR-01 … ADR-05 were written inside
> `docs/IMPLEMENTATION_PLAN.md`, an internal document that is `.gitignore`d and not tracked
> here. The tracked series therefore begins at 06; the earlier numbers are cited by the
> older ADRs as historical context (e.g. "ADR-01's streaming constraint",
> "ADR-02's 1-registration grain", "ADR-03 fail-open") but their texts are not in this repo.

## Index

| ADR | Title | Date | Status |
|-----|-------|------|--------|
| [0006](0006-pydantic-silver-contract.md) | Pydantic v2 for the per-row Silver contract | 2026-09-27 | Accepted |
| [0007](0007-weekly-cross-file-dedup-and-freshness-gate.md) | Cross-file dedup and a freshness gate in the weekly build | 2026-10-05 | ⚠️ Superseded by [ADR-10](0010-cumulative-combined-layers-duckdb.md) |
| [0008](0008-layered-bronze-silver-gold-releases.md) | Bronze / Silver / Gold published as separate layer releases | 2026-10-06 | ⚠️ Superseded by [ADR-10](0010-cumulative-combined-layers-duckdb.md) |
| [0009](0009-release-hardening-raw-schema-and-markers.md) | Release hardening — pin the raw schema, mark no-data days, one publish path | 2026-10-06 | Accepted |
| [0010](0010-cumulative-combined-layers-duckdb.md) | Daily cumulative Silver+Gold in one DuckDB | 2026-10-06 | Accepted |
| [0011](0011-reproducibility-and-ci-quality-gates.md) | Reproducible dependencies + a CI quality gate | 2026-10-08 | Accepted |

## The story in order

1. **ADR-06** chose pydantic v2 to validate each registration row individually, so a bad
   value is *named* (`annual_amount_below_min`) instead of vanishing into an aggregate.
2. **ADRs-07 and -08** built a *weekly* world: pool seven daily files, dedup across them
   with a `row_hash`, gate on freshness, ship Silver (tables) and Gold (views) as two
   separate DuckDBs. Correct, but it synthesized a week out of daily files.
3. **ADR-09** hardened the release surface: pin the entire 44-column raw schema, publish a
   status marker on *every* run so "quiet day" and "job never ran" are distinguishable, and
   funnel all publishing through one client.
4. **ADR-10** replaced the whole weekly apparatus with a **single cumulative DuckDB** updated
   daily by primary-key upsert. It deletes more code than it adds and is the decision that
   defines the current system. See [ARCHITECTURE](../ARCHITECTURE.md).
5. **ADR-11** made the environment reproducible (`uv.lock`) and added the CI quality gate
   (`make lint`, `make coverage`) that now blocks a push.

## Related design artifacts

- [Silver contract design spec](../superpowers/specs/2026-09-27-silver-contract-design.md) —
  the field-by-field rationale, payload defects, and the market-health gate (approved
  2026-09-27; predates ADRs-07…-10, so prefer the ADRs where they disagree).
- [Silver contract implementation plan](../superpowers/plans/2026-09-27-silver-contract.md) — the
  task-by-task TDD plan that shipped ADR-06. Historical.

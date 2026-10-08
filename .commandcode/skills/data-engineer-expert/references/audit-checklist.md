# Audit checklist — signal → good → fix

Scan the repo for each signal. Report misses as P0/P1/P2 per the severity rubric in `SKILL.md`.

| # | Principle | Signal to look for | "Good" looks like | If missing |
|---|-----------|--------------------|-------------------|------------|
| 1 | Idempotency | upsert/merge vs append | PK + `INSERT OR REPLACE` / merge key | **P0** — non-idempotent writes duplicate on re-run |
| 2 | Contract | typed boundary; quarantine bucket | every row in valid/quarantined; named violations | **P0** — silent drop of bad rows |
| 3 | Metrics | median + `n` on every aggregate | medians, `n`, documented exclusions | **P1** — untrustworthy headline |
| 4 | Freshness | `_meta`/watermark + max-lag gate | readable `data_through`, fail on stall | **P1** — stale data ships silently |
| 5 | Run status | per-run status record | machine-readable outcome + row count | **P1** |
| 6 | fail-open/loud | documented per stage | ADR states which stages are fail-open | **P1** — a swallow hides a broken gate |
| 7 | Tests | schema/dtype asserts, trap coverage | `frame.schema` asserted; edge fixtures | **P1** — dtype/flake regressions invisible |
| 8 | CI gates | format/lint/type/test/coverage in CI | all five on every change | **P2** (P1 if no tests in CI) |
| 9 | Lockfile | `uv.lock`/`poetry.lock` committed | whole-version pin via lock | **P1** — unpinned dep can break prod |
| 10 | One writer | one job owns each artifact/tag | no two workflows publish the same tag | **P0** — race clobbers a release |
| 11 | Schedules | documented order & dependency | downstream after upstream artifact | **P1** |
| 12 | Secrets | no secrets in repo/logs/artifacts | env/secret store; presence validated | **P0** if leaked, **P2** if unvalidated |
| 13 | ADRs | decision records | ADRs for architecture choices | **P2** |
| 14 | Docs sync | README/changelog updated with code | same PR updates docs | **P2** |
| 15 | Dead code / dup | one source per rule | shared helper, no copy-paste rule | **P2** |
| 16 | Magic numbers | named constants in one place | a single constant, documented | **P2** |
| 17 | Perf | columnar, streaming where needed | Parquet; streaming only if large | **P2** — avoid premature tuning |
| 18 | Lineage | layer semantics explicit | bronze/silver/gold documented | **P2** |

## How to run the audit fast

1. `pyproject.toml` / `requirements.txt` / `setup.cfg` — deps, pins, lockfile, lint/type config.
2. `.github/workflows/` — triggers, what each job runs, who publishes, debug flags.
3. The transform/loader entry points — upsert vs append, `try/except`, wall-clock in outputs.
4. The contract module — quarantine, named violations, `Field()` range checks at the boundary.
5. `tests/` — do they assert dtype/schema, and do they cover the traps or only the happy path?
6. `docs/adr/` + README — decisions recorded, docs in sync with behavior.

## Reporting

Severity-rank the findings. For each: what (with file:line), why (consumer/over-time impact), the
minimal fix, blast radius. Explicitly call out anything that should be **deleted** rather than added.

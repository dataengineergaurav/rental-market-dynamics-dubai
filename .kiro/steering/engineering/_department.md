---
inclusion: manual
name: engineering-department
description: Engineering department router for rental-market-dynamics-dubai. Load for any task about HOW the data moves — the medallion ETL, Ejari extraction, the weekly DuckDB + star schema, releases/ops, CI, and data-quality gates.
owner: "@dataengineergaurav"
department: engineering
last-verified: 2026-10-24
---

# Engineering Department

The Engineering department owns the *mechanics*: extraction, the Bronze→Silver→Gold
flow, the weekly DuckDB build, releases/ops, and the data-quality gates that
enforce the rules BI defines. All execution runs through `uv`/`make` per the
`uv-environment` steering rule.

## Shared context (holds for every Engineering leaf)

- **Pipeline order is fixed:** DOWNLOAD → TRANSFORM → ANALYZE → PUBLISH
  (`main` in `run_etl_pipeline.py`). The daily job is **fail-open**: an empty
  window is a success, recorded as `NO_NEW_DATA` + `etl_status.json`.
- **The live aggregate is weekly**, built by
  `lib/analysis/build_weekly_duckdb.py` into `rental_analytics_weekly_YYYYWxx.duckdb`
  (wired in `.github/workflows/weekly.yml`). There is NO `build_layers_duckdb.py`.
- **Idempotency:** daily tag `release-YYYY-MM-DD`, weekly tag `release-week-YYYYWxx`;
  `GitHubRelease.publish` reuses a same-day tag rather than duplicating.
- **Stack is fixed** to polars + duckdb + scrapy. A new dependency needs an ADR.
- **Raw dtypes are pinned once** (`RAW_RENTS_CSV_DTYPES`) so daily and weekly read
  the same raw CSV without drift.

## Routing table

| Leaf skill | Load when the task is about |
|---|---|
| `medallion-layer-contracts` | Bronze/Silver/Gold invariants, the weekly DuckDB, the star schema SQL, dedupe |
| `ejari-extraction` | Scrapy spider vs direct downloader, the 2-day window, fail-open / NO_NEW_DATA |
| `release-and-ops` | dated/weekly release tags, idempotent publish, the freshness gate, CI workflows |
| `data-quality-gates` | validators, PSF/bulk fixtures, the coverage ratchet, `make check` |

## Handoff map (cross-skill contracts)

- `ejari-extraction` → `medallion-layer-contracts` (Bronze CSV feeds Silver/Gold)
- `medallion-layer-contracts` → `release-and-ops` (the built artifact is published)
- `data-quality-gates` → `rent-figure-trust` (BI) (QA enforces the publish rule BI owns)

## References

- `run_etl_pipeline.py` — `main` — the DOWNLOAD→TRANSFORM→ANALYZE→PUBLISH entry point.
- `lib/analysis/build_weekly_duckdb.py` — `build_weekly_duckdb` — the live weekly aggregate builder.

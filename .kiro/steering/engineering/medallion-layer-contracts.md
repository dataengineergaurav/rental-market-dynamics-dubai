---
inclusion: manual
name: medallion-layer-contracts
description: The Bronze→Silver→Gold contracts for the Dubai rents pipeline — the weekly DuckDB build, cross-file dedupe, the star schema, and the invariants each layer owes the next. Load when working on the weekly aggregate, the dim/fact SQL, or layer correctness.
owner: "@dataengineergaurav"
department: engineering
last-verified: 2026-10-24
---

# Medallion Layer Contracts

Owns the medallion flow and the contracts between layers. The live aggregate is
**weekly**, not cumulative — one DuckDB file per ISO week.

## Contract

- **owns:** Bronze→Silver→Gold invariants, the weekly DuckDB build, the star schema.
- **hands-off-to:**
  - `release-and-ops` — once the weekly artifact is built, it is published.
  - `data-quality-gates` — the gate rules measured against the built layers.
- **routes-from:**
  - `ejari-extraction` — the Bronze daily CSV is the input to Silver/Gold.
  - `engineering/_department` — any layer-correctness task.

## Triggers

Weekly DuckDB, `gold_*` views, dim/fact SQL, cross-file dedupe, `row_hash`,
Silver contract, "why is this row double-counted", star schema grain.

## Workflow

- **Bronze → Silver.** Daily CSV is read with pinned dtypes (`RAW_RENTS_CSV_DTYPES`)
  so daily and weekly readers cannot infer different types from the same raw text.
  The Silver contract (`to_silver`) validates and quarantines; a broken contract
  fails the run rather than logging a skip.
- **Weekly pooling.** `build_weekly_duckdb` pools a Mon–Sun window of daily CSVs.
  Before enrichment it does two things it owes the Gold views:
  1. **Cross-file dedupe** (`dedupe_pooled` + `add_row_hash`): the 2-day extract
     window means a registration can appear in two consecutive files. A row is
     kept only in the earliest file its `row_hash` appears in. Within-file repeats
     survive — bulk blocks (Naif 79×1,540,471) repeat one `(area, amount)` on
     purpose and the bulk flag must count all of them.
  2. **Freshness gate** (`WEEKLY_FRESHNESS_GATE`): too few daily files, or a newest
     registration lagging the window, raises rather than publishing a stale artifact.
- **Gold.** The shipping views (`gold_area_median`, `gold_standard_lease`,
  `gold_top_metros_daily`) live in `build_weekly_duckdb.py` and carry the three
  exclusions (Hotel / Labor Camps on sub_type, Virtual Unit on the TYPE column,
  bulk groups). `gold_indexes.py` is a tested but UNWIRED reimplementation — do
  not describe it as production.
- **Star schema.** `ddl.sql` + `dim_*.sql` + `fact_rental_contract.sql`, grain =
  one contract.

## Gates

- Daily and weekly must read the raw CSV through the SAME pinned dtypes.
- Cross-file dedupe keys on `row_hash`; never dedupe within a file.
- The freshness gate raises — a stale window must not publish.
- There is NO `build_layers_duckdb.py` / `rents_layers.duckdb`. Cite the weekly module.

## References

- `lib/analysis/build_weekly_duckdb.py` — `build_weekly_duckdb` — the weekly aggregate builder.
- `lib/analysis/build_weekly_duckdb.py` — `dedupe_pooled` — cross-file dedupe by row_hash.
- `lib/analysis/build_weekly_duckdb.py` — `add_row_hash` — attaches Silver's fingerprint.
- `lib/config.py` — `RAW_RENTS_CSV_DTYPES` — pinned raw dtypes shared by daily + weekly.
- `lib/config.py` — `WEEKLY_FRESHNESS_GATE` — stale-window gate.
- `lib/analysis/fact_rental_contract.sql` — `fact_rental_contract` — the fact grain.

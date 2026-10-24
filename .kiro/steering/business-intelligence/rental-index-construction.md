---
inclusion: manual
name: rental-index-construction
description: How defensible rental indices are built from the feed — the Area Median Index and the Standard Lease Index, stratified-median now vs hedonic later, and the exclusions every index must apply. Folds in analyst SQL for the Gold index views. Load for any index/methodology task.
owner: "@dataengineergaurav"
department: business-intelligence
last-verified: 2026-10-24
---

# Rental Index Construction (+ analyst)

Owns index methodology: what a defensible index is, which rows feed it, and how
the shipped views build it. Analyst SQL for the index views is folded in.

## Contract

- **owns:** index methodology and the construction rules.
- **hands-off-to:**
  - `rera-rent-advisory` — an index band informs (but is not) the RERA cap.
  - `release-and-ops` (Engineering) — a movement report is a candidate release artifact.
- **routes-from:**
  - `business-intelligence/_department` — any index / methodology task.
  - `rent-figure-trust` — trusted, bulk-excluded rows are the index input.

## Triggers

Area Median Index, Standard Lease Index, hedonic, stratified median,
`gold_area_median`, `gold_standard_lease`, "build the index", index SQL.

## Methodology (verified against the shipped views)

- **Two indices, two jobs.**
  - **Area Median Index** (`gold_area_median`): one median per area on comparable,
    bulk-excluded stock with an n≥10 floor. The tested-but-unwired
    `build_area_median_index` in `gold_indexes.py` reproduces this rule in polars
    (it sorts by `median_rent`; the shipped view orders by `n DESC` — a known,
    intentional difference, not drift).
  - **Standard Lease Index** (`gold_standard_lease`): a Dubai-wide baseline on a
    standardized slice (standard annual lease, comparable stock).
- **Shared comparability.** `_comparable_rows` is the single filter both builders
  share: drop bulk groups (`count <= BULK_GROUP_MAX`), exclude Hotel/Labor Camps
  on the sub_type column and Virtual Unit on the TYPE column, require a positive
  non-null amount. This is why the two indices cannot drift on "what counts".
- **Stratified median now, hedonic later.** With a short window, a stratified
  median (per area × sub-type, n≥10, IQR) is the only defensible index. A hedonic
  model becomes defensible only with ~90 days of history; do not claim a trend or
  YoY on a handful of days.
- **The health gate is not an index.** `build_residential_market_index` is a
  measurement surface for the `GATE_*` bands, not a second published index.

## Analyst SQL (folded in)

```sql
-- Standard Lease Index baseline from the shipped view
SELECT * FROM gold_standard_lease;

-- Area Median Index, publishable rows only
SELECT area_name_en, n, median_rent
FROM gold_area_median
WHERE n >= 10
ORDER BY n DESC;
```

Query the weekly `rental_analytics_weekly_YYYYWxx.duckdb` through `uv`. The
`_meta` view carries the window and provenance the index covers — `week`,
`week_start`, `week_end`, `pooled_rows`, `deduped_rows`,
`row_hash_duplicates_removed`, `daily_files`/`expected_daily_files`/`missing_daily_files`,
`data_through` (per `build_weekly_duckdb.py`). Older local artifacts may carry a
smaller `_meta` schema; trust the current code. Cite it when quoting an index.

## Week-over-week movement (two-point measurement, NOT a trend)

`lib/analysis/wow_movement.py` compares two weekly artifacts and reports per-area
median movement. This does NOT violate the no-trend-on-thin-data gate below: it
is an honest TWO-POINT measurement of two already-gated `gold_area_median`
medians, bounded by the two `_meta.data_through` dates. The distinction is
load-bearing — a trend infers a direction from many noisy points; this states
the difference between exactly two defensible numbers and claims nothing beyond
them.

Rules it keeps:
- Include an area only when `n >= 10` in BOTH weeks (`compute_movement`).
- Exclusions are inherited from `gold_area_median`; no re-filtering of stock.
- NO mean-of-medians / city-wide aggregate — that would reintroduce the
  mean-over-median error.
- Areas entering/leaving the n≥10 set are reported (`set_changes`), never dropped.
- Run via `make movement` (two most recent weekly DuckDBs) or
  `uv run python -m lib.analysis.wow_movement --from <w1> --to <w2>`.

## Gates

- Every index applies `_comparable_rows` exclusions + the n≥10 floor.
- No hedonic / trend / YoY claim on under ~90 days of data. (Week-over-week
  movement is a two-point measurement, not a trend — see above.)
- Distinguish the published indices from the health-gate measurement surface.

## References

- `lib/analysis/gold_indexes.py` — `build_area_median_index` — tested polars Area Median Index.
- `lib/analysis/gold_indexes.py` — `_comparable_rows` — the shared exclusion filter.
- `lib/analysis/gold_indexes.py` — `build_residential_market_index` — the health-gate surface (not an index).
- `lib/analysis/gold_indexes.py` — `MIN_AREA_ROWS` — the n≥10 floor sourced from MARKET_METRICS.
- `lib/analysis/build_weekly_duckdb.py` — `build_weekly_duckdb` — builds the shipped gold_area_median / gold_standard_lease views.
- `lib/analysis/wow_movement.py` — `compute_movement` — gated two-point week-over-week movement.
- `lib/analysis/wow_movement.py` — `set_changes` — areas entering/leaving the n≥10 set.

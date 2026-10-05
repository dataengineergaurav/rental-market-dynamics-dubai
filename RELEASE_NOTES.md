# Dubai Rental Market Data — Release Notes

**Last updated:** 2026-10-05
**Data source:** Dubai Land Department (DLD) — Ejari rent transactions

This file describes the artifacts the pipeline currently publishes. Per-release history lives on
[GitHub Releases](https://github.com/dataengineergaurav/rental-market-dynamics-dubai/releases).

## Artifacts

### Daily — `rent_contracts_YYYYMMDD.csv` (tag `release-YYYY-MM-DD`)

Raw Ejari rent transactions for the day, extracted over an incremental 2-day window. One file per
data date, contract-level columns as returned by the endpoint (unit area, annual and contract
amounts, registration/start/end dates, area, property type/usage, nearest metro, parking, project).

### Weekly — `rental_analytics_weekly_YYYYWxx.duckdb` (tag `release-week-YYYYWxx`)

A 7-day Mon–Sun DuckDB built by `lib/analysis/build_weekly_duckdb.py`. Contents:

| Object | Type | Description |
|--------|------|-------------|
| `fact_rental_contract` | table | Enriched, cross-file deduped contracts for the week |
| `gold_area_median` | view | Per-area median/mean/min/max rent, `n >= 10`, market exclusions applied |
| `gold_standard_lease` | view | Single Dubai-wide Flat benchmark, 180–365-day leases |
| `gold_top_metros_daily` | view | Top-3 metros by contract count per registration day |
| `_meta` | view | Week label, row counts and freshness provenance for the build |

## Data guarantees

- **Cross-file dedup.** A registration can appear in two consecutive daily files (the extract window
  is 2 days). The weekly build drops rows repeating a `row_hash` first seen in an earlier daily
  file, so counts and medians are not double-counted. Within-file repeats — bulk registrations — are
  preserved.
- **Freshness gate.** The weekly build refuses to publish when a day in the window has no usable
  daily CSV, or when the newest registration trails the window end.
- **Medians over means**, with guards for bulk registrations and implausible unit areas, so headline
  rent figures stay trustworthy. Hotel and Labor Camps sub-types and Virtual Unit property types are
  excluded from area medians.

Inspect what a build was made of:

```sql
SELECT * FROM _meta;
-- week, week_start, week_end, pooled_rows, deduped_rows,
-- row_hash_duplicates_removed, daily_files, expected_daily_files,
-- missing_daily_files, data_through
```

## Quick start

```python
import duckdb

con = duckdb.connect("rental_analytics_weekly_2026W37.duckdb", read_only=True)

# Area medians (n >= 10, exclusions applied)
print(con.execute("SELECT * FROM gold_area_median ORDER BY median_rent DESC LIMIT 10").fetchall())

# Daily top-3 metros by contract volume
print(con.execute("SELECT * FROM gold_top_metros_daily").fetchall())
```

## Notes

- Daily artifacts are CSV; the weekly analytics database is DuckDB. Older releases described a
  large star-schema database (`rental_data.db`) that the current pipeline does not build.
- Currency: UAE Dirham (AED). Amounts are annual rent values.
- Data is provided by the Dubai Land Department for public use.

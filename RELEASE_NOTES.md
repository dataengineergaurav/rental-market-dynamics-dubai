# Dubai Rental Market Data — Release Notes

**Last updated:** 2026-10-06
**Data source:** Dubai Land Department (DLD) — Ejari rent transactions

This file describes the artifacts the pipeline currently publishes, organized as a
**bronze → silver → gold** flow. Per-release history lives on
[GitHub Releases](https://github.com/dataengineergaurav/rental-market-dynamics-dubai/releases).

## Artifacts

### Bronze (raw) — `rent_contracts_YYYYMMDD.csv` (tag `release-YYYY-MM-DD`, daily)

Raw Ejari rent transactions for the day, extracted over an incremental 2-day window. One file per
data date, contract-level columns as returned by the endpoint (unit area, annual and contract
amounts, registration/start/end dates, area, property type/usage, nearest metro, parking, project).
Untransformed — this is the source of truth every later layer is rebuilt from, and the immutable
dated history.

Each release also carries `etl_status.json` (`outcome`, `data_date`, window, `data_rows`). On a quiet
day with no registrations the release contains **only** the status file (`outcome: no_data`), so a
missing release means the job did not run while a status-only release means it ran and found nothing.

### Silver + Gold (one cumulative file) — `rents_layers.duckdb` (tag `release-layers-latest`, daily)

A single DuckDB that is **cumulative** and **updated every day**: the day's contracts are upserted
into it. It holds both the normalized Silver tables and the Gold analytics views, in the same file,
so a plain `duckdb.connect` resolves every view (no `ATTACH`).

| Object | Type | Description |
|--------|------|-------------|
| `FctContract` | table | One row per registration; measures + natural-key foreign keys. Keyed on `contract_id` |
| `DimArea` | table | One row per area (`area_name_en`, `area_tier`) |
| `DimPropertyType` | table | One row per (type, sub-type, usage) combination |
| `DimMetro` | table | One row per nearest-metro name |
| `_meta` | table | One row describing the current store (`built_at`, `data_from`, `data_through`, `total_contracts`, `last_ingested_window`) |
| `gold_area_median` | view | Per-area median/mean/min/max rent, `n >= 10`, market exclusions applied |
| `gold_standard_lease` | view | Single Dubai-wide Flat benchmark, 180–365-day leases |
| `gold_top_metros_daily` | view | Top-3 metros by contract count per registration day |
| `AggAreaRentStats` | view | Per-area rent distribution (`p10`, `p90`) and PSF, each with `n` |
| `AggMetroPremium` | view | Median-based metro premium vs the city median, with `n` |
| `AggMonthlyRegistrations` | view | Monthly contract count and total annual value — a trend over all history |
| `AggProjectRentStats` | view | Per-project median/mean rent, with `n` |

Keys are natural (`area_name_en`, type/sub-type/usage, metro name), not the upstream `*_ID` columns,
which are single-valued in the payload. The dimensions are rebuilt from the fact each run, so they
cannot drift.

## Data guarantees

- **Idempotent by primary key.** A registration can appear in two consecutive daily files (the
  extract window is 2 days). `FctContract` is keyed on `contract_id`, so re-ingesting a day — a
  same-day re-run, or the next day's overlapping window — replaces the row instead of duplicating it.
  Counts and medians are never double-counted, with no cross-file dedup code.
- **A stalled feed fails; a quiet day does not.** Ingest refuses when the newest registration trails
  the data date its filename claims by more than one day. The store's freshness is readable from
  `_meta.data_through`.
- **Medians over means**, with guards for bulk registrations and implausible unit areas, so headline
  rent figures stay trustworthy. Hotel and Labor Camps sub-types and Virtual Unit property types are
  excluded from area medians.

Inspect the store:

```sql
SELECT * FROM _meta;
-- built_at, data_from, data_through, total_contracts, last_ingested_window
```

## Quick start

The Silver tables and Gold views are in one file, so a plain connection resolves everything:

```python
import duckdb

con = duckdb.connect("rents_layers.duckdb", read_only=True)  # no ATTACH needed

# Area medians (n >= 10, exclusions applied)
print(con.execute("SELECT * FROM gold_area_median ORDER BY median_rent DESC LIMIT 10").fetchall())

# Monthly trend over all accumulated history
print(con.execute("SELECT * FROM AggMonthlyRegistrations").fetchall())

# Daily top-3 metros by contract volume
print(con.execute("SELECT * FROM gold_top_metros_daily").fetchall())
```

`lib.analysis.layers.connect_layers(path)` opens the file read-only, and
`python -m lib.analysis.metro_volume --db rents_layers.duckdb` is a worked example.

## Notes

- Amounts are UAE Dirham (AED) annual rent values.
- Naming follows the layers: `rent_contracts_*.csv` (bronze) and the combined `rents_layers.duckdb`
  (silver + gold). The pre-2026-10 weekly artifacts — `rental_analytics_weekly_*.duckdb`
  (`release-week-YYYYWww`) and the two-file `silver_*.duckdb` / `gold_*.duckdb`
  (`release-silver-*` / `release-gold-*`) — are superseded by the combined store.
- Data is provided by the Dubai Land Department for public use.

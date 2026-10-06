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
Untransformed and untyped — this is the source of truth every later layer is rebuilt from.

### Silver (normalized tables) — `silver_YYYYWww.duckdb` (tag `release-silver-YYYYWww`, weekly)

The 7-day Mon–Sun pool, cross-file deduplicated and enriched, normalized into a small star:

| Object | Type | Description |
|--------|------|-------------|
| `FctContract` | table | One row per deduplicated registration; measures + foreign keys to the dimensions |
| `DimArea` | table | One row per area (`area_name_en`, `area_tier`) |
| `DimPropertyType` | table | One row per (type, sub-type, usage) combination |
| `DimMetro` | table | One row per nearest-metro name (`nearest_metro` lives on the fact, not the area) |
| `_meta` | table | Week label, row counts and freshness provenance for the build |

Keys are natural (`area_name_en`, type/sub-type/usage, metro name), not the upstream `*_ID` columns,
which are single-valued in the payload.

### Gold (analytics views) — `gold_YYYYWww.duckdb` (tag `release-gold-YYYYWww`, weekly)

Views defined over the Silver tables. They resolve only while Silver is attached under the
`silver` alias (see below). No data is copied into the Gold file.

| View | Description |
|------|-------------|
| `gold_area_median` | Per-area median/mean/min/max rent, `n >= 10`, market exclusions applied |
| `gold_standard_lease` | Single Dubai-wide Flat benchmark, 180–365-day leases |
| `gold_top_metros_daily` | Top-3 metros by contract count per registration day |
| `AggAreaRentStats` | Per-area rent distribution (`p10`, `p90`) and PSF, each with `n` |
| `AggMetroPremium` | Median-based metro premium vs the city median, with `n` |
| `AggMonthlyRegistrations` | Monthly contract count and total annual value over deduplicated contracts |
| `AggProjectRentStats` | Per-project median/mean rent, with `n` |

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
SELECT * FROM _meta;   -- from silver_YYYYWww.duckdb
-- week, week_start, week_end, pooled_rows, deduped_rows,
-- row_hash_duplicates_removed, daily_files, expected_daily_files,
-- missing_daily_files, data_through
```

## Quick start

The Gold views read the Silver tables, so attach Silver under the alias `silver` first:

```python
import duckdb

con = duckdb.connect("gold_2026W37.duckdb", read_only=True)
con.execute("ATTACH 'silver_2026W37.duckdb' AS silver (READ_ONLY)")

# Area medians (n >= 10, exclusions applied)
print(con.execute("SELECT * FROM gold_area_median ORDER BY median_rent DESC LIMIT 10").fetchall())

# Daily top-3 metros by contract volume
print(con.execute("SELECT * FROM gold_top_metros_daily").fetchall())
```

`lib.analysis.layers.connect_gold(gold_path)` does the attach for you, and
`python -m lib.analysis.metro_volume --gold gold_2026W37.duckdb` is a worked example.

## Notes

- Amounts are UAE Dirham (AED) annual rent values.
- Naming follows the layers: `rent_contracts_*.csv` (bronze), `silver_*.duckdb` (silver),
  `gold_*.duckdb` (gold). The pre-2026-10 weekly artifact `rental_analytics_weekly_*.duckdb`
  (`release-week-YYYYWww`) is superseded by the Silver + Gold pair.
- Data is provided by the Dubai Land Department for public use.

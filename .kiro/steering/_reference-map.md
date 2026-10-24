---
inclusion: manual
name: skills-reference-map
owner: "@dataengineergaurav"
last-verified: 2026-10-24
---

# Skills Reference Map (verified ground truth)

This is the single verified anchor table every skill cites. It was produced by
reading the current code, not the old `SKILL.md`. When a skill cites a
`file:line`, the line must match a row here. `make skills-check` validates that
every cited `file:symbol` still resolves.

> Verification policy: cite `file.py` + a **symbol name** (function/class/const),
> not a bare line number, because line numbers drift. Where a line number is
> useful, treat the symbol as the source of truth and the line as a hint.

## BI / domain anchors

| Symbol | File | Verified location | Notes |
|---|---|---|---|
| `AreaTier` (enum) | `lib/config.py` | class at config.py:30 | values Premium / Mid-Tier / Budget / Emerging |
| `AREA_CLASSIFICATIONS` | `lib/config.py` | dict at config.py:50 | **placeholder** hardcoded map; ~30 areas only |
| `get_area_tier(area_name: str)` | `lib/config.py` | func at config.py:259 | returns `AREA_CLASSIFICATIONS.get(name, MID_TIER)` — unknown areas default Mid-Tier |
| `is_residential(usage: str)` | `lib/config.py` | func at config.py:289 | takes a **str**, returns `usage in RESIDENTIAL_USAGE`. The old "is_residential(Expr) always False" critique is **OUTDATED** — not an Expr, not always False |
| `is_commercial(usage: str)` | `lib/config.py` | func after config.py:289 | str-typed, `usage in COMMERCIAL_USAGE` |
| `RESIDENTIAL_USAGE` | `lib/config.py` | list at config.py:129 | owner of the residential membership set |
| `COMMERCIAL_USAGE` | `lib/config.py` | list at config.py:137 | |
| `VALIDATION_THRESHOLDS` | `lib/config.py` | dict after AREA_CLASSIFICATIONS | PSF gates live HERE (not at `:86`): `min_psf_residential 20`, `max_psf_residential 500`, `min_psf_commercial 30`, `max_psf_commercial 800`; `min_property_size 200`; rent 10k–5M; contract days 30–730 |
| `MARKET_METRICS["min_area_sample_size"]` | `lib/config.py` | dict `MARKET_METRICS` | **= 10**, the n>=10 floor. One owner; `gold_indexes.MIN_AREA_ROWS` reads it |
| `_add_psf` | `lib/transform/enrichment.py` | method at enrichment.py:88 | PSF null unless `actual_area >= 200` and `annual_amount > 0` |
| `_add_area_tier` | `lib/transform/enrichment.py` | method at enrichment.py:105 | `replace_strict(tier_map, default=Mid-Tier)` |
| `_add_bulk_flag` / `is_bulk_registration` | `lib/transform/enrichment.py` | method at enrichment.py:242; column assigned enrichment.py:247 | **ALREADY IMPLEMENTED** (not a to-do). Flags `(area, amount)` groups with count > `BULK_GROUP_MAX` |
| `BULK_GROUP_MAX` | `lib/transform/enrichment.py` | const at enrichment.py:26 | **= 10**. One owner; `gold_indexes.py` reads it |
| `build_area_median_index` | `lib/analysis/gold_indexes.py` | func in gold_indexes.py | **tested but UNWIRED** reimplementation; sorts by `median_rent`. The shipped view is in `build_weekly_duckdb.py` |
| `compute_movement` | `lib/analysis/wow_movement.py` | func in wow_movement.py | per-area median movement between two weeks; gated n≥10 in BOTH weeks; two-point measurement, NOT a trend |
| `set_changes` | `lib/analysis/wow_movement.py` | func in wow_movement.py | areas that entered/left the n≥10 comparable set between weeks (no silent drops) |
| `load_area_medians` | `lib/analysis/wow_movement.py` | func in wow_movement.py | reads `gold_area_median` + `_meta` (week, data_through) from a weekly DuckDB |
| `movement_for_series` | `lib/analysis/wow_movement.py` | func in wow_movement.py | movement over consecutive pairs of an ordered artifact list |
| `build_residential_market_index` | `lib/analysis/gold_indexes.py` | func in gold_indexes.py | gate measurement surface; residential-only; emits p95/max |
| `_comparable_rows` | `lib/analysis/gold_indexes.py` | func in gold_indexes.py | shared exclusion filter: `count <= BULK_GROUP_MAX`, excl Hotel/Labor Camps (sub_type), excl Virtual Unit (TYPE column), non-null positive amount |
| GATE bands | `lib/analysis/gold_indexes.py` | `GATE_MEDIAN_FLOOR_AED 20_000`, `GATE_MEDIAN_CEILING_AED 500_000`, `GATE_P95_CEILING_AED 1_000_000` | residential rent-band health gate |
| `analyze_by_area` | `lib/classes/market_analytics.py` | method at market_analytics.py:100 | area stats, filters `contract_count >= min_area_sample_size`, emits mean AND median |
| `calculate_rental_trends` | `lib/classes/market_analytics.py` | method at market_analytics.py:281 | trend calc — NOT defensible on <30 days of data |

## Engineering / pipeline anchors

| Symbol | File | Verified location | Notes |
|---|---|---|---|
| `main()` pipeline | `run_etl_pipeline.py` | func `main` at run_etl_pipeline.py:~258 | order: DOWNLOAD → TRANSFORM → ANALYZE → PUBLISH. Fail-open on empty window |
| `download_rents` | `run_etl_pipeline.py` | func | Scrapy spider first; `EjariRentsDownloader` fallback; skip-if-exists; `touch()` empty placeholder on no rows |
| `_incremental_window` | `run_etl_pipeline.py` | func | **2-day** window yesterday→today (single-day returns 0 rows); data date = yesterday |
| `NO_NEW_DATA` marker + `etl_status.json` | `run_etl_pipeline.py` | `_write_run_status`, `_data_rows`, `main` | empty window = success, recorded via `outcome="no_data"` + greppable `NO_NEW_DATA` log line |
| `transform_rents` / Silver | `run_etl_pipeline.py` + `lib/classes/silver_contract.py` | func `transform_rents` | Silver `to_silver(df)` on main path; broken contract now FAILS the run |
| `RAW_RENTS_CSV_DTYPES` | `lib/config.py` | dict at config.py:219 | pinned dtypes so daily + weekly read the SAME raw CSV without drift |
| `build_weekly_duckdb` | `lib/analysis/build_weekly_duckdb.py` | func at build_weekly_duckdb.py:143 | **THE LIVE module.** Weekly Mon–Sun DuckDB; `--week 2026W37` or `--from/--to` |
| `gold_area_median` / `gold_standard_lease` views | `lib/analysis/build_weekly_duckdb.py` | ~build_weekly_duckdb.py:116–144 | **shipping** Gold views (carry the three exclusions correctly) |
| `dedupe_pooled` / `add_row_hash` | `lib/analysis/build_weekly_duckdb.py` | funcs | cross-file dedupe by `row_hash`; within-file multiplicity preserved (bulk blocks survive) |
| `WEEKLY_FRESHNESS_GATE` | `lib/config.py` | dict at config.py:158 | `max_missing_daily_files 0`, `max_contract_date_lag_days 1`; a stalled feed FAILS the weekly build |
| star-schema DDL/SQL | `lib/analysis/` | `ddl.sql`, `dim_{date,location,project,property,tenant,contract_type}.sql`, `fact_rental_contract.sql` | the star; grain = 1 contract |
| `metro_volume` | `lib/analysis/metro_volume.py` | `TopMetroDailyRow`, `GOLD_TOP_METROS_DAILY_SQL` (imported from build_weekly_duckdb) | |
| `GitHubRelease.publish` | `lib/workspace/github_client.py` | method `publish` | idempotent: reuses same-tag release for same-day re-runs (`release_exists`) |
| tag format | `run_etl_pipeline.py` `publish_artifacts_to_github` + `github_client.py` | daily tag `release-YYYY-MM-DD`; weekly tag `release-week-YYYYWxx` | derived from `rent_contracts_YYYYMMDD.csv` |
| `validate_rent_contracts(df, strict=False)` | `lib/classes/validators.py` | func at validators.py:242 | fail-open validator |
| `PropertyUsage` | `lib/classes/property_usage.py` | class | excludes `is_bulk_registration == False` rows when the enriched column exists |

## CORRECTIONS to the old monolithic SKILL.md (stale claims)

1. **`is_residential` is NOT "always False".** It is `str`-typed (config.py:289) and
   returns correct membership. Drop that critique.
2. **`is_bulk_registration` is NOT a missing to-do.** It is implemented at
   enrichment.py:247 (`_add_bulk_flag`) with `BULK_GROUP_MAX = 10`, and is
   consumed by `gold_indexes.py`, `property_usage.py`, and the shipped
   `build_weekly_duckdb.py` views. Describe it as existing, not pending.
3. **PSF thresholds are not at `config.py:86`.** They live in the
   `VALIDATION_THRESHOLDS` dict. The PSF publish guard (`actual_area >= 200`) is
   enforced in `_add_psf` at enrichment.py:88.
4. **`get_area_tier` is at config.py:259**, not `:217`.
5. **ARCHITECTURE IS WEEKLY, NOT CUMULATIVE-LAYERS.** The live artifact is
   `rental_analytics_weekly_YYYYWxx.duckdb` built by
   `lib/analysis/build_weekly_duckdb.py` (wired in `.github/workflows/weekly.yml`,
   tested in `tests/test_weekly_dedup_and_gate.py`). Its `_meta` view carries
   `week`, `week_start`, `week_end`, `pooled_rows`, `deduped_rows`,
   `row_hash_duplicates_removed`, `daily_files`/`expected_daily_files`/`missing_daily_files`,
   `data_through` (current code; older local artifacts may have fewer columns).
   There is **NO**
   `build_layers_duckdb.py` and **NO** `rents_layers.duckdb` in the tree. The
   README's "cumulative combined layers" narrative and the Makefile
   `layers` / `layers-publish` targets reference modules/files that do not exist
   — documentation/Makefile drift, flagged for a separate fix, NOT cited by any
   skill.

## OPEN drift flagged (out of scope for the skills work, surfaced for the owner)

- `README.md` describes `build_layers_duckdb.py` / `rents_layers.duckdb` (ADR-10
  cumulative design) that is not implemented. The shipping design is weekly.
- `Makefile` `layers` and `layers-publish` targets invoke the non-existent
  `lib.analysis.build_layers_duckdb` and read `output/rents_layers.duckdb`.
  These targets will fail if run. Recommend aligning docs + Makefile to the
  weekly module, or implementing the layers module — owner's call.

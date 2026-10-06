# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- **No-data release markers.** Every daily run now publishes `etl_status.json` to its
  `release-YYYY-MM-DD`, including quiet days that produce no CSV. A missing release now means the
  job never ran; a release without a CSV means no data — previously the two were indistinguishable.
- **Whole raw CSV schema pinned.** `RAW_RENTS_CSV_DTYPES` now covers all 44 payload columns, not
  just the ones that have bitten, so no column is left to per-file inference. `tests/test_raw_schema.py`
  fails if a payload column is added without pinning it.
- **One publish client.** Weekly Silver/Gold publication moved off the `gh` CLI (workflow + Makefile)
  to `lib.workspace.publish_layers` over `GitHubRelease`; daily bronze and weekly layers now share a
  single publish path that clobbers existing assets.
- **Layered Bronze/Silver/Gold releases.** The weekly artifact is split into two DuckDBs:
  `silver_YYYYWww.duckdb` (normalized `DimArea`, `DimPropertyType`, `DimMetro`, `FctContract`,
  `_meta` tables) tagged `release-silver-YYYYWww`, and `gold_YYYYWww.duckdb` (seven analytics
  views over the attached Silver tables) tagged `release-gold-YYYYWww`. The daily raw CSV remains
  the bronze layer (`release-YYYY-MM-DD`). Gold views are defined against the `silver` catalog
  alias, so consumers attach Silver first (`lib/analysis/layers.connect_gold`). See
  [ADR-08](docs/adr/0008-layered-bronze-silver-gold-releases.md).
- **Weekly cross-file dedup.** The weekly DuckDB build now drops rows repeating a Silver `row_hash`
  first seen in an earlier daily file, before enrichment, so the 2-day extract window no longer
  double-counts into the Gold medians and counts. Within-file repeats (bulk registrations) are
  preserved. `_meta` gains `pooled_rows`, `deduped_rows` and `row_hash_duplicates_removed`.
- **Weekly freshness gate.** The build raises instead of publishing when a day in the requested
  window has no usable daily CSV, or when the newest registration trails the window end. Thresholds
  live in `lib/config.py` (`WEEKLY_FRESHNESS_GATE`); `_meta` gains `daily_files`,
  `expected_daily_files`, `missing_daily_files` and `data_through`.
- **Explicit no-data record.** The daily pipeline writes `output/etl_status.json` and logs a
  greppable `NO_NEW_DATA` line for an empty incremental window instead of returning silently. It
  remains fail-open (ADR-03); a missing file after download is no longer treated as "no data".
- `make weekly` now derives the previous complete ISO week instead of hardcoded dates.
- Fixed the repository owner in README badges and clone URL; rewrote stale release notes.
- Added [ADR-07](docs/adr/0007-weekly-cross-file-dedup-and-freshness-gate.md) and
  `tests/test_weekly_dedup_and_gate.py`.

## Released

- Initial project structure enhancements.
- Added CI/CD integration.
- Updated [README.md](README.md) with detailed project overview, setup, usage, and contribution guidelines.
- Refactored Makefile with new targets and comprehensive documentation.

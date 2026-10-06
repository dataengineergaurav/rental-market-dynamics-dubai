# ADR-10: Daily cumulative Silver+Gold in one DuckDB (supersedes ADR-07 and ADR-08)

**Status:** Accepted · **Date:** 2026-10-06 · **Supersedes:** [ADR-07](0007-weekly-cross-file-dedup-and-freshness-gate.md) (weekly cadence + freshness gate), [ADR-08](0008-layered-bronze-silver-gold-releases.md) (weekly cadence + two-file split)

## Context

The weekly build (ADR-07) synthesized a Mon–Sun window from seven daily CSVs, gated it, deduped it,
and wrote two weekly DuckDBs (ADR-08): `silver_YYYYWww.duckdb` (tables) and `gold_YYYYWww.duckdb`
(views over an attached `silver` alias).

Two problems surfaced once the consumer was taken seriously. The consumer is **market
intelligence** — agents and BI dashboards asking *trend* questions ("Marina rents year on year",
"which areas are heating up", "metro-premium drift"). But:

- **A 7-day window discards the one thing the product is made of — history.** The Gold views are
  already written for a time series (`AggMonthlyRegistrations` groups by month; per-area
  `n >= 10` medians only stabilise over long history), yet fed a week they return a single month
  and a spot price, not a market.
- **The weekly cadence made even the snapshot stale for six days out of seven**, and the whole
  window/dedup/gate apparatus existed only to synthesize a week from daily files.

## Decision

Publish **one cumulative DuckDB**, updated **daily**, published to the stable tag
`release-layers-latest` (asset clobbered each run).

| Layer | Artifact | Tag | Contents |
|-------|----------|-----|----------|
| Bronze | `rent_contracts_YYYYMMDD.csv` | `release-YYYY-MM-DD` | raw daily extract (unchanged) |
| Layers | `rents_layers.duckdb` | `release-layers-latest` | tables `DimArea`, `DimPropertyType`, `DimMetro`, `FctContract`, `_meta` **and** the seven Gold views, in one file |

- **Cumulative, not windowed.** `build_layers_duckdb.ingest_layers` `INSERT OR REPLACE`s the day's
  contracts into `FctContract` and rebuilds the dimensions and views. The store grows; it is never
  truncated to a window.
- **Idempotency by primary key, not `row_hash`.** `FctContract` is keyed on `contract_id`
  (payload `CONTRACT_NUMBER`), so a same-day re-run and the next day's 2-day-window overlap
  overwrite a row instead of duplicating it. This replaces ADR-07's cross-file dedup *code* with a
  DB constraint.
- **Natural-key dimensions.** `DimArea` keys on `area_name_en`, `DimPropertyType` on
  (type, sub_type, usage), `DimMetro` on `nearest_metro_en`; the fact carries those natural keys as
  its foreign keys, and the dimensions are rebuilt from the fact every run so they cannot drift.
  The `ROW_NUMBER` surrogate keys are gone — they collide across incremental runs.
- **No freshness gate.** Feed staleness is already recorded by the daily bronze job
  (`etl_status.json`, `outcome: no_data`). Ingest keeps one trust-boundary check: the newest
  registration may trail the data date its filename claims by at most `MAX_REGISTRATION_LAG_DAYS`
  (1), i.e. a stall fails, a quiet day does not.
- **Seeded empty, accumulates forward.** The first run creates the file from that day's CSV;
  `ingest_layers` accepts repeated `--csv`, so a backfill is available without new code.

## Rationale

- **The product is a time series.** A cumulative store is what makes the existing Gold views
  meaningful; the windowed design was actively fighting the marts it shipped.
- **A primary key deletes more code than it adds.** The window resolver, `missing_days`,
  `collect_week_csvs`, `dedupe_pooled`, `add_row_hash`, `_file_idx`, the freshness gate, and the
  seven-release rehydrate loop all existed to synthesize a window — all removed. What replaced them
  is one `INSERT OR REPLACE`.
- **One file removes the `ATTACH` tax.** ADR-08 kept the views as definitions over an attached
  Silver alias. Since the store is a single self-contained artifact for consumers, the views
  reference the tables directly and a plain `duckdb.connect(path)` resolves everything.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Rebuild a trailing-7-day snapshot daily | Refreshes freshness but still discards history; keeps the window/dedup/gate machinery for no product benefit |
| Rolling 28-day window | Same history loss, more rehydrate I/O |
| Cumulative but append-only (no key) | The 2-day extract window would double-count every overlapping registration |
| Cumulative with `row_hash` dedup retained | The key is a DB concern; a stable business key (`contract_id`) is simpler and also handles corrections within the overlap window |
| Keep weekly `silver_*`/`gold_*` two-file releases alongside | Entanglement ADR-08 already rejected; consumers want one file |

## Consequences

- **`lib/analysis/weekly_pool.py`, `build_weekly_duckdb.py`, `build_silver_duckdb.py` and
  `build_gold_duckdb.py` are deleted**; their durable parts (raw-CSV aliases, enrichment) live in
  `build_layers_duckdb._prepare_increment`. `lib/analysis/layers.connect_gold` becomes
  `connect_layers` (a plain connect — no attach).
- `WEEKLY_FRESHNESS_GATE` is removed from `lib/config.py`; `publish_weekly_layers` becomes
  `publish_layers` (one artifact → one stable tag).
- `.github/workflows/weekly.yml` becomes `daily_layers.yml`; `make weekly`/`weekly-publish` become
  `make layers`/`layers-publish`.
- `_meta` is now one row describing the current store: `built_at`, `data_from`, `data_through`,
  `total_contracts`, `last_ingested_window`.

### Known ceilings

- **The published asset grows.** The whole file is re-uploaded daily. Fine to a few hundred MB;
  move to object storage (R2/S3) beyond that. (ponytail note in the code.)
- **Corrections are recent-only.** Upsert by `contract_id` updates a row re-sent in a later 2-day
  window; a revision older than that window is not propagated.
- **Cold start.** Seeded empty, so trend views are sparse until history accrues.

## Verification

- `tests/test_build_layers_duckdb.py` — combined file table/view set; views resolve on a bare
  connect; **re-ingesting a day adds no rows**; a second day grows the fact without double-counting
  the overlap; `AggMonthlyRegistrations` spans the ingested months; a stale increment raises; a
  placeholder is not ingested.
- `tests/test_publish_layers.py` — one artifact → `release-layers-latest`; missing artifact fails
  before any upload.
- `tests/test_metro_volume.py` — combined file, `_meta.data_through`, no attach.
- End-to-end CLI: ingest day 1 → 2 contracts; day 2 (overlap) → 3; re-ingest day 2 → 3; monthly view
  returns 2026-09 and 2026-10.

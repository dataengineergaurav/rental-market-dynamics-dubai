# Rental Market Dynamics — Dubai

![Build Status](https://img.shields.io/github/actions/workflow/status/dataengineergaurav/rental-market-dynamics-dubai/build_and_deploy.yml?branch=main)
![License](https://img.shields.io/github/license/dataengineergaurav/rental-market-dynamics-dubai)

ETL and analytics pipeline for **Dubai Ejari rent transactions** — registered tenancy contracts from the configured Rent Transaction Details endpoint.

Sales, title deeds, and short-term stay data are out of scope. Analysis favors **medians over means**, with guards for bulk registrations and unrealistic unit areas so headline rent figures stay trustworthy.

## What it does

| Cadence | Output |
|---------|--------|
| **Daily** | Incremental extract → transform/enrich → Bronze release (raw CSV) |
| **Daily** | Ingest the day's contracts into one cumulative DuckDB with Silver (normalized dim/fact tables) and Gold (analytics views) in a single file |

The pipeline treats data as a **bronze → silver → gold** flow. Bronze is one file per day; Silver and
Gold live together in a single **cumulative** store that grows every day:

| Layer | Artifact | Tag | Contents |
|-------|----------|-----|----------|
| **Bronze** | `rent_contracts_YYYYMMDD.csv` | `release-YYYY-MM-DD` | raw daily extract, as returned by the endpoint |
| **Silver** | `rents_layers.duckdb` | `release-YYYY-MM-DD` | tables `DimArea`, `DimPropertyType`, `DimMetro`, `FctContract`, `_meta` |
| **Gold** | `rents_layers.duckdb` | `release-YYYY-MM-DD` | views `gold_area_median`, `gold_standard_lease`, `gold_top_metros_daily`, `AggAreaRentStats`, `AggMetroPremium`, `AggMonthlyRegistrations`, `AggProjectRentStats` |

One release per data date (`release-YYYY-MM-DD`) holds the raw CSV, `etl_status.json` and the combined `rents_layers.duckdb` together.

The Silver tables and the Gold views are in the **same** DuckDB file, so a plain
`duckdb.connect("rents_layers.duckdb")` resolves every view — no `ATTACH`, no alias. The store is
cumulative (seeded empty, grown daily), so the time-series marts (`AggMonthlyRegistrations`, and the
per-area medians) span all history rather than a single week.

## Data guarantees

- **The daily job is fail-open, and every run is marked.** An empty incremental window is a success,
  not an error, but it is never silent: the run writes `output/etl_status.json` and logs a greppable
  `NO_NEW_DATA` line. That status is also published to the day's release, so a release with no CSV is
  a *quiet day* and a missing release is a run that never happened — the two are distinguishable from
  the releases list alone. A *missing* file after a successful download is not treated as "no data" —
  it proceeds and fails.
- **Idempotent by primary key.** The extract uses a 2-day window, so a registration can appear in two
  consecutive daily files. `FctContract` is keyed on `contract_id`, so ingesting a day again (a
  re-run, or the next day's overlapping window) **replaces** the row instead of duplicating it — no
  cross-file dedup code, just the DB constraint.
- **A stalled feed fails.** Ingest keeps one trust check: the newest registration may trail the data
  date its filename claims by at most `MAX_REGISTRATION_LAG_DAYS` (1). A quiet day is fine; a feed
  that stops advancing is not. The store's freshness is readable from `_meta.data_through`.

The `_meta` table is one row describing the current store — `built_at`, `data_from`, `data_through`,
`total_contracts`, `last_ingested_window`.

## Architecture

```
EJARI_URL (paginated)
        │
        ▼
   Extract (Scrapy / downloader)
        │
        ▼
   Bronze: rent_contracts_YYYYMMDD.csv ──► daily release (empty window → NO_NEW_DATA, exit 0)
        │
        ▼
   Ingest ──► enrich (Polars) ──► INSERT OR REPLACE (key: contract_id)
        │
        ▼
   rents_layers.duckdb   (cumulative, one file)
        ├── Silver tables: DimArea, DimPropertyType, DimMetro, FctContract, _meta
        └── Gold views:    gold_*, Agg*   (resolve in-file, no ATTACH)
```

Orchestration is Make-driven locally and via GitHub Actions (daily ETL, daily layers ingest, push
builds). See [ADR-10](docs/adr/0010-cumulative-combined-layers-duckdb.md) for why the layers are
cumulative, keyed and combined in one file (it supersedes
[ADR-07](docs/adr/0007-weekly-cross-file-dedup-and-freshness-gate.md) and
[ADR-08](docs/adr/0008-layered-bronze-silver-gold-releases.md)).

## Stack

- **Python** 3.9+ (CI uses 3.12)
- **Polars** / **PyArrow** for transform
- **Pydantic** v2 for the Silver contract
- **DuckDB** for the cumulative layers store
- **Scrapy** for Ejari extraction
- **uv** (or pip) for dependencies

## Setup

```bash
git clone https://github.com/dataengineergaurav/rental-market-dynamics-dubai
cd rental-market-dynamics-dubai
uv sync   # or: pip install -r requirements.txt && pip install lib/
cp .env.example .env
```

Required environment variables:

- `EJARI_URL` — Ejari rent transactions endpoint
- `GH_TOKEN` — GitHub token (for publishing releases)

## Common commands

```bash
make all              # build → daily ETL → tests
make layers           # ingest today's CSV into the cumulative layers DuckDB
make layers-publish   # publish the layers DuckDB into the day's release
make test             # pytest
make lint             # ruff check + format-check
make coverage         # pytest with a coverage floor
make scrapy-rents     # Scrapy extract only
```

Quality gates run in CI on every push (`build_and_deploy.yml`): `make lint` (high-signal ruff —
undefined names, unused imports, real-bug lints) and `make coverage` (a ratchet floor in the
`Makefile`). Dependencies are locked — `uv sync` reproduces the environment from `uv.lock`. See
[ADR-11](docs/adr/0011-reproducibility-and-ci-quality-gates.md).

Daily entry point: `run_etl_pipeline.py`. Layers ingest:
`python -m lib.analysis.build_layers_duckdb --csv output/rent_contracts_YYYYMMDD.csv` (repeat `--csv`
to backfill several days at once). Publishing:
`python -m lib.workspace.publish_layers --artifact output/rents_layers.duckdb`.

## Layout

```
lib/              Extract, transform, enrichment, analytics, release helpers
rents_scraper/    Scrapy spider for Ejari rents
output/           Daily CSVs, Parquet, the cumulative layers DuckDB
tests/            Pipeline and data-quality gates
docs/             Documentation hub: architecture, data dictionary, analyst cookbook, operations, ADRs
.github/          Daily ETL / daily layers / push workflows
```

## Data & releases

One release per data date (`release-YYYY-MM-DD`) carries the raw CSV, `etl_status.json` and the
combined `rents_layers.duckdb` — see
[GitHub Releases](https://github.com/dataengineergaurav/rental-market-dynamics-dubai/releases).
Each day's release holds a full snapshot of the cumulative DuckDB as of that date, so any
historical layer file is also directly retrievable.

## Documentation

Start at the [documentation hub](docs/README.md), or jump straight to the part you need:

- [Architecture](docs/ARCHITECTURE.md) — the pipeline end to end, the medallion contracts, and the invariants the tests pin
- [Data dictionary](docs/DATA_DICTIONARY.md) — every table, view, column, code and constant
- [Analyst cookbook](docs/ANALYST_COOKBOOK.md) — real market questions answered with SQL against the Gold views
- [Operations](docs/OPERATIONS.md) — schedules, releases, commands, triage, release verification
- [Library usage guide](docs/LIBRARY_USAGE_GUIDE.md) — `MarketAnalytics` / enrichment APIs

Decisions and history:

- [ADRs](docs/adr/README.md) — [06](docs/adr/0006-pydantic-silver-contract.md) (pydantic Silver contract), [09](docs/adr/0009-release-hardening-raw-schema-and-markers.md) (release hardening), [10](docs/adr/0010-cumulative-combined-layers-duckdb.md) (cumulative combined layers), [11](docs/adr/0011-reproducibility-and-ci-quality-gates.md) (reproducible deps + CI gate). ADR-07/08 are superseded by ADR-10.
- [Silver contract design](docs/superpowers/specs/2026-09-27-silver-contract-design.md) — field measurements and the market-health gate
- [CHANGELOG](CHANGELOG.md) · [RELEASE_NOTES](RELEASE_NOTES.md) — what changed, and what each release contains

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [MIT](https://mit-license.org/).

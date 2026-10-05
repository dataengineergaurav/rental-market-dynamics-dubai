# Rental Market Dynamics — Dubai

![Build Status](https://img.shields.io/github/actions/workflow/status/dataengineergaurav/rental-market-dynamics-dubai/build_and_deploy.yml?branch=main)
![License](https://img.shields.io/github/license/dataengineergaurav/rental-market-dynamics-dubai)

ETL and analytics pipeline for **Dubai Ejari rent transactions** — registered tenancy contracts from the configured Rent Transaction Details endpoint.

Sales, title deeds, and short-term stay data are out of scope. Analysis favors **medians over means**, with guards for bulk registrations and unrealistic unit areas so headline rent figures stay trustworthy.

## What it does

| Cadence | Output |
|---------|--------|
| **Daily** | Incremental extract → transform/enrich → optional GitHub Release CSV |
| **Weekly** | Pool recent daily CSVs into a DuckDB analytics database with gold summary views |

The pipeline treats data in a simple bronze → silver → gold flow under `output/`: raw daily contracts, enriched Parquet, then weekly fact tables and area-level median views.

## Data guarantees

- **The daily job is fail-open.** An empty incremental window is a success, not an error, but it is
  never silent: the run writes `output/etl_status.json` and logs a greppable `NO_NEW_DATA` line. A
  *missing* file after a successful download is not treated as "no data" — it proceeds and fails.
- **Cross-file dedup.** The extract uses a 2-day window, so a registration can appear in two
  consecutive daily files. The weekly build drops rows repeating a `row_hash` first seen in an
  earlier file, before enrichment, so Gold medians and counts are not double-counted.
- **Freshness gate.** The weekly build refuses to publish a DuckDB when a day in the requested
  window has no usable CSV, or when the newest registration trails the window end — it raises
  instead of shipping a stale artifact. Thresholds: `WEEKLY_FRESHNESS_GATE` in `lib/config.py`.

Every weekly build records what it was made of — `pooled_rows`, `deduped_rows`,
`row_hash_duplicates_removed`, `daily_files`/`expected_daily_files`/`missing_daily_files`, and
`data_through` — in the `_meta` view.

## Architecture

```
EJARI_URL (paginated)
        │
        ▼
   Extract (Scrapy / downloader)
        │
        ▼
   Transform + enrich (Polars) ──► Silver contract (validate + row_hash)
        │
        ├──► daily CSV release      (empty window → NO_NEW_DATA, exit 0)
        │
        ▼
   Weekly pool ──► freshness gate ──► cross-file dedup ──► DuckDB (fact + gold views)
```

Orchestration is Make-driven locally and via GitHub Actions (daily ETL, weekly DuckDB build, push builds). See [ADR-07](docs/adr/0007-weekly-cross-file-dedup-and-freshness-gate.md) for why dedup is cross-file and the gate sits at the weekly boundary.

## Stack

- **Python** 3.9+ (CI uses 3.12)
- **Polars** / **PyArrow** for transform
- **Pydantic** v2 for the Silver contract
- **DuckDB** for weekly analytics
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
make weekly           # build weekly DuckDB for the previous complete ISO week
make test             # pytest
make scrapy-rents     # Scrapy extract only
```

Daily entry point: `run_etl_pipeline.py`. Weekly analytics: `python -m lib.analysis.build_weekly_duckdb`,
which takes `--week 2026W37` or an explicit `--from YYYYMMDD --to YYYYMMDD`; it raises rather than
build a week its daily files do not cover.

## Layout

```
lib/              Extract, transform, enrichment, analytics, release helpers
rents_scraper/    Scrapy spider for Ejari rents
output/           Daily CSVs, Parquet, weekly DuckDB artifacts
tests/            Pipeline and data-quality gates
docs/             Implementation plan, roadmap, library usage
.github/          Daily / weekly / push workflows
```

## Data & releases

Historical daily CSVs (`release-YYYY-MM-DD`) and weekly DuckDBs (`release-week-YYYYWxx`) are published as [GitHub Releases](https://github.com/dataengineergaurav/rental-market-dynamics-dubai/releases).

## Further reading

- [Library usage guide](docs/LIBRARY_USAGE_GUIDE.md) — `MarketAnalytics` / enrichment APIs
- [ADR-06: pydantic v2 Silver contract](docs/adr/0006-pydantic-silver-contract.md)
- [ADR-07: weekly cross-file dedup and freshness gate](docs/adr/0007-weekly-cross-file-dedup-and-freshness-gate.md)
- [Silver contract design](docs/superpowers/specs/2026-09-27-silver-contract-design.md) — field measurements and the market-health gate

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [MIT](https://mit-license.org/).

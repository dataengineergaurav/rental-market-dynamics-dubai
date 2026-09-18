# Rental Market Dynamics — Dubai

![Build Status](https://img.shields.io/github/actions/workflow/status/ggurjar333/rental-market-dynamics-dubai/build_and_deploy.yml?branch=main)
![License](https://img.shields.io/github/license/ggurjar333/rental-market-dynamics-dubai)

ETL and analytics pipeline for **Dubai Ejari rent transactions** — registered tenancy contracts from the configured Rent Transaction Details endpoint.

Sales, title deeds, and short-term stay data are out of scope. Analysis favors **medians over means**, with guards for bulk registrations and unrealistic unit areas so headline rent figures stay trustworthy.

## What it does

| Cadence | Output |
|---------|--------|
| **Daily** | Incremental extract → transform/enrich → optional GitHub Release CSV |
| **Weekly** | Pool recent daily CSVs into a DuckDB analytics database with gold summary views |

The pipeline treats data in a simple bronze → silver → gold flow under `output/`: raw daily contracts, enriched Parquet, then weekly fact tables and area-level median views.

## Architecture

```
EJARI_URL (paginated)
        │
        ▼
   Extract (Scrapy / downloader)
        │
        ▼
   Transform + enrich (Polars)
        │
        ├──► daily CSV release
        │
        ▼
   Weekly pool → DuckDB (fact + gold views)
```

Orchestration is Make-driven locally and via GitHub Actions (daily ETL, weekly DuckDB build, push builds).

## Stack

- **Python** 3.9+ (CI uses 3.12)
- **Polars** / **PyArrow** for transform
- **DuckDB** for weekly analytics
- **Scrapy** for Ejari extraction
- **uv** (or pip) for dependencies
- Optional **dbt-duckdb** scaffold under `analysis/`

## Setup

```bash
git clone https://github.com/ggurjar333/rental-market-dynamics-dubai
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
make weekly           # build weekly DuckDB
make test             # pytest
make scrapy-rents     # Scrapy extract only
```

Daily entry point: `run_etl_pipeline.py`. Weekly analytics: `python -m lib.analysis.build_weekly_duckdb`.

## Layout

```
lib/              Extract, transform, enrichment, analytics, release helpers
rents_scraper/    Scrapy spider for Ejari rents
analysis/         Optional dbt-duckdb starter project
output/           Daily CSVs, Parquet, weekly DuckDB artifacts
tests/            Pipeline and data-quality gates
docs/             Implementation plan, roadmap, library usage
.github/          Daily / weekly / push workflows
```

## Data & releases

Historical daily CSVs (`release-YYYY-MM-DD`) and weekly DuckDBs (`release-week-YYYYWxx`) are published as [GitHub Releases](https://github.com/dataengineergaurav/rental-market-dynamics-dubai/releases).

## Further reading

- [Library usage guide](docs/LIBRARY_USAGE_GUIDE.md) — `MarketAnalytics` / enrichment APIs
- [Implementation plan](docs/IMPLEMENTATION_PLAN.md) — design decisions and WBS
- [Expert review & roadmap](docs/EXPERT_REVIEW_AND_ROADMAP.md) — quality gates and next steps
- [Architecture overview](docs/architecture.html)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [MIT](https://mit-license.org/).

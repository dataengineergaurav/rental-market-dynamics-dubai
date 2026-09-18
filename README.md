# Rental Market Dynamics - Dubai

![Build Status](https://img.shields.io/github/actions/workflow/status/ggurjar333/rental-market-dynamics-dubai/build_and_deploy.yml?branch=main)
![License](https://img.shields.io/github/license/ggurjar333/rental-market-dynamics-dubai)
![Coverage](https://img.shields.io/codecov/c/github/ggurjar333/rental-market-dynamics-dubai)

Ejari rent transactions only — sourced from the configured `EJARI_URL` endpoint
(Rent Transaction Details). No sales, no carea, no legacy Pulse code.

**Medians, not means.** Bulk registrations (Naif `79×1.54M` Hotels, Hor `663k` dupes) and `1 sqft` area shells are flagged and excluded (`is_bulk_registration`, `actual_area≥200` → `null PSF`).

## Features

- **Automated Data Extraction:** Paginated gateway rents fetch (`EjariRentsDownloader`) with Scrapy `rents` spider fallback — same `EJARI_URL` (`P_TAKE/P_SKIP`, retry+backoff, 900s timeout).
- **Data Transformation:** Rents CSV → Parquet with canonical aliases (`RentsTransformer` `scan_csv` → `sink_parquet zstd`).
- **Enrichment (P0-hardened):** `enrich_rent_contracts` — PSF `null<200`, `area_tier` via `AREA_CLASSIFICATIONS` (`replace_strict`), property-type normalize via `PROPERTY_TYPE_MAPPINGS`, bulk flag `>10` same `area+amount`, duration, luxury, usage category.
- **Validation Gate (fail-open):** `validate_rent_contracts` in `transform_rents` — logs `errors/warnings` but never blocks release; PSF sanity `45-150` not `4691`.
- **Weekly Analytics DB:** `rental_analytics_weekly_YYYYWxx.duckdb` — 7-day Mon-Sun pooled enriched fact + gold views `gold_area_median` (`n≥10`) + `gold_standard_lease` (`Flat 6-12m`).
- **Property Usage & Market Analytics:** `PropertyUsage` (usage mix, medians, bulk-excluded) + `MarketAnalytics` (PSF `20-500/30-800`, `is_in` gated).
- **Automated Releases:** Daily `rent_contracts_YYYYMMDD.csv` (`release-YYYY-MM-DD`) + weekly DuckDB (`release-week-YYYYWxx`) via `GitHubRelease` (reuse same-day tag).
- **CI/CD:** Daily `05:30 UTC` ETL + weekly Mondays `06:00 UTC` DuckDB + `push` build.

## Prerequisites

- **Python:** 3.9 or higher (CI 3.12)
- **Make:** To execute build commands
- **uv** (or pip) — `pyproject.toml` includes `polars`, `duckdb`, `pyarrow`, `scrapy`

## Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/ggurjar333/rental-market-dynamics-dubai
    cd rental-market-dynamics-dubai
    ```

2. **Install dependencies**
    ```bash
    uv sync
    # or: pip install -r requirements.txt && pip install lib/
    ```

3. **Create a `.env` file**

    Copy the provided example and update the values:
    ```bash
    cp .env.example .env
    ```
    `.env` needs only:
    ```bash
    EJARI_URL=your_ejari_endpoint_here
    GH_TOKEN=your_github_token  # for releases
    ```

## Folder Structure
```bash
.
├── lib
│   ├── extract/ejari_rents_downloader.py  # POST EJARI_URL (P_TAKE/P_SKIP pagination)
│   ├── transform/rents_transformer.py      # rents CSV -> Parquet (canonical aliases)
│   ├── transform/enrichment.py             # enrich_rent_contracts (PSF null<200, tiers replace_strict, bulk flag >10)
│   ├── classes/validators.py               # RentContractValidator / validate_rent_contracts
│   ├── classes/market_analytics.py         # MarketAnalytics (PSF is_in gated, trends)
│   ├── classes/property_usage.py           # PropertyUsage (usage mix, bulk-excluded)
│   ├── analysis/build_weekly_duckdb.py     # weekly Mon-Sun DuckDB (fact + gold views)
│   ├── analysis/*.sql                      # star-schema DDL (dim_*, fact_rental_contract) — deferred to N≥30
│   ├── workspace/github_client.py          # GitHubRelease (dated tag, reuse same-day)
│   ├── config.py                           # thresholds, tier mappings, API_CONFIG
│   └── logging_helpers.py
├── rents_scraper/rents_scraper/spiders/rents.py  # Scrapy rents spider (same EJARI_URL)
├── output                                  # rent_contracts_YYYYMMDD.csv, rents_silver_pooled.parquet, rental_analytics_weekly_*.duckdb, area_median_index_*.csv
├── tests/test_etl_pipeline.py + test_p0_gates.py
├── run_etl_pipeline.py                     # incremental pipeline (yesterday->today, fail-open validation)
├── SKILL.md                                # bundle: Dubai expert (hard mode) + Data Eng + Arch + QA + Ops + Orchestrator
├── docs/EXPERT_REVIEW_AND_ROADMAP.md       # P0->P3 gates, 5-day pooled plan
├── docs/IMPLEMENTATION_PLAN.md             # SA WBS + ADRs
├── docs/LIBRARY_USAGE_GUIDE.md
├── .github/workflows/cron.yml              # daily 05:30 UTC make all
├── .github/workflows/weekly.yml            # weekly Mondays 06:00 UTC build DuckDB
├── .env.example
├── Makefile
└── pyproject.toml
```

## Getting Started
- **ETL Pipeline (daily incremental):**
    ```bash
    make all   # build → etl → test → publish rent_contracts_YYYYMMDD.csv
    ```
- **Weekly DuckDB (Mon-Sun pooled from Releases):**
    ```bash
    make weekly                    # builds output/rental_analytics_weekly_2026W37.duckdb (5d fallback if 7d thin)
    make weekly-publish WEEK=2026W37  # → release-week-2026W37 asset
    # or manual: uv run python -m lib.analysis.build_weekly_duckdb --week 2026W37
    #            uv run python -m lib.analysis.build_weekly_duckdb --from 20260913 --to 20260919
    ```
- **Testing:**
    ```bash
    make test   # pytest — includes P0 gates: tier mapping, PSF null<200, p95<300, is_bulk
    ```

## Usage Examples
- **Downloading & Transforming Data (daily):**
    ```bash
    make etl
    # or
    python run_etl_pipeline.py
    # logs: etl.log — DOWNLOAD→TRANSFORM (validation gate)→ANALYZE→PUBLISH
    ```

- **Scrapy rents spider:**
    ```bash
    make scrapy-rents
    # or
    cd rents_scraper && uv run scrapy crawl rents -a from_date=09/12/2026 -a to_date=09/13/2026 -O ../output/rents.jsonl
    ```

- **Weekly analytics DB:**
    ```bash
    uv run python -m lib.analysis.build_weekly_duckdb --from 20260913 --to 20260919
    duckdb output/rental_analytics_weekly_2026W37.duckdb "SELECT * FROM gold_area_median LIMIT 5;"
    # fact_rental_contract: 16075 rows pooled (13-17 Sep), 121 areas n≥10
    # gold_area_median ex Hotel/Labor Doom/Virtual+bulk: Barsha South Fourth 399 med 66k, Burj Khalifa 238 172k, Business Bay 353 116k
    # gold_standard_lease Flat 6-12m 7713 med 64k
    ```

- **Publishing Releases:**
    Daily `rent_contracts_YYYYMMDD.csv` → `release-YYYY-MM-DD`; weekly DuckDB → `release-week-YYYYWxx` (reuse same-day tag, `GH_TOKEN` only).

## Historical Data
Download daily CSVs and weekly DuckDBs from [releases](https://github.com/dataengineergaurav/rental-market-dynamics-dubai/releases) (`release-YYYY-MM-DD` and `release-week-YYYYWxx`).

## Contributing
Contributions are welcome! Please review the [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Licenses
This project is licensed under the terms of the [MIT License](https://mit-license.org/).

## Changelog
Refer to [CHANGELOG.md](changelog.md) for a complete history of changes.

## Contact
For questions or feedback, please open an issue on GitHub.

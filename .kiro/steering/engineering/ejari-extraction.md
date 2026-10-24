---
inclusion: manual
name: ejari-extraction
description: How Bronze data is extracted from the Ejari endpoint — the Scrapy spider with a direct-downloader fallback, the 2-day incremental window, and the fail-open / NO_NEW_DATA behavior. Load when working on download, pagination, or the empty-window contract.
owner: "@dataengineergaurav"
department: engineering
last-verified: 2026-10-24
---

# Ejari Extraction (Bronze)

Owns getting registered tenancy contracts out of the configured `EJARI_URL`
endpoint into a daily Bronze CSV, with a fail-open contract for empty windows.

## Contract

- **owns:** extraction, pagination, the incremental window, the empty-window rule.
- **hands-off-to:**
  - `medallion-layer-contracts` — the Bronze CSV feeds Silver/Gold.
- **routes-from:**
  - `engineering/_department` — any download / extraction task.

## Triggers

Scrapy spider, direct downloader, `P_TAKE`/`P_SKIP`, pagination, `EJARI_URL`,
incremental window, "download returned 0 rows", NO_NEW_DATA, placeholder CSV.

## Workflow

- **Spider first, downloader fallback.** `download_rents` runs the Scrapy `rents`
  spider via subprocess (900s timeout); on timeout or failure it falls back to
  `EjariRentsDownloader.run` (POST with `max_retries` + exponential backoff from
  `API_CONFIG`). Test URLs containing `example.com` skip the spider to keep the
  patch target stable.
- **Incremental 2-day window.** `_incremental_window` returns yesterday→today in
  `MM/DD/YYYY`. A single-day (from==to) range returns zero rows from the gateway,
  so the window always spans 2 days; the data date is **yesterday** (today's
  registrations aren't published yet). The 2-day overlap is why the weekly builder
  must dedupe cross-file.
- **Skip-if-exists + placeholder.** If the daily file already exists, download is
  skipped (idempotent re-run). If no rows come back, the downloader `touch()`es an
  empty placeholder so TRANSFORM can be skipped gracefully.
- **Fail-open.** An empty window is a SUCCESS, not an error — but never silent:
  `main` logs `NO_NEW_DATA`, writes `etl_status.json` with `outcome="no_data"`,
  and exits 0. A MISSING file after download is NOT this case (download guarantees
  a file), so absence is an anomaly that must reach TRANSFORM and fail.

## Gates

- Empty window → exit 0 + `NO_NEW_DATA` + status record. Never a silent success.
- A genuinely missing file is an anomaly, not a quiet day — let it fail.
- Extraction commands run through `uv`/`make` (e.g. `make scrapy-rents`).

## References

- `run_etl_pipeline.py` — `download_rents` — spider-first with downloader fallback.
- `run_etl_pipeline.py` — `_incremental_window` — the 2-day yesterday→today window.
- `run_etl_pipeline.py` — `_data_rows` — counts data rows for the NO_NEW_DATA decision.
- `run_etl_pipeline.py` — `_write_run_status` — writes etl_status.json.
- `lib/extract/ejari_rents_downloader.py` — `EjariRentsDownloader` — the direct fallback fetcher.
- `lib/config.py` — `API_CONFIG` — retries, backoff, timeout.

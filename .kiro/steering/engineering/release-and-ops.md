---
inclusion: manual
name: release-and-ops
description: How artifacts are published and operated — dated daily and weekly release tags, idempotent same-day publish, the freshness gate, and the CI workflows. Load when working on releases, tags, GH_TOKEN, cron schedules, or CI.
owner: "@dataengineergaurav"
department: engineering
last-verified: 2026-10-24
---

# Release & Ops

Owns shipping dated releases idempotently and the operational surface around them.

## Contract

- **owns:** release tagging, idempotent publish, the freshness gate's operational role, CI workflows.
- **hands-off-to:**
  - `data-quality-gates` — a release should only ship behind green gates.
- **routes-from:**
  - `medallion-layer-contracts` — the built weekly artifact is what gets published.
  - `engineering/_department` — any release / ops task.

## Triggers

GitHub release, `release-YYYY-MM-DD`, `release-week-YYYYWxx`, GH_TOKEN, tag reuse,
cron, `weekly.yml`, "publish failed", "why did it reuse the release".

## Workflow

- **Tagging.** Daily artifacts tag `release-YYYY-MM-DD`, derived from the
  `rent_contracts_YYYYMMDD.csv` filename in `publish_artifacts_to_github`. Weekly
  artifacts tag `release-week-YYYYWxx`. One release per data date / week holds the
  CSV, `etl_status.json`, and the DuckDB together.
- **Idempotent publish.** `GitHubRelease.publish` checks `release_exists(tag)`:
  if the tag exists (a same-day re-run), it REUSES the release and uploads into
  it rather than creating a duplicate. Requires `GH_TOKEN`; absent token → publish
  is skipped with a warning, not an error.
- **Freshness as an ops signal.** The weekly build's `WEEKLY_FRESHNESS_GATE`
  raises on a stalled feed, so a missing weekly release means "the feed stalled",
  distinct from a quiet day (a release with `etl_status.json` and no CSV).
- **CI.** `.github/workflows/weekly.yml` builds and publishes the weekly DuckDB
  via `uv run python -m lib.analysis.build_weekly_duckdb`. Quality gates run on
  push (`build_and_deploy.yml`): `make lint` + `make coverage`.

## Gates

- Publish only if there are existing files; never overwrite with an empty artifact.
- Reuse an existing same-day/week tag; do not duplicate releases.
- A stalled feed must surface as a missing release, not a stale one.
- All ops commands via `uv`/`make`.

## References

- `run_etl_pipeline.py` — `publish_artifacts_to_github` — derives the tag and publishes.
- `lib/workspace/github_client.py` — `publish` — idempotent release publish.
- `lib/workspace/github_client.py` — `release_exists` — same-tag reuse check.
- `lib/config.py` — `WEEKLY_FRESHNESS_GATE` — the stalled-feed signal.
- `lib/analysis/build_weekly_duckdb.py` — `build_weekly_duckdb` — what CI builds and publishes.

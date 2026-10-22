# Independent codebase review — cumulative layers pipeline

Review of the codebase at `0ac3bf8` (branch `rmdd-code-review`). All load-bearing claims below
were verified against source and, where possible, reproduced against the five real production
CSVs in `output/` (16,075 rows) — not just test fixtures.

**Verdict: NEEDS_CHANGES.** Two defects ship wrong data to the published product, and the
publish/restore path can destroy the cumulative history it exists to protect.

## Findings (ranked)

### P0-1. The cumulative store does not accumulate — `contract_id` is `RN` (CONFIRMED, reproduced)

`_prepare_increment` coalesces null `CONTRACT_NUMBER` to `RN` (`lib/analysis/build_layers_duckdb.py:170-176`).
`CONTRACT_NUMBER` is 100% null in every observed payload (0 of 16,075 rows across the five files —
matching the branch's own `lib/classes/silver_contract.py:59` and `docs/DATA_DICTIONARY.md`), and
`RN` restarts at 1 in every file. `INSERT OR REPLACE` on that key therefore overwrites unrelated
rows from previous days.

Reproduced through the real `ingest_layers` on the five real CSVs:

```
after 20260913 (1,714 in):  store=1,714
after 20260914 (  886 in):  store=1,714
after 20260915 (4,589 in):  store=4,589
after 20260916 (4,580 in):  store=4,589
after 20260917 (4,306 in):  store=4,589   ← 4,306 of today + 274 + 9 orphaned tails

input rows: 16,075   store total: 4,589 (28.6%)   destroyed: 11,486
```

`_meta` masks the loss: `data_from` advances to 2026-09-14, so the store *reports* a coherent
3-day window while silently discarding 71% of history. `AggMonthlyRegistrations` — the view
ADR-10 names as the reason for going cumulative — can never form real history.

This contradicts the codebase's own documentation three times over:
- `silver_contract.py:145-157`: "no candidate key is both stable and unique … Cross-day use must
  key on `row_hash`" — `row_hash` already exists and deliberately excludes `RN`.
- `ADR-10` rejects `row_hash` dedup in favor of "a stable business key (`contract_id`)" — a
  premise its own data dictionary falsifies.
- `README.md` "Data guarantees": "replaces the row instead of duplicating it" — it replaces
  *unrelated* rows.

The test suite passes because `tests/test_build_layers_duckdb.py:54-57` makes `contract` a
required fixture argument and every call supplies `"C1"`/`"C2"`/`"C3"`; the production path is
never exercised.

**Fix direction:** key the fact on `row_hash` (with an explicit, documented collision policy for
genuinely identical registrations), or a data-date-qualified composite (`data_date + RN`), and add
a regression test whose fixture leaves `CONTRACT_NUMBER` null with `RN` restarting across days.
Amend ADR-10.

### P0-2. `Agg*` views ignore the exclusions the module claims as contract (CONFIRMED, reproduced)

`gold_layer.py:7-11` states bulk/Hotel/Labor Camps/Virtual Unit exclusions as a module-wide
contract. Only `gold_area_median` and `gold_standard_lease` implement them.
`AggAreaRentStats`, `AggMetroPremium`, `AggMonthlyRegistrations`, `AggProjectRentStats` filter
nothing. On the real data (after the P0-1 store collapse):

```
Al Khairan First: gold n=29,  median 170,000   |  Agg n=231, median 105,000  (62% apart)
Business Bay:     gold n=150, median 104,804   |  Agg n=223, median  80,000
```

`AggMetroPremium` compounds it: both the metro median and the city baseline are unfiltered, so
`premium_vs_city_pct` is a ratio of two contaminated numbers.

`AggAreaRentStats` also publishes `avg/median_price_per_sqft` straight off
`FctContract.price_per_sqft` with no band and no usage filter. Live values: Business Bay
**1,185.6 AED/sqft**, Burj Khalifa **2,883.3** — against the documented residential band of 20-500
(`lib/config.py:psf_band_filter` exists precisely because an unbanded `avg_psf 4691` shipped
before; it is not applied in any SQL). `AggMonthlyRegistrations` additionally emits a `NULL`
month bucket (no date filter).

**Fix direction:** a shared `reportable_rows` predicate (or a view over the fact) applied by all
seven views; apply the PSF band in SQL or drop the columns; test the *populations*, not just
that views resolve.

### P0-3. Publishing is fail-open and destructive-then-fail (CONFIRMED)

- `GitHubRelease.upload_files` (`lib/workspace/github_client.py:90-107`) catches per-file errors
  and continues; `publish` (`:109-124`) catches everything and returns.
- `_delete_asset_named` (`:69-88`) deletes the existing asset **before** the upload — a failed
  upload leaves the release with *no* asset where it had a good one.
- `release_exists` (`:126-142`) returns `None` for any status besides 200/404, so a 403/500
  routes into `create_release`, whose 422/error is then swallowed.
- `publish_layers` (`lib/workspace/publish_layers.py:47-48`) returns the tag unconditionally;
  `main()` logs `Published <tag>`; the workflow step exits 0.

For a data product whose only delivery channel is this upload, publish failure indistinguishable
from success is the wrong default. **Re-raise; verify after upload; never delete-then-fail.**

### P1-4. `daily_layers.yml` converts every GitHub failure into a data state (CONFIRMED)

- Restore step (`.github/workflows/daily_layers.yml:48-56`): all `gh release download` errors are
  suppressed (`2>/dev/null`); 30 misses → "cold start". An auth/token/network outage after day one
  → the store is rebuilt from today's CSV alone → published as the newest cumulative snapshot →
  **the next run restores that truncated store**. Combined with P0-1/P0-3 this is a permanent
  history-loss channel through the release system itself.
- Bronze download (`:59-69`): any nonzero exit becomes "quiet day"; `etl_status.json` is never
  fetched or validated, so a real quiet day and a failed download are indistinguishable.
- Publish step runs **before** the test step (`:83-93`) — a failing gate cannot protect the
  artifact it was written to guard.
- No `concurrency:` group: a `workflow_dispatch` backfill overlapping the 07:00 cron gives two
  runs the same restore→upsert→publish cycle; last-writer-wins.
- The publish condition `hashFiles('output/rents_layers.duckdb') != ''` is satisfied by the file
  restored from a prior release, so a quiet day re-uploads an unchanged store under today's tag.

### P1-5. The freshness check fails open on both of its inputs (CONFIRMED)

`_check_freshness` (`build_layers_duckdb.py:199-211`) returns silently when `expected is None`
(filename doesn't match `rent_contracts_(\d{8})\.csv$` — a renamed asset, `.csv.gz`, or a future
naming change) **and** when `newest is None` (all-null/unparseable registration dates). It is the
only fail-closed check in the layers path, and it is skippable without a trace. An increment with
rows but blank dates is inserted with null `contract_registration_date`; `_meta.data_through`
stays empty.

### P1-6. Batch-scoped labels are frozen into cumulative facts (CONFIRMED)

`is_bulk_registration` counts `(area, amount)` within the daily increment
(`lib/transform/enrichment.py:225-235`): a bulk group split across files (6 + 14) is never
flagged, and `gold_area_median`'s exclusion silently fails. `is_luxury` percentiles
(`:186-207`) are daily-batch-relative but persisted as if they described the cumulative
population. Recompute population-relative labels after upsert, or document the cohort.

### P1-7. `gold_standard_lease` is missing the rent predicate (CONFIRMED)

No `annual_amount IS NOT NULL AND > 0` (`gold_layer.py:35-46`): `count(*)` includes null-rent rows
while `median` ignores them, and negative rents are admitted — conflicting with the documented
Gold contract in `docs/DATA_DICTIONARY.md`.

### P2-8. The published store bypasses the Silver contract entirely

`README.md` bills "Pydantic v2 for the Silver contract", but `to_silver` runs only on the local
parquet path (`run_etl_pipeline.transform_rents`). The layers DuckDB — the *published* artifact —
reads, aliases, enriches and inserts with no quarantine, no violation accounting, and no
`row_hash`. The two "Silver" implementations have already drifted: the pydantic path nulls
`rent_per_sqft` above `max_property_size` (50,000 sqft, `silver_contract.py:185-197`), while
`psf_expression` (`lib/config.py:392-408`) applies only the 200 sqft floor — so the fact carries
PSF values the Silver contract refuses. Either route the layers path through the contract or stop
calling it the Silver contract.

### P2-9. Silent population changes via dimension joins

`gold_area_median`/`gold_standard_lease` inner-join `DimPropertyType` `USING
(ejari_property_type_en, ejari_property_sub_type_en, property_usage_en)`; `DimPropertyType` is a
plain `SELECT DISTINCT` with no null filter (`silver_layer.py:111-119`), and `USING` does not
match nulls. Fact rows with a null type/sub_type/usage drop out of views that previously counted
them (the old flat-fact `gold_area_median` never referenced usage). Probably wanted, but the
decision lives in join semantics, not in a filter or a test.

### P2-10. Secrets and debug surface

- The raw `EJARI_URL` is passed as Scrapy argv (`run_etl_pipeline.py:137-151`), visible to any
  process listing; the `TimeoutExpired` exception (which embeds that command) is logged
  (`:154-157`); spider stderr is logged raw on failure (`:180-182`). `_redact_url` covers exactly
  one log line.
- `GH_TOKEN` is job-wide in `daily_layers.yml` (available to pip install and tests); a read-only
  credential for restore and a scoped write credential for publish would be the minimum.
- `ACTIONS_RUNNER_DEBUG`/`ACTIONS_STEP_DEBUG` are hardcoded `true` in all three workflows —
  debug logging on every scheduled run.

### P2-11. The quality gate is not reproducible and does not pass locally

- `build_and_deploy.yml` installs unpinned `requirements*.txt` and never runs `uv sync --locked`;
  ADR-11's reproducibility claim holds only for the `uv run` steps. Bare `ruff` behind
  `ruff format --check` means a ruff release breaks CI with no code change.
- `make lint` fails on any checkout with `.looker-skills` populated (3 F541 in upstream code;
  not in `[tool.ruff] exclude`) — CI is green only because `actions/checkout` never populates the
  submodule. `.looker-skills` is a bare 160000 gitlink with no `.gitmodules` entry, so
  `git submodule status` fails and the six `.agents/skills/*` symlinks dangle on a fresh clone.
- `daily_layers.yml` installs deps twice through two managers (unpinned pip, then `uv run` from
  the lock) — the pip layer is dead weight and divergent.

### P2-12. Test gaps that let P0-1/P0-2 ship

135 tests pass in <1s, but: the production key path (null `CONTRACT_NUMBER`, restarting `RN`) is
never exercised; no test asserts which rows the `Agg*` views include; the dimension-join null-drop
is untested; `publish_layers` is never tested against an upload failure; `_check_freshness` with
an unresolvable date or all-null dates is uncovered; the Scrapy argv/stderr redaction paths are
untested. `--cov=lib` excludes `run_etl_pipeline.py` — the orchestration layer that owns the
publish and no-data decisions — from the coverage floor entirely.

## What is genuinely strong

- **Raw-schema pinning** (`lib/config.py:197-295`): all 44 payload columns pinned in one owner,
  with a drift test and a test that overrides beat inference past the 100-row window. This closes
  two recorded production breakages (PARKING String→Boolean, money-column vstack mismatch).
- **PSF single-owner consolidation**: one floor, one guarded division, one band helper.
- **`_delete_asset_named`**: fixes the real 422 duplicate-asset failure that blocked same-day
  re-runs, for every publisher through the shared client.
- **Documentation honesty**: `dropped_columns` rationale, the `'?'`-mask analysis, the
  dedup-never-sum group rollup, and the `row_hash`/`record_id` key tradeoff are documented with
  measured numbers. ADR-07/08 carry supersession banners. This review could pin its findings to
  the branch's own docs precisely because they are that specific.
- The fail-open no-data design (ADR-03) is deliberate, documented, and surfaced via
  `etl_status.json` + `NO_NEW_DATA` log lines.

## Relationship to the prior two reviews

Both earlier reviews (`review-gpt-5.6-sol.md`, `review-opus-5.md`) flagged the RN key, the Agg
exclusions, the fail-open publish, and the workflow error suppression. This review independently
confirms all of those, and adds: quantified production-data loss (16,075 → 4,589; 11,486 rows),
the P2-8 bypass of the Silver contract on the published path, the PSF ceiling drift between the
two Silver implementations, the hardcoded debug flags, and the exact composition of the surviving
store (today's rows + orphaned tails). One nuance: the "quiet-day republish of a stale store" is
wasted bandwidth and a misleading release, not data corruption — `_meta.data_through` still tells
the truth; I'd rank it below the restore-failure → cold-start channel (P1-4).

## Recommended fix order

1. P0-1 stable key + regression fixture (nothing else matters until the store accumulates).
2. P0-2 shared reportable predicate across all seven views + PSF band + population tests.
3. P0-3/P1-4 make publish and restore fail-closed; add a concurrency group; run tests before
   publishing; validate `etl_status.json`.
4. P1-5/6/7 freshness hardening, post-upsert label recomputation, rent predicate.
5. P2 items — Silver-contract routing decision, join intent, secrets, CI pinning, lint boundary.

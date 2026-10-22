# Final consolidated issues report — daily cumulative Silver+Gold DuckDB release pipeline, raw-schema pinning, and CI quality gates (HEAD)

Issues are sorted from highest to lowest combined importance/confidence.

---

## 1. Cumulative store keys on `RN` and overwrites its own history

**Severity: High** · *(cluster: cumulative identity — see also #10)*

- **User impact:** The "cumulative" artifact holds roughly one day of contracts, not history. Every consumer query is wrong: `AggMonthlyRegistrations` can only show the current month or two, `_meta.data_from` advances with `data_through` so the loss is invisible, and yesterday's rows are permanently gone from the published database.
- **Issue:** `_prepare_increment` in `lib/analysis/build_layers_duckdb.py:171-175` coalesces a null `contract_id` to `RN`, and the fact is upserted on that key with `INSERT OR REPLACE` (`build_layers_duckdb.py:194-195`, `lib/analysis/silver_layer.py:65-95,165-169`). The branch's own docs record `CONTRACT_NUMBER` as 100% null in every observed payload (`docs/DATA_DICTIONARY.md:54`, `lib/classes/silver_contract.py:59`; 0 non-null of 16,075 rows in `docs/superpowers/specs/2026-09-27-silver-contract-design.md`) and `RN` as a per-fetch gateway ordinal that restarts at 1 each file. Two payload-shaped days with `RN=1..3` were reproduced as 3 rows total, with day 1 fully replaced. ADR-10's stable-business-key premise does not hold for the production feed.
- **Recommendation:** Key `FctContract` on something stable across files — Silver's `row_hash` (deliberately computed without `RN`) composed with the data date, or an explicit compound provenance key with a documented collision and correction policy. Add a fixture whose `CONTRACT_NUMBER` is null and assert cross-day growth.

## 2. Publishing is fail-open end to end, and asset refresh deletes before it uploads

**Severity: High** · *(cluster: release integrity — see also #4, #7, #16)*

- **User impact:** A failed upload looks exactly like a success: the log says `Published release-YYYY-MM-DD`, the workflow exits 0, and consumers get a release with no cumulative database — or, during a refresh, one where the previously good asset has already been deleted and nothing replaced it.
- **Issue:** `GitHubRelease.publish()` and `upload_files()` catch every exception and only log it (`lib/workspace/github_client.py:91-125`), `_delete_asset_named` removes the existing asset before the upload attempt (`:72-88`), and `publish_layers()` returns the tag unconditionally (`lib/workspace/publish_layers.py:47-48`). A forced upload exception still returned `release-2026-10-05`. `release_exists()` compounds it by returning `None` for any status other than 200/404, so a 403 or 500 falls through to `create_release` and that error is swallowed too.
- **Recommendation:** Re-raise or return a checked status from `publish`/`upload_files`, have `publish_layers` fail the process on a failed upload, stage and verify the new asset before deleting the old one, and treat non-200/404 release lookups as errors rather than "absent".

## 3. The freshness gate — the only fail-closed check — is skippable

**Severity: High**

- **User impact:** A stalled feed ships as a normal day. Nothing else in the daily path turns a stall into a failure, so consumers see a fresh release tag over unchanged or date-less data.
- **Issue:** `_check_freshness` in `lib/analysis/build_layers_duckdb.py:199-205` returns immediately when `expected` is `None`, and `expected` comes from a strict `rent_contracts_(\d{8})\.csv$` filename match — any rename, `.csv.gz`, or backfill filename disables the gate. Independently, when an expected date does exist, the check returns when the increment's maximum registration date is `None`; the reader ignores parse errors and parses dates non-strictly (`:154-161`), so a CSV of rows with blank registration dates was ingested with null `contract_registration_date` and an empty `_meta.data_through`.
- **Recommendation:** Require an explicitly resolved data date (CLI argument, falling back to filename) and fail when it cannot be resolved. Require parseable, non-null date evidence in the increment and reject before opening or mutating the DuckDB.

## 4. GitHub download failures are read as cold starts and quiet days

**Severity: High** · *(cluster: release integrity — see also #2, #7)*

- **User impact:** An auth, API, or network outage produces a valid-looking release. Depending on which download failed, consumers get a one-day database published as the newest cumulative snapshot, or a stale store up to 30 days old attached to today's tag, implying a freshness it does not have.
- **Issue:** The prior-store restore loop in `.github/workflows/daily_layers.yml:49-56` suppresses every `gh release download` error and treats 30 consecutive failures as a cold start. The Bronze download step (`:59-69`) converts any nonzero exit to "quiet day" and never validates `etl_status.json`. The publish step is gated on `hashFiles('output/rents_layers.duckdb')`, which is satisfied by the store pulled from a prior release, so the ingest step's early `exit 0` and the publish step's condition disagree about what "nothing to do" means.
- **Recommendation:** Distinguish an absent asset from an API/auth/transport failure and stop the job on the latter. Treat a missing CSV as quiet only when a validated `etl_status.json` reports `outcome="no_data"`, make bootstrap an explicit mode rather than an inferred one, and gate publication on the ingest step having actually written a new store.

## 5. `EJARI_URL` escapes redaction through subprocess argv and stderr

**Severity: High**

- **User impact:** The endpoint token can be read by any process listing on the runner and can land in CI logs — the exposure the redaction helper was added to prevent.
- **Issue:** `_redact_url` protects only the initial download log line. The raw URL is still passed as a Scrapy argument (`run_etl_pipeline.py:124,146`), the timeout handler logs the `TimeoutExpired` object that carries that full command (`:154-157`), and spider stderr is logged raw (`:181-182`) where Scrapy writes request lines containing the query string. Tests stub the downloader through an `example.com` seam, so neither path is exercised.
- **Recommendation:** Pass the URL to the spider via the environment instead of argv, log a fixed message on timeout rather than the exception object, and sanitize or suppress all captured subprocess output before logging.

## 6. The four new `Agg*` Gold views apply none of the documented exclusions

**Severity: High** · *(cluster: Gold contract — see also #8, #9, #20)*

- **User impact:** Two views in the same file report medians 7.5x apart for the same area. Anyone reading `AggAreaRentStats`, `AggMetroPremium`, `AggMonthlyRegistrations`, or `AggProjectRentStats` gets bulk registrations, Hotel/Labor Camps, Virtual Units, and non-positive rents mixed into headline rent figures, while the module docstring promises those are excluded.
- **Issue:** `lib/analysis/gold_layer.py` states the exclusions as a module-wide contract, but only `gold_area_median` and `gold_standard_lease` implement them. A seeded fact of 48 rows returned `gold_area_median` = 12 rows / 100,000 median and `AggAreaRentStats` = 48 rows / 750,000 median for the same area. `AggMetroPremium` divides one contaminated median by another, so `premium_vs_city_pct` is unusable.
- **Recommendation:** Build one reportable-rows view over `FctContract` carrying the shared predicate and have every `Agg*` view read from it, or narrow the `gold_layer.py` docstring so it no longer claims coverage the views lack. Add tests asserting which rows each view includes.

## 7. The release is mutated before the tests that are supposed to guard it

**Severity: High** · *(cluster: release integrity — see also #2, #4)*

- **User impact:** A failing quality gate cannot protect the public artifact, because the artifact is already published when the gate runs.
- **Issue:** `.github/workflows/daily_layers.yml:83-93` publishes the cumulative DuckDB into the day's release and then runs the test suite.
- **Recommendation:** Run the test suite and artifact verification (row counts, `_meta`, view resolution) before any release write, and publish only on success.

## 8. `gold_standard_lease` admits rents its own contract excludes

**Severity: Medium** · *(cluster: Gold contract — see also #6)*

- **User impact:** The standard-lease median is computed over leases that should not be in the population, and `n` overstates the sample. One AED 80,000 lease plus one AED -1,000 lease returns `n=2`, `median_rent=39,500`.
- **Issue:** Unlike `gold_area_median`, the `gold_standard_lease` definition in `lib/analysis/gold_layer.py:17-50` has no positive/non-null annual-rent predicate, contradicting the documented Gold contract (`docs/DATA_DICTIONARY.md:154-166`). Null rents are counted in `n` while the median ignores them.
- **Recommendation:** Apply the same money predicate used by `gold_area_median` and pin both `n` and the aggregates with invalid-rent fixtures.

## 9. Batch-relative bulk and luxury labels are frozen into cumulative facts

**Severity: Medium** · *(cluster: Gold contract — see also #6)*

- **User impact:** Bulk registrations split across days are never flagged, so they stay in `gold_area_median`, which depends on that exclusion. Luxury flags describe a single day's distribution while being published as attributes of the cumulative population.
- **Issue:** `lib/transform/enrichment.py:186-205,225-234` computes luxury percentiles and bulk group counts from the incoming frame only, and the cumulative builder persists those booleans without recomputing over `FctContract` (`lib/analysis/build_layers_duckdb.py:191`). Six identical area/amount rows on each of two days produced 12 cumulative rows with zero bulk flags, against a published rule of "group over 10 is bulk".
- **Recommendation:** Recompute population-relative labels over the fact after upsert, or rename the columns and document the cohort scope explicitly in schema and docs.

## 10. The production key path, release failures, and view populations are untested

**Severity: Medium** · *(cluster: cumulative identity — see also #1)*

- **User impact:** All 135 tests pass and coverage sits at 79% against a 75% floor while the pipeline silently loses data, so CI gives no warning before a broken artifact reaches consumers.
- **Issue:** Every fixture in `tests/test_build_layers_duckdb.py` supplies an explicit `CONTRACT_NUMBER` (`:55-58`), so the `RN` fallback that governs all real data has no coverage and `test_second_day_grows_without_double_counting_overlap` asserts the right property on data the pipeline never sees. Also uncovered: unresolvable or all-null registration dates, release download and upload failures, cross-increment bulk groups, invalid rents in `gold_standard_lease`, `Agg*` view populations, the dimension-join null drop, and the argv/stderr redaction paths. `--cov=lib` excludes `run_etl_pipeline.py`, which owns the publish and no-data decisions, and `market_analytics.py` sits at 24%.
- **Recommendation:** Add regression coverage for each shipping contract above, starting with a null-`CONTRACT_NUMBER` cross-day growth test, and bring the orchestration module inside the coverage measurement.

## 11. Dimension inner joins silently changed who is counted

**Severity: Medium**

- **User impact:** Rows the previous `gold_area_median` counted are now dropped with no record of the decision, so area medians shift for reasons no document explains.
- **Issue:** The new views inner-join natural-key dimensions (`JOIN DimPropertyType USING (ejari_property_type_en, ejari_property_sub_type_en, property_usage_en)` in `lib/analysis/gold_layer.py`), and `USING` does not match nulls. `DimPropertyType` is rebuilt as a plain `SELECT DISTINCT` with no null filter, so any fact row with a null type, sub-type, or usage disappears — 12 null-usage rows vanished from a seeded probe.
- **Recommendation:** Make the intent explicit: filter nulls in the fact with a stated reason, or use left joins and handle the unmatched groups deliberately. Add a test that pins the chosen behavior.

## 12. `daily_layers.yml` has no concurrency guard on a single-writer store

**Severity: Medium**

- **User impact:** A manual backfill overlapping the 07:00 cron gives last-writer-wins on the cumulative file, and the loser's rows are gone from the published artifact.
- **Issue:** `.github/workflows/daily_layers.yml` accepts a `workflow_dispatch` `data_date` input and declares no `concurrency` group, so two runs can perform the same download-upsert-publish cycle against one store.
- **Recommendation:** Add a `concurrency` group (with `cancel-in-progress: false`) covering the workflow so cumulative writes serialize.

## 13. `.looker-skills` is an unconfigured gitlink that breaks the lint gate it ships with

**Severity: Medium**

- **User impact:** A fresh clone cannot materialize the directory, so the six new `.agents/skills/*` symlinks dangle for everyone but the author, and `make lint` — the gate this change introduces — fails on any machine where the directory is populated.
- **Issue:** `.looker-skills` is committed as a bare 160000 gitlink with no `.gitmodules` entry, so `git submodule status` errors out. `[tool.ruff] exclude` in `pyproject.toml` covers `output`, `rents_scraper`, `.venv`, and `build` but not `.looker-skills`, so `ruff check .` reports 3 F541 errors from upstream code and `ruff format --check .` wants to reformat a file. CI stays green only because `actions/checkout@v3` runs without `submodules: true`.
- **Recommendation:** Add the `.gitmodules` mapping or vendor the content directly, and add `.looker-skills` to the Ruff exclude list so the lint boundary matches repository-owned source.

## 14. The CI gate does not install from the committed lockfile

**Severity: Medium**

- **User impact:** ADR-11's reproducibility claim does not hold for the gate that actually blocks merges; a transitive release can turn CI red or green with no code change.
- **Issue:** `.github/workflows/build_and_deploy.yml:29-43` runs `make build` and pip-installs unpinned `requirements.txt` / `requirements-dev.txt` (`Makefile:40-45`); `uv.lock` is consulted only by the `uv run` steps in `daily_layers.yml`.
- **Recommendation:** Run `uv sync --locked --all-groups` and execute the push gate inside that environment, or narrow ADR-11's claim to the daily workflow.

## 15. The run-status marker is best-effort

**Severity: Medium**

- **User impact:** The marker added to make "no data" unambiguous can be missing while the run reports success, leaving consumers unable to tell a quiet day from a failed one.
- **Issue:** `_write_run_status()` downgrades filesystem failures to warnings (`run_etl_pipeline.py:88-108`) and an empty publication list is a non-fatal return (`:273-277`), so the pipeline can succeed without producing or publishing `etl_status.json`.
- **Recommendation:** Treat marker creation and publication as required steps and fail the run when either does not complete.

## 16. The release token is scoped to the whole job

**Severity: Medium** · *(cluster: release integrity — see also #2)*

- **User impact:** A write-capable credential is exposed to far more code than needs it, so a compromised dependency or spider step can mutate releases.
- **Issue:** `GH_TOKEN` is declared at job level in `.github/workflows/daily_layers.yml:17-19`, making it available during dependency installation, ingestion, smoke queries, and tests.
- **Recommendation:** Use a read-only credential for the restore step and a narrowly scoped write credential only on the publish step, with minimal `permissions`.

## 17. PSF floor is centralized but the ceiling is not

**Severity: Medium**

- **User impact:** Rows Silver refuses to price still carry a `price_per_sqft` into the fact, where the unbanded Gold PSF columns average them into published figures.
- **Issue:** Silver nulls `rent_per_sqft` when `actual_area > max_property_size`, while `psf_expression` in `lib/config.py` applies only the `PSF_MIN_AREA_SQFT` floor and divides. `docs/superpowers/plans/2026-09-27-silver-contract.md` measures this as live (3 rows above the 50,000 max on 20260917).
- **Recommendation:** Move the ceiling into `psf_expression` so the floor and the maximum have the same single owner.

## 18. Unbanded PSF columns in `AggAreaRentStats`

**Severity: Medium** · *(cluster: Gold contract — see also #6)*

- **User impact:** Reopens a defect that already shipped a wrong headline once — an unbanded PSF average of 4,691 against a 20-500 band.
- **Issue:** `AggAreaRentStats` publishes `avg_price_per_sqft` and `median_price_per_sqft` directly off `FctContract.price_per_sqft` with neither the `psf_band_filter` band nor a sample-size floor (`lib/analysis/gold_layer.py`).
- **Recommendation:** Apply the SQL equivalent of `psf_band_filter` plus the sample-size floor, or drop the two columns from the view.

## 19. Lint and artifact-facing dependencies are unpinned

**Severity: Low**

- **User impact:** CI can break on a release with no code change, and consumers may receive a `.duckdb` file written by whatever version CI happened to resolve.
- **Issue:** `requirements-dev.txt` carries a bare `ruff` while `make lint` runs `ruff format --check`, whose output changes across releases. `duckdb` is likewise unpinned even though the published database is the product consumers open.
- **Recommendation:** Pin `ruff` and `duckdb` to exact versions, and let `uv.lock` govern the gate (see #14).

## 20. Null month bucket in the registrations trend view

**Severity: Low** · *(cluster: Gold contract — see also #6)*

- **User impact:** A `NULL` month row appears next to real months in the trend view, breaking charts and time-series joins.
- **Issue:** `AggMonthlyRegistrations` in `lib/analysis/gold_layer.py` does not filter rows with a null `contract_registration_date` before bucketing by month.
- **Recommendation:** Exclude null registration dates from the view.

## 21. The placeholder-CSV heuristic has two owners

**Severity: Low**

- **User impact:** The two copies can drift, so the ingest path and the orchestrator may disagree about whether a given file counts as "no data".
- **Issue:** The "<100 bytes or header-only means no data" rule is implemented in both `build_layers_duckdb._usable` and `run_etl_pipeline._data_rows`.
- **Recommendation:** Give the rule a single owner and have both call sites use it.

## 22. Dependencies are installed twice through two managers

**Severity: Low**

- **User impact:** Longer runs and an ambiguous environment: only one of the two installed dependency sets is reproducible.
- **Issue:** `.github/workflows/daily_layers.yml` runs `pip install -r requirements.txt && pip install lib/ && pip install duckdb pyarrow`, then executes every command under `uv run`, which resolves its own environment from `pyproject.toml` and `uv.lock`.
- **Recommendation:** Drop the pip layer and rely on the uv-managed, locked environment.

## 23. Stale code pointer in `requirements.txt`

**Severity: Low**

- **User impact:** A reader auditing why `pyarrow` is required is sent to a file that no longer exists and may conclude the dependency is dead.
- **Issue:** The comment justifying `pyarrow` cites `build_weekly_duckdb.py:112`, deleted in this change; the real consumer is `increment.to_arrow()` in `lib/analysis/build_layers_duckdb.py`.
- **Recommendation:** Repoint the comment at `build_layers_duckdb`; keep the dependency.

## 24. Three unrelated strands ship in one branch

**Severity: Low**

- **User impact:** The pipeline change is harder to review, and the agent-tooling bundle is what breaks the lint gate and carries the orphaned gitlink.
- **Issue:** The branch combines the layers pipeline work, a full documentation set (ARCHITECTURE, DATA_DICTIONARY, OPERATIONS, ANALYST_COOKBOOK, ADRs 0007-0011), and an unrelated Looker/agent tooling bundle (`.looker-skills`, six `.agents/skills/*` symlinks, `.commandcode/` agents, skills, settings, taste files).
- **Recommendation:** Split the tooling bundle into its own change so the pipeline and docs can be evaluated and reverted independently.

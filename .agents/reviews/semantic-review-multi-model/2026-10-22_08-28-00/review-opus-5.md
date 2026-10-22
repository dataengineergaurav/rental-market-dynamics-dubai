# Cumulative Silver+Gold DuckDB layers store, raw-schema pinning, and a CI quality gate

This branch replaces the weekly pooled DuckDB with a single cumulative `rents_layers.duckdb` that the daily job upserts into and publishes to the day's `release-YYYY-MM-DD` tag alongside the bronze CSV. The weekly builder, its cross-file `row_hash` dedup and the freshness gate are deleted; idempotency now rests on a `contract_id` primary key with `INSERT OR REPLACE`. Supporting work pins the whole 44-column raw Ejari CSV schema so per-file type inference can't break a build, consolidates the PSF floor and guarded division into single owners in `lib.config`, redacts `EJARI_URL` from the download log, stops push builds from running the ETL, and adds a `ruff` + coverage gate. A large docs set (ARCHITECTURE, DATA_DICTIONARY, OPERATIONS, ANALYST_COOKBOOK, ADRs 0007-0011) lands with it.

**Watch for:** the cumulative store does not accumulate — `CONTRACT_NUMBER` is 100% null in every observed payload (the branch's own `docs/DATA_DICTIONARY.md` says so), so `contract_id` falls back to `RN`, a per-file row ordinal that restarts at 1 each day, and day 2 overwrites day 1 (**confirmed**, reproduced below). The four new `Agg*` views apply none of the bulk/Hotel/Virtual-Unit exclusions the module docstring claims, so two views in the same file report medians 7.5x apart for the same area (**confirmed**). `make lint`, the gate this change adds, fails on this branch (**confirmed**). Publishing is fail-open from end to end: a failed upload is logged and the job exits 0 (**confirmed**).

**Verdict**: NEEDS_CHANGES

## High-level view

The architectural move is sound and well argued: a cumulative store keyed on a contract identifier replaces window-and-dedup code with a database constraint, and ADR-10 makes the case clearly. The problem is the identifier. `CONTRACT_NUMBER` is null in the entire observed payload, so every row's `contract_id` is `RN` — a gateway row ordinal that is unique within a file and nowhere else. `INSERT OR REPLACE` therefore replaces yesterday's row 1 with today's row 1. The store holds roughly one day of data forever, `_meta.data_from` advances with `data_through`, and `AggMonthlyRegistrations` — the view ADR-10 names as the reason for going cumulative — can only ever return the current month or two. The test suite misses this because every fixture row supplies an explicit `CONTRACT_NUMBER`, so the production path is never exercised.

The Gold layer now has two tiers of trustworthiness in one file. `gold_area_median` and `gold_standard_lease` carry the documented exclusions (bulk registrations, Hotel and Labor Camps sub-types, Virtual Unit property types, non-positive rent). The four new `Agg*` views carry none, and `AggAreaRentStats` additionally publishes `avg_price_per_sqft` with no banding at all. On a seeded fact these disagree by a factor of 7.5 on the same area's median rent. The `gold_layer.py` docstring states the exclusions as a module-wide contract, which makes the gap easy to miss downstream.

The views also switched from filtering a flat fact to inner-joining natural-key dimensions, which silently changes who is counted: rows with a null `property_usage_en` were in the old `gold_area_median` population and are now dropped by the `DimPropertyType` join, since `USING` does not match on nulls.

Freshness is the one fail-closed check in the new path, and it is reachable only when the input filename matches `rent_contracts_YYYYMMDD.csv`; any other name leaves `expected_date` as `None` and the check returns without looking. Everything downstream of it is fail-open. `GitHubRelease.publish` and `upload_files` swallow every exception, so `publish_layers` returns its tag and logs "Published release-..." whether or not the asset landed, and the workflow step exits 0.

`daily_layers.yml` gates publishing on `hashFiles('output/rents_layers.duckdb')`, which is satisfied by the DuckDB pulled from a *prior* release. On a quiet day the ingest step exits 0 early and the publish step attaches an unchanged — possibly 30-day-old — store to today's tag. The workflow has no `concurrency` group, so a manual backfill overlapping the 07:00 cron has both runs read-modify-write the same cumulative file with last-writer-wins.

The raw-schema pinning is the strongest piece of the change. Pinning all 44 payload columns in one place, with a test that fails when the column set drifts and a test that proves the overrides beat inference past the 100-row window, closes a hole that has broken both the daily and weekly builds. The PSF consolidation is similarly real: `PSF_MIN_AREA_SQFT` and `psf_expression` now have one owner and `market_analytics` no longer re-divides. One asymmetry survives — Silver nulls PSF above `max_property_size`, `psf_expression` has no upper bound.

Secret handling improved but is partial. `_redact_url` strips the query string from the one log line that interpolated it, and that line is pinned by a test. The same URL still reaches the Scrapy subprocess argv, and the spider's stderr is logged on failure.

The new CI gate does not pass on the branch that introduces it: `.looker-skills`, a submodule added here, is absent from `[tool.ruff] exclude`, so `ruff check .` reports 3 errors and `ruff format --check .` wants to reformat a file. CI happens to stay green only because `actions/checkout@v3` runs without `submodules: true` — and that submodule is committed as a bare gitlink with no `.gitmodules` entry at all, so `git submodule status` fails and the six `.agents/skills/*` symlinks pointing into it dangle on a fresh clone. ADR-11 claims reproducible dependencies, but the CI gate installs from unpinned `requirements.txt` and `requirements-dev.txt`; the committed `uv.lock` is only consulted by the `uv run` steps in `daily_layers.yml`. An unpinned `ruff` behind `format --check` will fail CI on a release with no code change.

Removing `EJARI_URL` and `GH_TOKEN` from the push build's job env, so a push can no longer extract or publish, is the right fix for the race it describes and is worth keeping in mind as the model for the other workflows.

<details>
<summary>Issues (18)</summary>

1. **`contract_id` collapses to `RN`** (confirmed) — `CONTRACT_NUMBER` is 100% null upstream, so the PK is a per-file row ordinal and each day's ingest overwrites the previous day's rows. Key the fact on something stable across files (Silver's `record_id`/`row_hash` composition, or a date-qualified key) and add a test whose fixture leaves `CONTRACT_NUMBER` null.
2. **`Agg*` views skip the documented exclusions** (confirmed) — `AggAreaRentStats`, `AggMetroPremium`, `AggMonthlyRegistrations` and `AggProjectRentStats` include bulk registrations, Hotel/Labor Camps, Virtual Unit and non-positive rents. Apply the shared predicate (a `FctContract` reportable-rows view) or narrow the `gold_layer.py` docstring so it doesn't claim coverage it lacks.
3. **Unbanded PSF in Gold** (confirmed) — `AggAreaRentStats` publishes `avg_price_per_sqft` / `median_price_per_sqft` with neither the band nor a sample-size floor. Apply `psf_band_filter`'s SQL equivalent or drop the columns.
4. **Dimension joins change who is counted** (confirmed) — inner `USING` joins drop rows with a null `area_name_en`, `property_usage_en`, type or sub_type that the previous flat-fact views included. Make the intent explicit: filter nulls in the fact, or use left joins.
5. **Freshness check silently skipped** (confirmed) — `_check_freshness` returns immediately when the CSV filename doesn't match `rent_contracts_YYYYMMDD.csv`. Require an expected date (CLI arg or filename) and fail when it can't be resolved.
6. **Publishing is fail-open** (confirmed) — `GitHubRelease.publish`/`upload_files` swallow all exceptions, so `publish_layers` logs success after a failed upload and the workflow exits 0. Re-raise, or have `publish` return a status that `publish_layers` checks.
7. **Quiet-day republish of a stale store** (confirmed) — the publish step's `hashFiles` condition is satisfied by the DuckDB pulled from a prior release, so an unchanged store is attached to today's tag. Gate on the ingest step having actually run.
8. **No concurrency guard on `daily_layers.yml`** (confirmed) — a `workflow_dispatch` backfill overlapping the cron has two runs read-modify-write the same cumulative file. Add a `concurrency` group.
9. **`make lint` fails on this branch** (confirmed) — `.looker-skills` is not in `[tool.ruff] exclude`; `ruff check .` reports 3 F541 errors and `ruff format --check .` flags one file. Add the exclude.
10. **Submodule committed without `.gitmodules`** (confirmed) — `.looker-skills` is a bare 160000 gitlink, so `git submodule status` errors and the six `.agents/skills/*` symlinks into it dangle on a fresh clone. Add the mapping or commit the content directly.
11. **Lint tooling unpinned behind a formatter check** (likely) — bare `ruff` in `requirements-dev.txt` plus `ruff format --check` means a ruff release breaks CI with no code change. Pin it.
12. **CI doesn't use the committed lock** (confirmed) — `build_and_deploy.yml` installs from unpinned `requirements.txt`/`requirements-dev.txt`, so ADR-11's reproducibility claim holds only for the `uv run` steps. Install via `uv sync --locked`, or scope the claim.
13. **Redaction covers one log line** (argv leak confirmed, stderr leak likely) — the raw `EJARI_URL` still reaches the Scrapy subprocess argv and the spider's stderr is logged on failure. Pass the URL via env to the spider and redact before logging stderr.
14. **PSF upper bound diverges** (confirmed) — Silver nulls PSF above `max_property_size`; `psf_expression` applies only the floor, so `enrichment`/`market_analytics` compute a PSF for rows Silver refuses. Move the ceiling into `psf_expression`.
15. **Duplicated placeholder heuristic** (confirmed) — the "<100 bytes or header-only means no data" rule exists in both `build_layers_duckdb._usable` and `run_etl_pipeline._data_rows`. Give it one owner.
16. **Null month bucket in the trend view** (confirmed) — `AggMonthlyRegistrations` doesn't filter a null `contract_registration_date`, so a `NULL` month row appears alongside real months. Exclude it.
17. **Stale pointer in `requirements.txt`** (confirmed) — the comment justifying `pyarrow` cites `build_weekly_duckdb.py:112`, deleted here. Repoint it at `build_layers_duckdb` (`increment.to_arrow()`); the dependency itself is still needed.
18. **Three unrelated strands in one branch** (confirmed) — pipeline change, docs set, and a Looker/agent tooling bundle (`.looker-skills`, `.commandcode/`) that breaks the lint gate and carries the orphaned gitlink. Split the tooling out.

</details>

<details>
<summary>Details</summary>

### `contract_id` is `RN`, so the cumulative store overwrites itself

`silver_layer.py` makes the key choice explicit and justifies it well:

> The fact is keyed on `contract_id` (the payload `CONTRACT_NUMBER`) so a same-day re-run and the consecutive-day 2-day-window overlap are idempotent via `INSERT OR REPLACE` — the DB constraint replaces the cross-file dedup the weekly builder used to do in code.

`_prepare_increment` then coalesces to `RN` when the contract number is absent:

```python
combined = combined.with_columns(
    pl.coalesce([pl.col("contract_id"), pl.col("RN").cast(pl.Utf8)]).alias("contract_id")
)
```

The branch's own documentation records what that fallback means in practice. `docs/DATA_DICTIONARY.md`: `CONTRACT_NUMBER` is "100% null in every observed file. The `contract_id` PK falls back to `RN`", and `RN` is the "Gateway row ordinal" whose derived `record_id` is "unique within a file". `docs/superpowers/specs/2026-09-27-silver-contract-design.md` quantifies it: 0 non-null of 16,075 rows across five daily files. So the fallback is not an edge case — it is the only path.

Reproduced against the real `ingest_layers` with two days of payload-shaped CSV (`CONTRACT_NUMBER` empty, `RN` restarting at 1, distinct registration dates):

```
after day 1: 3
after day 2: 3
contract_ids: [('1',), ('2',), ('3',)]
_meta: (data_from=2026-09-15, data_through=2026-09-15, total_contracts=3)
monthly: [('2026-09', 3, 240000.0)]
```

Day 2 replaced day 1 in full. `data_from` moved forward with `data_through`, so `_meta` reports a one-day store rather than exposing the loss, and the cumulative history that `AggMonthlyRegistrations` exists to serve never forms. In production the store will hold roughly `max(daily row count)` rows: today's rows plus a tail of stale rows from whichever past day happened to be longest.

The test suite cannot catch this because `tests/test_build_layers_duckdb.py::_row` takes `contract` as a required argument and every call passes `"C1"`, `"C2"`, `"C3"`. `test_second_day_grows_without_double_counting_overlap` asserts exactly the right property on data the pipeline never sees.

Silver already derives a key built for this — `record_id` is `row_hash:RN`, and `row_hash` deliberately "excludes `RN`" so it is "safe across days". A fact key composed from the stable fingerprint, or from `RN` qualified by the data date, would restore both idempotency and accumulation.

### The Gold views disagree about what is reportable

`gold_layer.py` states the exclusions as a module contract:

> Hotel / Labor Camps sub-types and Virtual Unit property types are excluded from rent medians, as is bulk-registration stock, matching the shipped `gold_area_median` contract.

Two of the seven views implement it. Seeding a fact with 12 clean residential flats at 100k, 12 bulk rows at 1M, 12 Hotel rows at 2M and 12 null-usage rows at 500k, all in one area:

```
fact rows: 48
gold_area_median : [('Dubai Marina', 12, 100000.0)]
AggAreaRentStats : [('Dubai Marina', 48, 750000.0)]
AggMetroPremium  : [('M1', 48, 750000.0)]
AggMonthlyRegs   : [(None, 48, 43200000.0)]
```

Same file, same area, same metric name, 7.5x apart. `AggMetroPremium` compounds it: both the per-metro median and the city baseline it divides by are computed over unfiltered stock, so `premium_vs_city_pct` is a ratio of two contaminated numbers. `AggProjectRentStats` has the same shape.

`AggAreaRentStats` also publishes `avg_price_per_sqft` and `median_price_per_sqft` straight off `FctContract.price_per_sqft` with no band. The band exists because an unbanded PSF has already shipped a wrong headline once (`psf_band_filter`'s docstring cites `avg_psf 4691` against a 20-500 band), and these columns reopen that path in SQL.

The null-month row in `AggMonthlyRegistrations` is the same class of omission: no filter on a null `contract_registration_date`, so a `NULL` month bucket appears in the trend view.

### Natural-key joins quietly changed the population

The old views filtered a flat fact. The new ones inner-join the dimensions:

```sql
FROM FctContract f
JOIN DimArea a USING (area_name_en)
JOIN DimPropertyType pt USING (ejari_property_type_en, ejari_property_sub_type_en, property_usage_en)
```

`USING` does not match on nulls, and `DimPropertyType` is rebuilt as a plain `SELECT DISTINCT` with no null filter, so any fact row with a null type, sub_type or usage is dropped. A row with a null `property_usage_en` but a valid sub_type was counted by the previous `gold_area_median` (which never referenced usage) and is dropped now — visible as the 12 `nousage` rows absent from the probe above. `DimArea` and `DimMetro` do filter nulls, so the null-area group the old view could emit is gone too.

Both exclusions are probably wanted, but neither is stated anywhere: the decision lives in join semantics instead of in a filter, so nothing marks it as intentional or protects it from a later switch to left joins.

### Freshness is the only fail-closed check, and it is skippable

`MAX_REGISTRATION_LAG_DAYS` is the replacement for the deleted `WEEKLY_FRESHNESS_GATE`, and `run_etl_pipeline` now explicitly leans on it: the daily job stays fail-open on a quiet day because "the layers ingest is where a stall becomes a defect". That check opens with:

```python
def _check_freshness(increment: pl.DataFrame, expected: date | None) -> None:
    if expected is None:
        return
```

`expected` comes from `_csv_data_date`, a strict `rent_contracts_(\d{8})\.csv$` match. A renamed asset, a `.csv.gz`, a backfill file with any other name, or a future change to the release asset naming turns the stall detector off silently. Given it is the only place a stalled feed becomes an error, it should refuse to run rather than skip.

### Publishing reports success it cannot verify

`publish_layers` raises on a missing artifact or a missing tag/date before any network call. What it then delegates to cannot fail:

```python
def publish(self, files, tag_name=None, name=None, body=None):
    try:
        ...
        self.upload_files(release, files)
    except Exception as e:
        logger.error(f"Failed to publish release: {e}")
```

`upload_files` catches per file as well, so a 422, a 403, an expired token or a failed stale-asset delete each produce a log line and nothing else. `publish_layers` then returns the tag and `main()` logs `Published release-2026-10-05`, and the workflow step exits 0. For a data product whose only delivery channel is this upload, a publish failure that looks identical to success is the wrong default — particularly now that one release tag carries the bronze CSV, `etl_status.json` and the cumulative store together.

`release_exists` contributes: it returns `None` for any status other than 200/404, which is falsy, so a 403 or 500 routes into `create_release` and the resulting error is swallowed by the same handler.

The `_delete_asset_named` addition is a genuine fix — the upload endpoint 422s on a duplicate name, which is exactly what blocked same-day re-runs — and it benefits every publisher through the shared client.

### `daily_layers.yml` gating

```yaml
- name: Ingest into the cumulative layers DuckDB
  run: |
    CSV=$(ls -t output/rent_contracts_*.csv 2>/dev/null | head -1)
    if [ -z "$CSV" ]; then echo "No CSV to ingest (quiet day) — nothing to do"; exit 0; fi

- name: Publish layers into the day's release
  if: hashFiles('output/rents_layers.duckdb') != ''
```

The two steps don't agree on what "nothing to do" means (confirmed from the workflow definition; not observed on a live run). The earlier refresh step pulls the store from the most recent release within 30 days, so on a quiet day `output/rents_layers.duckdb` exists, `hashFiles` is non-empty, and an unchanged store — up to a month old — is uploaded under today's tag. The release list then implies a freshness the asset doesn't have, which cuts against `_meta.data_through` being the at-a-glance freshness signal.

No `concurrency` group is declared. With `workflow_dispatch` taking a `data_date`, a backfill overlapping the 07:00 cron gives two runs the same download-upsert-publish cycle against one cumulative file, and the loser's rows are gone. A single-writer cumulative store needs a serialization guarantee.

The step also installs dependencies twice through two managers: `pip install -r requirements.txt && pip install lib/ && pip install duckdb pyarrow`, then every command runs under `uv run`, which resolves its own environment from `pyproject.toml` and `uv.lock`. The pip layer is dead weight, and only the uv path is reproducible.

### The quality gate doesn't pass on the branch that adds it

ADR-11 lists `make check` — "ruff clean, coverage ≥ floor, the full test suite passes" — as its verification. Coverage and tests hold up: 135 passed, 79% against the 75 floor. Lint does not.

```
.looker-skills/skills/looker-architect/looker_scaffold.py:186:25: F541 f-string without any placeholders
.looker-skills/skills/looker-architect/looker_scaffold.py:485:15: F541
.looker-skills/skills/looker-architect/looker_scaffold.py:487:15: F541
Found 3 errors.
1 file would be reformatted, 130 files already formatted
```

`[tool.ruff] exclude` covers `output`, `rents_scraper`, `.venv`, `build` — not `.looker-skills`, which this diff adds. CI stays green only incidentally: `actions/checkout@v3` runs without `submodules: true`, so the directory is empty on the runner. A gate that passes in CI and fails on every developer machine with the submodule populated gets switched off.

That submodule has a second problem. It is committed as a bare gitlink with no `.gitmodules`:

```
$ git submodule status
fatal: no submodule mapping found in .gitmodules for path '.looker-skills'
$ git ls-tree HEAD .looker-skills .gitmodules
160000 commit 5fe764896862c4131368a353fcc9f59969416601	.looker-skills
```

A fresh clone gets an empty, unpopulatable directory, and the six new `.agents/skills/*` symlinks that point into `../../.looker-skills/skills/...` all dangle. Nothing in the pipeline depends on them, so this won't break a build — it will just never work for anyone else.

On pinning: `requirements-dev.txt` carries bare `ruff`, and `make lint` runs `ruff format --check`. Formatter output changes across releases, so CI will fail on a ruff upgrade with no code change — the exact drift class ADR-11 was written to close. The committed `uv.lock` doesn't cover it either, since `build_and_deploy.yml` installs with pip from the unpinned requirements files. `duckdb` is also unpinned while the published `.duckdb` file is a product consumers must open, which couples artifact readability to whatever version CI resolved.

### PSF now has one owner, with one gap

`PSF_MIN_AREA_SQFT` and `psf_expression` live in `lib.config`; `market_analytics` dropped its second division and its private `200`, and the long "three PSF sites, do not call them unable to disagree" comments are gone because the claim is now true for the floor.

The ceiling didn't come along. Silver's branch order is `actual_area > max_property_size` before the floor check, so a 60,000 sqft unit gets `actual_area_above_max` and a null `rent_per_sqft`; `psf_expression` tests only `>= PSF_MIN_AREA_SQFT` and divides. `docs/superpowers/plans/2026-09-27-silver-contract.md` measures this as live — on 20260917, "446 clear the 200 floor but 3 exceed the 50000 max". Those rows now carry a `price_per_sqft` into `FctContract`, where the unbanded `AggAreaRentStats` averages reach them.

### Secret redaction covers the log line it was written for

`_redact_url` returns `<configured EJARI_URL endpoint>` for unparseable input rather than falling back to the original, and the test asserts the token is absent from `caplog` at the `download_rents` call site rather than only from the helper.

Two paths still carry the raw value. The Scrapy invocation passes it as an argument:

```python
cmd = [sys.executable, "-m", "scrapy", "crawl", "rents", "-o", out, "-a", f"url={url}", ...]
```

which puts a query-string token in the child process's argv, readable by any process listing on the runner (confirmed). And on spider failure the branch logs `result.stderr[:800]`; Scrapy writes request lines to stderr, so a `POST https://...?token=...` can land in the log the redaction was added to protect (likely — it depends on the spider's log level and on which 800 bytes are captured). The test stubs the downloader and routes through the `example.com` seam, so neither path is exercised.

`EJARI_URL` and `GH_TOKEN` are gone from `build_and_deploy.yml`'s job env, which is the stronger half of this work: a push build can no longer extract or publish, and the ETL-clobbers-the-day's-release race the comment describes is closed structurally rather than by convention.

### Test coverage

Covered: the raw-schema contract (column coverage plus overrides beating inference past the 100-row window), ingest idempotency and cross-day overlap, all seven views resolving on a plain connection, the stale-increment refusal, the placeholder CSV, the `SELECT * FROM _meta` TIMESTAMPTZ regression, and the no-release-on-missing-artifact guard. Each new test traces back to a recorded production failure. Suite: 135 passing, `lib` coverage 79%.

Not tested: the production key path — every fixture supplies a non-null `CONTRACT_NUMBER`, so the `RN` fallback that governs all real data has no coverage, which is why the store's failure to accumulate passes CI. Nothing asserts which rows the `Agg*` views include, so their divergence from `gold_area_median` is invisible to the suite. The dimension-join null-drop has no test. `publish_layers` is never tested against an upload failure, so the fail-open behavior is unpinned. `_check_freshness` with an unresolvable data date is uncovered. The redaction tests exercise only the stubbed path, not the Scrapy argv or stderr paths. And `--cov=lib` excludes `run_etl_pipeline.py`, the orchestration layer that owns the publish decisions and the no-data branch, from the floor entirely; within `lib`, `market_analytics.py` sits at 24%.

### Unrelated changes bundled in

Three strands ship together with the layers work: the pipeline change itself (store, schema pinning, publish path, redaction, CI gate), a full documentation set (five ADRs plus ARCHITECTURE, DATA_DICTIONARY, OPERATIONS, ANALYST_COOKBOOK, docs/README), and an agent-tooling bundle with no relationship to either (`.looker-skills` with its six Looker/LookML skill symlinks, `.commandcode/` agents, skills, settings and taste files). The Looker material is what breaks the lint gate and what carries the orphaned gitlink, and it would be easier to evaluate on its own.

The documentation is handled with care worth noting: ADR-0007 and ADR-0008 carry explicit supersession banners pointing at ADR-10, and the silver-contract spec warns up front that some of its code pointers name deleted files. One live pointer did go stale — the comment in `requirements.txt` justifying `pyarrow` cites `build_weekly_duckdb.py:112`, deleted here. The dependency is still load-bearing (`increment.to_arrow()` in `build_layers_duckdb`), so only the reference needs updating.

</details>

<details>
<summary>File map</summary>

**Layers store (new)**
- `lib/analysis/build_layers_duckdb.py` — daily upsert into the cumulative store: read, canonicalise, enrich, freshness-check, `INSERT OR REPLACE`, rebuild dims and views, write `_meta`
- `lib/analysis/silver_layer.py` — `FctContract` DDL, natural-key dimension rebuilds, the idempotent insert, `_meta` SQL
- `lib/analysis/gold_layer.py` — the seven Gold views
- `lib/analysis/layers.py` — read-only connection helper
- `lib/workspace/publish_layers.py` — publishes the DuckDB into `release-<data_through>`
- `lib/analysis/build_weekly_duckdb.py` — deleted
- `lib/analysis/metro_volume.py` — reads the layers store; `week` → `data_through`
- `lib/analysis/gold_indexes.py` — docstring pointers retargeted to `gold_layer.py`

**Schema and shared rules**
- `lib/config.py` — `RAW_RENTS_CSV_DTYPES` / `RAW_RENTS_CSV_COLUMNS` (44 columns), `PSF_MIN_AREA_SQFT`, `psf_expression`
- `lib/transform/rents_transformer.py` — uses the shared raw schema
- `lib/transform/enrichment.py` — PSF via `psf_expression`
- `lib/classes/market_analytics.py` — second PSF division removed
- `lib/classes/silver_contract.py` — imports the shared floor
- `lib/classes/property_usage.py`, `lib/classes/validators.py` — formatting only

**Pipeline and publishing**
- `run_etl_pipeline.py` — `_redact_url`, `_data_rows`, `_write_run_status`, no-data marker release, explicit `data_date` tagging
- `lib/workspace/github_client.py` — same-name asset delete before upload; optional release name/body

**CI and tooling**
- `.github/workflows/daily_layers.yml` — new; `.github/workflows/weekly.yml` — deleted
- `.github/workflows/build_and_deploy.yml` — build/test only, plus lint and coverage; ETL secrets removed
- `Makefile` — `layers`, `layers-publish`, `lint`, `coverage`, `check`
- `pyproject.toml`, `requirements.txt`, `requirements-dev.txt`, `uv.lock`, `.gitignore`, `conftest.py`

**Tests**
- New: `test_build_layers_duckdb.py`, `test_raw_schema.py`, `test_logging_redaction.py`, `test_daily_run_status.py`, `test_publish_layers.py`
- Updated: `test_etl_pipeline.py`, `test_metro_volume.py`, `test_p0_gates.py`, `test_silver_contract.py`, `test_gold_indexes.py`

**Docs** — `docs/ARCHITECTURE.md`, `DATA_DICTIONARY.md`, `OPERATIONS.md`, `ANALYST_COOKBOOK.md`, `docs/README.md`, `docs/adr/0007`-`0011` + `adr/README.md`, `CHANGELOG.md`, `RELEASE_NOTES.md`, `README.md`, `CONTRIBUTING.md`, `LIBRARY_USAGE_GUIDE.md`

**Agent tooling (unrelated)** — `.looker-skills` gitlink, `.agents/skills/*` symlinks, `.commandcode/{agents,skills,taste,settings.json}`

Full diff: `git diff main...HEAD` (87 files, +12406/-1526; `uv.lock` is 6726 of the added lines)

</details>

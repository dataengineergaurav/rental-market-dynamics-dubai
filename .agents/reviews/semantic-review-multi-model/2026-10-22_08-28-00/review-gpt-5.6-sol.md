# Daily cumulative Bronze, Silver, and Gold release pipeline

## Summary

This branch replaces weekly analytics snapshots with a daily cumulative `rents_layers.duckdb`, colocated with the raw CSV and run marker in each date-scoped GitHub release. It also pins the Ejari schema, centralizes PSF rules, adds freshness and CI gates, and documents the resulting data product. The cumulative path is not safe to ship: the production feed's missing contract number makes `RN` the effective primary key, release failures become valid-looking quiet days or cold starts, and several quality gates fail open. The 135-test suite passes, but production-shaped probes reproduce data loss, freshness bypass, incorrect bulk classification, and an invalid standard-lease median.

Watch for: **confirmed** cumulative row replacement through the `RN` fallback; **confirmed** stale or truncated releases after suppressed GitHub failures; **confirmed** acceptance of increments with no parseable registration date; **confirmed** release mutation before tests; **confirmed** credential exposure beyond the intended steps and log line.

**Verdict**: NEEDS_CHANGES

## High-level view

The intended topology is a two-job handoff: Bronze publishes a raw CSV plus `etl_status.json`; the layers job restores the latest DuckDB, upserts the increment, rebuilds dimensions and Gold views, and adds the cumulative file to the same release. Keeping tables and views together removes the former `ATTACH` contract and gives the monthly marts historical input.

**Confirmed:** the idempotency key cannot preserve that history. `CONTRACT_NUMBER` is 100% null in observed files, so `contract_id` falls back to response ordinal `RN`; repeated or renumbered ordinals overwrite unrelated earlier registrations.

**Confirmed:** release failures are interpreted as data states. Restore and Bronze-download errors can look like an intentional cold start or quiet day, while the publisher catches upload failures and still reports success.

**Confirmed:** the trust boundary and metric contracts also fail open. Missing registration dates bypass freshness, batch-relative labels are frozen into cumulative facts, and `gold_standard_lease` includes non-positive rents while counting null rents in `n`.

**Confirmed:** security and reproducibility controls stop short of their advertised boundaries. A timeout can log the raw endpoint, the release token is job-wide, CI does not enforce `uv.lock`, and the Looker skill gitlink cannot be materialized from a clean clone.

<details>
<summary>Issues (13)</summary>

1. **Unstable cumulative identity** — **confirmed**: production rows fall back from null `CONTRACT_NUMBER` to fetch-local `RN`, so later days replace unrelated history. Define and test a stable cross-fetch key before using `INSERT OR REPLACE`.
2. **Suppressed release downloads** — **confirmed**: all prior-DB and Bronze download errors become cold-start or quiet-day paths. Distinguish absent assets from API/auth/network failures, validate `etl_status.json`, and require an explicit bootstrap mode.
3. **Destructive publisher reports success** — **confirmed**: refresh deletes the good asset first, then swallows upload failures and returns the tag. Propagate failures and stage/verify replacements before removing the previous asset.
4. **Publication precedes validation** — **confirmed**: the layers workflow mutates the release before running tests. Run tests and artifact checks before any release write.
5. **Best-effort status marker** — **confirmed**: marker write or publication can fail while the ETL still succeeds, recreating the ambiguity the marker was added to remove. Require marker creation/publication in CI.
6. **Null-date freshness bypass** — **confirmed**: an increment with no parseable registration date passes `_check_freshness`. Reject missing/all-null date evidence before opening the database.
7. **Batch-scoped cumulative labels** — **confirmed** for bulk and **likely** for luxury semantics: daily-only classification is persisted as if it described the cumulative population. Recompute over the fact or record and document the cohort.
8. **Invalid standard-lease population** — **confirmed**: `gold_standard_lease` admits zero/negative rent and counts null rent in `n`. Apply the documented positive/non-null rent contract and test the aggregate values.
9. **EJARI timeout secret leak** — **confirmed**: the raw URL is in the subprocess command and the timeout exception logs that command. Log a fixed message and sanitize all subprocess output.
10. **Job-wide release credential** — **confirmed**: `GH_TOKEN` is available to dependency installation, ingestion, smoke queries, and tests. Scope read and write credentials to the minimum steps and permissions.
11. **Lockfile not enforced** — **confirmed**: the push gate installs unpinned requirement files instead of using `uv.lock`. Run `uv sync --locked --all-groups` and execute the gate in that environment.
12. **Unconfigured Looker gitlink** — **confirmed**: `.looker-skills` has no `.gitmodules` mapping, leaving six skill links unresolved in a clean clone and polluting local lint when manually populated. Configure or vendor it and define the Ruff boundary.
13. **Production failure paths untested** — **confirmed**: tests omit null contract numbers, missing dates, release failures, cross-increment bulk groups, invalid standard rents, and timeout leakage. Add regression coverage for each shipping contract.

</details>

<details>
<summary>Details</summary>

### One dated release becomes the Bronze-to-Gold handoff

A release is now both the ingestion boundary and distribution unit. Bronze publishes the date's response and marker; the layers job recovers state, applies the increment, and publishes a point-in-time DuckDB containing Silver tables and Gold views.

```text
Ejari endpoint
     |
     v
release-YYYY-MM-DD
  |- rent_contracts_YYYYMMDD.csv
  |- etl_status.json
  `- rents_layers.duckdb  <--- prior snapshot + today's upsert + rebuilt views
```

This makes identity, restore behavior, and publication atomicity part of the data contract rather than operational details.

### `RN` cannot key a cumulative store

**Confirmed:** `_prepare_increment` coalesces null `CONTRACT_NUMBER` to `RN` (`lib/analysis/build_layers_duckdb.py:171-175`), then deduplicates and upserts on that key (`:194-195`; `lib/analysis/silver_layer.py:65-95,165-169`). The branch also documents that `CONTRACT_NUMBER` is 100% null (`lib/classes/silver_contract.py:59`; `docs/DATA_DICTIONARY.md:54`) and `RN` is a response ordinal that can renumber, with cross-day use required to key on `row_hash` (`lib/classes/silver_contract.py:146-157,359-365`). ADR-10's stable-business-key premise therefore contradicts the measured feed contract.

A production-shaped probe ingested two dates with null contract numbers and `RN=1`; after the second ingest, `FctContract` contained only `Day Two`.

```text
2026-09-14: CONTRACT_NUMBER=null, RN=1 -> contract_id="1" -> Day One
2026-09-15: CONTRACT_NUMBER=null, RN=1 -> contract_id="1" -> Day Two
                                                   INSERT OR REPLACE
Result: Day One is lost
```

The existing `row_hash` is stable but intentionally non-unique, while `record_id` is fetch-local. A replacement needs an explicit compound/provenance key, collision policy, and correction semantics rather than `RN` alone.

### GitHub failures masquerade as quiet days and cold starts

**Confirmed:** the prior-store loop suppresses every `gh release download` error and treats 30 failures as “cold start” (`.github/workflows/daily_layers.yml:49-56`). After the first successful deployment, a restore outage followed by a successful CSV download creates a one-day database and publishes it as the newest cumulative snapshot.

**Confirmed:** today's Bronze download converts every nonzero result to “quiet day” and never verifies `etl_status.json` (`daily_layers.yml:59-69`). A restored database is then republished unchanged under today's tag; without a restored database, no layers artifact is produced. Missing CSV should count as quiet only after a validated marker says `outcome="no_data"`; auth, API, and transport failures must stop the job, and bootstrap should be explicit.

### Asset replacement deletes first and still reports success

**Confirmed:** refresh deletes an existing asset before upload (`lib/workspace/github_client.py:72-88`), while `upload_files()` and `publish()` catch failures without re-raising (`:91-108,110-125`). `publish_layers()` then returns the tag unconditionally (`lib/workspace/publish_layers.py:47-48`). A forced upload exception still returned `release-2026-10-05`; during refresh, the previous good asset can already be gone.

**Confirmed:** `daily_layers.yml:83-93` publishes before running tests, so a failing gate cannot protect the public artifact. Tests and artifact verification must precede release mutation, and replacement needs staged upload/verification with propagated errors.

**Confirmed:** `_write_run_status()` downgrades filesystem failures to warnings (`run_etl_pipeline.py:88-108`), while an empty publication list is a non-fatal return (`:273-277`). CI can therefore succeed without the marker intended to make absence unambiguous.

### `_check_freshness` accepts an increment with no usable date

**Confirmed:** the reader ignores parse errors, date parsing is non-strict, and `_check_freshness()` returns when the maximum date is `None` (`lib/analysis/build_layers_duckdb.py:154-161,199-205`). A CSV with rows but blank registration dates was inserted with null `contract_registration_date`; `_meta.data_through` remained empty. When an expected date exists, require parseable date evidence and reject before opening or mutating the DuckDB.

### Daily enrichment freezes batch-relative labels into cumulative facts

**Confirmed:** enrichment computes luxury percentiles and bulk counts from the incoming frame (`lib/transform/enrichment.py:186-205,225-234`), and the cumulative builder persists those booleans without recomputing over `FctContract` (`lib/analysis/build_layers_duckdb.py:191`). Six identical area/amount rows on each of two days produced 12 cumulative rows with zero bulk flags, although the published rule says a group over 10 is bulk and `gold_area_median` relies on that exclusion.

**Likely:** `is_luxury` has the same cohort ambiguity; daily percentile flags are exposed together without their cohorts. Recompute population-relative labels after upsert, or make cohort scope explicit in schema and documentation.

### `gold_standard_lease` admits rents excluded by its contract

**Confirmed:** unlike `gold_area_median`, `gold_standard_lease` lacks a positive/non-null annual-rent condition (`lib/analysis/gold_layer.py:17-50`), conflicting with the documented Gold contract (`docs/DATA_DICTIONARY.md:154-166`). One AED 80,000 lease plus one AED -1,000 lease returned `n=2`, `median_rent=39,500`, and `mean_rent=39,500`; null rents would inflate `n` while median ignores them. Apply the same money predicate and pin both `n` and the aggregates with invalid-rent fixtures.

### Endpoint and release credentials cross unintended boundaries

**Confirmed:** `_redact_url()` protects the initial log, but the raw URL remains in the Scrapy command (`run_etl_pipeline.py:124,146`), and the timeout handler logs the `TimeoutExpired` object containing that command (`:154-157`). **Likely:** captured Scrapy stderr can carry the same endpoint and is logged raw (`:181-182`). Use a fixed timeout message and sanitize or suppress all subprocess output.

**Confirmed:** job-scoped `GH_TOKEN` (`.github/workflows/daily_layers.yml:17-19`) is available during dependency installation, ingestion, smoke queries, and tests. Use a read-only credential for restore and a narrowly scoped write credential only for publication. ADR-11 deliberately defers Action-version and debug-setting changes, so those are not findings.

### `uv.lock` and Looker skills are not reproducible from a clean clone

**Confirmed:** ADR-11 calls `uv.lock` the source of truth, but the push workflow uses `make build` and unpinned `requirements.txt`/`requirements-dev.txt`; it never runs `uv sync --locked` (`.github/workflows/build_and_deploy.yml:29-43`; `Makefile:40-45`). The lock does not constrain the primary CI gate.

**Confirmed:** `.looker-skills` is a gitlink with no `.gitmodules` mapping, while six `.agents/skills/*` entries point into it. `git submodule status` fails, so clean clones cannot materialize the skills. In this populated checkout, `ruff check .` also fails on three upstream F541 findings; repository-owned source passes when `.looker-skills` is excluded. Configure or vendor the dependency and encode the lint boundary.

### Green fixtures miss the production contracts

**Confirmed:** all 135 tests pass and coverage is 78.97%, above the 75% floor. Ruff and format checks pass for repository-owned source when the populated Looker gitlink is excluded; the declared literal lint path fails in this checkout because that boundary is absent.

Not tested: **confirmed** null `CONTRACT_NUMBER` with repeated `RN`; **confirmed** all-null/unparseable registration dates; **confirmed** release download and replacement failures; **confirmed** cross-increment bulk groups; **confirmed** invalid rents in `gold_standard_lease`; **confirmed** timeout redaction. `tests/test_build_layers_duckdb.py:55-58` always supplies synthetic contract numbers, so its growth assertion covers a key the observed feed does not provide.

</details>

<details>
<summary>File map</summary>

- `.agents/skills/looker-architect` — adds a symlink to the external Looker architect skill.
- `.agents/skills/looker-performance-optimizer` — adds a symlink to the external Looker performance skill.
- `.agents/skills/lookml-explore` — adds a symlink to the external Explore skill.
- `.agents/skills/lookml-fields` — adds a symlink to the external fields skill.
- `.agents/skills/lookml-tests` — adds a symlink to the external LookML test skill.
- `.agents/skills/lookml-view` — adds a symlink to the external view skill.
- `.commandcode/agents/data-quality-reviewer.md` — adds a read-only data-quality review agent.
- `.commandcode/agents/market-analyst.md` — adds a market-analysis agent contract.
- `.commandcode/agents/pipeline-triage.md` — adds pipeline failure triage guidance.
- `.commandcode/agents/release-verifier.md` — adds release artifact verification guidance.
- `.commandcode/settings.json` — configures Command Code project behavior.
- `.commandcode/skills/ci-triage/SKILL.md` — adds the CI triage workflow.
- `.commandcode/skills/ci-triage/references/known-failures.md` — catalogs known pipeline failure signatures.
- `.commandcode/skills/data-engineer-expert/SKILL.md` — adds a data-engineering review skill.
- `.commandcode/skills/data-engineer-expert/references/audit-checklist.md` — provides its audit checklist.
- `.commandcode/skills/data-engineer-expert/references/best-practices.md` — provides its engineering practices.
- `.commandcode/skills/dubai-real-estate-investor/SKILL.md` — adds investor-oriented analysis guidance.
- `.commandcode/skills/dubai-real-estate-investor/references/underwriting-checklist.md` — adds an underwriting checklist.
- `.commandcode/skills/uae-market-context/SKILL.md` — adds UAE market-domain guidance.
- `.commandcode/skills/uae-market-context/references/metric-contracts.md` — records analytics metric contracts.
- `.commandcode/skills/uae-market-context/references/uae-rental-market.md` — records UAE rental-market context.
- `.commandcode/skills/uv/SKILL.md` — adds uv environment-management guidance.
- `.commandcode/skills/uv/references/troubleshooting.md` — adds uv troubleshooting notes.
- `.commandcode/taste/taste.md` — adds a taste/configuration pointer.
- `.commandcode/taste/taste/taste.md` — adds nested taste preferences.
- `.github/workflows/build_and_deploy.yml` — turns push automation into build/test/lint/coverage only.
- `.github/workflows/daily_layers.yml` — adds daily cumulative restore, ingest, smoke, publish, and test automation.
- `.github/workflows/weekly.yml` — removes the weekly artifact workflow.
- `.gitignore` — allows lockfiles and project skill files to be tracked.
- `.looker-skills` — adds an external-repository gitlink without submodule metadata.
- `CHANGELOG.md` — records the pipeline, release, and quality-gate changes.
- `CONTRIBUTING.md` — updates development and validation instructions.
- `Makefile` — adds layer, publish, lint, coverage, and aggregate check targets.
- `README.md` — documents the cumulative daily product and consumer API.
- `RELEASE_NOTES.md` — replaces weekly release notes with the cumulative release model.
- `conftest.py` — makes root-level test imports deterministic.
- `docs/ANALYST_COOKBOOK.md` — adds query recipes for the layers artifact.
- `docs/ARCHITECTURE.md` — documents the two-job cumulative architecture.
- `docs/DATA_DICTIONARY.md` — defines Bronze, Silver, Gold, and derived-field contracts.
- `docs/LIBRARY_USAGE_GUIDE.md` — updates examples for the new APIs and artifacts.
- `docs/OPERATIONS.md` — adds schedules, runbooks, recovery, and release verification.
- `docs/README.md` — indexes the documentation set.
- `docs/adr/0007-weekly-cross-file-dedup-and-freshness-gate.md` — records the superseded weekly design.
- `docs/adr/0008-layered-bronze-silver-gold-releases.md` — records the superseded split-layer design.
- `docs/adr/0009-release-hardening-raw-schema-and-markers.md` — records schema pinning and status markers.
- `docs/adr/0010-cumulative-combined-layers-duckdb.md` — records the daily cumulative decision.
- `docs/adr/0011-reproducibility-and-ci-quality-gates.md` — records lockfile and quality-gate decisions.
- `docs/adr/README.md` — indexes architectural decisions.
- `docs/superpowers/plans/2026-09-27-silver-contract.md` — updates the Silver implementation plan.
- `docs/superpowers/specs/2026-09-27-silver-contract-design.md` — updates the Silver design specification.
- `lib/__init__.py` — updates package metadata/exports.
- `lib/analysis/build_layers_duckdb.py` — adds the cumulative ingest builder.
- `lib/analysis/build_weekly_duckdb.py` — removes the weekly snapshot builder.
- `lib/analysis/gold_indexes.py` — centralizes bulk and sample-size rule ownership.
- `lib/analysis/gold_layer.py` — adds seven Gold views over the cumulative fact.
- `lib/analysis/layers.py` — adds the plain combined-artifact connection helper.
- `lib/analysis/metro_volume.py` — reads metro output from the combined layers file.
- `lib/analysis/silver_layer.py` — defines cumulative fact/dimension DDL and upsert SQL.
- `lib/classes/__init__.py` — removes a stale class export.
- `lib/classes/market_analytics.py` — shares PSF computation and reportability rules.
- `lib/classes/property_usage.py` — reads Silver PSF and excludes bulk rows.
- `lib/classes/silver_contract.py` — hardens validation, hashing, grouping, and quarantine behavior.
- `lib/classes/validators.py` — narrows aggregate validation ownership.
- `lib/config.py` — pins the raw schema and centralizes PSF configuration.
- `lib/extract/ejari_rents_downloader.py` — formats and adjusts incremental downloader behavior.
- `lib/logging_helpers.py` — prevents duplicate root handlers.
- `lib/setup.py` — updates package setup metadata.
- `lib/transform/enrichment.py` — centralizes PSF and bulk-rule ownership.
- `lib/transform/rents_transformer.py` — uses the pinned raw schema and strict UTF-8.
- `lib/workspace/__init__.py` — adjusts workspace exports.
- `lib/workspace/github_client.py` — reuses releases and replaces same-name assets.
- `lib/workspace/publish_layers.py` — adds the cumulative layer publisher CLI.
- `pyproject.toml` — adds runtime/dev dependencies and Ruff configuration.
- `requirements-dev.txt` — adds Ruff and coverage tools.
- `requirements.txt` — constrains Polars and adds PyArrow.
- `run_etl_pipeline.py` — adds status markers, URL redaction, explicit dates, and daily release contents.
- `tests/test_build_layers_duckdb.py` — tests cumulative ingest, views, freshness, and idempotency.
- `tests/test_daily_run_status.py` — tests no-data row counting and status markers.
- `tests/test_etl_pipeline.py` — expands daily pipeline, marker, and Silver-gate coverage.
- `tests/test_gold_indexes.py` — expands Gold filtering and market-health invariants.
- `tests/test_logging_redaction.py` — tests the direct download-log redaction helper.
- `tests/test_metro_volume.py` — updates metro queries for the combined artifact.
- `tests/test_p0_gates.py` — expands shared PSF and bulk-rule tests.
- `tests/test_publish_layers.py` — tests layer tagging and successful publication.
- `tests/test_raw_schema.py` — tests full raw-payload schema pinning.
- `tests/test_silver_contract.py` — expands Silver validation, identity, and grouping tests.
- `uv.lock` — commits the resolved Python dependency graph.

Full diff: `git diff main...HEAD`

</details>

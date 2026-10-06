# ADR-09: Release hardening — pin the raw schema, mark no-data days, one publish path

**Status:** Accepted · **Date:** 2026-10-06

## Context

Three gaps surfaced while reading the release history (315 releases, 313 daily:

- **Coverage is unreadable.** The daily job is fail-open (ADR-03): a quiet day publishes nothing, and
  `etl_status.json` is local-only. So the release list cannot distinguish a *no-data* day from a
  **stalled feed or a broken run** — 277 of 590 calendar days have no release, and a five-week hole
  (2026-08-06 → 09-12) is indistinguishable from a quiet stretch.
- **The raw schema was only partly pinned.** `RAW_RENTS_CSV_DTYPES` covered 6 of the 44 payload
  columns. Both recent production failures were the same class — an unpinned column flipping type
  under per-file inference (PARKING → String, `CONTRACT_AMOUNT` → Int64 vs Float64). Patching the
  columns that bit leaves the hole open for the next one.
- **Two publish paths.** Daily used the Python `GitHubRelease` client; weekly used `gh` in
  `weekly.yml` *and* again in the Makefile. They had already drifted (no-clobber vs clobber; a
  `$GITHUB_ENV` tag bug that released the wrong week).

## Decision

1. **Mark every run.** The daily pipeline publishes `etl_status.json` to `release-YYYY-MM-DD` on
   **both** outcomes — alongside the CSV on a data day, alone on a quiet day. The tag comes from an
   explicit `data_date`, since a marker has no CSV filename to derive it from.
2. **Pin the whole raw payload.** `RAW_RENTS_CSV_DTYPES` lists all 44 columns, and
   `RAW_RENTS_CSV_COLUMNS` records the payload header. Both raw readers use the one schema.
3. **One publish client.** Weekly Silver/Gold publication goes through
   `lib.workspace.publish_layers` over `GitHubRelease` (which now accepts a release name/body and
   clobbers existing assets). The workflow and Makefile call its CLI; no `gh` release logic remains.

## Rationale

- **Absence must be unambiguous.** With a marker, the release list answers "did it run?" and "was
  there data?" separately. That is the cheapest possible fix for an observability gap, and it needs
  no external state.
- **Pin at the boundary with the feed.** The payload is untyped text; the only place its types are
  knowable is where it is read. One schema there beats N defensive casts downstream. The Arabic and
  fully-null columns are pinned too, because "this column is text" is a fact worth recording even
  when every value is null.
- **Duplicated release logic is a bug factory.** The two paths diverged within days. Funnelling both
  through one client means clobber semantics, tag derivation and error handling are fixed once.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| A single rolling `release-index` asset instead of per-day markers | Read-modify-write across runs; racy and stateful. Per-day markers ride the existing tag model |
| Keep `etl_status.json` local, rely on CI logs | Logs expire and are not queryable; the release list is the durable public surface |
| Pin only the columns that have failed | The demonstrated failure mode is "the unpinned column", not a fixed list of names |
| A runtime payload contract test (fetch a real CSV) | Needs network in CI; the schema-plus-fixture test catches typos and drift offline |
| Keep `gh` for weekly, share only the daily client | The drift was *between* the two paths; sharing is the point |

## Consequences

- `release-YYYY-MM-DD` may now contain no CSV (quiet day). The weekly rehydrate downloads
  `--pattern "rent_contracts_*.csv"`, so markers do not disturb it.
- `publish_artifacts_to_github` gained a `data_date` parameter; the no-data branch now publishes.
- `RAW_RENTS_CSV_DTYPES` is larger but mechanical; adding a payload column without pinning it fails
  `tests/test_raw_schema.py`.
- `make weekly-publish` and `weekly.yml` now run `python -m lib.workspace.publish_layers` and require
  a GitHub token with `repo` scope (as before).

## Verification

- `tests/test_raw_schema.py` — the schema covers all 44 columns, the overrides beat inference on a
  numeric-looking text column, and a 120-row null prefix cannot change a pinned type.
- `tests/test_publish_layers.py` — each layer goes to its own tag; a missing artifact fails before
  any release is created.
- `tests/test_etl_pipeline.py` — the no-data run publishes the marker under the data date; the marker
  tag is the data date, not today.
- Real daily transform of `rent_contracts_20261003.csv` still produces `has_parking: Boolean` with
  the full schema.

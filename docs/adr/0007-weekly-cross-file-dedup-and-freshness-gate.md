# ADR-07: Cross-file dedup and a freshness gate in the weekly build

**Status:** Accepted · **Date:** 2026-10-05

## Context

The daily extract asks the gateway for a 2-day window (`run_etl_pipeline.py`, `_incremental_window`)
because a single-day range returns zero rows. Two consecutive daily files can therefore contain the
same registration. The weekly builder pooled every daily CSV with a bare `pl.concat` and wrote the
result straight into `fact_rental_contract`, so those boundary-day rows double-counted into every
Gold median and count.

The Silver contract already computes a `row_hash` explicitly documented as "the only key safe to use
across days" (`lib/classes/silver_contract.py`, `_row_hash`), but nothing at the weekly layer
consumed it.

Separately, the daily job is deliberately fail-open (ADR-03): an empty incremental window is not a
failure. The downside is that a stalled feed — several days of empty or missing daily files —
produces a green pipeline. A weekly artifact that claims to cover a Mon–Sun range but was pooled
from three of those files looked identical to a healthy one.

## Decision

Two guarantees in `lib/analysis/build_weekly_duckdb.py`, both enforced before enrichment:

1. **Cross-file dedup.** Attach Silver's `row_hash` to every pooled row and drop any row whose hash
   was first seen in an earlier daily file. Within-file repeats are preserved.
2. **Freshness gate.** Raise — do not publish — when a day in the requested window has no usable
   daily CSV, or when the newest `contract_registration_date` in the pool trails the window end by
   more than `WEEKLY_FRESHNESS_GATE["max_contract_date_lag_days"]` (one day, to absorb a
   filename/data-date off-by-one). Thresholds live in `lib/config.py`.

`_meta` now records `pooled_rows`, `deduped_rows`, `row_hash_duplicates_removed`, `daily_files`,
`expected_daily_files`, `missing_daily_files` and `data_through`.

## Rationale

- **Dedup must be cross-file, not `unique(row_hash)`.** Measured on the pooled 20260913–17 files:
  16,075 rows carry only 12,707 distinct hashes, but only 55 hashes (139 rows) span more than one
  file — the rest are within-file repeats. Those repeats are the Naif-style bulk registrations
  (79×1,540,471) that enrichment's `is_bulk_registration` flag and the Gold exclusions are built
  around. Collapsing them would delete roughly 3,229 real rows and break bulk detection. Only the
  cross-file overlap is the defect. Dedup removes 71 rows on that window and changes 22 of 121 Gold
  areas' counts and/or medians.
- **Reuse, do not restate, the hash.** `add_row_hash` calls Silver's `_row_hash` so the fingerprint
  stays byte-identical; a second implementation would drift and let rows Silver deduplicates survive.
- **Gate at the aggregation boundary.** A quiet day is not a failure, so the daily job stays
  fail-open. A *week* is a claim of coverage, and a week built from missing days is wrong — that is
  where enforcement belongs. The existing artifact for 2026W37 was published from a 7-day window
  with only 5 daily files; the gate now rejects exactly that.
- **Failure is loud.** Publication is `gh release upload`/GitHub API, not a database transaction; a
  stale artifact that is already published cannot be un-published. Raising before the DuckDB is
  written is cheaper than retracting a release.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| `pl.unique(subset=["row_hash"])` on the pool | Drops legitimate within-file bulk repeats (~3,229 rows) |
| Dedup on `record_id` | `record_id` embeds `RN`, the pagination ordinal; a renumbered page changes it across days |
| Enforce freshness in the daily job (exit non-zero on empty) | Reverses ADR-03 and pages on quiet days; a stall is only a defect once a range is claimed |
| Persist a consecutive-no-data counter across CI runs | Needs durable state the runner does not have; the daily set already carries coverage in its filenames |
| Fix file coverage only, ignore `data_through` | A present-but-stale file passes a filename check; the registration date is the actual signal |

## Consequences

- `build_weekly_duckdb` imports `_row_hash`/`_ROW_HASH_FIELDS` from `lib.classes.silver_contract`.
  These are private; the coupling is deliberate and should not be replaced with a copy.
- The weekly build can now fail where it previously published. `weekly.yml` and `make weekly` must
  supply a genuinely complete window; `make weekly` derives the previous ISO week rather than
  hardcoding dates.
- The daily pipeline writes `output/etl_status.json` and logs `NO_NEW_DATA` so an empty window is an
  explicit record rather than a silent success. It still exits 0 (ADR-03).
- A missing daily CSV after a successful download is *not* treated as "no new data": the pipeline
  proceeds and fails, because `download_rents` guarantees a file via `touch()`.

## Verification

- `tests/test_weekly_dedup_and_gate.py` pins cross-file dedup (earliest file wins, within-file
  repeats survive), `row_hash` parity with Silver, `missing_days`, the placeholder-vs-data
  distinction, and both gate rejections.
- Real-data rebuild of 20260913–17: 16,075 → 16,004 rows (71 removed), recorded in `_meta`.

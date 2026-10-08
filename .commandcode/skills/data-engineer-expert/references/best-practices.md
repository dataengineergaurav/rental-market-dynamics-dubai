# Data engineering best practices — in depth

Generic, vendor-neutral guidance a senior data/analytics engineer applies to a batch pipeline.
Pair with `audit-checklist.md` for a fast repo scan.

## 1. Idempotency and determinism

- A pipeline re-run must not duplicate or mutate historical rows. Enforce it with a **primary key
  and an upsert** (`INSERT OR REPLACE`), not with diffing code.
- Determinism: no `now()` in a value that lands in data; no dependence on dict/set ordering or
  filesystem listing order. A "2-day window" that overlaps the next run is fine *because* the key
  makes it idempotent — make that reasoning explicit.
- Separate the **data date** (the event's date) from the **run date** (when the job ran). Name
  artifacts by the data date.

## 2. Contracts and schemas

- Type the boundary once and reuse it. A per-row contract should put every input row in exactly one
  of two disjoint buckets (valid / quarantined) so nothing vanishes silently.
- Prefer **named violations** over generic rejections: collect a `violations` list + counts so an
  operator sees *which rule* fired and *how many* rows.
- Watch schema-inference footguns: Polars infers from the first 100 rows by default
  (`infer_schema_length=None` to scan all); a money column can flip Int64↔Float64 across files.
  Pin dtypes at every `read_csv`.
- Don't drop `NaN`/null rows to make a gate pass — that hides upstream drift.

## 3. Metrics

- State the **grain** ("one row per registration") and aggregate only at it.
- **Median** for headline rent/price; a mean is a bulk-registration and outlier magnet.
- **Every aggregate carries `n`.** A median without its sample size is not publishable.
- Put the **exclusions in one place** and document them (sub-types, virtual units, bulk clusters).
- Expose spread (`p10`/`p90`), not just central tendency.

## 4. Observability

- **Freshness watermark** readable from the store (`_meta.data_through`) and a max-lag trust check
  that *fails* a stalled feed (a quiet day is fine; a stopped feed is not).
- **Run status per run**: outcome, window, row count, timestamp — machine-readable.
- Structured, greppable logs with stable tokens (`NO_NEW_DATA`), so alerts and humans can both key on
  them.
- Make **fail-open vs fail-loud** an explicit decision per stage, written down (an ADR). Fail-open is
  fine for "no new data"; it is a bug for "contract module failed to import".

## 5. Testing

- **The pyramid for data:** data-quality gates → contract tests → unit tests → integration tests.
- Assert **schema and dtype**, not just values (`DataFrame.equals()` compares values, not dtypes).
- Test **the trap**, not the happy path: the value at row 101, the empty/placeholder file, the
  boundary area, the timezone edge, the sub-threshold sample.
- Use **realistic fixtures** and state why the load-bearing row exists in a docstring — so a future
  edit that guts the test is caught.
- A green suite over a `try/except` that swallows the gate is worse than no suite.

## 6. CI as a quality gate

- Run, on every change: format check, lint, type-check, tests, and a coverage floor. A red gate
  blocks merge.
- **Pin and lock** dependencies. A range (`polars`) is a future incident: an upgrade can move a
  dtype or a default with no test change. Use a lockfile (`uv.lock`/`poetry.lock`) and commit it.
- Keep CI logs signal-dense: turn off `ACTIONS_STEP_DEBUG`/`ACTIONS_RUNNER_DEBUG` outside active
  debugging.
- One job, one responsibility. Never let a push build silently publish data a scheduled job owns.

## 7. Reproducibility

- A single documented command reproduces the environment and the run.
- No hidden global state, no reliance on an operator's local files.
- Version the data format and the transform together.

## 8. Orchestration

- **One writer per artifact.** Two jobs publishing the same tag is a race and a data-integrity risk.
- Document the schedule and **ordering** (the downstream job must run after the upstream artifact
  exists). Prefer event triggers or explicit dependency over hoping the cron offsets never drift.
- Retries must be idempotent; a partial failure must be safe to re-run.

## 9. Security and secrets

- Secrets never in the repo, logs, or published artifacts. Validate presence at startup and fail
  clearly if a required one is missing.
- Scope tokens to least privilege; rotate; prefer short-lived credentials.
- Treat source data as untrusted; validate at the boundary.

## 10. Documentation and lineage

- An ADR per significant decision (context, decision, consequences), superseding rather than
  deleting prior ones.
- README states the contracts (what each layer is, its grain, its guarantees).
- Changelog / release notes per shipped change; docs move in the same PR as the code.

## 11. Performance and cost (deliberately)

- Columnar formats (Parquet), predicate pushdown, streaming for large scans.
- Partition/prune only when a real access pattern demands it.
- Measure before optimizing; a batch job that runs in seconds does not need a distributed engine.

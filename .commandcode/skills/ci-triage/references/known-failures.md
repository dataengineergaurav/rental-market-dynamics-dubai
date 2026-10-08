# Known failure modes — Dubai rents pipeline

Match the first meaningful error line here. "Signature" is a grep-able fragment, not a full quote.

## Quick table

| Signature (grep) | Layer | Category | Severity | Next command |
|------------------|-------|----------|----------|--------------|
| `EJARI_URL environment variable not set` | bronze | secret/config | P0 | set `EJARI_URL` (repo secret + `.env`) |
| `gh: ... authentication` / `could not read Username` | CI | secret/config | P0 | re-auth `gh` / check `GH_TOKEN` secret |
| `Layers ingest refused: newest registration ... trails the data date` | silver | data/feed | P0 | inspect feed; confirm with a backfill `--expected-date` |
| `No usable CSV to ingest` | silver | data/feed | P1 | ensure `rent_contracts_YYYYMMDD.csv` has data rows |
| `ComputeError` from `pl.DataFrame(records)` schema inference | silver | data-dependent flake | P1 | set `infer_schema_length=None` at the construction |
| dtype mismatch on `n` (UInt32 vs Int64) | gold | code defect | P2 | assert `frame.schema`, don't rely on `.equals()` |
| `NameError` importing a `lib.classes` module | silver | code defect | P0 | leading-underscore pydantic field w/ `Field()` |
| Gold view fails to resolve on plain `connect` | gold | code defect | P0 | view reads a missing table/alias — remove `ATTACH` assumptions |
| `RuntimeError` from `subprocess` spider timeout | bronze | data/feed | P2 | usually self-heals via direct-downloader fallback |
| transform/analyze raises after download | silver | code defect | P1 | `make test`; inspect traceback |
| ETL runs on a `push` event | CI | code defect (regression) | P0 | push must only `make build && make test` |

## Detail

### Missing `EJARI_URL`
`run_etl_pipeline.main()` logs `EJARI_URL environment variable not set. Please set it in .env file.`
and returns `False`, so `run_etl_pipeline.py` exits `1`. Set the repo secret (used by `cron.yml`)
and/or `.env` locally. `GH_TOKEN` being unset is **not** fatal — publish is skipped.

### Stalled feed (freshness gate)
`lib/analysis/build_layers_duckdb.py::_check_freshness` raises:
`Layers ingest refused: newest registration <d> trails the data date <D> by N days (allowed 1). The
daily feed has stalled.` This is a deliberate trust check, not a bug. Do **not** loosen
`MAX_REGISTRATION_LAG_DAYS` to silence it. Investigate the upstream feed; for a legitimate backfill,
pass `--expected-date YYYYMMDD`.

### `No usable CSV to ingest`
`ingest_layers` raises `FileNotFoundError: No usable CSV to ingest ...`. `_usable()` requires the
file to exist, be ≥100 bytes, and have more than one line. A `touch()`ed placeholder (empty window)
correctly has no data rows — that path should have been skipped upstream as a quiet day, not
ingested.

### polars first-100-rows schema inference (data-dependent flake)
`pl.DataFrame(records)` infers the schema from the first 100 rows only. A column that is null in
rows 1–100 and a string at row 101 (real example: `MASTER_PROJECT_EN` = "Hills Park" in
`output/rent_contracts_20260916.csv`) raises `ComputeError`. It looks like flaky data because four
of five daily files pass. Fix is in code: pass `infer_schema_length=None`.
Pinned by `tests/test_silver_contract.py`.

### dtype trap (`n` is UInt32 vs Int64)
`pl.DataFrame.equals()` compares values, not dtypes, so a `UInt32`/`Int64` regression is invisible
to `.equals()`. `pl.len()` yields `UInt32`; published Gold `n` carries `Int64`. Assert `frame.schema`
when the dtype is under test. `polars` is unpinned, so an upgrade can move a dtype with no failure.

### pydantic v2 import-time `NameError`
A leading-underscore field with an explicit `Field()` raises `NameError` **at class-definition
time**, so the module fails to import and everything importing it dies. Never name pydantic fields
with a leading underscore. Also: a derived field declared without a default is *required*; and a
`Field(ge=0)` bound rejects before the validator sees the value (quarantining the row instead of
naming a violation). Keep range checks in `mode="after"` validators.

### Gold view does not resolve
Every Gold view must resolve on a plain `duckdb.connect("rents_layers.duckdb")` — no `ATTACH`, no
catalog alias. A view referencing a table/alias that isn't in the file fails the ingest step.
`daily_layers.yml` proves this by selecting from `gold_area_median` and `AggMonthlyRegistrations`
after ingest; a `Catalog Error` there is this failure.

### Push build running the ETL (regression)
`build_and_deploy.yml` must run only `make build && make test` on push. If a push re-extracts and
publishes, it clobbers the day's release and races the scheduled jobs (previously fixed in
`fix(ci): stop push builds from running the ETL`). Releases are written only by `cron.yml` and
`daily_layers.yml`.

### Debug-flag log noise
`ACTIONS_RUNNER_DEBUG` and `ACTIONS_STEP_DEBUG` are `true` in both workflows, flooding logs and
making `--log-failed` hard to read. Not a failure itself; recommend turning them off when triaging
log-parsing issues.

# Operations

Runbook: how the scheduled jobs work, how to run and publish locally, and how to diagnose
and verify when something looks wrong.

## The two jobs

Both are GitHub Actions, both write into the **same** release tag for a data date.

| Job | Workflow | Schedule (UTC) | What it does |
|-----|----------|----------------|--------------|
| **Bronze extract** | [`cron.yml`](../.github/workflows/cron.yml) | `30 5 * * *` daily | `make all` — build, ETL (`run_etl_pipeline.py`), tests. Publishes the raw CSV + `etl_status.json`. |
| **Layers ingest** | [`daily_layers.yml`](../.github/workflows/daily_layers.yml) | `0 7 * * *` daily | Pulls the prior DuckDB + today's CSV, upserts, publishes `rents_layers.duckdb`. Also `workflow_dispatch` with an optional `data_date`. |
| **Push build** | [`build_and_deploy.yml`](../.github/workflows/build_and_deploy.yml) | on push to `dev`/`main`, and tags | `make build && make test`, then `make lint` and `make coverage`. **Never publishes** — running the ETL on push would clobber the day's release. |

The layers job runs 90 minutes after the bronze job so the day's CSV exists. The layers job can
also be dispatched manually with a `data_date` to re-run a specific day.

### Release model

**One release per data date**, tag `release-YYYY-MM-DD`, holding:

- `rent_contracts_YYYYMMDD.csv` — raw bronze (absent on a quiet day),
- `etl_status.json` — always (the run marker),
- `rents_layers.duckdb` — a full snapshot of the cumulative store as of that date.

Deriving `release-YYYY-MM-DD` from the **data date** (yesterday, UTC) rather than the run date
is what makes the release list self-describing:

- **no release** for a date → the job did not run;
- **status-only** release → it ran, and there was no data;
- **CSV + status (+ DuckDB)** → a normal day.

### Environment and secrets

| Variable | Required | Used by |
|----------|----------|---------|
| `EJARI_URL` | yes for the ETL | Download endpoint. Read from env only — **no hardcoded fallback**. May carry a token in its query string; the log redacts it. |
| `GH_TOKEN` | for publishing | All release writes. If unset, publishing is skipped (local runs still work). |

Copy [`.env.example`](../.env.example) to `.env`. CI supplies both from repo secrets.

## Local commands

```bash
make all              # build → ETL → tests
make etl              # run the daily pipeline (needs EJARI_URL, GH_TOKEN to publish)
make layers           # ingest the newest output/rent_contracts_*.csv into the cumulative DuckDB
make layers-publish   # publish output/rents_layers.duckdb into the day's release
make scrapy-rents     # Scrapy extract only (slow, ~10s/page)
make test             # pytest .
make lint             # ruff check + format-check
make coverage         # pytest with the coverage floor (COVERAGE_FLOOR := 75)
make check            # lint + coverage — the local CI gate
make clean            # remove build artifacts and output/*.csv|*.parquet
```

Ingest a specific day, or backfill several at once (the primary key makes overlap safe):

```bash
uv run python -m lib.analysis.build_layers_duckdb \
    --csv output/rent_contracts_20261004.csv output/rent_contracts_20261005.csv
```

Publish explicitly:

```bash
uv run python -m lib.workspace.publish_layers \
    --artifact output/rents_layers.duckdb --data-through 2026-10-05
```

## Quality gates

`make check` is what CI runs on every push:

- **`make lint`** — `ruff check` + `ruff format --check`. High-signal only (`F`, `E9`, `B`):
  undefined names, unused/duplicate imports, real-bug lints. Not a style gate.
- **`make coverage`** — `pytest --cov=lib --cov-fail-under=75`. The floor is a **ratchet**:
  raise it as coverage improves, never lower it to pass.

Dependencies are locked (`uv sync` reproduces from `uv.lock`); `polars` is bounded
(`>=1.0,<2`) because an unbounded upgrade can move a dtype behind a green suite. See
[ADR-11](adr/0011-reproducibility-and-ci-quality-gates.md).

## Triage

### A red CI run

Start with the [`ci-triage` skill](../.commandcode/skills/ci-triage/SKILL.md): it maps each
workflow to its failure surface and classifies the failed step against
[`known-failures.md`](../.commandcode/skills/ci-triage/references/known-failures.md).

Common signatures and what they mean:

| Signature | Layer | Likely cause |
|-----------|-------|--------------|
| `casting from Utf8View to Boolean not supported` | ingest | an unpinned raw column flipped type — the raw schema must cover it |
| `failed to vstack column 'CONTRACT_AMOUNT'` | ingest | whole-number money column inferred Int64 vs Float64 — pin the dtype |
| `Catalog "silver" does not exist` | consumer | a stale reader still expects the pre-ADR-10 two-file store (use `connect_layers`) |
| coverage below floor | tests | new uncovered code, or the floor was raised |
| undefined name / unused import | lint | ruff `F` — fix, don't broaden the ruleset |

### A stale store

If `_meta.data_through` stops advancing:

1. Check the newest `release-YYYY-MM-DD`. No release → the bronze job did not run. Status-only
   → it ran, no data (a quiet day is fine; a *run* of quiet days is a stall).
2. If releases exist and carry a CSV but the store is behind, the layers job failed — read the
   ingest step. A **`Layers ingest refused ... feed has stalled`** error means the newest
   registration trails the data date by more than `MAX_REGISTRATION_LAG_DAYS` (1): a real feed
   problem, and ingest correctly wrote nothing.
3. Re-run the layers job with `workflow_dispatch` and the right `data_date`.

The `pipeline-triage` agent runs this playbook end to end.

## Verify a release

Trust the artifact, not the green checkmark. [`release-verifier`](../.commandcode/agents/release-verifier.md)
downloads the actual assets and inspects them. By hand:

```bash
TAG=release-2026-10-05
gh release download "$TAG" --repo dataengineergaurav/rental-market-dynamics-dubai --dir /tmp/rel
ls -lh /tmp/rel

python - <<'PY'
import duckdb
con = duckdb.connect("/tmp/rel/rents_layers.duckdb", read_only=True)
print("_meta:", con.execute("SELECT * FROM _meta").fetchall())
print("contracts:", con.execute("SELECT count(*) FROM FctContract").fetchone())
# a Gold view must resolve on a PLAIN connection — no ATTACH, no silver alias
print(con.execute("SELECT * FROM gold_area_median LIMIT 3").fetchall())
PY
```

A pass means: the CSV has data rows, `_meta.data_through` matches the release date, and a Gold
view resolves without any `ATTACH`.

## Your AI copilots

This repo ships its own expert playbooks as Command Code skills and agents under
[`.commandcode/`](../.commandcode/). They are read-only advisors you invoke with the task.

**Skills** (`.commandcode/skills/`):

| Skill | Use it for |
|-------|-----------|
| `uae-market-context` | The domain truth — Ejari/RERA/DLD, area taxonomy, seasonality, regulation — **and the metric contracts** every Gold view must obey. |
| `dubai-real-estate-investor` | Investment framing, the return bridge, and the "rents only, no yield without sale prices" limit. |
| `data-engineer-expert` | A senior data/analytics-engineer audit lens: batch-pipeline best practices and a P0/P1/P2 checklist. |
| `ci-triage` | Diagnose a failed Actions run from its logs. |
| `uv` | Operate the repo with Astral `uv` — sync/lock/run/add, and troubleshooting. |

**Agents** (`.commandcode/agents/`, all read-only):

| Agent | Use it for |
|-------|-----------|
| `market-analyst` | Answer a Dubai market question from `rents_layers.duckdb`, always with `n` and as-of date. |
| `release-verifier` | Verify a published release end to end from the downloaded artifacts. |
| `pipeline-triage` | Diagnose a failed or stale pipeline. |
| `data-quality-reviewer` | Review a change to `lib/analysis`, `lib/transform`, `lib/classes`, tests or workflows before merge. |

## When you change something

- **Code**: run `make check` (lint + coverage) and `make test` before pushing.
- **A metric or view**: update the invariant tests in `tests/`, and keep
  [`metric-contracts.md`](../.commandcode/skills/uae-market-context/references/metric-contracts.md)
  and the Gold-view table in the [README](../README.md) in sync.
- **The raw schema**: add the column to `RAW_RENTS_CSV_DTYPES` and `RAW_RENTS_CSV_COLUMNS`, or
  `tests/test_raw_schema.py` fails.
- **A durable decision**: write an ADR in [`docs/adr/`](adr/) and add it to the
  [index](adr/README.md). Update the [CHANGELOG](../CHANGELOG.md) too.

---
name: ci-triage
description: Triage a failed GitHub Actions run for the Dubai rents pipeline. Pulls the failed-step logs, classifies the failure against this repo's known failure modes, and reports a severity-ranked (P0/P1/P2) diagnosis with the exact next command. Use when a workflow run failed, the build badge is red, or the user asks "why did CI fail", "check the failed run", or "triage the pipeline".
when_to_use: A GitHub Actions run is red, the build/test badge fails, the daily ETL or layers ingest did not produce a release, or the user asks to diagnose a failing run.
argument-hint: "[run-id | workflow-name]"
allowed-tools: Read Grep Glob Bash(gh:*) Bash(git:*)
---

# CI Triage — Dubai Ejari rents pipeline

Diagnose a failed workflow run. This skill is **read-only**: it pulls logs, classifies the
failure, and proposes a fix. It never reruns, pushes, edits a workflow, or publishes a release.

## When to use this skill

- A run in `dataengineergaurav/rental-market-dynamics-dubai` failed (Actions tab, badge, or `gh`).
- The daily outputs are missing: no `release-YYYY-MM-DD` tag, or a release with no CSV / no
  `rents_layers.duckdb`.
- The user says "why did CI fail", "triage the failed run", "check the pipeline".

## Guardrails (do not skip)

- **Read-only.** Never run `gh run rerun`, never `git push`, never edit `.github/workflows/`, and
  never publish or delete a release. Report the fix and stop.
- **Auth.** `gh` uses the `GH_TOKEN` secret inside CI and your local `gh auth` on a laptop. If a
  call fails with an auth error, say so and stop — do not guess at logs.
- **Reproduce before believing.** A red run over this stack is often data-dependent (see
  `references/known-failures.md`); confirm with a local run before calling it a code defect.

## Procedure

### 1. Resolve the run

```bash
# If the user gave a run id, use it. Otherwise find the latest failures:
gh run list --repo dataengineergaurav/rental-market-dynamics-dubai --status failure --limit 10
```

Map the run to its workflow, because each one has a different failure surface:

| Workflow | Trigger | What it runs | Typical failure surface |
|----------|---------|--------------|-------------------------|
| `build_and_deploy.yml` | push to `dev`/`main`/tags | `make build` → `make test` | deps/`make build`, unit tests. **Must not run the ETL** |
| `cron.yml` | schedule `30 5 * * *` UTC | `make all` (build → ETL → test) | `EJARI_URL`/`GH_TOKEN` secrets, spider download, transform, publish |
| `daily_layers.yml` | schedule `0 7 * * *` UTC | pull prior layers → ingest → publish → pytest | freshness gate, `duckdb` views, release download/publish |

### 2. Pull the failed logs

```bash
gh run view <run-id> --repo dataengineergaurav/rental-market-dynamics-dubai --log-failed
```

Logs are **verbose** — both workflows set `ACTIONS_STEP_DEBUG` and `ACTIONS_RUNNER_DEBUG` to
`true`. Narrow before reading:

```bash
gh run view <run-id> --repo dataengineergaurav/rental-market-dynamics-dubai --log-failed \
  | grep -nEi 'error|traceback|runtimeerror|filenotfound|assert|failed|NO_NEW_DATA|refused|stalled'
```

If `--log-failed` is empty, fall back to `gh run view <id> --log` and
`gh run view <id> --json conclusion,event,headBranch,url,jobs`.

### 3. Classify

Match the first meaningful error line against `references/known-failures.md`. For the match, state:

- **Layer** — CI / bronze (extract) / silver (ingest) / gold (views).
- **Category** — code defect, secret/config, data/feed condition, or *expected* (quiet day).
- **Signature** — the exact log lines that prove it.

### 4. Reproduce where cheap

Mirror the failing stage locally so the diagnosis is grounded, not inferred:

```bash
uv run pytest -q                        # mirrors the CI test gate
uv run python -m lib.analysis.build_layers_duckdb --db output/rents_layers.duckdb --csv output/rent_contracts_YYYYMMDD.csv
```

### 5. Report

Deliver a **severity-ranked list** (the user prefers P0/P1/P2, not a flat narrative). For each
finding: severity, one-line cause, the proving log line, the exact next command, and a proposed
minimal diff. **Do not apply the diff unless explicitly asked** — if asked, hand off to a write
workflow on a `fix/ci-<slug>` branch.

## Expected-but-not-a-failure signals

Do not report these as defects:

- `NO_NEW_DATA: window ... produced no registrations` — an empty incremental window is a **success**
  (fail-open by design; ADR-03). The run writes `output/etl_status.json` and exits `0`.
- `no CSV for release-<date> (quiet day)` in `daily_layers.yml` — expected; the step exits `0`.
- `Skipping GitHub publication (GH_TOKEN not set)` — informational, not a failure.

## References

- [references/known-failures.md](references/known-failures.md) — signature → cause → next command.
- In-repo sources of truth: `CONTRIBUTING.md` (testing traps), `docs/adr/0010-cumulative-combined-layers-duckdb.md`,
  `lib/analysis/build_layers_duckdb.py` (`_check_freshness`, `_usable`), `run_etl_pipeline.py`.

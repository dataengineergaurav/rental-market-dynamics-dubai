# Documentation

A map of the docs, and a recommended path through them depending on who you are.

This repository turns **Dubai Ejari rent registrations** into a queryable rental-market
store. It is a small system with a lot of deliberate decisions, so the docs are organized
less like a manual and more like a tour: start with the shape of the thing, then go as deep
as your question needs.

## The map

| Document | What it answers | Length |
|----------|-----------------|--------|
| [README](../README.md) | What is this, at a glance? | short |
| [ARCHITECTURE](ARCHITECTURE.md) | How does the pipeline work end to end, and why is it shaped this way? | medium |
| [DATA_DICTIONARY](DATA_DICTIONARY.md) | What is every table, view, column, code and constant? | long (reference) |
| [ANALYST_COOKBOOK](ANALYST_COOKBOOK.md) | How do I answer a real market question from the store? | medium |
| [OPERATIONS](OPERATIONS.md) | How do I run, schedule, publish, triage and verify it? | medium |
| [LIBRARY_USAGE_GUIDE](LIBRARY_USAGE_GUIDE.md) | How do I call the Python library modules? | long (reference) |
| [ADRs](adr/README.md) | Why is each major decision what it is? | reference |
| [RELEASE_NOTES](../RELEASE_NOTES.md) | What exactly is in a published release? | short |
| [CONTRIBUTING](../CONTRIBUTING.md) | How do I change this without breaking it? | short |
| [CHANGELOG](../CHANGELOG.md) | What changed, most recent first? | reference |

Deep design artifacts (a point-in-time design spec and its implementation plan) live under
[`superpowers/`](superpowers/).

## Reading paths

Pick the one that matches why you are here.

**"I just want the data."**
[RELEASE_NOTES](../RELEASE_NOTES.md) → [ANALYST_COOKBOOK](ANALYST_COOKBOOK.md). One release
per day carries a raw CSV and a single cumulative DuckDB with everything queryable in it.

**"I want to understand the system."**
[README](../README.md) → [ARCHITECTURE](ARCHITECTURE.md) → [ADR-10](adr/0010-cumulative-combined-layers-duckdb.md)
(the decision that defines the current shape).

**"I'm going to change the code."**
[CONTRIBUTING](../CONTRIBUTING.md) → [ARCHITECTURE](ARCHITECTURE.md) →
[DATA_DICTIONARY](DATA_DICTIONARY.md) → the [ADRs](adr/README.md) touching what you'll edit.

**"It's broken and I need it fixed."**
[OPERATIONS](OPERATIONS.md) (runbook + triage) → the `ci-triage` / `pipeline-triage`
playbooks in [`.commandcode/`](../.commandcode/).

**"I'm an investor / analyst."**
[ANALYST_COOKBOOK](ANALYST_COOKBOOK.md) → the `uae-market-context` and
`dubai-real-estate-investor` skills (the domain truth, and the honest limits of a
rents-only dataset).

## The one-paragraph version

Two scheduled jobs run daily. The first downloads yesterday's Ejari registrations and
publishes a raw CSV plus a run-status file as one GitHub release tagged with the **data
date** (`release-YYYY-MM-DD`). The second upserts those contracts into a **single cumulative
DuckDB** — normalized Silver tables and Gold analytics views in the same file — and adds it
to the same release. Ingest is idempotent by a `contract_id` primary key, a quiet day is a
success (fail-open) but a *stalled* feed fails, and every headline is a **median with its
sample size**. That cumulative store is the product: history is what makes the trend views
mean anything.

## Conventions used in these docs

- **Data date** = the registration date a file is named for (yesterday, UTC), not the day
  the job ran. Releases are tagged by it.
- **`release-YYYY-MM-DD`** = the one release per data date, holding the raw CSV,
  `etl_status.json`, and the combined `rents_layers.duckdb`.
- **Bronze / Silver / Gold** are used in the strict medallion sense here:
  Bronze = raw files, Silver = normalized DuckDB tables, Gold = DuckDB views.
- **One owner** means a constant or rule has exactly one definition in the codebase; the
  tests enforce it.

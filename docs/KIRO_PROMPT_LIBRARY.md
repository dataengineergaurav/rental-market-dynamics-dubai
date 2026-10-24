# Kiro Prompt Library — Specs, Hooks, Steering, Skills

A demonstration of the prompts used (and the rewritten, more efficient prompts
that *should* have been used) to drive Kiro's four capabilities on this project:

- **Specs** — structured requirements → design → tasks (`.kiro/specs/`)
- **Hooks** — event-triggered automation (`.kiro/hooks/`)
- **Steering** — always/conditional/manual context (`.kiro/steering/`)
- **Skills** — bundled domain personas (`.agents/skills/`)

Each section pairs a weak first-attempt prompt with a rewritten prompt that is
scoped, grounded in real files, and states its own acceptance gate. The
rewritten prompts are the reusable templates.

---

## What makes a prompt "efficient" here

Every rewritten prompt below follows the same five rules. Use them as a checklist.

1. **Name the artifact type and its destination path.** "Create a spec at
   `.kiro/specs/<name>/`", not "help me plan a feature".
2. **Ground every claim in a real `file:symbol`.** Point at
   `.kiro/steering/_reference-map.md` so the agent cites verified anchors, not
   line numbers it guessed.
3. **State the hard constraints up front.** uv-only execution, no new
   dependencies, the n≥10 gate, the two-point-measurement boundary — say them
   once, clearly, instead of correcting the agent later.
4. **Give the acceptance gate in the prompt.** "`make skills-check` green,
   `make check` clean, coverage floor stays 75." The agent can then self-verify.
5. **Say what NOT to touch.** "No cross-area aggregate", "do not lower the
   coverage floor", "README/Makefile `layers` drift is out of scope."

---

## 1. Specs — `wow-movement`

The spec at `.kiro/specs/wow-movement/` (requirements → design → tasks) adds a
standalone `lib/analysis/wow_movement.py` that reports gated week-over-week
median-rent movement between two weekly DuckDB artifacts.

### 1a. Weak prompt (what not to do)

> Add a feature that shows how rents change week over week and write up a spec
> for it.

Why it is weak: no path, no grounding in the existing `gold_area_median` view,
no mention of the n≥10 gate or the trend-vs-measurement boundary, no acceptance
criteria. The agent would invent a cross-area trend line — exactly what the BI
steering forbids.

### 1b. Rewritten prompt — create the spec

> Create a Kiro spec at `.kiro/specs/wow-movement/` (requirements.md,
> design.md, tasks.md) for a week-over-week rent-movement report.
>
> Context, grounded in `.kiro/steering/_reference-map.md`:
> - Each weekly artifact `output/rental_analytics_weekly_YYYYWxx.duckdb` carries
>   a per-area view `gold_area_median` and a `_meta` view (`week`,
>   `data_through`). Read, do not `ATTACH` one artifact into another (shape B).
> - The BI skills `rental-index-construction` and `rent-figure-trust` forbid
>   smoothed trend / YoY claims on thin data. This feature must be framed as an
>   **honest two-point measurement** of two already-gated medians, not a trend.
>   Put that boundary in the module docstring and in the requirements intro.
>
> Requirements must specify: include an area only if `n >= 10` in BOTH weeks;
> emit `area_name_en, n_from, n_to, median_from, median_to, delta_aed,
> delta_pct`; `low_confidence = True` when either n is in [10, 15]; report areas
> that `entered`/`left` the comparable set (no silent drops); a `#`-comment
> provenance header citing `from_week/to_week/from_data_through/to_data_through`;
> a `--from/--to` pair CLI and a `--series` list; NO cross-area aggregate.
>
> Non-functional: uv/make only, no new dependency (polars + duckdb), mirror the
> pure-function style of `lib/analysis/gold_indexes.py`. Acceptance gate:
> `make skills-check`, `make check`, and `make movement` all pass; coverage
> floor stays 75.

### 1c. Rewritten prompt — execute one task

> Implement task 1 in `.kiro/specs/wow-movement/tasks.md` only. Create
> `lib/analysis/wow_movement.py` with `from __future__ import annotations` and a
> docstring stating the two-point measurement boundary. Implement
> `compute_movement` (inner-join on `area_name_en`, gate `n_from >= 10 AND
> n_to >= 10`, emit delta_aed/delta_pct/low_confidence) and `set_changes`
> (entered/left on the n≥10 sets). No cross-area aggregate anywhere. Stop after
> task 1; do not start task 2. Run `uv run pytest` on any new tests.

Note: the `pytest-after-task` hook runs `make check` automatically when a spec
task is marked complete — so the prompt does not need to ask for it.

---

## 2. Hooks

Three hooks live in `.kiro/hooks/`. All actions run through `make`, honoring the
`uv-environment` rule (one env, one runner).

| Hook file | Trigger | Matcher | Action |
|---|---|---|---|
| `ruff-on-save.json` | PostFileSave | `\.py$` | `make fmt` |
| `pytest-after-task.json` | PostTaskExec | — | `make check` |
| `skills-audit-on-save.json` | PostFileSave | `_department\.md$` | `make skills-audit` |

### 2a. Weak prompt (what not to do)

> Set up some hooks to format my code and run tests automatically.

Why it is weak: no trigger, no matcher, no command. "Format" and "run tests" map
to different events (file save vs task completion). The agent would guess and
likely create several overlapping cosmetic hooks.

### 2b. Rewritten prompt — format-on-save (one consolidated hook)

> Create one Kiro hook `ruff-on-save` that triggers on PostFileSave, matches
> `\.py$`, and runs the single command `make fmt` (ruff format + lint-fix via
> uv). Keep formatting and linting in this one hook — do not split them into
> separate hooks.

### 2c. Rewritten prompt — check-after-task

> Create a Kiro hook `pytest-after-task` that triggers on PostTaskExec (after a
> spec task is marked complete) and runs `make check` (lint + coverage via uv).
> No matcher — it should fire after every task.

### 2d. Rewritten prompt — targeted skills audit

> Create a Kiro hook `skills-audit-on-save` that triggers on PostFileSave,
> matches only `_department\.md$`, and runs `make skills-audit`. Rationale to
> put in the hook description: routing-table and leaf-registration edges live in
> the `_department.md` files, so a full hierarchy audit is worth running when one
> of those saves; individual leaf-skill edits are covered by the manual target
> and CI, not on every save. Do not match every `.md` file — that would run the
> audit far too often.

---

## 3. Steering

Steering is a department/leaf hierarchy under `.kiro/steering/`. `orchestrator.md`
plans and dispatches; each `_department.md` (business-intelligence, engineering)
is a router; leaf skills own one bounded context; `_reference-map.md` is the
single verified `file:symbol` ground truth; `_skill-template.md` is the scaffold
for new leaves. `uv-environment.md` is an always-on execution rule.

### 3a. Weak prompt (what not to do)

> Write some steering docs so Kiro knows how this project works.

Why it is weak: no structure, no inclusion mode, no grounding. The agent would
produce one flat always-included blob that goes stale and bloats every prompt.

### 3b. Rewritten prompt — an always-on execution rule

> Create an always-included steering file `.kiro/steering/uv-environment.md`.
> The rule: all execution in this project runs through `uv` — `uv run python`,
> `uv run pytest`, `uv run ruff`, `uv sync`, `uv add`/`uv remove`. Never bare
> python/pip/pytest/ruff and never a manually activated virtualenv. Hooks,
> scripts, CI, and Makefile targets must invoke tooling through `uv` so behavior
> is identical on every machine. State the rationale in two sentences
> (reproducibility across machines and CI). Keep it short — it loads on every
> interaction.

### 3c. Rewritten prompt — a manual-inclusion leaf skill

> Add a new leaf steering skill following the project's growth recipe in
> `.kiro/steering/orchestrator.md` (the "How to add a new skill" section):
> 1. Copy `.kiro/steering/_skill-template.md` into
>    `.kiro/steering/business-intelligence/` as `<dash-slug>.md`.
> 2. Fill the front-matter (`inclusion: manual`, `name`, `description` with
>    triggers, `owner`, `department: business-intelligence`, `last-verified`)
>    and the `## Contract` block (owns / hands-off-to / routes-from).
> 3. Cite ONLY anchors that already exist in `.kiro/steering/_reference-map.md`.
>    If you need a new anchor, add it to the reference map first, verified
>    against the actual code (symbol name, not a guessed line number).
> 4. Add exactly ONE row to `business-intelligence/_department.md`'s routing
>    table. Do not touch `SKILL.md`.
> 5. Run `make skills-check` (references resolve) and `make lint`.

### 3d. Rewritten prompt — wire an existing feature into steering

> Wire the shipped `lib/analysis/wow_movement.py` into the steering hierarchy
> (this is task 4 of the wow-movement spec). Use the checker's exact reference
> format `` - `path` — `symbol` — description ``:
> - Add module anchors (`compute_movement`, `set_changes`, `load_area_medians`,
>   `movement_for_series`) to `.kiro/steering/_reference-map.md`.
> - Add a section + a References bullet + a contract edge in
>   `.kiro/steering/business-intelligence/rental-index-construction.md`, stating
>   the two-point-measurement-vs-forbidden-trend distinction.
> - Add a one-line pointer in
>   `.kiro/steering/business-intelligence/rent-figure-trust.md`.
> Verify with `make skills-check` (all new references resolve).

---

## 4. Skills

`.agents/skills/dubai-rental-bundle/SKILL.md` is one bundle that routes a task to
one of five domain personas (Orchestrator, Dubai Rental Market Expert, Data
Engineer, Solution Architect, QA/Data Validator) plus Release & Ops. Each persona
carries triggers, a workflow, and hard gates, and cites `file:line` anchors.

### 4a. Weak prompt (what not to do)

> Make a skill that knows about Dubai rentals and our pipeline.

Why it is weak: no personas, no triggers, no gates, no citations. The result
would be a generic description with no routing and no enforceable rules.

### 4b. Rewritten prompt — author the bundle

> Create a composite skill `.agents/skills/dubai-rental-bundle/SKILL.md` for this
> repo. Front-matter: `name`, a `description` that states it routes tasks to the
> right domain expert, and `metadata` (project, stack: polars/duckdb/scrapy/ejari).
>
> Bundle these personas, each with **Triggers → Workflow → Gates** and
> `file:line` citations into `lib/`, `rents_scraper/`, `run_etl_pipeline.py`:
> 0. Orchestrator — entry point for any multi-step task; reads the project +
>    docs first, then sequences a gated plan and dispatches 1–5.
> 1. Dubai Rental Market Expert — Ejari/RERA semantics, median-not-mean,
>    the PSF gate (`actual_area >= 200`), bulk-registration exclusion.
> 2. Data Engineer — Bronze→Silver→Gold correctness; stack fixed, no new dep.
> 3. Solution Architect — WBS + ADRs + phase gates.
> 4. QA / Data Validator — validators, fixtures, `make test`, reject any area
>    median with n < 10.
> 5. Release & Ops — dated/weekly release tags, idempotent publish, `GH_TOKEN`.
>
> Hard gates every persona enforces: reject any Gold output with a PSF over the
> residential ceiling, any Naif/Hor bulk leak, or any area median with n < 10.
> End with a runnable Verification block (`uv run pytest -q` + a polars
> row-count check + `make etl`).

### 4c. Rewritten prompt — keep the skill honest against code drift

> Audit `.agents/skills/dubai-rental-bundle/SKILL.md` against the current code and
> reconcile it with `.kiro/steering/_reference-map.md`. For every `file:line` or
> `file:symbol` the skill cites, confirm the symbol still resolves; where the
> skill makes a claim the reference map marks OUTDATED (e.g. "`is_residential`
> always False", "`is_bulk_registration` is a to-do", PSF thresholds at
> `config.py:86`, cumulative-layers architecture), correct the skill to match the
> verified ground truth. Do not invent new anchors — if one is missing from the
> reference map, add it there first, verified against code. Finish with
> `make skills-check`.

---

## Quick reference — prompt templates by capability

| Capability | Template opening | Must include |
|---|---|---|
| **Spec** | "Create a Kiro spec at `.kiro/specs/<name>/` …" | the n≥10 / measurement-not-trend boundary, uv/no-new-dep, `make check`/`skills-check`/target gate |
| **Hook** | "Create one Kiro hook `<id>` that triggers on `<Trigger>`, matches `<regex>`, runs `<make target>`" | single `make` command, precise matcher, consolidate cosmetic hooks |
| **Steering** | "Add a leaf steering skill following the growth recipe in `orchestrator.md` …" | inclusion mode, Contract block, cite only `_reference-map.md` anchors, one `_department.md` row |
| **Skill** | "Create a composite skill at `.agents/skills/<name>/SKILL.md` …" | personas with Triggers→Workflow→Gates, `file:line` citations, runnable Verification |

All four share the same spine: **name the path, ground every claim in
`_reference-map.md`, state the uv-only + no-new-dependency constraints, and put
the acceptance gate (`make skills-check` / `make check`) in the prompt itself.**

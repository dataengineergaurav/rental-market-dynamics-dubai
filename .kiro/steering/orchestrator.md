---
inclusion: manual
name: orchestrator
description: Top-level planner for rental-market-dynamics-dubai. Load for any multi-step task ("plan this", "fix the pipeline", "from docs to code") — it reads the project, sequences a gated plan, and dispatches to the Business Intelligence or Engineering department.
owner: "@dataengineergaurav"
last-verified: 2026-10-24
---

# Orchestrator — plan first, then dispatch

You are the orchestrator. You understand the project before you plan, then
route work to the right department and its leaf skills. You never write code
without first reading the reference map and the owning skill.

## Contract

- **owns:** turning any request into a sequential, gated plan and dispatching it.
- **hands-off-to:**
  - `business-intelligence/_department` — when the task is about market meaning, rent trust, geography, RERA, or index/analyst work.
  - `engineering/_department` — when the task is about ETL, the medallion layers, extraction, releases/ops, or data-quality gates.
- **routes-from:**
  - `SKILL.md` (company router) — when a request is multi-step or its department is unclear.

## What you read first (mandatory, in order)

1. `.kiro/steering/_reference-map.md` — verified `file:symbol` ground truth. Every citation you make must appear there.
2. The relevant `_department.md` (BI or Engineering) — shared context + routing table.
3. The specific leaf skill(s) the department points you to.

If the reference map is unread, you have not grounded the plan — stop and read it.

## How you plan (sequential, gated)

1. **Scope & ground.** State what is being asked, cite the department and leaves involved, and name the exit gate for each step.
2. **Map to department + leaves.** Pick the minimal set of skills. A rent-meaning question goes to BI; a pipeline-correctness question goes to Engineering; many tasks cross both (BI defines the rule, Engineering/QA enforces it).
3. **Sequence with gates.** Emit an ordered list where each step has an explicit exit gate (e.g. PSF null unless area ≥ 200; weekly freshness gate passes; area medians have n ≥ 10).
4. **Minimal diff.** Keep the stack fixed (polars + duckdb + scrapy). A new dependency needs an ADR, not an inline add.
5. **Verification.** End every plan with runnable `uv`/`make` commands: `make skills-check`, `make check`, and the specific assertion the change is supposed to satisfy.

## How to add a new skill (the growth recipe)

The hierarchy is designed so growth touches as little as possible:

1. Copy `.kiro/steering/_skill-template.md` into the owning department folder
   (`business-intelligence/` or `engineering/`) with a dash-slug filename.
2. Fill the front-matter (`name`, `description`/triggers, `owner`, `department`,
   `last-verified`) and the `## Contract` block (owns / hands-off-to / routes-from).
3. Cite only anchors that exist in `_reference-map.md`. If you need a new anchor,
   add it to the reference map first (verified against code).
4. Add ONE row to that department's `_department.md` routing table. The company
   router (`SKILL.md`) does not change — departments absorb growth.
5. Run `make skills-check` (references resolve) and `make lint`.

## Delegation quick rules

- Rental threshold or market meaning? → BI defines it, Engineering's
  `data-quality-gates` enforces it.
- New dependency or infra? → write an ADR before any Engineering leaf adds it.
- Release blocked? → Engineering `release-and-ops` (GH_TOKEN / tag reuse), then
  `data-quality-gates`.
- Docs vs code disagree? → code wins; cite the `_reference-map.md` anchor and
  file a docs update as a follow-up task.

## References

- `run_etl_pipeline.py` — `main` — pipeline entry point the plan's verification exercises.

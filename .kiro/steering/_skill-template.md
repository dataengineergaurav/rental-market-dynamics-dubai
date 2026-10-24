---
inclusion: manual
name: <dash-slug-name>
description: <one line — what this skill covers AND its triggers, so routing is unambiguous>
owner: "@github-handle"
department: business-intelligence | engineering
last-verified: YYYY-MM-DD
---

# <Skill Title>

<One-paragraph statement of what this skill is responsible for and when to load it.>

## Contract

The contract block makes inter-skill communication explicit. The router and
`_department.md` files render these edges into a handoff map.

- **owns:** <the bounded context this skill is the single source of truth for>
- **hands-off-to:** <other skills this one routes work TO, with the trigger>
  - `skill-name` — when <condition>
- **routes-from:** <skills/departments that route INTO this one>
  - `department` / `skill-name` — when <condition>

## Triggers

Load this skill when the task involves: <concrete keywords / questions>.

## Workflow

<The ordered steps / mental model. Cite `file.py` + symbol from `_reference-map.md`.>

## Gates

<Hard rules that must hold. What this skill rejects.>

## References

Every anchor below is verified in `_reference-map.md` (do not cite a line number
that is not in that table).

- `path/to/file.py` — `symbol_name` — <what it is>

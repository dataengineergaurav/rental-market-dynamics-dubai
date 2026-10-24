#!/usr/bin/env python3
"""skills-graph-check — consistency monitor for the cross-skill COMMUNICATION layer.

Where `skills_check.py` validates that a skill's cited `file:symbol` code
references resolve, THIS checker validates the skill-to-skill graph that the
hierarchy uses to communicate: the `hands-off-to` / `routes-from` contract edges
and the department routing tables.

Canonical skill id = the file stem (e.g. `rent-figure-trust`), because the
contract edges and routing-table rows name skills by that slug, not by the
front-matter `name`. A department is identified as `<dept>/_department`.

Checks (agreed design):
  ERROR   — a `hands-off-to` / `routes-from` edge names a skill that does not exist.
  ERROR   — a department routing-table row names a leaf that does not exist.
  ERROR   — a leaf is not registered in its own department's routing table
            (the leaf's front-matter `department` is the source of truth).
  WARN    — a `hands-off-to: B` has no matching `routes-from: A` on B
            (asymmetry is sometimes intentional, e.g. an "enforces" edge).

Exit 0 = no errors (warnings allowed). Exit 1 = at least one error.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

STEERING = Path(".kiro/steering")
REPO_ROOT = Path(__file__).resolve().parent.parent

# Meta files that are not skills in the graph.
META_SKIP = {"_reference-map.md", "_skill-template.md"}

# An edge/row reference is a backticked slug, optionally with a `/_department`
# suffix. We capture the slug and ignore trailing annotations like "(BI)".
_REF = re.compile(r"`([a-z0-9/_-]+)`")
_SECTION = re.compile(r"^\s*-\s*\*\*(hands-off-to|routes-from)\s*:\*\*")
_BULLET = re.compile(r"^\s*-\s+`([a-z0-9/_-]+)`")
_FRONT_DEPARTMENT = re.compile(r"^department:\s*(\S+)", re.MULTILINE)
# First cell of a routing-table row: a backticked leaf slug, optionally followed
# by an annotation like "(+analyst)" before the cell boundary `|`.
_TABLE_ROW = re.compile(r"^\|\s*`([a-z0-9_-]+)`[^|]*\|")


def _skill_id(path: Path) -> str:
    """File stem is the canonical id. `_department.md` -> `<dept>/_department`."""
    if path.name == "_department.md":
        return f"{path.parent.name}/_department"
    return path.stem


def _parse_skill(path: Path) -> dict:
    """Extract department, hands-off-to and routes-from edge targets from a skill."""
    text = path.read_text(encoding="utf-8")
    dept_m = _FRONT_DEPARTMENT.search(text)
    department = dept_m.group(1).strip().strip('"') if dept_m else None

    hands_off: list[str] = []
    routes_from: list[str] = []
    current: str | None = None
    for line in text.splitlines():
        sec = _SECTION.match(line)
        if sec:
            current = sec.group(1)
            continue
        # a new top-level bullet that is NOT a sub-bullet ends the section
        if current and re.match(r"^\s*-\s*\*\*", line):
            current = None
        b = _BULLET.match(line)
        if b and current:
            target = b.group(1)
            (hands_off if current == "hands-off-to" else routes_from).append(target)
    return {
        "department": department,
        "hands_off_to": hands_off,
        "routes_from": routes_from,
    }


def _parse_routing_table(path: Path) -> list[str]:
    """Leaf slugs registered in a department's routing table."""
    rows: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        m = _TABLE_ROW.match(line)
        if m:
            rows.append(m.group(1))
    return rows


def main() -> int:
    steering = REPO_ROOT / STEERING
    skill_files = [p for p in sorted(steering.rglob("*.md")) if p.name not in META_SKIP]
    # Known skill ids: every skill file plus every department id.
    skills: dict[str, dict] = {}
    departments: dict[str, Path] = {}
    leaves_by_dept: dict[str, list[str]] = {}

    for p in skill_files:
        sid = _skill_id(p)
        skills[sid] = _parse_skill(p)
        if p.name == "_department.md":
            departments[sid] = p
        elif skills[sid]["department"]:
            leaves_by_dept.setdefault(skills[sid]["department"], []).append(sid)

    known_ids = set(skills.keys())
    errors: list[str] = []
    warnings: list[str] = []

    # 1. edge targets must exist
    for sid, info in skills.items():
        for target in info["hands_off_to"] + info["routes_from"]:
            if target not in known_ids:
                errors.append(f"{sid}: edge target does not exist: `{target}`")

    # 2. routing tables must resolve, and leaves must be registered
    registered: dict[str, list[str]] = {}
    for dep_id, dep_path in departments.items():
        dept_name = dep_id.split("/")[0]
        rows = _parse_routing_table(dep_path)
        for leaf in rows:
            registered.setdefault(dept_name, []).append(leaf)
            if leaf not in known_ids:
                errors.append(
                    f"{dep_id}: routing-table row names a missing leaf: `{leaf}`"
                )

    # 3. every leaf registered in exactly its own department's table
    for dept_name, leaves in leaves_by_dept.items():
        table = set(registered.get(dept_name, []))
        for leaf in leaves:
            if leaf not in table:
                errors.append(
                    f"{dept_name}: leaf `{leaf}` (department={dept_name}) is NOT in the routing table"
                )

    # 4. asymmetry warnings: A hands-off-to B but B lacks routes-from A
    for sid, info in skills.items():
        for target in info["hands_off_to"]:
            if target in skills and sid not in skills[target]["routes_from"]:
                warnings.append(
                    f"{sid}: hands-off-to `{target}` has no matching routes-from on `{target}` "
                    f"(intentional asymmetry? confirm)"
                )

    print(
        f"skills-graph-check: {len(skills)} skills, "
        f"{len(departments)} departments, {len(errors)} error(s), {len(warnings)} warning(s)"
    )
    for w in warnings:
        print(f"  WARN  {w}")
    if errors:
        print("skills-graph-check: ERRORS:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("skills-graph-check: graph consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

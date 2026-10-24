#!/usr/bin/env python3
"""skills-check — drift monitor for the steering skill hierarchy.

Parses every skill file under .kiro/steering/ and verifies that each code
reference it cites still resolves against the working tree. This is DRIFT
DETECTION only: it confirms a cited `file.py` exists and, when a symbol is
named, that the symbol is defined in that file. It does NOT verify that the
surrounding prose is still true — that relies on the periodic human/agent sweep
tracked by each skill's `last-verified` front-matter field.

A reference is written in a skill as an inline code span pairing a path with a
symbol, e.g. `lib/config.py` ... `get_area_tier`. To keep the checker simple and
deterministic, references are declared explicitly in each skill's "References"
section as lines of the form:

    - `path/to/file.py` — `symbol_name` — description

The checker extracts (path, symbol) pairs from those lines, confirms the file
exists, and greps the file for a definition of the symbol (def/class/assignment).

Exit 0 = all references resolve. Exit 1 = at least one dangling reference.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

STEERING_DIR = Path(".kiro/steering")
REPO_ROOT = Path(__file__).resolve().parent.parent

# a References bullet: - `path` — `symbol` — desc   (em dash or hyphen separators)
REF_LINE = re.compile(r"^\s*-\s+`([^`]+\.(?:py|sql))`\s*[—-]\s*`([^`]+)`")


def _symbol_defined(file_path: Path, symbol: str) -> bool:
    """True if `symbol` appears as a definition in file_path.

    Accepts: def symbol, class symbol, symbol =, symbol: (dict key not required).
    SQL files: match the symbol as a whole word (view/table names, columns).
    """
    try:
        text = file_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    if file_path.suffix == ".sql":
        return re.search(rf"\b{re.escape(symbol)}\b", text) is not None
    patterns = [
        rf"^\s*def\s+{re.escape(symbol)}\b",
        rf"^\s*class\s+{re.escape(symbol)}\b",
        rf"^\s*{re.escape(symbol)}\s*[:=]",
        # method / attribute defined anywhere
        rf"\b{re.escape(symbol)}\b",
    ]
    return any(re.search(p, text, re.MULTILINE) for p in patterns)


def main() -> int:
    steering = REPO_ROOT / STEERING_DIR
    if not steering.exists():
        print(f"skills-check: no steering dir at {STEERING_DIR}", file=sys.stderr)
        return 1

    # _skill-template.md and _reference-map.md are meta files: the template
    # carries placeholder refs by design, and the reference-map is the ground
    # truth the others cite rather than a skill itself. Department routers
    # (_department.md) ARE checked — they carry real references.
    META_SKIP = {"_skill-template.md", "_reference-map.md"}
    skill_files = sorted(p for p in steering.rglob("*.md") if p.name not in META_SKIP)
    if not skill_files:
        print("skills-check: no skill files found", file=sys.stderr)
        return 1

    total_refs = 0
    dangling: list[str] = []

    for skill in skill_files:
        for lineno, line in enumerate(
            skill.read_text(encoding="utf-8").splitlines(), 1
        ):
            m = REF_LINE.match(line)
            if not m:
                continue
            rel_path, symbol = m.group(1), m.group(2)
            total_refs += 1
            target = REPO_ROOT / rel_path
            rel_skill = skill.relative_to(REPO_ROOT)
            if not target.exists():
                dangling.append(f"{rel_skill}:{lineno}  missing file: {rel_path}")
                continue
            if not _symbol_defined(target, symbol):
                dangling.append(
                    f"{rel_skill}:{lineno}  symbol not found: {symbol} in {rel_path}"
                )

    print(
        f"skills-check: scanned {len(skill_files)} skill files, {total_refs} references"
    )
    if dangling:
        print(f"skills-check: {len(dangling)} DANGLING reference(s):", file=sys.stderr)
        for d in dangling:
            print(f"  - {d}", file=sys.stderr)
        return 1
    print("skills-check: all references resolve")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

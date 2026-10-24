# Tasks — Week-over-Week Rent Movement

- [ ] 1. Core movement computation + gating
  - Create `lib/analysis/wow_movement.py` (pure functions, polars,
    `from __future__ import annotations`, docstring stating the two-point
    measurement boundary).
  - Implement `compute_movement` (inner-join on `area_name_en`, gate
    `n_from >= 10 AND n_to >= 10`, emit delta_aed / delta_pct / low_confidence).
  - Implement `set_changes` (entered/left on the n>=10 sets).
  - No cross-area aggregate anywhere.
  - _Requirements: 1, 2, 3_

- [ ] 2. Artifact I/O, CSV writer, CLI, series support
  - Implement `load_area_medians` (duckdb read-only: `gold_area_median` +
    `_meta`, week-label fallback to filename regex).
  - Implement `movement_for_series` over consecutive pairs.
  - Implement `write_movement_csv` with the `#`-comment provenance header.
  - Add the argparse CLI (`--from/--to` and `--series`).
  - _Requirements: 4, 5, 6_

- [ ] 3. Tests
  - Create `tests/test_wow_movement.py`: fixture-based unit tests (exclusion,
    delta math, low_confidence band, set_changes labels) and a skip-if-absent
    integration test against the real W37/W38 artifacts (~118 rows, both n>=10).
  - Run with `uv run pytest`.
  - _Requirements: 1, 2, 3, 5_

- [ ] 4. Make target + skill wiring
  - Add a uv-backed `make movement` target (two most recent weekly DuckDBs);
    update help text and `.PHONY`.
  - Add module anchors to `.kiro/steering/_reference-map.md`.
  - Add a section + References bullet + contract edge in
    `.kiro/steering/business-intelligence/rental-index-construction.md`, stating
    the two-point-measurement-vs-forbidden-trend distinction.
  - Add a one-line pointer in
    `.kiro/steering/business-intelligence/rent-figure-trust.md`.
  - References must use the checker format: `` - `path` — `symbol` — desc ``.
  - _Requirements: 7_

- [ ] 5. Verification sweep
  - `make skills-check` green (new References resolve).
  - `make check` clean (lint + coverage; do not lower the 75 floor).
  - `make movement` produces `output/rent_movement_2026W37_2026W38.csv` with a
    `#` header and ~118 data rows; show the first ~15 lines.
  - _Requirements: 1, 2, 3, 4, 6, 7_

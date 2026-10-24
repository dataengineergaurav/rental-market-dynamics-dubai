# Requirements — Week-over-Week Rent Movement

## Introduction

The pipeline ships self-contained weekly DuckDB artifacts
(`output/rental_analytics_weekly_YYYYWxx.duckdb`), each carrying a per-area
trusted-median view `gold_area_median`. Nothing today reports how an area's
median rent **moved** between weeks.

The BI steering skills (`rental-index-construction`, `rent-figure-trust`) forbid
smoothed trend / YoY claims on thin data. This feature does not violate that
rule: it is an **honest two-point measurement** of two already-gated weekly
medians, not an inferred trend. It makes no claim beyond the two `data_through`
dates it compares.

This feature adds a standalone `lib/analysis/` module (shape B — no coupling
between artifacts) that reads two or more weekly artifacts and emits a gated
week-over-week movement report as a CSV sidecar.

## Requirements

### Requirement 1 — Gated per-area movement

**User story:** As an analyst, I want the median-rent change for each Dubai area
between two weeks, so that I can see real movement without being misled by thin
or bulk-contaminated samples.

#### Acceptance criteria

1. WHEN two weekly artifacts are compared THEN the system SHALL include an area
   only IF its `n >= 10` in BOTH weeks.
2. WHEN an area qualifies THEN the system SHALL emit `area_name_en`, `n_from`,
   `n_to`, `median_from`, `median_to`, `delta_aed` (= `median_to - median_from`),
   and `delta_pct` (= `100 * delta_aed / median_from`).
3. The system SHALL inherit the comparable-row exclusions from
   `gold_area_median` (bulk groups, Hotel/Labor Camps, Virtual Unit) and SHALL
   NOT re-filter raw stock.
4. The system SHALL NOT compute any city-wide mean-of-medians or any aggregate
   across areas.

### Requirement 2 — Confidence flagging

**User story:** As an analyst, I want near-floor samples flagged, so that I don't
over-read a median built on a handful of contracts.

#### Acceptance criteria

1. WHEN either `n_from` OR `n_to` is in the inclusive range [10, 15] THEN the
   row's `low_confidence` SHALL be `True`.
2. WHEN both n values exceed 15 THEN `low_confidence` SHALL be `False`.
3. The [10, 15] band SHALL be documented as a tunable heuristic.

### Requirement 3 — Visible set changes (no silent drops)

**User story:** As an analyst, I want to know which areas entered or left the
comparable set, so that a disappearance is visible rather than silently omitted.

#### Acceptance criteria

1. WHEN an area clears `n >= 10` in the `to` week only THEN it SHALL be reported
   as `entered`.
2. WHEN an area clears `n >= 10` in the `from` week only THEN it SHALL be
   reported as `left`.
3. The set-change report SHALL be emitted alongside the movement report.

### Requirement 4 — Provenance in output

**User story:** As a consumer, I want each report to state the exact window it
compares, so that the "from → to" is honest.

#### Acceptance criteria

1. The output SHALL cite `from_week`, `to_week`, `from_data_through`, and
   `to_data_through` read from each artifact's `_meta`.
2. The output SHALL state the count of compared areas and the entered/left counts.
3. WHEN a week label cannot be read from `_meta` THEN it SHALL fall back to the
   filename pattern `rental_analytics_weekly_(\w+).duckdb`.

### Requirement 5 — Reusable across a series

**User story:** As a maintainer, I want the core to handle an ordered list of
artifacts, so that the same code serves a longer time series later without a
rewrite.

#### Acceptance criteria

1. The core SHALL expose a function computing movement over each consecutive
   pair of an ordered list of artifact paths.
2. The CLI SHALL support both a single `--from/--to` pair and a `--series` list.

### Requirement 6 — Output format

#### Acceptance criteria

1. The system SHALL write `output/rent_movement_<fromweek>_<toweek>.csv`.
2. The CSV SHALL be prefixed with a metadata header as `#`-comment lines
   carrying the Requirement 4 provenance fields.

### Requirement 7 — Project conventions (non-functional)

#### Acceptance criteria

1. All execution SHALL run through `uv`/`make` (no bare python/pip/pytest/ruff).
2. The implementation SHALL add NO new dependency (polars + duckdb only).
3. The module SHALL mirror the standalone, tested, pure-function style of
   `lib/analysis/gold_indexes.py`.
4. `make skills-check`, `make check`, and `make movement` SHALL all pass; the
   coverage floor (75) SHALL NOT be lowered.

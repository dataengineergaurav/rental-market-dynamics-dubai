---
inclusion: manual
name: data-quality-gates
description: The guardrails that enforce the publish rules BI defines — the fail-open validator, PSF/bulk exclusions, the market-health gate, and the pytest coverage ratchet. Load when working on validation, test fixtures, thresholds, or make check.
owner: "@dataengineergaurav"
department: engineering
last-verified: 2026-10-24
---

# Data-Quality Gates

Owns enforcement: the code and tests that make the trust rules real. This leaf
is where BI's "don't publish a bad number" becomes a failing check.

## Contract

- **owns:** validators, the PSF/bulk exclusions in code, the coverage ratchet, CI gate wiring.
- **hands-off-to:**
  - `rent-figure-trust` (BI) — this leaf ENFORCES the publish rule that BI defines.
- **routes-from:**
  - `engineering/_department` — any test / validation / threshold task.
  - `release-and-ops` — a release ships only behind green gates.

## Triggers

Validators, `make test` / `make check`, coverage floor, PSF fixture, bulk flag
test, `strict` mode, "why did the gate fail", market-health band.

## Workflow

- **Fail-open validation.** `validate_rent_contracts(df, strict=False)` collects
  violations rather than raising; `strict=True` promotes warnings to errors.
  Follows ADR-03 — a bad row is recorded, not thrown.
- **Bulk exclusion is live.** `PropertyUsage` filters `is_bulk_registration == False`
  when the enriched column is present, so bulk blocks never reach the usage report.
  (The flag itself is set in `enrichment._add_bulk_flag`; see
  `rent-figure-trust` for the rule.)
- **PSF guard.** PSF is null unless `actual_area >= 200` (`_add_psf`); the band
  itself (`min/max_psf_residential|commercial`) lives in `VALIDATION_THRESHOLDS`.
- **Market-health gate.** `build_residential_market_index` in `gold_indexes.py`
  is the measurement surface; the bands are `GATE_MEDIAN_FLOOR_AED`,
  `GATE_MEDIAN_CEILING_AED`, `GATE_P95_CEILING_AED`.
- **Coverage ratchet.** `make coverage` enforces `COVERAGE_FLOOR` (currently 75);
  raise it as coverage improves, never lower it to pass. `make check` = lint +
  coverage, the CI quality gate.

## Gates

- CI green (`make check`) required before shipping.
- A median with `n < MARKET_METRICS["min_area_sample_size"]` (10) is rejected.
- PSF rows failing the `>= 200` area guard are nulled, not published.
- The coverage floor only ratchets up.

## References

- `lib/classes/validators.py` — `validate_rent_contracts` — the fail-open validator.
- `lib/classes/property_usage.py` — `PropertyUsage` — excludes bulk rows from the usage report.
- `lib/transform/enrichment.py` — `_add_psf` — the `actual_area >= 200` PSF guard.
- `lib/config.py` — `VALIDATION_THRESHOLDS` — the PSF/size/rent bands.
- `lib/config.py` — `MARKET_METRICS` — the n≥10 floor.
- `lib/analysis/gold_indexes.py` — `build_residential_market_index` — the market-health measurement surface.
- `lib/analysis/gold_indexes.py` — `GATE_MEDIAN_FLOOR_AED` — the residential median floor.

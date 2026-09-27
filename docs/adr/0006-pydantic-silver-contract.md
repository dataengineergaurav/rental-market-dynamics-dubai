# ADR-06: Pydantic v2 for the per-row Silver contract

**Status:** Accepted · **Date:** 2026-09-27

## Context

`docs/IMPLEMENTATION_PLAN.md:68` requires an ADR before any new dependency. The Silver layer needs
to attribute data-quality violations to individual fields, so they can be counted and queried
rather than logged as a single aggregate line.

## Decision

Use pydantic v2 for the per-row contract in `lib/classes/silver_contract.py`.

## Rationale

- **Cost is negligible.** Measured on the real `output/rent_contracts_20260917.csv` payload
  (4306 rows): ~200k model constructions/sec for `SilverRentContract(**row)`, and 18k rows/sec
  for a whole `to_silver()` including `row_hash`, `model_dump()` and the polars frame build. A
  90-day backfill of ~150k rows is therefore ~8s end to end, against a daily file of ~1.7k rows.
  ADR-01's streaming constraint is not a reason to keep this vectorised.
- **Field-level attribution.** `schema_overrides` in `lib/transform/rents_transformer.py:27` can
  coerce a column but cannot say *which* rule a specific row violated. On 20260917, 9 rows below
  `min_annual_rent` and 1 above `max_annual_rent` are indistinguishable from 3860 rows below the
  PSF floor without a per-row model; `to_silver().violation_counts` reports all of them by name
  (`annual_amount_below_min: 9`, `annual_amount_above_max: 1`,
  `actual_area_below_psf_floor: 3860`, plus `actual_area_above_max: 3`,
  `amount_duration_mismatch: 85` and `merged_contract_group: 10`).
- **Fails loudly by default.** The original `BronzeRentContract` used leading-underscore field
  names, which pydantic v2 rejects at class-definition time with `NameError`. That took down
  `lib/classes/validators.py` entirely and the failure was invisible in production because
  `run_etl_pipeline.py` swallowed it. The module now has an import smoke test
  (`tests/test_silver_contract.py:8`).

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Extend `pl.scan_csv` `schema_overrides` only | Cannot attribute violations to rows or rules |
| Vectorised polars expressions throughout | Loses per-row context; a later reader cannot tell which rule fired |
| dataclasses instead of pydantic | No coercion, no declarative field bounds, more hand-written parsing |

## Consequences

pydantic v2 is a permanent runtime dependency. `requirements.txt` must track `pyproject.toml`.
Field naming is constrained: canonical snake_case pipeline names only, and no leading underscores.

Two behaviours are load-bearing and easy to undo by accident:

- **The model is `frozen=True`**, so derived fields are set with `object.__setattr__(self, ...)`
  inside `mode="after"` validators. A bare `self.x = ...` raises at validation time.
- **No pydantic bounds on the numeric fields.** A bound like `Field(ge=0)` rejects the value
  *before* `mode="after"` runs, so the named violation is lost and the row is quarantined instead
  of being counted. All range checking lives in `_derive_and_collect`.

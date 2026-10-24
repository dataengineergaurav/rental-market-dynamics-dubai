---
inclusion: manual
name: ejari-contract-semantics
description: The ubiquitous language of a Dubai Ejari tenancy contract — what each field means, New vs Renewed, freehold-as-title (not price), the sub-type/type exclusions, and duration buckets. Load before interpreting any field or writing a filter on the raw feed.
owner: "@dataengineergaurav"
department: business-intelligence
last-verified: 2026-10-24
---

# Ejari Contract Semantics

Owns the meaning of the fields. Every other BI leaf leans on this one for "what
does this column actually say".

## Contract

- **owns:** field semantics, the vocabulary of a registered tenancy contract.
- **hands-off-to:**
  - `dubai-market-geography` — field meaning (area/project columns) feeds tiering.
  - `rent-figure-trust` — the exclusions here are what trust filtering acts on.
- **routes-from:**
  - `business-intelligence/_department` — any "what does this field mean" task.

## Triggers

Field meaning, `VERSION_EN` New/Renewed, `IS_FREE_HOLD_EN`, `PROP_SUB_TYPE_EN`,
`ejari_property_type_en`, Virtual Unit, Labor Camps, Hotel, contract duration.

## Core knowledge (verified)

- **This feed is Ejari registered tenancy contracts** — registered leases, not
  DLD sales, not off-plan, not short-term/holiday-home data.
- **New vs Renewed matters.** A Renewed lease is rent-capped under the RERA
  framework; a New lease is at market. Never pool them when reasoning about rent
  level (see `rera-rent-advisory`).
- **Freehold is TITLE, not price.** `IS_FREE_HOLD_EN` marks an investment/freehold
  zone, not a rent premium. "Non Free Hold average > Free Hold average" in a
  window is usually a bulk-registration artifact, not a market signal.
- **Exclusions live on two different columns** — a common mistake:
  - Hotel and Labor Camps are values of the **sub_type** column
    (`ejari_property_sub_type_en`) → excluded there.
  - Virtual Unit is a value of the **type** column (`ejari_property_type_en`),
    NEVER the sub_type column. Filtering "Virtual Unit" on sub_type is a silent
    no-op. The shipped code filters it on the type column (`EXCLUDED_PROPERTY_TYPES`
    in `gold_indexes.py`).
- **Usage sets have one owner.** `RESIDENTIAL_USAGE` / `COMMERCIAL_USAGE` in
  `lib/config.py` are the membership lists; `is_residential(usage: str)` /
  `is_commercial(usage: str)` test against them. These are `str`-typed and return
  correct membership — the old "is_residential always False" critique is outdated.
- **Duration buckets.** Enrichment labels `< 180` days Short-term, `< 365`
  Medium-term, `>= 365` Long-term (`_add_contract_duration`). A standard annual
  lease is the 6–12 month band.

## Gates

- Never cite "Virtual Unit" as a sub_type exclusion — it is a type value.
- Never read freehold as a price signal.
- Never pool New and Renewed when discussing rent level.

## References

- `lib/config.py` — `RESIDENTIAL_USAGE` — residential usage membership set.
- `lib/config.py` — `COMMERCIAL_USAGE` — commercial usage membership set.
- `lib/config.py` — `is_residential` — str-typed residential test (NOT always False).
- `lib/config.py` — `DLD_SCHEMA` — the raw→canonical column name map.
- `lib/transform/enrichment.py` — `_add_contract_duration` — the duration buckets.
- `lib/analysis/gold_indexes.py` — `EXCLUDED_PROPERTY_TYPES` — Virtual Unit on the TYPE column.
- `lib/analysis/gold_indexes.py` — `EXCLUDED_SUB_TYPES` — Hotel/Labor Camps on the sub_type column.

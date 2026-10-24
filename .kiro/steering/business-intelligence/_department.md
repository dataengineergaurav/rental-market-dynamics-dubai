---
inclusion: manual
name: business-intelligence-department
description: Business Intelligence department router for rental-market-dynamics-dubai. Load for any task about what a rent figure MEANS — Ejari contract semantics, rent-figure trust, Dubai geography/tiering, RERA rent bands, rental index construction, and analyst SQL against the Gold views.
owner: "@dataengineergaurav"
department: business-intelligence
last-verified: 2026-10-24
---

# Business Intelligence Department

The BI department owns the *meaning* of the data: what Ejari contracts are, when
a rent figure is trustworthy, how Dubai's geography tiers, what the RERA framework
says, and how defensible rental indices are built. Analysts live here too — the
SQL cookbook guidance is folded into the trust and index leaves.

## Shared context (holds for every BI leaf)

- **This feed is Ejari registered tenancy contracts** — not DLD sales, not
  short-term/holiday-home data. Scope every claim accordingly.
- **Median, never mean; always stratify.** Bulk registrations and mixed stock
  make the mean lie. The project ships medians with an `n >= 10` floor
  (`MARKET_METRICS["min_area_sample_size"]` in `lib/config.py`).
- **Bulk registrations are already flagged**, not a to-do: `_add_bulk_flag` sets
  `is_bulk_registration` for `(area, amount)` groups past `BULK_GROUP_MAX` (= 10)
  in `lib/transform/enrichment.py`.
- **PSF is guarded**: null unless `actual_area >= 200` (`_add_psf`).
- **The architecture is weekly** (`build_weekly_duckdb.py` → `rental_analytics_weekly_YYYYWxx.duckdb`),
  not cumulative layers. Cite the weekly module, never a `build_layers` module.

## Routing table

| Leaf skill | Load when the task is about |
|---|---|
| `ejari-contract-semantics` | field meanings, New vs Renewed, freehold-as-title, sub-type exclusions, duration buckets |
| `rent-figure-trust` (+analyst) | "is this rent right / publishable?", median vs mean, bulk detection, PSF gate, n≥10/IQR, safe analyst SQL |
| `dubai-market-geography` | area tiers, master projects, freehold zones, metro proximity, tier validation |
| `rera-rent-advisory` | Decree 43 rent-increase bands, RERA Rental Index, New vs Renewed caps (descriptive, not legal advice) |
| `rental-index-construction` (+analyst) | building the Area Median / Standard Lease indices, stratified-median vs hedonic, index SQL |

## Handoff map (cross-skill contracts)

- `ejari-contract-semantics` → `dubai-market-geography` (field meaning feeds tiering)
- `rent-figure-trust` → `rental-index-construction` (trusted rows feed the index)
- `data-quality-gates` (Engineering) → `rent-figure-trust` (QA enforces the publish rule BI defines)

## References

- `lib/config.py` — `MARKET_METRICS` — the n≥10 sample-size floor lives here.
- `lib/transform/enrichment.py` — `BULK_GROUP_MAX` — the bulk-group bound shared across BI leaves.

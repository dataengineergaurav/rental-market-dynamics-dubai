---
inclusion: manual
name: dubai-market-geography
description: How Dubai's geography maps to rent — master-project/developer tiers, freehold zones, metro proximity, and why the hardcoded AREA_CLASSIFICATIONS is a placeholder that must be validated against live medians. Load for any area-tier or location claim.
owner: "@dataengineergaurav"
department: business-intelligence
last-verified: 2026-10-24
---

# Dubai Market Geography

Owns the spatial model: how area, master project, freehold zone, and metro
proximity relate to rent — and the caveat that the shipped tiering is a
placeholder.

## Contract

- **owns:** area tiering, master-project/zone/metro reasoning, tier validation.
- **hands-off-to:**
  - `rent-figure-trust` — a tier claim still needs the n≥10 / bulk screen.
- **routes-from:**
  - `business-intelligence/_department` — any area/tier/location task.
  - `ejari-contract-semantics` — the area/project field meanings feed this leaf.

## Triggers

Area tier, Premium/Mid/Budget/Emerging, master project, freehold zone, metro
proximity, `AREA_CLASSIFICATIONS`, `get_area_tier`, "why is this area mid-tier".

## Core knowledge (verified)

- **`AREA_CLASSIFICATIONS` is a placeholder.** It is a hardcoded dict of ~30
  areas in `lib/config.py`; `get_area_tier(area_name)` returns `MID_TIER` for
  anything not listed. So most areas in the feed resolve to Mid-Tier by default,
  and `_add_area_tier` stamps that default. Do NOT trust a tier value as a
  market statement until it is validated against live medians.
- **Real tiers are developer/master-project driven.** `master_project_en`
  (Emaar Downtown/Burj Khalifa, Nakheel Palm) + freehold zones + infrastructure
  (metro proximity) explain price far better than the placeholder map.
- **Tier validation.** Validate a tier against the trusted area medians (from
  `rent-figure-trust`): a `Premium` area should show a high median on comparable,
  bulk-excluded stock; a `Budget` area a low one. If every area returns
  `Mid-Tier`, tiering is unvalidated — say so.
- **Freehold is title, not tier** (see `ejari-contract-semantics`): an
  investment-zone flag is not a premium.

## Gates

- Never present a `get_area_tier` result as validated truth — it defaults to Mid-Tier.
- A tier claim must be backed by a trusted (n≥10, bulk-excluded) median.

## References

- `lib/config.py` — `AREA_CLASSIFICATIONS` — the placeholder tier map.
- `lib/config.py` — `get_area_tier` — defaults unknown areas to Mid-Tier.
- `lib/config.py` — `AreaTier` — the tier enum (Premium/Mid-Tier/Budget/Emerging).
- `lib/config.py` — `DLD_SCHEMA` — where `master_project_en` / `area_name_en` are named.
- `lib/transform/enrichment.py` — `_add_area_tier` — stamps the (placeholder) tier.

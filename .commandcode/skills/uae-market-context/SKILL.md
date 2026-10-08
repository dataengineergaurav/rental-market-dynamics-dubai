---
name: uae-market-context
description: Ground Dubai/UAE rental-market analysis in current domain reality — Ejari/RERA/DLD context, the area taxonomy, seasonality, rent-increase regulation, and the metric contracts every Gold view must follow. Use when analyzing Dubai rents, adding market metrics or Gold views, interpreting area medians, or answering "what is happening in the Dubai rental market".
when_to_use: Any task that interprets or extends the Dubai rental market outputs — reading `gold_*`/`Agg*` views, adding analytics, classifying areas, explaining a rent move, or briefing a client/agent on market conditions.
---

# UAE market context — Dubai rental analytics

Framing for anyone (human or model) reading or extending this repo's market outputs. The pipeline
is the **Ejari rent registration** feed only; sales, title deeds, and short-term stays are out of
scope. Treat every number as a *registered lease* number, not a listing or a sale price.

## When to use this skill

- Interpreting a `gold_*` / `Agg*` view or a `property_usage_*.csv` figure.
- Adding or changing a market metric, area classification, or Gold view.
- Answering "what's happening in the Dubai rental market" from this data.
- Reviewing whether an analysis claim is grounded in what Ejari actually measures.

## Step 0 — read the in-repo sources of truth first

Do not answer from memory. Load, in order:

1. `README.md` — layer semantics (bronze CSV → silver tables + gold views in one DuckDB).
2. `lib/config.py` — `AREA_CLASSIFICATIONS`, `psf_band_filter`, `VALIDATION_THRESHOLDS`; the only
   place area tiers and PSF bands are defined.
3. `lib/analysis/gold_layer.py` — the current views and their exclusion rules.
4. `docs/adr/0010-cumulative-combined-layers-duckdb.md` — why layers are cumulative + keyed.
5. `docs/LIBRARY_USAGE_GUIDE.md` — `MarketAnalytics` / enrichment API.

## Metric contracts (non-negotiable)

Every figure you report or add must respect what the pipeline already guarantees. See
[references/metric-contracts.md](references/metric-contracts.md) for the full set; the invariants:

- **Medians over means** for headline rent. A mean is distorted by bulk registrations and outliers.
- **Every aggregate carries `n`.** A median without its sample size is not a trustworthy headline.
  `gold_area_median` already enforces `HAVING count(*) >= 10`.
- **Exclusions:** Hotel / Labor Camps sub-types, `Virtual Unit` property types, and
  `is_bulk_registration = true` are out of rent medians.
- **PSF** comes from Silver's `rent_per_sqft` (null unless area ≥ 200 sqft), never recomputed; and
  band-filtered with `lib.config.psf_band_filter`.
- **Views resolve in-file** on a plain `duckdb.connect` — no `ATTACH`, no catalog alias.

## Grounding the analysis (UAE domain)

Read [references/uae-rental-market.md](references/uae-rental-market.md) for the domain pack. Key
points that change how a number should be read:

- **What a row is.** An Ejari record is a *registered tenancy contract*, keyed on `contract_id`.
  Renewals, new leases, and multi-property blocks can all appear; `is_bulk_registration` filters the
  last. A rent *level* here is a contract value, not a residential asking rent.
- **Registration vs move-in.** Use `contract_registration_date` for "when the market moved"; the
  contract start date can differ. The cumulative store's freshness is `_meta.data_through`.
- **Seasonality** (school-year cycles, summer expat turnover, Ramadan) shifts *volume* and the mix
  of contract types even when levels are flat — read `AggMonthlyRegistrations` as volume, not price.
- **Metro / community premium** is what `AggMetroPremium` measures against a city median.
- **Regulation** (RERA Smart Rental Index, rent-increase caps, notice periods) shapes renewals —
  verify current caps against official DLD/RERA sources before quoting; they change.

## Keeping it *current* (do not quote stale figures)

This repo intentionally does not hard-code benchmark numbers. When a task needs "current" market
context:

1. Prefer the repo's own latest `_meta.data_through` and the day's release artifacts.
2. For external benchmarks, fetch from official/first-party sources (Dubai Land Department / Dubai
   Pulse open data, RERA Rental Index) and reconcile against `gold_area_median` / `AggAreaRentStats`
   — always carrying `n` and the as-of date.
3. State the as-of date on every external figure. Never present a remembered number as current.

## Common mistakes

- Reporting a mean where a median belongs, or a median with no `n`.
- Quoting a residential asking rent from an Ejari contract number (or vice versa).
- Reading flat levels from `AggMonthlyRegistrations` (it is a volume series).
- Editing `AREA_CLASSIFICATIONS` to add an area without a test, or assuming an unknown area is
  Premium — `enrich_rent_contracts` defaults unknown areas to `Mid-Tier`.
- Adding a per-sqft figure that bypasses the 200 sqft floor or the shared PSF band.

## References

- [references/uae-rental-market.md](references/uae-rental-market.md) — domain pack (Ejari/RERA/DLD,
  seasonality, regulation, area tiers).
- [references/metric-contracts.md](references/metric-contracts.md) — Gold view contracts and
  current view inventory.

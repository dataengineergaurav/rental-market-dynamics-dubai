---
inclusion: manual
name: rent-figure-trust
description: The publish-decision skill — "can I trust/publish this rent figure?" Covers median-not-mean, bulk-registration detection, the PSF sanity gate, n≥10/IQR, and folds in safe analyst SQL against the Gold views. Load for any claim about a rent number.
owner: "@dataengineergaurav"
department: business-intelligence
last-verified: 2026-10-24
---

# Rent-Figure Trust (+ analyst)

Owns the question every rent number must pass: is this trustworthy enough to
publish? Analyst guidance is folded in here because a safe query is a trust
decision.

## Contract

- **owns:** the publish/trust decision for any rent figure; safe analyst SQL patterns.
- **hands-off-to:**
  - `rental-index-construction` — trusted rows feed the index builders.
- **routes-from:**
  - `business-intelligence/_department` — any "is this rent right/publishable" task.
  - `ejari-contract-semantics` — exclusions defined there are applied here.
  - `data-quality-gates` (Engineering) — that leaf ENFORCES what this leaf defines.

## Triggers

"Is this rent right?", publish a number, median vs mean, bulk registration,
PSF too high, n too small, outliers, analyst query, Gold view SQL.

## The trust checklist (run on every rent claim)

1. **State scope.** "Ejari registrations, window X, ex-Hotel/Labor/Virtual, n=Y."
2. **Median, never mean.** Bulk blocks and mixed stock make the mean lie. Quote
   the median; if you must show the mean, show both and explain the gap.
3. **Screen bulk.** `is_bulk_registration` is already set by `_add_bulk_flag` for
   `(area, amount)` groups past `BULK_GROUP_MAX` (= 10). Exclude bulk before
   quoting an area figure. (It is live code, not a to-do.)
4. **n ≥ 10.** Require `MARKET_METRICS["min_area_sample_size"]` rows per area with
   an IQR reported. Fewer rows → do not quote an area median.
5. **PSF gate.** PSF is null unless `actual_area >= 200` (`_add_psf`) AND within
   the band in `VALIDATION_THRESHOLDS` (residential 20–500, commercial 30–800).
   A row that fails is nulled, not published.
6. **Comparables only.** Never average Labor Camps into residential; never compare
   across `PROP_SUB_TYPE_EN` without stratifying.

## Analyst SQL (folded in) — safe Gold queries

The shipping Gold views already apply the exclusions. Query them, not raw stock:

```sql
-- area medians, already excluding Hotel/Labor/Virtual and bulk groups
SELECT area_name_en, n, median_rent
FROM gold_area_median
WHERE n >= 10
ORDER BY median_rent DESC;
```

Run through `uv`: `uv run python -c "import duckdb; ..."` against the weekly
`rental_analytics_weekly_YYYYWxx.duckdb`. Never hand-roll an area median off raw
rows — it will double-count the 2-day window and include bulk blocks.

## Gates

- Reject any area median with `n < 10`.
- Reject any PSF that bypasses the `area >= 200` guard or sits outside the band.
- Reject a Naif/Hor-style figure quoted without bulk exclusion.

## References

- `lib/transform/enrichment.py` — `_add_bulk_flag` — sets `is_bulk_registration`.
- `lib/transform/enrichment.py` — `BULK_GROUP_MAX` — the bulk-group bound (=10).
- `lib/transform/enrichment.py` — `_add_psf` — the PSF publish guard.
- `lib/config.py` — `VALIDATION_THRESHOLDS` — PSF/size/rent bands.
- `lib/config.py` — `MARKET_METRICS` — the n≥10 floor.
- `lib/classes/market_analytics.py` — `analyze_by_area` — area stats with the n floor.
- `lib/analysis/wow_movement.py` — `compute_movement` — week-over-week movement; gated n≥10 both weeks (see `rental-index-construction`).

# Gold view & metric contracts

The rules every metric in this repo must satisfy, plus the current view inventory. Keep this in
sync with `lib/analysis/gold_layer.py` and `lib/config.py` — those files are authoritative; this is
the checklist you apply.

## Invariants

1. **Median, not mean, for a headline rent.** Report `mean_rent`/`avg_rent` only alongside the
   median and `n`, never as the headline.
2. **Every aggregate carries `n`.** A median without its sample size is not publishable.
   Area medians additionally require `HAVING count(*) >= 10`.
3. **Exclusions from rent medians:**
   - `DimPropertyType.ejari_property_sub_type_en NOT IN ('Hotel', 'Labor Camps')`
   - `ejari_property_type_en != 'Virtual Unit'`
   - `FctContract.is_bulk_registration = false`
   - `annual_amount IS NOT NULL AND annual_amount > 0`
4. **PSF** reads Silver's `price_per_sqft` / `rent_per_sqft` — never recomputed in a view. It is
   null unless the unit is ≥ 200 sqft, and is band-filtered via `lib.config.psf_band_filter`
   (Residential band 20–500 AED/sqft). `psf_band_filter` is the single publishing rule; do not
   introduce a second copy.
5. **Grain.** `FctContract` is one row per registration (`contract_id` PK). Aggregate over it, not
   over re-expanded multi-property blocks, so a block is not counted twice.
6. **Views resolve in-file.** Every view selects from Silver tables in the same DuckDB file on a
   plain `duckdb.connect(path)` — no `ATTACH`, no catalog alias.
7. **Freshness.** `_meta.data_through` is the store's as-of date; `MAX_REGISTRATION_LAG_DAYS = 1`
   is the staleness trust check. Report the as-of date with any metric.

## Current Gold views (`lib/analysis/gold_layer.py`)

| View | Grain | Headline metric | Notes |
|------|-------|-----------------|-------|
| `gold_area_median` | area | `median_rent` + `n` | exclusions + `n >= 10` |
| `gold_standard_lease` | city | `median_rent`, `mean_rent`, `n` | Flat, 180–365 day leases |
| `gold_top_metros_daily` | metro × day | `number_of_rent_contracts`, `contract_rank` | top 3 metros/day |
| `AggAreaRentStats` | area | `median_rent`, `p10_rent`, `p90_rent`, `median_price_per_sqft` | spread view |
| `AggMetroPremium` | metro | `median_rent`, `premium_vs_city_pct` | vs city median (median-based) |
| `AggMonthlyRegistrations` | month | `n_contracts`, `total_annual_value` | **volume** trend, all history |
| `AggProjectRentStats` | project | `median_rent`, `n` | buildings/projects |

## Adding a view — checklist

- [ ] Written in `lib/analysis/gold_layer.py`, appended to `GOLD_VIEW_SQL`.
- [ ] Carries `n` and uses `median()` for the headline.
- [ ] Applies the exclusions in rule 3 when it quotes a rent.
- [ ] Resolves on a plain `duckdb.connect` (no `ATTACH`).
- [ ] Covered by a test in `tests/` (see `tests/test_gold_indexes.py`).
- [ ] README Gold-layer table updated if the view is user-facing.

# Analyst Cookbook

How to turn `rents_layers.duckdb` into a market read — and how to do it without fooling
yourself.

This is a working guide, not a reference. Every recipe is a real question with a real query
against the shipped Gold views. The rules underneath them (the **metric contracts**) are
non-negotiable; the queries are just the easy part.

## Ground rules before you quote a number

These are the same invariants the code enforces. Break one and your number is wrong even if the
SQL runs.

1. **Median, not mean.** A headline rent is a median. If you show a mean, show it beside the
   median and `n`.
2. **Every aggregate carries `n`.** A median without its sample size is not a finding. Area
   medians additionally require `n >= 10`.
3. **Respect the exclusions.** Rent medians drop Hotel, Labor Camps, Virtual Units,
   bulk registrations and non-positive rents. The Gold views already do this — do not
   re-expand a raw `FctContract` scan and forget them.
4. **PSF is read, never recomputed.** Use `price_per_sqft`; it is already null below 200 sqft.
   Apply `lib.config.psf_band_filter` (Residential 20–500) before quoting a per-sqft figure.
5. **Aggregate over `FctContract`, not re-expanded blocks.** It is one row per registration, so
   a multi-property contract is counted once.
6. **Separate level, volume and spread.** A spike in registration *volume* is not a rent
   increase. A wide `p10`–`p90` is not a high median.
7. **Always report the as-of date.** That is `_meta.data_through`.

## Open the store

One file, no `ATTACH`:

```python
import duckdb

con = duckdb.connect("rents_layers.duckdb", read_only=True)

print(con.execute("SELECT * FROM _meta").fetchall())
# built_at, data_from, data_through, total_contracts, last_ingested_window

print(con.execute("SELECT * FROM gold_area_median ORDER BY median_rent DESC LIMIT 10").fetchall())
```

Or from the shell, `lib.analysis.layers.connect_layers(path)` opens it read-only, and
`python -m lib.analysis.metro_volume --db rents_layers.duckdb` is a worked example.

## The questions you can actually answer

| Question | View | Watch out for |
|----------|------|---------------|
| Typical rent in an area, and on how many contracts? | `gold_area_median` | `n >= 10`; exclusions applied |
| How wide is the spread in an area? | `AggAreaRentStats` | `p10`/`p90`, not just the median |
| What's the benchmark "standard lease"? | `gold_standard_lease` | Flat, 180–365 days only |
| How much do metro-adjacent contracts cost vs the city? | `AggMetroPremium` | Median-based, vs city median |
| What's the median rent in a specific building? | `AggProjectRentStats` | Non-null project names only |
| When do registrations peak? | `AggMonthlyRegistrations` | **Volume**, not price |
| Which metros are hottest day to day? | `gold_top_metros_daily` | Top 3 per day |
| Segment by rooms / parking / freehold / luxury? | `FctContract` | Re-apply exclusions yourself |

## Recipes

### 1. Rank areas by typical rent (with sample size)

```sql
SELECT area_name_en, n, ROUND(median_rent) AS median_rent_aed
FROM gold_area_median
ORDER BY median_rent DESC
LIMIT 15;
```

Read `n` before you read the rent. An area with `n = 11` is a hint; `n = 4000` is a market.

### 2. See the spread, not just the middle

```sql
SELECT area_name_en, n, ROUND(median_rent) AS median,
       ROUND(p10_rent) AS p10, ROUND(p90_rent) AS p90,
       ROUND(median_price_per_sqft) AS median_psf
FROM AggAreaRentStats
WHERE area_name_en IN ('Dubai Marina', 'Jumeirah Village Circle', 'International City')
ORDER BY area_name_en;
```

A tight `p10`–`p90` and a wide one can share a median; the spread is the story.

### 3. The metro premium

```sql
SELECT nearest_metro_en, n,
       ROUND(median_rent)      AS median_rent,
       ROUND(city_median_rent) AS city_median,
       premium_vs_city_pct
FROM AggMetroPremium
WHERE n >= 50
ORDER BY premium_vs_city_pct DESC
LIMIT 15;
```

This is a *median* premium against the city median, not a mean PSF premium. `n` tells you
whether to trust it.

### 4. Building-level pricing

```sql
SELECT project_name_en, n, ROUND(median_rent) AS median_rent
FROM AggProjectRentStats
WHERE n >= 30
ORDER BY median_rent DESC
LIMIT 20;
```

### 5. The city benchmark

```sql
SELECT n, ROUND(median_rent) AS standard_lease_median
FROM gold_standard_lease;
```

One row: what a normal Dubai *Flat* lease (180–365 days) typically costs. Use it as the
denominator in "how does area *X* compare to the city".

### 6. Volume and seasonality over all history

```sql
SELECT month, n_contracts, ROUND(total_annual_value) AS total_annual_value
FROM AggMonthlyRegistrations
ORDER BY month;
```

`n_contracts` is the volume signal. Expect a summer/expat-turnover peak; remember new-supply
handovers raise the count even when rents soften. **Never read a rent level from this view.**

### 7. Daily metro hotspots

```sql
SELECT * FROM gold_top_metros_daily
WHERE contract_reg_date = (SELECT MAX(contract_reg_date) FROM gold_top_metros_daily)
ORDER BY contract_rank;
```

### 8. Segmented queries (on the fact)

For anything the views don't cover, query `FctContract` directly — and re-apply the contracts:

```sql
SELECT rooms,
       COUNT(*)                       AS n,
       ROUND(MEDIAN(annual_amount))   AS median_rent
FROM FctContract f
WHERE f.is_bulk_registration = false
  AND f.annual_amount > 0
  AND f.ejari_property_sub_type_en NOT IN ('Hotel', 'Labor Camps')
  AND f.ejari_property_type_en <> 'Virtual Unit'
GROUP BY rooms
HAVING COUNT(*) >= 10
ORDER BY rooms;
```

## The investor lens (and its honest limit)

The `dubai-real-estate-investor` skill frames this properly: **this repository supplies the
rent leg only.** There are no sale prices here, so **a yield cannot be computed from this repo
alone**. A gross yield needs an external capital value; a net yield also needs service charges,
vacancy, management and fees.

What you *can* build from the store:

- rank areas by rent level and spread,
- quantify the metro premium and building-level pricing,
- supply the standard-lease benchmark,
- read registration volume and seasonality.

What you must bring from outside: DLD sales prices, Dubai Pulse figures, RERA Rental Index,
service charges and vacancy. Always reconcile an external number to the store's **as-of date**,
and state it.

## Traps that have actually bitten

- **`DataFrame.equals()` in Polars compares values, not dtypes** — a UInt32/Int64 drift is
  invisible. Assert `frame.schema` when the dtype is the thing under test.
- **`pl.DataFrame(records)` infers from the first 100 rows only** — a field null there and a
  string later raises. The Silver builder pins `infer_schema_length=None`.
- **Volume is not price.** `AggMonthlyRegistrations` and any `COUNT(*)` answer "how many", not
  "how much".
- **A regulated flat median can be regulation, not a soft market.** Tiered rent-increase caps
  hold medians down; read the cap before calling a flat market.
- **Bulk registrations inflate medians.** A mass-landlord filing (e.g. 87 identical amounts)
  can drag an area; the `is_bulk_registration` flag and the view exclusions exist for this.

## Automate it

The `market-analyst` agent (`.commandcode/agents/market-analyst.md`) does exactly this
workflow: grounds via `uae-market-context`, reads `_meta.data_through` first, queries the Gold
views, and reports every figure with its `n` and as-of date. Ask it a market question the way
you'd ask a colleague.

For the domain truth behind these rules — Ejari/RERA/DLD, seasonality, regulation, the area
taxonomy — read the `uae-market-context` skill and its references.

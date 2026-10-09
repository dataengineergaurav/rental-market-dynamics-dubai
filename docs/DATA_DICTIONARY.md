# Data Dictionary

Every table, view, column, code and constant in the pipeline, in one place.

Sources of truth (this document mirrors them; if they disagree, the code wins):
[`lib/config.py`](../lib/config.py) · [`lib/analysis/silver_layer.py`](../lib/analysis/silver_layer.py) ·
[`lib/analysis/gold_layer.py`](../lib/analysis/gold_layer.py) ·
[`lib/classes/silver_contract.py`](../lib/classes/silver_contract.py) ·
[`lib/transform/enrichment.py`](../lib/transform/enrichment.py).

There are three schemas to keep straight:

1. the **raw payload** (44 text columns, as the endpoint returns them) — the Bronze CSV;
2. the **Silver tables** (normalized, typed, contract-grain) — inside `rents_layers.duckdb`;
3. the **Gold views** (analytics) — also inside `rents_layers.duckdb`.

---

## 1. The raw payload (Bronze)

`rent_contracts_YYYYMMDD.csv`. **44 columns**, untyped text, pinned in
`lib/config.RAW_RENTS_CSV_DTYPES` so per-file inference can never flip a type. It is pinned
*whole* — all 44, including the Arabic and the fully-null columns — not just the columns that
have bitten (ADR-09). Adding a payload column without pinning it fails
`tests/test_raw_schema.py`.

| Column | Pinned as | Notes |
|--------|-----------|-------|
| `RN` | Int64 | Gateway row ordinal. Used as the `contract_id` fallback and in `record_id`. |
| `DEFAULT_SORT` | Utf8 | Query echo, not data. |
| `TOTAL` | Int64 | Per-response batch count — constant *within* a file, differs *between* files. |
| `TOTAL_PROPERTIES` | Int64 | How many properties this contract covers (`1` = single). |
| `IS_FREE_HOLD_EN` / `IS_FREE_HOLD_AR` | Utf8 | Text form; the boolean `IS_FREE_HOLD` is authoritative. `_AR` is dropped. |
| `VERSION_EN` / `VERSION_AR` | Utf8 | Contract version label. `_AR` is 100% `?`-masked upstream (dropped). |
| `REGISTRATION_DATE` | Utf8 | ISO datetime text; parsed downstream. **This is the market-activity date.** |
| `START_DATE` / `END_DATE` | Utf8 | ISO datetime text; tenancy start / end. |
| `AREA_ID` | Int64 | Single-valued (`0`) — dropped. |
| `AREA_EN` / `AREA_AR` | Utf8 | Area / community name. `AREA_EN` → `area_name_en`. |
| `CONTRACT_AMOUNT` | Float64 | Total contract value. |
| `ANNUAL_AMOUNT` | Float64 | Annual rent. **The headline measure.** |
| `IS_FREE_HOLD` | Int64 | `1`/`0` → boolean `is_free_hold`. |
| `ACTUAL_AREA` | Float64 | Unit area in **square feet** (see [note](#area-units)). |
| `EJARI_PROPERTY_TYPE_ID` / `EJARI_PROPERTY_SUB_TYPE_ID` / `PROPERTY_USAGE_ID` | Int64 | Single-valued (`0`) — dropped. |
| `PROP_TYPE_EN` / `PROP_TYPE_AR` | Utf8 | Property type (e.g. `Flat`, `Villa`). |
| `PROP_SUB_TYPE_EN` / `PROP_SUB_TYPE_AR` | Utf8 | Sub-type (e.g. `Flat`, `Hotel`, `Labor Camps`). |
| `ROOMS` | Int64 | Room count. |
| `USAGE_EN` / `USAGE_AR` | Utf8 | Usage (e.g. `Residential`, `Commercial`). `USAGE_EN` → `property_usage_en`. |
| `NEAREST_METRO_EN` / `NEAREST_METRO_AR` | Utf8 | Nearest metro station. `_EN` → `nearest_metro_en`. |
| `NEAREST_MALL_EN` / `NEAREST_MALL_AR` | Utf8 | Nearest mall (carried by the row contract, not published to the fact). |
| `NEAREST_LANDMARK_EN` / `NEAREST_LANDMARK_AR` | Utf8 | Nearest landmark (as above). |
| `PARKING` | Int64 | ~98% null; cast to boolean `has_parking` (null → `false`). |
| `PROJECT_EN` / `PROJECT_AR` | Utf8 | Building/project name. `_EN` → `project_name_en`. |
| `MASTER_PROJECT_EN` / `MASTER_PROJECT_AR` | Utf8 | Master development. Near-total null but **not** 100% — carried, not dropped. |
| `CONTRACT_NUMBER` | Utf8 | 100% null in every observed file. The `contract_id` PK falls back to `RN`. |
| `VERSION_NUMBER` | Utf8→Int64 | Constant `0` — dropped. |
| `PROPERTY_ID` | Int64 | Single-valued (`0`) — dropped. |
| `PARCEL_ID` | Utf8 | 100% null — dropped. |
| `LAND_PROPERTY_ID` | Int64 | Constant `0` — dropped. |

<a id="area-units"></a>
> **Area units.** `ACTUAL_AREA` is **square feet**, confirmed by residential flats in the
> 400–1200 range landing at ~80.6 AED/sqft. The codebase never names an area field `*_sqm`,
> and `tests/test_silver_contract.py` guards the m²/sqft 10.76× corruption.

---

## 2. Silver tables

Inside `rents_layers.duckdb`. Typed, normalized, contract-grain. Rebuilt forward daily.

### `FctContract` — one row per registration (grain: ADR-02)

Primary key: **`contract_id`** = payload `CONTRACT_NUMBER`, falling back to `RN` when null.
The PK is what makes ingest idempotent (`INSERT OR REPLACE`).

| Column | Type | Meaning |
|--------|------|---------|
| `contract_id` | VARCHAR PK | Registration key. Payload contract number, else `RN`. |
| `rn` | BIGINT | Gateway row ordinal. |
| `area_name_en` | VARCHAR | Area / community (FK → `DimArea`). |
| `area_tier` | VARCHAR | Premium / Mid-Tier / Budget / Emerging (see [tiers](#area-tiers)). |
| `ejari_property_type_en` | VARCHAR | Type (FK → `DimPropertyType`). |
| `ejari_property_sub_type_en` | VARCHAR | Sub-type (FK → `DimPropertyType`). |
| `property_usage_en` | VARCHAR | Usage (FK → `DimPropertyType`). |
| `usage_category` | VARCHAR | Residential / Commercial / Other. |
| `nearest_metro_en` | VARCHAR | Nearest metro (FK → `DimMetro`). |
| `contract_registration_date` | TIMESTAMP | Registration date — the market-activity date. |
| `contract_start_date` | TIMESTAMP | Tenancy start. |
| `contract_end_date` | TIMESTAMP | Tenancy end. |
| `contract_year` | BIGINT | Derived from start date. |
| `contract_quarter` | BIGINT | Derived from start date. |
| `contract_month` | BIGINT | Derived from start date. |
| `contract_weekday` | BIGINT | Derived from start date. |
| `contract_season` | VARCHAR | Winter / Spring / Summer / Fall. |
| `annual_amount` | DOUBLE | **Annual rent (AED) — the headline measure.** |
| `contract_amount` | DOUBLE | Total contract value. |
| `actual_area` | DOUBLE | Unit area in **sq ft**. |
| `price_per_sqft` | DOUBLE | Guarded PSF; **null** below 200 sqft or non-positive rent (see [PSF](#psf)). |
| `contract_duration_days` | BIGINT | End − start, in days. |
| `contract_duration_category` | VARCHAR | Short-term (<180) / Medium-term (<365) / Long-term (≥365) / Unknown. |
| `rooms` | BIGINT | Room count. |
| `total_properties` | BIGINT | Properties in the contract block. |
| `has_parking` | BOOLEAN | From `PARKING` (null → false). |
| `is_free_hold` | BOOLEAN | Freehold flag. |
| `is_luxury` | BOOLEAN | Top ~25% by PSF **or** top ~20% by rent (percentile flags). |
| `is_bulk_registration` | BOOLEAN | Same (area, amount) repeated **> 10** times. |
| `project_name_en` | VARCHAR | Building / project. |
| `master_project_en` | VARCHAR | Master development (near-total null). |

### `DimArea`

Rebuilt from the fact every run (`SELECT ... GROUP BY area_name_en`), so it can never drift.

| Column | Type | Meaning |
|--------|------|---------|
| `area_name_en` | VARCHAR | Area name. |
| `area_tier` | VARCHAR | Market tier. |

### `DimPropertyType`

A `SELECT DISTINCT` of four fact columns:

| Column | Type |
|--------|------|
| `ejari_property_type_en` | VARCHAR |
| `ejari_property_sub_type_en` | VARCHAR |
| `property_usage_en` | VARCHAR |
| `usage_category` | VARCHAR |

### `DimMetro`

| Column | Type | Meaning |
|--------|------|---------|
| `nearest_metro_en` | VARCHAR | Metro name (non-null only). |

### `_meta` — one row describing the current store

Not a run log: the current state of the cumulative store.

| Column | Type | Meaning |
|--------|------|---------|
| `built_at` | TIMESTAMP | When the file was last built. |
| `data_from` | DATE | Earliest registration date held. |
| `data_through` | DATE | **Latest registration date held — the as-of date.** A stalled feed shows here. |
| `total_contracts` | BIGINT | Row count of `FctContract`. |
| `last_ingested_window` | VARCHAR | e.g. `2026-10-04..2026-10-05`. |

---

## 3. Gold views

Seven views, all in `lib/analysis/gold_layer.py`, all selecting from the Silver tables in the
**same file** (a plain `duckdb.connect` resolves them). Contract: every aggregate carries `n`,
the headline is a median, and rent medians apply the market exclusions
(Hotel / Labor Camps / Virtual Unit / bulk / non-positive rent).

### `gold_area_median` — per-area headline rent

**Columns:** `area_name_en`, `n`, `median_rent`, `mean_rent`, `min_rent`, `max_rent`
**Guards:** excludes Hotel / Labor Camps sub-types, Virtual Unit type, bulk stock, non-positive
rent; `HAVING n >= 10`; ordered by `n` desc. This is the canonical "what does it cost in *X*" view.

### `gold_standard_lease` — one city-wide benchmark

A **single row**: the Dubai-wide "standard lease" — a **Flat**, **180–365 days**, excluding
Virtual Unit and bulk.
**Columns:** `n`, `median_rent`, `mean_rent`

### `gold_top_metros_daily` — metro hotspots by volume

Top **3** metros per registration day, by contract count.
**Columns:** `nearest_metro`, `contract_reg_date`, `number_of_rent_contracts`, `contract_rank`
(`contract_rank <= 3`)

### `AggAreaRentStats` — spread, not just a point

**Columns:** `area_name_en`, `n`, `avg_rent`, `median_rent`, `p10_rent`, `p90_rent`,
`avg_price_per_sqft`, `median_price_per_sqft`
No `HAVING` — all stock. Use `p10`/`p90` to see dispersion. PSF is read from Silver, never
recomputed.

### `AggMetroPremium` — metro vs city

**Columns:** `nearest_metro_en`, `n`, `median_rent`, `city_median_rent`,
`premium_vs_city_pct`
The premium is `round(100 * (median_rent − city_median_rent) / city_median_rent, 2)`, where the
city baseline is the median over all deduplicated contracts. Median-based, not mean-PSF.

### `AggMonthlyRegistrations` — the trend view

**Columns:** `month` (`YYYY-MM`), `n_contracts`, `total_annual_value`
One row per month **of all accumulated history**. This is a **volume** series —
`total_annual_value` is a sum, not a rent level. Never quote a level from it.

### `AggProjectRentStats` — building-level

**Columns:** `project_name_en`, `n`, `median_rent`, `mean_rent`
Non-null project names only.

---

## 4. Derived columns

### Added by enrichment (`lib/transform/enrichment.py`)

These feed `FctContract` (some are not published to the fact, e.g.
`property_type_normalized`).

| Column | Rule |
|--------|------|
| `price_per_sqft` | `psf_expression` — null unless `annual_amount > 0` and `actual_area >= 200`. |
| `area_tier` | `AREA_CLASSIFICATIONS` lookup; unknown → `Mid-Tier`. |
| `property_type_normalized` | lowercase → `PROPERTY_TYPE_MAPPINGS` → title-case fallback. |
| `contract_year/quarter/month/weekday` | From `contract_start_date`. |
| `contract_season` | Month → Winter (12,1,2) / Spring (3,4,5) / Summer (6,7,8) / Fall. |
| `contract_duration_days` | `contract_end_date − contract_start_date`. |
| `contract_duration_category` | Short-term (<180) / Medium-term (<365) / Long-term (≥365) / Unknown. |
| `is_luxury` | `price_per_sqft >= p75` **or** `annual_amount >= p80`. |
| `usage_category` | Regex on usage → Residential / Commercial / Other. |
| `is_bulk_registration` | `(area_name_en, annual_amount)` count **> `BULK_GROUP_MAX` (10)**. |

### Added by the row contract (`lib/classes/silver_contract.py`)

Set inside the `mode="after"` validator; the model is frozen. A bad value is **kept**, not
dropped — it is named in `violations`.

| Field | Meaning |
|-------|---------|
| `duration_days` | End − start, when both dates parse with end after start. |
| `is_short_term` | `duration_days < 300` (`SHORT_TERM_DAYS`). |
| `monthly_rent` | `annual_amount / 12` (null if amount ≤ 0). |
| `rent_per_sqft` | `annual_amount / actual_area`, only when `actual_area >= 200`. |
| `implied_years` | `contract_amount / annual_amount`. |
| `psf_eligible` | True when the area clears the PSF floor. |
| `violations` | List of rule names this row triggered. |
| `row_hash` | Stable fingerprint (excludes `RN`) — safe across days; not unique by design. |
| `record_id` | `row_hash:RN` — unique within a file. |

<a id="psf"></a>
### PSF (price per square foot): one owner, three uses

The rule is defined **once**, in `lib/config.py`:

- `PSF_MIN_AREA_SQFT = 200` — the **reporting floor**. Below it, a per-sqft figure is
  meaningless, so the guarded division returns **null**. Deliberately equal to
  `VALIDATION_THRESHOLDS["min_property_size"]` but a *separate* constant (validity range vs
  reporting floor).
- `psf_expression()` — the guarded division (null unless amount > 0 and area ≥ 200).
- `psf_band_filter()` — the **publishing band**: Residential 20–500 AED/sqft, Commercial
  30–800. A 200+ sqft unit at 8228 AED/sqft is a real registration (Silver keeps it) that Gold
  declines to report.

`silver_contract`, `enrichment` and `market_analytics` all import these; none carries its own
`200`. `tests/test_p0_gates.py` pins the single ownership.

---

## 5. Violation codes

Attached per-row (`violations`) and tallied (`violation_counts`) by `to_silver`. `to_silver`
never raises; a row that pydantic cannot even construct is **quarantined**, not dropped, so
`len(frame) + len(quarantined) == input rows`.

| Code | Fired when |
|------|-----------|
| `annual_amount_not_positive` | `annual_amount <= 0`. |
| `annual_amount_below_min` / `annual_amount_above_max` | Outside `VALIDATION_THRESHOLDS` rent range. |
| `actual_area_negative` / `actual_area_above_max` | Outside the size range. |
| `actual_area_below_psf_floor` | `actual_area < 200` (PSF not reportable). |
| `end_before_start` | `contract_end_date <= contract_start_date`. |
| `amount_duration_mismatch` | `implied_years` disagrees with the declared duration by > 5%. |
| `merged_contract_group` | A declared block key matched more rows than declared (two contracts merged). |
| `partial_contract_capture` | Fewer rows seen than declared (window captured part of the block). |
| `masked_arabic_cell` | A `?_?` upstream word-mask was nulled on a partial Arabic column. |
| `schema:<field>` | pydantic could not construct the row (quarantined). |

> `merged_contract_group` and `partial_contract_capture` are counted **separately** — one is a
> key problem (irreducible without a contract number), the other a capture problem.

---

## 6. Constants (and who owns them)

| Constant | Value | Owner |
|----------|-------|-------|
| `PSF_MIN_AREA_SQFT` | 200 | `lib/config.py` |
| `min_rent` / `max_rent` | 10,000 / 5,000,000 AED | `VALIDATION_THRESHOLDS` |
| `min_property_size` / `max_property_size` | 200 / 50,000 sqft | `VALIDATION_THRESHOLDS` |
| `min_psf_residential` / `max_psf_residential` | 20 / 500 | `VALIDATION_THRESHOLDS` |
| `min_psf_commercial` / `max_psf_commercial` | 30 / 800 | `VALIDATION_THRESHOLDS` |
| `min_contract_days` / `max_contract_days` | 30 / 730 | `VALIDATION_THRESHOLDS` |
| `luxury_psf_percentile` / `luxury_rent_percentile` | 75 / 80 | `MARKET_METRICS` |
| `outlier_iqr_multiplier` | 3.0 | `MARKET_METRICS` |
| `min_area_sample_size` | 10 | `MARKET_METRICS` |
| `BULK_GROUP_MAX` | 10 | `lib/transform/enrichment.py` |
| `SHORT_TERM_DAYS` | 300 | `lib/classes/silver_contract.py` |
| `RECONCILE_TOLERANCE` | 0.05 | `lib/classes/silver_contract.py` |
| `DAYS_PER_YEAR` | 365.25 | `lib/classes/silver_contract.py` |
| `MAX_REGISTRATION_LAG_DAYS` | 1 | `lib/analysis/build_layers_duckdb.py` |
| `COVERAGE_FLOOR` | 75 | `Makefile` |
| `RAW_RENTS_CSV_DTYPES` / `RAW_RENTS_CSV_COLUMNS` | 44 columns | `lib/config.py` |

`lib/analysis/gold_indexes.py` holds the market-health gate bands
(`GATE_MEDIAN_FLOOR_AED = 20_000`, `GATE_MEDIAN_CEILING_AED = 500_000`,
`GATE_P95_CEILING_AED = 1_000_000`) and `MIN_AREA_ROWS = 10`. It is a
**pending-extraction, non-production** module — nothing on the live path calls it; the shipped
`gold_layer.py` views are the source of truth.

<a id="area-tiers"></a>
## 7. Area tiers

From `AREA_CLASSIFICATIONS`. **Unknown areas default to Mid-Tier, not Premium.**

| Tier | Areas |
|------|-------|
| **Premium** | Downtown Dubai, Dubai Marina, Palm Jumeirah, Emirates Hills, Jumeirah Beach Residence, Business Bay, Dubai Hills Estate, Arabian Ranches |
| **Mid-Tier** | Jumeirah Village Circle, Jumeirah Village Triangle, Dubai Sports City, Motor City, The Greens, The Views, Discovery Gardens, Mirdif |
| **Budget** | International City, Deira, Bur Dubai, Al Nahda, Al Qusais |
| **Emerging** | Dubai South, Dubailand, Dubai Production City |

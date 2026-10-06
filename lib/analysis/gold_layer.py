"""Gold layer — the analytics view DDL built on the Silver tables.

Every view selects from the Silver tables in the SAME DuckDB file (no catalog alias, no
`ATTACH`): the combined layers store carries the tables and the views together, so the views
resolve on a plain `duckdb.connect(path)`.

Contracts:
  - All rent-per-sqft figures read Silver's FctContract.price_per_sqft — never recomputed here.
  - Every aggregate carries `n`; a median without its sample size is not a trustworthy headline.
  - Hotel / Labor Camps sub-types and Virtual Unit property types are excluded from rent medians,
    as is bulk-registration stock, matching the shipped `gold_area_median` contract.
"""
from __future__ import annotations

_GOLD_AREA_MEDIAN = """
CREATE OR REPLACE VIEW gold_area_median AS
SELECT a.area_name_en, count(*) AS n,
       median(f.annual_amount) AS median_rent,
       avg(f.annual_amount) AS mean_rent,
       min(f.annual_amount) AS min_rent,
       max(f.annual_amount) AS max_rent
FROM FctContract f
JOIN DimArea a USING (area_name_en)
JOIN DimPropertyType pt USING (ejari_property_type_en, ejari_property_sub_type_en, property_usage_en)
WHERE pt.ejari_property_sub_type_en NOT IN ('Hotel','Labor Camps')
  AND pt.ejari_property_type_en != 'Virtual Unit'
  AND f.is_bulk_registration = false
  AND f.annual_amount IS NOT NULL AND f.annual_amount > 0
GROUP BY a.area_name_en
HAVING count(*) >= 10
ORDER BY n DESC
"""

_GOLD_STANDARD_LEASE = """
CREATE OR REPLACE VIEW gold_standard_lease AS
SELECT count(*) AS n,
       median(f.annual_amount) AS median_rent,
       avg(f.annual_amount) AS mean_rent
FROM FctContract f
JOIN DimPropertyType pt USING (ejari_property_type_en, ejari_property_sub_type_en, property_usage_en)
WHERE pt.ejari_property_sub_type_en = 'Flat'
  AND f.contract_duration_days >= 180 AND f.contract_duration_days <= 365
  AND pt.ejari_property_type_en != 'Virtual Unit'
  AND f.is_bulk_registration = false
"""

GOLD_TOP_METROS_DAILY_SQL = """
CREATE OR REPLACE VIEW gold_top_metros_daily AS
SELECT
    m.nearest_metro_en AS nearest_metro,
    DATE(f.contract_registration_date) AS contract_reg_date,
    COUNT(f.rn) AS number_of_rent_contracts,
    RANK() OVER (
        PARTITION BY DATE(f.contract_registration_date)
        ORDER BY COUNT(f.rn) DESC
    ) AS contract_rank
FROM FctContract f
JOIN DimMetro m USING (nearest_metro_en)
GROUP BY m.nearest_metro_en, contract_reg_date
QUALIFY contract_rank <= 3
ORDER BY contract_reg_date DESC, contract_rank ASC
"""

_AGG_AREA_RENT_STATS = """
CREATE OR REPLACE VIEW AggAreaRentStats AS
SELECT a.area_name_en,
       count(*) AS n,
       avg(f.annual_amount) AS avg_rent,
       median(f.annual_amount) AS median_rent,
       quantile_cont(f.annual_amount, 0.10) AS p10_rent,
       quantile_cont(f.annual_amount, 0.90) AS p90_rent,
       avg(f.price_per_sqft) AS avg_price_per_sqft,
       median(f.price_per_sqft) AS median_price_per_sqft
FROM FctContract f
JOIN DimArea a USING (area_name_en)
GROUP BY a.area_name_en
"""

# Median-based premium (the spec's mean-PSF `premium_vs_city_avg_pct` is not viable). The city
# baseline is the median over every deduplicated contract, with its own n exposed so the reader can
# judge the comparison.
_AGG_METRO_PREMIUM = """
CREATE OR REPLACE VIEW AggMetroPremium AS
WITH city AS (SELECT median(annual_amount) AS city_median_rent FROM FctContract)
SELECT m.nearest_metro_en AS nearest_metro_en,
       count(*) AS n,
       median(f.annual_amount) AS median_rent,
       city.city_median_rent,
       round(100.0 * (median(f.annual_amount) - city.city_median_rent) / city.city_median_rent, 2)
           AS premium_vs_city_pct
FROM FctContract f
JOIN DimMetro m USING (nearest_metro_en)
CROSS JOIN city
GROUP BY m.nearest_metro_en, city.city_median_rent
"""

# Monthly totals aggregate over the deduplicated contract grain (FctContract is one row per
# registration), so multi-property blocks are not summed more than once. Over a cumulative store
# this is the trend view: one row per month of history.
_AGG_MONTHLY_REGISTRATIONS = """
CREATE OR REPLACE VIEW AggMonthlyRegistrations AS
SELECT strftime(date_trunc('month', f.contract_registration_date), '%Y-%m') AS month,
       count(*) AS n_contracts,
       sum(f.annual_amount) AS total_annual_value
FROM FctContract f
GROUP BY month
ORDER BY month
"""

_AGG_PROJECT_RENT_STATS = """
CREATE OR REPLACE VIEW AggProjectRentStats AS
SELECT f.project_name_en,
       count(*) AS n,
       median(f.annual_amount) AS median_rent,
       avg(f.annual_amount) AS mean_rent
FROM FctContract f
WHERE f.project_name_en IS NOT NULL
GROUP BY f.project_name_en
"""

GOLD_VIEW_SQL: list[str] = [
    _GOLD_AREA_MEDIAN,
    _GOLD_STANDARD_LEASE,
    GOLD_TOP_METROS_DAILY_SQL,
    _AGG_AREA_RENT_STATS,
    _AGG_METRO_PREMIUM,
    _AGG_MONTHLY_REGISTRATIONS,
    _AGG_PROJECT_RENT_STATS,
]

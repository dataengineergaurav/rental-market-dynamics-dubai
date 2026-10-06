"""Silver layer — the normalized DuckDB table DDL.

Silver is the cleaned, valid, contract-grain layer: one row per registration in `FctContract`,
with the repeated descriptive text pushed into small natural-key dimensions. It is built from the
pooled, deduped, enriched frame that `weekly_pool.pool_and_enrich` registers on the connection as
`enriched_df`.

Shapes follow the Silver design spec §3.7 corrections:

  - Keys are NATURAL, not the upstream `*_ID` columns — those are single-valued in the payload
    (every row 0), so keying on them would materialise one-row dimensions. `DimArea` keys on
    `area_name_en`, `DimPropertyType` on (type, sub_type, usage), `DimMetro` on `nearest_metro_en`.
  - `nearest_metro` lives on the FACT (as `metro_key`), not on `DimArea`: 77 of 161 areas carry
    more than one metro, so it is not a 1:1 area attribute.
  - Every aggregate built on top of this (see gold_layer) carries `n`.

Enrichment stays here: `is_bulk_registration`, the area tier, duration and PSF columns are already
on `enriched_df`, and the Gold marts depend on them.
"""
from __future__ import annotations

# Ordered so dimensions exist before the fact that references them.
FACT_TABLE = "FctContract"
DIMENSION_TABLES = ("DimArea", "DimPropertyType", "DimMetro")

DIM_AREA_SQL = """
CREATE OR REPLACE TABLE DimArea AS
SELECT ROW_NUMBER() OVER (ORDER BY area_name_en) AS area_key,
       area_name_en,
       any_value(area_tier) AS area_tier
FROM enriched_df
WHERE area_name_en IS NOT NULL
GROUP BY area_name_en
"""

DIM_PROPERTY_TYPE_SQL = """
CREATE OR REPLACE TABLE DimPropertyType AS
SELECT ROW_NUMBER() OVER (
           ORDER BY ejari_property_type_en, ejari_property_sub_type_en, property_usage_en
       ) AS property_type_key,
       ejari_property_type_en,
       ejari_property_sub_type_en,
       property_usage_en,
       any_value(usage_category) AS usage_category
FROM enriched_df
GROUP BY ejari_property_type_en, ejari_property_sub_type_en, property_usage_en
"""

DIM_METRO_SQL = """
CREATE OR REPLACE TABLE DimMetro AS
SELECT ROW_NUMBER() OVER (ORDER BY nearest_metro_en) AS metro_key,
       nearest_metro_en AS nearest_metro_en
FROM enriched_df
WHERE nearest_metro_en IS NOT NULL
GROUP BY nearest_metro_en
"""

# Keep the fact narrow and canonical: measures + foreign keys + the temporal attributes that are
# not worth a dimension yet. `record_id` mirrors the Silver parquet's `row_hash:RN` contract.
FACT_SQL = """
CREATE OR REPLACE TABLE FctContract AS
SELECT
    f.row_hash || ':' || CAST(f.RN AS VARCHAR) AS record_id,
    f.row_hash,
    f.RN AS rn,
    da.area_key,
    dpt.property_type_key,
    dm.metro_key,
    CAST(f.contract_registration_date AS TIMESTAMP) AS contract_registration_date,
    f.contract_start_date,
    f.contract_end_date,
    f.contract_year,
    f.contract_quarter,
    f.contract_month,
    f.contract_weekday,
    f.contract_season,
    f.annual_amount AS annual_amount,
    f.contract_amount AS contract_amount,
    f.actual_area AS actual_area,
    f.price_per_sqft,
    f.contract_duration_days,
    f.contract_duration_category,
    f.ROOMS AS rooms,
    f.TOTAL_PROPERTIES AS total_properties,
    CAST(f.PARKING AS BOOLEAN) AS has_parking,
    CAST(f.IS_FREE_HOLD AS BOOLEAN) AS is_free_hold,
    f.is_luxury,
    f.is_bulk_registration,
    f.project_name_en,
    f.master_project_en AS master_project_en
FROM enriched_df f
LEFT JOIN DimArea da USING (area_name_en)
LEFT JOIN DimPropertyType dpt USING (ejari_property_type_en, ejari_property_sub_type_en, property_usage_en)
LEFT JOIN DimMetro dm USING (nearest_metro_en)
"""

SILVER_TABLE_SQL: list[str] = [
    DIM_AREA_SQL,
    DIM_PROPERTY_TYPE_SQL,
    DIM_METRO_SQL,
    FACT_SQL,
]


def meta_sql(pooled) -> str:
    """The `_meta` provenance table — the operator's only view of what the build was made of.

    Keeps `pooled_rows` (pre-dedup) next to `deduped_rows` so the collapsed cross-file duplicates
    are visible, not inferred from a lower-than-expected count.
    """
    data_through_sql = (
        "NULL" if pooled.data_through_date is None else f"'{pooled.data_through_date.isoformat()}'"
    )
    return (
        "CREATE OR REPLACE TABLE _meta AS SELECT "
        f"'{pooled.week}' AS week, "
        f"'{pooled.start}' AS week_start, "
        f"'{pooled.end}' AS week_end, "
        f"{pooled.pooled_rows} AS pooled_rows, "
        f"{pooled.frame.height} AS deduped_rows, "
        f"{pooled.duplicates_removed} AS row_hash_duplicates_removed, "
        f"{len(pooled.files)} AS daily_files, "
        f"{pooled.expected_days} AS expected_daily_files, "
        f"{len(pooled.missing)} AS missing_daily_files, "
        f"{data_through_sql} AS data_through"
    )

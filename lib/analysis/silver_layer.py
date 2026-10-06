"""Silver layer — the normalized DuckDB table DDL for the cumulative layers store.

Silver is the cleaned, valid, contract-grain layer: one row per registration in `FctContract`,
with the repeated descriptive text pushed into natural-key dimensions. It is built from the
enriched daily increment that `build_layers_duckdb` registers on the connection as
`increment_df`, and it accumulates across days.

Shapes:

  - Keys are NATURAL, not the upstream `*_ID` columns — those are single-valued in the payload
    (every row 0), so keying on them would materialise one-row dimensions. `DimArea` keys on
    `area_name_en`, `DimPropertyType` on (type, sub_type, usage), `DimMetro` on
    `nearest_metro_en`. The fact carries these natural keys as its foreign keys.
  - The fact is keyed on `contract_id` (the payload `CONTRACT_NUMBER`) so a same-day re-run and
    the consecutive-day 2-day-window overlap are idempotent via `INSERT OR REPLACE` — the DB
    constraint replaces the cross-file dedup the weekly builder used to do in code.
  - Dimensions are rebuilt from the fact every run, so they cannot drift from it.

This module only defines the DDL and the insert/`_meta` SQL; `build_layers_duckdb` executes it.
"""
from __future__ import annotations

FACT_TABLE = "FctContract"
DIMENSION_TABLES = ("DimArea", "DimPropertyType", "DimMetro")

# Column order of FctContract. One owner: the DDL, the insert, and any reader build the column
# list from here so they cannot drift.
FACT_COLUMNS: tuple[str, ...] = (
    "contract_id",
    "rn",
    "area_name_en",
    "area_tier",
    "ejari_property_type_en",
    "ejari_property_sub_type_en",
    "property_usage_en",
    "usage_category",
    "nearest_metro_en",
    "contract_registration_date",
    "contract_start_date",
    "contract_end_date",
    "contract_year",
    "contract_quarter",
    "contract_month",
    "contract_weekday",
    "contract_season",
    "annual_amount",
    "contract_amount",
    "actual_area",
    "price_per_sqft",
    "contract_duration_days",
    "contract_duration_category",
    "rooms",
    "total_properties",
    "has_parking",
    "is_free_hold",
    "is_luxury",
    "is_bulk_registration",
    "project_name_en",
    "master_project_en",
)

_FACT_DDL = """
CREATE TABLE IF NOT EXISTS FctContract (
    contract_id                 VARCHAR PRIMARY KEY,
    rn                          BIGINT,
    area_name_en                VARCHAR,
    area_tier                   VARCHAR,
    ejari_property_type_en      VARCHAR,
    ejari_property_sub_type_en  VARCHAR,
    property_usage_en           VARCHAR,
    usage_category              VARCHAR,
    nearest_metro_en            VARCHAR,
    contract_registration_date  TIMESTAMP,
    contract_start_date         TIMESTAMP,
    contract_end_date           TIMESTAMP,
    contract_year               BIGINT,
    contract_quarter            BIGINT,
    contract_month              BIGINT,
    contract_weekday            BIGINT,
    contract_season             VARCHAR,
    annual_amount               DOUBLE,
    contract_amount             DOUBLE,
    actual_area                 DOUBLE,
    price_per_sqft              DOUBLE,
    contract_duration_days      BIGINT,
    contract_duration_category  VARCHAR,
    rooms                       BIGINT,
    total_properties            BIGINT,
    has_parking                 BOOLEAN,
    is_free_hold                BOOLEAN,
    is_luxury                   BOOLEAN,
    is_bulk_registration        BOOLEAN,
    project_name_en             VARCHAR,
    master_project_en           VARCHAR
)
"""

SILVER_DDL: list[str] = [_FACT_DDL]

# Rebuilt from the fact every run: a DISTINCT of columns the fact already carries, so the
# dimensions can never disagree with the rows they describe.
DIM_AREA_SQL = """
CREATE OR REPLACE TABLE DimArea AS
SELECT area_name_en, any_value(area_tier) AS area_tier
FROM FctContract
WHERE area_name_en IS NOT NULL
GROUP BY area_name_en
"""

DIM_PROPERTY_TYPE_SQL = """
CREATE OR REPLACE TABLE DimPropertyType AS
SELECT DISTINCT
       ejari_property_type_en,
       ejari_property_sub_type_en,
       property_usage_en,
       usage_category
FROM FctContract
"""

DIM_METRO_SQL = """
CREATE OR REPLACE TABLE DimMetro AS
SELECT DISTINCT nearest_metro_en
FROM FctContract
WHERE nearest_metro_en IS NOT NULL
"""

SILVER_REBUILD_DIMS: list[str] = [DIM_AREA_SQL, DIM_PROPERTY_TYPE_SQL, DIM_METRO_SQL]

# target fact column -> expression over the registered relation `f` (the enriched increment).
_FACT_SELECT: dict[str, str] = {
    "contract_id": "CAST(f.contract_id AS VARCHAR)",
    "rn": "CAST(f.RN AS BIGINT)",
    "area_name_en": "CAST(f.area_name_en AS VARCHAR)",
    "area_tier": "CAST(f.area_tier AS VARCHAR)",
    "ejari_property_type_en": "CAST(f.ejari_property_type_en AS VARCHAR)",
    "ejari_property_sub_type_en": "CAST(f.ejari_property_sub_type_en AS VARCHAR)",
    "property_usage_en": "CAST(f.property_usage_en AS VARCHAR)",
    "usage_category": "CAST(f.usage_category AS VARCHAR)",
    "nearest_metro_en": "CAST(f.nearest_metro_en AS VARCHAR)",
    "contract_registration_date": "CAST(f.contract_registration_date AS TIMESTAMP)",
    "contract_start_date": "CAST(f.contract_start_date AS TIMESTAMP)",
    "contract_end_date": "CAST(f.contract_end_date AS TIMESTAMP)",
    "contract_year": "CAST(f.contract_year AS BIGINT)",
    "contract_quarter": "CAST(f.contract_quarter AS BIGINT)",
    "contract_month": "CAST(f.contract_month AS BIGINT)",
    "contract_weekday": "CAST(f.contract_weekday AS BIGINT)",
    "contract_season": "CAST(f.contract_season AS VARCHAR)",
    "annual_amount": "CAST(f.annual_amount AS DOUBLE)",
    "contract_amount": "CAST(f.contract_amount AS DOUBLE)",
    "actual_area": "CAST(f.actual_area AS DOUBLE)",
    "price_per_sqft": "CAST(f.price_per_sqft AS DOUBLE)",
    "contract_duration_days": "CAST(f.contract_duration_days AS BIGINT)",
    "contract_duration_category": "CAST(f.contract_duration_category AS VARCHAR)",
    "rooms": "CAST(f.ROOMS AS BIGINT)",
    "total_properties": "CAST(f.TOTAL_PROPERTIES AS BIGINT)",
    "has_parking": "CAST(f.PARKING AS BOOLEAN)",
    "is_free_hold": "CAST(f.IS_FREE_HOLD AS BOOLEAN)",
    "is_luxury": "CAST(f.is_luxury AS BOOLEAN)",
    "is_bulk_registration": "CAST(f.is_bulk_registration AS BOOLEAN)",
    "project_name_en": "CAST(f.project_name_en AS VARCHAR)",
    "master_project_en": "CAST(f.master_project_en AS VARCHAR)",
}


def insert_sql(table: str = FACT_TABLE) -> str:
    """The idempotent fact insert. `INSERT OR REPLACE` keys on `contract_id`: a re-sent
    registration (same-day re-run, or the next day's 2-day window) overwrites its row instead
    of duplicating. `FACT_COLUMNS` is the single owner of the column list."""
    cols = ", ".join(FACT_COLUMNS)
    exprs = ", ".join(_FACT_SELECT[c] for c in FACT_COLUMNS)
    return f"INSERT OR REPLACE INTO {table} ({cols}) SELECT {exprs} FROM increment_df f"


def meta_sql(ingested_window: str) -> str:
    """The one-row `_meta` — the current state of the cumulative store, not a per-run log.

    `data_from`/`data_through` are the registration-date bounds of everything held, so the
    freshness of the store is readable at a glance (a stalled feed shows up as `data_through`
    not advancing).
    """
    return (
        "CREATE OR REPLACE TABLE _meta AS SELECT "
        "CAST(now() AS TIMESTAMP) AS built_at, "
        "(SELECT min(CAST(contract_registration_date AS DATE)) FROM FctContract) AS data_from, "
        "(SELECT max(CAST(contract_registration_date AS DATE)) FROM FctContract) AS data_through, "
        "(SELECT count(*) FROM FctContract) AS total_contracts, "
        f"'{ingested_window}' AS last_ingested_window"
    )

-- SQLite
PRAGMA foreign_keys = ON;

-- =========================
-- dim_contract_type
-- =========================
CREATE TABLE dim_contract_type (
    contract_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_reg_type_id INTEGER NOT NULL,
    contract_id TEXT,
    contract_reg_type_en TEXT,
    contract_reg_type_ar TEXT
);

-- =========================
-- dim_property
-- =========================
CREATE TABLE dim_property (
    property_key INTEGER PRIMARY KEY AUTOINCREMENT,
    ejari_bus_property_type_id INTEGER NOT NULL,
    ejari_property_type_id INTEGER NOT NULL,
    ejari_bus_property_type_en TEXT,
    ejari_bus_property_type_ar TEXT,
    ejari_property_type_en TEXT,
    ejari_property_type_ar TEXT,
    ejari_property_sub_type_en TEXT,
    ejari_property_sub_type_ar TEXT,
    property_usage_en TEXT,
    property_usage_ar TEXT
);

-- =========================
-- dim_project
-- =========================
create table dim_project (
	project_key INTEGER PRIMARY KEY AUTOINCREMENT,
	project_number INTEGER NOT NULL,
    project_name_ar TEXT,
    project_name_en TEXT,
    master_project_ar TEXT,
    master_project_en TEXT
);


-- =========================
-- dim_location
-- =========================
CREATE TABLE dim_location (
    location_key INTEGER PRIMARY KEY AUTOINCREMENT,
    area_id INTEGER NOT NULL,
    area_name_en TEXT,
    area_name_ar TEXT,
    actual_area TEXT,
    nearest_landmark_en TEXT,
    nearest_metro_en TEXT,
    nearest_mall_en TEXT,
    nearest_landmark_ar TEXT,
    nearest_metro_ar TEXT,
    nearest_mall_ar TEXT
);

-- =========================
-- dim_tenant
-- =========================
CREATE TABLE dim_tenant (
    tenant_key INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_type_id INTEGER NOT NULL,
    tenant_type_en TEXT,
	tenant_type_ar TEXT
);

-- =========================
-- dim_date
-- =========================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key       INTEGER PRIMARY KEY,      -- YYYYMMDD
    full_date      DATE NOT NULL,
    year           INTEGER NOT NULL,
    month          INTEGER NOT NULL,
    day_of_month   INTEGER NOT NULL,
    quarter        INTEGER NOT NULL,
    day_of_week    INTEGER NOT NULL            -- 1=Mon … 7=Sun
);

-- =========================
-- fact_rental_contract
-- =========================
CREATE TABLE fact_rent_contract (
    rent_contract_key INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_type_key INTEGER NOT NULL,     -- FK to dim_contract_type
    property_key INTEGER NOT NULL,          -- FK to dim_property
    project_key INTEGER NOT NULL,          -- FK to dim_project
    location_key INTEGER NOT NULL,          -- FK to dim_location
    tenant_key INTEGER NOT NULL,            -- FK to dim_tenant
    contract_start_date DATE,        
    contract_end_date DATE,    
    annual_amount INTEGER NOT NULL,
    contract_amount INTEGER NOT NULL,
    no_of_prop INTEGER NOT NULL,
    line_number INTEGER NOT NULL,
    is_free_hold INTEGER NOT NULL,
    CONSTRAINT fk_contract_type FOREIGN KEY (contract_type_key)
        REFERENCES dim_contract_type(contract_type_key),
    CONSTRAINT fk_property FOREIGN KEY (property_key)
        REFERENCES dim_property(property_key),
    CONSTRAINT fk_location FOREIGN KEY (location_key)
        REFERENCES dim_location(location_key),
    CONSTRAINT fk_tenant FOREIGN KEY (tenant_key)
        REFERENCES dim_tenant(tenant_key),
    CONSTRAINT fk_start_date FOREIGN KEY (contract_start_date)
        REFERENCES dim_date(date_key),
    CONSTRAINT fk_end_date FOREIGN KEY (contract_end_date)
        REFERENCES dim_date(date_key)
);
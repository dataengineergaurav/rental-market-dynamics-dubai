# Dubai Rental Market Data Release

**Release Date:** 2026-02-10  
**Data Source:** Dubai Land Department (DLD)

## 📦 Data Artifacts

### 1. rent_contracts_silver_20260210.parquet (194 MB)
**Single Source of Truth - Cleaned Data**

Contains 9,749,606 rent contracts with:
- Proper data types (BIGINT, DATE)
- Parsed dates (contract_start_date, contract_end_date)
- Data quality flags (_has_date_issues, _has_amount_issues)
- Audit columns (_ingestion_timestamp, _source_file, _cleaned_timestamp)

**Use this for:**
- Data analysis
- Machine learning
- Reporting
- External tool integration

### 2. rental_data.db (1.5 GB)
**Full DuckDB Database with Medallion Architecture**

#### Bronze Layer (Raw)
- `bronze.rent_contracts`: 9,749,606 rows
- Raw CSV data with audit columns

#### Silver Layer (Cleaned)
- `silver.rent_contracts`: 9,749,606 rows
- Cleaned data with proper types
- Quality flags for data issues

#### Gold Layer (Analytics)
**Dimension Tables:**
- `gold.dim_contract_type`: 8,137,572 rows
- `gold.dim_date`: 1,100,213 rows
- `gold.dim_location`: 101,020 rows
- `gold.dim_project`: 1,637 rows
- `gold.dim_property`: 833 rows
- `gold.dim_tenant`: 2 rows

**Fact Table:**
- `gold.fact_rent_contract`: 9,749,606 rows
- Star schema with surrogate keys

**View:**
- `gold.v_expiring_contracts_15d`: 43,505 rows
- Contracts expiring in next 15 days (2026-02-10 to 2026-02-25)

## 🔍 Key Insights

### Expiring Contracts (Next 15 Days): 43,505
**By Property Usage:**
- Residential: 26,444 contracts (avg AED 265,657)
- Commercial: 16,726 contracts (avg AED 93,807)
- Industrial: 232 contracts (avg AED 211,534)

## 🚀 Quick Start

### Using Parquet (Recommended for Analysis)
```python
import polars as pl

# Read silver data
df = pl.read_parquet("rent_contracts_silver_20260210.parquet")
print(f"Total contracts: {len(df):,}")
```

### Using DuckDB (Recommended for SQL Analytics)
```python
import duckdb

conn = duckdb.connect("rental_data.db")

# Query expiring contracts
result = conn.execute("""
    SELECT * FROM gold.v_expiring_contracts_15d
    WHERE property_usage_en = 'Residential'
    ORDER BY days_until_expiry
""").fetchall()

# Query star schema
result = conn.execute("""
    SELECT 
        dp.property_usage_en,
        COUNT(*) as count,
        AVG(f.annual_amount) as avg_rent
    FROM gold.fact_rent_contract f
    JOIN gold.dim_property dp ON f.property_key = dp.property_key
    GROUP BY dp.property_usage_en
""").fetchall()
```

## 📊 Medallion Architecture

```
┌─────────────────────────────────────────────────────┐
│  Bronze Layer (Raw)                                  │
│  - Raw CSV data                                      │
│  - Audit columns                                     │
├─────────────────────────────────────────────────────┤
│  Silver Layer (Cleaned)                              │
│  - Proper data types                                 │
│  - Date parsing                                      │
│  - Quality flags                                     │
├─────────────────────────────────────────────────────┤
│  Gold Layer (Analytics)                              │
│  - Star schema                                       │
│  - Dimensions + Fact tables                          │
│  - Virtual views                                     │
└─────────────────────────────────────────────────────┘
```

## 📈 Storage Optimization

- **Previous:** 3.9 GB (2.8 GB main + 1.1 GB expiring DB)
- **Current:** 1.7 GB (1.5 GB DB + 0.2 GB parquet)
- **Saved:** 2.2 GB (56% reduction)

## 🔗 Repository

Code and documentation: https://github.com/dataengineergaurav/rental-market-dynamics-dubai

## 📄 License

Data provided by Dubai Land Department (DLD) for public use.

## 📝 Notes

- Data covers rent contracts from 2019 to present
- Contract dates are in DD-MM-YYYY format in source
- Currency: UAE Dirham (AED)
- All amounts are annual rent values

# Dubai Real Estate Library - Usage Guide

## Overview

This enhanced library provides comprehensive tools for analyzing Dubai rental market data from the Dubai Land Department. The library now includes advanced market analytics, data validation, and enrichment capabilities.

## New Modules

### 1. Configuration Management (`lib/config.py`)

Centralized configuration for Dubai market settings.

```python
from lib.config import (
    get_area_tier,
    normalize_property_type,
    is_residential,
    is_commercial,
    VALIDATION_THRESHOLDS,
    AREA_CLASSIFICATIONS
)

# Get area tier
tier = get_area_tier("Dubai Marina")  # Returns AreaTier.PREMIUM

# Normalize property type
normalized = normalize_property_type("apt")  # Returns "Apartment"

# Check usage type
is_res = is_residential("Residential - Apartment")  # Returns True
```

### 2. Data Validation (`lib/classes/validators.py`)

Comprehensive validation for rent contract data.

```python
import polars as pl
from lib.classes.validators import validate_rent_contracts

# Load data
df = pl.read_parquet("rent_contracts.parquet")

# Validate
result = validate_rent_contracts(df, strict=False)

# Check results
print(result.get_summary())
print(result)  # Detailed validation report

if result.is_valid:
    print("Data passed validation!")
else:
    print(f"Found {len(result.errors)} errors")
```

### 3. Market Analytics (`lib/classes/market_analytics.py`)

Advanced market intelligence and analytics.

```python
import polars as pl
from lib.classes.market_analytics import MarketAnalytics

# Load data
df = pl.read_parquet("rent_contracts.parquet")

# Initialize analytics
analytics = MarketAnalytics(df)

# Calculate PSF metrics
psf_data = analytics.calculate_psf_metrics()

# Analyze by area
area_stats = analytics.analyze_by_area(area_column="area_en")
print(area_stats)

# Identify high-demand areas
top_areas = analytics.identify_high_demand_areas(top_n=20)

# Analyze by property type
type_stats = analytics.analyze_by_property_type()

# Segment by usage
usage_stats = analytics.segment_by_usage()

# Identify luxury properties
luxury_data = analytics.identify_luxury_properties()

# Calculate rental trends
monthly_trends = analytics.calculate_rental_trends(period="monthly")
quarterly_trends = analytics.calculate_rental_trends(period="quarterly")

# Generate market summary
summary = analytics.generate_market_summary()
print(summary)
```

### 4. Data Enrichment (`lib/transform/enrichment.py`)

Enrich raw data with calculated fields and classifications.

```python
import polars as pl
from lib.transform.enrichment import enrich_rent_contracts

# Load raw data
df = pl.read_parquet("rent_contracts.parquet")

# Enrich data
enriched_df = enrich_rent_contracts(df)

# New columns added:
# - price_per_sqft: Rent per square foot
# - area_tier: Area classification (Premium/Mid-Tier/Budget/Emerging)
# - property_type_normalized: Standardized property type
# - registration_year, registration_quarter, registration_month: Temporal features
# - registration_season: Season classification
# - contract_duration_days: Contract length in days
# - contract_duration_category: Short/Medium/Long-term
# - is_luxury: Luxury property flag
# - usage_category: Simplified category (Residential/Commercial/Other)

# Save enriched data
enriched_df.write_parquet("rent_contracts_enriched.parquet")
```

### 5. Enhanced Property Usage (`lib/classes/property_usage.py`)

Comprehensive property usage analysis with market insights.

```python
from lib.classes.property_usage import PropertyUsage

# Initialize analyzer
analyzer = PropertyUsage(output="property_usage_report.csv")

# Generate comprehensive report
analyzer.transform("rent_contracts.parquet")

# Compare two periods
analyzer.compare_periods(
    current_file="rent_contracts_2024.parquet",
    previous_file="rent_contracts_2023.parquet",
    output_comparison="yoy_comparison.csv"
)
```

## ETL Pipeline (`run_etl_pipeline.py`)

Incremental rents pipeline: `download_rents()` (Scrapy spider with direct `EjariRentsDownloader` fallback) → `transform_rents()` → `analyze_property_usage()` → `publish_artifacts_to_github()`. Skips download if file exists, treats 0-row windows as success.

Core modules:

### Rents Downloader (`lib/extract/ejari_rents_downloader.py`)

```python
from lib.extract.ejari_rents_downloader import EjariRentsDownloader

downloader = EjariRentsDownloader(url)  # url or EJARI_URL env
success: bool = downloader.run(filename, from_date="09/12/2026", to_date="09/13/2026")

# Features:
# - Paginated POST with P_TAKE/P_SKIP, retry with exponential backoff
# - Writes CSV (default) or JSON if filename endswith .json
# - Configurable timeout via API_CONFIG
```

### Rents Transformer (`lib/transform/rents_transformer.py`)

```python
from lib.transform.rents_transformer import RentsTransformer

transformer = RentsTransformer(input_file="input.csv", output_file="output.parquet")
success: bool = transformer.transform()

# Features:
# - scan_csv (utf8-lossy) with schema_overrides for ANNUAL_AMOUNT/ACTUAL_AREA
# - Parses REGISTRATION_DATE/START_DATE/END_DATE to datetime
# - Adds canonical lowercase aliases (annual_amount, actual_area, area_name_en, ...)
# - sink_parquet with zstd (see FILE_CONFIG)
```

## Complete Example: ETL with Analytics (rents only)

```python
import os
import polars as pl
from datetime import date
from dotenv import load_dotenv

from lib.extract.ejari_rents_downloader import EjariRentsDownloader
from lib.transform.rents_transformer import RentsTransformer
from lib.transform.enrichment import enrich_rent_contracts
from lib.classes.property_usage import PropertyUsage
from lib.classes.market_analytics import MarketAnalytics
from lib.classes.validators import validate_rent_contracts
from lib.logging_helpers import configure_root_logger, get_logger

# Configure logging
configure_root_logger(logfile="etl.log", loglevel="INFO")
logger = get_logger("ETL")

load_dotenv()

# Step 1: Download (rents only, incremental window)
url = os.getenv("EJARI_URL")
csv_file = f"output/rent_contracts_{date.today():%Y%m%d}.csv"

downloader = EjariRentsDownloader(url)
if not downloader.run(csv_file, from_date="09/12/2026", to_date="09/13/2026"):
    logger.error("Download failed")
    exit(1)

# Step 2: Transform
parquet_file = f"output/rent_contracts_{date.today():%Y%m%d}.parquet"

transformer = RentsTransformer(csv_file, parquet_file)
if not transformer.transform():
    logger.error("Transformation failed")
    exit(1)

# Step 3: Enrich data
df = pl.read_parquet(parquet_file)
enriched_df = enrich_rent_contracts(df)
enriched_file = f"output/rent_contracts_enriched_{date.today():%Y%m%d}.parquet"
enriched_df.write_parquet(enriched_file)

# Step 4: Generate property usage report
property_usage = PropertyUsage(f"output/property_usage_{date.today():%Y%m%d}.csv")
property_usage.transform(enriched_file)

# Step 5: Run market analytics
analytics = MarketAnalytics(enriched_df)

# Generate various reports
area_stats = analytics.analyze_by_area()
area_stats.write_csv(f"output/area_analysis_{date.today():%Y%m%d}.csv")

top_areas = analytics.identify_high_demand_areas(top_n=20)
top_areas.write_csv(f"output/top_areas_{date.today():%Y%m%d}.csv")

type_stats = analytics.analyze_by_property_type()
type_stats.write_csv(f"output/property_type_analysis_{date.today():%Y%m%d}.csv")

monthly_trends = analytics.calculate_rental_trends(period="monthly")
monthly_trends.write_csv(f"output/monthly_trends_{date.today():%Y%m%d}.csv")

market_summary = analytics.generate_market_summary()
logger.info(f"Market Summary: {market_summary}")

logger.info("ETL pipeline with analytics completed successfully!")
# Note: run_etl_pipeline.py is the canonical incremental pipeline (Scrapy -> direct fallback)
```

## Configuration

### Environment Variables

```bash
# .env file (see .env.example)
EJARI_URL=your_ejari_endpoint_here
GH_TOKEN=your_github_token
```

### Validation Thresholds

Customize validation thresholds in `lib/config.py`:

```python
VALIDATION_THRESHOLDS = {
    "min_annual_rent": 10000,
    "max_annual_rent": 5000000,
    "min_property_size": 200,
    "max_property_size": 50000,
    "min_psf_residential": 20,
    "max_psf_residential": 500,
    # ... more thresholds
}
```

### Area Classifications

Add or modify area classifications in `lib/config.py`:

```python
AREA_CLASSIFICATIONS = {
    "Downtown Dubai": AreaTier.PREMIUM,
    "Dubai Marina": AreaTier.PREMIUM,
    # ... more areas
}
```

## Output Files

The enhanced library generates the following outputs:

1. **rent_contracts_YYYY-MM-DD.parquet** - Transformed data
2. **rent_contracts_enriched_YYYY-MM-DD.parquet** - Enriched data with calculated fields
3. **property_usage_report_YYYY-MM-DD.csv** - Comprehensive usage statistics
4. **area_analysis_YYYY-MM-DD.csv** - Area-wise market analysis
5. **top_areas_YYYY-MM-DD.csv** - High-demand areas
6. **property_type_analysis_YYYY-MM-DD.csv** - Property type statistics
7. **monthly_trends_YYYY-MM-DD.csv** - Time-series trends
8. **yoy_comparison_YYYY-MM-DD.csv** - Year-over-year comparison

## Dubai Market Insights

### Area Tiers

- **Premium**: Downtown Dubai, Dubai Marina, Palm Jumeirah, Emirates Hills
- **Mid-Tier**: JVC, JVT, Dubai Sports City, Motor City
- **Budget**: International City, Deira, Bur Dubai
- **Emerging**: Dubai South, Dubailand, Dubai Production City

### Key Metrics

- **PSF (Price per Square Foot)**: Primary metric for comparing properties
- **Market Share**: Percentage of total contracts by category
- **Luxury Threshold**: Top 25% by PSF or top 20% by rent
- **Contract Duration**: Categorized as Short (<6mo), Medium (6-12mo), Long (>12mo)

## Best Practices

1. **Always validate data** before analysis
2. **Enrich data** to enable advanced analytics
3. **Use PSF metrics** for fair property comparisons
4. **Filter outliers** using validation thresholds
5. **Track trends** over time for market insights
6. **Segment by area tier** for targeted analysis

## Troubleshooting

### Validation Errors

If validation fails, check:
- Data quality in source CSV
- Threshold settings in config.py
- Required columns are present

### Missing Columns

Some analytics require specific columns:
- `actual_area` for PSF calculations
- `registration_date` for trend analysis
- `area_en` for area-based analysis

### Performance

For large datasets:
- Use LazyFrames when possible
- Enable streaming for transformations
- Filter data before analytics

## Next Steps

Potential enhancements:
- Location intelligence module
- Caching mechanism for repeated queries
- Visualization helpers
- Interactive dashboards
- API endpoints for analytics

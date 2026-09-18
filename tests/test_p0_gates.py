import polars as pl
from lib.transform.enrichment import enrich_rent_contracts
from lib.classes.market_analytics import MarketAnalytics
from lib.classes.validators import validate_rent_contracts


def test_enrichment_tier_mapping():
    df = pl.DataFrame({
        "area_name_en": ["Dubai Marina", "International City", "Unknown Area X", "Business Bay"],
        "ejari_property_type_en": ["Flat", "Flat", "Flat", "Flat"],
        "annual_amount": [100000, 50000, 60000, 200000],
        "actual_area": [800, 800, 800, 800],
    })
    enriched = enrich_rent_contracts(df)
    assert enriched.filter(pl.col("area_name_en") == "Dubai Marina")["area_tier"][0] == "Premium"
    assert enriched.filter(pl.col("area_name_en") == "International City")["area_tier"][0] == "Budget"
    assert enriched.filter(pl.col("area_name_en") == "Unknown Area X")["area_tier"][0] == "Mid-Tier"
    assert enriched.filter(pl.col("area_name_en") == "Business Bay")["area_tier"][0] == "Premium"


def test_enrichment_psf_null_for_small_area():
    df = pl.DataFrame({
        "area_name_en": ["Dubai Marina"] * 3,
        "ejari_property_type_en": ["Flat"] * 3,
        "annual_amount": [100000, 50000, 60000],
        "actual_area": [1.0, 15.9, 199],
        "property_usage_en": ["Residential"] * 3,
    })
    enriched = enrich_rent_contracts(df)
    assert enriched["price_per_sqft"].null_count() == 3, "PSF must be null for area <200"


def test_enrichment_psf_valid():
    df = pl.DataFrame({
        "area_name_en": ["Dubai Marina"],
        "ejari_property_type_en": ["Flat"],
        "annual_amount": [100000],
        "actual_area": [1000],
        "property_usage_en": ["Residential"],
    })
    enriched = enrich_rent_contracts(df)
    assert enriched["price_per_sqft"][0] == 100.0


def test_market_analytics_psf_filter():
    # 1 valid residential PSF 50, 1 outlier 5000, 1 small area filtered by >=200
    df = pl.DataFrame({
        "annual_amount": [50000, 5000000, 100000],
        "actual_area": [1000, 1000, 50],
        "property_usage_en": ["Residential", "Residential", "Residential"],
    })
    ma = MarketAnalytics(df)
    psf = ma.calculate_psf_metrics()
    # only first row should survive (50 PSF in 20-500, area 1000)
    assert psf.height == 1
    assert psf["psf"][0] == 50.0


def test_bulk_flag_detection():
    # 11 same area+amount → bulk, 5 same → not bulk
    bulk_rows = [{"area_name_en": "Naif", "annual_amount": 1540471, "ejari_property_type_en": "Flat", "actual_area": 500, "property_usage_en": "Commercial"}] * 11
    normal_rows = [{"area_name_en": "Naif", "annual_amount": 50000, "ejari_property_type_en": "Flat", "actual_area": 500, "property_usage_en": "Commercial"}] * 5
    df = pl.DataFrame(bulk_rows + normal_rows)
    enriched = enrich_rent_contracts(df)
    assert enriched.filter(pl.col("annual_amount") == 1540471)["is_bulk_registration"].all()
    assert not enriched.filter(pl.col("annual_amount") == 50000)["is_bulk_registration"].any()


def test_validator_gate_no_crash_on_small_df():
    df = pl.DataFrame({
        "contract_id": [1, 2],
        "contract_start_date": [None, None],
        "property_usage_en": ["Residential", "Commercial"],
        "annual_amount": [50000, 70000],
        "actual_area": [1.0, 15.9],
    })
    result = validate_rent_contracts(df, strict=False)
    # should not crash, warnings for small area
    assert result is not None
    assert "Validating 2 records" in result.info[0]

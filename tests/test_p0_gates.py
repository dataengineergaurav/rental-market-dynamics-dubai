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


def test_property_usage_psf_respects_the_200_sqft_floor(tmp_path):
    """PSF must be READ from Silver's rent_per_sqft, never recomputed.

    Recomputing annual_amount/actual_area bypassed the >=200 floor and shipped
    Residential avg_psf 4691 (output/property_usage_20260913.csv), 9x the top of
    the plausible band.

    The 150 sqft row is the load-bearing one: rent/area is 100 AED/sqft, INSIDE
    the 20-500 band, so the band cannot be what excludes it — only the area
    floor can. Hence the exact-equality assert, not just the band check.
    """
    from lib.classes.silver_contract import to_silver
    from lib.classes.property_usage import PropertyUsage

    silver = to_silver(pl.DataFrame({
        "area_name_en": ["Dubai Marina"] * 3,
        "property_usage_en": ["Residential"] * 3,
        "annual_amount": [100000, 15000, 300000],
        "actual_area": [1.0, 150.0, 1000.0],
        "RN": [1, 2, 3],
    })).frame
    assert silver["rent_per_sqft"].to_list()[:2] == [None, None], (
        "fixture is wrong: the two sub-200 sqft rows must already be nulled by Silver"
    )

    src = tmp_path / "silver.parquet"
    out = tmp_path / "property_usage.csv"
    silver.write_parquet(src)
    PropertyUsage(str(out)).transform(str(src))

    avg_psf = pl.read_csv(out)["avg_psf"][0]
    assert avg_psf is not None, "PSF dropped entirely instead of being guarded"
    assert 20 <= avg_psf <= 500, f"avg_psf {avg_psf} outside the 20-500 band"
    assert avg_psf == 300.0, (
        f"avg_psf {avg_psf}: sub-200 sqft rows contributed. Only the 1000 sqft "
        "row is PSF-eligible, so the mean must be exactly its own PSF"
    )


def test_property_usage_omits_psf_when_the_column_is_absent(tmp_path):
    """A pre-Silver parquet carries actual_area but no rent_per_sqft. The report
    must still be written, with no PSF columns — never a recomputed PSF, which
    is the bug this whole guard exists to prevent."""
    from lib.classes.property_usage import PropertyUsage

    src = tmp_path / "legacy.parquet"
    out = tmp_path / "property_usage.csv"
    pl.DataFrame({
        "property_usage_en": ["Residential", "Residential"],
        "annual_amount": [100000.0, 300000.0],
        "actual_area": [1.0, 1000.0],
    }).write_parquet(src)
    PropertyUsage(str(out)).transform(str(src))

    report = pl.read_csv(out)
    assert "avg_psf" not in report.columns
    assert "avg_area_sqft" in report.columns, "the rest of the report must survive"


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

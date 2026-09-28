"""
Configuration module for Dubai Real Estate Analysis.

This module contains all configuration settings, constants, and market-specific
parameters for analyzing Dubai rental market data.
"""

from typing import Dict
from enum import Enum

import polars as pl


# DLD Data Schema - Actual Column Names
DLD_SCHEMA = {
    "contract_id": "contract_id",
    "contract_start_date": "contract_start_date",
    "contract_end_date": "contract_end_date",
    "annual_amount": "annual_amount",
    "property_type": "ejari_property_type_en",
    "property_sub_type": "ejari_property_sub_type_en",
    "property_usage": "property_usage_en",
    "area_name": "area_name_en",
    "project_name": "project_name_en",
    "master_project": "master_project_en",
    "tenant_type": "tenant_type_en",
}


class AreaTier(Enum):
    """Classification of Dubai areas by market tier."""
    PREMIUM = "Premium"
    MID_TIER = "Mid-Tier"
    BUDGET = "Budget"
    EMERGING = "Emerging"


class PropertyType(Enum):
    """Standard property types in Dubai."""
    APARTMENT = "Apartment"
    VILLA = "Villa"
    TOWNHOUSE = "Townhouse"
    PENTHOUSE = "Penthouse"
    STUDIO = "Studio"
    OFFICE = "Office"
    RETAIL = "Retail"
    WAREHOUSE = "Warehouse"
    LAND = "Land"


# Dubai Area Classifications
AREA_CLASSIFICATIONS: Dict[str, AreaTier] = {
    # Premium Areas
    "Downtown Dubai": AreaTier.PREMIUM,
    "Dubai Marina": AreaTier.PREMIUM,
    "Palm Jumeirah": AreaTier.PREMIUM,
    "Emirates Hills": AreaTier.PREMIUM,
    "Jumeirah Beach Residence": AreaTier.PREMIUM,
    "Business Bay": AreaTier.PREMIUM,
    "Dubai Hills Estate": AreaTier.PREMIUM,
    "Arabian Ranches": AreaTier.PREMIUM,
    
    # Mid-Tier Areas
    "Jumeirah Village Circle": AreaTier.MID_TIER,
    "Jumeirah Village Triangle": AreaTier.MID_TIER,
    "Dubai Sports City": AreaTier.MID_TIER,
    "Motor City": AreaTier.MID_TIER,
    "The Greens": AreaTier.MID_TIER,
    "The Views": AreaTier.MID_TIER,
    "Discovery Gardens": AreaTier.MID_TIER,
    "Mirdif": AreaTier.MID_TIER,
    
    # Budget Areas
    "International City": AreaTier.BUDGET,
    "Deira": AreaTier.BUDGET,
    "Bur Dubai": AreaTier.BUDGET,
    "Al Nahda": AreaTier.BUDGET,
    "Al Qusais": AreaTier.BUDGET,
    
    # Emerging Areas
    "Dubai South": AreaTier.EMERGING,
    "Dubailand": AreaTier.EMERGING,
    "Dubai Production City": AreaTier.EMERGING,
}


# Market Validation Thresholds
VALIDATION_THRESHOLDS = {
    # Rent amount ranges (AED per year)
    "min_annual_rent": 10000,
    "max_annual_rent": 5000000,
    
    # Property size ranges (square feet)
    "min_property_size": 200,
    "max_property_size": 50000,
    
    # Price per square foot ranges (AED)
    "min_psf_residential": 20,
    "max_psf_residential": 500,
    "min_psf_commercial": 30,
    "max_psf_commercial": 800,
    
    # Contract duration (days)
    "min_contract_days": 30,
    "max_contract_days": 730,  # 2 years
}


# Property Type Mappings (for normalization)
PROPERTY_TYPE_MAPPINGS: Dict[str, str] = {
    "apt": "Apartment",
    "apartment": "Apartment",
    "flat": "Apartment",
    "villa": "Villa",
    "townhouse": "Townhouse",
    "town house": "Townhouse",
    "penthouse": "Penthouse",
    "studio": "Studio",
    "office": "Office",
    "shop": "Retail",
    "retail": "Retail",
    "warehouse": "Warehouse",
    "land": "Land",
    "plot": "Land",
}


# Usage Categories
RESIDENTIAL_USAGE = [
    "Residential",
    "Residential - Apartment",
    "Residential - Villa",
    "Residential - Townhouse",
    "Residential - Studio",
]

COMMERCIAL_USAGE = [
    "Commercial",
    "Commercial - Office",
    "Commercial - Retail",
    "Commercial - Warehouse",
]


# Market Metrics Configuration
MARKET_METRICS = {
    # Percentile thresholds for luxury classification
    "luxury_psf_percentile": 75,
    "luxury_rent_percentile": 80,
    
    # Outlier detection (IQR multiplier)
    "outlier_iqr_multiplier": 3.0,
    
    # Minimum sample size for area statistics
    "min_area_sample_size": 10,
}


# API and Data Source Configuration (Ejari rents only)
# URL must come from EJARI_URL env (repo secret in CI). No hardcoded fallback.
import os as _os

API_CONFIG = {
    "gateway_rents_url": _os.getenv("EJARI_URL"),
    "request_timeout": 30,  # seconds
    "max_retries": 3,
    "retry_backoff_factor": 2,  # exponential backoff
}


# File Configuration
FILE_CONFIG = {
    "output_dir": "output",
    "cache_dir": ".cache",
    "log_dir": "logs",
    "parquet_compression": "zstd",
    "parquet_compression_level": 3,
}


# Logging Configuration
LOG_CONFIG = {
    "log_format": "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "default_level": "INFO",
}


# Data Quality Checks
DATA_QUALITY_RULES = {
    "required_fields": [
        "contract_start_date",
        "property_usage_en",
        "annual_amount",
    ],
    
    "date_fields": [
        "contract_start_date",
        "contract_end_date",
    ],
}


# Report Configuration
REPORT_CONFIG = {
    "top_n_areas": 20,  # Top N areas to include in reports
    "trend_periods": ["monthly", "quarterly", "yearly"],
    "export_formats": ["csv", "parquet"],
}


def get_area_tier(area_name: str) -> AreaTier:
    """
    Get the market tier for a given area.
    
    Args:
        area_name: Name of the area
        
    Returns:
        AreaTier enum value, defaults to MID_TIER if not found
    """
    return AREA_CLASSIFICATIONS.get(area_name, AreaTier.MID_TIER)


def normalize_property_type(property_type: str) -> str:
    """
    Normalize property type to standard format.
    
    Args:
        property_type: Raw property type string
        
    Returns:
        Normalized property type string
    """
    if not property_type:
        return "Unknown"
    
    normalized = property_type.lower().strip()
    return PROPERTY_TYPE_MAPPINGS.get(normalized, property_type.title())


def is_residential(usage: str) -> bool:
    """
    Check if property usage is residential.
    
    Args:
        usage: Property usage string
        
    Returns:
        True if residential, False otherwise
    """
    return usage in RESIDENTIAL_USAGE


def is_commercial(usage: str) -> bool:
    """
    Check if property usage is commercial.
    
    Args:
        usage: Property usage string
        
    Returns:
        True if commercial, False otherwise
    """
    return usage in COMMERCIAL_USAGE


def psf_band_filter(psf_column: str = "psf"):
    """Polars expression: keep a PSF only if it sits inside the validity band for
    its usage class. The area floor is NOT applied here — Silver already nulls
    PSF below PSF_MIN_AREA_SQFT, and repeating it is what previously let
    avg_psf 4691 ship (output/property_usage_20260913.csv).

    This is a publishing decision, not a computation: a 200+ sqft unit at
    8228 AED/sqft is a real registration, so Silver keeps it and Gold declines
    to report it. PropertyUsage and MarketAnalytics share this helper, so the
    BAND is defined once.

    The AREA FLOOR is not shared, and there are three PSF computation sites, not
    two: silver_contract.py:189 (behind PSF_MIN_AREA_SQFT, the reporting floor
    this helper's docstring refers to), market_analytics.py:82 and
    enrichment.py:94, each still carrying its own `200` literal. Follow-up: have
    the latter two read Silver's rent_per_sqft and delete two divisions and two
    literals. Do not describe the surfaces as unable to disagree until that lands.
    """
    return (
        (
            pl.col("property_usage_en").is_in(RESIDENTIAL_USAGE)
            & pl.col(psf_column).is_between(
                VALIDATION_THRESHOLDS["min_psf_residential"],
                VALIDATION_THRESHOLDS["max_psf_residential"],
            )
        )
        | (
            pl.col("property_usage_en").is_in(COMMERCIAL_USAGE)
            & pl.col(psf_column).is_between(
                VALIDATION_THRESHOLDS["min_psf_commercial"],
                VALIDATION_THRESHOLDS["max_psf_commercial"],
            )
        )
    )

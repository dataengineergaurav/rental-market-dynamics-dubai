"""
Data enrichment module for Dubai Real Estate.

Enriches raw rent contract data with calculated fields, classifications,
and temporal features to enable better analysis.
"""

import logging
from datetime import datetime
import polars as pl

from lib.config import (
    AREA_CLASSIFICATIONS,
    AreaTier,
    MARKET_METRICS,
    PROPERTY_TYPE_MAPPINGS,
)

logger = logging.getLogger(__name__)


class RentContractsEnricher:
    """
    Enriches rent contract data with calculated fields and classifications.
    
    Features:
    - Price per square foot calculation
    - Area tier classification
    - Property type normalization
    - Temporal features (quarter, season, year)
    - Contract duration calculation
    - Luxury property flagging
    """
    
    def __init__(self, data: pl.DataFrame):
        """
        Initialize enricher.
        
        Args:
            data: DataFrame containing rent contract data
        """
        self.data = data
        
    def enrich(self) -> pl.DataFrame:
        """
        Apply all enrichment transformations.
        
        Returns:
            Enriched DataFrame
        """
        logger.info("Starting data enrichment...")
        
        enriched = self.data
        
        # Calculate PSF
        enriched = self._add_psf(enriched)
        
        # Add area tier classification
        enriched = self._add_area_tier(enriched)
        
        # Normalize property types
        enriched = self._normalize_property_types(enriched)
        
        # Add temporal features
        enriched = self._add_temporal_features(enriched)
        
        # Calculate contract duration
        enriched = self._add_contract_duration(enriched)
        
        # Flag luxury properties
        enriched = self._flag_luxury_properties(enriched)
        
        # Add usage category
        enriched = self._add_usage_category(enriched)

        # Flag bulk registrations (Naif 79×1.54M etc) — count over (area, amount) >10
        enriched = self._add_bulk_flag(enriched)

        logger.info(f"Enrichment complete. Added {len(enriched.columns) - len(self.data.columns)} new columns")

        return enriched
        
    def _add_psf(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add price per square foot — null if area <200 (unusable)."""
        if "actual_area" in df.columns and "annual_amount" in df.columns:
            logger.debug("Adding PSF calculation...")
            df = df.with_columns(
                pl.when(
                    (pl.col("actual_area").is_not_null()) &
                    (pl.col("actual_area") >= 200) &
                    (pl.col("annual_amount").is_not_null()) &
                    (pl.col("annual_amount") > 0)
                )
                .then(pl.col("annual_amount") / pl.col("actual_area"))
                .otherwise(None)
                .alias("price_per_sqft")
            )
        return df
        
    def _add_area_tier(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add area tier classification via AREA_CLASSIFICATIONS."""
        if "area_name_en" in df.columns:
            logger.debug("Adding area tier classification...")
            tier_map = {k: v.value for k, v in AREA_CLASSIFICATIONS.items()}
            df = df.with_columns(
                pl.col("area_name_en").replace_strict(tier_map, default=AreaTier.MID_TIER.value).alias("area_tier")
            )
        return df
        
    def _normalize_property_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize property type names via PROPERTY_TYPE_MAPPINGS."""
        if "ejari_property_type_en" in df.columns:
            logger.debug("Normalizing property types...")
            df = df.with_columns(
                pl.col("ejari_property_type_en")
                .str.to_lowercase()
                .str.strip_chars()
                .replace_strict(PROPERTY_TYPE_MAPPINGS, default=None)
                .fill_null(pl.col("ejari_property_type_en").str.strip_chars().str.to_titlecase())
                .alias("property_type_normalized")
            )
        return df
        
    def _add_temporal_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add temporal features from contract start date."""
        if "contract_start_date" in df.columns and df["contract_start_date"].dtype != pl.Null:
            # skip if dtype is not temporal (e.g. all-null test fixture)
            if df["contract_start_date"].dtype not in (pl.Date, pl.Datetime, pl.Datetime("ns"), pl.Datetime("ms"), pl.Datetime("us")):
                # try to parse, if still not temporal skip
                if df["contract_start_date"].null_count() == df.height:
                    return df
            logger.debug("Adding temporal features...")
            try:
                df = df.with_columns([
                    pl.col("contract_start_date").dt.year().alias("contract_year"),
                    pl.col("contract_start_date").dt.quarter().alias("contract_quarter"),
                    pl.col("contract_start_date").dt.month().alias("contract_month"),
                    pl.col("contract_start_date").dt.weekday().alias("contract_weekday"),
                ])
            except Exception:
                return df
            
            # Add season
            df = df.with_columns(
                pl.when(pl.col("contract_month").is_in([12, 1, 2]))
                .then(pl.lit("Winter"))
                .when(pl.col("contract_month").is_in([3, 4, 5]))
                .then(pl.lit("Spring"))
                .when(pl.col("contract_month").is_in([6, 7, 8]))
                .then(pl.lit("Summer"))
                .otherwise(pl.lit("Fall"))
                .alias("contract_season")
            )
        
        return df
        
    def _add_contract_duration(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate contract duration in days."""
        if "contract_start_date" in df.columns and "contract_end_date" in df.columns:
            logger.debug("Calculating contract duration...")
            
            df = df.with_columns(
                pl.when(
                    (pl.col("contract_start_date").is_not_null()) &
                    (pl.col("contract_end_date").is_not_null())
                )
                .then((pl.col("contract_end_date") - pl.col("contract_start_date")).dt.total_days())
                .otherwise(None)
                .alias("contract_duration_days")
            )
            
            # Add duration category
            df = df.with_columns(
                pl.when(pl.col("contract_duration_days") < 180)
                .then(pl.lit("Short-term"))
                .when(pl.col("contract_duration_days") < 365)
                .then(pl.lit("Medium-term"))
                .when(pl.col("contract_duration_days") >= 365)
                .then(pl.lit("Long-term"))
                .otherwise(pl.lit("Unknown"))
                .alias("contract_duration_category")
            )
        
        return df
        
    def _flag_luxury_properties(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flag luxury properties based on rent and PSF."""
        if "price_per_sqft" in df.columns and "annual_amount" in df.columns:
            logger.debug("Flagging luxury properties...")
            
            # Calculate percentile thresholds
            valid_data = df.filter(
                (pl.col("price_per_sqft").is_not_null()) &
                (pl.col("annual_amount").is_not_null())
            )
            
            if valid_data.height > 0:
                psf_threshold = valid_data["price_per_sqft"].quantile(
                    MARKET_METRICS["luxury_psf_percentile"] / 100
                )
                rent_threshold = valid_data["annual_amount"].quantile(
                    MARKET_METRICS["luxury_rent_percentile"] / 100
                )
                
                df = df.with_columns(
                    pl.when(
                        (pl.col("price_per_sqft") >= psf_threshold) |
                        (pl.col("annual_amount") >= rent_threshold)
                    )
                    .then(pl.lit(True))
                    .otherwise(pl.lit(False))
                    .alias("is_luxury")
                )
            else:
                df = df.with_columns(pl.lit(False).alias("is_luxury"))
        
        return df
        
    def _add_usage_category(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add simplified usage category (Residential/Commercial/Other)."""
        if "property_usage_en" in df.columns:
            logger.debug("Adding usage category...")
            
            df = df.with_columns(
                pl.when(pl.col("property_usage_en").str.contains("(?i)residential"))
                .then(pl.lit("Residential"))
                .when(pl.col("property_usage_en").str.contains("(?i)commercial"))
                .then(pl.lit("Commercial"))
                .otherwise(pl.lit("Other"))
                .alias("usage_category")
            )
        
        return df

    def _add_bulk_flag(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flag bulk registrations: same area+amount repeated >10 times (Naif/Hor)."""
        if "area_name_en" in df.columns and "annual_amount" in df.columns:
            logger.debug("Adding bulk registration flag...")
            # count per (area, amount) — marks whole group if count>10
            counts = df.group_by(["area_name_en", "annual_amount"]).agg(pl.len().alias("_bulk_n"))
            df = df.join(counts, on=["area_name_en", "annual_amount"], how="left")
            df = df.with_columns((pl.col("_bulk_n") > 10).alias("is_bulk_registration")).drop("_bulk_n")
        else:
            df = df.with_columns(pl.lit(False).alias("is_bulk_registration"))
        return df


def enrich_rent_contracts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convenience function to enrich rent contracts data.
    
    Args:
        df: DataFrame to enrich
        
    Returns:
        Enriched DataFrame
    """
    enricher = RentContractsEnricher(df)
    return enricher.enrich()

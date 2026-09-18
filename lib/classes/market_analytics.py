"""
Market analytics module for Dubai Real Estate.

Provides comprehensive market intelligence and analytics for Dubai rental market,
including price per square foot calculations, trend analysis, and market segmentation.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import date, datetime
import polars as pl

from lib.config import (
    COMMERCIAL_USAGE,
    MARKET_METRICS,
    RESIDENTIAL_USAGE,
    VALIDATION_THRESHOLDS,
)

logger = logging.getLogger(__name__)


class MarketAnalytics:
    """
    Comprehensive market analytics for Dubai rental market.
    
    Features:
    - Price per square foot (PSF) calculations
    - Rental trend analysis by area/community
    - Market segmentation
    - High-demand area identification
    - Occupancy rate tracking
    """
    
    def __init__(self, data: pl.DataFrame):
        """
        Initialize market analytics.
        
        Args:
            data: DataFrame containing rent contract data
        """
        self.data = data
        self._validate_data()
        
    def _validate_data(self):
        """Validate that required columns exist."""
        required_cols = ["annual_amount", "property_usage_en"]
        missing = [col for col in required_cols if col not in self.data.columns]
        
        if missing:
            raise ValueError(f"Missing required columns: {', '.join(missing)}")
            
    def calculate_psf_metrics(self) -> pl.DataFrame:
        """
        Calculate price per square foot metrics.
        
        Note: Requires 'actual_area' column. If not available, returns empty DataFrame.
        
        Returns:
            DataFrame with PSF calculations
        """
        logger.info("Calculating PSF metrics...")
        
        # Check if area column exists
        if "actual_area" not in self.data.columns:
            logger.warning("Column 'actual_area' not found. PSF calculations skipped.")
            logger.info("PSF metrics require property size data which is not available in current schema.")
            return pl.DataFrame()
        
        # Filter records with valid area and rent (area <200 is unusable per P0 gate)
        valid_data = self.data.filter(
            (pl.col("actual_area").is_not_null()) &
            (pl.col("actual_area") >= 200) &
            (pl.col("annual_amount").is_not_null()) &
            (pl.col("annual_amount") > 0)
        )
        
        if valid_data.height == 0:
            logger.warning("No valid data for PSF calculation")
            return pl.DataFrame()
        
        # Calculate PSF
        psf_data = valid_data.with_columns(
            (pl.col("annual_amount") / pl.col("actual_area")).alias("psf")
        )
        
        # Filter outliers
        min_psf_res = VALIDATION_THRESHOLDS["min_psf_residential"]
        max_psf_res = VALIDATION_THRESHOLDS["max_psf_residential"]
        min_psf_com = VALIDATION_THRESHOLDS["min_psf_commercial"]
        max_psf_com = VALIDATION_THRESHOLDS["max_psf_commercial"]
        
        psf_data = psf_data.filter(
            ((pl.col("property_usage_en").is_in(RESIDENTIAL_USAGE)) &
             (pl.col("psf") >= min_psf_res) &
             (pl.col("psf") <= max_psf_res)) |
            ((pl.col("property_usage_en").is_in(COMMERCIAL_USAGE)) &
             (pl.col("psf") >= min_psf_com) &
             (pl.col("psf") <= max_psf_com))
        )
        
        logger.info(f"Calculated PSF for {psf_data.height:,} records")
        
        return psf_data
        
    def analyze_by_area(self, area_column: str = "area_name_en") -> pl.DataFrame:
        """
        Analyze rental metrics by area.
        
        Args:
            area_column: Column name containing area information
            
        Returns:
            DataFrame with area-wise statistics
        """
        logger.info(f"Analyzing by area using column: {area_column}")
        
        if area_column not in self.data.columns:
            logger.error(f"Column {area_column} not found")
            return pl.DataFrame()
        
        # Calculate PSF first
        psf_data = self.calculate_psf_metrics()
        
        if psf_data.height == 0:
            return pl.DataFrame()
        
        # Group by area and calculate statistics
        area_stats = psf_data.group_by(area_column).agg([
            pl.len().alias("contract_count"),
            pl.col("annual_amount").mean().alias("avg_rent"),
            pl.col("annual_amount").median().alias("median_rent"),
            pl.col("annual_amount").min().alias("min_rent"),
            pl.col("annual_amount").max().alias("max_rent"),
            pl.col("psf").mean().alias("avg_psf"),
            pl.col("psf").median().alias("median_psf"),
            pl.col("actual_area").mean().alias("avg_area"),
        ]).filter(
            pl.col("contract_count") >= MARKET_METRICS["min_area_sample_size"]
        ).sort("contract_count", descending=True)
        
        logger.info(f"Analyzed {area_stats.height} areas")
        
        return area_stats
        
    def identify_high_demand_areas(
        self, 
        area_column: str = "area_name_en",
        top_n: int = 20
    ) -> pl.DataFrame:
        """
        Identify high-demand areas based on contract volume.
        
        Args:
            area_column: Column name containing area information
            top_n: Number of top areas to return
            
        Returns:
            DataFrame with top N high-demand areas
        """
        logger.info(f"Identifying top {top_n} high-demand areas...")
        
        area_stats = self.analyze_by_area(area_column)
        
        if area_stats.height == 0:
            return pl.DataFrame()
        
        top_areas = area_stats.limit(top_n)
        
        # Add market share percentage
        total_contracts = area_stats["contract_count"].sum()
        top_areas = top_areas.with_columns(
            ((pl.col("contract_count") / total_contracts) * 100).alias("market_share_pct")
        )
        
        return top_areas
        
    def analyze_by_property_type(self) -> pl.DataFrame:
        """
        Analyze rental metrics by property type.
        
        Returns:
            DataFrame with property type statistics
        """
        logger.info("Analyzing by property type...")
        
        if "ejari_property_type_en" not in self.data.columns:
            logger.warning("ejari_property_type_en column not found, trying ejari_property_sub_type_en")
            type_col = "ejari_property_sub_type_en" if "ejari_property_sub_type_en" in self.data.columns else None
            if not type_col:
                return pl.DataFrame()
        else:
            type_col = "ejari_property_type_en"
        
        # Use rent data even without PSF
        valid_data = self.data.filter(
            (pl.col("annual_amount").is_not_null()) &
            (pl.col("annual_amount") > 0)
        )
        
        if valid_data.height == 0:
            return pl.DataFrame()
        
        type_stats = valid_data.group_by(type_col).agg([
            pl.len().alias("contract_count"),
            pl.col("annual_amount").mean().alias("avg_rent"),
            pl.col("annual_amount").median().alias("median_rent"),
            pl.col("annual_amount").min().alias("min_rent"),
            pl.col("annual_amount").max().alias("max_rent"),
        ]).sort("contract_count", descending=True)
        
        # Add market share
        total_contracts = type_stats["contract_count"].sum()
        type_stats = type_stats.with_columns(
            ((pl.col("contract_count") / total_contracts) * 100).alias("market_share_pct")
        )
        
        logger.info(f"Analyzed {type_stats.height} property types")
        
        return type_stats
        
    def segment_by_usage(self) -> pl.DataFrame:
        """
        Segment market by property usage (residential vs commercial).
        
        Returns:
            DataFrame with usage segmentation
        """
        logger.info("Segmenting by property usage...")
        
        usage_stats = self.data.filter(
            (pl.col("annual_amount").is_not_null()) &
            (pl.col("annual_amount") > 0)
        ).group_by("property_usage_en").agg([
            pl.len().alias("contract_count"),
            pl.col("annual_amount").mean().alias("avg_rent"),
            pl.col("annual_amount").median().alias("median_rent"),
            pl.col("annual_amount").min().alias("min_rent"),
            pl.col("annual_amount").max().alias("max_rent"),
        ]).sort("contract_count", descending=True)
        
        # Add market share
        total_contracts = usage_stats["contract_count"].sum()
        usage_stats = usage_stats.with_columns(
            ((pl.col("contract_count") / total_contracts) * 100).alias("market_share_pct")
        )
        
        logger.info(f"Analyzed {usage_stats.height} usage categories")
        
        return usage_stats
        
    def identify_luxury_properties(self) -> pl.DataFrame:
        """
        Identify luxury properties based on PSF and rent percentiles.
        
        Returns:
            DataFrame with luxury property indicators
        """
        logger.info("Identifying luxury properties...")
        
        psf_data = self.calculate_psf_metrics()
        
        if psf_data.height == 0:
            return pl.DataFrame()
        
        # Calculate percentile thresholds
        psf_threshold = psf_data["psf"].quantile(
            MARKET_METRICS["luxury_psf_percentile"] / 100
        )
        rent_threshold = psf_data["annual_amount"].quantile(
            MARKET_METRICS["luxury_rent_percentile"] / 100
        )
        
        # Mark luxury properties
        luxury_data = psf_data.with_columns(
            ((pl.col("psf") >= psf_threshold) | 
             (pl.col("annual_amount") >= rent_threshold)).alias("is_luxury")
        )
        
        luxury_count = luxury_data.filter(pl.col("is_luxury"))["is_luxury"].sum()
        luxury_pct = (luxury_count / luxury_data.height) * 100
        
        logger.info(f"Identified {luxury_count:,} luxury properties ({luxury_pct:.1f}%)")
        
        return luxury_data
        
    def calculate_rental_trends(
        self, 
        date_column: str = "contract_start_date",
        period: str = "monthly"
    ) -> pl.DataFrame:
        """
        Calculate rental trends over time.
        
        Args:
            date_column: Column containing date information
            period: Aggregation period ('monthly', 'quarterly', 'yearly')
            
        Returns:
            DataFrame with time-series trends
        """
        logger.info(f"Calculating {period} rental trends...")
        
        if date_column not in self.data.columns:
            logger.error(f"Column {date_column} not found")
            return pl.DataFrame()
        
        valid_data = self.data.filter(
            (pl.col(date_column).is_not_null()) &
            (pl.col("annual_amount").is_not_null()) &
            (pl.col("annual_amount") > 0)
        )
        
        if valid_data.height == 0:
            return pl.DataFrame()
        
        # Add period column based on aggregation
        if period == "monthly":
            period_col = pl.col(date_column).dt.truncate("1mo")
        elif period == "quarterly":
            period_col = pl.col(date_column).dt.quarter()
        elif period == "yearly":
            period_col = pl.col(date_column).dt.year()
        else:
            logger.error(f"Invalid period: {period}")
            return pl.DataFrame()
        
        trend_data = valid_data.with_columns(
            period_col.alias("period")
        ).group_by("period").agg([
            pl.len().alias("contract_count"),
            pl.col("annual_amount").mean().alias("avg_rent"),
            pl.col("annual_amount").median().alias("median_rent"),
        ]).sort("period")
        
        logger.info(f"Calculated trends for {trend_data.height} periods")
        
        return trend_data
        
    def generate_market_summary(self) -> Dict[str, any]:
        """
        Generate comprehensive market summary.
        
        Returns:
            Dictionary with market summary statistics
        """
        logger.info("Generating market summary...")
        
        summary = {}
        
        # Overall statistics
        valid_rents = self.data.filter(
            (pl.col("annual_amount").is_not_null()) &
            (pl.col("annual_amount") > 0)
        )
        
        if valid_rents.height > 0:
            stats = valid_rents.select([
                pl.col("annual_amount").mean().alias("avg_rent"),
                pl.col("annual_amount").median().alias("median_rent"),
                pl.col("annual_amount").min().alias("min_rent"),
                pl.col("annual_amount").max().alias("max_rent"),
            ]).row(0)
            
            summary["total_contracts"] = self.data.height
            summary["avg_rent"] = float(stats[0])
            summary["median_rent"] = float(stats[1])
            summary["min_rent"] = float(stats[2])
            summary["max_rent"] = float(stats[3])
        
        # PSF statistics
        psf_data = self.calculate_psf_metrics()
        if psf_data.height > 0:
            psf_stats = psf_data.select([
                pl.col("psf").mean().alias("avg_psf"),
                pl.col("psf").median().alias("median_psf"),
            ]).row(0)
            
            summary["avg_psf"] = float(psf_stats[0])
            summary["median_psf"] = float(psf_stats[1])
        
        # Usage breakdown
        usage_stats = self.segment_by_usage()
        if usage_stats.height > 0:
            summary["usage_breakdown"] = usage_stats.to_dicts()
        
        logger.info("Market summary generated")
        
        return summary

"""
Property usage analysis with enhanced market insights.
"""

import logging
import polars as pl
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


class PropertyUsage:
    """
    Processes property usage data and generates comprehensive reports.
    
    Features:
    - Contract counts by usage type
    - Average, min, max rent statistics
    - Market share percentages
    - Property size distributions
    - Year-over-year comparisons
    """
    
    def __init__(self, output: str):
        """
        Initialize property usage analyzer.
        
        Args:
            output: Path to output file
        """
        self.output = output

    def transform(self, input_file: str, include_yoy: bool = False) -> None:
        """
        Transform property usage data and generate comprehensive report.
        
        Args:
            input_file: Path to input parquet file
            include_yoy: Whether to include year-over-year comparison
        """
        logger.info(f"Analyzing property usage from {input_file}")
        
        try:
            lf = pl.scan_parquet(input_file)
            # P0 guard: if is_bulk_registration exists (enriched), exclude bulk
            schema = lf.collect_schema().names()
            if "is_bulk_registration" in schema:
                lf = lf.filter(pl.col("is_bulk_registration") == False)  # noqa: E712

            # Basic property usage statistics
            property_usage_stats = lf.filter(
                (pl.col("property_usage_en").is_not_null()) &
                (pl.col("annual_amount").is_not_null()) &
                (pl.col("annual_amount") > 0)
            ).group_by("property_usage_en").agg([
                pl.len().alias("no_of_contracts"),
                pl.col("annual_amount").mean().alias("avg_rent"),
                pl.col("annual_amount").median().alias("median_rent"),
                pl.col("annual_amount").min().alias("min_rent"),
                pl.col("annual_amount").max().alias("max_rent"),
                pl.col("annual_amount").std().alias("std_rent"),
            ])
            
            # Collect to add calculated columns
            df = property_usage_stats.collect()
            
            # Add market share percentage
            total_contracts = df["no_of_contracts"].sum()
            df = df.with_columns(
                ((pl.col("no_of_contracts") / total_contracts) * 100).alias("market_share_pct")
            )
            
            # Add property size statistics if available
            if "actual_area" in lf.collect_schema().names():
                size_stats = lf.filter(
                    (pl.col("property_usage_en").is_not_null()) &
                    (pl.col("actual_area").is_not_null()) &
                    (pl.col("actual_area") > 0)
                ).group_by("property_usage_en").agg([
                    pl.col("actual_area").mean().alias("avg_area_sqft"),
                    pl.col("actual_area").median().alias("median_area_sqft"),
                ]).collect()
                
                # Join with main stats
                df = df.join(size_stats, on="property_usage_en", how="left")
            
            # Add PSF if area data available
            if "actual_area" in lf.collect_schema().names():
                psf_stats = lf.filter(
                    (pl.col("property_usage_en").is_not_null()) &
                    (pl.col("actual_area").is_not_null()) &
                    (pl.col("actual_area") > 0) &
                    (pl.col("annual_amount").is_not_null()) &
                    (pl.col("annual_amount") > 0)
                ).with_columns(
                    (pl.col("annual_amount") / pl.col("actual_area")).alias("psf")
                ).group_by("property_usage_en").agg([
                    pl.col("psf").mean().alias("avg_psf"),
                    pl.col("psf").median().alias("median_psf"),
                ]).collect()
                
                # Join with main stats
                df = df.join(psf_stats, on="property_usage_en", how="left")
            
            # Add report date
            df = df.with_columns(
                pl.lit(date.today()).cast(pl.Date).alias("report_date")
            )
            
            # Sort by contract count descending
            df = df.sort("no_of_contracts", descending=True)
            
            # Save to CSV
            df.write_csv(self.output)
            
            logger.info(f"Property usage report saved to {self.output}")
            logger.info(f"Analyzed {len(df)} usage categories with {total_contracts:,} total contracts")
            
            # Log top 5 categories
            logger.info("Top 5 property usage categories:")
            for row in df.head(5).iter_rows(named=True):
                logger.info(
                    f"  {row['property_usage_en']}: {row['no_of_contracts']:,} contracts "
                    f"({row['market_share_pct']:.1f}%), avg rent: AED {row['avg_rent']:,.0f}"
                )
                
        except Exception as e:
            logger.error(f"Error analyzing property usage: {e}")
            raise
            
    def compare_periods(
        self, 
        current_file: str, 
        previous_file: str,
        output_comparison: str
    ) -> None:
        """
        Compare property usage between two periods.
        
        Args:
            current_file: Path to current period data
            previous_file: Path to previous period data
            output_comparison: Path to save comparison report
        """
        logger.info("Comparing property usage across periods...")
        
        try:
            # Load both periods
            current = pl.scan_parquet(current_file).filter(
                (pl.col("property_usage_en").is_not_null()) &
                (pl.col("annual_amount").is_not_null()) &
                (pl.col("annual_amount") > 0)
            ).group_by("property_usage_en").agg([
                pl.len().alias("current_contracts"),
                pl.col("annual_amount").mean().alias("current_avg_rent"),
            ]).collect()
            
            previous = pl.scan_parquet(previous_file).filter(
                (pl.col("property_usage_en").is_not_null()) &
                (pl.col("annual_amount").is_not_null()) &
                (pl.col("annual_amount") > 0)
            ).group_by("property_usage_en").agg([
                pl.len().alias("previous_contracts"),
                pl.col("annual_amount").mean().alias("previous_avg_rent"),
            ]).collect()
            
            # Join and calculate changes
            comparison = current.join(
                previous, 
                on="property_usage_en", 
                how="outer"
            ).with_columns([
                ((pl.col("current_contracts") - pl.col("previous_contracts")) / 
                 pl.col("previous_contracts") * 100).alias("contract_change_pct"),
                ((pl.col("current_avg_rent") - pl.col("previous_avg_rent")) / 
                 pl.col("previous_avg_rent") * 100).alias("rent_change_pct"),
            ]).sort("current_contracts", descending=True)
            
            # Add report date
            comparison = comparison.with_columns(
                pl.lit(date.today()).cast(pl.Date).alias("report_date")
            )
            
            # Save comparison
            comparison.write_csv(output_comparison)
            
            logger.info(f"Period comparison saved to {output_comparison}")
            
        except Exception as e:
            logger.error(f"Error comparing periods: {e}")
            raise

        
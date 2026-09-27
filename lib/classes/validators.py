"""
Data validation module for Dubai Real Estate rent contracts.

This module provides comprehensive validation for rent contract data,
ensuring data quality and flagging anomalies specific to Dubai market.
"""

import logging
from typing import Dict, List, Tuple, Optional
import polars as pl

from lib.config import (
    VALIDATION_THRESHOLDS,
    DATA_QUALITY_RULES,
    is_residential,
    is_commercial,
)

logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for validation results."""
    
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.info: List[str] = []
        self.is_valid: bool = True
        
    def add_error(self, message: str):
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False
        
    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)
        
    def add_info(self, message: str):
        """Add an info message."""
        self.info.append(message)
        
    def get_summary(self) -> Dict[str, int]:
        """Get summary of validation results."""
        return {
            "errors": len(self.errors),
            "warnings": len(self.warnings),
            "info": len(self.info),
            "is_valid": self.is_valid,
        }
        
    def __str__(self) -> str:
        """String representation of validation results."""
        lines = []
        if self.errors:
            lines.append(f"Errors ({len(self.errors)}):")
            lines.extend(f"  - {err}" for err in self.errors)
        if self.warnings:
            lines.append(f"Warnings ({len(self.warnings)}):")
            lines.extend(f"  - {warn}" for warn in self.warnings)
        if self.info:
            lines.append(f"Info ({len(self.info)}):")
            lines.extend(f"  - {info}" for info in self.info)
        return "\n".join(lines) if lines else "All validations passed"


class RentContractValidator:
    """
    Validator for Dubai rent contract data.
    
    Aggregate checks only. Performs:
    - Required field checks
    - Date sanity (nulls, end after start, contract duration)
    - Outlier detection
    
    Per-row types and ranges (rent amounts, property sizes) are the Silver
    contract's job — see lib/classes/silver_contract.py.
    """
    
    def __init__(self, strict_mode: bool = False):
        """
        Initialize validator.
        
        Args:
            strict_mode: If True, warnings are treated as errors
        """
        self.strict_mode = strict_mode
        
    def validate_dataframe(self, df: pl.DataFrame) -> ValidationResult:
        """
        Validate entire dataframe.
        
        Args:
            df: Polars DataFrame containing rent contract data
            
        Returns:
            ValidationResult object with all validation findings
        """
        result = ValidationResult()
        
        # Check if dataframe is empty
        if df.height == 0:
            result.add_error("DataFrame is empty")
            return result
            
        result.add_info(f"Validating {df.height:,} records")
        
        # Validate schema
        self._validate_schema(df, result)
        
        # Validate required fields
        self._validate_required_fields(df, result)
        
        # Per-row types and ranges are the Silver contract's job —
        # see lib/classes/silver_contract.py. Kept here: schema presence,
        # null rates, date sanity, outliers.
        self._validate_dates(df, result)
        
        # Validate business logic
        self._validate_business_logic(df, result)
        
        # Detect outliers
        self._detect_outliers(df, result)
        
        logger.info(f"Validation complete: {result.get_summary()}")
        
        return result
        
    def _validate_schema(self, df: pl.DataFrame, result: ValidationResult):
        """Validate that expected columns exist."""
        expected_cols = DATA_QUALITY_RULES["required_fields"]
        missing_cols = [col for col in expected_cols if col not in df.columns]
        
        if missing_cols:
            result.add_error(f"Missing required columns: {', '.join(missing_cols)}")
            
    def _validate_required_fields(self, df: pl.DataFrame, result: ValidationResult):
        """Validate that required fields are not null."""
        for field in DATA_QUALITY_RULES["required_fields"]:
            if field not in df.columns:
                continue
                
            null_count = df[field].null_count()
            if null_count > 0:
                pct = (null_count / df.height) * 100
                msg = f"Field '{field}' has {null_count:,} null values ({pct:.2f}%)"
                
                if self.strict_mode:
                    result.add_error(msg)
                else:
                    result.add_warning(msg)
                    
    def _validate_dates(self, df: pl.DataFrame, result: ValidationResult):
        """Validate date fields."""
        date_fields = DATA_QUALITY_RULES.get("date_fields", [])
        
        for field in date_fields:
            if field not in df.columns:
                continue
                
            # Check for null dates
            null_count = df[field].null_count()
            if null_count > 0:
                pct = (null_count / df.height) * 100
                result.add_warning(f"Field '{field}' has {null_count:,} null dates ({pct:.2f}%)")
                
    def _validate_business_logic(self, df: pl.DataFrame, result: ValidationResult):
        """Validate business logic rules."""
        # Check if end_date > start_date
        if "contract_start_date" in df.columns and "contract_end_date" in df.columns:
            invalid_dates = df.filter(
                (pl.col("contract_start_date").is_not_null()) &
                (pl.col("contract_end_date").is_not_null()) &
                (pl.col("contract_end_date") <= pl.col("contract_start_date"))
            )
            
            if invalid_dates.height > 0:
                result.add_error(
                    f"Found {invalid_dates.height:,} records where end_date <= start_date"
                )
                
        # Check for reasonable contract durations
        if "contract_start_date" in df.columns and "contract_end_date" in df.columns:
            df_with_duration = df.filter(
                (pl.col("contract_start_date").is_not_null()) &
                (pl.col("contract_end_date").is_not_null())
            ).with_columns(
                ((pl.col("contract_end_date") - pl.col("contract_start_date")).dt.total_days()).alias("duration_days")
            )
            
            min_days = VALIDATION_THRESHOLDS["min_contract_days"]
            max_days = VALIDATION_THRESHOLDS["max_contract_days"]
            
            too_short = df_with_duration.filter(pl.col("duration_days") < min_days)
            if too_short.height > 0:
                result.add_warning(
                    f"Found {too_short.height:,} contracts shorter than {min_days} days"
                )
                
            too_long = df_with_duration.filter(pl.col("duration_days") > max_days)
            if too_long.height > 0:
                result.add_warning(
                    f"Found {too_long.height:,} contracts longer than {max_days} days"
                )
                
    def _detect_outliers(self, df: pl.DataFrame, result: ValidationResult):
        """Detect statistical outliers in rent amounts."""
        if "annual_amount" not in df.columns:
            return
            
        valid_rents = df.filter(
            (pl.col("annual_amount").is_not_null()) &
            (pl.col("annual_amount") > 0)
        )
        
        if valid_rents.height < 10:  # Need minimum sample size
            return
            
        # Calculate IQR
        stats = valid_rents.select([
            pl.col("annual_amount").quantile(0.25).alias("q1"),
            pl.col("annual_amount").quantile(0.75).alias("q3"),
        ]).row(0)
        
        q1, q3 = stats
        iqr = q3 - q1
        
        lower_bound = q1 - (3.0 * iqr)
        upper_bound = q3 + (3.0 * iqr)
        
        outliers = valid_rents.filter(
            (pl.col("annual_amount") < lower_bound) |
            (pl.col("annual_amount") > upper_bound)
        )
        
        if outliers.height > 0:
            pct = (outliers.height / valid_rents.height) * 100
            result.add_info(
                f"Detected {outliers.height:,} statistical outliers in rent amounts ({pct:.2f}%)"
            )


def validate_rent_contracts(df: pl.DataFrame, strict: bool = False) -> ValidationResult:
    """
    Convenience function to validate rent contracts.
    
    Args:
        df: DataFrame to validate
        strict: If True, warnings are treated as errors
        
    Returns:
        ValidationResult object
    """
    validator = RentContractValidator(strict_mode=strict)
    return validator.validate_dataframe(df)

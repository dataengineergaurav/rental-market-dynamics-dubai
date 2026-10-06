"""
Rents transformer — Ejari rents CSV (Rent Transaction Details) -> Parquet
Fields: REGISTRATION_DATE, START_DATE, END_DATE, AREA_EN, ANNUAL_AMOUNT, CONTRACT_AMOUNT, ACTUAL_AREA, PROP_TYPE_EN, USAGE_EN ...
"""
import logging
import polars as pl
from lib.config import FILE_CONFIG, RAW_RENTS_CSV_DTYPES

logger = logging.getLogger(__name__)

class RentsTransformer:
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file

    def transform(self) -> bool:
        try:
            logger.info(f"Transforming rents {self.input_file} -> {self.output_file}")
            lf = pl.scan_csv(
                self.input_file,
                null_values=["null", "NULL", ""],
                # polars only accepts "utf8" (strict) or "utf8-lossy". The payload
                # is valid UTF-8, so decode strictly and let a bad byte fail loudly
                # instead of being silently replaced with U+FFFD.
                encoding="utf8",
                ignore_errors=True,
                # PARKING included: null through the 100-row inference window it
                # would be read as String, which Silver cannot cast to Boolean.
                schema_overrides=RAW_RENTS_CSV_DTYPES,
            )
            # Parse rent dates (ISO like 2026-09-10T00:11:17)
            schema_cols = pl.scan_csv(self.input_file, n_rows=0).collect_schema().names()
            for col in ["REGISTRATION_DATE", "START_DATE", "END_DATE"]:
                if col in schema_cols:
                    lf = lf.with_columns(pl.col(col).str.to_datetime(strict=False))

            # Canonical lowercase aliases so downstream (PropertyUsage, validators)
            # keeps working on rents schema
            aliases = {
                "USAGE_EN": "property_usage_en",
                "ANNUAL_AMOUNT": "annual_amount",
                "ACTUAL_AREA": "actual_area",
                "AREA_EN": "area_name_en",
                "PROJECT_EN": "project_name_en",
                "MASTER_PROJECT_EN": "master_project_en",
                "PROP_TYPE_EN": "ejari_property_type_en",
                "PROP_SUB_TYPE_EN": "ejari_property_sub_type_en",
                "START_DATE": "contract_start_date",
                "END_DATE": "contract_end_date",
                "REGISTRATION_DATE": "contract_registration_date",
                "CONTRACT_AMOUNT": "contract_amount",
            }
            for src, dst in aliases.items():
                if src in schema_cols and dst not in schema_cols:
                    lf = lf.with_columns(pl.col(src).alias(dst))

            sample = lf.head(100_000).collect()
            logger.info(f"Rents sample {sample.height:,} rows, {len(sample.columns)} cols. ANNUAL_AMOUNT nulls: {sample['ANNUAL_AMOUNT'].null_count() if 'ANNUAL_AMOUNT' in sample.columns else 'n/a'}")
            if "AREA_EN" in sample.columns:
                logger.info(f"Top areas: {sample.group_by('AREA_EN').len().sort('len', descending=True).head(3).to_dict(as_series=False)}")

            lf.sink_parquet(
                self.output_file,
                compression=FILE_CONFIG["parquet_compression"],
                compression_level=FILE_CONFIG["parquet_compression_level"],
            )
            logger.info(f"Rents done -> {self.output_file}")
            return True
        except FileNotFoundError as e:
            logger.error(f"Not found: {e}")
            return False
        except Exception as e:
            logger.exception(f"Rents transform failed: {e}")
            return False

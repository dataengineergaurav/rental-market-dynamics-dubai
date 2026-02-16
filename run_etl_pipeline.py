"""
ETL Pipeline for Dubai Rental Market Data with Medallion Architecture.

This pipeline implements a three-layer medallion architecture:
- Bronze: Raw data ingestion from source
- Silver: Cleaned and validated data (single source of truth)
- Gold: Business-ready star schema + views for analytics

Optimized design:
- Single parquet export (Silver only)
- Virtual view for expiring contracts (no duplicate database)
- Clean separation with no redundant storage

Usage:
    python run_etl_pipeline.py
"""

from pathlib import Path
import polars as pl
import os
from datetime import date
from dotenv import load_dotenv
import logging

from lib.extract.rent_contracts_downloader import RentContractsDownloader
from lib.logging_helpers import get_logger, configure_root_logger
from lib.transform.rent_contracts_transformer import RentContractsTransformer
from lib.workspace import DuckDBStore, GitHubRelease
from typing import Optional

load_dotenv()

configure_root_logger(logfile="etl.log", loglevel="DEBUG")
logger = get_logger("ETL")


def download_rent_contracts(url: str, filename: str) -> bool:
    """Download rent contracts data from source.
    
    Args:
        url: Source URL for the data
        filename: Local filename to save the data
        
    Returns:
        True if download successful or file already exists
    """
    logger.info("=== PHASE: Download ===")
    
    if os.path.isfile(filename):
        logger.info(f"File already exists: {filename}. Skipping download.")
        return True

    logger.info(f"Downloading from {url} to {filename}")
    try:
        downloader = RentContractsDownloader(url)
        downloader.run(filename)
        logger.info(f"Download complete: {filename}")
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise


def medallion_pipeline(
    csv_file: str, 
    db_path: str = "rental_data.db"
) -> dict:
    """Execute full medallion architecture pipeline.
    
    Args:
        csv_file: Path to the source CSV file
        db_path: Path to the DuckDB database file
        
    Returns:
        Dictionary with row counts for each layer
    """
    logger.info("=== MEDALLION ARCHITECTURE PIPELINE ===")
    
    results = {
        "bronze": {},
        "silver": {},
        "gold": {},
        "expiring_view": 0
    }
    
    try:
        with DuckDBStore(db_path) as store:
            # =====================================================================
            # BRONZE LAYER: Raw data ingestion
            # =====================================================================
            logger.info("=== BRONZE LAYER: Raw Data Ingestion ===")
            
            bronze_rows = store.bronze_ingest_csv(csv_file, "rent_contracts")
            results["bronze"]["rent_contracts"] = bronze_rows
            
            # Export bronze layer to parquet for backup (optional, can be skipped in optimized design)
            output_path = f"bronze.parquet"
            store.export_to_parquet("bronze.rent_contracts", output_path)
            logger.info(f"Bronze layer complete: {bronze_rows:,} raw rows")
            
            # Delete the bronze layer to free up space (optional, can be skipped in optimized design)
            store.connection.execute("DROP SCHEMA IF EXISTS bronze CASCADE")
            logger.info("Bronze schema dropped to free up space")

            # =====================================================================
            # SILVER LAYER: Cleaned and validated data
            # =====================================================================
            logger.info("=== SILVER LAYER: Data Cleaning & Validation ===")
            
            silver_rows = store.silver_clean_rent_contracts()
            results["silver"]["rent_contracts"] = silver_rows
            
            # Export silver layer to parquet (single source of truth for cleaned data)
            silver_parquet_path = f"silver.parquet"
            store.export_to_parquet("silver_rent_contracts", silver_parquet_path)
            logger.info(f"Silver layer complete: {silver_rows:,} cleaned rows")

            # Delete the silver layer to free up space (optional, can be skipped in optimized design)
            store.connection.execute("DROP SCHEMA IF EXISTS silver CASCADE")
            logger.info("Silver schema dropped to free up space")
            
            # =====================================================================
            # GOLD LAYER: Star schema for analytics
            # =====================================================================
            logger.info("=== GOLD LAYER: Star Schema Creation ===")
            
            gold_results = store.gold_create_star_schema()
            results["gold"] = gold_results
            
            logger.info("Gold layer complete:")
            for table, count in gold_results.items():
                logger.info(f"  - {table}: {count:,} rows")
            
            # =====================================================================
            # EXPIRING CONTRACTS VIEW (Virtual - no physical duplication)
            # =====================================================================
            logger.info("=== EXPIRING CONTRACTS VIEW (15 days) ===")
            
            expiring_count = store.create_expiring_view(days_window=15)
            results["expiring_view"] = expiring_count
            
            logger.info(f"Expiring view created: {expiring_count:,} contracts")
            
            # Get complete summary
            summary = store.get_medallion_summary()
            
            logger.info("=== MEDALLION ARCHITECTURE SUMMARY ===")
            for layer, tables in summary.items():
                total_rows = sum(tables.values())
                logger.info(f"{layer.upper()}: {len(tables)} tables, {total_rows:,} total rows")
                for table, count in tables.items():
                    logger.info(f"  - {layer}.{table}: {count:,} rows")
            
            return results
            
    except Exception as e:
        logger.error(f"Medallion pipeline failed: {e}")
        raise



def publish_artifacts_to_github(files: list, release_notes: Optional[str] = None) -> None:
    """Publish data artifacts to GitHub Release.
    
    Args:
        files: List of file paths to publish (parquet, db, release notes)
        release_notes: Path to release notes file
    """
    logger.info("=== PUBLISH: GitHub Release ===")
    
    # Check for GitHub token
    if not os.getenv("GH_TOKEN"):
        logger.warning("GH_TOKEN not set. Skipping GitHub publication.")
        logger.info("To publish, set GH_TOKEN environment variable.")
        return
    
    try:
        # Add release notes if provided
        all_files = files.copy()
        if release_notes and os.path.exists(release_notes):
            all_files.append(release_notes)
        
        # Filter to only existing files
        existing_files = [f for f in all_files if os.path.exists(f)]
        missing_files = [f for f in all_files if not os.path.exists(f)]
        
        if missing_files:
            logger.warning(f"Missing files (skipped): {missing_files}")
        
        if not existing_files:
            logger.error("No files to publish!")
            return
        
        logger.info(f"Publishing {len(existing_files)} files to GitHub:")
        for f in existing_files:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            logger.info(f"  - {f} ({size_mb:.1f} MB)")
        
        # Publish to GitHub
        publisher = GitHubRelease('dataengineergaurav/rental-market-dynamics-dubai')
        publisher.publish(files=existing_files)
        
        logger.info("✓ GitHub publication complete!")
        
    except Exception as e:
        logger.error(f"GitHub publication failed: {e}")
        raise


def main(publish_to_github: bool = False):
    """Main ETL pipeline entry point.

    Args:
        publish_to_github: Whether to publish artifacts to GitHub release
                          (requires GH_TOKEN environment variable)
    """
    logger.info("=" * 60)
    logger.info("DUBAI RENTAL MARKET ETL PIPELINE")
    logger.info("Medallion Architecture: Bronze -> Silver -> Gold")
    logger.info("Optimized: Single parquet + Virtual views")
    if publish_to_github:
        logger.info("Mode: PUBLISH (artifacts will be uploaded to GitHub)")
    logger.info("=" * 60)

    # Get configuration
    url = os.getenv("DLD_URL")
    if not url:
        logger.error("DLD_URL environment variable not set.")
        return

    # Define file paths
    date_str = date.today().strftime('%Y%m%d')
    csv_filename = f'output/rent_contracts_{date.today()}.csv'
    db_path = "rental_data.db"
    silver_parquet = f'rent_contracts_silver_{date_str}.parquet'
    release_notes = "RELEASE_NOTES.md"

    try:
        # Phase 1: Download
        download_rent_contracts(url, csv_filename)

        # Phase 2: Medallion Architecture (Bronze -> Silver -> Gold + View)
        medallion_results = medallion_pipeline(csv_filename, db_path)

        # Phase 4: Publish to GitHub (optional)
        if publish_to_github:
            artifacts = [
                db_path,           # Full database
                silver_parquet,    # Silver parquet
            ]
            publish_artifacts_to_github(artifacts, release_notes)

        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

        # Log final summary
        logger.info("FINAL OUTPUTS:")
        logger.info(f"  - Silver Parquet: {parquet_path}")
        logger.info(f"  - Database: {db_path}")
        logger.info("  - Database Layers:")
        logger.info(f"    * Gold: {medallion_results['gold']}")
        logger.info(f"  - Expiring View (15d): {medallion_results['expiring_view']:,} contracts")

        if publish_to_github:
            logger.info("\n📦 Artifacts published to GitHub Release")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()

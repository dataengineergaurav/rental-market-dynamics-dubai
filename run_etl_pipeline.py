"""
ETL Pipeline for Dubai Rental Market Data.

This pipeline automates the extraction, transformation, and analysis of rent contracts data
using a clean, Polars-based workflow.

Stages:
1. Download: Fetches raw CSV data from the Dubai Land Department.
2. Transform: Cleans and converts CSV data to optimized Parquet format with validation.
3. Analyze: Generates property usage reports and market insights.
4. Publish: (Optional) Uploads artifacts to GitHub Release.

Usage:
    python run_etl_pipeline.py
"""

from pathlib import Path
import os
from datetime import date
from dotenv import load_dotenv
import logging

from lib.extract.rent_contracts_downloader import RentContractsDownloader
from lib.transform.rent_contracts_transformer import RentContractsTransformer
from lib.classes.property_usage import PropertyUsage
from lib.workspace import GitHubRelease
from lib.logging_helpers import get_logger, configure_root_logger

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
    logger.info("=== PHASE 1: DOWNLOAD ===")
    
    if os.path.isfile(filename):
        logger.info(f"File already exists: {filename}. Skipping download.")
        return True

    logger.info(f"Downloading from {url} to {filename}")
    try:
        downloader = RentContractsDownloader(url)
        if downloader.run(filename):
            logger.info(f"Download complete: {filename}")
            return True
        else:
            logger.error("Download failed.")
            return False
    except Exception as e:
        logger.error(f"Download failed with exception: {e}")
        raise


def transform_data(input_csv: str, output_parquet: str) -> bool:
    """Transform raw CSV to Parquet with validation.
    
    Args:
        input_csv: Path to source CSV file
        output_parquet: Path to destination Parquet file
        
    Returns:
        True if transformation successful
    """
    logger.info("=== PHASE 2: TRANSFORM ===")
    
    try:
        transformer = RentContractsTransformer(input_csv, output_parquet, validate=True)
        if transformer.transform():
            logger.info(f"Transformation complete: {output_parquet}")
            return True
        else:
            logger.error("Transformation failed.")
            return False
    except Exception as e:
        logger.error(f"Transformation failed with exception: {e}")
        raise


def analyze_property_usage(input_parquet: str, output_report: str) -> bool:
    """Generate property usage analysis report.
    
    Args:
        input_parquet: Path to clean Parquet data
        output_report: Path to output CSV report
        
    Returns:
        True if analysis successful
    """
    logger.info("=== PHASE 3: ANALYZE ===")
    
    try:
        analyzer = PropertyUsage(output_report)
        analyzer.transform(input_parquet)
        logger.info(f"Analysis complete: {output_report}")
        return True
    except Exception as e:
        logger.error(f"Analysis failed with exception: {e}")
        raise


def publish_artifacts_to_github(files: list, release_notes: str = "RELEASE_NOTES.md") -> None:
    """Publish data artifacts to GitHub Release.
    
    Args:
        files: List of file paths to publish (parquet, reports)
        release_notes: Path to release notes file
    """
    logger.info("=== PHASE 4: PUBLISH ===")
    
    if not os.getenv("GH_TOKEN"):
        logger.warning("GH_TOKEN not set. Skipping GitHub publication.")
        return
    
    try:
        # Filter to only existing files
        existing_files = [f for f in files if os.path.exists(f)]
        
        if not existing_files:
            logger.error("No files to publish!")
            return
        
        logger.info(f"Publishing {len(existing_files)} files to GitHub:")
        for f in existing_files:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            logger.info(f"  - {f} ({size_mb:.1f} MB)")
        
        publisher = GitHubRelease('dataengineergaurav/rental-market-dynamics-dubai')
        publisher.publish(files=existing_files)
        
        logger.info("✓ GitHub publication complete!")
        
    except Exception as e:
        logger.error(f"GitHub publication failed: {e}")
        raise


def main():
    """Main ETL pipeline entry point."""
    logger.info("=" * 60)
    logger.info("DUBAI RENTAL MARKET ETL PIPELINE")
    logger.info("Workflow: Download -> Transform -> Analyze -> Publish")
    logger.info("=" * 60)

    # Configuration
    url = os.getenv("DLD_URL")
    if not url:
        logger.error("DLD_URL environment variable not set. Please set it in .env file.")
        return

    # File paths
    date_str = date.today().strftime('%Y%m%d')
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    csv_filename = output_dir / f'rent_contracts_{date.today()}.csv'
    parquet_filename = f'rent_contracts_{date_str}.parquet'
    property_usage_report = f'property_usage_{date_str}.csv'
    
    try:
        # Step 1: Download
        if not download_rent_contracts(url, str(csv_filename)):
            logger.error("Pipeline stopped at Download phase.")
            return

        # Step 2: Transform
        if not transform_data(str(csv_filename), parquet_filename):
            logger.error("Pipeline stopped at Transform phase.")
            return

        # Step 3: Analyze
        if not analyze_property_usage(parquet_filename, property_usage_report):
            logger.error("Pipeline stopped at Analysis phase.")
            return

        # Step 4: Publish (Optional)
        # Check if GH_TOKEN is set to determine if we should publish
        if os.getenv("GH_TOKEN"):
            artifacts = [
                parquet_filename,
                property_usage_report
            ]
            publish_artifacts_to_github(artifacts)
        else:
            logger.info("Skipping GitHub publication (GH_TOKEN not set)")

        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Output files:")
        logger.info(f"  - Parquet: {parquet_filename}")
        logger.info(f"  - Report:  {property_usage_report}")
        logger.info("=" * 60)

    except Exception as e:
        logger.critical(f"Pipeline failed with unhandled exception: {e}")
        raise


if __name__ == "__main__":
    main()

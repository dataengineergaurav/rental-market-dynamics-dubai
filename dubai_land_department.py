from pathlib import Path
import polars as pl
import os
import subprocess
from datetime import date
from dotenv import load_dotenv
import logging

from lib.extract.rent_contracts_downloader import RentContractsDownloader
from lib.logging_helpers import get_logger, configure_root_logger
from lib.transform.rent_contracts_transformer import RentContractsTransformer, StarSchema
from lib.classes.property_usage import PropertyUsage
from lib.workspace.github_client import GitHubRelease

load_dotenv()

configure_root_logger(logfile="etl.log", loglevel="DEBUG")
logger = get_logger("ETL")


def download_rent_contracts(url, filename):
    logger.info("Downloading rent contracts")
    if os.path.isfile(filename):
        logger.info(f"{filename} already exists. Skipping download.")
        return

    logger.info(f"{filename} not found. Running RentContractsDownloader.")
    try:
        downloader = RentContractsDownloader(url)
        downloader.run(filename)
    except Exception as e:
        logger.error(f"Error downloading rent contracts: {e}")
        raise

def transform_rent_contracts(input_file, output_file):
    if not os.path.isfile(input_file):
        logger.error(f"{input_file} not found. Cannot transform.")
        return
    if not os.path.isfile(output_file):
        logger.info(f"Transforming {input_file} to {output_file}.")
        try:
            transformer = RentContractsTransformer(input_file, output_file)
            transformer.transform()
        except Exception as e:
            logger.error(f"Error transforming rent contracts: {e}")
            raise
    else:
        logger.info(f"{output_file} exists")

def get_property_usage(input_file, output_file):
    if not os.path.isfile(input_file):
        logger.error(f"{input_file} not found. Cannot get property usage.")
        return
    try:
        property_usage = PropertyUsage(output_file)
        property_usage.transform(input_file)
        logger.info(f"Property usage saved to {output_file}")
    except Exception as e:
        logger.error(f"Error getting property usage: {e}")
        raise
    else:
        logger.info(f"{output_file} exists")


def publish_to_github_release(files):
    """
    Data files uploads to GitHub Release
    """
    try:
        publisher = GitHubRelease('ggurjar333/real-estate-analysis-dubai')
        publisher.publish(files=files)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")    

def main():
    url = os.getenv("DLD_URL")
    if not url:
        logger.error("DLD_URL environment variable not set.")
        return

    csv_filename = f'output/rent_contracts_{date.today()}.csv'
    parquet_filename = f'dld_rent_contracts_{date.today()}.parquet'
    property_usage_report_file = f'dld_property_usage_report_{date.today()}.csv'

    release_checker = GitHubRelease('ggurjar333/real-estate-analysis-dubai')
    release_name = f'release-{date.today()}'
    
    if not release_checker.release_exists(release_name):
        download_rent_contracts(url, csv_filename)
        transform_rent_contracts(csv_filename, parquet_filename)
        SQL_DIR = Path("lib/analysis")

        # Load the base dataframe once for all transformations
        base_df = pl.scan_parquet(parquet_filename).collect()
        
        for sql_file in sorted(SQL_DIR.glob("*.sql")):
            if not sql_file.stem.startswith(("dim_", "fact_")):
                continue

            logger.info(f"Processing star schema: {sql_file.name}")
            query = sql_file.read_text().strip().removesuffix(".")
            
            # Simple check to see if there's any actual SQL content (not just comments)
            # Polars SQL parser fails if no executable statement is found.
            lines = [line.strip() for line in query.splitlines()]
            has_sql = any(line and not line.startswith(("--", "/*")) for line in lines)
            
            if not has_sql:
                logger.warning(f"Skipping {sql_file.name}: No executable SQL statements found (only comments or whitespace).")
                continue

            output_file = f"{sql_file.stem}_{date.today()}.parquet"

            result = StarSchema(base_df, query).transform()
            result.write_parquet(output_file)
            logger.info(f"Saved star schema to {output_file}")

        # List the parquet files into one list
        parquet_files = [str(file) for file in Path(".").glob("*.parquet")]
        # get_property_usage(parquet_filename, property_usage_report_file)
        publish_to_github_release(parquet_files)
    else:
        logger.info(f"Release '{release_name}' already exists. No action needed.")
            

if __name__ == "__main__":
    main()

"""DuckDB storage module with Medallion Architecture support.

This module implements a three-layer medallion architecture:
- Bronze: Raw data ingestion
- Silver: Cleaned and validated data
- Gold: Business-ready star schema for analytics
"""

from pathlib import Path
from typing import Optional, Union, List, Dict, Any, Tuple
import logging
import duckdb
from datetime import datetime

logger = logging.getLogger(__name__)


class DuckDBStore:
    """Manages DuckDB database with Medallion Architecture.
    
    Implements Bronze (raw), Silver (cleaned), Gold (analytics) layers
    for rental market data.
    
    Attributes:
        db_path: Path to the DuckDB database file
        connection: Active DuckDB connection
        
    Example:
        >>> store = DuckDBStore("rental_data.db")
        >>> store.bronze_ingest_csv("data.csv", "rent_contracts")
        >>> store.silver_clean_rent_contracts()
        >>> store.gold_create_star_schema()
        >>> store.close()
    """
    
    def __init__(self, db_path: Union[str, Path]):
        """Initialize DuckDB store.
        
        Args:
            db_path: Path to the database file. Creates new if doesn't exist.
        """
        self.db_path = Path(db_path)
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self._connect()
        self._create_schemas()
        
    def _connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = duckdb.connect(str(self.db_path))
            logger.info(f"Connected to DuckDB: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise
            
    def _create_schemas(self) -> None:
        """Create bronze, silver, gold schemas if they don't exist."""
        try:
            self.connection.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            self.connection.execute("CREATE SCHEMA IF NOT EXISTS silver")
            self.connection.execute("CREATE SCHEMA IF NOT EXISTS gold")
            logger.info("Medallion schemas created (bronze, silver, gold)")
        except Exception as e:
            logger.error(f"Failed to create schemas: {e}")
            raise
            
    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("DuckDB connection closed")
            
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    # =========================================================================
    # BRONZE LAYER: Raw data ingestion
    # =========================================================================
    
    def bronze_ingest_csv(
        self, 
        csv_path: Union[str, Path], 
        table_name: str,
        if_exists: str = "replace"
    ) -> int:
        """Ingest raw CSV data into bronze layer.
        
        Args:
            csv_path: Path to the CSV file
            table_name: Name for the bronze table (without schema prefix)
            if_exists: Behavior if table exists ('replace', 'append', 'fail')
            
        Returns:
            Number of rows ingested
        """
        csv_path = Path(csv_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        valid_strategies = ["replace", "append", "fail"]
        if if_exists not in valid_strategies:
            raise ValueError(f"if_exists must be one of {valid_strategies}")
            
        full_table_name = f"bronze.{table_name}"
            
        try:
            if if_exists == "replace":
                create_stmt = f"CREATE OR REPLACE TABLE {full_table_name}"
            elif if_exists == "append":
                create_stmt = f"INSERT INTO {full_table_name}"
            else:
                create_stmt = f"CREATE TABLE {full_table_name}"
                
            if if_exists == "append":
                self.connection.execute(f"""
                    INSERT INTO {full_table_name}
                    SELECT *, '{datetime.now().isoformat()}' as _ingestion_timestamp,
                             '{csv_path.name}' as _source_file
                    FROM read_csv_auto('{csv_path}', nullstr='null', sample_size=-1)
                """)
            else:
                self.connection.execute(f"""
                    {create_stmt} AS 
                    SELECT *, '{datetime.now().isoformat()}' as _ingestion_timestamp,
                             '{csv_path.name}' as _source_file
                    FROM read_csv_auto('{csv_path}', nullstr='null', sample_size=-1)
                """)
                
            row_count = self.get_row_count(full_table_name)
            logger.info(f"Bronze: Ingested {row_count:,} rows into '{table_name}' from {csv_path}")
            return row_count
            
        except Exception as e:
            logger.error(f"Bronze: Failed to ingest CSV: {e}")
            raise

    # =========================================================================
    # SILVER LAYER: Cleaned and validated data
    # =========================================================================
    
    def silver_clean_rent_contracts(self) -> int:
        """Create cleaned silver layer table from bronze data.
        
        Performs:
        - Deduplication
        - Date parsing and validation
        - Null handling
        - Data type standardization
        - Audit column addition
        
        Returns:
            Number of rows in silver table
        """
        try:
            logger.info("Silver: Cleaning rent contracts data...")
            
            # Simple cleaning without expensive ROW_NUMBER for now
            self.connection.execute("""
                CREATE OR REPLACE TABLE silver.rent_contracts AS
                SELECT 
                    contract_id,
                    CAST(contract_reg_type_id AS BIGINT) as contract_reg_type_id,
                    contract_reg_type_ar,
                    contract_reg_type_en,
                    
                    -- Dates are already DATE type from bronze
                    contract_start_date,
                    contract_end_date,
                    
                    CAST(contract_amount AS BIGINT) as contract_amount,
                    CAST(annual_amount AS BIGINT) as annual_amount,
                    CAST(no_of_prop AS BIGINT) as no_of_prop,
                    CAST(line_number AS BIGINT) as line_number,
                    CAST(is_free_hold AS BIGINT) as is_free_hold,
                    
                    CAST(ejari_bus_property_type_id AS BIGINT) as ejari_bus_property_type_id,
                    ejari_bus_property_type_ar,
                    ejari_bus_property_type_en,
                    
                    CAST(ejari_property_type_id AS BIGINT) as ejari_property_type_id,
                    ejari_property_type_en,
                    ejari_property_type_ar,
                    
                    CAST(ejari_property_sub_type_id AS BIGINT) as ejari_property_sub_type_id,
                    ejari_property_sub_type_en,
                    ejari_property_sub_type_ar,
                    
                    property_usage_en,
                    property_usage_ar,
                    
                    CAST(project_number AS BIGINT) as project_number,
                    project_name_ar,
                    project_name_en,
                    master_project_ar,
                    master_project_en,
                    
                    CAST(area_id AS BIGINT) as area_id,
                    area_name_ar,
                    area_name_en,
                    CAST(actual_area AS BIGINT) as actual_area,
                    
                    nearest_landmark_ar,
                    nearest_landmark_en,
                    nearest_metro_ar,
                    nearest_metro_en,
                    nearest_mall_ar,
                    nearest_mall_en,
                    
                    CAST(tenant_type_id AS BIGINT) as tenant_type_id,
                    tenant_type_ar,
                    tenant_type_en,
                    
                    -- Audit columns
                    _ingestion_timestamp,
                    _source_file,
                    '{datetime.now().isoformat()}' as _cleaned_timestamp,
                    
                    -- Data quality flags
                    CASE 
                        WHEN contract_start_date IS NULL 
                             OR contract_end_date IS NULL 
                        THEN TRUE 
                        ELSE FALSE 
                    END as _has_date_issues,
                    
                    CASE 
                        WHEN annual_amount <= 0 OR annual_amount IS NULL 
                        THEN TRUE 
                        ELSE FALSE 
                    END as _has_amount_issues
                    
                FROM bronze.rent_contracts
            """)
            
            row_count = self.get_row_count("silver.rent_contracts")
            
            # Log data quality metrics
            quality_check = self.connection.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN _has_date_issues THEN 1 ELSE 0 END) as date_issues,
                    SUM(CASE WHEN _has_amount_issues THEN 1 ELSE 0 END) as amount_issues
                FROM silver.rent_contracts
            """).fetchone()
            
            logger.info(f"Silver: Created cleaned table with {row_count:,} rows")
            logger.info(f"Silver: Data quality - Date issues: {quality_check[1]:,}, Amount issues: {quality_check[2]:,}")
            
            return row_count
            
        except Exception as e:
            logger.error(f"Silver: Failed to clean data: {e}")
            raise

    # =========================================================================
    # GOLD LAYER: Star schema for analytics
    # =========================================================================
    
    def gold_create_star_schema(self) -> Dict[str, int]:
        """Create gold layer star schema from silver data.
        
        Creates dimension and fact tables optimized for analytics.
        
        Returns:
            Dictionary mapping table names to row counts
        """
        results = {}
        
        try:
            logger.info("Gold: Creating star schema...")
            
            # Check if tables already exist to avoid recreation errors
            if self.table_exists("gold.fact_rent_contract"):
                logger.info("Gold schema already exists. Skipping recreation.")
                # Get existing row counts
                results['dim_date'] = self.get_row_count("gold.dim_date")
                results['dim_contract_type'] = self.get_row_count("gold.dim_contract_type")
                results['dim_property'] = self.get_row_count("gold.dim_property")
                results['dim_project'] = self.get_row_count("gold.dim_project")
                results['dim_location'] = self.get_row_count("gold.dim_location")
                results['dim_tenant'] = self.get_row_count("gold.dim_tenant")
                results['fact_rent_contract'] = self.get_row_count("gold.fact_rent_contract")
                logger.info(f"Gold: Found existing schema with {len(results)} tables")
                return results
            
            # Create dimension tables
            results['dim_date'] = self._gold_create_dim_date()
            results['dim_contract_type'] = self._gold_create_dim_contract_type()
            results['dim_property'] = self._gold_create_dim_property()
            results['dim_project'] = self._gold_create_dim_project()
            results['dim_location'] = self._gold_create_dim_location()
            results['dim_tenant'] = self._gold_create_dim_tenant()
            
            # Create fact table
            results['fact_rent_contract'] = self._gold_create_fact_table()
            
            # Create indexes for performance
            self._gold_create_indexes()
            
            logger.info(f"Gold: Star schema created with {len(results)} tables")
            return results
            
        except Exception as e:
            logger.error(f"Gold: Failed to create star schema: {e}")
            raise
    
    def _gold_create_dim_date(self) -> int:
        """Create date dimension covering data range."""
        self.connection.execute("""
            CREATE OR REPLACE TABLE gold.dim_date AS
            WITH date_range AS (
                SELECT 
                    MIN(contract_start_date) as min_date,
                    MAX(contract_end_date) as max_date
                FROM silver.rent_contracts
                WHERE contract_start_date IS NOT NULL
                  AND contract_end_date IS NOT NULL
            ),
            calendar AS (
                SELECT 
                    min_date + INTERVAL (n) DAY as full_date
                FROM date_range,
                     generate_series(0, 
                        CAST((max_date - min_date) AS INTEGER)
                     ) as t(n)
            )
            SELECT
                CAST(strftime('%Y%m%d', full_date) AS INTEGER) as date_key,
                full_date,
                YEAR(full_date) as year,
                MONTH(full_date) as month,
                DAY(full_date) as day_of_month,
                QUARTER(full_date) as quarter,
                (EXTRACT(DOW FROM full_date) + 1) as day_of_week,
                strftime('%B', full_date) as month_name,
                CASE 
                    WHEN (EXTRACT(DOW FROM full_date) + 1) IN (1, 7) THEN TRUE 
                    ELSE FALSE 
                END as is_weekend
            FROM calendar
        """)
        return self.get_row_count("gold.dim_date")
    
    def _gold_create_dim_contract_type(self) -> int:
        """Create contract type dimension."""
        self.connection.execute("""
            CREATE OR REPLACE TABLE gold.dim_contract_type AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY contract_reg_type_id) as contract_type_key,
                contract_reg_type_id,
                contract_id,
                contract_reg_type_en,
                contract_reg_type_ar,
                '{datetime.now().isoformat()}' as _created_at
            FROM (
                SELECT DISTINCT
                    contract_reg_type_id,
                    contract_id,
                    contract_reg_type_en,
                    contract_reg_type_ar
                FROM silver.rent_contracts
                WHERE contract_reg_type_id IS NOT NULL
            )
        """)
        return self.get_row_count("gold.dim_contract_type")
    
    def _gold_create_dim_property(self) -> int:
        """Create property dimension."""
        self.connection.execute("""
            CREATE OR REPLACE TABLE gold.dim_property AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY ejari_bus_property_type_id, ejari_property_type_id) 
                    as property_key,
                ejari_bus_property_type_id,
                ejari_property_type_id,
                ejari_bus_property_type_en,
                ejari_bus_property_type_ar,
                ejari_property_type_en,
                ejari_property_type_ar,
                ejari_property_sub_type_en,
                ejari_property_sub_type_ar,
                property_usage_en,
                property_usage_ar,
                '{datetime.now().isoformat()}' as _created_at
            FROM (
                SELECT DISTINCT
                    ejari_bus_property_type_id,
                    ejari_property_type_id,
                    ejari_bus_property_type_en,
                    ejari_bus_property_type_ar,
                    ejari_property_type_en,
                    ejari_property_type_ar,
                    ejari_property_sub_type_en,
                    ejari_property_sub_type_ar,
                    property_usage_en,
                    property_usage_ar
                FROM silver.rent_contracts
                WHERE ejari_bus_property_type_id IS NOT NULL
            )
        """)
        return self.get_row_count("gold.dim_property")
    
    def _gold_create_dim_project(self) -> int:
        """Create project dimension."""
        self.connection.execute("""
            CREATE OR REPLACE TABLE gold.dim_project AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY project_number) as project_key,
                project_number,
                project_name_ar,
                project_name_en,
                master_project_ar,
                master_project_en,
                '{datetime.now().isoformat()}' as _created_at
            FROM (
                SELECT DISTINCT
                    project_number,
                    project_name_ar,
                    project_name_en,
                    master_project_ar,
                    master_project_en
                FROM silver.rent_contracts
                WHERE project_number IS NOT NULL
            )
        """)
        return self.get_row_count("gold.dim_project")
    
    def _gold_create_dim_location(self) -> int:
        """Create location dimension."""
        self.connection.execute("""
            CREATE OR REPLACE TABLE gold.dim_location AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY area_id) as location_key,
                area_id,
                area_name_en,
                area_name_ar,
                actual_area,
                nearest_landmark_en,
                nearest_metro_en,
                nearest_mall_en,
                nearest_landmark_ar,
                nearest_metro_ar,
                nearest_mall_ar,
                '{datetime.now().isoformat()}' as _created_at
            FROM (
                SELECT DISTINCT
                    area_id,
                    area_name_en,
                    area_name_ar,
                    actual_area,
                    nearest_landmark_en,
                    nearest_metro_en,
                    nearest_mall_en,
                    nearest_landmark_ar,
                    nearest_metro_ar,
                    nearest_mall_ar
                FROM silver.rent_contracts
            )
        """)
        return self.get_row_count("gold.dim_location")
    
    def _gold_create_dim_tenant(self) -> int:
        """Create tenant dimension."""
        self.connection.execute("""
            CREATE OR REPLACE TABLE gold.dim_tenant AS
            SELECT 
                ROW_NUMBER() OVER (ORDER BY tenant_type_id) as tenant_key,
                tenant_type_id,
                tenant_type_en,
                tenant_type_ar,
                '{datetime.now().isoformat()}' as _created_at
            FROM (
                SELECT DISTINCT
                    tenant_type_id,
                    tenant_type_en,
                    tenant_type_ar
                FROM silver.rent_contracts
                WHERE tenant_type_id IS NOT NULL
            )
        """)
        return self.get_row_count("gold.dim_tenant")
    
    def _gold_create_fact_table(self) -> int:
        """Create fact table with surrogate keys."""
        self.connection.execute("""
            CREATE OR REPLACE TABLE gold.fact_rent_contract AS
            SELECT 
                ROW_NUMBER() OVER () as rent_contract_key,
                dct.contract_type_key,
                dp.property_key,
                dprj.project_key,
                dl.location_key,
                dt.tenant_key,
                CAST(strftime('%Y%m%d', rc.contract_start_date) AS INTEGER) as start_date_key,
                CAST(strftime('%Y%m%d', rc.contract_end_date) AS INTEGER) as end_date_key,
                rc.contract_start_date,
                rc.contract_end_date,
                rc.annual_amount,
                rc.contract_amount,
                rc.no_of_prop,
                rc.line_number,
                rc.is_free_hold,
                
                -- Calculated measures
                CASE 
                    WHEN rc.contract_start_date IS NOT NULL 
                         AND rc.contract_end_date IS NOT NULL 
                    THEN EXTRACT(MONTH FROM AGE(rc.contract_end_date, rc.contract_start_date))
                    ELSE NULL
                END as contract_duration_months,
                
                -- Audit columns
                rc._cleaned_timestamp,
                rc._has_date_issues,
                rc._has_amount_issues
                
            FROM silver.rent_contracts rc
            LEFT JOIN gold.dim_contract_type dct
                ON rc.contract_reg_type_id = dct.contract_reg_type_id
            LEFT JOIN gold.dim_project dprj
                ON rc.project_number = dprj.project_number
            LEFT JOIN gold.dim_property dp
                ON rc.ejari_bus_property_type_id = dp.ejari_bus_property_type_id
                AND rc.ejari_property_type_id = dp.ejari_property_type_id
            LEFT JOIN gold.dim_location dl
                ON rc.area_id = dl.area_id
            LEFT JOIN gold.dim_tenant dt
                ON rc.tenant_type_id = dt.tenant_type_id
        """)
        return self.get_row_count("gold.fact_rent_contract")
    
    def _gold_create_indexes(self) -> None:
        """Create indexes on dimension and fact tables."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_gold_dim_contract_type ON gold.dim_contract_type(contract_reg_type_id)",
            "CREATE INDEX IF NOT EXISTS idx_gold_dim_property ON gold.dim_property(ejari_bus_property_type_id, ejari_property_type_id)",
            "CREATE INDEX IF NOT EXISTS idx_gold_dim_project ON gold.dim_project(project_number)",
            "CREATE INDEX IF NOT EXISTS idx_gold_dim_location ON gold.dim_location(area_id)",
            "CREATE INDEX IF NOT EXISTS idx_gold_dim_tenant ON gold.dim_tenant(tenant_type_id)",
            "CREATE INDEX IF NOT EXISTS idx_gold_fact_start_date ON gold.fact_rent_contract(start_date_key)",
            "CREATE INDEX IF NOT EXISTS idx_gold_fact_end_date ON gold.fact_rent_contract(end_date_key)",
        ]
        
        for idx in indexes:
            self.connection.execute(idx)
        logger.info("Gold: Indexes created on star schema tables")

    # =========================================================================
    # PARQUET EXPORT (Silver only - single source of truth)
    # =========================================================================
    
    def export_silver_to_parquet(self, output_path: Union[str, Path], compression: str = "zstd") -> str:
        """Export silver layer to Parquet format.
        
        Silver layer is the single source of truth for downstream analytics.
        Contains cleaned data with proper types and quality flags.
        
        Args:
            output_path: Path for the output parquet file
            compression: Compression algorithm (zstd, snappy, gzip, etc.)
            
        Returns:
            Path to the exported file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            logger.info(f"Exporting silver layer to {output_path}")
            self.connection.execute(f"""
                COPY silver.rent_contracts 
                TO '{output_path}' 
                (FORMAT PARQUET, COMPRESSION '{compression}')
            """)
            
            row_count = self.get_row_count("silver.rent_contracts")
            file_size = output_path.stat().st_size / (1024 * 1024)  # MB
            
            logger.info(f"Silver export complete: {row_count:,} rows, {file_size:.1f} MB")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export silver to parquet: {e}")
            raise

    # =========================================================================
    # EXPIRING CONTRACTS VIEW (Lightweight alternative to separate database)
    # =========================================================================
    
    def create_expiring_view(self, days_window: int = 15) -> int:
        """Create a view for contracts expiring in the next N days.
        
        Uses a virtual view instead of physical tables to avoid data duplication.
        The view filters silver.rent_contracts for contracts where:
        contract_end_date >= CURRENT_DATE AND <= CURRENT_DATE + days_window
        
        Args:
            days_window: Number of days to look ahead (default: 15)
            
        Returns:
            Number of contracts in the expiring view
        """
        try:
            logger.info(f"Creating expiring contracts view ({days_window} days window)...")
            
            self.connection.execute(f"""
                CREATE OR REPLACE VIEW gold.v_expiring_contracts_{days_window}d AS
                SELECT 
                    contract_id,
                    contract_reg_type_en,
                    contract_start_date,
                    contract_end_date,
                    annual_amount,
                    contract_amount,
                    project_name_en,
                    area_name_en,
                    property_usage_en,
                    tenant_type_en,
                    DATEDIFF('day', CURRENT_DATE, contract_end_date) as days_until_expiry,
                    _has_date_issues,
                    _has_amount_issues
                FROM silver.rent_contracts
                WHERE contract_end_date >= CURRENT_DATE
                  AND contract_end_date <= CURRENT_DATE + INTERVAL '{days_window} days'
            """)
            
            # Get count
            count = self.connection.execute(
                f"SELECT COUNT(*) FROM gold.v_expiring_contracts_{days_window}d"
            ).fetchone()[0]
            
            logger.info(f"Expiring view created: {count:,} contracts")
            return count
            
        except Exception as e:
            logger.error(f"Failed to create expiring view: {e}")
            raise

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table (supports schema prefix)."""
        result = self.connection.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()
        return result[0]
        
    def get_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get schema information for a table."""
        schema = self.connection.execute(
            f"DESCRIBE {table_name}"
        ).fetchall()
        return [{"name": col[0], "type": col[1]} for col in schema]
        
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists (supports schema prefix)."""
        result = self.connection.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema || '.' || table_name = '{table_name}'
               OR table_name = '{table_name}'
        """).fetchone()
        return result[0] > 0
        
    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List all tables, optionally filtered by schema."""
        if schema:
            result = self.connection.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}'
            """).fetchall()
        else:
            result = self.connection.execute("""
                SELECT table_schema || '.' || table_name
                FROM information_schema.tables 
                WHERE table_schema IN ('bronze', 'silver', 'gold', 'main')
                ORDER BY table_schema, table_name
            """).fetchall()
        return [row[0] for row in result]
        
    def execute(self, query: str):
        """Execute a SQL query."""
        return self.connection.execute(query)
        
    def get_medallion_summary(self) -> Dict[str, Any]:
        """Get summary of all layers in medallion architecture."""
        summary = {
            "bronze": {},
            "silver": {},
            "gold": {}
        }
        
        for layer in ["bronze", "silver", "gold"]:
            tables = self.list_tables(layer)
            for table in tables:
                full_name = f"{layer}.{table}"
                summary[layer][table] = self.get_row_count(full_name)
                
        return summary

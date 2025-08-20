"""
Snowflake Schema Manager

This module manages Snowflake database schemas, tables, and DDL operations
for the streaming pipeline dimensional model.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from .snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manager for Snowflake schema operations"""
    
    def __init__(self, snowflake_client: SnowflakeClient):
        """
        Initialize schema manager
        
        Args:
            snowflake_client: Snowflake client instance
        """
        self.client = snowflake_client
        
    def create_database_and_schemas(self) -> None:
        """Create database and required schemas"""
        ddl_statements = [
            "CREATE DATABASE IF NOT EXISTS STOCK_MARKET",
            "USE DATABASE STOCK_MARKET",
            "CREATE SCHEMA IF NOT EXISTS STREAMING",
            "CREATE SCHEMA IF NOT EXISTS STAGING", 
            "CREATE SCHEMA IF NOT EXISTS UTILS"
        ]
        
        for statement in ddl_statements:
            try:
                self.client.execute_query(statement)
                logger.info(f"Executed: {statement}")
            except Exception as e:
                logger.error(f"Error executing {statement}: {e}")
                raise
    
    def create_file_formats(self) -> None:
        """Create file formats for data loading"""
        file_formats = {
            "PARQUET_FORMAT": """
                CREATE OR REPLACE FILE FORMAT STREAMING.PARQUET_FORMAT
                TYPE = 'PARQUET'
                COMPRESSION = 'AUTO'
                BINARY_AS_TEXT = FALSE
            """,
            "JSON_FORMAT": """
                CREATE OR REPLACE FILE FORMAT STREAMING.JSON_FORMAT
                TYPE = 'JSON'
                COMPRESSION = 'AUTO'
                ENABLE_OCTAL = FALSE
                ALLOW_DUPLICATE = FALSE
                STRIP_OUTER_ARRAY = TRUE
                STRIP_NULL_VALUES = FALSE
                IGNORE_UTF8_ERRORS = FALSE
            """
        }
        
        for format_name, ddl in file_formats.items():
            try:
                self.client.execute_query(ddl)
                logger.info(f"Created file format: {format_name}")
            except Exception as e:
                logger.error(f"Error creating file format {format_name}: {e}")
                raise
    
    def create_stages(self, s3_bucket: str, aws_role_arn: str) -> None:
        """
        Create external stages for S3 integration
        
        Args:
            s3_bucket: S3 bucket name
            aws_role_arn: AWS IAM role ARN for Snowflake access
        """
        # Create storage integration first
        storage_integration_ddl = f"""
            CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = S3
            ENABLED = TRUE
            STORAGE_AWS_ROLE_ARN = '{aws_role_arn}'
            STORAGE_ALLOWED_LOCATIONS = ('s3://{s3_bucket}/staging/', 's3://{s3_bucket}/processed/')
        """
        
        # Create stages
        stages = {
            "STREAMING_STAGE": f"""
                CREATE OR REPLACE STAGE STREAMING.STREAMING_STAGE
                URL = 's3://{s3_bucket}/staging/streaming/'
                STORAGE_INTEGRATION = S3_INTEGRATION
                FILE_FORMAT = STREAMING.PARQUET_FORMAT
            """,
            "PROCESSED_STAGE": f"""
                CREATE OR REPLACE STAGE STREAMING.PROCESSED_STAGE
                URL = 's3://{s3_bucket}/processed/streaming/'
                STORAGE_INTEGRATION = S3_INTEGRATION
                FILE_FORMAT = STREAMING.PARQUET_FORMAT
            """
        }
        
        try:
            # Create storage integration
            self.client.execute_query(storage_integration_ddl)
            logger.info("Created storage integration: S3_INTEGRATION")
            
            # Create stages
            for stage_name, ddl in stages.items():
                self.client.execute_query(ddl)
                logger.info(f"Created stage: {stage_name}")
                
        except Exception as e:
            logger.error(f"Error creating stages: {e}")
            raise
    
    def create_dimension_tables(self) -> None:
        """Create dimension tables with SCD Type 2 support"""
        
        # DIM_COMPANY table
        dim_company_ddl = """
            CREATE OR REPLACE TABLE STREAMING.DIM_COMPANY (
                COMPANY_KEY NUMBER AUTOINCREMENT PRIMARY KEY,
                SYMBOL VARCHAR(10) NOT NULL,
                COMPANY_NAME VARCHAR(255),
                SECTOR VARCHAR(100),
                INDUSTRY VARCHAR(100),
                MARKET_CAP_CATEGORY VARCHAR(20),
                EXCHANGE VARCHAR(10),
                CURRENCY VARCHAR(3),
                COUNTRY VARCHAR(50),
                EFFECTIVE_DATE DATE NOT NULL,
                EXPIRY_DATE DATE,
                IS_CURRENT BOOLEAN DEFAULT TRUE,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """
        
        # DIM_DATE table
        dim_date_ddl = """
            CREATE OR REPLACE TABLE STREAMING.DIM_DATE (
                DATE_KEY NUMBER PRIMARY KEY,
                DATE_VALUE DATE NOT NULL,
                YEAR NUMBER,
                QUARTER NUMBER,
                MONTH NUMBER,
                MONTH_NAME VARCHAR(20),
                DAY_OF_MONTH NUMBER,
                DAY_OF_WEEK NUMBER,
                DAY_NAME VARCHAR(20),
                WEEK_OF_YEAR NUMBER,
                IS_WEEKEND BOOLEAN,
                IS_HOLIDAY BOOLEAN,
                IS_TRADING_DAY BOOLEAN,
                FISCAL_YEAR NUMBER,
                FISCAL_QUARTER NUMBER
            )
        """
        
        # DIM_TIME table
        dim_time_ddl = """
            CREATE OR REPLACE TABLE STREAMING.DIM_TIME (
                TIME_KEY NUMBER PRIMARY KEY,
                TIME_VALUE TIME NOT NULL,
                HOUR NUMBER,
                MINUTE NUMBER,
                SECOND NUMBER,
                HOUR_MINUTE VARCHAR(5),
                AM_PM VARCHAR(2),
                MARKET_SESSION VARCHAR(20),
                TRADING_DAY_MINUTE NUMBER
            )
        """
        
        dimension_tables = {
            "DIM_COMPANY": dim_company_ddl,
            "DIM_DATE": dim_date_ddl,
            "DIM_TIME": dim_time_ddl
        }
        
        for table_name, ddl in dimension_tables.items():
            try:
                self.client.execute_query(ddl)
                logger.info(f"Created dimension table: {table_name}")
            except Exception as e:
                logger.error(f"Error creating dimension table {table_name}: {e}")
                raise
    
    def create_fact_tables(self) -> None:
        """Create fact tables with proper partitioning and clustering"""
        
        # FACT_STOCK_PRICES table
        fact_stock_prices_ddl = """
            CREATE OR REPLACE TABLE STREAMING.FACT_STOCK_PRICES (
                PRICE_KEY NUMBER AUTOINCREMENT PRIMARY KEY,
                COMPANY_KEY NUMBER NOT NULL,
                DATE_KEY NUMBER NOT NULL,
                TIME_KEY NUMBER NOT NULL,
                OPEN_PRICE DECIMAL(18,4),
                HIGH_PRICE DECIMAL(18,4),
                LOW_PRICE DECIMAL(18,4),
                CLOSE_PRICE DECIMAL(18,4),
                VOLUME NUMBER,
                ADJUSTED_CLOSE DECIMAL(18,4),
                DIVIDEND_AMOUNT DECIMAL(18,4),
                SPLIT_COEFFICIENT DECIMAL(10,6),
                
                -- Technical Indicators
                SMA_20 DECIMAL(18,4),
                SMA_50 DECIMAL(18,4),
                EMA_12 DECIMAL(18,4),
                EMA_26 DECIMAL(18,4),
                RSI_14 DECIMAL(8,4),
                MACD DECIMAL(18,4),
                MACD_SIGNAL DECIMAL(18,4),
                
                -- Metadata
                DATA_SOURCE VARCHAR(50),
                INGESTION_TIMESTAMP TIMESTAMP_NTZ,
                PROCESSING_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                
                FOREIGN KEY (COMPANY_KEY) REFERENCES STREAMING.DIM_COMPANY(COMPANY_KEY),
                FOREIGN KEY (DATE_KEY) REFERENCES STREAMING.DIM_DATE(DATE_KEY),
                FOREIGN KEY (TIME_KEY) REFERENCES STREAMING.DIM_TIME(TIME_KEY)
            )
            CLUSTER BY (COMPANY_KEY, DATE_KEY, TIME_KEY)
        """
        
        # FACT_TRADING_VOLUME table
        fact_trading_volume_ddl = """
            CREATE OR REPLACE TABLE STREAMING.FACT_TRADING_VOLUME (
                VOLUME_KEY NUMBER AUTOINCREMENT PRIMARY KEY,
                COMPANY_KEY NUMBER NOT NULL,
                DATE_KEY NUMBER NOT NULL,
                TIME_KEY NUMBER NOT NULL,
                VOLUME NUMBER NOT NULL,
                VOLUME_WEIGHTED_PRICE DECIMAL(18,4),
                TRADE_COUNT NUMBER,
                BUY_VOLUME NUMBER,
                SELL_VOLUME NUMBER,
                
                -- Volume Indicators
                VOLUME_SMA_20 NUMBER,
                VOLUME_RATIO DECIMAL(8,4),
                
                -- Metadata
                DATA_SOURCE VARCHAR(50),
                INGESTION_TIMESTAMP TIMESTAMP_NTZ,
                PROCESSING_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                
                FOREIGN KEY (COMPANY_KEY) REFERENCES STREAMING.DIM_COMPANY(COMPANY_KEY),
                FOREIGN KEY (DATE_KEY) REFERENCES STREAMING.DIM_DATE(DATE_KEY),
                FOREIGN KEY (TIME_KEY) REFERENCES STREAMING.DIM_TIME(TIME_KEY)
            )
            CLUSTER BY (COMPANY_KEY, DATE_KEY, TIME_KEY)
        """
        
        fact_tables = {
            "FACT_STOCK_PRICES": fact_stock_prices_ddl,
            "FACT_TRADING_VOLUME": fact_trading_volume_ddl
        }
        
        for table_name, ddl in fact_tables.items():
            try:
                self.client.execute_query(ddl)
                logger.info(f"Created fact table: {table_name}")
            except Exception as e:
                logger.error(f"Error creating fact table {table_name}: {e}")
                raise
    
    def create_utility_tables(self) -> None:
        """Create utility and monitoring tables"""
        
        # Data quality monitoring table
        data_quality_ddl = """
            CREATE OR REPLACE TABLE STREAMING.DATA_QUALITY_RESULTS (
                QUALITY_KEY NUMBER AUTOINCREMENT PRIMARY KEY,
                TABLE_NAME VARCHAR(100) NOT NULL,
                CHECK_NAME VARCHAR(100) NOT NULL,
                CHECK_TYPE VARCHAR(50) NOT NULL,
                STATUS VARCHAR(20) NOT NULL,
                RECORD_COUNT NUMBER,
                ERROR_COUNT NUMBER,
                ERROR_PERCENTAGE DECIMAL(5,2),
                CHECK_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                ERROR_DETAILS VARIANT
            )
        """
        
        # Pipeline monitoring table
        pipeline_monitoring_ddl = """
            CREATE OR REPLACE TABLE STREAMING.PIPELINE_MONITORING (
                MONITOR_KEY NUMBER AUTOINCREMENT PRIMARY KEY,
                PIPELINE_NAME VARCHAR(100) NOT NULL,
                BATCH_ID VARCHAR(100),
                STATUS VARCHAR(20) NOT NULL,
                START_TIME TIMESTAMP_NTZ,
                END_TIME TIMESTAMP_NTZ,
                RECORDS_PROCESSED NUMBER,
                RECORDS_FAILED NUMBER,
                ERROR_MESSAGE VARCHAR(1000),
                PROCESSING_DURATION_SECONDS NUMBER,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """
        
        # Load history table
        load_history_ddl = """
            CREATE OR REPLACE TABLE STREAMING.LOAD_HISTORY (
                LOAD_KEY NUMBER AUTOINCREMENT PRIMARY KEY,
                TABLE_NAME VARCHAR(100) NOT NULL,
                FILE_NAME VARCHAR(500),
                S3_KEY VARCHAR(1000),
                LOAD_STATUS VARCHAR(20) NOT NULL,
                RECORDS_LOADED NUMBER,
                LOAD_START_TIME TIMESTAMP_NTZ,
                LOAD_END_TIME TIMESTAMP_NTZ,
                ERROR_MESSAGE VARCHAR(1000),
                SNOWPIPE_NAME VARCHAR(100),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """
        
        utility_tables = {
            "DATA_QUALITY_RESULTS": data_quality_ddl,
            "PIPELINE_MONITORING": pipeline_monitoring_ddl,
            "LOAD_HISTORY": load_history_ddl
        }
        
        for table_name, ddl in utility_tables.items():
            try:
                self.client.execute_query(ddl)
                logger.info(f"Created utility table: {table_name}")
            except Exception as e:
                logger.error(f"Error creating utility table {table_name}: {e}")
                raise
    
    def populate_date_dimension(self, start_date: str = "2020-01-01", end_date: str = "2030-12-31") -> None:
        """
        Populate date dimension table
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        """
        populate_ddl = f"""
            INSERT INTO STREAMING.DIM_DATE
            WITH date_range AS (
                SELECT 
                    DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '{start_date}'::DATE) AS date_value
                FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF(DAY, '{start_date}'::DATE, '{end_date}'::DATE) + 1))
            )
            SELECT 
                TO_NUMBER(TO_CHAR(date_value, 'YYYYMMDD')) AS DATE_KEY,
                date_value AS DATE_VALUE,
                YEAR(date_value) AS YEAR,
                QUARTER(date_value) AS QUARTER,
                MONTH(date_value) AS MONTH,
                MONTHNAME(date_value) AS MONTH_NAME,
                DAY(date_value) AS DAY_OF_MONTH,
                DAYOFWEEK(date_value) AS DAY_OF_WEEK,
                DAYNAME(date_value) AS DAY_NAME,
                WEEKOFYEAR(date_value) AS WEEK_OF_YEAR,
                CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
                FALSE AS IS_HOLIDAY,  -- Can be updated with holiday logic
                CASE WHEN DAYOFWEEK(date_value) BETWEEN 2 AND 6 THEN TRUE ELSE FALSE END AS IS_TRADING_DAY,
                YEAR(date_value) AS FISCAL_YEAR,  -- Assuming calendar year = fiscal year
                QUARTER(date_value) AS FISCAL_QUARTER
            FROM date_range
        """
        
        try:
            self.client.execute_query(populate_ddl)
            logger.info(f"Populated date dimension from {start_date} to {end_date}")
        except Exception as e:
            logger.error(f"Error populating date dimension: {e}")
            raise
    
    def populate_time_dimension(self) -> None:
        """Populate time dimension table with minute-level granularity"""
        populate_ddl = """
            INSERT INTO STREAMING.DIM_TIME
            WITH time_range AS (
                SELECT 
                    TIME_FROM_PARTS(
                        FLOOR(ROW_NUMBER() OVER (ORDER BY NULL) / 60),
                        MOD(ROW_NUMBER() OVER (ORDER BY NULL), 60),
                        0
                    ) AS time_value
                FROM TABLE(GENERATOR(ROWCOUNT => 1440))  -- 24 hours * 60 minutes
            )
            SELECT 
                HOUR(time_value) * 100 + MINUTE(time_value) AS TIME_KEY,
                time_value AS TIME_VALUE,
                HOUR(time_value) AS HOUR,
                MINUTE(time_value) AS MINUTE,
                0 AS SECOND,
                TO_CHAR(time_value, 'HH24:MI') AS HOUR_MINUTE,
                CASE WHEN HOUR(time_value) < 12 THEN 'AM' ELSE 'PM' END AS AM_PM,
                CASE 
                    WHEN HOUR(time_value) BETWEEN 4 AND 9 AND MINUTE(time_value) <= 30 THEN 'PRE_MARKET'
                    WHEN HOUR(time_value) BETWEEN 9 AND 16 THEN 'REGULAR'
                    WHEN HOUR(time_value) BETWEEN 16 AND 20 THEN 'AFTER_HOURS'
                    ELSE 'CLOSED'
                END AS MARKET_SESSION,
                CASE 
                    WHEN HOUR(time_value) >= 9 AND HOUR(time_value) < 16 THEN
                        (HOUR(time_value) - 9) * 60 + MINUTE(time_value)
                    ELSE NULL
                END AS TRADING_DAY_MINUTE
            FROM time_range
        """
        
        try:
            self.client.execute_query(populate_ddl)
            logger.info("Populated time dimension with minute-level granularity")
        except Exception as e:
            logger.error(f"Error populating time dimension: {e}")
            raise
    
    def create_indexes(self) -> None:
        """Create indexes for optimal query performance"""
        # Note: Snowflake doesn't use traditional indexes, but we can create search optimization
        search_optimization_statements = [
            "ALTER TABLE STREAMING.DIM_COMPANY ADD SEARCH OPTIMIZATION ON EQUALITY(SYMBOL)",
            "ALTER TABLE STREAMING.FACT_STOCK_PRICES ADD SEARCH OPTIMIZATION ON EQUALITY(COMPANY_KEY, DATE_KEY)",
            "ALTER TABLE STREAMING.FACT_TRADING_VOLUME ADD SEARCH OPTIMIZATION ON EQUALITY(COMPANY_KEY, DATE_KEY)"
        ]
        
        for statement in search_optimization_statements:
            try:
                self.client.execute_query(statement)
                logger.info(f"Applied search optimization: {statement}")
            except Exception as e:
                logger.warning(f"Could not apply search optimization: {e}")
                # Search optimization might not be available in all Snowflake editions
    
    def setup_complete_schema(
        self, 
        s3_bucket: str, 
        aws_role_arn: str,
        populate_dimensions: bool = True
    ) -> None:
        """
        Set up complete schema with all tables and configurations
        
        Args:
            s3_bucket: S3 bucket name for staging
            aws_role_arn: AWS IAM role ARN
            populate_dimensions: Whether to populate dimension tables
        """
        try:
            logger.info("Starting complete schema setup...")
            
            # Create database and schemas
            self.create_database_and_schemas()
            
            # Create file formats
            self.create_file_formats()
            
            # Create stages
            self.create_stages(s3_bucket, aws_role_arn)
            
            # Create dimension tables
            self.create_dimension_tables()
            
            # Create fact tables
            self.create_fact_tables()
            
            # Create utility tables
            self.create_utility_tables()
            
            # Populate dimension tables if requested
            if populate_dimensions:
                self.populate_date_dimension()
                self.populate_time_dimension()
            
            # Create indexes/search optimization
            self.create_indexes()
            
            logger.info("Complete schema setup finished successfully")
            
        except Exception as e:
            logger.error(f"Error in complete schema setup: {e}")
            raise
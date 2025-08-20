"""
Snowflake Configuration for Stock Market Data Pipeline

This module contains configuration settings for connecting to Snowflake
and executing SQL commands for both batch and streaming pipelines.
"""
import os
from typing import Dict, Any

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "STOCK_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "STOCK_MARKET"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
}

# S3 Integration for Snowpipe
S3_INTEGRATION_CONFIG = {
    "storage_aws_role_arn": os.getenv("AWS_ROLE_ARN"),
    "storage_aws_external_id": os.getenv("AWS_EXTERNAL_ID"),
    "notification_aws_sns_topic": os.getenv("AWS_SNS_TOPIC_ARN"),
}

# File formats
FILE_FORMATS = {
    "parquet_format": """
    CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO';
    """,
    
    "csv_format": """
    CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '\\N')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"';
    """
}

# Snowpipe configurations
SNOWPIPE_CONFIGS = {
    "stock_prices_pipe": {
        "name": "STOCK_PRICES_PIPE",
        "auto_ingest": True,
        "copy_statement": """
        COPY INTO STOCK_MARKET.PUBLIC.FACT_STOCK_PRICES
        FROM @STOCK_MARKET.PUBLIC.STOCK_MARKET_STAGE/transformed/stream/
        FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
    },
    "anomalies_pipe": {
        "name": "ANOMALIES_PIPE",
        "auto_ingest": True,
        "copy_statement": """
        COPY INTO STOCK_MARKET.PUBLIC.ANOMALY_DETECTION_RESULTS
        FROM @STOCK_MARKET.PUBLIC.STOCK_MARKET_STAGE/anomalies/stream/
        FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
    }
}

def get_snowflake_jdbc_url() -> str:
    """Generate JDBC URL for Snowflake connection"""
    return (
        f"jdbc:snowflake://{SNOWFLAKE_CONFIG['account']}.snowflakecomputing.com/"
        f"?warehouse={SNOWFLAKE_CONFIG['warehouse']}"
        f"&db={SNOWFLAKE_CONFIG['database']}"
        f"&schema={SNOWFLAKE_CONFIG['schema']}"
        f"&role={SNOWFLAKE_CONFIG['role']}"
    )

def get_snowflake_spark_options() -> Dict[str, str]:
    """Get Spark options for Snowflake connection"""
    return {
        "sfURL": f"{SNOWFLAKE_CONFIG['account']}.snowflakecomputing.com",
        "sfUser": SNOWFLAKE_CONFIG["user"],
        "sfPassword": SNOWFLAKE_CONFIG["password"],
        "sfDatabase": SNOWFLAKE_CONFIG["database"],
        "sfSchema": SNOWFLAKE_CONFIG["schema"],
        "sfWarehouse": SNOWFLAKE_CONFIG["warehouse"],
        "sfRole": SNOWFLAKE_CONFIG["role"],
    }

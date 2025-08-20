"""
Example Usage of Snowflake Data Warehouse Integration

This module demonstrates how to use the Snowflake integration components
for the streaming pipeline.
"""

import logging
import pandas as pd
from datetime import datetime, timezone
import asyncio
from typing import Dict, Any

from .integration import SnowflakeIntegration
from .snowflake_client import SnowflakeClient
from .schema_manager import SchemaManager
from .s3_staging import S3StagingManager
from .snowpipe_manager import SnowpipeManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_sample_stock_data() -> pd.DataFrame:
    """Create sample stock price data for demonstration"""
    return pd.DataFrame({
        'company_key': [1, 2, 3, 1, 2, 3],
        'date_key': [20240118, 20240118, 20240118, 20240118, 20240118, 20240118],
        'time_key': [930, 931, 932, 933, 934, 935],
        'open_price': [150.25, 2500.50, 300.75, 150.30, 2501.00, 300.80],
        'high_price': [151.00, 2510.00, 301.50, 151.25, 2511.50, 301.75],
        'low_price': [149.50, 2495.00, 299.25, 149.75, 2496.50, 299.50],
        'close_price': [150.75, 2505.25, 301.00, 151.00, 2507.75, 301.25],
        'volume': [1000000, 500000, 750000, 1100000, 520000, 780000],
        'adjusted_close': [150.75, 2505.25, 301.00, 151.00, 2507.75, 301.25],
        'sma_20': [149.50, 2480.00, 298.50, 149.75, 2482.50, 298.75],
        'sma_50': [148.25, 2460.00, 296.25, 148.50, 2462.50, 296.50],
        'rsi_14': [65.5, 58.2, 72.1, 66.0, 58.8, 72.5],
        'data_source': ['ALPHA_VANTAGE'] * 6,
        'ingestion_timestamp': [datetime.now(timezone.utc)] * 6
    })


def create_sample_volume_data() -> pd.DataFrame:
    """Create sample trading volume data for demonstration"""
    return pd.DataFrame({
        'company_key': [1, 2, 3],
        'date_key': [20240118, 20240118, 20240118],
        'time_key': [930, 930, 930],
        'volume': [1000000, 500000, 750000],
        'volume_weighted_price': [150.50, 2502.75, 300.85],
        'trade_count': [5000, 2500, 3750],
        'buy_volume': [600000, 300000, 450000],
        'sell_volume': [400000, 200000, 300000],
        'volume_sma_20': [950000, 480000, 720000],
        'volume_ratio': [1.05, 1.04, 1.04],
        'data_source': ['ALPHA_VANTAGE'] * 3,
        'ingestion_timestamp': [datetime.now(timezone.utc)] * 3
    })


def create_sample_data_quality_results() -> pd.DataFrame:
    """Create sample data quality results for demonstration"""
    return pd.DataFrame({
        'table_name': ['FACT_STOCK_PRICES', 'FACT_TRADING_VOLUME', 'FACT_STOCK_PRICES'],
        'check_name': ['price_validation', 'volume_validation', 'completeness_check'],
        'check_type': ['RANGE_CHECK', 'POSITIVE_CHECK', 'NULL_CHECK'],
        'status': ['PASSED', 'PASSED', 'WARNING'],
        'record_count': [1000, 500, 1000],
        'error_count': [0, 0, 5],
        'error_percentage': [0.0, 0.0, 0.5],
        'check_timestamp': [datetime.now(timezone.utc)] * 3,
        'error_details': [None, None, '{"null_fields": ["dividend_amount"]}']
    })


async def example_basic_usage():
    """Example of basic Snowflake integration usage"""
    logger.info("=== Basic Snowflake Integration Usage ===")
    
    try:
        # Initialize the integration
        integration = SnowflakeIntegration()
        
        # Initialize the warehouse (creates all tables, stages, pipes)
        logger.info("Initializing data warehouse...")
        success = integration.initialize_warehouse()
        
        if not success:
            logger.error("Failed to initialize warehouse")
            return
        
        logger.info("Warehouse initialized successfully")
        
        # Create sample data
        stock_data = create_sample_stock_data()
        volume_data = create_sample_volume_data()
        
        # Load stock prices data
        logger.info("Loading stock prices data...")
        stock_result = integration.load_stock_prices_data(stock_data)
        logger.info(f"Stock prices load result: {stock_result}")
        
        # Load trading volume data
        logger.info("Loading trading volume data...")
        volume_result = integration.load_trading_volume_data(volume_data)
        logger.info(f"Trading volume load result: {volume_result}")
        
        # Wait a bit for Snowpipe to process
        logger.info("Waiting for Snowpipe processing...")
        await asyncio.sleep(30)
        
        # Check pipeline health
        logger.info("Checking pipeline health...")
        health = integration.get_pipeline_health()
        logger.info(f"Pipeline health: {health['overall_status']}")
        
        # Close the integration
        integration.close()
        
    except Exception as e:
        logger.error(f"Error in basic usage example: {e}")


def example_individual_components():
    """Example of using individual components"""
    logger.info("=== Individual Components Usage ===")
    
    try:
        # 1. Snowflake Client Usage
        logger.info("1. Using Snowflake Client...")
        client = SnowflakeClient()
        
        # Check if a table exists
        exists = client.check_table_exists("FACT_STOCK_PRICES", "STREAMING")
        logger.info(f"FACT_STOCK_PRICES exists: {exists}")
        
        # Get table info
        if exists:
            table_info = client.get_table_info("FACT_STOCK_PRICES", "STREAMING")
            logger.info(f"Table has {len(table_info)} columns")
        
        # 2. Schema Manager Usage
        logger.info("2. Using Schema Manager...")
        schema_manager = SchemaManager(client)
        
        # Create a single dimension table (if not exists)
        try:
            schema_manager.create_dimension_tables()
            logger.info("Dimension tables created/verified")
        except Exception as e:
            logger.warning(f"Dimension tables may already exist: {e}")
        
        # 3. S3 Staging Manager Usage
        logger.info("3. Using S3 Staging Manager...")
        s3_staging = S3StagingManager()
        
        # Upload sample data
        sample_df = create_sample_stock_data()
        s3_key = s3_staging.upload_dataframe_as_parquet(
            sample_df, 
            "fact_stock_prices"
        )
        logger.info(f"Uploaded to S3: {s3_key}")
        
        # List staged files
        staged_files = s3_staging.list_staged_files("fact_stock_prices")
        logger.info(f"Found {len(staged_files)} staged files")
        
        # 4. Snowpipe Manager Usage
        logger.info("4. Using Snowpipe Manager...")
        snowpipe_manager = SnowpipeManager(client)
        
        # Get pipe status
        pipe_status = snowpipe_manager.get_pipe_status("STOCK_PRICES_PIPE")
        if pipe_status:
            logger.info(f"Pipe status: {pipe_status.get('PIPE_EXECUTION_PAUSED', 'Unknown')}")
        
        # Monitor pipe health
        pipe_health = snowpipe_manager.monitor_pipe_health("STOCK_PRICES_PIPE", hours=1)
        logger.info(f"Pipe health: {pipe_health.get('health_status', 'Unknown')}")
        
        # Close client
        client.close()
        
    except Exception as e:
        logger.error(f"Error in individual components example: {e}")


def example_monitoring_and_maintenance():
    """Example of monitoring and maintenance operations"""
    logger.info("=== Monitoring and Maintenance ===")
    
    try:
        integration = SnowflakeIntegration()
        
        # Get comprehensive pipeline health
        health = integration.get_pipeline_health()
        logger.info(f"Overall pipeline status: {health.get('overall_status', 'Unknown')}")
        
        # Print pipe health details
        for pipe_name, pipe_health in health.get('pipes', {}).items():
            logger.info(f"{pipe_name}: {pipe_health.get('health_status', 'Unknown')} "
                       f"({pipe_health.get('total_files_processed', 0)} files, "
                       f"{pipe_health.get('error_rate', 0):.1f}% error rate)")
        
        # Get warehouse usage report
        usage_report = integration.get_warehouse_usage_report(days=1)
        logger.info(f"Warehouse usage: {usage_report.get('total_credits_used', 0)} credits, "
                   f"{usage_report.get('total_queries', 0)} queries")
        
        # Optimize tables
        logger.info("Optimizing tables...")
        optimization_results = integration.optimize_tables()
        successful_optimizations = sum(1 for success in optimization_results.values() if success)
        logger.info(f"Optimized {successful_optimizations}/{len(optimization_results)} tables")
        
        # Cleanup old data (simulate)
        logger.info("Cleaning up old data...")
        cleanup_results = integration.cleanup_old_data(days_to_keep=7)
        logger.info(f"Cleanup results: {cleanup_results}")
        
        integration.close()
        
    except Exception as e:
        logger.error(f"Error in monitoring example: {e}")


def example_data_quality_integration():
    """Example of integrating data quality results"""
    logger.info("=== Data Quality Integration ===")
    
    try:
        integration = SnowflakeIntegration()
        
        # Create sample data quality results
        dq_results = create_sample_data_quality_results()
        
        # Load data quality results
        logger.info("Loading data quality results...")
        dq_load_result = integration.load_data_quality_results(dq_results)
        logger.info(f"Data quality load result: {dq_load_result}")
        
        # Query recent data quality results (using direct client)
        client = integration.snowflake_client
        recent_dq_query = """
            SELECT 
                table_name,
                check_name,
                status,
                error_percentage,
                check_timestamp
            FROM STREAMING.DATA_QUALITY_RESULTS
            WHERE check_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
            ORDER BY check_timestamp DESC
        """
        
        try:
            recent_results = client.execute_query(recent_dq_query, fetch=True)
            logger.info(f"Found {len(recent_results or [])} recent data quality results")
            
            for result in (recent_results or [])[:3]:  # Show first 3
                logger.info(f"  {result.get('TABLE_NAME')}.{result.get('CHECK_NAME')}: "
                           f"{result.get('STATUS')} ({result.get('ERROR_PERCENTAGE', 0)}% errors)")
        except Exception as e:
            logger.warning(f"Could not query data quality results: {e}")
        
        integration.close()
        
    except Exception as e:
        logger.error(f"Error in data quality example: {e}")


def example_error_handling():
    """Example of error handling scenarios"""
    logger.info("=== Error Handling Examples ===")
    
    try:
        integration = SnowflakeIntegration()
        
        # Test with invalid data
        logger.info("Testing with invalid data...")
        invalid_df = pd.DataFrame({
            'invalid_column': ['test'],
            'another_invalid': [123]
        })
        
        result = integration.load_stock_prices_data(invalid_df)
        if not result['success']:
            logger.info(f"Expected failure handled: {result.get('error', 'Unknown error')}")
        
        # Test with empty data
        logger.info("Testing with empty data...")
        empty_df = pd.DataFrame()
        result = integration.load_stock_prices_data(empty_df)
        logger.info(f"Empty data result: {result}")
        
        # Test pipe operations with non-existent pipe
        snowpipe_manager = integration.snowpipe_manager
        status = snowpipe_manager.get_pipe_status("NON_EXISTENT_PIPE")
        logger.info(f"Non-existent pipe status: {status}")
        
        integration.close()
        
    except Exception as e:
        logger.error(f"Error in error handling example: {e}")


async def run_all_examples():
    """Run all examples"""
    logger.info("Starting Snowflake Integration Examples...")
    
    # Run examples in sequence
    await example_basic_usage()
    example_individual_components()
    example_monitoring_and_maintenance()
    example_data_quality_integration()
    example_error_handling()
    
    logger.info("All examples completed!")


if __name__ == "__main__":
    # Run the examples
    asyncio.run(run_all_examples())
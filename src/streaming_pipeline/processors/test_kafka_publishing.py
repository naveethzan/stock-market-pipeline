#!/usr/bin/env python3
"""
Test script for Kafka publishing functionality in StreamProcessor.
"""
import logging
import sys
import os
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from streaming_pipeline.config.settings import ConfigManager
from streaming_pipeline.processors.stream_processor import StreamProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_test_config():
    """Create a test configuration."""
    # Mock environment variables for testing
    test_env = {
        'ALPHA_VANTAGE_API_KEY': 'test_key',
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_PASSWORD': 'test_password',
        'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
        'SNOWFLAKE_DATABASE': 'test_database',
        'SNOWFLAKE_SCHEMA': 'test_schema'
    }
    
    with patch.dict(os.environ, test_env):
        return ConfigManager()


def create_test_spark_session():
    """Create a test Spark session."""
    return (SparkSession.builder
            .appName("test-kafka-publishing")
            .master("local[2]")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate())


def create_test_dataframe(spark):
    """Create a test DataFrame with stock data."""
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("current_price", DoubleType(), False),
        StructField("volume", LongType(), True),
        StructField("previous_close", DoubleType(), True),
        StructField("change", DoubleType(), True),
        StructField("change_percent", DoubleType(), True),
        StructField("producer_timestamp", TimestampType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("sma_5min", DoubleType(), True),
        StructField("sma_20min", DoubleType(), True),
        StructField("volume_sma_5min", DoubleType(), True),
        StructField("price_trend_5min", StringType(), True),
        StructField("price_volatility", DoubleType(), True),
        StructField("volume_ratio", DoubleType(), True),
        StructField("trading_session", StringType(), True),
        StructField("volume_weighted_price", DoubleType(), True)
    ])
    
    test_data = [
        ("AAPL", 150.0, 152.0, 149.0, 151.0, 1000000, 149.5, 1.5, 1.0, 
         "2024-01-01 10:00:00", "2024-01-01 10:01:00", 150.5, 149.8, 950000.0, 
         "up", 2.0, 1.05, "regular", 151.0),
        ("GOOGL", 2800.0, 2820.0, 2790.0, 2810.0, 500000, 2795.0, 15.0, 0.54,
         "2024-01-01 10:00:00", "2024-01-01 10:01:00", 2805.0, 2800.0, 480000.0,
         "up", 1.07, 1.04, "regular", 2810.0)
    ]
    
    return spark.createDataFrame(test_data, schema)


def test_prepare_processed_data():
    """Test the data preparation methods."""
    logger.info("Testing data preparation methods")
    
    config = create_test_config()
    spark = create_test_spark_session()
    
    try:
        processor = StreamProcessor(config, spark)
        test_df = create_test_dataframe(spark)
        
        # Test processed stock prices preparation
        logger.info("Testing processed stock prices preparation")
        stock_prices_df = processor.prepare_processed_stock_prices(test_df)
        stock_prices_count = stock_prices_df.count()
        logger.info(f"Processed stock prices count: {stock_prices_count}")
        
        # Show schema and sample data
        logger.info("Stock prices schema:")
        stock_prices_df.printSchema()
        logger.info("Stock prices sample data:")
        stock_prices_df.show(truncate=False)
        
        # Test processed trading volume preparation
        logger.info("Testing processed trading volume preparation")
        volume_df = processor.prepare_processed_trading_volume(test_df)
        volume_count = volume_df.count()
        logger.info(f"Processed trading volume count: {volume_count}")
        
        # Show schema and sample data
        logger.info("Trading volume schema:")
        volume_df.printSchema()
        logger.info("Trading volume sample data:")
        volume_df.show(truncate=False)
        
        # Test processed technical indicators preparation
        logger.info("Testing processed technical indicators preparation")
        indicators_df = processor.prepare_processed_technical_indicators(test_df)
        indicators_count = indicators_df.count()
        logger.info(f"Processed technical indicators count: {indicators_count}")
        
        # Show schema and sample data
        logger.info("Technical indicators schema:")
        indicators_df.printSchema()
        logger.info("Technical indicators sample data:")
        indicators_df.show(truncate=False)
        
        logger.info("All data preparation tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"Data preparation test failed: {str(e)}")
        return False
    finally:
        spark.stop()


def test_kafka_serialization():
    """Test Kafka serialization format."""
    logger.info("Testing Kafka serialization format")
    
    config = create_test_config()
    spark = create_test_spark_session()
    
    try:
        processor = StreamProcessor(config, spark)
        test_df = create_test_dataframe(spark)
        
        # Prepare data for Kafka
        stock_prices_df = processor.prepare_processed_stock_prices(test_df)
        
        # Test Kafka format conversion
        from pyspark.sql import functions as F
        kafka_df = (stock_prices_df
                   .select(
                       F.col("symbol").alias("key"),
                       F.to_json(F.struct(*stock_prices_df.columns)).alias("value")
                   ))
        
        logger.info("Kafka format schema:")
        kafka_df.printSchema()
        logger.info("Kafka format sample data:")
        kafka_df.show(truncate=False)
        
        # Verify JSON structure
        json_values = kafka_df.select("value").collect()
        for row in json_values:
            logger.info(f"JSON value: {row['value']}")
        
        logger.info("Kafka serialization test passed!")
        return True
        
    except Exception as e:
        logger.error(f"Kafka serialization test failed: {str(e)}")
        return False
    finally:
        spark.stop()


def main():
    """Run all tests."""
    logger.info("Starting Kafka publishing tests")
    
    tests = [
        test_prepare_processed_data,
        test_kafka_serialization
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
                logger.info(f"✓ {test.__name__} passed")
            else:
                failed += 1
                logger.error(f"✗ {test.__name__} failed")
        except Exception as e:
            failed += 1
            logger.error(f"✗ {test.__name__} failed with exception: {str(e)}")
    
    logger.info(f"Test results: {passed} passed, {failed} failed")
    
    if failed > 0:
        sys.exit(1)
    else:
        logger.info("All tests passed!")


if __name__ == "__main__":
    main()
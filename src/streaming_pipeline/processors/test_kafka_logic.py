#!/usr/bin/env python3
"""
Unit tests for Kafka publishing logic without requiring Spark.
"""
import logging
import sys
import os
from unittest.mock import Mock, patch

# Set up test environment variables before importing
os.environ.update({
    'ALPHA_VANTAGE_API_KEY': 'test_key',
    'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
    'SNOWFLAKE_ACCOUNT': 'test_account',
    'SNOWFLAKE_USER': 'test_user',
    'SNOWFLAKE_PASSWORD': 'test_password',
    'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
    'SNOWFLAKE_DATABASE': 'test_database',
    'SNOWFLAKE_SCHEMA': 'test_schema'
})

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from streaming_pipeline.config.settings import ConfigManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_config_kafka_topics():
    """Test that the configuration includes the new Kafka topics."""
    logger.info("Testing Kafka topic configuration")
    
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
    
    try:
        with patch.dict(os.environ, test_env):
            config = ConfigManager()
            
            # Test input topics
            assert hasattr(config.kafka, 'stock_quotes_topic')
            assert hasattr(config.kafka, 'stock_intraday_topic')
            
            # Test output topics
            assert hasattr(config.kafka, 'processed_stock_prices_topic')
            assert hasattr(config.kafka, 'processed_trading_volume_topic')
            assert hasattr(config.kafka, 'processed_technical_indicators_topic')
            assert hasattr(config.kafka, 'data_quality_alerts_topic')
            
            # Test default values
            assert config.kafka.processed_stock_prices_topic == "processed-stock-prices"
            assert config.kafka.processed_trading_volume_topic == "processed-trading-volume"
            assert config.kafka.processed_technical_indicators_topic == "processed-technical-indicators"
            assert config.kafka.data_quality_alerts_topic == "data-quality-alerts"
            
            logger.info("✓ All Kafka topics are properly configured")
            logger.info(f"  Input topics: {config.kafka.stock_quotes_topic}, {config.kafka.stock_intraday_topic}")
            logger.info(f"  Output topics: {config.kafka.processed_stock_prices_topic}, {config.kafka.processed_trading_volume_topic}, {config.kafka.processed_technical_indicators_topic}, {config.kafka.data_quality_alerts_topic}")
            
            return True
            
    except Exception as e:
        logger.error(f"Kafka topic configuration test failed: {str(e)}")
        return False


def test_config_custom_topics():
    """Test that custom topic names can be set via environment variables."""
    logger.info("Testing custom Kafka topic configuration")
    
    # Mock environment variables with custom topic names
    test_env = {
        'ALPHA_VANTAGE_API_KEY': 'test_key',
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
        'KAFKA_PROCESSED_STOCK_PRICES_TOPIC': 'custom-stock-prices',
        'KAFKA_PROCESSED_TRADING_VOLUME_TOPIC': 'custom-trading-volume',
        'KAFKA_PROCESSED_TECHNICAL_INDICATORS_TOPIC': 'custom-technical-indicators',
        'KAFKA_DATA_QUALITY_ALERTS_TOPIC': 'custom-data-quality-alerts',
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_PASSWORD': 'test_password',
        'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
        'SNOWFLAKE_DATABASE': 'test_database',
        'SNOWFLAKE_SCHEMA': 'test_schema'
    }
    
    try:
        with patch.dict(os.environ, test_env):
            config = ConfigManager()
            
            # Test custom values
            assert config.kafka.processed_stock_prices_topic == "custom-stock-prices"
            assert config.kafka.processed_trading_volume_topic == "custom-trading-volume"
            assert config.kafka.processed_technical_indicators_topic == "custom-technical-indicators"
            assert config.kafka.data_quality_alerts_topic == "custom-data-quality-alerts"
            
            logger.info("✓ Custom Kafka topics are properly configured")
            logger.info(f"  Custom topics: {config.kafka.processed_stock_prices_topic}, {config.kafka.processed_trading_volume_topic}, {config.kafka.processed_technical_indicators_topic}, {config.kafka.data_quality_alerts_topic}")
            
            return True
            
    except Exception as e:
        logger.error(f"Custom Kafka topic configuration test failed: {str(e)}")
        return False


def test_stream_processor_imports():
    """Test that StreamProcessor can be imported and has the new methods."""
    logger.info("Testing StreamProcessor imports and methods")
    
    try:
        from streaming_pipeline.processors.stream_processor import StreamProcessor, StreamProcessorError
        
        # Check that the class exists
        assert StreamProcessor is not None
        assert StreamProcessorError is not None
        
        # Check that new methods exist
        assert hasattr(StreamProcessor, 'write_to_kafka')
        assert hasattr(StreamProcessor, 'prepare_processed_stock_prices')
        assert hasattr(StreamProcessor, 'prepare_processed_trading_volume')
        assert hasattr(StreamProcessor, 'prepare_processed_technical_indicators')
        assert hasattr(StreamProcessor, 'publish_to_kafka_topics')
        assert hasattr(StreamProcessor, 'handle_kafka_publishing_errors')
        
        logger.info("✓ StreamProcessor imports and methods are available")
        logger.info("  New methods: write_to_kafka, prepare_processed_*, publish_to_kafka_topics, handle_kafka_publishing_errors")
        
        return True
        
    except Exception as e:
        logger.error(f"StreamProcessor import test failed: {str(e)}")
        return False


def test_error_handling_logic():
    """Test the error handling logic structure."""
    logger.info("Testing error handling logic")
    
    try:
        from streaming_pipeline.processors.stream_processor import StreamProcessor
        
        # Check that error handling method exists and has proper structure
        method = getattr(StreamProcessor, 'handle_kafka_publishing_errors')
        assert method is not None
        
        # Check that private restart method exists
        restart_method = getattr(StreamProcessor, '_restart_kafka_query')
        assert restart_method is not None
        
        logger.info("✓ Error handling methods are properly structured")
        
        return True
        
    except Exception as e:
        logger.error(f"Error handling logic test failed: {str(e)}")
        return False


def main():
    """Run all tests."""
    logger.info("Starting Kafka publishing logic tests")
    
    tests = [
        test_config_kafka_topics,
        test_config_custom_topics,
        test_stream_processor_imports,
        test_error_handling_logic
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
        logger.info("All logic tests passed!")


if __name__ == "__main__":
    main()
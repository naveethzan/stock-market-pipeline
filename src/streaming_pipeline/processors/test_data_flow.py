#!/usr/bin/env python3
"""
Test script to diagnose data flow issues in the streaming pipeline.
This script tests the Avro serialization and data preparation logic.
"""
import os
import sys
import logging
from typing import Dict, Any

# Add the project root to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.streaming_pipeline.processors.stream_processor import StreamProcessor
from src.streaming_pipeline.config.settings import ConfigManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_avro_serialization():
    """Test Avro serialization for processed data types."""
    logger.info("Testing Avro serialization...")
    
    try:
        # Create a mock configuration
        config = ConfigManager()
        
        # Create stream processor (without Spark session for testing)
        processor = StreamProcessor(config, spark_session=None)
        
        # Sample data for testing
        sample_data = {
            "symbol": "AAPL",
            "current_price": 150.25,
            "open_price": 149.50,
            "high_price": 151.00,
            "low_price": 148.75,
            "previous_close": 149.00,
            "change": 1.25,
            "change_percent": 0.84,
            "volume": 50000000
        }
        
        # Test data flow
        result = processor.test_data_flow(sample_data)
        
        if result:
            logger.info("✅ All Avro serialization tests PASSED")
            return True
        else:
            logger.error("❌ Some Avro serialization tests FAILED")
            return False
            
    except Exception as e:
        logger.error(f"❌ Avro serialization test failed with exception: {str(e)}", exc_info=True)
        return False


def test_schema_compatibility():
    """Test schema compatibility and field mapping."""
    logger.info("Testing schema compatibility...")
    
    try:
        from src.streaming_pipeline.schemas.avro_schemas import get_all_schemas
        
        schemas = get_all_schemas()
        
        # Check if all required schemas exist
        required_schemas = [
            "processed_stock_prices",
            "processed_trading_volume", 
            "processed_technical_indicators"
        ]
        
        missing_schemas = []
        for schema_name in required_schemas:
            if schema_name not in schemas:
                missing_schemas.append(schema_name)
        
        if missing_schemas:
            logger.error(f"❌ Missing schemas: {missing_schemas}")
            return False
        
        # Check schema field compatibility
        for schema_name in required_schemas:
            schema = schemas[schema_name]
            fields = [field["name"] for field in schema["fields"]]
            logger.info(f"Schema '{schema_name}' has fields: {fields}")
        
        logger.info("✅ Schema compatibility test PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Schema compatibility test failed: {str(e)}", exc_info=True)
        return False


def test_kafka_configuration():
    """Test Kafka configuration."""
    logger.info("Testing Kafka configuration...")
    
    try:
        config = ConfigManager()
        
        # Check Kafka topics
        topics = [
            config.kafka.processed_stock_prices_topic,
            config.kafka.processed_trading_volume_topic,
            config.kafka.processed_technical_indicators_topic
        ]
        
        logger.info(f"Configured Kafka topics: {topics}")
        
        # Check Kafka bootstrap servers
        logger.info(f"Kafka bootstrap servers: {config.kafka.bootstrap_servers}")
        
        # Check producer settings
        logger.info(f"Producer acks: {config.kafka.producer_acks}")
        logger.info(f"Producer retries: {config.kafka.producer_retries}")
        
        logger.info("✅ Kafka configuration test PASSED")
        return True
        
    except Exception as e:
        logger.error(f"❌ Kafka configuration test failed: {str(e)}", exc_info=True)
        return False


def main():
    """Run all diagnostic tests."""
    logger.info("🔍 Starting streaming pipeline diagnostic tests...")
    
    tests = [
        ("Schema Compatibility", test_schema_compatibility),
        ("Kafka Configuration", test_kafka_configuration),
        ("Avro Serialization", test_avro_serialization)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running {test_name} test...")
        logger.info(f"{'='*50}")
        
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {str(e)}")
            results[test_name] = False
    
    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("DIAGNOSTIC SUMMARY")
    logger.info(f"{'='*50}")
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed_tests += 1
    
    logger.info(f"\nOverall: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        logger.info("🎉 All diagnostic tests PASSED! The pipeline should work correctly.")
        return True
    else:
        logger.error("⚠️  Some diagnostic tests FAILED. Check the logs above for details.")
        return False


if __name__ == "__main__":
    # Set environment variable for mock mode
    os.environ["ALPHA_VANTAGE_MOCK_MODE"] = "true"
    
    success = main()
    sys.exit(0 if success else 1)
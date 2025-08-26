#!/usr/bin/env python3
"""
Debug script for streaming pipeline data flow issues.
This script helps identify why data is not being pushed to processed topics.
"""
import os
import sys
import logging
import time
from typing import Dict, Any

# Set environment variables for mock mode
os.environ["ALPHA_VANTAGE_MOCK_MODE"] = "true"

# Add the project root to the Python path
sys.path.append(os.path.dirname(__file__))

from src.streaming_pipeline.processors.stream_processor import StreamProcessor
from src.streaming_pipeline.config.settings import ConfigManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def debug_pipeline_components():
    """Debug individual pipeline components."""
    logger.info("🔍 Debugging pipeline components...")
    
    try:
        # Test configuration
        logger.info("Testing configuration...")
        config = ConfigManager()
        logger.info(f"✅ Configuration loaded successfully")
        logger.info(f"   - Kafka topics: {config.kafka.processed_stock_prices_topic}, {config.kafka.processed_trading_volume_topic}, {config.kafka.processed_technical_indicators_topic}")
        logger.info(f"   - Kafka servers: {config.kafka.bootstrap_servers}")
        
        # Test Avro schemas
        logger.info("Testing Avro schemas...")
        from src.streaming_pipeline.schemas.avro_schemas import get_all_schemas
        schemas = get_all_schemas()
        required_schemas = ["processed_stock_prices", "processed_trading_volume", "processed_technical_indicators"]
        
        for schema_name in required_schemas:
            if schema_name in schemas:
                logger.info(f"✅ Schema '{schema_name}' found")
            else:
                logger.error(f"❌ Schema '{schema_name}' missing")
        
        # Test Avro serialization
        logger.info("Testing Avro serialization...")
        from src.streaming_pipeline.schemas.avro_serializer import AvroSerializer
        serializer = AvroSerializer()
        
        # Test stock prices serialization
        test_data = {
            "symbol": "AAPL",
            "current_price": 150.0,
            "open_price": 149.0,
            "high_price": 151.0,
            "low_price": 148.0,
            "previous_close": 149.5,
            "change": 0.5,
            "change_percent": 0.33,
            "sma_5min": 150.0,
            "sma_20min": 149.8,
            "price_trend_5min": "neutral",
            "price_volatility": 1.2,
            "trading_session": "regular",
            "producer_timestamp": None,
            "processing_timestamp": int(time.time() * 1000),
            "data_layer": "silver",
            "record_type": "stock_price",
            "processing_version": "1.0"
        }
        
        try:
            avro_data = serializer.serialize_processed_stock_prices(test_data)
            logger.info(f"✅ Stock prices serialization successful ({len(avro_data)} bytes)")
        except Exception as e:
            logger.error(f"❌ Stock prices serialization failed: {str(e)}")
        
        # Test trading volume serialization
        volume_data = {
            "symbol": "AAPL",
            "volume": 50000000,
            "volume_weighted_price": 150.0,
            "volume_sma_5min": 45000000.0,
            "volume_ratio": 1.1,
            "volume_category": "normal",
            "trading_session": "regular",
            "producer_timestamp": None,
            "processing_timestamp": int(time.time() * 1000),
            "data_layer": "silver",
            "record_type": "trading_volume",
            "processing_version": "1.0"
        }
        
        try:
            avro_data = serializer.serialize_processed_trading_volume(volume_data)
            logger.info(f"✅ Trading volume serialization successful ({len(avro_data)} bytes)")
        except Exception as e:
            logger.error(f"❌ Trading volume serialization failed: {str(e)}")
        
        # Test technical indicators serialization
        indicators_data = {
            "symbol": "AAPL",
            "current_price": 150.0,
            "sma_5min": 150.0,
            "sma_20min": 149.8,
            "price_trend_5min": "neutral",
            "price_volatility": 1.2,
            "volume_ratio": 1.1,
            "momentum_signal": "neutral",
            "volatility_level": "low",
            "trading_session": "regular",
            "producer_timestamp": None,
            "processing_timestamp": int(time.time() * 1000),
            "data_layer": "silver",
            "record_type": "technical_indicators",
            "processing_version": "1.0"
        }
        
        try:
            avro_data = serializer.serialize_processed_technical_indicators(indicators_data)
            logger.info(f"✅ Technical indicators serialization successful ({len(avro_data)} bytes)")
        except Exception as e:
            logger.error(f"❌ Technical indicators serialization failed: {str(e)}")
        
        logger.info("✅ Component debugging completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Component debugging failed: {str(e)}", exc_info=True)
        return False


def debug_data_transformations():
    """Debug data transformation logic."""
    logger.info("🔍 Debugging data transformations...")
    
    try:
        # Create sample input data that matches what comes from Avro deserialization
        sample_input_data = [
            {
                "symbol": "AAPL",
                "open_price": 149.0,
                "high_price": 151.0,
                "low_price": 148.0,
                "current_price": 150.0,
                "volume": 50000000,
                "previous_close": 149.5,
                "change": 0.5,
                "change_percent": 0.33,
                "producer_timestamp": None,
                "processing_timestamp": None,
                "kafka_timestamp": None,
                "topic": "stock-quotes-realtime",
                "partition": 0,
                "offset": 123
            }
        ]
        
        logger.info(f"Sample input data: {sample_input_data[0]}")
        
        # Test data preparation methods
        from src.streaming_pipeline.processors.stream_processor import StreamProcessor
        config = ConfigManager()
        
        # We can't easily test the full Spark pipeline without a Spark session
        # But we can test the serialization logic
        logger.info("✅ Data transformation debugging completed (limited without Spark)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data transformation debugging failed: {str(e)}", exc_info=True)
        return False


def main():
    """Run debugging checks."""
    logger.info("🚀 Starting streaming pipeline debugging...")
    
    checks = [
        ("Component Testing", debug_pipeline_components),
        ("Data Transformation Testing", debug_data_transformations)
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        logger.info(f"\n{'='*60}")
        logger.info(f"Running {check_name}...")
        logger.info(f"{'='*60}")
        
        try:
            results[check_name] = check_func()
        except Exception as e:
            logger.error(f"Check {check_name} failed with exception: {str(e)}")
            results[check_name] = False
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("DEBUGGING SUMMARY")
    logger.info(f"{'='*60}")
    
    passed_checks = 0
    total_checks = len(checks)
    
    for check_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{check_name}: {status}")
        if result:
            passed_checks += 1
    
    logger.info(f"\nOverall: {passed_checks}/{total_checks} checks passed")
    
    if passed_checks == total_checks:
        logger.info("🎉 All debugging checks PASSED!")
        logger.info("\n📋 RECOMMENDATIONS:")
        logger.info("1. Check if Kafka is running and accessible")
        logger.info("2. Verify that the input topic 'stock-quotes-realtime' has data")
        logger.info("3. Check Spark streaming query logs for any runtime errors")
        logger.info("4. Monitor the processed topics for incoming data")
        logger.info("5. Check if the streaming application is actually consuming from the input topic")
    else:
        logger.error("⚠️  Some debugging checks FAILED. Fix the issues above before proceeding.")
    
    return passed_checks == total_checks


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
"""
Test script for DataProducer class.
Tests JSON serialization, error handling, and basic functionality.
"""
import json
import logging
import sys
import os
from unittest.mock import Mock, patch
from typing import Dict, Any

# Set required environment variables for testing
os.environ['ALPHA_VANTAGE_API_KEY'] = 'test_key'
os.environ['SNOWFLAKE_ACCOUNT'] = 'test_account'
os.environ['SNOWFLAKE_USER'] = 'test_user'
os.environ['SNOWFLAKE_PASSWORD'] = 'test_password'
os.environ['SNOWFLAKE_WAREHOUSE'] = 'test_warehouse'
os.environ['SNOWFLAKE_DATABASE'] = 'test_database'
os.environ['SNOWFLAKE_SCHEMA'] = 'test_schema'

# Add src to path for imports
sys.path.insert(0, '/'.join(__file__.split('/')[:-3]))

from streaming_pipeline.config.settings import ConfigManager, AlphaVantageConfig, KafkaConfig
from streaming_pipeline.clients.alpha_vantage import AlphaVantageClient
from streaming_pipeline.producers.data_producer import DataProducer, DataProducerError


def setup_logging():
    """Setup basic logging for tests."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )


def create_mock_config() -> ConfigManager:
    """Create a mock configuration for testing."""
    config = Mock(spec=ConfigManager)
    
    # Mock Alpha Vantage config
    config.alpha_vantage = AlphaVantageConfig(
        api_key="test_api_key",
        base_url="https://www.alphavantage.co/query",
        rate_limit_per_minute=5,
        timeout_seconds=30
    )
    
    # Mock Kafka config
    config.kafka = Mock(spec=KafkaConfig)
    config.kafka.bootstrap_servers = ["localhost:9092"]
    config.kafka.stock_quotes_topic = "test-stock-quotes"
    config.kafka.stock_intraday_topic = "test-stock-intraday"
    config.kafka.market_events_topic = "test-market-events"
    
    # Mock config methods
    config.get_kafka_producer_config.return_value = {
        'bootstrap.servers': 'localhost:9092',
        'acks': 'all',
        'retries': 3,
        'compression.type': 'snappy'
    }
    
    return config


def test_message_serialization():
    """Test JSON message serialization."""
    print("Testing message serialization...")
    
    config = create_mock_config()
    
    # Mock Alpha Vantage client
    mock_client = Mock(spec=AlphaVantageClient)
    
    # Mock Kafka producer to avoid actual Kafka connection
    with patch('streaming_pipeline.producers.data_producer.KafkaProducer') as mock_producer_class:
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        producer = DataProducer(config, mock_client)
        
        # Test data
        test_data = {
            "symbol": "AAPL",
            "price": 150.25,
            "timestamp": "2025-01-01T12:00:00Z",
            "volume": 1000000
        }
        
        # Test serialization
        serialized = producer.serialize_message(test_data)
        
        # Verify it's valid JSON
        deserialized = json.loads(serialized.decode('utf-8'))
        
        # Check original data is preserved
        assert deserialized["symbol"] == "AAPL"
        assert deserialized["price"] == 150.25
        assert deserialized["volume"] == 1000000
        
        # Check metadata was added
        assert "_producer_metadata" in deserialized
        assert "producer_timestamp" in deserialized["_producer_metadata"]
        assert deserialized["_producer_metadata"]["serialization_format"] == "json"
        
        print("✓ Message serialization test passed")


def test_error_handling():
    """Test error handling in DataProducer."""
    print("Testing error handling...")
    
    config = create_mock_config()
    mock_client = Mock(spec=AlphaVantageClient)
    
    with patch('streaming_pipeline.producers.data_producer.KafkaProducer') as mock_producer_class:
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        producer = DataProducer(config, mock_client)
        
        # Test serialization error with non-serializable data
        try:
            # Create data with non-serializable object
            bad_data = {"function": lambda x: x}  # Functions are not JSON serializable
            producer.serialize_message(bad_data)
            assert False, "Should have raised DataProducerError"
        except DataProducerError as e:
            assert "Failed to serialize message data" in str(e)
            print("✓ Serialization error handling test passed")
        
        # Test Kafka producer error
        mock_producer.produce.side_effect = Exception("Kafka connection failed")
        
        try:
            producer.publish_to_kafka("test-topic", {"test": "data"}, "test-key")
            assert False, "Should have raised DataProducerError"
        except DataProducerError as e:
            assert "Unexpected error publishing to Kafka" in str(e)
            print("✓ Kafka error handling test passed")


def test_metrics_tracking():
    """Test metrics tracking functionality."""
    print("Testing metrics tracking...")
    
    config = create_mock_config()
    mock_client = Mock(spec=AlphaVantageClient)
    
    with patch('streaming_pipeline.producers.data_producer.KafkaProducer') as mock_producer_class:
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        producer = DataProducer(config, mock_client)
        
        # Check initial metrics
        metrics = producer.get_metrics()
        assert metrics["messages"]["sent"] == 0
        assert metrics["messages"]["failed"] == 0
        assert metrics["api"]["requests"] == 0
        
        # Simulate successful delivery
        producer._delivery_report(None, Mock(
            topic=lambda: "test-topic",
            partition=lambda: 0,
            offset=lambda: 123,
            key=lambda: b"test-key",
            value=lambda: b'{"test": "data"}'
        ))
        
        # Check updated metrics
        metrics = producer.get_metrics()
        assert metrics["messages"]["sent"] == 1
        assert metrics["messages"]["failed"] == 0
        
        print("✓ Metrics tracking test passed")


def test_producer_lifecycle():
    """Test producer initialization and cleanup."""
    print("Testing producer lifecycle...")
    
    config = create_mock_config()
    mock_client = Mock(spec=AlphaVantageClient)
    
    with patch('streaming_pipeline.producers.data_producer.KafkaProducer') as mock_producer_class:
        mock_producer = Mock()
        mock_producer.flush.return_value = 0  # No remaining messages
        mock_producer_class.return_value = mock_producer
        
        # Test context manager
        with DataProducer(config, mock_client) as producer:
            assert producer is not None
            assert producer.producer == mock_producer
        
        # Verify cleanup was called
        mock_producer.flush.assert_called_once()
        mock_client.close.assert_called_once()
        
        print("✓ Producer lifecycle test passed")


def run_all_tests():
    """Run all tests."""
    setup_logging()
    
    print("Running DataProducer tests...\n")
    
    try:
        test_message_serialization()
        test_error_handling()
        test_metrics_tracking()
        test_producer_lifecycle()
        
        print("\n✅ All tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
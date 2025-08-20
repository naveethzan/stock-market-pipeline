"""
Integration test for DataProducer with Alpha Vantage client.
Tests the complete flow from API data fetching to Kafka message production.
"""
import json
import logging
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Set up test environment
os.environ.setdefault('ALPHA_VANTAGE_API_KEY', 'test_api_key')
os.environ.setdefault('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
os.environ.setdefault('SNOWFLAKE_ACCOUNT', 'test_account')
os.environ.setdefault('SNOWFLAKE_USER', 'test_user')
os.environ.setdefault('SNOWFLAKE_PASSWORD', 'test_password')
os.environ.setdefault('SNOWFLAKE_WAREHOUSE', 'test_warehouse')
os.environ.setdefault('SNOWFLAKE_DATABASE', 'test_database')
os.environ.setdefault('SNOWFLAKE_SCHEMA', 'test_schema')

# Add src to path
sys.path.insert(0, '/'.join(__file__.split('/')[:-3]))

from streaming_pipeline.config.settings import ConfigManager
from streaming_pipeline.clients.alpha_vantage import AlphaVantageClient
from streaming_pipeline.producers.data_producer import DataProducer


def setup_logging():
    """Setup logging for integration test."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )


def create_mock_alpha_vantage_response() -> Dict[str, Any]:
    """Create mock Alpha Vantage API response."""
    return {
        "Global Quote": {
            "01. symbol": "AAPL",
            "02. open": "150.0000",
            "03. high": "152.5000",
            "04. low": "149.0000",
            "05. price": "151.2500",
            "06. volume": "50000000",
            "07. latest trading day": "2025-01-01",
            "08. previous close": "150.5000",
            "09. change": "0.7500",
            "10. change percent": "0.4987%"
        }
    }


def create_mock_intraday_response() -> Dict[str, Any]:
    """Create mock Alpha Vantage intraday response."""
    return {
        "Meta Data": {
            "1. Information": "Intraday (1min) open, high, low, close prices and volume",
            "2. Symbol": "AAPL",
            "3. Last Refreshed": "2025-01-01 16:00:00",
            "4. Interval": "1min",
            "5. Output Size": "Compact",
            "6. Time Zone": "US/Eastern"
        },
        "Time Series (1min)": {
            "2025-01-01 16:00:00": {
                "1. open": "151.0000",
                "2. high": "151.5000",
                "3. low": "150.8000",
                "4. close": "151.2500",
                "5. volume": "1000000"
            },
            "2025-01-01 15:59:00": {
                "1. open": "150.8000",
                "2. high": "151.1000",
                "3. low": "150.7000",
                "4. close": "151.0000",
                "5. volume": "800000"
            }
        }
    }


def test_end_to_end_quote_production():
    """Test complete flow from Alpha Vantage API to Kafka message."""
    print("Testing end-to-end quote production...")
    
    # Mock Alpha Vantage API response
    mock_response = create_mock_alpha_vantage_response()
    
    # Track produced messages
    produced_messages = []
    
    def mock_produce(topic, value, key, callback):
        """Mock Kafka producer.produce method."""
        # Decode and parse the message
        message_data = json.loads(value.decode('utf-8'))
        produced_messages.append({
            'topic': topic,
            'key': key.decode('utf-8') if key else None,
            'data': message_data
        })
        
        # Simulate successful delivery
        mock_msg = Mock()
        mock_msg.topic.return_value = topic
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 123
        mock_msg.key.return_value = key
        mock_msg.value.return_value = value
        
        # Call the callback to simulate delivery
        callback(None, mock_msg)
    
    # Mock the HTTP request to Alpha Vantage
    with patch('requests.Session.get') as mock_get:
        mock_response_obj = Mock()
        mock_response_obj.status_code = 200
        mock_response_obj.json.return_value = mock_response
        mock_response_obj.content = b'{"test": "response"}'  # Add content for len() call
        mock_response_obj.text = '{"test": "response"}'
        mock_response_obj.reason = 'OK'
        mock_get.return_value = mock_response_obj
        
        # Mock Kafka producer
        with patch('streaming_pipeline.producers.data_producer.KafkaProducer') as mock_producer_class:
            mock_producer = Mock()
            mock_producer.produce.side_effect = mock_produce
            mock_producer.poll.return_value = None
            mock_producer.flush.return_value = 0
            mock_producer_class.return_value = mock_producer
            
            # Initialize configuration and producer
            config = ConfigManager()
            alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)
            
            with DataProducer(config, alpha_vantage_client) as producer:
                # Produce real-time quote
                results = producer.produce_real_time_quotes(['AAPL'])
                
                # Verify results
                assert results['AAPL'] == True, "Quote production should succeed"
                assert len(produced_messages) == 1, "Should produce exactly one message"
                
                # Verify message content
                message = produced_messages[0]
                assert message['topic'] == config.kafka.stock_quotes_topic
                assert message['key'] == 'AAPL'
                
                # Verify message data structure
                data = message['data']
                assert '01. symbol' in data, "Should contain Alpha Vantage quote data"
                assert data['01. symbol'] == 'AAPL'
                assert data['05. price'] == '151.2500'
                
                # Verify metadata was added
                assert '_metadata' in data, "Should contain metadata"
                assert data['_metadata']['symbol'] == 'AAPL'
                assert data['_metadata']['data_source'] == 'alpha_vantage'
                
                # Verify producer metadata was added
                assert '_producer_metadata' in data, "Should contain producer metadata"
                assert data['_producer_metadata']['serialization_format'] == 'json'
                
                # Verify metrics
                metrics = producer.get_metrics()
                assert metrics['messages']['sent'] == 1
                assert metrics['api']['requests'] == 1
                
    print("✓ End-to-end quote production test passed")


def test_end_to_end_intraday_production():
    """Test complete flow for intraday data production."""
    print("Testing end-to-end intraday data production...")
    
    # Mock Alpha Vantage API response
    mock_response = create_mock_intraday_response()
    
    # Track produced messages
    produced_messages = []
    
    def mock_produce(topic, value, key, callback):
        """Mock Kafka producer.produce method."""
        message_data = json.loads(value.decode('utf-8'))
        produced_messages.append({
            'topic': topic,
            'key': key.decode('utf-8') if key else None,
            'data': message_data
        })
        
        # Simulate successful delivery
        mock_msg = Mock()
        mock_msg.topic.return_value = topic
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 124
        mock_msg.key.return_value = key
        mock_msg.value.return_value = value
        
        callback(None, mock_msg)
    
    # Mock the HTTP request to Alpha Vantage
    with patch('requests.Session.get') as mock_get:
        mock_response_obj = Mock()
        mock_response_obj.status_code = 200
        mock_response_obj.json.return_value = mock_response
        mock_response_obj.content = b'{"test": "response"}'  # Add content for len() call
        mock_response_obj.text = '{"test": "response"}'
        mock_response_obj.reason = 'OK'
        mock_get.return_value = mock_response_obj
        
        # Mock Kafka producer
        with patch('streaming_pipeline.producers.data_producer.KafkaProducer') as mock_producer_class:
            mock_producer = Mock()
            mock_producer.produce.side_effect = mock_produce
            mock_producer.poll.return_value = None
            mock_producer.flush.return_value = 0
            mock_producer_class.return_value = mock_producer
            
            # Initialize configuration and producer
            config = ConfigManager()
            alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)
            
            with DataProducer(config, alpha_vantage_client) as producer:
                # Produce intraday data
                results = producer.produce_intraday_data(['AAPL'], '1min')
                
                # Verify results
                assert results['AAPL'] == True, "Intraday production should succeed"
                assert len(produced_messages) == 1, "Should produce exactly one message"
                
                # Verify message content
                message = produced_messages[0]
                assert message['topic'] == config.kafka.stock_intraday_topic
                assert message['key'] == 'AAPL_1min'
                
                # Verify message data structure
                data = message['data']
                assert 'Meta Data' in data, "Should contain metadata"
                assert 'Time Series' in data, "Should contain time series data"
                assert data['Meta Data']['2. Symbol'] == 'AAPL'
                
                # Verify time series data
                time_series = data['Time Series']
                assert len(time_series) == 2, "Should contain 2 data points"
                assert '2025-01-01 16:00:00' in time_series
                
                # Verify producer metadata
                assert '_metadata' in data, "Should contain metadata"
                assert data['_metadata']['symbol'] == 'AAPL'
                assert data['_metadata']['interval'] == '1min'
                assert data['_metadata']['data_points'] == 2
                
    print("✓ End-to-end intraday data production test passed")


def test_error_handling_integration():
    """Test error handling in the complete integration."""
    print("Testing error handling integration...")
    
    # Mock Alpha Vantage API error response
    mock_error_response = {
        "Error Message": "Invalid API call. Please retry or visit the documentation"
    }
    
    # Mock the HTTP request to return error
    with patch('requests.Session.get') as mock_get:
        mock_response_obj = Mock()
        mock_response_obj.status_code = 200
        mock_response_obj.json.return_value = mock_error_response
        mock_response_obj.content = b'{"Error Message": "Invalid API call"}'
        mock_response_obj.text = '{"Error Message": "Invalid API call"}'
        mock_response_obj.reason = 'OK'
        mock_get.return_value = mock_response_obj
        
        # Mock Kafka producer
        with patch('streaming_pipeline.producers.data_producer.KafkaProducer') as mock_producer_class:
            mock_producer = Mock()
            mock_producer.flush.return_value = 0
            mock_producer_class.return_value = mock_producer
            
            # Initialize configuration and producer
            config = ConfigManager()
            alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)
            
            with DataProducer(config, alpha_vantage_client) as producer:
                # Attempt to produce quote (should fail)
                results = producer.produce_real_time_quotes(['INVALID_SYMBOL'])
                
                # Verify error handling
                assert results['INVALID_SYMBOL'] == False, "Should fail for invalid symbol"
                
                # Verify metrics show the error
                metrics = producer.get_metrics()
                assert metrics['api']['requests'] == 1
                assert metrics['api']['errors'] == 1
                assert metrics['messages']['sent'] == 0
    
    print("✓ Error handling integration test passed")


def run_integration_tests():
    """Run all integration tests."""
    setup_logging()
    
    print("Running DataProducer Integration Tests")
    print("=" * 50)
    
    try:
        test_end_to_end_quote_production()
        test_end_to_end_intraday_production()
        test_error_handling_integration()
        
        print("\n✅ All integration tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Integration test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)
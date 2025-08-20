#!/usr/bin/env python3
"""
Test script for Alpha Vantage API client.
Tests authentication, rate limiting, and error handling.
"""
import os
import sys
import logging
import time
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from src.streaming_pipeline.config.settings import AlphaVantageConfig
from src.streaming_pipeline.clients.alpha_vantage import AlphaVantageClient, AlphaVantageAPIError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_alpha_vantage_client():
    """Test Alpha Vantage client functionality."""
    
    # Check for API key
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        logger.error("ALPHA_VANTAGE_API_KEY environment variable not set")
        logger.info("You can get a free API key from: https://www.alphavantage.co/support/#api-key")
        return False
    
    # Create configuration
    config = AlphaVantageConfig(
        api_key=api_key,
        rate_limit_per_minute=5,  # Conservative for testing
        timeout_seconds=30,
        retry_attempts=2,
        retry_backoff_factor=2.0
    )
    
    # Test client initialization
    logger.info("Testing Alpha Vantage client initialization...")
    try:
        with AlphaVantageClient(config) as client:
            logger.info("✓ Client initialized successfully")
            
            # Test client status
            logger.info("Testing client status...")
            status = client.get_client_status()
            logger.info(f"✓ Client status retrieved: {status['config']['base_url']}")
            
            # Test rate limiting status
            logger.info("Testing rate limiting status...")
            status = client.get_client_status()
            logger.info(f"✓ Rate limiting status: {status['rate_limiting']['requests_remaining']} requests remaining")
            
            # Test real-time quote
            logger.info("Testing real-time quote retrieval...")
            try:
                quote_data = client.get_real_time_quote("AAPL")
                logger.info(f"✓ Real-time quote retrieved for AAPL")
                logger.info(f"  Price: {quote_data.get('05. price', 'N/A')}")
                logger.info(f"  Change: {quote_data.get('09. change', 'N/A')}")
                logger.info(f"  Volume: {quote_data.get('06. volume', 'N/A')}")
            except AlphaVantageAPIError as e:
                logger.error(f"✗ Failed to get real-time quote: {e}")
                return False
            
            # Test intraday data
            logger.info("Testing intraday data retrieval...")
            try:
                intraday_data = client.get_intraday_data("AAPL", "5min", "compact")
                logger.info(f"✓ Intraday data retrieved for AAPL")
                logger.info(f"  Data points: {intraday_data['_metadata']['data_points']}")
                
                # Show latest data point
                time_series = intraday_data.get('Time Series', {})
                if time_series:
                    latest_time = max(time_series.keys())
                    latest_data = time_series[latest_time]
                    logger.info(f"  Latest ({latest_time}): {latest_data.get('4. close', 'N/A')}")
                
            except AlphaVantageAPIError as e:
                logger.error(f"✗ Failed to get intraday data: {e}")
                return False
            
            # Test rate limiting (make multiple requests)
            logger.info("Testing rate limiting with multiple requests...")
            test_symbols = ["MSFT", "GOOGL", "AMZN"]
            
            for i, symbol in enumerate(test_symbols):
                try:
                    logger.info(f"Request {i+1}: Getting quote for {symbol}")
                    start_time = time.time()
                    quote_data = client.get_real_time_quote(symbol)
                    end_time = time.time()
                    
                    logger.info(f"✓ {symbol} quote retrieved in {end_time - start_time:.2f}s")
                    logger.info(f"  Price: {quote_data.get('05. price', 'N/A')}")
                    
                    # Show rate limiting status after each request
                    status = client.get_client_status()
                    rate_limit = status['rate_limiting']
                    logger.info(f"  Rate limit: {rate_limit['current_request_count']}/{config.rate_limit_per_minute} used")
                    
                except AlphaVantageAPIError as e:
                    logger.warning(f"⚠ Request for {symbol} failed: {e}")
                    # Continue with other symbols
            
            # Final status check
            logger.info("Final client status check...")
            final_status = client.get_client_status()
            logger.info(f"✓ Final rate limit status: {final_status['rate_limiting']['current_request_count']} requests used")
            
            logger.info("✓ All tests completed successfully!")
            return True
            
    except Exception as e:
        logger.error(f"✗ Test failed with exception: {e}")
        return False


def test_error_handling():
    """Test error handling with invalid configuration."""
    logger.info("Testing error handling...")
    
    # Test with invalid API key
    config = AlphaVantageConfig(
        api_key="invalid_key",
        rate_limit_per_minute=5,
        timeout_seconds=10
    )
    
    try:
        with AlphaVantageClient(config) as client:
            quote_data = client.get_real_time_quote("AAPL")
            logger.error("✗ Expected error with invalid API key, but request succeeded")
            return False
    except AlphaVantageAPIError as e:
        logger.info(f"✓ Correctly handled invalid API key: {e}")
        return True
    except Exception as e:
        logger.error(f"✗ Unexpected error type: {e}")
        return False


if __name__ == "__main__":
    logger.info("Starting Alpha Vantage client tests...")
    
    # Test main functionality
    success = test_alpha_vantage_client()
    
    if success:
        # Test error handling
        error_handling_success = test_error_handling()
        
        if error_handling_success:
            logger.info("🎉 All tests passed!")
            sys.exit(0)
        else:
            logger.error("❌ Error handling tests failed")
            sys.exit(1)
    else:
        logger.error("❌ Main functionality tests failed")
        sys.exit(1)
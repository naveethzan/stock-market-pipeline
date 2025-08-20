"""
Example usage of DataProducer for streaming financial data.
Demonstrates real-time quote production and intraday data streaming.
"""
import logging
import time
import os
from typing import List

# Set up environment variables (in production, these would be set externally)
os.environ.setdefault('ALPHA_VANTAGE_API_KEY', 'your_alpha_vantage_api_key_here')
os.environ.setdefault('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
os.environ.setdefault('SNOWFLAKE_ACCOUNT', 'your_account')
os.environ.setdefault('SNOWFLAKE_USER', 'your_user')
os.environ.setdefault('SNOWFLAKE_PASSWORD', 'your_password')
os.environ.setdefault('SNOWFLAKE_WAREHOUSE', 'your_warehouse')
os.environ.setdefault('SNOWFLAKE_DATABASE', 'your_database')
os.environ.setdefault('SNOWFLAKE_SCHEMA', 'your_schema')

from streaming_pipeline.config.settings import ConfigManager
from streaming_pipeline.clients.alpha_vantage import AlphaVantageClient
from streaming_pipeline.producers.data_producer import DataProducer, DataProducerError


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def example_real_time_quotes():
    """Example: Produce real-time quotes for multiple symbols."""
    print("Example: Real-time quote production")
    print("-" * 40)
    
    # Stock symbols to track
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    
    try:
        # Initialize configuration
        config = ConfigManager()
        
        # Create Alpha Vantage client
        alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)
        
        # Create data producer
        with DataProducer(config, alpha_vantage_client) as producer:
            print(f"Producing real-time quotes for symbols: {symbols}")
            
            # Produce quotes
            results = producer.produce_real_time_quotes(symbols)
            
            # Display results
            for symbol, success in results.items():
                status = "✓" if success else "✗"
                print(f"  {status} {symbol}: {'Success' if success else 'Failed'}")
            
            # Show metrics
            metrics = producer.get_metrics()
            print(f"\nMetrics:")
            print(f"  Messages sent: {metrics['messages']['sent']}")
            print(f"  Messages failed: {metrics['messages']['failed']}")
            print(f"  API requests: {metrics['api']['requests']}")
            print(f"  Success rate: {metrics['messages']['success_rate']:.2%}")
            
    except Exception as e:
        print(f"Error: {str(e)}")


def example_intraday_data():
    """Example: Produce intraday data for symbols."""
    print("\nExample: Intraday data production")
    print("-" * 40)
    
    # Stock symbols and interval
    symbols = ['AAPL', 'GOOGL']
    interval = '5min'
    
    try:
        # Initialize configuration
        config = ConfigManager()
        
        # Create Alpha Vantage client
        alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)
        
        # Create data producer
        with DataProducer(config, alpha_vantage_client) as producer:
            print(f"Producing {interval} intraday data for symbols: {symbols}")
            
            # Produce intraday data
            results = producer.produce_intraday_data(symbols, interval)
            
            # Display results
            for symbol, success in results.items():
                status = "✓" if success else "✗"
                print(f"  {status} {symbol}: {'Success' if success else 'Failed'}")
            
            # Show metrics
            metrics = producer.get_metrics()
            print(f"\nMetrics:")
            print(f"  Messages sent: {metrics['messages']['sent']}")
            print(f"  API requests: {metrics['api']['requests']}")
            print(f"  Bytes sent: {metrics['throughput']['bytes_sent']}")
            
    except Exception as e:
        print(f"Error: {str(e)}")


def example_market_events():
    """Example: Produce market event messages."""
    print("\nExample: Market event production")
    print("-" * 40)
    
    try:
        # Initialize configuration
        config = ConfigManager()
        
        # Create data producer (no Alpha Vantage client needed for events)
        with DataProducer(config) as producer:
            # Produce market open event
            producer.produce_market_event(
                event_type="market_open",
                event_data={
                    "market": "NYSE",
                    "timezone": "America/New_York",
                    "trading_session": "regular"
                }
            )
            print("✓ Market open event produced")
            
            # Produce market close event
            producer.produce_market_event(
                event_type="market_close",
                event_data={
                    "market": "NYSE",
                    "timezone": "America/New_York",
                    "trading_session": "regular",
                    "volume_summary": {
                        "total_volume": 1500000000,
                        "avg_volume": 75000000
                    }
                }
            )
            print("✓ Market close event produced")
            
    except Exception as e:
        print(f"Error: {str(e)}")


def example_continuous_streaming():
    """Example: Continuous streaming with intervals."""
    print("\nExample: Continuous streaming (demo)")
    print("-" * 40)
    
    symbols = ['AAPL', 'MSFT']
    interval_seconds = 10  # Short interval for demo
    max_iterations = 3     # Limit for demo
    
    try:
        # Initialize configuration
        config = ConfigManager()
        
        # Create Alpha Vantage client
        alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)
        
        # Create data producer
        with DataProducer(config, alpha_vantage_client) as producer:
            print(f"Starting continuous streaming for {symbols} every {interval_seconds}s")
            print(f"Will run for {max_iterations} iterations...")
            
            for iteration in range(max_iterations):
                print(f"\nIteration {iteration + 1}/{max_iterations}")
                
                # Produce real-time quotes
                results = producer.produce_real_time_quotes(symbols)
                
                successful = sum(1 for success in results.values() if success)
                print(f"  Produced quotes for {successful}/{len(symbols)} symbols")
                
                # Wait before next iteration (except for last iteration)
                if iteration < max_iterations - 1:
                    print(f"  Waiting {interval_seconds} seconds...")
                    time.sleep(interval_seconds)
            
            # Final metrics
            metrics = producer.get_metrics()
            print(f"\nFinal metrics:")
            print(f"  Total messages: {metrics['messages']['sent']}")
            print(f"  Total API requests: {metrics['api']['requests']}")
            print(f"  Average throughput: {metrics['throughput']['messages_per_second']:.2f} msg/sec")
            
    except KeyboardInterrupt:
        print("\nStreaming stopped by user")
    except Exception as e:
        print(f"Error: {str(e)}")


def main():
    """Run all examples."""
    setup_logging()
    
    print("DataProducer Usage Examples")
    print("=" * 50)
    
    # Check if API key is configured
    if os.getenv('ALPHA_VANTAGE_API_KEY') == 'your_alpha_vantage_api_key_here':
        print("⚠️  Warning: Using placeholder API key. Set ALPHA_VANTAGE_API_KEY for real data.")
        print("   Examples will demonstrate functionality with mock responses.\n")
    
    try:
        # Run examples
        example_real_time_quotes()
        example_intraday_data()
        example_market_events()
        example_continuous_streaming()
        
        print("\n" + "=" * 50)
        print("✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Example failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
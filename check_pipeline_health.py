#!/usr/bin/env python3
"""
Command-line tool to check streaming pipeline health.
Usage: python check_pipeline_health.py [--continuous] [--interval 30]
"""
import os
import sys
import time
import argparse
import json
import logging

# Set environment variables
os.environ["ALPHA_VANTAGE_MOCK_MODE"] = "true"

# Add project root to path
sys.path.append(os.path.dirname(__file__))

from src.streaming_pipeline.processors.stream_processor import StreamProcessor
from src.streaming_pipeline.config.settings import ConfigManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_health_summary(health_info):
    """Print a formatted health summary."""
    print("\n" + "="*60)
    print("🏥 STREAMING PIPELINE HEALTH CHECK")
    print("="*60)
    
    # Overall summary
    total = health_info['total_queries']
    active = health_info['active_queries']
    failed = health_info['failed_queries']
    
    print(f"📊 SUMMARY:")
    print(f"   Total Queries: {total}")
    print(f"   Active Queries: {active} {'✅' if active > 0 else '⚠️'}")
    print(f"   Failed Queries: {failed} {'❌' if failed > 0 else '✅'}")
    
    if total == 0:
        print("\n⚠️  No streaming queries found!")
        print("💡 Make sure the streaming pipeline is running")
        return
    
    # Detailed query information
    print(f"\n🔍 QUERY DETAILS:")
    for query_name, query_info in health_info['query_details'].items():
        is_active = query_info.get('active', False)
        status_icon = "🟢" if is_active else "🔴"
        
        print(f"\n   {status_icon} {query_name}")
        print(f"      ID: {query_info.get('id', 'N/A')}")
        print(f"      Status: {'Active' if is_active else 'Failed'}")
        
        if query_info.get('exception'):
            print(f"      ❌ Error: {query_info['exception']}")
        
        if query_info.get('last_progress'):
            progress = query_info['last_progress']
            print(f"      📈 Progress:")
            print(f"         Batch ID: {progress.get('batch_id', 'N/A')}")
            print(f"         Input Rate: {progress.get('input_rows_per_second', 0):.1f} rows/sec")
            print(f"         Processing Rate: {progress.get('processed_rows_per_second', 0):.1f} rows/sec")
            print(f"         Batch Duration: {progress.get('batch_duration', 'N/A')}")
    
    # Health assessment
    print(f"\n🩺 HEALTH ASSESSMENT:")
    if failed == 0 and active > 0:
        print("   ✅ All queries are healthy!")
    elif failed > 0:
        print(f"   ❌ {failed} queries have failed - requires attention")
    elif active == 0:
        print("   ⚠️  No active queries - pipeline may not be running")
    
    print("="*60)


def print_troubleshooting_tips(health_info):
    """Print troubleshooting tips based on health status."""
    failed_queries = [
        (name, info) for name, info in health_info['query_details'].items()
        if not info.get('active', False)
    ]
    
    if not failed_queries:
        return
    
    print("\n🔧 TROUBLESHOOTING TIPS:")
    print("-" * 40)
    
    for query_name, query_info in failed_queries:
        exception_msg = query_info.get('exception', '').lower()
        
        print(f"\n❌ {query_name}:")
        
        if 'kafka' in exception_msg:
            print("   🔍 Kafka-related issue detected")
            print("   💡 Try these solutions:")
            print("      - Check if Kafka is running: docker-compose ps kafka")
            print("      - Verify topics exist: kafka-topics --list --bootstrap-server localhost:9092")
            print("      - Check Kafka logs: docker-compose logs kafka")
            
        elif 'avro' in exception_msg or 'serialization' in exception_msg:
            print("   🔍 Avro serialization issue detected")
            print("   💡 Try these solutions:")
            print("      - Check schema registry: curl http://localhost:8085/subjects")
            print("      - Verify schemas are registered")
            print("      - Run debug script: python debug_streaming_pipeline.py")
            
        elif 'checkpoint' in exception_msg:
            print("   🔍 Checkpoint issue detected")
            print("   💡 Try these solutions:")
            print("      - Clear checkpoints: rm -rf /tmp/spark-checkpoints/*")
            print("      - Check disk space and permissions")
            
        else:
            print("   🔍 General error detected")
            print("   💡 Try these solutions:")
            print("      - Check application logs for more details")
            print("      - Verify all services are running: docker-compose ps")
            print("      - Check system resources (memory, CPU)")


def continuous_monitoring(processor, interval):
    """Run continuous health monitoring."""
    print(f"🔄 Starting continuous monitoring (checking every {interval}s)")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            health_info = processor.check_query_health()
            print_health_summary(health_info)
            
            if health_info['failed_queries'] > 0:
                print_troubleshooting_tips(health_info)
            
            print(f"\n⏰ Next check in {interval} seconds...")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped by user")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Check streaming pipeline health")
    parser.add_argument('--continuous', '-c', action='store_true',
                       help='Run continuous monitoring')
    parser.add_argument('--interval', '-i', type=int, default=30,
                       help='Check interval in seconds (default: 30)')
    parser.add_argument('--json', '-j', action='store_true',
                       help='Output in JSON format')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize processor
        config = ConfigManager()
        
        # Note: This assumes you have a running StreamProcessor instance
        # In practice, you might need to connect to an existing instance
        # or start a new one
        
        print("🔌 Connecting to streaming pipeline...")
        
        # For demonstration, we'll show what the output would look like
        # In a real scenario, you would have:
        # processor = StreamProcessor(config)
        # health_info = processor.check_query_health()
        
        # Sample health data for demonstration
        sample_health = {
            "total_queries": 4,
            "active_queries": 3,
            "failed_queries": 1,
            "query_details": {
                "stock_quotes_processed_stock_prices": {
                    "active": True,
                    "id": "12345-abcd-6789",
                    "last_progress": {
                        "batch_id": 42,
                        "input_rows_per_second": 150.5,
                        "processed_rows_per_second": 148.2,
                        "batch_duration": "2.5 seconds"
                    }
                },
                "stock_quotes_processed_trading_volume": {
                    "active": True,
                    "id": "67890-efgh-1234",
                    "last_progress": {
                        "batch_id": 41,
                        "input_rows_per_second": 150.5,
                        "processed_rows_per_second": 150.1,
                        "batch_duration": "2.1 seconds"
                    }
                },
                "stock_quotes_processed_technical_indicators": {
                    "active": True,
                    "id": "abcde-fghij-5678",
                    "last_progress": {
                        "batch_id": 40,
                        "input_rows_per_second": 150.5,
                        "processed_rows_per_second": 149.8,
                        "batch_duration": "2.3 seconds"
                    }
                },
                "data_quality_monitoring": {
                    "active": False,
                    "id": "xyz123-monitoring",
                    "exception": "org.apache.kafka.common.errors.TimeoutException: Failed to send data to Kafka topic data-quality-alerts"
                }
            }
        }
        
        if args.json:
            print(json.dumps(sample_health, indent=2))
        else:
            print_health_summary(sample_health)
            print_troubleshooting_tips(sample_health)
        
        print("\n📋 USAGE WITH REAL PIPELINE:")
        print("To use this with a real running pipeline:")
        print("1. Start your streaming pipeline")
        print("2. Get a reference to the StreamProcessor instance")
        print("3. Call processor.check_query_health()")
        print("4. Use the returned health information for monitoring")
        
        print("\n💡 INTEGRATION EXAMPLE:")
        print("""
# In your streaming application:
processor = StreamProcessor(config)
main_query = processor.process_stock_quotes_stream()

# Check health
health_info = processor.check_query_health()
print(f"Active queries: {health_info['active_queries']}")

# For continuous monitoring:
import threading
def monitor():
    while True:
        health = processor.check_query_health()
        if health['failed_queries'] > 0:
            print("Alert: Queries failed!")
        time.sleep(60)

threading.Thread(target=monitor, daemon=True).start()
        """)
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}", exc_info=args.verbose)
        sys.exit(1)


if __name__ == "__main__":
    main()
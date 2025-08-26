#!/usr/bin/env python3
"""
Script to monitor streaming pipeline query health and diagnose issues.
This script shows how to use the check_query_health method to monitor your pipeline.
"""
import os
import sys
import time
import json
import logging
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


def monitor_pipeline_health(processor: StreamProcessor, duration_minutes: int = 5):
    """
    Monitor pipeline health for a specified duration.
    
    Args:
        processor: StreamProcessor instance with active queries
        duration_minutes: How long to monitor (in minutes)
    """
    logger.info(f"🔍 Starting pipeline health monitoring for {duration_minutes} minutes...")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    check_interval = 30  # Check every 30 seconds
    
    while time.time() < end_time:
        try:
            # Get health information
            health_info = processor.check_query_health()
            
            # Display summary
            logger.info(f"📊 HEALTH CHECK SUMMARY:")
            logger.info(f"   Total Queries: {health_info['total_queries']}")
            logger.info(f"   Active Queries: {health_info['active_queries']}")
            logger.info(f"   Failed Queries: {health_info['failed_queries']}")
            
            # Display detailed information for each query
            for query_name, query_info in health_info['query_details'].items():
                status = "🟢 ACTIVE" if query_info.get('active', False) else "🔴 FAILED"
                logger.info(f"   {query_name}: {status}")
                
                if query_info.get('exception'):
                    logger.error(f"      ❌ Exception: {query_info['exception']}")
                
                if query_info.get('last_progress'):
                    progress = query_info['last_progress']
                    logger.info(f"      📈 Batch ID: {progress.get('batch_id', 'N/A')}")
                    logger.info(f"      📊 Input Rate: {progress.get('input_rows_per_second', 0):.2f} rows/sec")
                    logger.info(f"      ⚡ Processing Rate: {progress.get('processed_rows_per_second', 0):.2f} rows/sec")
                    logger.info(f"      ⏱️  Batch Duration: {progress.get('batch_duration', 'N/A')}")
            
            # Check for issues
            if health_info['failed_queries'] > 0:
                logger.warning(f"⚠️  {health_info['failed_queries']} queries have failed!")
                
                # Show recommendations for failed queries
                for query_name, query_info in health_info['query_details'].items():
                    if not query_info.get('active', False) and query_info.get('exception'):
                        logger.error(f"🔧 TROUBLESHOOTING for {query_name}:")
                        exception_msg = query_info['exception'].lower()
                        
                        if 'kafka' in exception_msg:
                            logger.error("   - Check if Kafka is running and accessible")
                            logger.error("   - Verify Kafka topic exists and has correct permissions")
                            logger.error("   - Check network connectivity to Kafka brokers")
                        
                        if 'avro' in exception_msg or 'serialization' in exception_msg:
                            logger.error("   - Check Avro schema compatibility")
                            logger.error("   - Verify schema registry is accessible")
                            logger.error("   - Check data format matches expected schema")
                        
                        if 'checkpoint' in exception_msg:
                            logger.error("   - Check checkpoint directory permissions")
                            logger.error("   - Verify checkpoint location is accessible")
                            logger.error("   - Consider clearing checkpoint if corrupted")
            
            elif health_info['active_queries'] == 0:
                logger.warning("⚠️  No active queries found! Pipeline may not be running.")
            else:
                logger.info("✅ All queries are healthy!")
            
            logger.info("-" * 60)
            
            # Wait before next check
            time.sleep(check_interval)
            
        except KeyboardInterrupt:
            logger.info("🛑 Monitoring stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Error during health check: {str(e)}")
            time.sleep(check_interval)
    
    logger.info("🏁 Health monitoring completed")


def check_query_progress(processor: StreamProcessor):
    """
    Check detailed progress information for all queries.
    
    Args:
        processor: StreamProcessor instance with active queries
    """
    logger.info("📊 Checking detailed query progress...")
    
    health_info = processor.check_query_health()
    
    if health_info['total_queries'] == 0:
        logger.warning("⚠️  No queries found. Make sure the pipeline is running.")
        return
    
    for query_name, query_info in health_info['query_details'].items():
        logger.info(f"\n🔍 DETAILED INFO for {query_name}:")
        logger.info(f"   Query ID: {query_info.get('id', 'N/A')}")
        logger.info(f"   Status: {'Active' if query_info.get('active', False) else 'Inactive'}")
        
        if query_info.get('exception'):
            logger.error(f"   Exception: {query_info['exception']}")
        
        if query_info.get('last_progress'):
            progress = query_info['last_progress']
            logger.info(f"   Last Progress:")
            logger.info(f"     - Batch ID: {progress.get('batch_id', 'N/A')}")
            logger.info(f"     - Input Rows/Sec: {progress.get('input_rows_per_second', 0)}")
            logger.info(f"     - Processed Rows/Sec: {progress.get('processed_rows_per_second', 0)}")
            logger.info(f"     - Batch Duration: {progress.get('batch_duration', 'N/A')}")
        else:
            logger.info("   No progress information available")


def diagnose_pipeline_issues(processor: StreamProcessor):
    """
    Diagnose common pipeline issues based on query health.
    
    Args:
        processor: StreamProcessor instance with active queries
    """
    logger.info("🔧 Diagnosing pipeline issues...")
    
    health_info = processor.check_query_health()
    
    # Check overall health
    if health_info['total_queries'] == 0:
        logger.error("❌ ISSUE: No streaming queries found")
        logger.info("💡 SOLUTION: Start the streaming pipeline first")
        return
    
    if health_info['failed_queries'] == 0 and health_info['active_queries'] > 0:
        logger.info("✅ No issues detected - all queries are healthy!")
        return
    
    # Analyze specific issues
    issues_found = []
    
    for query_name, query_info in health_info['query_details'].items():
        if not query_info.get('active', False):
            exception_msg = query_info.get('exception', '').lower()
            
            if 'kafka' in exception_msg:
                issues_found.append({
                    'query': query_name,
                    'issue': 'Kafka connectivity',
                    'solutions': [
                        'Check if Kafka is running: docker-compose ps kafka',
                        'Verify Kafka topics exist: kafka-topics --list --bootstrap-server localhost:9092',
                        'Check network connectivity to Kafka brokers',
                        'Verify Kafka configuration in settings'
                    ]
                })
            
            elif 'avro' in exception_msg or 'serialization' in exception_msg:
                issues_found.append({
                    'query': query_name,
                    'issue': 'Avro serialization',
                    'solutions': [
                        'Check schema registry is running: curl http://localhost:8085/subjects',
                        'Verify Avro schemas are registered',
                        'Check data format matches expected schema',
                        'Run the debug script: python debug_streaming_pipeline.py'
                    ]
                })
            
            elif 'checkpoint' in exception_msg:
                issues_found.append({
                    'query': query_name,
                    'issue': 'Checkpoint issues',
                    'solutions': [
                        'Check checkpoint directory permissions',
                        'Clear corrupted checkpoints: rm -rf /tmp/spark-checkpoints/*',
                        'Verify checkpoint location is accessible',
                        'Check disk space availability'
                    ]
                })
            
            else:
                issues_found.append({
                    'query': query_name,
                    'issue': 'Unknown error',
                    'solutions': [
                        'Check Spark logs for detailed error messages',
                        'Verify all dependencies are running',
                        'Check system resources (memory, CPU)',
                        'Review application logs for more details'
                    ]
                })
    
    # Display issues and solutions
    if issues_found:
        logger.error(f"❌ Found {len(issues_found)} issues:")
        
        for i, issue in enumerate(issues_found, 1):
            logger.error(f"\n{i}. ISSUE in {issue['query']}: {issue['issue']}")
            logger.info("   💡 SOLUTIONS:")
            for solution in issue['solutions']:
                logger.info(f"      - {solution}")
    
    # General recommendations
    logger.info("\n🔧 GENERAL TROUBLESHOOTING STEPS:")
    logger.info("1. Check if all services are running: docker-compose ps")
    logger.info("2. Verify input topic has data: kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-quotes-realtime --from-beginning")
    logger.info("3. Check processed topics: kafka-console-consumer --bootstrap-server localhost:9092 --topic processed-stock-prices --from-beginning")
    logger.info("4. Review application logs for detailed error messages")
    logger.info("5. Monitor system resources (memory, CPU, disk space)")


def main():
    """
    Main function demonstrating how to use query health monitoring.
    """
    logger.info("🚀 Starting streaming pipeline query health monitoring...")
    
    try:
        # Initialize configuration and processor
        config = ConfigManager()
        
        # Note: In a real scenario, you would have an active StreamProcessor
        # with running queries. For demonstration, we'll show how to use it.
        
        logger.info("📋 USAGE EXAMPLES:")
        logger.info("\n1. To monitor an active pipeline:")
        logger.info("   processor = StreamProcessor(config)")
        logger.info("   main_query = processor.process_stock_quotes_stream()")
        logger.info("   health_info = processor.check_query_health()")
        
        logger.info("\n2. To continuously monitor:")
        logger.info("   monitor_pipeline_health(processor, duration_minutes=10)")
        
        logger.info("\n3. To check detailed progress:")
        logger.info("   check_query_progress(processor)")
        
        logger.info("\n4. To diagnose issues:")
        logger.info("   diagnose_pipeline_issues(processor)")
        
        logger.info("\n📊 SAMPLE HEALTH CHECK OUTPUT:")
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
                    "active": False,
                    "id": "67890-efgh-1234",
                    "exception": "org.apache.kafka.common.errors.TimeoutException: Failed to send data to Kafka"
                }
            }
        }
        
        logger.info(json.dumps(sample_health, indent=2))
        
        logger.info("\n🔧 INTEGRATION EXAMPLE:")
        logger.info("""
# In your main streaming application:
from src.streaming_pipeline.processors.stream_processor import StreamProcessor
from src.streaming_pipeline.config.settings import ConfigManager

def run_streaming_pipeline():
    config = ConfigManager()
    processor = StreamProcessor(config)
    
    try:
        # Start the pipeline
        main_query = processor.process_stock_quotes_stream()
        
        # Monitor health periodically
        import threading
        import time
        
        def health_monitor():
            while True:
                health_info = processor.check_query_health()
                if health_info['failed_queries'] > 0:
                    print(f"⚠️ {health_info['failed_queries']} queries failed!")
                    # Take corrective action or alert
                time.sleep(60)  # Check every minute
        
        # Start health monitoring in background
        monitor_thread = threading.Thread(target=health_monitor, daemon=True)
        monitor_thread.start()
        
        # Wait for queries to run
        main_query.awaitTermination()
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        # Check health for diagnosis
        diagnose_pipeline_issues(processor)
    finally:
        processor.close()
        """)
        
    except Exception as e:
        logger.error(f"❌ Error in monitoring setup: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
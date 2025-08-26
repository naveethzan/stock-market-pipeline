#!/usr/bin/env python3
"""
Example streaming application with integrated query health monitoring.
This shows how to use the check_query_health method in a real streaming pipeline.
"""
import os
import sys
import time
import threading
import logging
from typing import Optional

# Set environment variables
os.environ["ALPHA_VANTAGE_MOCK_MODE"] = "true"

from ..config.settings import ConfigManager
from .stream_processor import StreamProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MonitoredStreamingPipeline:
    """
    Streaming pipeline with integrated health monitoring.
    """
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.processor = StreamProcessor(config)
        self.monitoring_active = False
        self.monitor_thread: Optional[threading.Thread] = None
    
    def start_health_monitoring(self, check_interval: int = 60):
        """
        Start background health monitoring.
        
        Args:
            check_interval: How often to check health (in seconds)
        """
        def monitor_loop():
            logger.info("🔍 Starting background health monitoring...")
            
            while self.monitoring_active:
                try:
                    health_info = self.processor.check_query_health()
                    
                    # Log health summary
                    logger.info(f"Health Check: {health_info['active_queries']}/{health_info['total_queries']} queries active")
                    
                    # Check for issues
                    if health_info['failed_queries'] > 0:
                        logger.warning(f"⚠️ {health_info['failed_queries']} queries have failed!")
                        
                        # Log details about failed queries
                        for query_name, query_info in health_info['query_details'].items():
                            if not query_info.get('active', False):
                                exception = query_info.get('exception', 'Unknown error')
                                logger.error(f"Failed query {query_name}: {exception}")
                                
                                # Auto-restart logic for retryable errors
                                if self._is_retryable_error(exception):
                                    logger.info(f"Attempting to restart query: {query_name}")
                                    self._attempt_query_restart(query_name)
                    
                    # Log progress for active queries
                    for query_name, query_info in health_info['query_details'].items():
                        if query_info.get('active', False) and query_info.get('last_progress'):
                            progress = query_info['last_progress']
                            input_rate = progress.get('input_rows_per_second', 0)
                            processed_rate = progress.get('processed_rows_per_second', 0)
                            
                            if input_rate > 0:
                                logger.info(f"{query_name}: Processing {processed_rate:.1f}/{input_rate:.1f} rows/sec")
                            
                            # Alert if processing is lagging
                            if input_rate > 0 and processed_rate < input_rate * 0.8:
                                logger.warning(f"⚠️ {query_name} is lagging: {processed_rate:.1f} < {input_rate:.1f} rows/sec")
                    
                except Exception as e:
                    logger.error(f"Error in health monitoring: {str(e)}")
                
                time.sleep(check_interval)
            
            logger.info("🛑 Health monitoring stopped")
        
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info(f"✅ Health monitoring started (checking every {check_interval}s)")
    
    def stop_health_monitoring(self):
        """Stop background health monitoring."""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("🛑 Health monitoring stopped")
    
    def _is_retryable_error(self, exception_msg: str) -> bool:
        """Check if an error is retryable."""
        retryable_patterns = [
            "TimeoutException",
            "Connection refused",
            "Broker may not be available",
            "NetworkException"
        ]
        return any(pattern in exception_msg for pattern in retryable_patterns)
    
    def _attempt_query_restart(self, query_name: str):
        """Attempt to restart a failed query."""
        try:
            # This is a simplified restart logic
            # In practice, you might need more sophisticated restart mechanisms
            logger.info(f"🔄 Attempting to restart {query_name}...")
            
            # For now, just log that a restart would be attempted
            # In a real implementation, you might:
            # 1. Stop the failed query
            # 2. Clear checkpoints if needed
            # 3. Restart the specific query
            
            logger.warning(f"Query restart for {query_name} requires manual intervention")
            
        except Exception as e:
            logger.error(f"Failed to restart query {query_name}: {str(e)}")
    
    def run_pipeline(self):
        """Run the streaming pipeline with monitoring."""
        try:
            logger.info("🚀 Starting streaming pipeline...")
            
            # Start the main pipeline
            main_query = self.processor.process_stock_quotes_stream()
            
            # Start health monitoring
            self.start_health_monitoring(check_interval=30)  # Check every 30 seconds
            
            # Initial health check
            time.sleep(5)  # Wait for queries to initialize
            initial_health = self.processor.check_query_health()
            logger.info(f"Initial health: {initial_health['active_queries']}/{initial_health['total_queries']} queries active")
            
            # Display query details
            for query_name, query_info in initial_health['query_details'].items():
                status = "🟢 ACTIVE" if query_info.get('active', False) else "🔴 FAILED"
                logger.info(f"  {query_name}: {status} (ID: {query_info.get('id', 'N/A')})")
            
            logger.info("✅ Pipeline started successfully. Monitoring in background...")
            logger.info("Press Ctrl+C to stop the pipeline")
            
            # Wait for termination
            main_query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("🛑 Pipeline stopped by user")
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}", exc_info=True)
            
            # Diagnose issues
            logger.info("🔧 Diagnosing issues...")
            health_info = self.processor.check_query_health()
            
            for query_name, query_info in health_info['query_details'].items():
                if query_info.get('exception'):
                    logger.error(f"Query {query_name} failed: {query_info['exception']}")
        
        finally:
            # Cleanup
            self.stop_health_monitoring()
            self.processor.close()
            logger.info("🏁 Pipeline shutdown complete")
    
    def get_current_health(self):
        """Get current health status (for external monitoring)."""
        return self.processor.check_query_health()


def main():
    """Main function to run the monitored pipeline."""
    try:
        config = ConfigManager()
        pipeline = MonitoredStreamingPipeline(config)
        pipeline.run_pipeline()
    except Exception as e:
        logger.error(f"Failed to start pipeline: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
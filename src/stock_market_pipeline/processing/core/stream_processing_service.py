"""
Stream Processing Service

Simple service to orchestrate stream processing components.
Provides basic service management, health monitoring, and error handling.
"""

import signal
import sys
import time
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from stock_market_pipeline.core.exceptions import ProcessingError
from stock_market_pipeline.utils import PipelineLogger
from stock_market_pipeline.config import config

# Local imports
from .stream_consumer import StreamConsumer


class StreamProcessingService:
    """
    Service orchestrator for stream processing pipeline.
    
    Manages the complete stream processing lifecycle including Spark session
    creation, StreamConsumer initialization, and service lifecycle management
    with proper signal handling and graceful shutdown capabilities.
    """
    
    def __init__(self, config: Any = None, spark_session: SparkSession = None):
        """
        Initialize the stream processing service.
        
        Args:
            config: Configuration object (uses global config if None)
            spark_session: Spark session (creates new if None)
        """
        self.config = config or config.get_config()
        self.logger = PipelineLogger(__name__)
        
        # Initialize Spark session if not provided
        if spark_session is None:
            self.spark = self._create_spark_session()
        else:
            self.spark = spark_session
        
        # Initialize stream consumer
        self.consumer = StreamConsumer(self.spark, self.config)
        
        # Service state
        self.is_running = False
        self.start_time = None
        
        self.logger.info("StreamProcessingService initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create Spark session for stream processing.
        
        Returns:
            SparkSession object
        """
        try:
            spark = SparkSession.builder \
                .appName("StockMarketStreamProcessing") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            self.logger.info("Spark session created successfully")
            return spark
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise ProcessingError(f"Failed to create Spark session: {str(e)}") from e
    
    def start(self, topics: list = None, checkpoint_location: str = None) -> None:
        """
        Start the stream processing service.
        
        Args:
            topics: List of Kafka topics to consume from
            checkpoint_location: Checkpoint location for fault tolerance
        """
        self.logger.info("Starting stream processing service")
        
        try:
            # Set default topics if not provided
            if topics is None:
                topics = [
                    self.config.kafka.stock_quotes_topic,
                    self.config.kafka.stock_intraday_topic
                ]
            
            # Start stream consumer
            self.consumer.start_streaming(topics, checkpoint_location)
            
            # Update service state
            self.is_running = True
            self.start_time = time.time()
            
            self.logger.info(
                "Stream processing service started successfully",
                topics=topics,
                checkpoint_location=checkpoint_location
            )
            
        except Exception as e:
            self.logger.error(f"Failed to start stream processing service: {str(e)}")
            raise ProcessingError(f"Failed to start stream processing service: {str(e)}") from e
    
    def stop(self) -> None:
        """
        Stop the stream processing service.
        """
        self.logger.info("Stopping stream processing service")
        
        try:
            # Stop stream consumer
            self.consumer.stop_streaming()
            
            # Update service state
            self.is_running = False
            
            self.logger.info("Stream processing service stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to stop stream processing service: {str(e)}")
            raise ProcessingError(f"Failed to stop stream processing service: {str(e)}") from e
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the service.
        
        Returns:
            Health status dictionary
        """
        try:
            # Get consumer health status
            consumer_health = self.consumer.get_health_status()
            
            # Determine overall service health
            service_healthy = (
                self.is_running and
                consumer_health.get("status") == "healthy"
            )
            
            return {
                "status": "healthy" if service_healthy else "unhealthy",
                "service_running": self.is_running,
                "start_time": self.start_time,
                "uptime_seconds": time.time() - self.start_time if self.start_time else 0,
                "consumer": consumer_health
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get health status: {str(e)}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get service metrics.
        
        Returns:
            Metrics dictionary
        """
        try:
            # Get consumer metrics
            consumer_metrics = self.consumer.get_metrics()
            
            return {
                "service": {
                    "running": self.is_running,
                    "start_time": self.start_time,
                    "uptime_seconds": time.time() - self.start_time if self.start_time else 0
                },
                "consumer": consumer_metrics
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get metrics: {str(e)}")
            return {
                "error": str(e)
            }
    
    def is_healthy(self) -> bool:
        """
        Check if service is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            health_status = self.get_health_status()
            return health_status.get("status") == "healthy"
        except Exception:
            return False


# Global service instance for signal handling
service: Optional[StreamProcessingService] = None


def signal_handler(signum, frame):
    """
    Handle shutdown signals.
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global service
    
    if service:
        service.logger.info(f"Received signal {signum}, initiating graceful shutdown")
        try:
            service.stop()
        except Exception as e:
            service.logger.error(f"Error during shutdown: {str(e)}")
    
    sys.exit(0)


def main():
    """
    Main entry point for the Stream Processing Service.
    """
    global service
    
    try:
        # Load configuration
        config_instance = config.get_config()
        
        # Create service
        service = StreamProcessingService(config_instance)
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start service
        service.start()
        
        # Wait for shutdown signal
        service.logger.info("Stream processing service running. Press Ctrl+C to stop.")
        while service.is_running:
            time.sleep(1)
        
    except KeyboardInterrupt:
        service.logger.info("Keyboard interrupt received")
    except Exception as e:
        service.logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup
        if service:
            service.logger.info("Cleaning up resources")
            try:
                service.stop()
            except Exception as e:
                service.logger.error(f"Error during cleanup: {str(e)}")
        
        service.logger.info("Stream processing service shutdown complete")


if __name__ == "__main__":
    main()

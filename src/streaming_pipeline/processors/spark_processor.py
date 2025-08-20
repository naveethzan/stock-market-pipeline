#!/usr/bin/env python3
"""
Spark Structured Streaming Processor Entry Point

This module serves as the main entry point for the Spark Structured Streaming
processor Docker container. It initializes Spark, sets up streaming queries,
and provides health monitoring.
"""
import logging
import signal
import sys
import time
import threading
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

from ..config.settings import ConfigManager
from ..config.loader import load_config
from .stream_processor import StreamProcessor, StreamProcessorError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/processor.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)

# Global variables for graceful shutdown
processor: Optional[StreamProcessor] = None
processor_thread: Optional[threading.Thread] = None
shutdown_event = threading.Event()
app = FastAPI(title="Spark Streaming Processor", version="1.0.0")


class ProcessorService:
    """Service class to manage the Spark streaming processor lifecycle."""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.processor: Optional[StreamProcessor] = None
        self.is_running = False
        self.start_time = datetime.now(timezone.utc)
        self.last_health_check = datetime.now(timezone.utc)
        self.error_count = 0
        self.restart_count = 0
        
    def initialize_processor(self) -> None:
        """Initialize the Spark streaming processor with error handling."""
        try:
            logger.info("Initializing Spark streaming processor")
            
            # Create stream processor
            self.processor = StreamProcessor(self.config)
            
            logger.info("Spark streaming processor initialized successfully")
            
        except Exception as e:
            error_msg = f"Failed to initialize processor: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def start_streaming_queries(self) -> None:
        """Start all streaming queries."""
        if not self.processor:
            raise StreamProcessorError("Processor not initialized")
        
        try:
            logger.info("Starting streaming queries")
            
            # Configure output paths
            output_base_path = self.config.get_output_base_path()
            
            # Start stock quotes processing
            quotes_query = self.processor.process_stock_quotes_stream(output_base_path)
            
            logger.info(
                f"Stock quotes streaming query started: {quotes_query.id}"
            )
            
            # TODO: Add more streaming queries as needed
            # - Intraday data processing
            # - Market events processing
            # - Aggregation queries
            
        except Exception as e:
            error_msg = f"Failed to start streaming queries: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def monitor_streaming_queries(self) -> None:
        """Monitor streaming queries and handle failures."""
        logger.info("Starting streaming query monitoring")
        self.is_running = True
        
        # Monitoring interval (default: 30 seconds)
        monitoring_interval = 30
        
        while self.is_running and not shutdown_event.is_set():
            try:
                self.last_health_check = datetime.now(timezone.utc)
                
                if not self.processor:
                    logger.error("Processor is None during monitoring")
                    break
                
                # Check status of all active queries
                all_healthy = True
                for query_name in list(self.processor.active_queries.keys()):
                    status = self.processor.get_query_status(query_name)
                    
                    if "error" in status:
                        logger.error(f"Query {query_name} error: {status['error']}")
                        all_healthy = False
                        self.error_count += 1
                    elif not status.get("is_active", False):
                        logger.warning(f"Query {query_name} is not active")
                        all_healthy = False
                        
                        # Check for exceptions
                        if "exception" in status:
                            logger.error(f"Query {query_name} exception: {status['exception']}")
                            self.error_count += 1
                    else:
                        # Log query progress
                        logger.info(
                            f"Query {query_name} status: "
                            f"batch_id={status.get('batch_id', 'N/A')}, "
                            f"input_rate={status.get('input_rows_per_second', 0):.2f} rows/sec, "
                            f"processing_rate={status.get('processed_rows_per_second', 0):.2f} rows/sec"
                        )
                
                # Reset error count on successful monitoring cycle
                if all_healthy:
                    self.error_count = max(0, self.error_count - 1)
                
                # Handle high error rates
                if self.error_count > 10:
                    logger.error("High error rate detected, attempting to restart queries")
                    self._restart_queries()
                
                # Wait for next monitoring cycle
                if shutdown_event.wait(monitoring_interval):
                    break  # Shutdown requested
                    
            except Exception as e:
                self.error_count += 1
                logger.error(f"Error in monitoring loop: {str(e)}", exc_info=True)
                
                # Wait before retrying
                if shutdown_event.wait(min(60, monitoring_interval)):
                    break
        
        self.is_running = False
        logger.info("Streaming query monitoring stopped")
    
    def _restart_queries(self) -> None:
        """Restart all streaming queries."""
        try:
            logger.info("Restarting streaming queries")
            
            if self.processor:
                # Stop all queries
                self.processor.stop_all_queries()
                
                # Wait a bit before restarting
                time.sleep(5)
                
                # Restart queries
                self.start_streaming_queries()
                
                self.restart_count += 1
                self.error_count = 0
                
                logger.info("Streaming queries restarted successfully")
            
        except Exception as e:
            logger.error(f"Failed to restart queries: {str(e)}", exc_info=True)
    
    def stop(self) -> None:
        """Stop the processor service."""
        logger.info("Stopping processor service")
        self.is_running = False
        
        if self.processor:
            try:
                self.processor.close()
            except Exception as e:
                logger.error(f"Error closing processor: {str(e)}")
        
        logger.info("Processor service stopped")
    
    def get_health_status(self) -> dict:
        """Get health status for health checks."""
        now = datetime.now(timezone.utc)
        uptime_seconds = (now - self.start_time).total_seconds()
        time_since_last_check = (now - self.last_health_check).total_seconds()
        
        # Get query statuses
        query_statuses = {}
        active_queries = 0
        healthy_queries = 0
        
        if self.processor:
            for query_name in self.processor.active_queries.keys():
                status = self.processor.get_query_status(query_name)
                query_statuses[query_name] = status
                active_queries += 1
                
                if status.get("is_active", False) and "error" not in status:
                    healthy_queries += 1
        
        # Consider healthy if:
        # - Service is running
        # - Recent health check (< 2 minutes)
        # - Low error rate
        # - At least one active query
        # - Most queries are healthy
        is_healthy = (
            self.is_running and
            time_since_last_check < 120 and  # 2 minutes
            self.error_count < 20 and
            active_queries > 0 and
            (healthy_queries / active_queries >= 0.5 if active_queries > 0 else False)
        )
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "is_running": self.is_running,
            "uptime_seconds": uptime_seconds,
            "last_health_check": self.last_health_check.isoformat(),
            "time_since_last_check_seconds": time_since_last_check,
            "error_count": self.error_count,
            "restart_count": self.restart_count,
            "active_queries": active_queries,
            "healthy_queries": healthy_queries,
            "query_statuses": query_statuses
        }


# Global processor service instance
processor_service: Optional[ProcessorService] = None


@app.get("/health")
async def health_check():
    """Health check endpoint for Docker health checks."""
    global processor_service
    
    if not processor_service:
        raise HTTPException(status_code=503, detail="Processor service not initialized")
    
    health_status = processor_service.get_health_status()
    
    if health_status["status"] == "healthy":
        return JSONResponse(content=health_status, status_code=200)
    else:
        return JSONResponse(content=health_status, status_code=503)


@app.get("/metrics")
async def get_metrics():
    """Get detailed processor metrics."""
    global processor_service
    
    if not processor_service:
        raise HTTPException(status_code=503, detail="Processor service not initialized")
    
    return processor_service.get_health_status()


@app.get("/queries")
async def get_query_status():
    """Get status of all streaming queries."""
    global processor_service
    
    if not processor_service or not processor_service.processor:
        raise HTTPException(status_code=503, detail="Processor not available")
    
    query_statuses = {}
    for query_name in processor_service.processor.active_queries.keys():
        query_statuses[query_name] = processor_service.processor.get_query_status(query_name)
    
    return query_statuses


@app.post("/queries/{query_name}/restart")
async def restart_query(query_name: str):
    """Restart a specific streaming query."""
    global processor_service
    
    if not processor_service or not processor_service.processor:
        raise HTTPException(status_code=503, detail="Processor not available")
    
    try:
        # Stop the query
        success = processor_service.processor.stop_query(query_name)
        if not success:
            raise HTTPException(status_code=404, detail=f"Query '{query_name}' not found")
        
        # Wait a bit
        time.sleep(2)
        
        # Restart based on query type
        if query_name == "stock_quotes":
            output_base_path = processor_service.config.get_output_base_path()
            processor_service.processor.process_stock_quotes_stream(output_base_path)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown query type: {query_name}")
        
        return {"message": f"Query '{query_name}' restarted successfully"}
        
    except Exception as e:
        logger.error(f"Failed to restart query {query_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/shutdown")
async def shutdown():
    """Graceful shutdown endpoint."""
    logger.info("Shutdown requested via API")
    shutdown_event.set()
    return {"message": "Shutdown initiated"}


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, initiating graceful shutdown")
    shutdown_event.set()


def run_processor_thread():
    """Run the processor monitoring in a separate thread."""
    global processor_service
    
    try:
        if processor_service:
            # Start streaming queries
            processor_service.start_streaming_queries()
            
            # Start monitoring
            processor_service.monitor_streaming_queries()
    except Exception as e:
        logger.error(f"Processor thread error: {str(e)}", exc_info=True)
    finally:
        logger.info("Processor thread finished")


def main():
    """Main entry point for the Spark streaming processor."""
    global processor_service, processor_thread
    
    logger.info("Starting Spark Structured Streaming Processor")
    
    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded successfully")
        
        # Initialize processor service
        processor_service = ProcessorService(config)
        processor_service.initialize_processor()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start processor in separate thread
        processor_thread = threading.Thread(target=run_processor_thread, daemon=True)
        processor_thread.start()
        
        # Start health check server
        logger.info("Starting health check server on port 8080")
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8080,
            log_level="info",
            access_log=True
        )
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup
        logger.info("Cleaning up resources")
        shutdown_event.set()
        
        if processor_service:
            processor_service.stop()
        
        if processor_thread and processor_thread.is_alive():
            logger.info("Waiting for processor thread to finish")
            processor_thread.join(timeout=30)
        
        logger.info("Spark Streaming Processor shutdown complete")


if __name__ == "__main__":
    main()
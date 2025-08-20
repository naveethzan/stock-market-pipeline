#!/usr/bin/env python3
"""
Alpha Vantage Data Producer Entry Point

This module serves as the main entry point for the Alpha Vantage data producer
Docker container. It initializes the producer, sets up health checks, and runs
the continuous data streaming process.
"""
import asyncio
import logging
import os
import signal
import sys
import time
from typing import Optional
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

from ..config.settings import ConfigManager
from ..config.loader import load_config
from .avro_data_producer import AvroDataProducer, AvroDataProducerError
from ..clients.alpha_vantage import AlphaVantageClient, AlphaVantageAPIError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/producer.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)

# Global variables for graceful shutdown
producer: Optional[AvroDataProducer] = None
producer_thread: Optional[threading.Thread] = None
shutdown_event = threading.Event()
app = FastAPI(title="Alpha Vantage Data Producer", version="1.0.0")


class ProducerService:
    """Service class to manage the Avro data producer lifecycle."""
    
    def __init__(self, config: ConfigManager, schema_registry_url: str = "http://schema-registry:8081"):
        self.config = config
        self.schema_registry_url = schema_registry_url
        self.producer: Optional[AvroDataProducer] = None
        self.is_running = False
        self.last_health_check = datetime.now(timezone.utc)
        self.error_count = 0
        self.total_runs = 0
        
    def initialize_producer(self) -> None:
        """Initialize the Avro data producer with error handling."""
        try:
            logger.info("Initializing Alpha Vantage Avro data producer")
            
            # Create Alpha Vantage client
            alpha_vantage_client = AlphaVantageClient(self.config.alpha_vantage)
            
            # Create Avro data producer
            self.producer = AvroDataProducer(
                self.config, 
                alpha_vantage_client, 
                self.schema_registry_url
            )
            
            logger.info("Avro data producer initialized successfully")
            
        except Exception as e:
            error_msg = f"Failed to initialize Avro producer: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroDataProducerError(error_msg) from e
    
    def run_production_cycle(self) -> None:
        """Run a single production cycle for all configured symbols using Avro serialization."""
        if not self.producer:
            raise AvroDataProducerError("Avro producer not initialized")
        
        try:
            symbols = self.config.get_stock_symbols()
            logger.info(f"Starting Avro production cycle for {len(symbols)} symbols")
            
            # Produce real-time quotes with Avro serialization
            quote_results = self.producer.produce_real_time_quotes_avro(symbols)
            successful_quotes = sum(1 for success in quote_results.values() if success)
            
            # Note: Intraday data production removed for simplicity
            # Focus only on real-time quotes
            
            # Update metrics
            self.total_runs += 1
            self.last_health_check = datetime.now(timezone.utc)
            
            if successful_quotes < len(symbols) * 0.8:  # Less than 80% success
                self.error_count += 1
                logger.warning(
                    f"Low success rate in Avro production cycle: {successful_quotes}/{len(symbols)} quotes successful"
                )
            else:
                self.error_count = max(0, self.error_count - 1)  # Reduce error count on success
            
            logger.info(
                f"Avro production cycle completed: {successful_quotes}/{len(symbols)} quotes successful"
            )
            
        except Exception as e:
            self.error_count += 1
            error_msg = f"Avro production cycle failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroDataProducerError(error_msg) from e
    
    def start_continuous_production(self) -> None:
        """Start continuous data production with configurable intervals."""
        logger.info("Starting continuous data production")
        self.is_running = True
        
        # Production interval (default: 60 seconds)
        production_interval = self.config.get_production_interval()
        
        while self.is_running and not shutdown_event.is_set():
            try:
                cycle_start = time.time()
                
                # Run production cycle
                self.run_production_cycle()
                
                # Calculate sleep time to maintain interval
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, production_interval - cycle_duration)
                
                if sleep_time > 0:
                    logger.debug(f"Sleeping for {sleep_time:.2f} seconds until next cycle")
                    if shutdown_event.wait(sleep_time):
                        break  # Shutdown requested
                else:
                    logger.warning(f"Production cycle took {cycle_duration:.2f}s, longer than interval {production_interval}s")
                
            except AvroDataProducerError as e:
                logger.error(f"Avro production error: {str(e)}")
                # Wait before retrying
                if not shutdown_event.wait(min(30, production_interval)):
                    continue
                else:
                    break
                    
            except Exception as e:
                logger.error(f"Unexpected error in production loop: {str(e)}", exc_info=True)
                # Wait before retrying
                if not shutdown_event.wait(min(60, production_interval)):
                    continue
                else:
                    break
        
        self.is_running = False
        logger.info("Continuous data production stopped")
    
    def stop(self) -> None:
        """Stop the producer service."""
        logger.info("Stopping producer service")
        self.is_running = False
        
        if self.producer:
            try:
                self.producer.close()
            except Exception as e:
                logger.error(f"Error closing producer: {str(e)}")
        
        logger.info("Producer service stopped")
    
    def get_health_status(self) -> dict:
        """Get health status for health checks."""
        now = datetime.now(timezone.utc)
        time_since_last_check = (now - self.last_health_check).total_seconds()
        
        # Consider unhealthy if no activity for more than 5 minutes or high error rate
        is_healthy = (
            self.is_running and
            time_since_last_check < 300 and  # 5 minutes
            self.error_count < 10 and  # Less than 10 consecutive errors
            (self.total_runs == 0 or self.error_count / self.total_runs < 0.5)  # Error rate < 50%
        )
        
        metrics = {}
        if self.producer:
            try:
                metrics = self.producer.get_metrics()
            except Exception as e:
                logger.warning(f"Failed to get producer metrics: {str(e)}")
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "is_running": self.is_running,
            "last_health_check": self.last_health_check.isoformat(),
            "time_since_last_check_seconds": time_since_last_check,
            "error_count": self.error_count,
            "total_runs": self.total_runs,
            "error_rate": self.error_count / self.total_runs if self.total_runs > 0 else 0.0,
            "metrics": metrics
        }


# Global producer service instance
producer_service: Optional[ProducerService] = None


@app.get("/health")
async def health_check():
    """Health check endpoint for Docker health checks."""
    global producer_service
    
    if not producer_service:
        raise HTTPException(status_code=503, detail="Producer service not initialized")
    
    health_status = producer_service.get_health_status()
    
    if health_status["status"] == "healthy":
        return JSONResponse(content=health_status, status_code=200)
    else:
        return JSONResponse(content=health_status, status_code=503)


@app.get("/metrics")
async def get_metrics():
    """Get detailed producer metrics."""
    global producer_service
    
    if not producer_service:
        raise HTTPException(status_code=503, detail="Producer service not initialized")
    
    return producer_service.get_health_status()


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


def run_producer_thread():
    """Run the producer in a separate thread."""
    global producer_service
    
    try:
        if producer_service:
            producer_service.start_continuous_production()
    except Exception as e:
        logger.error(f"Producer thread error: {str(e)}", exc_info=True)
    finally:
        logger.info("Producer thread finished")


def main():
    """Main entry point for the Alpha Vantage data producer."""
    global producer_service, producer_thread
    
    logger.info("Starting Alpha Vantage Data Producer")
    
    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded successfully")
        
        # Initialize producer service with Schema Registry URL
        schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
        producer_service = ProducerService(config, schema_registry_url)
        producer_service.initialize_producer()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start producer in separate thread
        producer_thread = threading.Thread(target=run_producer_thread, daemon=True)
        producer_thread.start()
        
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
        
        if producer_service:
            producer_service.stop()
        
        if producer_thread and producer_thread.is_alive():
            logger.info("Waiting for producer thread to finish")
            producer_thread.join(timeout=30)
        
        logger.info("Alpha Vantage Data Producer shutdown complete")


if __name__ == "__main__":
    main()
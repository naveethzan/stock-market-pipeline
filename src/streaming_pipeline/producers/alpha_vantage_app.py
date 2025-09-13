#!/usr/bin/env python3
"""
Stock Market Data Streaming Service Entry Point

This module serves as the main entry point for the stock market data streaming
service. It fetches real-time stock quotes from Alpha Vantage API and publishes
them to Kafka topics for downstream processing.
"""
import asyncio
import logging
import os
import signal
import sys
import time
from typing import Optional, List, Dict
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response
import uvicorn

from ..config.settings import ConfigManager
from ..config.loader import initialize_configuration
from .kafka_avro_producer import AvroDataProducer, AvroDataProducerError
from ..clients.alpha_vantage import AlphaVantageClient, AlphaVantageAPIError
from ..clients.alpha_vantage_mock import MockAlphaVantageClient


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
app = FastAPI(title="Stock Market Data Streaming Service", version="1.0.0")


class ProducerService:
    """Service class to manage the stock market data streaming lifecycle."""
    
    def __init__(self, config: ConfigManager, schema_registry_url: str = "http://schema-registry:8081"):
        self.config = config
        self.schema_registry_url = schema_registry_url
        self.producer: Optional[AvroDataProducer] = None
        self.is_running = False
        self.last_health_check = datetime.now(timezone.utc)
        self.error_count = 0
        self.total_runs = 0
        
    def initialize_stock_data_producer(self) -> None:
        """Initialize the stock market data producer with error handling."""
        try:
            logger.info("Initializing stock market data streaming service")
            
            # Create Alpha Vantage client (real or mock based on configuration)
            if self.config.alpha_vantage.mock_mode:
                logger.info("Mock mode enabled - using MockAlphaVantageClient for continuous streaming")
                alpha_vantage_client = MockAlphaVantageClient(self.config.alpha_vantage)
            else:
                logger.info("Using real Alpha Vantage API client")
                alpha_vantage_client = AlphaVantageClient(self.config.alpha_vantage)
            
            # Create stock market data producer
            self.producer = AvroDataProducer(
                self.config, 
                alpha_vantage_client, 
                self.schema_registry_url
            )
            
            logger.info(
                "Stock market data producer initialized successfully",
                extra={
                    "mock_mode": self.config.alpha_vantage.mock_mode,
                    "client_type": "MockAlphaVantageClient" if self.config.alpha_vantage.mock_mode else "AlphaVantageClient"
                }
            )
            
        except Exception as e:
            error_msg = f"Failed to initialize Avro producer: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroDataProducerError(error_msg) from e
    
    def fetch_and_stream_quote_cycle(self) -> None:
        """Fetch quotes from Alpha Vantage API and stream them to Kafka for all configured symbols."""
        if not self.producer:
            raise AvroDataProducerError("Stock market data producer not initialized")
        
        try:
            symbols = self.config.get_stock_symbols()
            logger.info(f"Fetching and streaming quotes for {len(symbols)} symbols")
            
            # Fetch live quotes from API and stream to Kafka
            quote_results = self.producer.fetch_and_stream_quotes(symbols)
            successful_quotes = sum(1 for success in quote_results.values() if success)
            
            # Fetch and stream intraday data for comprehensive market data
            intraday_results = self.fetch_and_stream_intraday_data(symbols)
            successful_intraday = sum(1 for success in intraday_results.values() if success)
            
            # Update metrics
            self.total_runs += 1
            self.last_health_check = datetime.now(timezone.utc)
            
            if successful_quotes < len(symbols) * 0.8:  # Less than 80% success
                self.error_count += 1
                logger.warning(
                    f"Low success rate in quote fetching and streaming: {successful_quotes}/{len(symbols)} quotes successful"
                )
            else:
                self.error_count = max(0, self.error_count - 1)  # Reduce error count on success
            
            logger.info(
                f"Quote fetching and streaming cycle completed: {successful_quotes}/{len(symbols)} quotes successful, {successful_intraday}/{len(symbols)} intraday data successful"
            )
            
        except Exception as e:
            self.error_count += 1
            error_msg = f"Quote fetching and streaming cycle failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroDataProducerError(error_msg) from e
    
    def fetch_and_stream_intraday_data(self, symbols: List[str], interval: str = "5min") -> Dict[str, bool]:
        """Fetch intraday data from Alpha Vantage API and stream to Kafka topic.
        
        Args:
            symbols: List of stock symbols to fetch intraday data for
            interval: Time interval (1min, 5min, 15min, 30min, 60min)
            
        Returns:
            Dictionary mapping symbols to streaming success status
        """
        if not self.producer:
            raise AvroDataProducerError("Stock market data producer not initialized")
        
        results = {}
        topic = self.config.kafka.stock_intraday_topic
        
        logger.info(
            f"Fetching and streaming intraday data for {len(symbols)} symbols",
            extra={
                "symbols": symbols,
                "topic": topic,
                "interval": interval,
                "symbol_count": len(symbols)
            }
        )
        
        for symbol in symbols:
            try:
                # Fetch intraday data from Alpha Vantage API
                intraday_data = self.producer.alpha_vantage_client.get_intraday_data(symbol, interval)
                
                # Stream intraday data to Kafka topic
                self.producer.publish_intraday_data(topic, intraday_data, key=symbol)
                results[symbol] = True
                
                logger.info(
                    f"Intraday data published successfully for {symbol}",
                    extra={
                        "symbol": symbol,
                        "topic": topic,
                        "interval": interval
                    }
                )
                
            except Exception as e:
                results[symbol] = False
                logger.error(
                    f"Failed to fetch and stream intraday data for {symbol}: {str(e)}",
                    extra={
                        "symbol": symbol,
                        "topic": topic,
                        "interval": interval,
                        "error": str(e)
                    }
                )
        
        return results
    
    def start_continuous_stock_streaming(self) -> None:
        """Start continuous stock market data streaming with configurable intervals."""
        logger.info("Starting continuous stock market data production")
        self.is_running = True
        
        # Production interval (default: 60 seconds)
        production_interval = self.config.get_production_interval()
        
        while self.is_running and not shutdown_event.is_set():
            try:
                cycle_start = time.time()
                
                # Run quote fetch and streaming cycle
                self.fetch_and_stream_quote_cycle()
                
                # Calculate sleep time to maintain interval
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, production_interval - cycle_duration)
                
                if sleep_time > 0:
                    # Sleeping until next cycle
                    if shutdown_event.wait(sleep_time):
                        break  # Shutdown requested
                else:
                    logger.warning(f"Quote cycle took {cycle_duration:.2f}s, longer than interval {production_interval}s")
                
            except AvroDataProducerError as e:
                logger.error(f"Stock market data production error: {str(e)}")
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
        logger.info("Continuous stock market data production stopped")
    
    def run_stock_quote_cycle(self) -> None:
        """Backward compatibility alias for fetch_and_stream_quote_cycle."""
        return self.fetch_and_stream_quote_cycle()
    
    # Additional intuitive aliases for data flow
    def stream_live_market_data(self) -> None:
        """Stream live market data continuously from Alpha Vantage to Kafka."""
        return self.start_continuous_stock_streaming()
    
    def fetch_and_stream_cycle(self) -> None:
        """Fetch quotes from API and stream them to Kafka (single cycle)."""
        return self.fetch_and_stream_quote_cycle()
    
    def setup_streaming_pipeline(self) -> None:
        """Set up the stock market data streaming pipeline."""
        return self.initialize_stock_data_producer()
    
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
    """Get detailed producer metrics in Prometheus format."""
    global producer_service
    
    if not producer_service:
        raise HTTPException(status_code=503, detail="Producer service not initialized")
    
    health_status = producer_service.get_health_status()
    
    # Convert to Prometheus format
    prometheus_metrics = []
    
    # Messages metrics
    messages = health_status.get("metrics", {}).get("messages", {})
    prometheus_metrics.append(f"# HELP alpha_vantage_messages_sent_total Total number of messages sent to Kafka")
    prometheus_metrics.append(f"# TYPE alpha_vantage_messages_sent_total counter")
    prometheus_metrics.append(f'alpha_vantage_messages_sent_total{{job="streaming-producer"}} {messages.get("sent", 0)}')
    
    prometheus_metrics.append(f"# HELP alpha_vantage_messages_failed_total Total number of failed messages")
    prometheus_metrics.append(f"# TYPE alpha_vantage_messages_failed_total counter")
    prometheus_metrics.append(f'alpha_vantage_messages_failed_total{{job="streaming-producer"}} {messages.get("failed", 0)}')
    
    prometheus_metrics.append(f"# HELP alpha_vantage_messages_pending_current Number of pending messages")
    prometheus_metrics.append(f"# TYPE alpha_vantage_messages_pending_current gauge")
    prometheus_metrics.append(f'alpha_vantage_messages_pending_current{{job="streaming-producer"}} {messages.get("pending", 0)}')
    
    prometheus_metrics.append(f"# HELP alpha_vantage_success_rate Current success rate")
    prometheus_metrics.append(f"# TYPE alpha_vantage_success_rate gauge")
    prometheus_metrics.append(f'alpha_vantage_success_rate{{job="streaming-producer"}} {messages.get("success_rate", 0.0)}')
    
    # API metrics
    api = health_status.get("metrics", {}).get("api", {})
    prometheus_metrics.append(f"# HELP alpha_vantage_api_calls_total Total number of API calls")
    prometheus_metrics.append(f"# TYPE alpha_vantage_api_calls_total counter")
    prometheus_metrics.append(f'alpha_vantage_api_calls_total{{job="streaming-producer"}} {api.get("requests", 0)}')
    
    prometheus_metrics.append(f"# HELP alpha_vantage_api_errors_total Total number of API errors")
    prometheus_metrics.append(f"# TYPE alpha_vantage_api_errors_total counter")
    prometheus_metrics.append(f'alpha_vantage_api_errors_total{{job="streaming-producer"}} {api.get("errors", 0)}')
    
    # Throughput metrics
    throughput = health_status.get("metrics", {}).get("throughput", {})
    prometheus_metrics.append(f"# HELP alpha_vantage_throughput_messages_per_second Current throughput in messages per second")
    prometheus_metrics.append(f"# TYPE alpha_vantage_throughput_messages_per_second gauge")
    prometheus_metrics.append(f'alpha_vantage_throughput_messages_per_second{{job="streaming-producer"}} {throughput.get("messages_per_second", 0.0)}')
    
    prometheus_metrics.append(f"# HELP alpha_vantage_bytes_sent_total Total bytes sent")
    prometheus_metrics.append(f"# TYPE alpha_vantage_bytes_sent_total counter")
    prometheus_metrics.append(f'alpha_vantage_bytes_sent_total{{job="streaming-producer"}} {throughput.get("bytes_sent", 0)}')
    
    # Health status
    is_healthy = 1 if health_status.get("status") == "healthy" else 0
    prometheus_metrics.append(f"# HELP alpha_vantage_service_healthy Service health status")
    prometheus_metrics.append(f"# TYPE alpha_vantage_service_healthy gauge")
    prometheus_metrics.append(f'alpha_vantage_service_healthy{{job="streaming-producer"}} {is_healthy}')
    
    return Response(content="\n".join(prometheus_metrics) + "\n", media_type="text/plain")


@app.get("/metrics/json")
async def get_metrics_json():
    """Get detailed producer metrics in JSON format."""
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
            producer_service.start_continuous_stock_streaming()
    except Exception as e:
        logger.error(f"Producer thread error: {str(e)}", exc_info=True)
    finally:
        logger.info("Producer thread finished")


def main():
    """Main entry point for the Stock Market Data Streaming Service."""
    global producer_service, producer_thread
    
    logger.info("Starting Stock Market Data Streaming Service")
    
    try:
        # Load configuration
        config = initialize_configuration()
        logger.info("Configuration loaded successfully")
        
        # Initialize producer service with Schema Registry URL
        schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
        producer_service = ProducerService(config, schema_registry_url)
        producer_service.initialize_stock_data_producer()
        
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
        
        logger.info("Stock Market Data Streaming Service shutdown complete")


if __name__ == "__main__":
    main()
"""
Enhanced producer service for managing data streaming.
Orchestrates clients and producers with proper error handling.
"""

import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from stock_market_pipeline.core.exceptions import IngestionError, KafkaProducerError
from stock_market_pipeline.utils import PipelineLogger
from stock_market_pipeline.ingestion.clients.alpha_vantage_client import AlphaVantageClient
from stock_market_pipeline.ingestion.clients.mock_alpha_vantage_client import MockAlphaVantageClient
from stock_market_pipeline.ingestion.producers.kafka_producer import KafkaProducer
from stock_market_pipeline.storage.schemas import SchemaManager


class ProducerService:
    """
    Enhanced producer service for data streaming.
    
    Orchestrates data ingestion from external APIs and publishing to Kafka topics.
    Manages client selection (mock vs real), handles streaming cycles, and provides
    comprehensive monitoring and health checking capabilities.
    """
    
    def __init__(self, config: Any, schema_registry_url: str = None):
        self.config = config
        self.logger = PipelineLogger(__name__)
        self.schema_registry_url = schema_registry_url or getattr(config, 'schema_registry_url', 'http://localhost:8081')
        
        self.client = self._create_client()
        self.producer = self._create_producer()
        self.schema_manager = SchemaManager(self.schema_registry_url)
        self.is_running = False
        
        self.logger.info(
            "Producer service initialized",
            client_type=type(self.client).__name__,
            schema_registry_url=self.schema_registry_url
        )
    
    def _create_client(self):
        """Create appropriate client based on configuration."""
        mock_mode = getattr(self.config, 'mock_mode', True)
        
        if mock_mode:
            self.logger.info("Using mock Alpha Vantage client")
            return MockAlphaVantageClient(self.config.api)
        else:
            self.logger.info("Using real Alpha Vantage client")
            return AlphaVantageClient(self.config.api)
    
    def _create_producer(self):
        """Create Kafka producer."""
        return KafkaProducer(self.config, self.schema_registry_url)
    
    def start_streaming(self, symbols: List[str], interval_seconds: int = 60) -> None:
        """
        Start continuous streaming of stock data.
        
        Args:
            symbols: List of stock symbols to stream
            interval_seconds: Time interval between streaming cycles
            
        Raises:
            IngestionError: If streaming fails or encounters critical errors
        """
        self.is_running = True
        self.logger.info(f"Starting streaming for symbols: {symbols}")
        
        try:
            while self.is_running:
                self._stream_cycle(symbols)
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            self.logger.info("Streaming stopped by user")
        except Exception as e:
            self.logger.error("Streaming failed", error=e)
            raise IngestionError(f"Streaming failed: {str(e)}")
        finally:
            self.stop_streaming()
    
    def _stream_cycle(self, symbols: List[str]) -> None:
        """Single streaming cycle."""
        for symbol in symbols:
            try:
                quote_data = self.client.get_real_time_quote(symbol)
                
                success = self.producer.produce_stock_quote(
                    topic=self.config.kafka.stock_quotes_topic,
                    data=quote_data,
                    key=symbol
                )
                
                if success:
                    self.logger.info(f"Successfully streamed {symbol}")
                else:
                    self.logger.warning(f"Failed to stream {symbol}")
                    
            except Exception as e:
                self.logger.error(f"Error streaming {symbol}", error=e)
    
    def stream_intraday_data(self, symbols: List[str]) -> Dict[str, bool]:
        """Stream intraday data for symbols."""
        results = {}
        for symbol in symbols:
            try:
                intraday_data = self.client.get_intraday_data(symbol, "5min")
                
                success = self.producer.produce_intraday_data(
                    topic=self.config.kafka.stock_intraday_topic,
                    data=intraday_data,
                    key=symbol
                )
                
                results[symbol] = success
                
            except Exception as e:
                self.logger.error(f"Error streaming intraday data for {symbol}", error=e)
                results[symbol] = False
        
        return results
    
    def stream_single_quote(self, symbol: str) -> bool:
        """Stream a single stock quote."""
        try:
            quote_data = self.client.get_real_time_quote(symbol)
            
            success = self.producer.produce_stock_quote(
                topic=self.config.kafka.stock_quotes_topic,
                data=quote_data,
                key=symbol
            )
            
            if success:
                self.logger.info(f"Successfully streamed single quote for {symbol}")
            else:
                self.logger.warning(f"Failed to stream single quote for {symbol}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error streaming single quote for {symbol}", error=e)
            return False
    
    def stop_streaming(self) -> None:
        """Stop streaming."""
        self.is_running = False
        if self.producer:
            self.producer.close()
        self.logger.info("Streaming stopped")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of all components."""
        return {
            'service_running': self.is_running,
            'client_healthy': self.client.is_healthy(),
            'producer_healthy': self.producer.is_healthy(),
            'client_metrics': self.client.get_metrics(),
            'producer_metrics': self.producer.get_metrics(),
            'schema_manager_available': len(self.schema_manager.get_available_schemas()) > 0
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get combined metrics from all components."""
        return {
            'service': {
                'running': self.is_running,
                'uptime': datetime.now(timezone.utc).isoformat()
            },
            'client': self.client.get_metrics(),
            'producer': self.producer.get_metrics(),
            'schema_manager': {
                'available_schemas': len(self.schema_manager.get_available_schemas()),
                'schemas': self.schema_manager.get_available_schemas()
            }
        }

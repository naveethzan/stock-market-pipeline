"""
Stock Market Data Producer for real-time financial data streaming.
Publishes stock quotes and market data to Kafka topics using Avro serialization.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError

from ..config.settings import ConfigManager
from ..clients.alpha_vantage import AlphaVantageClient, AlphaVantageAPIError
from ..clients.alpha_vantage_mock import MockAlphaVantageClient
from ..schemas.avro_serializer import AvroSerializer, AvroSerializationError
from ..schemas.schema_registry_client import SchemaRegistryClient
from .producer_base import ProducerMetrics


logger = logging.getLogger(__name__)


class AvroDataProducerError(Exception):
    """Custom exception for stock market data producer errors."""
    pass


class AvroDataProducer:
    """
    Stock Market Data Producer for real-time financial streaming.
    
    Fetches stock quotes from Alpha Vantage API and publishes to Kafka topics.
    Uses Avro serialization for type-safe message format and schema evolution.
    """
    
    def __init__(self, config: ConfigManager, alpha_vantage_client: Optional[AlphaVantageClient] = None,
                 schema_registry_url: str = "http://localhost:8085"):
        """
        Initialize the Avro data producer.
        
        Args:
            config: Configuration manager instance
            alpha_vantage_client: Optional Alpha Vantage client
            schema_registry_url: Schema Registry URL
        """
        self.config = config
        self.schema_registry_url = schema_registry_url
        self.metrics = ProducerMetrics()
        
        # Initialize Alpha Vantage client
        if alpha_vantage_client:
            self.alpha_vantage_client = alpha_vantage_client
        else:
            # Default to real client if none provided
            self.alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)
        
        # Check if using mock client
        is_mock_client = isinstance(self.alpha_vantage_client, MockAlphaVantageClient)
        
        # Initialize Schema Registry client
        self.schema_registry_client = SchemaRegistryClient(schema_registry_url)
        
        # Initialize Avro serializer
        self.avro_serializer = AvroSerializer(schema_registry_url)
        
        # Initialize stock data producer
        self.producer = self._create_stock_data_producer()
        
        # Track pending messages for proper shutdown
        self._pending_messages = 0
        
        logger.info(
            "Stock Market Data Producer initialized",
            extra={
                "kafka_brokers": config.kafka.bootstrap_servers,
                "schema_registry_url": schema_registry_url,
                "mock_mode": is_mock_client,
                "client_type": type(self.alpha_vantage_client).__name__,
                "topics": {
                    "quotes": config.kafka.stock_quotes_topic,
                    "intraday": config.kafka.stock_intraday_topic
                }
            }
        )
    
    def _create_stock_data_producer(self) -> AvroProducer:
        """Create and configure stock market data producer with Schema Registry."""
        producer_config = self.config.get_kafka_producer_config()
        
        # Add Schema Registry configuration for stock data serialization
        stock_producer_config = {
            **producer_config,
            'schema.registry.url': self.schema_registry_url,
            'on_delivery': self._stock_quote_delivery_report
        }
        
        logger.info(
            "Creating stock data producer",
            extra={
                "schema_registry_url": self.schema_registry_url,
                "config": {k: v for k, v in stock_producer_config.items() if 'password' not in k.lower()}
            }
        )
        
        try:
            producer = AvroProducer(stock_producer_config)
            logger.info("Stock data producer created successfully")
            return producer
        except Exception as e:
            error_msg = f"Failed to create stock data producer: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroDataProducerError(error_msg) from e
    
    def _stock_quote_delivery_report(self, err, msg) -> None:
        """
        Delivery callback for stock market data messages.
        
        Args:
            err: Kafka error if delivery failed
            msg: Kafka message object
        """
        self._pending_messages -= 1
        
        # Determine message type based on topic
        topic_name = msg.topic()
        is_intraday = "intraday" in topic_name.lower()
        message_type = "Intraday data" if is_intraday else "Stock quote"
        
        if err is not None:
            self.metrics.messages_failed += 1
            logger.error(
                f"{message_type} delivery failed",
                extra={
                    "topic": topic_name,
                    "partition": msg.partition(),
                    "key": msg.key().decode('utf-8') if msg.key() else None,
                    "error": str(err),
                    "pending_messages": self._pending_messages,
                    "message_type": message_type
                }
            )
        else:
            self.metrics.messages_sent += 1
            self.metrics.bytes_sent += len(msg.value()) if msg.value() else 0
            logger.info(
                f"{message_type} published successfully",
                extra={
                    "topic": topic_name,
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode('utf-8') if msg.key() else None,
                    "pending_messages": self._pending_messages,
                    "message_type": message_type
                }
            )
    
    def _get_quote_schema(self):
        """Get stock quote schema for message validation."""
        try:
            return self.avro_serializer._get_avro_schema("stock_quote")
        except Exception as e:
            logger.error(f"Failed to get stock quote schema: {e}")
            raise AvroDataProducerError(f"Schema error: {e}")
    
    def _get_market_data_schema(self):
        """Get intraday market data schema for message validation."""
        try:
            return self.avro_serializer._get_avro_schema("intraday_data")
        except Exception as e:
            logger.error(f"Failed to get intraday schema: {e}")
            raise AvroDataProducerError(f"Schema error: {e}")
    
# Market events removed - focusing only on stock data
    
    def publish_stock_quote(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """
        Publish stock quote data to Kafka topic.
        
        Args:
            topic: Kafka topic name
            data: Stock quote data from Alpha Vantage
            key: Optional message key for partitioning
            
        Raises:
            AvroDataProducerError: If publishing fails
        """
        try:
            # Transform stock quote data for Kafka message format
            transformed_data = self.avro_serializer._transform_stock_quote_data(data)
            
            # Get quote schema for validation
            value_schema = self._get_quote_schema()
            
            # Track pending stock quote message
            self._pending_messages += 1
            
            # Send stock quote to Kafka topic without key to avoid Avro key serialization issues
            self.producer.produce(
                topic=topic,
                value=transformed_data,
                value_schema=value_schema
            )
            
            # Trigger delivery callbacks
            self.producer.poll(0)
            
            logger.info(
                "Stock quote queued for delivery",
                extra={
                    "topic": topic,
                    "symbol": transformed_data.get("symbol"),
                    "key": key,
                    "pending_messages": self._pending_messages
                }
            )
            
        except SerializerError as e:
            self._pending_messages -= 1
            error_msg = f"Stock quote serialization error: {str(e)}"
            logger.error(
                "Stock quote serialization failed",
                extra={
                    "topic": topic,
                    "key": key,
                    "error": error_msg,
                    "serializer_error_type": type(e).__name__,
                    "data_keys": list(transformed_data.keys()) if transformed_data else None,
                    "pending_messages": self._pending_messages
                },
                exc_info=True
            )
            raise AvroDataProducerError(error_msg) from e
            
        except Exception as e:
            self._pending_messages -= 1
            error_msg = f"Failed to publish stock quote: {str(e)}"
            logger.error(
                "Stock quote publishing error",
                extra={
                    "topic": topic,
                    "key": key,
                    "error": error_msg,
                    "exception_type": type(e).__name__,
                    "data_keys": list(transformed_data.keys()) if 'transformed_data' in locals() and transformed_data else None,
                    "pending_messages": self._pending_messages
                },
                exc_info=True
            )
            raise AvroDataProducerError(error_msg) from e
    
    def publish_intraday_data(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """
        Publish intraday market data points to Kafka topic.
        
        Args:
            topic: Kafka topic name
            data: Intraday data from Alpha Vantage
            key: Optional message key for partitioning
        """
        try:
            # Transform market data to individual price points
            data_points = self.avro_serializer._transform_intraday_data(data)
            
            logger.info(
                f"Transformed intraday data: {len(data_points)} data points",
                extra={
                    "topic": topic,
                    "symbol": data.get("_metadata", {}).get("symbol"),
                    "data_points_count": len(data_points),
                    "original_data_keys": list(data.keys()) if isinstance(data, dict) else "not_dict"
                }
            )
            
            if not data_points:
                logger.warning(
                    "No intraday data points to publish - transformation returned empty list",
                    extra={
                        "topic": topic,
                        "symbol": data.get("_metadata", {}).get("symbol"),
                        "original_data": data
                    }
                )
                return
            
            # Get market data schema for validation
            value_schema = self._get_market_data_schema()
            
            # Track initial pending count for this batch
            initial_pending = self._pending_messages
            
            # Send each market data point to Kafka
            for i, point in enumerate(data_points):
                self._pending_messages += 1
                
                # Use timestamp as part of key for better partitioning
                point_key = f"{key}_{point['timestamp']}" if key else point['timestamp']
                
                logger.info(
                    f"Publishing intraday data point {i+1}/{len(data_points)}",
                    extra={
                        "topic": topic,
                        "symbol": point.get('symbol'),
                        "timestamp": point.get('timestamp'),
                        "point_key": point_key
                    }
                )
                
                # Send market data without key to avoid Avro key serialization issues
                self.producer.produce(
                    topic=topic,
                    value=point,
                    value_schema=value_schema
                )
            
            # Trigger delivery callbacks with proper polling and ensure delivery
            self.producer.poll(0.5)  # Increased timeout for multiple intraday messages
            
            # Force delivery of pending messages
            self.producer.flush(1.0)  # Wait up to 1 second for delivery
            
            logger.info(
                "Intraday market data queued for delivery",
                extra={
                    "topic": topic,
                    "symbol": data.get("_metadata", {}).get("symbol"),
                    "data_points": len(data_points),
                    "key_prefix": key,
                    "pending_messages": self._pending_messages,
                    "messages_added": self._pending_messages - initial_pending
                }
            )
            
        except SerializerError as e:
            # Adjust pending count for any failed serializations
            failed_points = len(data_points) if 'data_points' in locals() else 0
            self._pending_messages -= failed_points
            error_msg = f"Intraday data serialization error: {str(e)}"
            logger.error(
                "Intraday data serialization failed",
                extra={
                    "topic": topic,
                    "key": key,
                    "error": error_msg,
                    "serializer_error_type": type(e).__name__,
                    "pending_messages": self._pending_messages
                },
                exc_info=True
            )
            raise AvroDataProducerError(error_msg) from e
            
        except Exception as e:
            # Adjust pending count for any failed publishes
            failed_points = len(data_points) if 'data_points' in locals() else 0
            self._pending_messages -= failed_points
            error_msg = f"Failed to publish intraday data: {str(e)}"
            logger.error(
                "Intraday data publishing error",
                extra={
                    "topic": topic,
                    "key": key,
                    "error": error_msg,
                    "exception_type": type(e).__name__,
                    "pending_messages": self._pending_messages
                },
                exc_info=True
            )
            raise AvroDataProducerError(error_msg) from e
    
# Market events functionality removed
    
    def fetch_and_stream_quotes(self, symbols: List[str]) -> Dict[str, bool]:
        """
        Fetch live stock quotes from Alpha Vantage API and stream them to Kafka.
        
        Args:
            symbols: List of stock symbols to fetch and stream
            
        Returns:
            Dictionary mapping symbols to streaming success status
        """
        results = {}
        topic = self.config.kafka.stock_quotes_topic
        
        logger.info(
            "Fetching and streaming stock quotes to Kafka",
            extra={
                "symbols": symbols,
                "topic": topic,
                "symbol_count": len(symbols)
            }
        )
        
        for symbol in symbols:
            try:
                # Fetch latest stock quote from Alpha Vantage API
                self.metrics.api_requests += 1
                quote_data = self.alpha_vantage_client.get_real_time_quote(symbol)
                
                # Stream stock quote to Kafka topic
                self.publish_stock_quote(topic, quote_data, key=symbol)
                results[symbol] = True
                
                logger.info(
                    "Stock quote published successfully",
                    extra={
                        "symbol": symbol,
                        "price": quote_data.get("Global Quote", {}).get("05. price"),
                        "change": quote_data.get("Global Quote", {}).get("09. change")
                    }
                )
                
            except AlphaVantageAPIError as e:
                self.metrics.api_errors += 1
                results[symbol] = False
                logger.error(
                    "Alpha Vantage API error for stock quote",
                    extra={
                        "symbol": symbol,
                        "error": str(e),
                        "status_code": getattr(e, 'status_code', None)
                    }
                )
                
            except AvroDataProducerError as e:
                results[symbol] = False
                logger.error(
                    "Stock quote producer error",
                    extra={
                        "symbol": symbol,
                        "error": str(e)
                    }
                )
                
            except Exception as e:
                results[symbol] = False
                logger.error(
                    "Unexpected error producing stock quote",
                    extra={
                        "symbol": symbol,
                        "error": str(e)
                    },
                    exc_info=True
                )
        
        successful_count = sum(1 for success in results.values() if success)
        logger.info(
            "Stock quote fetching and streaming completed",
            extra={
                "total_symbols": len(symbols),
                "successful": successful_count,
                "failed": len(symbols) - successful_count,
                "success_rate": successful_count / len(symbols) if symbols else 0.0
            }
        )
        
        return results
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get stock market data producer performance metrics.
        
        Returns:
            Dictionary containing performance and streaming metrics
        """
        base_metrics = {
            "messages": {
                "sent": self.metrics.messages_sent,
                "failed": self.metrics.messages_failed,
                "pending": self._pending_messages,
                "success_rate": (
                    self.metrics.messages_sent / (self.metrics.messages_sent + self.metrics.messages_failed)
                    if (self.metrics.messages_sent + self.metrics.messages_failed) > 0 else 0.0
                )
            },
            "throughput": {
                "messages_per_second": self.metrics.get_throughput_per_second(),
                "bytes_sent": self.metrics.bytes_sent,
                "runtime_seconds": self.metrics.get_runtime_seconds()
            },
            "api": {
                "requests": self.metrics.api_requests,
                "errors": self.metrics.api_errors,
                "error_rate": (
                    self.metrics.api_errors / self.metrics.api_requests
                    if self.metrics.api_requests > 0 else 0.0
                )
            }
        }
        
        # Add streaming infrastructure metrics
        try:
            streaming_metrics = {
                "streaming": {
                    "schema_registry_url": self.schema_registry_url,
                    "serializer_status": self.avro_serializer.get_serializer_status(),
                    "schema_registry_status": self.schema_registry_client.get_registry_status()
                }
            }
            base_metrics.update(streaming_metrics)
        except Exception as e:
            logger.warning(f"Failed to get streaming infrastructure metrics: {e}")
        
        return base_metrics
    
    def flush_pending_quotes(self, timeout: float = 30.0) -> int:
        """
        Flush pending stock quotes and wait for delivery confirmation.
        
        Args:
            timeout: Maximum time to wait for delivery in seconds
            
        Returns:
            Number of stock quotes still pending after timeout
        """
        logger.info(
            "Flushing pending stock market messages",
            extra={
                "pending_messages": self._pending_messages,
                "timeout_seconds": timeout
            }
        )
        
        remaining = self.producer.flush(timeout)
        
        if remaining > 0:
            logger.warning(
                "Stock data producer flush timed out",
                extra={
                    "remaining_messages": remaining,
                    "timeout_seconds": timeout
                }
            )
        else:
            logger.info("All stock market messages flushed successfully")
        
        return remaining
    
    def flush(self, timeout: float = 30.0) -> int:
        """Backward compatibility alias for flush_pending_quotes."""
        return self.flush_pending_quotes(timeout)
    
    # Data flow intuitive method aliases
    def stream_stock_quotes(self, symbols: List[str]) -> Dict[str, bool]:
        """Intuitive alias for fetch_and_stream_quotes - fetch and stream stock quotes to Kafka."""
        return self.fetch_and_stream_quotes(symbols)
    
    def produce_real_time_quotes(self, symbols: List[str]) -> Dict[str, bool]:
        """Backward compatibility alias for fetch_and_stream_quotes."""
        return self.fetch_and_stream_quotes(symbols)
    
    def send_stock_quote(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """Intuitive alias for publish_stock_quote - send individual stock quote."""
        return self.publish_stock_quote(topic, data, key)
    
    def send_market_data(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """Intuitive alias for publish_intraday_data - send market data points."""
        return self.publish_intraday_data(topic, data, key)
    
    # Additional data flow aliases
    def stream_live_quotes(self, symbols: List[str]) -> Dict[str, bool]:
        """Stream live stock quotes from Alpha Vantage to Kafka topics."""
        return self.fetch_and_stream_quotes(symbols)
    
    def push_quote_to_kafka(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """Push a single stock quote to Kafka topic."""
        return self.publish_stock_quote(topic, data, key)
    
    def stream_market_data(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """Stream intraday market data points to Kafka."""
        return self.publish_intraday_data(topic, data, key)
    
    def close(self) -> None:
        """Close the producer and clean up resources."""
        logger.info("Closing Stock Market Data Producer")
        
        # Flush remaining stock quotes
        remaining = self.flush_pending_quotes()
        
        if remaining > 0:
            logger.warning(f"Stock data producer closed with {remaining} messages still pending")
        
        # Close components
        if hasattr(self, 'avro_serializer'):
            self.avro_serializer.close()
        
        if hasattr(self, 'schema_registry_client'):
            self.schema_registry_client.close()
        
        if hasattr(self, 'alpha_vantage_client') and hasattr(self.alpha_vantage_client, 'close'):
            self.alpha_vantage_client.close()
        
        # Log final metrics
        try:
            metrics = self.get_metrics()
            logger.info(
                "Stock Market Data Producer closed",
                extra={
                    "final_metrics": {k: v for k, v in metrics.items() if k != 'streaming'}  # Exclude complex streaming metrics
                }
            )
        except Exception as e:
            logger.warning(f"Failed to log final metrics: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
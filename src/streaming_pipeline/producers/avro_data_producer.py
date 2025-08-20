"""
Avro-enabled Kafka producer for streaming financial data.
Extends the base DataProducer with Avro serialization and Schema Registry integration.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError

from ..config.settings import ConfigManager
from ..clients.alpha_vantage import AlphaVantageClient, AlphaVantageAPIError
from ..schemas.avro_serializer import AvroSerializer, AvroSerializationError
from ..schemas.schema_registry_client import SchemaRegistryClient
from .data_producer import ProducerMetrics


logger = logging.getLogger(__name__)


class AvroDataProducerError(Exception):
    """Custom exception for Avro data producer errors."""
    pass


class AvroDataProducer:
    """
    Kafka producer for streaming financial data with Avro serialization.
    
    Integrates with Schema Registry for schema management and provides
    type-safe message serialization with schema evolution support.
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
            self.alpha_vantage_client = AlphaVantageClient(config.alpha_vantage)
        
        # Initialize Schema Registry client
        self.schema_registry_client = SchemaRegistryClient(schema_registry_url)
        
        # Initialize Avro serializer
        self.avro_serializer = AvroSerializer(schema_registry_url)
        
        # Initialize Avro producer
        self.producer = self._create_avro_producer()
        
        # Track pending messages for proper shutdown
        self._pending_messages = 0
        
        logger.info(
            "AvroDataProducer initialized",
            extra={
                "kafka_brokers": config.kafka.bootstrap_servers,
                "schema_registry_url": schema_registry_url,
                "topics": {
                    "quotes": config.kafka.stock_quotes_topic,
                    "intraday": config.kafka.stock_intraday_topic
                }
            }
        )
    
    def _create_avro_producer(self) -> AvroProducer:
        """Create and configure Avro producer with Schema Registry."""
        producer_config = self.config.get_kafka_producer_config()
        
        # Add Schema Registry configuration
        avro_config = {
            **producer_config,
            'schema.registry.url': self.schema_registry_url,
            'on_delivery': self._delivery_report
        }
        
        logger.info(
            "Creating Avro producer",
            extra={
                "schema_registry_url": self.schema_registry_url,
                "config": {k: v for k, v in avro_config.items() if 'password' not in k.lower()}
            }
        )
        
        try:
            producer = AvroProducer(avro_config)
            logger.info("Avro producer created successfully")
            return producer
        except Exception as e:
            error_msg = f"Failed to create Avro producer: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroDataProducerError(error_msg) from e
    
    def _delivery_report(self, err, msg) -> None:
        """
        Delivery callback for Kafka messages.
        
        Args:
            err: Kafka error if delivery failed
            msg: Kafka message object
        """
        self._pending_messages -= 1
        
        if err is not None:
            self.metrics.messages_failed += 1
            logger.error(
                "Avro message delivery failed",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "key": msg.key().decode('utf-8') if msg.key() else None,
                    "error": str(err),
                    "pending_messages": self._pending_messages
                }
            )
        else:
            self.metrics.messages_sent += 1
            self.metrics.bytes_sent += len(msg.value()) if msg.value() else 0
            logger.debug(
                "Avro message delivered successfully",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode('utf-8') if msg.key() else None,
                    "pending_messages": self._pending_messages
                }
            )
    
    def _get_stock_quote_schema(self):
        """Get stock quote Avro schema from registry."""
        try:
            return self.avro_serializer._get_avro_schema("stock_quote")
        except Exception as e:
            logger.error(f"Failed to get stock quote schema: {e}")
            raise AvroDataProducerError(f"Schema error: {e}")
    
    def _get_intraday_schema(self):
        """Get intraday data Avro schema from registry."""
        try:
            return self.avro_serializer._get_avro_schema("intraday_data")
        except Exception as e:
            logger.error(f"Failed to get intraday schema: {e}")
            raise AvroDataProducerError(f"Schema error: {e}")
    
# Market events removed - focusing only on stock data
    
    def publish_stock_quote_avro(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """
        Publish stock quote data to Kafka using Avro serialization.
        
        Args:
            topic: Kafka topic name
            data: Stock quote data from Alpha Vantage
            key: Optional message key for partitioning
            
        Raises:
            AvroDataProducerError: If publishing fails
        """
        try:
            # Transform data to match Avro schema
            transformed_data = self.avro_serializer._transform_stock_quote_data(data)
            
            # Get schema
            value_schema = self._get_stock_quote_schema()
            
            # Track pending message
            self._pending_messages += 1
            
            # Publish to Kafka with Avro serialization
            self.producer.produce(
                topic=topic,
                value=transformed_data,
                key=key,
                value_schema=value_schema
            )
            
            # Trigger delivery callbacks
            self.producer.poll(0)
            
            logger.info(
                "Avro stock quote message queued for delivery",
                extra={
                    "topic": topic,
                    "symbol": transformed_data.get("symbol"),
                    "key": key,
                    "pending_messages": self._pending_messages
                }
            )
            
        except SerializerError as e:
            self._pending_messages -= 1
            error_msg = f"Avro serialization error: {str(e)}"
            logger.error(
                "Avro serialization failed",
                extra={
                    "topic": topic,
                    "key": key,
                    "error": error_msg,
                    "pending_messages": self._pending_messages
                }
            )
            raise AvroDataProducerError(error_msg) from e
            
        except Exception as e:
            self._pending_messages -= 1
            error_msg = f"Unexpected error publishing Avro message: {str(e)}"
            logger.error(
                "Unexpected Avro publishing error",
                extra={
                    "topic": topic,
                    "key": key,
                    "error": error_msg,
                    "pending_messages": self._pending_messages
                },
                exc_info=True
            )
            raise AvroDataProducerError(error_msg) from e
    
    def publish_intraday_data_avro(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """
        Publish intraday data points to Kafka using Avro serialization.
        
        Args:
            topic: Kafka topic name
            data: Intraday data from Alpha Vantage
            key: Optional message key for partitioning
        """
        try:
            # Transform data to individual data points
            data_points = self.avro_serializer._transform_intraday_data(data)
            
            # Get schema
            value_schema = self._get_intraday_schema()
            
            # Publish each data point
            for point in data_points:
                self._pending_messages += 1
                
                # Use timestamp as part of key for better partitioning
                point_key = f"{key}_{point['timestamp']}" if key else point['timestamp']
                
                self.producer.produce(
                    topic=topic,
                    value=point,
                    key=point_key,
                    value_schema=value_schema
                )
            
            # Trigger delivery callbacks
            self.producer.poll(0)
            
            logger.info(
                "Avro intraday data messages queued for delivery",
                extra={
                    "topic": topic,
                    "symbol": data.get("_metadata", {}).get("symbol"),
                    "data_points": len(data_points),
                    "key_prefix": key,
                    "pending_messages": self._pending_messages
                }
            )
            
        except Exception as e:
            error_msg = f"Failed to publish intraday data: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroDataProducerError(error_msg) from e
    
# Market events functionality removed
    
    def produce_real_time_quotes_avro(self, symbols: List[str]) -> Dict[str, bool]:
        """
        Fetch and produce real-time quotes using Avro serialization.
        
        Args:
            symbols: List of stock symbols to fetch quotes for
            
        Returns:
            Dictionary mapping symbols to success status
        """
        results = {}
        topic = self.config.kafka.stock_quotes_topic
        
        logger.info(
            "Starting Avro real-time quote production",
            extra={
                "symbols": symbols,
                "topic": topic,
                "symbol_count": len(symbols)
            }
        )
        
        for symbol in symbols:
            try:
                # Fetch quote from Alpha Vantage
                self.metrics.api_requests += 1
                quote_data = self.alpha_vantage_client.get_real_time_quote(symbol)
                
                # Publish using Avro serialization
                self.publish_stock_quote_avro(topic, quote_data, key=symbol)
                results[symbol] = True
                
                logger.info(
                    "Avro real-time quote produced successfully",
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
                    "Alpha Vantage API error for Avro quote",
                    extra={
                        "symbol": symbol,
                        "error": str(e),
                        "status_code": getattr(e, 'status_code', None)
                    }
                )
                
            except AvroDataProducerError as e:
                results[symbol] = False
                logger.error(
                    "Avro producer error for quote",
                    extra={
                        "symbol": symbol,
                        "error": str(e)
                    }
                )
                
            except Exception as e:
                results[symbol] = False
                logger.error(
                    "Unexpected error producing Avro quote",
                    extra={
                        "symbol": symbol,
                        "error": str(e)
                    },
                    exc_info=True
                )
        
        successful_count = sum(1 for success in results.values() if success)
        logger.info(
            "Avro real-time quote production completed",
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
        Get producer performance metrics including Avro-specific metrics.
        
        Returns:
            Dictionary containing performance metrics
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
        
        # Add Avro-specific metrics
        try:
            avro_metrics = {
                "avro": {
                    "schema_registry_url": self.schema_registry_url,
                    "serializer_status": self.avro_serializer.get_serializer_status(),
                    "schema_registry_status": self.schema_registry_client.get_registry_status()
                }
            }
            base_metrics.update(avro_metrics)
        except Exception as e:
            logger.warning(f"Failed to get Avro metrics: {e}")
        
        return base_metrics
    
    def flush(self, timeout: float = 30.0) -> int:
        """
        Flush pending messages and wait for delivery.
        
        Args:
            timeout: Maximum time to wait for delivery in seconds
            
        Returns:
            Number of messages still pending after timeout
        """
        logger.info(
            "Flushing Avro producer messages",
            extra={
                "pending_messages": self._pending_messages,
                "timeout_seconds": timeout
            }
        )
        
        remaining = self.producer.flush(timeout)
        
        if remaining > 0:
            logger.warning(
                "Avro producer flush timed out",
                extra={
                    "remaining_messages": remaining,
                    "timeout_seconds": timeout
                }
            )
        else:
            logger.info("All Avro messages flushed successfully")
        
        return remaining
    
    def close(self) -> None:
        """Close the producer and clean up resources."""
        logger.info("Closing AvroDataProducer")
        
        # Flush remaining messages
        remaining = self.flush()
        
        if remaining > 0:
            logger.warning(f"Avro producer closed with {remaining} messages still pending")
        
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
                "AvroDataProducer closed",
                extra={
                    "final_metrics": {k: v for k, v in metrics.items() if k != 'avro'}  # Exclude complex Avro metrics
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
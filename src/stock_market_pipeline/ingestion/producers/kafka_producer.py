"""
Enhanced Kafka producer for publishing stock data.
Uses Avro serialization with Schema Registry integration.
"""

from typing import Dict, Any, Optional
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError

from stock_market_pipeline.ingestion.producers.base_producer import BaseKafkaProducer
from stock_market_pipeline.core.exceptions import KafkaProducerError, AvroSerializationError
from stock_market_pipeline.utils import PipelineLogger
from stock_market_pipeline.storage.schemas import AvroSerializer


class KafkaProducer(BaseKafkaProducer):
    """
    Enhanced Kafka producer with Avro serialization.
    
    Publishes stock market data to Kafka topics using Avro schemas for
    efficient serialization and schema evolution. Integrates with Schema
    Registry for schema management and provides robust error handling.
    """
    
    def __init__(self, config: Any, schema_registry_url: str):
        super().__init__(config, PipelineLogger(__name__))
        self.schema_registry_url = schema_registry_url
        self.avro_serializer = AvroSerializer(schema_registry_url)
        self.producer = self._create_producer()
    
    def produce(self, data: Dict[str, Any]) -> bool:
        """Produce data to Kafka (implements DataProducer interface)."""
        return self.produce_stock_quote(
            topic=self.config.kafka.stock_quotes_topic,
            data=data
        )
    
    def produce_stock_quote(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Produce stock quote to Kafka using GLOBAL_QUOTE schema."""
        try:
            serialized_data = self._serialize_stock_quote(data)
            
            self.producer.produce(
                topic=topic,
                value=serialized_data,
                key=key
            )
            
            self._update_metrics(True)
            self.logger.info(f"Stock quote produced to {topic}", symbol=data.get('symbol'))
            return True
            
        except Exception as e:
            self._update_metrics(False)
            self.logger.error(f"Failed to produce stock quote", error=e)
            raise KafkaProducerError(
                f"Failed to produce stock quote: {str(e)}",
                topic=topic,
                component="kafka_producer",
                context={"symbol": data.get('symbol'), "topic": topic}
            )
    
    def produce_intraday_data(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Produce intraday data to Kafka using TIME_SERIES_INTRADAY schema."""
        try:
            serialized_data = self._serialize_intraday_data(data)
            
            self.producer.produce(
                topic=topic,
                value=serialized_data,
                key=key
            )
            
            self._update_metrics(True)
            self.logger.info(f"Intraday data produced to {topic}", symbol=data.get('symbol'))
            return True
            
        except Exception as e:
            self._update_metrics(False)
            self.logger.error(f"Failed to produce intraday data", error=e)
            raise KafkaProducerError(
                f"Failed to produce intraday data: {str(e)}",
                topic=topic,
                component="kafka_producer",
                context={"symbol": data.get('symbol'), "topic": topic}
            )
    
    def _serialize_stock_quote(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize stock quote data using Avro serializer."""
        return self.avro_serializer.serialize_stock_quote(data)
    
    def _serialize_intraday_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize intraday data using Avro serializer."""
        return self.avro_serializer.serialize_intraday_data(data)
    
    def _create_producer(self) -> AvroProducer:
        """Create Avro producer with configuration."""
        producer_config = {
            'bootstrap.servers': ','.join(self.config.kafka.bootstrap_servers),
            'security.protocol': self.config.kafka.security_protocol,
            'acks': self.config.kafka.producer_acks,
            'retries': self.config.kafka.producer_retries,
            'batch.size': self.config.kafka.producer_batch_size,
            'linger.ms': self.config.kafka.producer_linger_ms,
            'compression.type': self.config.kafka.producer_compression,
            'schema.registry.url': self.schema_registry_url
        }
        
        return AvroProducer(producer_config)
    
    def flush(self, timeout: float = 30.0) -> int:
        """Flush pending messages."""
        return self.producer.flush(timeout)
    
    def close(self) -> None:
        """Close producer."""
        self.producer.flush()
        self.producer = None
        self._is_healthy = False

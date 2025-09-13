"""
Avro serialization and deserialization for Kafka messages.
Integrates with Schema Registry for schema management.
"""
import io
import json
import logging
import struct
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Union, List

import avro.schema
import avro.io
from confluent_kafka.avro import AvroProducer, AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from .schema_registry_client import SchemaRegistryClient
from .avro_schemas import get_all_schemas


logger = logging.getLogger(__name__)


class AvroSerializationError(Exception):
    """Custom exception for Avro serialization errors."""
    pass


class AvroSerializer:
    """
    Avro serializer for Kafka messages with Schema Registry integration.
    
    Handles serialization of Python objects to Avro binary format
    with schema validation and registry management.
    """
    
    def __init__(self, schema_registry_url: str = "http://localhost:8085"):
        """
        Initialize Avro serializer.
        
        Args:
            schema_registry_url: URL of Schema Registry service
        """
        self.schema_registry_url = schema_registry_url
        self.schema_registry_client = SchemaRegistryClient(schema_registry_url)
        self.schemas = get_all_schemas()
        self._schema_cache = {}
        
        logger.info(
            "Avro serializer initialized",
            extra={
                "schema_registry_url": schema_registry_url,
                "available_schemas": list(self.schemas.keys())
            }
        )
    
    def _get_avro_schema(self, schema_name: str) -> avro.schema.Schema:
        """
        Get compiled Avro schema object.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            Compiled Avro schema
        """
        if schema_name not in self._schema_cache:
            if schema_name not in self.schemas:
                raise AvroSerializationError(f"Unknown schema: {schema_name}")
            
            schema_dict = self.schemas[schema_name]
            self._schema_cache[schema_name] = avro.schema.parse(json.dumps(schema_dict))
        
        return self._schema_cache[schema_name]
    
    def _transform_stock_quote_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Alpha Vantage GLOBAL_QUOTE data to Avro schema format.
        
        Args:
            data: Raw data from Alpha Vantage API
            
        Returns:
            Transformed data matching Avro schema
        """
        # Extract Global Quote data
        global_quote = data.get("Global Quote", {})
        metadata = data.get("_metadata", {})
        
        # Helper function to safely convert to float
        def safe_float(value: str) -> Optional[float]:
            if not value or value == "None":
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        # Helper function to safely convert to int
        def safe_int(value: str) -> Optional[int]:
            if not value or value == "None":
                return None
            try:
                return int(float(value))  # Handle cases like "123.0"
            except (ValueError, TypeError):
                return None
        
        # Helper function to clean percentage
        def clean_percentage(value: str) -> Optional[float]:
            if not value or value == "None":
                return None
            try:
                # Remove % sign and convert to decimal
                cleaned = value.replace('%', '').strip()
                return float(cleaned)
            except (ValueError, TypeError):
                return None
        
        # Extract symbol with validation - ensure it's never null or empty
        symbol = global_quote.get("01. symbol", "")
        if not symbol or symbol.strip() == "":
            # Raise error instead of using fallback
            error_msg = f"Empty symbol in global quote data - this should not happen after filtering"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        transformed = {
            "symbol": symbol,
            "open_price": safe_float(global_quote.get("02. open")),
            "high_price": safe_float(global_quote.get("03. high")),
            "low_price": safe_float(global_quote.get("04. low")),
            "current_price": safe_float(global_quote.get("05. price", "0")) or 0.0,
            "volume": safe_int(global_quote.get("06. volume")),
            "latest_trading_day": global_quote.get("07. latest trading day"),
            "previous_close": safe_float(global_quote.get("08. previous close")),
            "change": safe_float(global_quote.get("09. change")),
            "change_percent": clean_percentage(global_quote.get("10. change percent")),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),  # epoch millis
            "producer_metadata": {
                "producer_timestamp": metadata.get("producer_timestamp", datetime.now(timezone.utc).isoformat()),
                "producer_version": metadata.get("producer_version", "1.0.0"),
                "data_source": metadata.get("data_source", "alpha_vantage")
            }
        }
        
        return transformed
    
    def _transform_intraday_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform Alpha Vantage TIME_SERIES_INTRADAY data to Avro schema format.
        
        Args:
            data: Raw intraday data from Alpha Vantage API
            
        Returns:
            List of transformed data points matching Avro schema
        """
        metadata = data.get("_metadata", {})
        meta_data = data.get("Meta Data", {})
        
        symbol = metadata.get("symbol", meta_data.get("2. Symbol", ""))
        # Validate symbol is not empty - skip if invalid
        if not symbol or symbol.strip() == "":
            logger.warning("Empty symbol in intraday data, skipping entire dataset")
            return []  # Return empty list to skip invalid data
        interval = metadata.get("interval", meta_data.get("4. Interval", "1min"))
        request_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        # Find time series data - handle both real API format and mock format
        time_series = {}
        for key in data.keys():
            if key.startswith("Time Series"):
                time_series = data[key]
                break
        
        # Fallback to generic "Time Series" key if no specific format found
        if not time_series:
            time_series = data.get("Time Series", {})
        
        transformed_points = []
        
        for timestamp_str, values in time_series.items():
            try:
                point = {
                    "symbol": symbol,
                    "timestamp": timestamp_str,
                    "open_price": float(values.get("1. open", 0)),
                    "high_price": float(values.get("2. high", 0)),
                    "low_price": float(values.get("3. low", 0)),
                    "close_price": float(values.get("4. close", 0)),
                    "volume": int(float(values.get("5. volume", 0))),
                    "interval": interval,
                    "request_timestamp": request_timestamp,
                    "producer_metadata": {
                        "producer_timestamp": metadata.get("producer_timestamp", datetime.now(timezone.utc).isoformat()),
                        "producer_version": metadata.get("producer_version", "1.0.0"),
                        "data_source": metadata.get("data_source", "alpha_vantage")
                    }
                }
                transformed_points.append(point)
                
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Skipping invalid intraday data point",
                    extra={
                        "symbol": symbol,
                        "timestamp": timestamp_str,
                        "error": str(e)
                    }
                )
                continue
        
        return transformed_points
    
    def serialize_stock_quote(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize stock quote data to Avro binary format.
        
        Args:
            data: Stock quote data from Alpha Vantage
            
        Returns:
            Avro serialized bytes
        """
        try:
            # Transform data to match schema
            transformed_data = self._transform_stock_quote_data(data)
            
            # Get schema
            schema = self._get_avro_schema("stock_quote")
            
            # Serialize
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(transformed_data, encoder)
            
            serialized_data = bytes_writer.getvalue()
            
            # Stock quote serialized successfully
            
            return serialized_data
            
        except Exception as e:
            error_msg = f"Failed to serialize stock quote: {str(e)}"
            logger.error(
                "Stock quote serialization error",
                extra={
                    "error": error_msg,
                    "data_keys": list(data.keys()) if isinstance(data, dict) else "non-dict"
                },
                exc_info=True
            )
            raise AvroSerializationError(error_msg) from e
    
    def serialize_processed_stock_prices(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize processed stock prices data to Avro binary format.
        
        Args:
            data: Processed stock prices data
            
        Returns:
            Avro serialized bytes
        """
        try:
            # Transform timestamps to epoch millis if needed
            transformed_data = data.copy()
            
            # Validate and ensure symbol is never null or empty
            if 'symbol' not in transformed_data or not transformed_data['symbol'] or transformed_data['symbol'].strip() == "":
                error_msg = "Empty or null symbol in processed stock prices data - this should not happen after filtering"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Convert timestamp fields to epoch millis
            if 'producer_timestamp' in transformed_data and transformed_data['producer_timestamp']:
                if isinstance(transformed_data['producer_timestamp'], str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(transformed_data['producer_timestamp'].replace('Z', '+00:00'))
                    transformed_data['producer_timestamp'] = int(dt.timestamp() * 1000)
                elif hasattr(transformed_data['producer_timestamp'], 'timestamp'):
                    # Handle Spark timestamp objects
                    transformed_data['producer_timestamp'] = int(transformed_data['producer_timestamp'].timestamp() * 1000)
            
            if 'processing_timestamp' in transformed_data and transformed_data['processing_timestamp']:
                if isinstance(transformed_data['processing_timestamp'], str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(transformed_data['processing_timestamp'].replace('Z', '+00:00'))
                    transformed_data['processing_timestamp'] = int(dt.timestamp() * 1000)
                elif hasattr(transformed_data['processing_timestamp'], 'timestamp'):
                    # Handle Spark timestamp objects
                    transformed_data['processing_timestamp'] = int(transformed_data['processing_timestamp'].timestamp() * 1000)
            
            # Ensure all required fields have proper values
            # Handle null values properly for Avro schema
            for field in ['open_price', 'high_price', 'low_price', 'previous_close', 'change', 'change_percent', 
                         'sma_5min', 'sma_20min', 'price_trend_5min', 'price_volatility', 'trading_session', 'producer_timestamp']:
                if field not in transformed_data:
                    transformed_data[field] = None
            
            # Ensure required fields are present
            if 'current_price' not in transformed_data:
                raise ValueError("current_price is required but missing from data")
            if 'processing_timestamp' not in transformed_data:
                import time
                transformed_data['processing_timestamp'] = int(time.time() * 1000)
            
            # Get schema and serialize
            schema = self._get_avro_schema("processed_stock_prices")
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(transformed_data, encoder)
            
            serialized_data = bytes_writer.getvalue()
            
            # Processed stock prices serialized successfully
            
            return serialized_data
            
        except Exception as e:
            error_msg = f"Failed to serialize processed stock prices: {str(e)}"
            logger.error(
                "Processed stock prices serialization error",
                extra={
                    "error": error_msg,
                    "data_keys": list(data.keys()) if isinstance(data, dict) else "non-dict",
                    "symbol": data.get("symbol", "unknown") if isinstance(data, dict) else "unknown"
                },
                exc_info=True
            )
            raise AvroSerializationError(error_msg) from e
    
    def serialize_processed_trading_volume(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize processed trading volume data to Avro binary format.
        
        Args:
            data: Processed trading volume data
            
        Returns:
            Avro serialized bytes
        """
        try:
            # Transform timestamps to epoch millis if needed
            transformed_data = data.copy()
            
            # Convert timestamp fields
            if 'producer_timestamp' in transformed_data and transformed_data['producer_timestamp']:
                if isinstance(transformed_data['producer_timestamp'], str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(transformed_data['producer_timestamp'].replace('Z', '+00:00'))
                    transformed_data['producer_timestamp'] = int(dt.timestamp() * 1000)
                elif hasattr(transformed_data['producer_timestamp'], 'timestamp'):
                    # Handle Spark timestamp objects
                    transformed_data['producer_timestamp'] = int(transformed_data['producer_timestamp'].timestamp() * 1000)
            
            if 'processing_timestamp' in transformed_data and transformed_data['processing_timestamp']:
                if isinstance(transformed_data['processing_timestamp'], str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(transformed_data['processing_timestamp'].replace('Z', '+00:00'))
                    transformed_data['processing_timestamp'] = int(dt.timestamp() * 1000)
                elif hasattr(transformed_data['processing_timestamp'], 'timestamp'):
                    # Handle Spark timestamp objects
                    transformed_data['processing_timestamp'] = int(transformed_data['processing_timestamp'].timestamp() * 1000)
            
            # Ensure all optional fields have proper values
            for field in ['volume', 'volume_weighted_price', 'volume_sma_5min', 'volume_ratio', 
                         'volume_category', 'trading_session', 'producer_timestamp']:
                if field not in transformed_data:
                    transformed_data[field] = None
            
            # Ensure required fields are present
            if 'processing_timestamp' not in transformed_data:
                import time
                transformed_data['processing_timestamp'] = int(time.time() * 1000)
            
            # Get schema and serialize
            schema = self._get_avro_schema("processed_trading_volume")
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(transformed_data, encoder)
            
            serialized_data = bytes_writer.getvalue()
            
            # Processed trading volume serialized successfully
            
            return serialized_data
            
        except Exception as e:
            error_msg = f"Failed to serialize processed trading volume: {str(e)}"
            logger.error(
                "Processed trading volume serialization error",
                extra={
                    "error": error_msg,
                    "data_keys": list(data.keys()) if isinstance(data, dict) else "non-dict",
                    "symbol": data.get("symbol", "unknown") if isinstance(data, dict) else "unknown"
                },
                exc_info=True
            )
            raise AvroSerializationError(error_msg) from e
    
    def serialize_processed_technical_indicators(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize processed technical indicators data to Avro binary format.
        
        Args:
            data: Processed technical indicators data
            
        Returns:
            Avro serialized bytes
        """
        try:
            # Transform timestamps to epoch millis if needed
            transformed_data = data.copy()
            
            # Convert timestamp fields
            if 'producer_timestamp' in transformed_data and transformed_data['producer_timestamp']:
                if isinstance(transformed_data['producer_timestamp'], str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(transformed_data['producer_timestamp'].replace('Z', '+00:00'))
                    transformed_data['producer_timestamp'] = int(dt.timestamp() * 1000)
                elif hasattr(transformed_data['producer_timestamp'], 'timestamp'):
                    # Handle Spark timestamp objects
                    transformed_data['producer_timestamp'] = int(transformed_data['producer_timestamp'].timestamp() * 1000)
            
            if 'processing_timestamp' in transformed_data and transformed_data['processing_timestamp']:
                if isinstance(transformed_data['processing_timestamp'], str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(transformed_data['processing_timestamp'].replace('Z', '+00:00'))
                    transformed_data['processing_timestamp'] = int(dt.timestamp() * 1000)
                elif hasattr(transformed_data['processing_timestamp'], 'timestamp'):
                    # Handle Spark timestamp objects
                    transformed_data['processing_timestamp'] = int(transformed_data['processing_timestamp'].timestamp() * 1000)
            
            # Ensure all optional fields have proper values
            for field in ['sma_5min', 'sma_20min', 'price_trend_5min', 'price_volatility', 'volume_ratio',
                         'momentum_signal', 'volatility_level', 'trading_session', 'producer_timestamp']:
                if field not in transformed_data:
                    transformed_data[field] = None
            
            # Ensure required fields are present
            if 'current_price' not in transformed_data:
                raise ValueError("current_price is required but missing from data")
            if 'processing_timestamp' not in transformed_data:
                import time
                transformed_data['processing_timestamp'] = int(time.time() * 1000)
            
            # Get schema and serialize
            schema = self._get_avro_schema("processed_technical_indicators")
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(transformed_data, encoder)
            
            serialized_data = bytes_writer.getvalue()
            
            # Processed technical indicators serialized successfully
            
            return serialized_data
            
        except Exception as e:
            error_msg = f"Failed to serialize processed technical indicators: {str(e)}"
            logger.error(
                "Processed technical indicators serialization error",
                extra={
                    "error": error_msg,
                    "data_keys": list(data.keys()) if isinstance(data, dict) else "non-dict",
                    "symbol": data.get("symbol", "unknown") if isinstance(data, dict) else "unknown"
                },
                exc_info=True
            )
            raise AvroSerializationError(error_msg) from e
    
    def serialize_data_quality_alert(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize data quality alert to Avro binary format.
        
        Args:
            data: Data quality alert data
            
        Returns:
            Avro serialized bytes
        """
        try:
            # Transform timestamp to epoch millis if needed
            transformed_data = data.copy()
            
            if 'timestamp' in transformed_data and transformed_data['timestamp']:
                if isinstance(transformed_data['timestamp'], str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(transformed_data['timestamp'].replace('Z', '+00:00'))
                    transformed_data['timestamp'] = int(dt.timestamp() * 1000)
                elif hasattr(transformed_data['timestamp'], 'timestamp'):
                    # datetime object
                    transformed_data['timestamp'] = int(transformed_data['timestamp'].timestamp() * 1000)
            
            # Get schema and serialize
            schema = self._get_avro_schema("data_quality_alert")
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(transformed_data, encoder)
            
            serialized_data = bytes_writer.getvalue()
            
            # Data quality alert serialized successfully
            
            return serialized_data
            
        except Exception as e:
            error_msg = f"Failed to serialize data quality alert: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroSerializationError(error_msg) from e

    def serialize_intraday_data_point(self, data: Dict[str, Any]) -> bytes:
        """
        Serialize single intraday data point to Avro binary format.
        
        Args:
            data: Single intraday data point
            
        Returns:
            Avro serialized bytes
        """
        try:
            # Get schema
            schema = self._get_avro_schema("intraday_data")
            
            # Serialize
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)
            
            serialized_data = bytes_writer.getvalue()
            
            # Intraday data point serialized successfully
            
            return serialized_data
            
        except Exception as e:
            error_msg = f"Failed to serialize intraday data point: {str(e)}"
            logger.error(
                "Intraday data serialization error",
                extra={
                    "error": error_msg,
                    "symbol": data.get("symbol") if isinstance(data, dict) else "unknown"
                },
                exc_info=True
            )
            raise AvroSerializationError(error_msg) from e
    
# Market events serialization removed
    
    def deserialize(self, data: bytes, schema_name: str) -> Dict[str, Any]:
        """
        Deserialize Avro binary data back to Python object.
        
        Args:
            data: Avro serialized bytes
            schema_name: Name of the schema to use for deserialization
            
        Returns:
            Deserialized Python object
        """
        try:
            # Get schema
            schema = self._get_avro_schema(schema_name)
            
            # Deserialize
            bytes_reader = io.BytesIO(data)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(schema)
            
            deserialized_data = reader.read(decoder)
            
            # Data deserialized successfully
            
            return deserialized_data
            
        except Exception as e:
            error_msg = f"Failed to deserialize data with schema {schema_name}: {str(e)}"
            logger.error(
                "Deserialization error",
                extra={
                    "error": error_msg,
                    "schema_name": schema_name,
                    "data_size_bytes": len(data)
                },
                exc_info=True
            )
            raise AvroSerializationError(error_msg) from e
    
    def get_serializer_status(self) -> Dict[str, Any]:
        """
        Get serializer status and metrics.
        
        Returns:
            Status information
        """
        return {
            "schema_registry_url": self.schema_registry_url,
            "available_schemas": list(self.schemas.keys()),
            "cached_schemas": list(self._schema_cache.keys()),
            "schema_registry_status": self.schema_registry_client.get_registry_status()
        }
    
    def close(self) -> None:
        """Close the serializer and cleanup resources."""
        if self.schema_registry_client:
            self.schema_registry_client.close()
        logger.info("Avro serializer closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class AvroKafkaProducer:
    """
    Kafka producer with Avro serialization and Schema Registry integration.
    
    Wraps confluent-kafka AvroProducer with custom serialization logic.
    """
    
    def __init__(self, kafka_config: Dict[str, Any], schema_registry_url: str = "http://localhost:8085"):
        """
        Initialize Avro Kafka producer.
        
        Args:
            kafka_config: Kafka producer configuration
            schema_registry_url: Schema Registry URL
        """
        self.serializer = AvroSerializer(schema_registry_url)
        
        # Configure for Avro
        avro_config = {
            **kafka_config,
            'schema.registry.url': schema_registry_url
        }
        
        self.producer = AvroProducer(avro_config)
        
        logger.info(
            "Avro Kafka producer initialized",
            extra={
                "schema_registry_url": schema_registry_url,
                "kafka_brokers": kafka_config.get('bootstrap.servers')
            }
        )
    
    def produce_stock_quote(self, topic: str, data: Dict[str, Any], key: Optional[str] = None):
        """
        Produce stock quote message with Avro serialization.
        
        Args:
            topic: Kafka topic
            data: Stock quote data
            key: Optional message key
        """
        try:
            # Use the built-in Avro producer with schema registry
            self.producer.produce(
                topic=topic,
                value=self.serializer._transform_stock_quote_data(data),
                key=key,
                value_schema=self.serializer._get_avro_schema("stock_quote")
            )
            
            # Stock quote message produced
            
        except Exception as e:
            error_msg = f"Failed to produce stock quote message: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise AvroSerializationError(error_msg) from e
    
    def flush(self, timeout: float = 30.0) -> int:
        """Flush pending messages."""
        return self.producer.flush(timeout)
    
    def close(self) -> None:
        """Close producer and cleanup resources."""
        if hasattr(self, 'producer'):
            self.producer.flush()
        if hasattr(self, 'serializer'):
            self.serializer.close()
        logger.info("Avro Kafka producer closed")
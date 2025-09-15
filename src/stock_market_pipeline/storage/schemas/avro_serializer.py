"""
Simplified Avro serializer for Kafka messages.
Focuses on core serialization/deserialization functionality.
"""

import io
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import avro.schema
import avro.io

from stock_market_pipeline.core.exceptions import AvroSerializationError
from stock_market_pipeline.utils import PipelineLogger
from stock_market_pipeline.storage.schemas.schema_manager import SchemaManager


class AvroSerializer:
    """
    Avro serializer for Kafka messages with schema validation.
    
    Provides comprehensive serialization capabilities for stock market data
    including real-time quotes, intraday data, and processed analytical data.
    Handles data transformation, validation, and Avro binary format conversion
    with proper error handling and logging.
    """
    
    def __init__(self, schema_registry_url: str = "http://localhost:8081"):
        """
        Initialize Avro serializer.
        
        Args:
            schema_registry_url: URL of Schema Registry service
        """
        self.schema_registry_url = schema_registry_url
        self.schema_manager = SchemaManager(schema_registry_url)
        self.logger = PipelineLogger(__name__)
        
        self.logger.info(
            "Avro serializer initialized",
            schema_registry_url=schema_registry_url,
            available_schemas=self.schema_manager.get_available_schemas()
        )
    
    def serialize_stock_quote(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize stock quote data to Avro format.
        
        Transforms Alpha Vantage GLOBAL_QUOTE API response into the standardized
        stock quote schema format with proper data type conversion and validation.
        
        Args:
            data: Stock quote data from Alpha Vantage GLOBAL_QUOTE API
            
        Returns:
            Transformed data ready for Avro serialization with schema compliance
            
        Raises:
            AvroSerializationError: If data transformation or validation fails
        """
        try:
            # Transform data to match schema
            transformed_data = self._transform_stock_quote_data(data)
            
            # Validate data
            if not self.schema_manager.validate_data("stock_quote", transformed_data):
                raise AvroSerializationError("Stock quote data validation failed")
            
            return transformed_data
            
        except Exception as e:
            self.logger.error("Failed to serialize stock quote", error=e)
            raise AvroSerializationError(f"Failed to serialize stock quote: {str(e)}")
    
    def serialize_intraday_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize intraday data to Avro format.
        
        Transforms Alpha Vantage TIME_SERIES_INTRADAY API response into the
        standardized intraday data schema format, extracting the most recent
        data point with proper OHLCV formatting.
        
        Args:
            data: Intraday data from Alpha Vantage TIME_SERIES_INTRADAY API
            
        Returns:
            Transformed data ready for Avro serialization with schema compliance
            
        Raises:
            AvroSerializationError: If data transformation or validation fails
        """
        try:
            # Transform data to match schema
            transformed_data = self._transform_intraday_data(data)
            
            # Validate data
            if not self.schema_manager.validate_data("intraday_data", transformed_data):
                raise AvroSerializationError("Intraday data validation failed")
            
            return transformed_data
            
        except Exception as e:
            self.logger.error("Failed to serialize intraday data", error=e)
            raise AvroSerializationError(f"Failed to serialize intraday data: {str(e)}")
    
    def serialize_processed_stock_prices(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize processed stock prices data to Avro format.
        
        Args:
            data: Processed stock prices data
            
        Returns:
            Transformed data ready for Avro serialization
        """
        try:
            # Transform data to match schema
            transformed_data = self._transform_processed_data(data, "processed_stock_prices")
            
            # Validate data
            if not self.schema_manager.validate_data("processed_stock_prices", transformed_data):
                raise AvroSerializationError("Processed stock prices data validation failed")
            
            return transformed_data
            
        except Exception as e:
            self.logger.error("Failed to serialize processed stock prices", error=e)
            raise AvroSerializationError(f"Failed to serialize processed stock prices: {str(e)}")
    
    def serialize_processed_trading_volume(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize processed trading volume data to Avro format.
        
        Args:
            data: Processed trading volume data
            
        Returns:
            Transformed data ready for Avro serialization
        """
        try:
            # Transform data to match schema
            transformed_data = self._transform_processed_data(data, "processed_trading_volume")
            
            # Validate data
            if not self.schema_manager.validate_data("processed_trading_volume", transformed_data):
                raise AvroSerializationError("Processed trading volume data validation failed")
            
            return transformed_data
            
        except Exception as e:
            self.logger.error("Failed to serialize processed trading volume", error=e)
            raise AvroSerializationError(f"Failed to serialize processed trading volume: {str(e)}")
    
    def serialize_processed_technical_indicators(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize processed technical indicators data to Avro format.
        
        Args:
            data: Processed technical indicators data
            
        Returns:
            Transformed data ready for Avro serialization
        """
        try:
            # Transform data to match schema
            transformed_data = self._transform_processed_data(data, "processed_technical_indicators")
            
            # Validate data
            if not self.schema_manager.validate_data("processed_technical_indicators", transformed_data):
                raise AvroSerializationError("Processed technical indicators data validation failed")
            
            return transformed_data
            
        except Exception as e:
            self.logger.error("Failed to serialize processed technical indicators", error=e)
            raise AvroSerializationError(f"Failed to serialize processed technical indicators: {str(e)}")
    
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
        
        def safe_float(value: str) -> Optional[float]:
            if not value or value == "None":
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        def safe_int(value: str) -> Optional[int]:
            if not value or value == "None":
                return None
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        
        # Extract symbol
        symbol = global_quote.get("01. symbol", "UNKNOWN")
        
        # Transform data
        transformed_data = {
            "symbol": symbol,
            "open_price": safe_float(global_quote.get("02. open")),
            "high_price": safe_float(global_quote.get("03. high")),
            "low_price": safe_float(global_quote.get("04. low")),
            "current_price": safe_float(global_quote.get("05. price")) or 0.0,
            "volume": safe_int(global_quote.get("06. volume")),
            "latest_trading_day": global_quote.get("07. latest trading day"),
            "previous_close": safe_float(global_quote.get("08. previous close")),
            "change": safe_float(global_quote.get("09. change")),
            "change_percent": safe_float(global_quote.get("10. change percent")),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "producer_metadata": {
                "producer_timestamp": datetime.now(timezone.utc).isoformat(),
                "producer_version": "1.0.0",
                "data_source": "alpha_vantage"
            }
        }
        
        return transformed_data
    
    def _transform_intraday_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Alpha Vantage TIME_SERIES_INTRADAY data to Avro schema format.
        
        Args:
            data: Raw data from Alpha Vantage API
            
        Returns:
            Transformed data matching Avro schema
        """
        # Extract metadata and time series
        metadata = data.get("Meta Data", {})
        time_series = data.get("Time Series (5min)", {})
        
        # Get symbol and interval
        symbol = metadata.get("2. Symbol", "UNKNOWN")
        interval = metadata.get("4. Interval", "5min")
        
        # Get the most recent data point
        if not time_series:
            raise AvroSerializationError("No time series data found")
        
        # Get the latest timestamp
        latest_timestamp = max(time_series.keys())
        latest_data = time_series[latest_timestamp]
        
        def safe_float(value: str) -> float:
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0.0
        
        def safe_int(value: str) -> int:
            try:
                return int(value)
            except (ValueError, TypeError):
                return 0
        
        # Transform data
        transformed_data = {
            "symbol": symbol,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "open_price": safe_float(latest_data.get("1. open")),
            "high_price": safe_float(latest_data.get("2. high")),
            "low_price": safe_float(latest_data.get("3. low")),
            "close_price": safe_float(latest_data.get("4. close")),
            "volume": safe_int(latest_data.get("5. volume")),
            "interval": interval,
            "producer_metadata": {
                "producer_timestamp": datetime.now(timezone.utc).isoformat(),
                "producer_version": "1.0.0",
                "data_source": "alpha_vantage"
            }
        }
        
        return transformed_data
    
    def _transform_processed_data(self, data: Dict[str, Any], schema_type: str) -> Dict[str, Any]:
        """
        Transform processed data to Avro schema format.
        
        Args:
            data: Processed data
            schema_type: Type of schema to transform for
            
        Returns:
            Transformed data matching Avro schema
        """
        transformed_data = data.copy()
        if 'producer_timestamp' in transformed_data and transformed_data['producer_timestamp']:
            if isinstance(transformed_data['producer_timestamp'], str):
                dt = datetime.fromisoformat(transformed_data['producer_timestamp'].replace('Z', '+00:00'))
                transformed_data['producer_timestamp'] = int(dt.timestamp() * 1000)
        
        if 'processing_timestamp' in transformed_data and transformed_data['processing_timestamp']:
            if isinstance(transformed_data['processing_timestamp'], str):
                dt = datetime.fromisoformat(transformed_data['processing_timestamp'].replace('Z', '+00:00'))
                transformed_data['processing_timestamp'] = int(dt.timestamp() * 1000)
        
        return transformed_data

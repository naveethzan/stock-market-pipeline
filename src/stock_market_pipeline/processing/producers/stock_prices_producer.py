"""
Stock Prices Output Processor

This module handles the processing and output of stock prices data
for the processed-stock-prices topic schema.

Responsibilities:
- Schema alignment for processed-stock-prices
- Data preparation and transformation
- Kafka output with Avro serialization
- Error handling and validation
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, when, isnan, isnull, lit, current_timestamp, coalesce

from stock_market_pipeline.core.exceptions import ProcessingError
from stock_market_pipeline.utils import PipelineLogger
from stock_market_pipeline.storage.schemas import AvroSerializer
from stock_market_pipeline.config import config
from stock_market_pipeline.core.constants import ProcessingConstants


class StockPricesProducer:
    """
    Producer for stock prices output schema.
    
    Specialized producer for the processed-stock-prices topic that handles
    data preparation, schema alignment, validation, and Avro serialization
    for stock price data with comprehensive metrics and quality scoring.
    """
    
    def __init__(self, config: Any = None, schema_registry_url: str = None):
        """
        Initialize the stock prices producer.
        
        Args:
            config: Configuration object
            schema_registry_url: Schema Registry URL for Avro serialization
        """
        self.config = config or config.get_config()
        self.schema_registry_url = schema_registry_url or self.config.schema_registry_url
        self.logger = PipelineLogger(__name__)
        
        # Initialize Avro serializer
        self.avro_serializer = AvroSerializer(self.schema_registry_url)
        
        # Output topic configuration
        self.output_topic = self.config.kafka.processed_stock_prices_topic
        
        # Required fields for PROCESSED_STOCK_PRICES_SCHEMA
        self.required_fields = [
            "symbol", "timestamp", "current_price", "open_price", "high_price", "low_price",
            "volume", "sma_5min", "sma_20min", "price_trend_5min", "price_volatility",
            "trading_session", "producer_timestamp", "processing_timestamp", "vwap",
            "price_change_abs", "price_momentum", "data_quality_score"
        ]
        
        # Metrics tracking
        self.metrics = {
            "processed_records": 0,
            "error_count": 0,
            "last_processed_timestamp": None
        }
        
        self.logger.info("StockPricesProducer initialized")
    
    def prepare_data(self, df: DataFrame) -> DataFrame:
        """
        Prepare data for stock prices schema.
        
        Args:
            df: Input transformed DataFrame
            
        Returns:
            DataFrame prepared for stock prices schema
        """
        self.logger.info("Preparing data for stock prices schema")
        
        try:
            # Validate required fields exist
            self._validate_required_fields(df)
            
            # Map current_price to close_price (they're the same)
            df = df.withColumn("close_price", col("current_price"))
            
            # Use processing_timestamp as timestamp
            df = df.withColumn("timestamp", col("processing_timestamp"))
            
            # Add missing fields with defaults if needed
            df = self._add_missing_fields_with_defaults(df)
            
            # Select required fields for schema
            processed_df = df.select(*self.required_fields)
            
            self.logger.info("Data prepared successfully for stock prices schema")
            return processed_df
            
        except Exception as e:
            self.logger.error(f"Failed to prepare data: {str(e)}")
            raise ProcessingError(f"Failed to prepare data: {str(e)}") from e
    
    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validate data against stock prices schema requirements.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Validated DataFrame
        """
        self.logger.info("Validating data against stock prices schema")
        
        try:
            # Check required fields
            missing_fields = [field for field in self.required_fields if field not in df.columns]
            if missing_fields:
                raise ProcessingError(f"Missing required fields: {missing_fields}")
            
            # Validate data types and ranges
            df = self._validate_data_types(df)
            df = self._validate_data_ranges(df)
            
            # Handle null values
            df = self._handle_null_values(df)
            
            self.logger.info("Data validation completed successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            raise ProcessingError(f"Data validation failed: {str(e)}") from e
    
    def add_stock_prices_metrics(self, df: DataFrame) -> DataFrame:
        """
        Add stock prices specific metrics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with stock prices metrics
        """
        self.logger.info("Adding stock prices specific metrics")
        
        try:
            df = df.withColumn("vwap", coalesce(col("vwap"), col("close_price")))
            df = df.withColumn("price_change_abs", coalesce(col("price_change_abs"), lit(0.0)))
            df = df.withColumn("price_momentum", coalesce(col("price_momentum"), lit(0.0)))
            df = df.withColumn("data_quality_score", coalesce(col("data_quality_score"), lit(1.0)))
            
            self.logger.info("Stock prices metrics added successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to add stock prices metrics: {str(e)}")
            raise ProcessingError(f"Failed to add stock prices metrics: {str(e)}") from e
    
    def add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add metadata fields for the output schema.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata fields
        """
        self.logger.info("Adding metadata fields")
        
        try:
            return (df
                    .withColumn("data_layer", lit("silver"))
                    .withColumn("record_type", lit("stock_price"))
                    .withColumn("processing_version", lit("1.0")))
            
        except Exception as e:
            self.logger.error(f"Failed to add metadata: {str(e)}")
            raise ProcessingError(f"Failed to add metadata: {str(e)}") from e
    
    def serialize_data(self, df: DataFrame) -> DataFrame:
        """
        Serialize data using Avro schema.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with serialized data
        """
        self.logger.info("Serializing data with Avro schema")
        
        try:
            # Use AvroSerializer for serialization
            serialized_df = self.avro_serializer.serialize_dataframe(
                df, 
                schema_name="PROCESSED_STOCK_PRICES_SCHEMA",
                key_column="symbol"
            )
            
            self.logger.info("Data serialized successfully")
            return serialized_df
            
        except Exception as e:
            self.logger.error(f"Failed to serialize data: {str(e)}")
            raise ProcessingError(f"Failed to serialize data: {str(e)}") from e
    
    def write_to_kafka(self, df: DataFrame) -> None:
        """
        Write data to Kafka topic.
        
        Args:
            df: Input DataFrame with serialized data
        """
        self.logger.info(f"Writing data to Kafka topic: {self.output_topic}")
        
        try:
            # Write to Kafka topic
            df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
                .option("topic", self.output_topic) \
                .option("kafka.security.protocol", self.config.kafka.security_protocol) \
                .save()
            
            self.logger.info("Data written to Kafka successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to write to Kafka: {str(e)}")
            raise ProcessingError(f"Failed to write to Kafka: {str(e)}") from e
    
    def process_batch(self, df: DataFrame) -> DataFrame:
        """
        Process a batch of data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Processed DataFrame ready for Kafka output
        """
        self.logger.info("Processing batch of stock prices data")
        
        try:
            # Update metrics
            self.metrics["processed_records"] += df.count()
            self.metrics["last_processed_timestamp"] = current_timestamp()
            
            # Process data through pipeline
            processed_df = df
            processed_df = self.prepare_data(processed_df)
            processed_df = self.validate_data(processed_df)
            processed_df = self.add_stock_prices_metrics(processed_df)
            processed_df = self.add_metadata(processed_df)
            processed_df = self.serialize_data(processed_df)
            
            self.logger.info("Batch processing completed successfully")
            return processed_df
            
        except Exception as e:
            self.metrics["error_count"] += 1
            self.logger.error(f"Batch processing failed: {str(e)}")
            raise ProcessingError(f"Batch processing failed: {str(e)}") from e
    
    def get_schema_fields(self) -> List[str]:
        """
        Get required fields for stock prices schema.
        
        Returns:
            List of required field names
        """
        return self.required_fields.copy()
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get processor metrics.
        
        Returns:
            Metrics dictionary
        """
        return self.metrics.copy()
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get processor health status.
        
        Returns:
            Health status dictionary
        """
        error_rate = self.metrics["error_count"] / max(self.metrics["processed_records"], 1)
        
        return {
            "status": "healthy" if error_rate < 0.1 else "unhealthy",
            "processed_records": self.metrics["processed_records"],
            "error_count": self.metrics["error_count"],
            "error_rate": error_rate,
            "last_processed": self.metrics["last_processed_timestamp"]
        }
    
    def _validate_required_fields(self, df: DataFrame) -> None:
        """Validate that all required fields exist in the DataFrame."""
        missing_fields = [field for field in self.required_fields if field not in df.columns]
        if missing_fields:
            raise ProcessingError(f"Missing required fields: {missing_fields}")
    
    def _add_missing_fields_with_defaults(self, df: DataFrame) -> DataFrame:
        """Add missing fields with default values."""
        df = df.withColumn("open_price", coalesce(col("open_price"), col("close_price")))
        df = df.withColumn("high_price", coalesce(col("high_price"), col("close_price")))
        df = df.withColumn("low_price", coalesce(col("low_price"), col("close_price")))
        df = df.withColumn("volume", coalesce(col("volume"), lit(0)))
        df = df.withColumn("sma_5min", coalesce(col("sma_5min"), col("close_price")))
        df = df.withColumn("sma_20min", coalesce(col("sma_20min"), col("close_price")))
        df = df.withColumn("price_trend_5min", coalesce(col("price_trend_5min"), lit(ProcessingConstants.PRICE_TREND_DEFAULT)))
        df = df.withColumn("price_volatility", coalesce(col("price_volatility"), lit(0.0)))
        df = df.withColumn("trading_session", coalesce(col("trading_session"), lit(ProcessingConstants.TRADING_SESSION_DEFAULT)))
        df = df.withColumn("producer_timestamp", coalesce(col("producer_timestamp"), current_timestamp()))
        df = df.withColumn("processing_timestamp", coalesce(col("processing_timestamp"), current_timestamp()))
        
        df = df.withColumn("vwap", coalesce(col("vwap"), col("close_price")))
        df = df.withColumn("price_change_abs", coalesce(col("price_change_abs"), lit(0.0)))
        df = df.withColumn("price_momentum", coalesce(col("price_momentum"), lit(0.0)))
        df = df.withColumn("data_quality_score", coalesce(col("data_quality_score"), lit(1.0)))
        
        return df
    
    def _validate_data_types(self, df: DataFrame) -> DataFrame:
        """Validate data types match schema requirements."""
        return df
    
    def _validate_data_ranges(self, df: DataFrame) -> DataFrame:
        """Validate data ranges are reasonable."""
        # Validate price ranges using constants
        df = df.filter(F.col("current_price") > 0)
        df = df.filter(F.col("current_price") < ProcessingConstants.MARKET_CAP_LARGE)  # Reasonable price cap
        df = df.filter(F.col("volume") >= 0)
        df = df.filter(F.col("volume") < ProcessingConstants.VOLUME_HIGH)  # Reasonable volume cap
        
        return df
    
    def _handle_null_values(self, df: DataFrame) -> DataFrame:
        """Handle null values appropriately."""
        # Replace nulls with defaults for critical fields
        df = df.withColumn("symbol", coalesce(col("symbol"), lit("UNKNOWN")))
        df = df.withColumn("close_price", coalesce(col("close_price"), lit(0.0)))
        df = df.withColumn("volume", coalesce(col("volume"), lit(0)))
        return df

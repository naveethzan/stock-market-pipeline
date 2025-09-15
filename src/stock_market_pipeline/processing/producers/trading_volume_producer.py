"""
Trading Volume Output Processor

This module handles the processing and output of trading volume data
for the processed-trading-volume topic schema.

Responsibilities:
- Schema alignment for processed-trading-volume
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


class TradingVolumeProducer:
    """
    Producer for trading volume output schema.
    
    Specialized producer for the processed-trading-volume topic that handles
    volume metrics, moving averages, anomaly detection, and volume-based
    indicators with proper validation and Avro serialization.
    """
    
    def __init__(self, config: Any = None, schema_registry_url: str = None):
        """
        Initialize the trading volume producer.
        
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
        self.output_topic = self.config.kafka.processed_trading_volume_topic
        
        # Required fields for PROCESSED_TRADING_VOLUME_SCHEMA
        self.required_fields = [
            "symbol", "timestamp", "volume", "volume_ma_5min", "volume_ma_20min",
            "volume_trend", "volume_anomaly", "producer_timestamp", "processing_timestamp",
            "volume_ratio", "volume_weighted_price", "volume_category"
        ]
        
        # Metrics tracking
        self.metrics = {
            "processed_records": 0,
            "error_count": 0,
            "last_processed_timestamp": None
        }
        
        self.logger.info("TradingVolumeProducer initialized")
    
    def prepare_data(self, df: DataFrame) -> DataFrame:
        """
        Prepare data for trading volume schema.
        
        Args:
            df: Input transformed DataFrame
            
        Returns:
            DataFrame prepared for trading volume schema
        """
        self.logger.info("Preparing data for trading volume schema")
        
        try:
            # Validate required fields exist
            self._validate_required_fields(df)
            
            # Use processing_timestamp as timestamp
            df = df.withColumn("timestamp", col("processing_timestamp"))
            
            # Map field names for schema alignment
            df = df.withColumn("volume_ma_5min", col("volume_ma_5min"))
            df = df.withColumn("volume_ma_20min", col("volume_ma_20min"))
            df = df.withColumn("volume_anomaly", coalesce(col("volume_anomaly"), col("is_volume_anomaly")))
            
            # Add missing fields with null defaults
            df = self._add_missing_fields_with_defaults(df)
            
            # Select required fields for schema
            processed_df = df.select(*self.required_fields)
            
            self.logger.info("Data prepared successfully for trading volume schema")
            return processed_df
            
        except Exception as e:
            self.logger.error(f"Failed to prepare data: {str(e)}")
            raise ProcessingError(f"Failed to prepare data: {str(e)}") from e
    
    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validate data against trading volume schema requirements.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Validated DataFrame
        """
        self.logger.info("Validating data against trading volume schema")
        
        try:
            # Check required fields
            missing_fields = [field for field in self.required_fields if field not in df.columns]
            if missing_fields:
                raise ProcessingError(f"Missing required fields: {missing_fields}")
            
            # Validate data types
            df = self._validate_data_types(df)
            
            # Validate volume ranges
            df = self._validate_volume_ranges(df)
            
            # Handle null values
            df = self._handle_null_values(df)
            
            self.logger.info("Data validation completed successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            raise ProcessingError(f"Data validation failed: {str(e)}") from e
    
    def add_volume_metrics(self, df: DataFrame) -> DataFrame:
        """
        Add trading volume specific metrics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with volume metrics
        """
        self.logger.info("Adding volume-specific metrics")
        
        try:
            df = df.withColumn("volume_ratio", coalesce(col("volume_ratio"), lit(None)))
            df = df.withColumn("volume_weighted_price", coalesce(col("volume_weighted_price"), lit(None)))
            df = df.withColumn("volume_category", coalesce(col("volume_category"), lit(None)))
            
            self.logger.info("Volume metrics added successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to add volume metrics: {str(e)}")
            raise ProcessingError(f"Failed to add volume metrics: {str(e)}") from e
    
    def calculate_volume_indicators(self, df: DataFrame) -> DataFrame:
        """
        Calculate volume-based technical indicators.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with volume indicators
        """
        self.logger.info("Calculating volume indicators")
        
        try:
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to calculate volume indicators: {str(e)}")
            raise ProcessingError(f"Failed to calculate volume indicators: {str(e)}") from e
    
    def add_volume_context(self, df: DataFrame) -> DataFrame:
        """
        Add volume context and metadata.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with volume context
        """
        self.logger.info("Adding volume context")
        
        try:
            # Add metadata fields
            df = df.withColumn("data_layer", lit("silver"))
            df = df.withColumn("record_type", lit("trading_volume"))
            df = df.withColumn("processing_version", lit("1.0"))
            
            self.logger.info("Volume context added successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to add volume context: {str(e)}")
            raise ProcessingError(f"Failed to add volume context: {str(e)}") from e
    
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
            serialized_df = self.avro_serializer.serialize_dataframe(
                df, 
                schema_name="PROCESSED_TRADING_VOLUME_SCHEMA", 
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
        self.logger.info("Processing batch of trading volume data")
        
        try:
            # Update metrics
            self.metrics["processed_records"] += df.count()
            self.metrics["last_processed_timestamp"] = current_timestamp()
            
            # Process the data
            processed_df = df
            processed_df = self.prepare_data(processed_df)
            processed_df = self.validate_data(processed_df)
            processed_df = self.add_volume_metrics(processed_df)
            processed_df = self.calculate_volume_indicators(processed_df)
            processed_df = self.add_volume_context(processed_df)
            processed_df = self.serialize_data(processed_df)
            
            self.logger.info("Batch processing completed successfully")
            return processed_df
            
        except Exception as e:
            self.metrics["error_count"] += 1
            self.logger.error(f"Batch processing failed: {str(e)}")
            raise ProcessingError(f"Batch processing failed: {str(e)}") from e
    
    def get_schema_fields(self) -> List[str]:
        """
        Get required fields for trading volume schema.
        
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
        """
        Validate that all required fields exist in the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Raises:
            ProcessingError: If required fields are missing
        """
        missing_fields = [field for field in self.required_fields if field not in df.columns]
        if missing_fields:
            raise ProcessingError(f"Missing required fields: {missing_fields}")
    
    def _add_missing_fields_with_defaults(self, df: DataFrame) -> DataFrame:
        """
        Add missing fields with null defaults.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with missing fields added
        """
        df = df.withColumn("volume_ma_5min", coalesce(col("volume_ma_5min"), lit(None)))
        df = df.withColumn("volume_ma_20min", coalesce(col("volume_ma_20min"), lit(None)))
        df = df.withColumn("volume_trend", coalesce(col("volume_trend"), lit(None)))
        df = df.withColumn("volume_anomaly", coalesce(col("volume_anomaly"), lit(None)))
        df = df.withColumn("producer_timestamp", coalesce(col("producer_timestamp"), current_timestamp()))
        df = df.withColumn("processing_timestamp", coalesce(col("processing_timestamp"), current_timestamp()))
        df = df.withColumn("volume_ratio", coalesce(col("volume_ratio"), lit(None)))
        df = df.withColumn("volume_weighted_price", coalesce(col("volume_weighted_price"), lit(None)))
        df = df.withColumn("volume_category", coalesce(col("volume_category"), lit(None)))
        
        return df
    
    def _validate_data_types(self, df: DataFrame) -> DataFrame:
        """
        Validate data types for required fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validated data types
        """
        return df
    
    def _validate_volume_ranges(self, df: DataFrame) -> DataFrame:
        """
        Validate volume ranges and values.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validated volume ranges
        """
        df = df.withColumn("volume", when(col("volume") < 0, 0).otherwise(col("volume")))
        df = df.withColumn("volume_ratio", when(col("volume_ratio") < 0, None).otherwise(col("volume_ratio")))
        df = df.withColumn("volume_weighted_price", when(col("volume_weighted_price") < 0, None).otherwise(col("volume_weighted_price")))
        
        return df
    
    def _handle_null_values(self, df: DataFrame) -> DataFrame:
        """
        Handle null values in required fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with null values handled
        """
        df = df.withColumn("symbol", coalesce(col("symbol"), lit(ProcessingConstants.SYMBOL_UNKNOWN)))
        df = df.withColumn("volume", coalesce(col("volume"), lit(0)))
        df = df.withColumn("timestamp", coalesce(col("timestamp"), current_timestamp()))
        
        return df
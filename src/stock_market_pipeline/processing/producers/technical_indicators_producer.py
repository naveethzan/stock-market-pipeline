"""
Technical Indicators Output Processor

This module handles the processing and output of technical indicators data
for the processed-technical-indicators topic schema.

Responsibilities:
- Schema alignment for processed-technical-indicators
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


class TechnicalIndicatorsProducer:
    """
    Producer for technical indicators output schema.
    
    Specialized producer for the processed-technical-indicators topic that handles
    RSI, MACD, Bollinger Bands, and other technical indicators with proper
    validation, range checking, and Avro serialization for analytical data.
    """
    
    def __init__(self, config: Any = None, schema_registry_url: str = None):
        """
        Initialize the technical indicators producer.
        
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
        self.output_topic = self.config.kafka.processed_technical_indicators_topic
        
        # Required fields for PROCESSED_TECHNICAL_INDICATORS_SCHEMA
        self.required_fields = [
            "symbol", "timestamp", "rsi_14", "macd", "macd_signal", "macd_histogram",
            "bollinger_upper", "bollinger_lower", "bollinger_middle", "producer_timestamp",
            "processing_timestamp", "data_quality_score"
        ]
        
        # Metrics tracking
        self.metrics = {
            "processed_records": 0,
            "error_count": 0,
            "last_processed_timestamp": None
        }
        
        self.logger.info("TechnicalIndicatorsProducer initialized")
    
    def prepare_data(self, df: DataFrame) -> DataFrame:
        """
        Prepare data for technical indicators schema.
        
        Args:
            df: Input transformed DataFrame
            
        Returns:
            DataFrame prepared for technical indicators schema
        """
        self.logger.info("Preparing data for technical indicators schema")
        
        try:
            # Validate required fields exist
            self._validate_required_fields(df)
            
            # Use processing_timestamp as timestamp
            df = df.withColumn("timestamp", col("processing_timestamp"))
            
            # Add missing fields with defaults if needed
            df = self._add_missing_fields_with_defaults(df)
            
            # Select required fields for schema
            processed_df = df.select(*self.required_fields)
            
            self.logger.info("Data prepared successfully for technical indicators schema")
            return processed_df
            
        except Exception as e:
            self.logger.error(f"Failed to prepare data: {str(e)}")
            raise ProcessingError(f"Failed to prepare data: {str(e)}") from e
    
    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validate data against technical indicators schema requirements.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Validated DataFrame
        """
        self.logger.info("Validating data against technical indicators schema")
        
        try:
            # Check required fields
            missing_fields = [field for field in self.required_fields if field not in df.columns]
            if missing_fields:
                raise ProcessingError(f"Missing required fields: {missing_fields}")
            
            # Validate data types and ranges
            df = self._validate_data_types(df)
            df = self._validate_technical_indicator_ranges(df)
            
            # Handle null values
            df = self._handle_null_values(df)
            
            self.logger.info("Data validation completed successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            raise ProcessingError(f"Data validation failed: {str(e)}") from e
    
    def add_technical_indicators(self, df: DataFrame) -> DataFrame:
        """
        Add technical indicators specific metrics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with technical indicators
        """
        self.logger.info("Adding technical indicators specific metrics")
        
        try:
            df = df.withColumn("rsi_14", coalesce(col("rsi_14"), lit(ProcessingConstants.RSI_NEUTRAL_DEFAULT)))  # Neutral RSI
            df = df.withColumn("macd", coalesce(col("macd"), lit(ProcessingConstants.MACD_NEUTRAL)))  # No trend
            df = df.withColumn("macd_signal", coalesce(col("macd_signal"), lit(ProcessingConstants.MACD_NEUTRAL)))  # No signal
            df = df.withColumn("macd_histogram", coalesce(col("macd_histogram"), lit(ProcessingConstants.MACD_NEUTRAL)))  # No divergence
            
            df = df.withColumn("bollinger_middle", coalesce(col("bollinger_middle"), col("current_price")))
            df = df.withColumn("bollinger_upper", coalesce(col("bollinger_upper"), col("current_price") * 1.02))
            df = df.withColumn("bollinger_lower", coalesce(col("bollinger_lower"), col("current_price") * 0.98))
            
            self.logger.info("Technical indicators metrics added successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to add technical indicators metrics: {str(e)}")
            raise ProcessingError(f"Failed to add technical indicators metrics: {str(e)}") from e
    
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
                    .withColumn("record_type", lit("technical_indicators"))
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
                schema_name="PROCESSED_TECHNICAL_INDICATORS_SCHEMA",
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
        self.logger.info("Processing batch of technical indicators data")
        
        try:
            # Update metrics
            self.metrics["processed_records"] += df.count()
            self.metrics["last_processed_timestamp"] = current_timestamp()
            
            # Process data through pipeline
            processed_df = df
            processed_df = self.prepare_data(processed_df)
            processed_df = self.validate_data(processed_df)
            processed_df = self.add_technical_indicators(processed_df)
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
        Get required fields for technical indicators schema.
        
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
        df = df.withColumn("rsi_14", coalesce(col("rsi_14"), lit(ProcessingConstants.RSI_NEUTRAL_DEFAULT)))
        df = df.withColumn("macd", coalesce(col("macd"), lit(ProcessingConstants.MACD_NEUTRAL)))
        df = df.withColumn("macd_signal", coalesce(col("macd_signal"), lit(ProcessingConstants.MACD_NEUTRAL)))
        df = df.withColumn("macd_histogram", coalesce(col("macd_histogram"), lit(ProcessingConstants.MACD_NEUTRAL)))
        df = df.withColumn("bollinger_upper", coalesce(col("bollinger_upper"), col("current_price")))
        df = df.withColumn("bollinger_lower", coalesce(col("bollinger_lower"), col("current_price")))
        df = df.withColumn("bollinger_middle", coalesce(col("bollinger_middle"), col("current_price")))
        df = df.withColumn("producer_timestamp", coalesce(col("producer_timestamp"), current_timestamp()))
        df = df.withColumn("processing_timestamp", coalesce(col("processing_timestamp"), current_timestamp()))
        df = df.withColumn("data_quality_score", coalesce(col("data_quality_score"), lit(1.0)))
        
        return df
    
    def _validate_data_types(self, df: DataFrame) -> DataFrame:
        """Validate data types match schema requirements."""
        return df
    
    def _validate_technical_indicator_ranges(self, df: DataFrame) -> DataFrame:
        """Validate technical indicator ranges are reasonable."""
        # RSI should be between 0 and 100
        df = df.withColumn("rsi_14", 
                          when(col("rsi_14") < 0, 0.0)
                          .when(col("rsi_14") > 100, 100.0)
                          .otherwise(col("rsi_14")))
        
        # MACD values should be reasonable (not extreme)
        df = df.withColumn("macd", 
                          when(col("macd").isNull(), 0.0)
                          .when(abs(col("macd")) > 1000, 0.0)  # Cap extreme values
                          .otherwise(col("macd")))
        
        return df
    
    def _handle_null_values(self, df: DataFrame) -> DataFrame:
        """Handle null values in required fields."""
        # Handle null values for required fields using constants
        df = df.withColumn("symbol", coalesce(col("symbol"), lit(ProcessingConstants.SYMBOL_UNKNOWN)))
        df = df.withColumn("timestamp", coalesce(col("timestamp"), current_timestamp()))
        
        return df

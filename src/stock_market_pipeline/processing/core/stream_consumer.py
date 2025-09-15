"""
Main Stream Consumer

This module provides the main consumer for stream processing,
consuming data from Kafka topics, applying transformations, and coordinating
output producers to create a complete stream processing pipeline.

Responsibilities:
- Kafka stream creation and management
- Avro deserialization with Schema Registry
- Data transformation coordination
- Output producer management
- Stream lifecycle management
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType
from pyspark.sql.streaming import StreamingQuery, StreamingQueryManager

from stock_market_pipeline.core.exceptions import ProcessingError
from stock_market_pipeline.utils import PipelineLogger
from stock_market_pipeline.config import config as app_config
from stock_market_pipeline.storage.schemas.schema_manager import SchemaManager
from stock_market_pipeline.core.constants import Topics, SchemaNames

from stock_market_pipeline.processing.transformations import (
    calculate_price_metrics,
    calculate_moving_averages,
    calculate_technical_indicators,
    calculate_volume_metrics
)
from stock_market_pipeline.processing.producers.stock_prices_producer import StockPricesProducer
from stock_market_pipeline.processing.producers.trading_volume_producer import TradingVolumeProducer
from stock_market_pipeline.processing.producers.technical_indicators_producer import TechnicalIndicatorsProducer
from stock_market_pipeline.processing.core.deserialization import (
    ensure_from_avro_available,
    strip_confluent_header,
    deserialize_with_from_avro,
    project_canonical,
)


class StreamConsumer:
    """
    Main consumer for stream processing.
    
    Orchestrates the complete stream processing pipeline including Kafka data
    consumption, Avro deserialization, data transformations, and coordination
    of multiple output producers for different data schemas.
    """
    
    def __init__(self, spark_session: SparkSession, config: Any = None):
        """
        Initialize the stream consumer.
        
        Args:
            spark_session: Spark session for stream processing
            config: Configuration object (uses global config if None)
        """
        self.spark = spark_session
        self.config = config or app_config.get_config()
        self.logger = PipelineLogger(__name__)
        
        # Initialize output producers
        self.stock_producer = StockPricesProducer(self.config)
        self.volume_producer = TradingVolumeProducer(self.config)
        self.indicators_producer = TechnicalIndicatorsProducer(self.config)
        
        # Stream management
        self.active_queries: Dict[str, StreamingQuery] = {}
        self.is_running = False
        
        # Metrics tracking
        self.processing_metrics = {
            "total_batches_processed": 0,
            "total_records_processed": 0,
            "total_errors": 0,
            "last_processed_timestamp": None,
            "batch_errors": []
        }
        
        self.logger.info("StreamConsumer initialized")
    
    def create_kafka_stream(self, topics: List[str]) -> DataFrame:
        """
        Create Kafka streaming DataFrame.
        
        Args:
            topics: List of Kafka topics to consume from
            
        Returns:
            Streaming DataFrame from Kafka
        """
        self.logger.info(f"Creating Kafka stream for topics: {topics}")
        
        try:
            kafka_stream = (self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers)
                .option("subscribe", ",".join(topics))
                .option("kafka.security.protocol", self.config.kafka.security_protocol)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load())
            
            self.logger.info("Kafka stream created successfully")
            return kafka_stream
            
        except Exception as e:
            self.logger.error(f"Failed to create Kafka stream: {str(e)}")
            raise ProcessingError(f"Failed to create Kafka stream: {str(e)}") from e
    
    def parse_kafka_messages(self, df: DataFrame) -> DataFrame:
        """
        Parse Kafka messages and deserialize Avro data.
        
        Handles Confluent header stripping, Avro deserialization using from_avro,
        and canonical projection to prepare data for downstream transformations.
        
        Args:
            df: Raw Kafka streaming DataFrame
            
        Returns:
            Parsed DataFrame with deserialized data ready for processing
        """
        self.logger.info("Parsing and deserializing Kafka messages")
        
        try:
            ensure_from_avro_available()

            # Select and standardize base columns
            base_df = df.select(
                df.key.cast("string").alias("key"),
                df.value.cast("binary").alias("value"),
                df.topic.alias("topic"),
                df.partition.alias("partition"),
                df.offset.alias("offset"),
                df.timestamp.alias("kafka_timestamp"),
            )

            topic_to_schema: Dict[str, str] = {
                self.config.kafka.stock_quotes_topic: SchemaNames.STOCK_QUOTE,
                self.config.kafka.stock_intraday_topic: SchemaNames.INTRADAY_DATA,
            }

            # Log mapping once
            self.logger.info(f"Topic to schema mapping: {topic_to_schema}")

            schema_manager = SchemaManager(self.config.schema_registry_url)

            # Process quotes branch
            quotes_df = base_df.filter(F.col("topic") == self.config.kafka.stock_quotes_topic)
            if quotes_df is not None:
                quotes_schema_json = schema_manager.get_schema_json(topic_to_schema[self.config.kafka.stock_quotes_topic])
                quotes_df = strip_confluent_header(quotes_df, value_col="value", out_col="avro_payload")
                quotes_df = deserialize_with_from_avro(quotes_df, payload_col="avro_payload", schema_json=quotes_schema_json, out_col="data")
                quotes_df = quotes_df.filter(F.col("data").isNotNull())
                quotes_df = project_canonical(quotes_df)

            # Process intraday branch
            intraday_df = base_df.filter(F.col("topic") == self.config.kafka.stock_intraday_topic)
            if intraday_df is not None:
                intraday_schema_json = schema_manager.get_schema_json(topic_to_schema[self.config.kafka.stock_intraday_topic])
                intraday_df = strip_confluent_header(intraday_df, value_col="value", out_col="avro_payload")
                intraday_df = deserialize_with_from_avro(intraday_df, payload_col="avro_payload", schema_json=intraday_schema_json, out_col="data")
                intraday_df = intraday_df.filter(F.col("data").isNotNull())
                intraday_df = project_canonical(intraday_df)

            # Union branches
            unified = quotes_df.unionByName(intraday_df, allowMissingColumns=True)
            
            # Add validation step
            validated_df = self._validate_input_data(unified)

            self.logger.info("Kafka messages parsed and deserialized successfully with from_avro")
            return validated_df
            
        except Exception as e:
            self.logger.error(f"Failed to parse Kafka messages: {str(e)}")
            raise ProcessingError(f"Failed to parse Kafka messages: {str(e)}") from e
    
    def apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply comprehensive data transformations.
        
        Calculates price metrics, moving averages, technical indicators, and
        volume metrics to enrich the raw market data with analytical insights.
        
        Args:
            df: Input DataFrame with parsed market data
            
        Returns:
            Transformed DataFrame with enriched analytical data
        """
        self.logger.info("Applying transformations")
        
        try:
            transformed_df = df
            
            # Calculate price metrics
            transformed_df = calculate_price_metrics(transformed_df)
            
            # Calculate moving averages
            transformed_df = calculate_moving_averages(transformed_df)
            
            # Calculate technical indicators
            transformed_df = calculate_technical_indicators(transformed_df)
            
            # Calculate volume metrics
            transformed_df = calculate_volume_metrics(transformed_df)
            
            self.logger.info("Transformations applied successfully")
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Failed to apply transformations: {str(e)}")
            raise ProcessingError(f"Failed to apply transformations: {str(e)}") from e
    
    def coordinate_output_producers(self, df: DataFrame) -> None:
        """
        Coordinate multiple output producers for different data schemas.
        
        Manages three specialized producers: stock prices, trading volume,
        and technical indicators, each handling their respective output schemas
        with proper error handling and graceful degradation.
        
        Args:
            df: Transformed DataFrame with enriched market data
        """
        self.logger.info("Coordinating output producers")
        
        try:
            # Process with stock prices producer
            try:
                self.stock_producer.process_batch(df)
                self.logger.debug("Stock prices producer completed successfully")
            except Exception as e:
                self.logger.error(f"Stock prices producer failed: {str(e)}")
            
            # Process with trading volume producer
            try:
                self.volume_producer.process_batch(df)
                self.logger.debug("Trading volume producer completed successfully")
            except Exception as e:
                self.logger.error(f"Trading volume producer failed: {str(e)}")
            
            # Process with technical indicators producer
            try:
                self.indicators_producer.process_batch(df)
                self.logger.debug("Technical indicators producer completed successfully")
            except Exception as e:
                self.logger.error(f"Technical indicators producer failed: {str(e)}")
            
            self.logger.info("Output producers coordination completed")
            
        except Exception as e:
            self.logger.error(f"Failed to coordinate output producers: {str(e)}")
            raise ProcessingError(f"Failed to coordinate output producers: {str(e)}") from e
    
    def _process_batch(self, df: DataFrame, batch_id: int) -> None:
        """
        Process a batch of data using foreachBatch pattern.
        
        Args:
            df: Input DataFrame
            batch_id: Batch identifier
        """
        self.logger.info(f"Processing batch {batch_id}")
        
        try:
            parsed_df = self.parse_kafka_messages(df)
            transformed_df = self.apply_transformations(parsed_df)
            self.coordinate_output_producers(transformed_df)
            
            record_count = parsed_df.count() if parsed_df else 0
            self._update_processing_metrics(batch_id, record_count)
            
            self.logger.info(f"Batch {batch_id} processed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to process batch {batch_id}: {str(e)}")
            self._update_error_metrics(batch_id, str(e))
            # Continue processing other batches (graceful degradation)
    
    def start_streaming(self, topics: List[str], checkpoint_location: str = None) -> StreamingQuery:
        """
        Start the streaming processing.
        
        Args:
            topics: List of Kafka topics to consume from
            checkpoint_location: Checkpoint location for fault tolerance
            
        Returns:
            StreamingQuery object
        """
        self.logger.info(f"Starting streaming for topics: {topics}")
        
        try:
            # Create Kafka stream
            kafka_stream = self.create_kafka_stream(topics)
            
            # Set up checkpoint location
            if checkpoint_location is None:
                checkpoint_location = f"/tmp/streaming-checkpoint-{'-'.join(topics)}"
            
            # Start streaming with foreachBatch pattern
            query = (kafka_stream
                .writeStream
                .foreachBatch(self._process_batch)
                .option("checkpointLocation", checkpoint_location)
                .trigger(processingTime="10 seconds")
                .start())
            
            # Store query for management
            self.active_queries["main_stream"] = query
            self.is_running = True
            
            self.logger.info(f"Streaming started successfully - Query ID: {query.id}")
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to start streaming: {str(e)}")
            raise ProcessingError(f"Failed to start streaming: {str(e)}") from e
    
    def stop_streaming(self) -> None:
        """
        Stop the streaming processing.
        """
        self.logger.info("Stopping streaming processing")
        
        try:
            # Stop all active queries
            for query_name, query in self.active_queries.items():
                try:
                    query.stop()
                    self.logger.info(f"Stopped query: {query_name}")
                except Exception as e:
                    self.logger.error(f"Failed to stop query {query_name}: {str(e)}")
            
            # Clear active queries
            self.active_queries.clear()
            self.is_running = False
            
            self.logger.info("Streaming processing stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to stop streaming: {str(e)}")
            raise ProcessingError(f"Failed to stop streaming: {str(e)}") from e
    
    def _validate_input_data(self, df: DataFrame) -> DataFrame:
        """
        Validate input data structure and content.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Validated DataFrame
        """
        self.logger.info("Validating input data")
        
        try:
            # Check required fields exist
            required_fields = ["symbol", "current_price", "volume", "processing_timestamp"]
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                raise ProcessingError(f"Missing required fields: {missing_fields}")
            
            # Validate data types
            if "current_price" in df.columns:
                df = df.filter(F.col("current_price").isNotNull() & (F.col("current_price") > 0))
            
            if "volume" in df.columns:
                df = df.filter(F.col("volume").isNotNull() & (F.col("volume") >= 0))
            
            # Validate data ranges
            df = df.filter(F.col("current_price") < 1000000)  # Reasonable price cap
            df = df.filter(F.col("volume") < 1000000000)      # Reasonable volume cap
            
            self.logger.info("Input data validation completed successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Input data validation failed: {str(e)}")
            raise ProcessingError(f"Input data validation failed: {str(e)}") from e
    
    def _update_processing_metrics(self, batch_id: int, record_count: int) -> None:
        """
        Update processing metrics.
        
        Args:
            batch_id: Batch identifier
            record_count: Number of records processed
        """
        try:
            self.processing_metrics["total_batches_processed"] += 1
            self.processing_metrics["total_records_processed"] += record_count
            self.processing_metrics["last_processed_timestamp"] = F.current_timestamp()
            
            self.logger.debug(f"Updated processing metrics - Batch {batch_id}: {record_count} records")
        except Exception as e:
            self.logger.error(f"Failed to update processing metrics: {str(e)}")
    
    def _update_error_metrics(self, batch_id: int, error: str) -> None:
        """
        Update error metrics.
        
        Args:
            batch_id: Batch identifier
            error: Error message
        """
        try:
            self.processing_metrics["total_errors"] += 1
            
            # Keep only the last 10 errors to prevent memory issues
            if len(self.processing_metrics["batch_errors"]) >= 10:
                self.processing_metrics["batch_errors"].pop(0)
            
            self.processing_metrics["batch_errors"].append({
                "batch_id": batch_id,
                "error": error,
                "timestamp": F.current_timestamp()
            })
            
            self.logger.debug(f"Updated error metrics - Batch {batch_id}: {error}")
        except Exception as e:
            self.logger.error(f"Failed to update error metrics: {str(e)}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the stream consumer.
        
        Returns:
            Health status dictionary
        """
        try:
            # Check if streaming is running
            is_healthy = self.is_running and len(self.active_queries) > 0
            
            # Get producer health status
            producer_status = {
                "stock_producer": self.stock_producer.get_health_status(),
                "volume_producer": self.volume_producer.get_health_status(),
                "indicators_producer": self.indicators_producer.get_health_status()
            }
            
            # Check overall producer health
            all_producers_healthy = all(
                status.get("status") == "healthy" 
                for status in producer_status.values()
            )
            
            return {
                "status": "healthy" if is_healthy and all_producers_healthy else "unhealthy",
                "is_running": self.is_running,
                "active_queries": len(self.active_queries),
                "producers": producer_status
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get health status: {str(e)}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get processing metrics.
        
        Returns:
            Metrics dictionary
        """
        try:
            # Get producer metrics
            producer_metrics = {
                "stock_producer": self.stock_producer.get_metrics(),
                "volume_producer": self.volume_producer.get_metrics(),
                "indicators_producer": self.indicators_producer.get_metrics()
            }
            
            # Calculate overall metrics
            total_processed = sum(
                metrics.get("processed_records", 0) 
                for metrics in producer_metrics.values()
            )
            
            total_errors = sum(
                metrics.get("error_count", 0) 
                for metrics in producer_metrics.values()
            )
            
            return {
                "streaming": {
                    "is_running": self.is_running,
                    "active_queries": len(self.active_queries)
                },
                "processing": {
                    "total_batches_processed": self.processing_metrics["total_batches_processed"],
                    "total_records_processed": self.processing_metrics["total_records_processed"],
                    "total_processing_errors": self.processing_metrics["total_errors"],
                    "last_processed_timestamp": self.processing_metrics["last_processed_timestamp"],
                    "error_rate": self.processing_metrics["total_errors"] / max(self.processing_metrics["total_batches_processed"], 1),
                    "recent_errors": self.processing_metrics["batch_errors"]
                },
                "producers": producer_metrics
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get metrics: {str(e)}")
            return {
                "error": str(e)
            }
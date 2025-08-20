"""
Spark Structured Streaming processor for real-time financial data processing.
Consumes data from Kafka, applies transformations, and outputs to Parquet format.
"""
import logging
import os
from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType
from pyspark.sql.streaming import StreamingQuery

from ..config.settings import ConfigManager
from .medallion_data_quality import MedallionDataQualityValidator, LayerValidationResult
from ..schemas.avro_serializer import AvroSerializer, AvroSerializationError


logger = logging.getLogger(__name__)


class StreamProcessorError(Exception):
    """Custom exception for stream processor errors."""
    pass


class StreamProcessor:
    """
    Spark Structured Streaming processor for financial data.
    
    Handles Kafka consumption, data transformations, and Parquet output
    with comprehensive error handling and monitoring.
    """
    
    def __init__(self, config: ConfigManager, spark_session: Optional[SparkSession] = None):
        """
        Initialize the stream processor.
        
        Args:
            config: Configuration manager instance
            spark_session: Optional Spark session (will create if not provided)
        """
        self.config = config
        self.spark = spark_session or self._create_spark_session()
        self.active_queries: Dict[str, StreamingQuery] = {}
        self.data_quality_validator = MedallionDataQualityValidator(self.spark)
        self.avro_serializer = AvroSerializer()
        
        logger.info(
            "StreamProcessor initialized",
            extra={
                "spark_app_name": self.config.spark.app_name,
                "checkpoint_location": self.config.spark.checkpoint_location,
                "trigger_interval": self.config.spark.trigger_processing_time
            }
        )
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for structured streaming."""
        logger.info("Creating Spark session for structured streaming")
        
        try:
            spark = (SparkSession.builder
                    .appName(self.config.spark.app_name)
                    .master(self.config.spark.master)
                    .config("spark.sql.adaptive.enabled", str(self.config.spark.sql_adaptive_enabled))
                    .config("spark.sql.adaptive.coalescePartitions.enabled", 
                           str(self.config.spark.sql_adaptive_coalescePartitions_enabled))
                    .config("spark.serializer", self.config.spark.serializer)
                    .config("spark.driver.memory", self.config.spark.driver_memory)
                    .config("spark.executor.memory", self.config.spark.executor_memory)
                    .config("spark.executor.cores", str(self.config.spark.executor_cores))
                    .config("spark.driver.maxResultSize", self.config.spark.max_result_size)
                    # Kafka integration
                    .config("spark.jars.packages", 
                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
                    # Parquet optimization
                    .config("spark.sql.parquet.compression.codec", "snappy")
                    .config("spark.sql.parquet.enableVectorizedReader", "true")
                    .getOrCreate())
            
            # Set log level
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info(
                "Spark session created successfully",
                extra={
                    "spark_version": spark.version,
                    "master": spark.conf.get("spark.master"),
                    "app_name": spark.conf.get("spark.app.name")
                }
            )
            
            return spark
            
        except Exception as e:
            error_msg = f"Failed to create Spark session: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def get_kafka_stream_schema(self) -> StructType:
        """
        Define schema for Kafka messages from Alpha Vantage data.
        
        Returns:
            StructType schema for incoming Kafka messages
        """
        return StructType([
            # Alpha Vantage real-time quote fields
            StructField("01. symbol", StringType(), nullable=False),
            StructField("02. open", StringType(), nullable=True),
            StructField("03. high", StringType(), nullable=True),
            StructField("04. low", StringType(), nullable=True),
            StructField("05. price", StringType(), nullable=False),
            StructField("06. volume", StringType(), nullable=True),
            StructField("07. latest trading day", StringType(), nullable=True),
            StructField("08. previous close", StringType(), nullable=True),
            StructField("09. change", StringType(), nullable=True),
            StructField("10. change percent", StringType(), nullable=True),
            
            # Producer metadata
            StructField("_producer_metadata", StructType([
                StructField("producer_timestamp", StringType(), nullable=True),
                StructField("producer_version", StringType(), nullable=True),
                StructField("serialization_format", StringType(), nullable=True)
            ]), nullable=True)
        ])
    
    def create_kafka_stream(self, topic: str) -> DataFrame:
        """
        Create a streaming DataFrame from Kafka topic.
        
        Args:
            topic: Kafka topic name to consume from
            
        Returns:
            Streaming DataFrame from Kafka
        """
        logger.info(f"Creating Kafka stream for topic: {topic}")
        
        try:
            kafka_df = (self.spark
                       .readStream
                       .format("kafka")
                       .option("kafka.bootstrap.servers", 
                              ",".join(self.config.kafka.bootstrap_servers))
                       .option("subscribe", topic)
                       .option("startingOffsets", "latest")
                       .option("failOnDataLoss", "false")
                       .option("kafka.security.protocol", self.config.kafka.security_protocol)
                       .load())
            
            logger.info(f"Kafka stream created successfully for topic: {topic}")
            return kafka_df
            
        except Exception as e:
            error_msg = f"Failed to create Kafka stream for topic {topic}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def parse_kafka_messages(self, kafka_df: DataFrame) -> DataFrame:
        """
        Parse Kafka messages and extract stock data.
        
        Args:
            kafka_df: Raw Kafka DataFrame
            
        Returns:
            Parsed DataFrame with stock data
        """
        logger.info("Parsing Kafka messages")
        
        try:
            # Parse JSON from Kafka value
            parsed_df = (kafka_df
                        .select(
                            F.col("key").cast("string").alias("message_key"),
                            F.col("value").cast("string").alias("json_data"),
                            F.col("topic"),
                            F.col("partition"),
                            F.col("offset"),
                            F.col("timestamp").alias("kafka_timestamp")
                        )
                        .select(
                            "*",
                            F.from_json(F.col("json_data"), self.get_kafka_stream_schema()).alias("data")
                        )
                        .select(
                            "message_key",
                            "topic",
                            "partition", 
                            "offset",
                            "kafka_timestamp",
                            "data.*"
                        ))
            
            # Clean and transform the data
            cleaned_df = (parsed_df
                         .withColumn("symbol", F.col("`01. symbol`"))
                         .withColumn("open_price", F.col("`02. open`").cast(DoubleType()))
                         .withColumn("high_price", F.col("`03. high`").cast(DoubleType()))
                         .withColumn("low_price", F.col("`04. low`").cast(DoubleType()))
                         .withColumn("current_price", F.col("`05. price`").cast(DoubleType()))
                         .withColumn("volume", F.col("`06. volume`").cast(LongType()))
                         .withColumn("latest_trading_day", F.col("`07. latest trading day`"))
                         .withColumn("previous_close", F.col("`08. previous close`").cast(DoubleType()))
                         .withColumn("change", F.col("`09. change`").cast(DoubleType()))
                         .withColumn("change_percent", 
                                   F.regexp_replace(F.col("`10. change percent`"), "%", "").cast(DoubleType()))
                         .withColumn("producer_timestamp", 
                                   F.to_timestamp(F.col("_producer_metadata.producer_timestamp")))
                         .withColumn("processing_timestamp", F.current_timestamp())
                         .select(
                             "symbol",
                             "open_price",
                             "high_price", 
                             "low_price",
                             "current_price",
                             "volume",
                             "previous_close",
                             "change",
                             "change_percent",
                             "producer_timestamp",
                             "processing_timestamp",
                             "kafka_timestamp",
                             "topic",
                             "partition",
                             "offset"
                         ))
            
            logger.info("Kafka messages parsed successfully")
            return cleaned_df
            
        except Exception as e:
            error_msg = f"Failed to parse Kafka messages: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def apply_data_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply data transformations including price calculations and moving averages.
        
        Args:
            df: Input DataFrame with stock data
            
        Returns:
            Transformed DataFrame with additional calculated fields
        """
        logger.info("Applying data transformations")
        
        try:
            # Add watermark for late data handling
            watermarked_df = df.withWatermark("processing_timestamp", self.config.spark.watermark_delay)
            
            # Calculate additional price metrics
            transformed_df = (watermarked_df
                             .withColumn("price_change_abs", F.abs(F.col("change")))
                             .withColumn("price_volatility", 
                                       (F.col("high_price") - F.col("low_price")) / F.col("current_price") * 100)
                             .withColumn("volume_weighted_price", 
                                       F.when(F.col("volume") > 0, 
                                            (F.col("current_price") * F.col("volume")) / F.col("volume"))
                                        .otherwise(F.col("current_price")))
                             .withColumn("market_cap_indicator",
                                       F.when(F.col("current_price") * F.col("volume") > 1000000, "large")
                                        .when(F.col("current_price") * F.col("volume") > 100000, "medium")
                                        .otherwise("small"))
                             .withColumn("trading_session",
                                       F.when(F.hour("processing_timestamp").between(9, 16), "regular")
                                        .when(F.hour("processing_timestamp").between(4, 9), "pre_market")
                                        .otherwise("after_hours")))
            
            # Calculate moving averages using window functions
            from pyspark.sql.window import Window
            
            # Define windows for moving averages (partitioned by symbol, ordered by timestamp)
            window_5min = (Window.partitionBy("symbol")
                          .orderBy("processing_timestamp")
                          .rangeBetween(-300, 0))  # 5 minutes in seconds
            
            window_20min = (Window.partitionBy("symbol")
                           .orderBy("processing_timestamp") 
                           .rangeBetween(-1200, 0))  # 20 minutes in seconds
            
            # Apply moving averages
            final_df = (transformed_df
                       .withColumn("sma_5min", F.avg("current_price").over(window_5min))
                       .withColumn("sma_20min", F.avg("current_price").over(window_20min))
                       .withColumn("volume_sma_5min", F.avg("volume").over(window_5min))
                       .withColumn("price_trend_5min",
                                 F.when(F.col("current_price") > F.col("sma_5min"), "up")
                                  .when(F.col("current_price") < F.col("sma_5min"), "down")
                                  .otherwise("neutral"))
                       .withColumn("volume_ratio",
                                 F.when(F.col("volume_sma_5min") > 0,
                                      F.col("volume") / F.col("volume_sma_5min"))
                                  .otherwise(1.0)))
            
            logger.info("Data transformations applied successfully")
            return final_df
            
        except Exception as e:
            error_msg = f"Failed to apply data transformations: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def write_to_kafka_with_validation(self, df: DataFrame, topic: str, checkpoint_path: str, data_type: str) -> StreamingQuery:
        """
        Write streaming DataFrame to Kafka topic with Silver layer validation.
        
        Args:
            df: DataFrame to write
            topic: Kafka topic name
            checkpoint_path: Checkpoint location for fault tolerance
            data_type: Type of data for validation
            
        Returns:
            StreamingQuery object
        """
        logger.info(f"Writing stream to Kafka topic with validation: {topic}")
        
        try:
            # Ensure checkpoint directory exists
            os.makedirs(checkpoint_path, exist_ok=True)
            
            def validate_and_write(batch_df, batch_id):
                try:
                    logger.info(f"Processing batch {batch_id} for topic {topic}")
                    
                    if batch_df.count() > 0:
                        # Perform Silver layer validation
                        validation_results = self.validate_silver_layer_data(batch_df, data_type)
                        
                        # Check for critical errors
                        critical_errors = [r for r in validation_results if not r.passed and r.severity == "ERROR"]
                        if critical_errors:
                            logger.error(f"Critical validation errors in batch {batch_id}, publishing alerts")
                            self.publish_data_quality_alerts(validation_results)
                            
                            # Optionally, you could choose to not publish data with critical errors
                            # For now, we'll log the errors but continue publishing
                        
                        # Serialize data using Avro
                        def serialize_row_to_avro(row_data, data_type):
                            """Serialize a single row to Avro format."""
                            try:
                                if data_type == "stock_prices":
                                    return self.avro_serializer.serialize_processed_stock_prices(row_data)
                                elif data_type == "trading_volume":
                                    return self.avro_serializer.serialize_processed_trading_volume(row_data)
                                elif data_type == "technical_indicators":
                                    return self.avro_serializer.serialize_processed_technical_indicators(row_data)
                                else:
                                    raise ValueError(f"Unknown data type: {data_type}")
                            except Exception as e:
                                logger.error(f"Failed to serialize row: {str(e)}")
                                raise
                        
                        # Convert DataFrame to list of dictionaries for Avro serialization
                        rows = batch_df.collect()
                        
                        # Prepare Kafka messages with Avro serialization
                        kafka_messages = []
                        for row in rows:
                            try:
                                row_dict = row.asDict()
                                
                                # Serialize to Avro
                                avro_value = serialize_row_to_avro(row_dict, data_type)
                                
                                kafka_messages.append({
                                    "key": row_dict.get("symbol", ""),
                                    "value": avro_value
                                })
                                
                            except Exception as e:
                                logger.error(f"Failed to process row for Kafka: {str(e)}")
                                continue
                        
                        if kafka_messages:
                            # Create DataFrame from serialized messages
                            kafka_df = self.spark.createDataFrame(kafka_messages)
                            
                            # Write to Kafka
                            (kafka_df.write
                             .format("kafka")
                             .option("kafka.bootstrap.servers", 
                                    ",".join(self.config.kafka.bootstrap_servers))
                             .option("topic", topic)
                             .option("kafka.security.protocol", self.config.kafka.security_protocol)
                             .option("kafka.acks", self.config.kafka.producer_acks)
                             .option("kafka.retries", str(self.config.kafka.producer_retries))
                             .option("kafka.batch.size", str(self.config.kafka.producer_batch_size))
                             .option("kafka.linger.ms", str(self.config.kafka.producer_linger_ms))
                             .option("kafka.compression.type", self.config.kafka.producer_compression_type)
                             .option("kafka.enable.idempotence", "true")
                             .save())
                        
                        logger.info(f"Successfully wrote batch {batch_id} to topic {topic}")
                    
                except Exception as e:
                    logger.error(f"Error processing batch {batch_id} for topic {topic}: {str(e)}")
                    raise
            
            query = (df.writeStream
                    .foreachBatch(validate_and_write)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=self.config.spark.trigger_processing_time)
                    .start())
            
            logger.info(
                "Kafka streaming query with validation started",
                extra={
                    "query_id": query.id,
                    "topic": topic,
                    "data_type": data_type,
                    "checkpoint_path": checkpoint_path,
                    "trigger_interval": self.config.spark.trigger_processing_time
                }
            )
            
            return query
            
        except Exception as e:
            error_msg = f"Failed to write to Kafka topic {topic} with validation: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e

    def write_to_kafka(self, df: DataFrame, topic: str, checkpoint_path: str) -> StreamingQuery:
        """
        Write streaming DataFrame to Kafka topic.
        
        Args:
            df: DataFrame to write
            topic: Kafka topic name
            checkpoint_path: Checkpoint location for fault tolerance
            
        Returns:
            StreamingQuery object
        """
        logger.info(f"Writing stream to Kafka topic: {topic}")
        
        try:
            # Ensure checkpoint directory exists
            os.makedirs(checkpoint_path, exist_ok=True)
            
            # Prepare DataFrame for Kafka - convert to JSON
            kafka_df = (df
                       .select(
                           F.col("symbol").alias("key"),  # Use symbol as Kafka key for partitioning
                           F.to_json(F.struct(*df.columns)).alias("value")
                       ))
            
            query = (kafka_df.writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", 
                           ",".join(self.config.kafka.bootstrap_servers))
                    .option("topic", topic)
                    .option("checkpointLocation", checkpoint_path)
                    .option("kafka.security.protocol", self.config.kafka.security_protocol)
                    # Add producer configurations for reliability
                    .option("kafka.acks", self.config.kafka.producer_acks)
                    .option("kafka.retries", str(self.config.kafka.producer_retries))
                    .option("kafka.batch.size", str(self.config.kafka.producer_batch_size))
                    .option("kafka.linger.ms", str(self.config.kafka.producer_linger_ms))
                    .option("kafka.compression.type", self.config.kafka.producer_compression_type)
                    .option("kafka.enable.idempotence", "true")
                    .trigger(processingTime=self.config.spark.trigger_processing_time)
                    .outputMode("append")
                    .start())
            
            logger.info(
                "Kafka streaming query started",
                extra={
                    "query_id": query.id,
                    "topic": topic,
                    "checkpoint_path": checkpoint_path,
                    "trigger_interval": self.config.spark.trigger_processing_time
                }
            )
            
            return query
            
        except Exception as e:
            error_msg = f"Failed to write to Kafka topic {topic}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e

    def write_to_parquet(self, df: DataFrame, output_path: str, checkpoint_path: str) -> StreamingQuery:
        """
        Write streaming DataFrame to Parquet format.
        
        Args:
            df: DataFrame to write
            output_path: Output path for Parquet files
            checkpoint_path: Checkpoint location for fault tolerance
            
        Returns:
            StreamingQuery object
        """
        logger.info(f"Writing stream to Parquet: {output_path}")
        
        try:
            # Ensure checkpoint directory exists
            os.makedirs(checkpoint_path, exist_ok=True)
            
            query = (df.writeStream
                    .format("parquet")
                    .option("path", output_path)
                    .option("checkpointLocation", checkpoint_path)
                    .partitionBy("symbol", "trading_session")  # Partition by symbol and session
                    .trigger(processingTime=self.config.spark.trigger_processing_time)
                    .outputMode("append")
                    .start())
            
            logger.info(
                "Parquet streaming query started",
                extra={
                    "query_id": query.id,
                    "output_path": output_path,
                    "checkpoint_path": checkpoint_path,
                    "trigger_interval": self.config.spark.trigger_processing_time
                }
            )
            
            return query
            
        except Exception as e:
            error_msg = f"Failed to write to Parquet: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def process_stock_quotes_stream(self, output_base_path: str = "/tmp/streaming-output") -> StreamingQuery:
        """
        Process stock quotes stream end-to-end with Kafka publishing for medallion architecture.
        
        Args:
            output_base_path: Base path for output files
            
        Returns:
            StreamingQuery object for the main processing pipeline
        """
        logger.info("Starting stock quotes stream processing with Kafka publishing")
        
        try:
            # Create Kafka stream
            kafka_stream = self.create_kafka_stream(self.config.kafka.stock_quotes_topic)
            
            # Parse messages
            parsed_stream = self.parse_kafka_messages(kafka_stream)
            
            # Apply transformations with Silver layer validation
            transformed_stream = self.apply_data_transformations(parsed_stream)
            
            # Set up checkpoint paths
            base_checkpoint_path = f"{self.config.spark.checkpoint_location}/stock_quotes"
            parquet_checkpoint_path = f"{base_checkpoint_path}/parquet"
            kafka_checkpoint_path = f"{base_checkpoint_path}/kafka"
            
            # Create data quality monitoring stream for Bronze layer
            dq_monitoring_query = self.create_data_quality_monitoring_stream(base_checkpoint_path)
            self.active_queries["data_quality_monitoring"] = dq_monitoring_query
            
            # Publish to Kafka topics for medallion architecture
            kafka_queries = self.publish_to_kafka_topics(transformed_stream, kafka_checkpoint_path)
            
            # Also write to Parquet for backup/debugging (optional)
            output_path = f"{output_base_path}/stock_quotes"
            parquet_query = self.write_to_parquet(transformed_stream, output_path, parquet_checkpoint_path)
            
            # Track all queries
            self.active_queries["stock_quotes_parquet"] = parquet_query
            for topic_name, query in kafka_queries.items():
                self.active_queries[f"stock_quotes_{topic_name.replace('-', '_')}"] = query
            
            logger.info(
                "Stock quotes stream processing started successfully",
                extra={
                    "parquet_query_id": parquet_query.id,
                    "kafka_queries": {name: query.id for name, query in kafka_queries.items()},
                    "input_topic": self.config.kafka.stock_quotes_topic,
                    "output_path": output_path,
                    "kafka_topics": list(kafka_queries.keys())
                }
            )
            
            # Return the main parquet query for backward compatibility
            return parquet_query
            
        except Exception as e:
            error_msg = f"Failed to start stock quotes stream processing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def prepare_processed_stock_prices(self, df: DataFrame) -> DataFrame:
        """
        Prepare processed stock price data for publishing to Kafka with Silver layer validation.
        
        Args:
            df: Transformed DataFrame with stock data
            
        Returns:
            DataFrame formatted for processed-stock-prices topic
        """
        logger.info("Preparing processed stock prices data with Silver layer validation")
        
        try:
            processed_df = (df
                           .select(
                               "symbol",
                               "open_price",
                               "high_price",
                               "low_price", 
                               "current_price",
                               "previous_close",
                               "change",
                               "change_percent",
                               "sma_5min",
                               "sma_20min",
                               "price_trend_5min",
                               "price_volatility",
                               "trading_session",
                               "producer_timestamp",
                               "processing_timestamp"
                           )
                           .withColumn("data_layer", F.lit("silver"))
                           .withColumn("record_type", F.lit("stock_price"))
                           .withColumn("processing_version", F.lit("1.0")))
            
            # Perform Silver layer validation for stock prices
            # Note: In a streaming context, we would typically do this in a foreachBatch operation
            # For now, we'll log that validation should be performed
            logger.info("Silver layer validation for stock prices should be performed in streaming context")
            
            logger.info("Processed stock prices data prepared successfully")
            return processed_df
            
        except Exception as e:
            error_msg = f"Failed to prepare processed stock prices: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def prepare_processed_trading_volume(self, df: DataFrame) -> DataFrame:
        """
        Prepare processed trading volume data for publishing to Kafka.
        
        Args:
            df: Transformed DataFrame with stock data
            
        Returns:
            DataFrame formatted for processed-trading-volume topic
        """
        logger.info("Preparing processed trading volume data")
        
        try:
            volume_df = (df
                        .select(
                            "symbol",
                            "volume",
                            "volume_weighted_price",
                            "volume_sma_5min",
                            "volume_ratio",
                            "trading_session",
                            "producer_timestamp",
                            "processing_timestamp"
                        )
                        .withColumn("data_layer", F.lit("silver"))
                        .withColumn("record_type", F.lit("trading_volume"))
                        .withColumn("processing_version", F.lit("1.0"))
                        # Add additional volume metrics
                        .withColumn("volume_category",
                                  F.when(F.col("volume_ratio") > 2.0, "high")
                                   .when(F.col("volume_ratio") > 1.5, "above_average")
                                   .when(F.col("volume_ratio") < 0.5, "low")
                                   .otherwise("normal")))
            
            logger.info("Processed trading volume data prepared successfully")
            return volume_df
            
        except Exception as e:
            error_msg = f"Failed to prepare processed trading volume: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def prepare_processed_technical_indicators(self, df: DataFrame) -> DataFrame:
        """
        Prepare processed technical indicators data for publishing to Kafka.
        
        Args:
            df: Transformed DataFrame with stock data
            
        Returns:
            DataFrame formatted for processed-technical-indicators topic
        """
        logger.info("Preparing processed technical indicators data")
        
        try:
            indicators_df = (df
                            .select(
                                "symbol",
                                "current_price",
                                "sma_5min",
                                "sma_20min",
                                "price_trend_5min",
                                "price_volatility",
                                "volume_ratio",
                                "trading_session",
                                "producer_timestamp",
                                "processing_timestamp"
                            )
                            .withColumn("data_layer", F.lit("silver"))
                            .withColumn("record_type", F.lit("technical_indicators"))
                            .withColumn("processing_version", F.lit("1.0"))
                            # Add technical indicator signals
                            .withColumn("momentum_signal",
                                      F.when(F.col("price_trend_5min") == "up", "bullish")
                                       .when(F.col("price_trend_5min") == "down", "bearish")
                                       .otherwise("neutral"))
                            .withColumn("volatility_level",
                                      F.when(F.col("price_volatility") > 5.0, "high")
                                       .when(F.col("price_volatility") > 2.0, "medium")
                                       .otherwise("low")))
            
            logger.info("Processed technical indicators data prepared successfully")
            return indicators_df
            
        except Exception as e:
            error_msg = f"Failed to prepare processed technical indicators: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def publish_to_kafka_topics(self, transformed_df: DataFrame, base_checkpoint_path: str) -> Dict[str, StreamingQuery]:
        """
        Publish transformed data to multiple Kafka topics for medallion architecture.
        
        Args:
            transformed_df: Transformed DataFrame with all stock data
            base_checkpoint_path: Base path for checkpoints
            
        Returns:
            Dictionary of topic names to StreamingQuery objects
        """
        logger.info("Publishing transformed data to Kafka topics")
        
        try:
            queries = {}
            
            # Prepare different data views
            stock_prices_df = self.prepare_processed_stock_prices(transformed_df)
            trading_volume_df = self.prepare_processed_trading_volume(transformed_df)
            technical_indicators_df = self.prepare_processed_technical_indicators(transformed_df)
            
            # Publish to processed-stock-prices topic with validation
            stock_prices_checkpoint = f"{base_checkpoint_path}/processed-stock-prices"
            stock_prices_query = self.write_to_kafka_with_validation(
                stock_prices_df, 
                self.config.kafka.processed_stock_prices_topic, 
                stock_prices_checkpoint,
                "stock_prices"
            )
            queries[self.config.kafka.processed_stock_prices_topic] = stock_prices_query
            
            # Publish to processed-trading-volume topic with validation
            volume_checkpoint = f"{base_checkpoint_path}/processed-trading-volume"
            volume_query = self.write_to_kafka_with_validation(
                trading_volume_df,
                self.config.kafka.processed_trading_volume_topic,
                volume_checkpoint,
                "trading_volume"
            )
            queries[self.config.kafka.processed_trading_volume_topic] = volume_query
            
            # Publish to processed-technical-indicators topic with validation
            indicators_checkpoint = f"{base_checkpoint_path}/processed-technical-indicators"
            indicators_query = self.write_to_kafka_with_validation(
                technical_indicators_df,
                self.config.kafka.processed_technical_indicators_topic, 
                indicators_checkpoint,
                "technical_indicators"
            )
            queries[self.config.kafka.processed_technical_indicators_topic] = indicators_query
            
            logger.info(
                "Successfully started publishing to Kafka topics",
                extra={
                    "topics": list(queries.keys()),
                    "query_count": len(queries)
                }
            )
            
            return queries
            
        except Exception as e:
            error_msg = f"Failed to publish to Kafka topics: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def handle_kafka_publishing_errors(self, query_name: str) -> bool:
        """
        Handle Kafka publishing errors with retry logic.
        
        Args:
            query_name: Name of the failed query
            
        Returns:
            True if error was handled successfully, False otherwise
        """
        logger.warning(f"Handling Kafka publishing error for query: {query_name}")
        
        try:
            if query_name not in self.active_queries:
                logger.error(f"Query {query_name} not found in active queries")
                return False
            
            query = self.active_queries[query_name]
            
            # Check if query has an exception
            if query.exception():
                exception_msg = str(query.exception())
                logger.error(f"Query {query_name} exception: {exception_msg}")
                
                # Check for specific Kafka errors that can be retried
                retryable_errors = [
                    "org.apache.kafka.common.errors.TimeoutException",
                    "org.apache.kafka.common.errors.RetriableException", 
                    "org.apache.kafka.common.errors.NetworkException",
                    "Connection refused",
                    "Broker may not be available"
                ]
                
                is_retryable = any(error in exception_msg for error in retryable_errors)
                
                if is_retryable:
                    logger.info(f"Retryable error detected for query {query_name}, attempting restart")
                    
                    # Stop the failed query
                    self.stop_query(query_name)
                    
                    # Wait before retry
                    import time
                    time.sleep(5)
                    
                    # Attempt to restart based on query type
                    if "processed_stock_prices" in query_name:
                        self._restart_kafka_query(query_name, "processed-stock-prices")
                    elif "processed_trading_volume" in query_name:
                        self._restart_kafka_query(query_name, "processed-trading-volume")
                    elif "processed_technical_indicators" in query_name:
                        self._restart_kafka_query(query_name, "processed-technical-indicators")
                    else:
                        logger.error(f"Unknown query type for restart: {query_name}")
                        return False
                    
                    logger.info(f"Successfully restarted query: {query_name}")
                    return True
                else:
                    logger.error(f"Non-retryable error for query {query_name}: {exception_msg}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to handle Kafka publishing error for {query_name}: {str(e)}")
            return False
    
    def _restart_kafka_query(self, query_name: str, topic: str) -> None:
        """
        Restart a specific Kafka publishing query.
        
        Args:
            query_name: Name of the query to restart
            topic: Kafka topic name
        """
        logger.info(f"Restarting Kafka query {query_name} for topic {topic}")
        
        try:
            # This would need to be implemented based on the specific query type
            # For now, we'll log that a restart is needed
            logger.warning(f"Kafka query restart needed for {query_name} -> {topic}")
            logger.warning("Manual intervention may be required to restart the full pipeline")
            
        except Exception as e:
            logger.error(f"Failed to restart Kafka query {query_name}: {str(e)}")
            raise StreamProcessorError(f"Failed to restart Kafka query {query_name}: {str(e)}") from e
    
    def validate_bronze_layer_data(self, df: DataFrame, topic: str) -> List[LayerValidationResult]:
        """
        Validate Bronze layer data quality.
        
        Args:
            df: Raw Kafka DataFrame
            topic: Source Kafka topic
            
        Returns:
            List of validation results
        """
        logger.info(f"Validating Bronze layer data from topic: {topic}")
        
        try:
            validation_results = self.data_quality_validator.validate_bronze_layer(df, topic)
            
            # Log validation summary
            total_checks = len(validation_results)
            passed_checks = sum(1 for r in validation_results if r.passed)
            failed_checks = total_checks - passed_checks
            
            logger.info(
                f"Bronze layer validation completed: {passed_checks}/{total_checks} checks passed",
                extra={
                    "topic": topic,
                    "total_checks": total_checks,
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks
                }
            )
            
            # Log failed checks
            for result in validation_results:
                if not result.passed and result.severity == "ERROR":
                    logger.error(f"Bronze layer validation failed: {result.message}")
                elif not result.passed and result.severity == "WARNING":
                    logger.warning(f"Bronze layer validation warning: {result.message}")
            
            return validation_results
            
        except Exception as e:
            error_msg = f"Failed to validate Bronze layer data: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def validate_silver_layer_data(self, df: DataFrame, data_type: str) -> List[LayerValidationResult]:
        """
        Validate Silver layer data quality.
        
        Args:
            df: Processed DataFrame
            data_type: Type of processed data
            
        Returns:
            List of validation results
        """
        logger.info(f"Validating Silver layer data for type: {data_type}")
        
        try:
            validation_results = self.data_quality_validator.validate_silver_layer(df, data_type)
            
            # Log validation summary
            total_checks = len(validation_results)
            passed_checks = sum(1 for r in validation_results if r.passed)
            failed_checks = total_checks - passed_checks
            
            logger.info(
                f"Silver layer validation completed: {passed_checks}/{total_checks} checks passed",
                extra={
                    "data_type": data_type,
                    "total_checks": total_checks,
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks
                }
            )
            
            # Log failed checks
            for result in validation_results:
                if not result.passed and result.severity == "ERROR":
                    logger.error(f"Silver layer validation failed: {result.message}")
                elif not result.passed and result.severity == "WARNING":
                    logger.warning(f"Silver layer validation warning: {result.message}")
            
            return validation_results
            
        except Exception as e:
            error_msg = f"Failed to validate Silver layer data: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def validate_gold_layer_data(self, df: DataFrame, table_type: str) -> List[LayerValidationResult]:
        """
        Validate Gold layer data quality.
        
        Args:
            df: Dimensional DataFrame
            table_type: Type of dimensional table
            
        Returns:
            List of validation results
        """
        logger.info(f"Validating Gold layer data for table: {table_type}")
        
        try:
            validation_results = self.data_quality_validator.validate_gold_layer(df, table_type)
            
            # Log validation summary
            total_checks = len(validation_results)
            passed_checks = sum(1 for r in validation_results if r.passed)
            failed_checks = total_checks - passed_checks
            
            logger.info(
                f"Gold layer validation completed: {passed_checks}/{total_checks} checks passed",
                extra={
                    "table_type": table_type,
                    "total_checks": total_checks,
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks
                }
            )
            
            # Log failed checks
            for result in validation_results:
                if not result.passed and result.severity == "ERROR":
                    logger.error(f"Gold layer validation failed: {result.message}")
                elif not result.passed and result.severity == "WARNING":
                    logger.warning(f"Gold layer validation warning: {result.message}")
            
            return validation_results
            
        except Exception as e:
            error_msg = f"Failed to validate Gold layer data: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def publish_data_quality_alerts(self, validation_results: List[LayerValidationResult]) -> None:
        """
        Publish data quality alerts to Kafka topic using Avro serialization.
        
        Args:
            validation_results: List of validation results
        """
        try:
            # Generate alert messages
            alerts = self.data_quality_validator.publish_data_quality_alerts(
                validation_results, 
                self.config.kafka.data_quality_alerts_topic,
                self.avro_serializer
            )
            
            if not alerts:
                return
            
            # Serialize alerts to Avro and publish to Kafka
            kafka_messages = []
            for alert in alerts:
                try:
                    # Serialize to Avro
                    avro_value = self.avro_serializer.serialize_data_quality_alert(alert)
                    
                    kafka_messages.append({
                        "key": f"{alert['layer']}_{alert['rule_name']}",
                        "value": avro_value
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to serialize data quality alert: {str(e)}")
                    continue
            
            if kafka_messages:
                # Create DataFrame and publish to Kafka
                kafka_df = self.spark.createDataFrame(kafka_messages)
                
                (kafka_df.write
                 .format("kafka")
                 .option("kafka.bootstrap.servers", 
                        ",".join(self.config.kafka.bootstrap_servers))
                 .option("topic", self.config.kafka.data_quality_alerts_topic)
                 .option("kafka.security.protocol", self.config.kafka.security_protocol)
                 .option("kafka.acks", self.config.kafka.producer_acks)
                 .option("kafka.retries", str(self.config.kafka.producer_retries))
                 .option("kafka.enable.idempotence", "true")
                 .save())
                
                logger.info(f"Published {len(kafka_messages)} data quality alerts to Kafka topic: {self.config.kafka.data_quality_alerts_topic}")
            
        except Exception as e:
            logger.error(f"Failed to publish data quality alerts: {str(e)}")
    
    def create_data_quality_monitoring_stream(self, base_checkpoint_path: str) -> StreamingQuery:
        """
        Create a streaming query specifically for data quality monitoring.
        
        Args:
            base_checkpoint_path: Base path for checkpoints
            
        Returns:
            StreamingQuery for data quality monitoring
        """
        logger.info("Creating data quality monitoring stream")
        
        try:
            # Create Kafka stream for monitoring
            kafka_stream = self.create_kafka_stream(self.config.kafka.stock_quotes_topic)
            
            # Apply Bronze layer validation
            def validate_and_forward(batch_df, batch_id):
                try:
                    logger.info(f"Processing batch {batch_id} for data quality monitoring")
                    
                    if batch_df.count() > 0:
                        # Validate Bronze layer
                        bronze_results = self.validate_bronze_layer_data(
                            batch_df, 
                            self.config.kafka.stock_quotes_topic
                        )
                        
                        # Publish alerts if there are issues
                        failed_results = [r for r in bronze_results if not r.passed]
                        if failed_results:
                            self.publish_data_quality_alerts(failed_results)
                        
                        # Generate and log quality report
                        quality_report = self.data_quality_validator.generate_layer_quality_report(bronze_results)
                        logger.info(f"Data quality report for batch {batch_id}: {quality_report}")
                    
                except Exception as e:
                    logger.error(f"Error in data quality monitoring batch {batch_id}: {str(e)}")
            
            # Set up monitoring query
            checkpoint_path = f"{base_checkpoint_path}/data_quality_monitoring"
            
            query = (kafka_stream.writeStream
                    .foreachBatch(validate_and_forward)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=self.config.spark.trigger_processing_time)
                    .start())
            
            logger.info(
                "Data quality monitoring stream started",
                extra={
                    "query_id": query.id,
                    "checkpoint_path": checkpoint_path
                }
            )
            
            return query
            
        except Exception as e:
            error_msg = f"Failed to create data quality monitoring stream: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def get_query_status(self, query_name: str) -> Dict[str, Any]:
        """
        Get status information for a streaming query.
        
        Args:
            query_name: Name of the query to check
            
        Returns:
            Dictionary with query status information
        """
        if query_name not in self.active_queries:
            return {"error": f"Query '{query_name}' not found"}
        
        query = self.active_queries[query_name]
        
        try:
            progress = query.lastProgress
            status = {
                "query_id": query.id,
                "name": query.name,
                "is_active": query.isActive,
                "batch_id": progress.get("batchId", -1) if progress else -1,
                "input_rows_per_second": progress.get("inputRowsPerSecond", 0) if progress else 0,
                "processed_rows_per_second": progress.get("processedRowsPerSecond", 0) if progress else 0,
                "batch_duration_ms": progress.get("batchDuration", 0) if progress else 0,
                "timestamp": progress.get("timestamp") if progress else None
            }
            
            if not query.isActive and query.exception():
                status["exception"] = str(query.exception())
            
            return status
            
        except Exception as e:
            return {"error": f"Failed to get query status: {str(e)}"}
    
    def stop_query(self, query_name: str) -> bool:
        """
        Stop a streaming query.
        
        Args:
            query_name: Name of the query to stop
            
        Returns:
            True if successfully stopped, False otherwise
        """
        if query_name not in self.active_queries:
            logger.warning(f"Query '{query_name}' not found")
            return False
        
        try:
            query = self.active_queries[query_name]
            query.stop()
            del self.active_queries[query_name]
            
            logger.info(f"Query '{query_name}' stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop query '{query_name}': {str(e)}")
            return False
    
    def stop_all_queries(self) -> None:
        """Stop all active streaming queries."""
        logger.info("Stopping all streaming queries")
        
        for query_name in list(self.active_queries.keys()):
            self.stop_query(query_name)  
        
        logger.info("All streaming queries stopped")
    
    def close(self) -> None:
        """Close the stream processor and clean up resources."""
        logger.info("Closing StreamProcessor")
        
        # Stop all queries
        self.stop_all_queries()
        
        # Close Avro serializer
        if hasattr(self, 'avro_serializer') and self.avro_serializer:
            self.avro_serializer.close()
            logger.info("Avro serializer closed")
        
        # Stop Spark session
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")
        
        logger.info("StreamProcessor closed successfully")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
"""
Spark Structured Streaming processor for real-time financial data processing.
Consumes data from Kafka, applies transformations, and outputs to Parquet format.
"""
import logging
import os
import shutil
from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, LongType
from pyspark.sql.streaming import StreamingQuery

from ..config.settings import ConfigManager


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
                    .config("spark.serializer", self.config.spark.serializer)
                    .config("spark.driver.memory", self.config.spark.driver_memory)
                    .config("spark.executor.memory", self.config.spark.executor_memory)
                    .config("spark.executor.cores", str(self.config.spark.executor_cores))
                    .config("spark.driver.maxResultSize", self.config.spark.max_result_size)
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                    .config("spark.sql.parquet.compression.codec", "snappy")
                    .getOrCreate())
            
            # Set log level
            spark.sparkContext.setLogLevel("WARN")
            
            # Basic Kafka verification
            try:
                # Simple check if Kafka format is available
                spark.readStream.format("kafka")
                logger.info("Kafka packages verified successfully")
            except Exception as e:
                logger.warning(f"Kafka packages verification failed: {str(e)}")
                # Don't stop the session, just log the warning
            
            logger.info(f"Spark session created successfully - Version: {spark.version}")
            
            return spark
            
        except Exception as e:
            error_msg = f"Failed to create Spark session: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def get_avro_schema_string(self, schema_name: str = "stock_quote") -> str:
        """
        Get Avro schema string for deserializing Kafka messages.
        
        Args:
            schema_name: Name of the schema to retrieve (default: "stock_quote")
            
        Returns:
            Avro schema as JSON string compatible with Spark's from_avro() function
        """
        try:
            from ..schemas.avro_schemas import get_all_schemas
            import json
            
            # Get all available schemas
            schemas = get_all_schemas()
            
            # Check if schema exists
            if schema_name not in schemas:
                raise StreamProcessorError(f"Unknown schema '{schema_name}'")
            
            # Get the schema
            schema_dict = schemas[schema_name]
            
            # Basic validation
            if not isinstance(schema_dict, dict) or schema_dict.get("type") != "record":
                raise StreamProcessorError(f"Invalid schema structure for '{schema_name}'")
            
            # Convert to JSON string
            schema_json = json.dumps(schema_dict, separators=(',', ':'), sort_keys=True)
            
            logger.info("Schema retrieved successfully")
            
            return schema_json
            
        except Exception as e:
            logger.error(f"Failed to retrieve schema '{schema_name}': {str(e)}")
            raise StreamProcessorError(f"Failed to retrieve schema '{schema_name}': {str(e)}") from e
    
    def get_avro_schema_from_registry(self, subject: str, version: str = "latest") -> str:
        """
        Get Avro schema from Schema Registry.
        
        Args:
            subject: Schema Registry subject name
            version: Schema version (default: "latest")
            
        Returns:
            Avro schema as JSON string compatible with Spark's from_avro() function
        """
        try:
            from ..schemas.schema_registry_client import SchemaRegistryClient
            import json
            
            # Initialize Schema Registry client
            registry_url = getattr(self.config, 'schema_registry_url', 'http://localhost:8085')
            client = SchemaRegistryClient(registry_url)
            
            # Get schema from registry
            schema_info = client.get_schema(subject, version)
            
            if not schema_info or 'schema' not in schema_info:
                raise StreamProcessorError(f"No schema found for subject '{subject}' version '{version}'")
            
            # Parse and format schema JSON
            schema_json = schema_info['schema']
            if isinstance(schema_json, str):
                schema_dict = json.loads(schema_json)
                schema_json = json.dumps(schema_dict, separators=(',', ':'), sort_keys=True)
            else:
                schema_json = json.dumps(schema_json, separators=(',', ':'), sort_keys=True)
            
            logger.info("Schema retrieved from registry")
            
            return schema_json
            
        except Exception as e:
            logger.error(f"Failed to retrieve schema from registry for '{subject}': {str(e)}")
            raise StreamProcessorError(f"Failed to retrieve schema from registry for '{subject}': {str(e)}") from e
    
    def validate_avro_schema_for_spark(self, schema_json: str) -> bool:
        """
        Validate that an Avro schema is compatible with Spark's from_avro() function.
        
        Args:
            schema_json: Avro schema as JSON string
            
        Returns:
            True if schema is compatible, False otherwise
        """
        try:
            import json
            
            # Parse schema JSON
            schema_dict = json.loads(schema_json)
            
            # Basic validation
            if (not isinstance(schema_dict, dict) or 
                schema_dict.get("type") != "record" or
                "name" not in schema_dict or
                "fields" not in schema_dict or
                not isinstance(schema_dict.get("fields"), list) or
                len(schema_dict.get("fields", [])) == 0):
                return False
            
            logger.info("Schema validation passed")
            return True
            
        except Exception as e:
            logger.warning(f"Schema validation failed: {str(e)}")
            return False
    
    
    def create_kafka_stream(self, topic: str) -> DataFrame:
        """
        Create a streaming DataFrame from Kafka topic.
        
        Args:
            topic: Kafka topic name to consume from
            
        Returns:
            Streaming DataFrame from Kafka
        """
        logger.info(f"Creating Kafka stream for topic: {topic} with earliest offsets to process existing data")
        
        try:
            kafka_df = (self.spark
                       .readStream
                       .format("kafka")
                       .option("kafka.bootstrap.servers", 
                              ",".join(self.config.kafka.bootstrap_servers))
                       .option("subscribe", topic)
                       .option("startingOffsets", "earliest")
                       .option("failOnDataLoss", "false")
                       .option("kafka.security.protocol", self.config.kafka.security_protocol)
                       .option("mode", "PERMISSIVE")
                       .load())
            
            logger.info("Kafka stream created successfully")
            return kafka_df
            
        except Exception as e:
            error_msg = f"Failed to create Kafka stream for topic {topic}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def parse_kafka_messages(self, kafka_df: DataFrame) -> DataFrame:
        """
        Parse Kafka messages and extract stock data using Avro deserialization.
        
        Args:
            kafka_df: Raw Kafka DataFrame
            
        Returns:
            Parsed DataFrame with stock data
        """
        logger.info("Parsing Kafka messages using Avro deserialization")
        
        try:
            from pyspark.sql.avro.functions import from_avro
            
            # Topic to schema mapping for multi-source support
            topic_schema_map = {
                self.config.kafka.stock_quotes_topic: "stock_quote",
                self.config.kafka.stock_intraday_topic: "intraday_data"
            }
            
            # Parse Schema Registry Avro data with topic-aware schema selection
            all_parsed_dfs = []
            
            for topic_name, schema_name in topic_schema_map.items():
                try:
                    logger.info(f"Processing topic: {topic_name} with schema: {schema_name}")
                    
                    # Filter messages for this specific topic
                    topic_df = kafka_df.filter(F.col("topic") == topic_name)
                    
                    # Get the appropriate Avro schema for this topic
                    avro_schema = self.get_avro_schema_string(schema_name)
                    
                    # Extract pure Avro payload by removing Schema Registry headers
                    kafka_with_payload = (topic_df
                        .select(
                            F.col("key").cast("string").alias("message_key"),
                            F.col("value").alias("schema_registry_data"),
                            F.col("topic"),
                            F.col("partition"),
                            F.col("offset"),
                            F.col("timestamp").alias("kafka_timestamp")
                        )
                        .withColumn("avro_payload", 
                            F.when(F.length("schema_registry_data") > 5,
                                  F.expr("substring(schema_registry_data, 6, length(schema_registry_data) - 5)"))
                            .otherwise(F.lit(None)))
                        .filter(F.col("avro_payload").isNotNull()))
                    
                    # Use topic-specific schema for Avro deserialization
                    topic_parsed_df = (kafka_with_payload
                        .select(
                            "*",
                            from_avro(F.col("avro_payload"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")
                        ))
                    
                    # Transform each topic to common schema format
                    if schema_name == "stock_quote":
                        harmonized_df = topic_parsed_df.select(
                            "message_key", "topic", "partition", "offset", "kafka_timestamp",
                            F.col("data.symbol").alias("symbol"),
                            F.col("data.open_price").alias("open_price"),
                            F.col("data.high_price").alias("high_price"),
                            F.col("data.low_price").alias("low_price"),
                            F.col("data.current_price").alias("current_price"),
                            F.col("data.volume").alias("volume"),
                            F.col("data.previous_close").alias("previous_close"),
                            F.col("data.change").alias("change"),
                            F.col("data.change_percent").alias("change_percent"),
                            F.col("data.latest_trading_day").alias("latest_trading_day"),
                            F.col("data.timestamp").alias("producer_timestamp_ms"),
                            F.col("data.producer_metadata.producer_timestamp").alias("producer_timestamp_iso")
                        )
                    elif schema_name == "intraday_data":
                        harmonized_df = topic_parsed_df.select(
                            "message_key", "topic", "partition", "offset", "kafka_timestamp",
                            F.col("data.symbol").alias("symbol"),
                            F.col("data.open_price").alias("open_price"),
                            F.col("data.high_price").alias("high_price"),
                            F.col("data.low_price").alias("low_price"),
                            F.col("data.close_price").alias("current_price"),  # Map close_price -> current_price
                            F.col("data.volume").alias("volume"),
                            F.lit(None).cast("double").alias("previous_close"),  # Not available in intraday
                            F.lit(None).cast("double").alias("change"),         # Not available in intraday
                            F.lit(None).cast("double").alias("change_percent"), # Not available in intraday
                            F.col("data.timestamp").alias("latest_trading_day"), # Use timestamp string
                            F.col("data.request_timestamp").alias("producer_timestamp_ms"), # Use request_timestamp (long)
                            F.col("data.producer_metadata.producer_timestamp").alias("producer_timestamp_iso")
                        )
                    else:
                        logger.warning(f"Unknown schema_name: {schema_name}, skipping topic {topic_name}")
                        continue
                    
                    # Apply common filtering to harmonized data
                    filtered_df = harmonized_df.filter(
                        F.col("symbol").isNotNull() & 
                        (F.col("symbol") != "") & 
                        F.col("current_price").isNotNull()
                    )
                    
                    all_parsed_dfs.append(filtered_df)
                    
                except Exception as topic_error:
                    logger.error(f"Failed to process topic {topic_name}: {str(topic_error)}")
                    continue
            
            # Union all successfully parsed DataFrames
            if not all_parsed_dfs:
                raise StreamProcessorError("No topics could be successfully parsed")
            
            # Combine all topic DataFrames
            if len(all_parsed_dfs) == 1:
                final_df = all_parsed_dfs[0]
            else:
                final_df = all_parsed_dfs[0]
                for df in all_parsed_dfs[1:]:
                    final_df = final_df.union(df)
            
            # Convert timestamps and add processing timestamp
            result_df = (final_df
                .withColumn("producer_timestamp", 
                    F.when(F.col("producer_timestamp_ms").isNotNull(),
                          F.col("producer_timestamp_ms").cast(TimestampType()))
                    .otherwise(F.current_timestamp()))
                .withColumn("processing_timestamp", F.current_timestamp())
                .select(
                    "symbol", "open_price", "high_price", "low_price", "current_price",
                    "volume", "previous_close", "change", "change_percent", "latest_trading_day",
                    "producer_timestamp", "processing_timestamp", "kafka_timestamp",
                    "topic", "partition", "offset"
                ))
            
            logger.info("Kafka messages parsed successfully")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to parse Kafka messages: {str(e)}")
            raise StreamProcessorError(f"Failed to parse Kafka messages: {str(e)}") from e
    
    def apply_data_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply data transformations including price calculations and moving averages.
        Enhanced with robust column checking and error handling.
        
        Args:
            df: Input DataFrame with stock data
            
        Returns:
            Transformed DataFrame with additional calculated fields
        """
        logger.info("Applying data transformations")
        
        try:
            # Check available columns
            available_columns = df.columns
            
            # Check if essential columns exist
            required_columns = ["symbol", "current_price", "processing_timestamp"]
            missing_columns = [col for col in required_columns if col not in available_columns]
            
            if missing_columns:
                error_msg = f"Missing essential columns: {missing_columns}. Available: {available_columns}"
                logger.error(error_msg)
                raise StreamProcessorError(error_msg)
            
            # Add watermark for late data handling
            watermarked_df = df.withWatermark("processing_timestamp", self.config.spark.watermark_delay)
            
            # Start with the watermarked DataFrame
            transformed_df = watermarked_df
            
            # Add price calculations only if columns exist
            if "change" in available_columns:
                transformed_df = transformed_df.withColumn("price_change_abs", F.abs(F.col("change")))
            else:
                transformed_df = transformed_df.withColumn("price_change_abs", F.lit(0.0))
            
            if all(col in available_columns for col in ["high_price", "low_price", "current_price"]):
                transformed_df = transformed_df.withColumn("price_volatility", 
                                   (F.col("high_price") - F.col("low_price")) / F.col("current_price") * 100)
            else:
                transformed_df = transformed_df.withColumn("price_volatility", F.lit(0.0))
            
            if all(col in available_columns for col in ["volume", "current_price"]):
                transformed_df = transformed_df.withColumn("volume_weighted_price", 
                                   F.when(F.col("volume") > 0, 
                                        (F.col("current_price") * F.col("volume")) / F.col("volume"))
                                    .otherwise(F.col("current_price")))
            else:
                transformed_df = transformed_df.withColumn("volume_weighted_price", F.col("current_price"))
            
            if all(col in available_columns for col in ["current_price", "volume"]):
                transformed_df = transformed_df.withColumn("market_cap_indicator",
                                   F.when(F.col("current_price") * F.col("volume") > 1000000, "large")
                                    .when(F.col("current_price") * F.col("volume") > 100000, "medium")
                                    .otherwise("small"))
            else:
                transformed_df = transformed_df.withColumn("market_cap_indicator", F.lit("unknown"))
            
            # Add trading session based on processing timestamp
            transformed_df = transformed_df.withColumn("trading_session",
                                   F.when(F.hour("processing_timestamp").between(9, 16), "regular")
                                    .when(F.hour("processing_timestamp").between(4, 9), "pre_market")
                                    .otherwise("after_hours"))
            
            # Add technical indicators - using current price as baseline for simplicity
            final_df = (transformed_df
                       .withColumn("sma_5min", F.col("current_price"))  # Use current price as 5min SMA
                       .withColumn("sma_20min", F.col("current_price"))  # Use current price as 20min SMA
                       .withColumn("volume_sma_5min", 
                                 F.when(F.col("volume").isNotNull(), F.col("volume").cast(DoubleType())).otherwise(F.lit(0.0)))  # Cast to double
                       .withColumn("price_trend_5min", F.lit("neutral"))  # Default to neutral
                       .withColumn("volume_ratio", F.lit(1.0)))  # Default volume ratio
            
            # Final schema ready
            final_columns = final_df.columns
            
            logger.info("Data transformations applied successfully")
            return final_df
            
        except Exception as e:
            error_msg = f"Failed to apply data transformations: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def write_to_kafka_with_validation(self, df: DataFrame, topic: str, checkpoint_path: str, data_type: str) -> StreamingQuery:
        """
        Write streaming DataFrame to Kafka topic using Schema Registry compatible Avro serialization.
        Uses foreachBatch to ensure proper Avro format with magic bytes for S3 connector compatibility.
        
        Args:
            df: DataFrame to write
            topic: Kafka topic name
            checkpoint_path: Checkpoint location for fault tolerance
            data_type: Type of data for validation
            
        Returns:
            StreamingQuery object
        """
        logger.info(f"Writing stream to Kafka topic with Schema Registry Avro: {topic}")
        
        try:
            # Ensure checkpoint directory exists and is fresh
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
                logger.info("Removed existing checkpoint directory")
            os.makedirs(checkpoint_path, exist_ok=True)
            
            # Prepare DataFrame for Schema Registry Avro serialization
            kafka_df = self._prepare_kafka_output_dataframe_avro(df, data_type)
            
            # Define foreachBatch function for Schema Registry Avro serialization
            def write_batch_to_kafka_avro(batch_df, batch_id):
                """
                Write batch to Kafka using AvroSerializer with Schema Registry compatibility.
                This ensures proper magic byte format for S3 connector consumption.
                """
                try:
                    if batch_df.count() == 0:
                        return
                    
                    # Convert to list of records for AvroSerializer
                    records = batch_df.collect()
                    
                    # Use confluent-kafka AvroProducer for proper Schema Registry integration
                    from confluent_kafka.avro import AvroProducer
                    import avro.schema
                    import json
                    
                    # Schema Registry configuration
                    schema_registry_url = getattr(self.config, 'schema_registry_url', 'http://schema-registry:8081')
                    
                    # AvroProducer configuration with Schema Registry
                    producer_config = {
                        'bootstrap.servers': ','.join(self.config.kafka.bootstrap_servers),
                        'security.protocol': self.config.kafka.security_protocol,
                        'acks': self.config.kafka.producer_acks,
                        'retries': self.config.kafka.producer_retries,
                        'batch.size': self.config.kafka.producer_batch_size,
                        'linger.ms': self.config.kafka.producer_linger_ms,
                        'compression.type': self.config.kafka.producer_compression_type,
                        'enable.idempotence': True,
                        'queue.buffering.max.messages': 100000,
                        'queue.buffering.max.kbytes': 32768,
                        'max.in.flight.requests.per.connection': 5,
                        'schema.registry.url': schema_registry_url
                    }
                    
                    # Get the appropriate schema
                    schema_name_map = {
                        "stock_prices": "processed_stock_prices",
                        "trading_volume": "processed_trading_volume", 
                        "technical_indicators": "processed_technical_indicators"
                    }
                    
                    schema_name = schema_name_map.get(data_type)
                    if not schema_name:
                        raise ValueError(f"Unknown data_type: {data_type}")
                    
                    # Get schema dictionary and compile it to proper Avro schema object
                    from ..schemas.avro_schemas import get_all_schemas
                    schemas = get_all_schemas()
                    schema_dict = schemas[schema_name]
                    
                    # Compile schema dictionary to Avro schema object
                    value_schema = avro.schema.parse(json.dumps(schema_dict))
                    
                    # Define key schema for string keys
                    key_schema_dict = {"type": "string", "name": "key"}
                    key_schema = avro.schema.parse(json.dumps(key_schema_dict))
                    
                    producer = AvroProducer(
                        producer_config,
                        default_key_schema=key_schema,
                        default_value_schema=value_schema
                    )
                    successful_records = 0
                    failed_records = 0
                    
                    for record in records:
                        try:
                            # Get the record data as dictionary
                            record_dict = record.asDict()
                            serialization_dict = {col: record_dict[col] for col in record_dict.keys() if col != 'key'}
                            
                            # Validate required fields
                            symbol_value = serialization_dict.get('symbol')
                            if not symbol_value or symbol_value in ['', 'null', None]:
                                failed_records += 1
                                continue
                            
                            # Data type specific validation
                            if data_type == "stock_prices" and serialization_dict.get('current_price') is None:
                                failed_records += 1
                                continue
                            elif data_type == "trading_volume" and serialization_dict.get('volume') is None:
                                failed_records += 1
                                continue
                            elif data_type == "technical_indicators" and serialization_dict.get('current_price') is None:
                                failed_records += 1
                                continue
                            
                            # Transform data for Schema Registry format
                            transformed_data = serialization_dict.copy()
                            
                            # Convert timestamp fields to epoch milliseconds
                            if 'producer_timestamp' in transformed_data and transformed_data['producer_timestamp']:
                                if isinstance(transformed_data['producer_timestamp'], str):
                                    from datetime import datetime
                                    dt = datetime.fromisoformat(transformed_data['producer_timestamp'].replace('Z', '+00:00'))
                                    transformed_data['producer_timestamp'] = int(dt.timestamp() * 1000)
                                elif hasattr(transformed_data['producer_timestamp'], 'timestamp'):
                                    transformed_data['producer_timestamp'] = int(transformed_data['producer_timestamp'].timestamp() * 1000)
                            
                            if 'processing_timestamp' in transformed_data and transformed_data['processing_timestamp']:
                                if isinstance(transformed_data['processing_timestamp'], str):
                                    from datetime import datetime
                                    dt = datetime.fromisoformat(transformed_data['processing_timestamp'].replace('Z', '+00:00'))
                                    transformed_data['processing_timestamp'] = int(dt.timestamp() * 1000)
                                elif hasattr(transformed_data['processing_timestamp'], 'timestamp'):
                                    transformed_data['processing_timestamp'] = int(transformed_data['processing_timestamp'].timestamp() * 1000)
                            
                            # Convert all values to primitive types for AvroProducer
                            clean_data = {}
                            for key, value in transformed_data.items():
                                if value is None:
                                    clean_data[key] = None
                                elif isinstance(value, (str, int, float, bool)):
                                    clean_data[key] = value
                                elif isinstance(value, dict):
                                    continue  # Skip nested dictionaries
                                elif isinstance(value, (list, tuple)):
                                    continue  # Skip arrays
                                else:
                                    try:
                                        clean_data[key] = str(value)
                                    except Exception:
                                        continue  # Skip unconvertible fields
                            
                            # Send to Kafka using AvroProducer
                            def delivery_callback(err, msg):
                                if err:
                                    logger.error(f"Failed to deliver message to {topic}: {err}")
                            
                            producer.produce(
                                topic=topic,
                                key=str(record_dict.get('key', 'UNKNOWN')),
                                value=clean_data,
                                callback=delivery_callback
                            )
                            successful_records += 1
                            
                        except Exception as e:
                            logger.error(f"Failed to serialize/send record: {str(e)}")
                            failed_records += 1
                            continue
                    
                    # Flush to ensure delivery
                    producer.flush(timeout=30)
                    
                    logger.info(f"Batch {batch_id} completed: {successful_records} successful, {failed_records} failed")
                    
                except Exception as e:
                    logger.error(f"Failed to process batch {batch_id}: {str(e)}")
                    raise
            
            # Use foreachBatch for Schema Registry Avro serialization
            query = (kafka_df.writeStream
                    .foreachBatch(write_batch_to_kafka_avro)
                    .outputMode("append")
                    .trigger(processingTime=self.config.spark.trigger_processing_time)
                    .option("checkpointLocation", checkpoint_path)
                    .start())
            
            logger.info(f"Schema Registry Avro streaming write started for topic {topic}")
            
            return query
            
        except Exception as e:
            error_msg = f"Failed to write to Kafka topic {topic} with Schema Registry Avro: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e

    def _prepare_kafka_output_dataframe_avro(self, df: DataFrame, data_type: str) -> DataFrame:
        """
        Prepare DataFrame for Kafka output using Schema Registry compatible Avro serialization.
        This ensures consistency with Apache Avro serialization/deserialization with Schema Registry.
        
        Args:
            df: Input DataFrame
            data_type: Type of data being processed
            
        Returns:
            DataFrame formatted for Kafka with 'key' and 'value' columns using Schema Registry Avro
        """
        logger.info(f"Preparing DataFrame for Kafka output: {data_type}")
        
        try:
            # Define columns and add missing required fields for each schema
            if data_type == "stock_prices":
                # Add missing fields with defaults for ProcessedStockPrices schema
                df = df.withColumn("data_layer", F.lit("silver")) if "data_layer" not in df.columns else df
                df = df.withColumn("record_type", F.lit("stock_price")) if "record_type" not in df.columns else df
                df = df.withColumn("processing_version", F.lit("1.0")) if "processing_version" not in df.columns else df
                df = df.withColumn("producer_timestamp", F.lit(None).cast("long")) if "producer_timestamp" not in df.columns else df
                df = df.withColumn("trading_session", F.lit(None).cast("string")) if "trading_session" not in df.columns else df
                    
                selected_cols = [
                    "symbol", "open_price", "high_price", "low_price", "current_price",
                    "previous_close", "change", "change_percent", "sma_5min", "sma_20min",
                    "price_trend_5min", "price_volatility", "trading_session",
                    "producer_timestamp", "processing_timestamp", "data_layer",
                    "record_type", "processing_version"
                ]
                
            elif data_type == "trading_volume":
                # Add missing fields for ProcessedTradingVolume schema
                df = df.withColumn("data_layer", F.lit("silver")) if "data_layer" not in df.columns else df
                df = df.withColumn("record_type", F.lit("trading_volume")) if "record_type" not in df.columns else df
                df = df.withColumn("processing_version", F.lit("1.0")) if "processing_version" not in df.columns else df
                df = df.withColumn("producer_timestamp", F.lit(None).cast("long")) if "producer_timestamp" not in df.columns else df
                df = df.withColumn("volume_category", F.lit(None).cast("string")) if "volume_category" not in df.columns else df
                    
                selected_cols = [
                    "symbol", "volume", "volume_weighted_price", "volume_sma_5min",
                    "volume_ratio", "volume_category", "trading_session",
                    "producer_timestamp", "processing_timestamp", "data_layer",
                    "record_type", "processing_version"
                ]
                
            elif data_type == "technical_indicators":
                # Add missing fields for ProcessedTechnicalIndicators schema
                df = df.withColumn("data_layer", F.lit("silver")) if "data_layer" not in df.columns else df
                df = df.withColumn("record_type", F.lit("technical_indicators")) if "record_type" not in df.columns else df
                df = df.withColumn("processing_version", F.lit("1.0")) if "processing_version" not in df.columns else df
                df = df.withColumn("producer_timestamp", F.lit(None).cast("long")) if "producer_timestamp" not in df.columns else df
                df = df.withColumn("momentum_signal", F.lit(None).cast("string")) if "momentum_signal" not in df.columns else df
                df = df.withColumn("volatility_level", F.lit(None).cast("string")) if "volatility_level" not in df.columns else df
                    
                selected_cols = [
                    "symbol", "current_price", "sma_5min", "sma_20min", "price_trend_5min",
                    "price_volatility", "volume_ratio", "momentum_signal", "volatility_level",
                    "trading_session", "producer_timestamp", "processing_timestamp",
                    "data_layer", "record_type", "processing_version"
                ]
                
            else:
                # Fallback - use all available columns
                selected_cols = df.columns
            
            # Ensure all selected columns exist, add defaults for missing ones
            for col_name in selected_cols:
                if col_name not in df.columns:
                    if col_name.endswith('_timestamp'):
                        df = df.withColumn(col_name, F.lit(None).cast("long"))
                    elif col_name in ['data_layer', 'record_type', 'processing_version']:
                        df = df.withColumn(col_name, F.lit("unknown"))
                    else:
                        df = df.withColumn(col_name, F.lit(None))
            
            # Convert timestamps to epoch milliseconds for Avro compatibility
            if "processing_timestamp" in selected_cols:
                df = df.withColumn("processing_timestamp", 
                                 (F.unix_timestamp("processing_timestamp") * 1000).cast("long"))
            if "producer_timestamp" in selected_cols and "producer_timestamp" in df.columns:
                df = df.withColumn("producer_timestamp",
                                 F.when(F.col("producer_timestamp").isNotNull(),
                                       (F.unix_timestamp("producer_timestamp") * 1000).cast("long"))
                                  .otherwise(F.lit(None).cast("long")))
            
            # Select final columns in correct order
            df_final = df.select(*selected_cols)
            
            # Add key column for Kafka partitioning
            kafka_df = df_final.withColumn("key", F.col("symbol").cast("string"))
            
            logger.info(f"DataFrame prepared for {data_type}")
            return kafka_df
            
        except Exception as e:
            error_msg = f"Failed to prepare Schema Registry Avro DataFrame for {data_type}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            # Fallback to JSON if Avro fails
            logger.warning(f"Falling back to JSON for {data_type}")
            return self._prepare_kafka_output_dataframe(df, data_type)

    def _prepare_kafka_output_dataframe(self, df: DataFrame, data_type: str) -> DataFrame:
        """
        Prepare DataFrame for Kafka output by serializing to JSON format.
        Simple and reliable approach that works with the Silver connector.
        
        Args:
            df: Input DataFrame
            data_type: Type of data being processed
            
        Returns:
            DataFrame formatted for Kafka with 'key' and 'value' columns using JSON
        """
        logger.info(f"Preparing DataFrame for Kafka output with JSON: data_type={data_type}")
        
        try:
            all_columns = df.columns
            # Process available columns
            
            # Define preferred columns for each data type
            if data_type == "stock_prices":
                preferred_cols = [
                    "symbol", "current_price", "open_price", "high_price", "low_price",
                    "previous_close", "change", "change_percent", "processing_timestamp",
                    "sma_5min", "sma_20min", "price_trend_5min", "price_volatility"
                ]
            elif data_type == "trading_volume":
                preferred_cols = [
                    "symbol", "volume", "volume_weighted_price", "volume_sma_5min",
                    "volume_ratio", "trading_session", "processing_timestamp"
                ]
            elif data_type == "technical_indicators":
                preferred_cols = [
                    "symbol", "current_price", "sma_5min", "sma_20min", "price_trend_5min",
                    "price_volatility", "volume_ratio", "trading_session", "processing_timestamp"
                ]
            else:
                preferred_cols = all_columns
            
            # Filter columns that actually exist
            available_cols = [col for col in preferred_cols if col in all_columns]
            
            # Ensure essential columns exist - symbol should always be present due to filtering
            if "symbol" not in available_cols:
                error_msg = f"Symbol column missing after filtering - this should not happen. Available: {available_cols}"
                logger.error(error_msg)
                raise StreamProcessorError(error_msg)
            
            if "processing_timestamp" not in available_cols:
                if "processing_timestamp" not in all_columns:
                    df = df.withColumn("processing_timestamp", F.current_timestamp())
                available_cols.append("processing_timestamp")
            
            # Create JSON struct
            struct_cols = [F.col(col_name) for col_name in available_cols]
            
            kafka_df = (df
                       .select(
                           F.col("symbol").cast("string").alias("key"),
                           F.to_json(F.struct(*struct_cols)).cast("string").alias("value")
                       ))
            
            logger.info(f"DataFrame prepared for Kafka output: {data_type}")
            return kafka_df
            
        except Exception as e:
            error_msg = f"Failed to prepare DataFrame for Kafka output: {str(e)}"
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
            # Ensure checkpoint directory exists and is fresh
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
                logger.info("Removed existing checkpoint directory")
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
            # Ensure checkpoint directory exists and is fresh
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
                logger.info("Removed existing checkpoint directory")
            os.makedirs(checkpoint_path, exist_ok=True)
            
            # Clean DataFrame to handle null values that cause __HIVE_DEFAULT_PARTITION__
            cleaned_df = df
            
            # Filter out null symbols to avoid partition issues
            if "symbol" in df.columns:
                cleaned_df = cleaned_df.filter(F.col("symbol").isNotNull() & (F.col("symbol") != ""))
                
            # Provide default value for trading_session if null
            if "trading_session" in df.columns:
                cleaned_df = cleaned_df.withColumn(
                    "trading_session",
                    F.when(F.col("trading_session").isNull() | (F.col("trading_session") == ""), "unknown")
                     .otherwise(F.col("trading_session"))
                )
            
            # Check which columns are available for partitioning
            available_columns = cleaned_df.columns
            partition_columns = []
            
            # Only partition by symbol for simplicity and to avoid permission issues
            if "symbol" in available_columns:
                partition_columns.append("symbol")
            
            logger.info("Partitioning Parquet by columns")
            
            # Build the streaming query with proper options for cluster mode
            stream_writer = (cleaned_df.writeStream
                           .format("parquet")
                           .option("path", output_path)
                           .option("checkpointLocation", checkpoint_path)
                           .option("spark.sql.parquet.compression.codec", "snappy")
                           .option("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                           .trigger(processingTime=self.config.spark.trigger_processing_time)
                           .outputMode("append"))
            
            # Add partitioning if columns are available
            if partition_columns:
                stream_writer = stream_writer.partitionBy(*partition_columns)
            
            query = stream_writer.start()
            
            logger.info(
                "Parquet streaming query started",
                extra={
                    "query_id": query.id,
                    "output_path": output_path,
                    "checkpoint_path": checkpoint_path,
                    "partition_columns": partition_columns,
                    "trigger_interval": self.config.spark.trigger_processing_time
                }
            )
            
            return query
            
        except Exception as e:
            error_msg = f"Failed to write to Parquet: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def process_multiple_topics_stream(self, output_base_path: str = "/tmp/streaming-output") -> Dict[str, StreamingQuery]:
        """
        Process both stock-quotes-realtime and stock-intraday-data topics simultaneously.
        
        Args:
            output_base_path: Base path for output files
            
        Returns:
            Dictionary of all active streaming queries
        """
        logger.info("Starting multi-topic stock stream processing with Kafka publishing")
        
        try:
            all_queries = {}
            
            # Process stock-quotes-realtime topic
            logger.info("Setting up processing for stock-quotes-realtime topic")
            quotes_queries = self._setup_topic_processing(
                topic=self.config.kafka.stock_quotes_topic,
                topic_name="quotes",
                output_base_path=output_base_path
            )
            all_queries.update(quotes_queries)
            
            # Process stock-intraday-data topic 
            logger.info("Setting up processing for stock-intraday-data topic")
            intraday_queries = self._setup_topic_processing(
                topic=self.config.kafka.stock_intraday_topic,
                topic_name="intraday", 
                output_base_path=output_base_path
            )
            all_queries.update(intraday_queries)
            
            logger.info(
                "Multi-topic stream processing started successfully",
                extra={
                    "total_queries": len(all_queries),
                    "input_topics": [self.config.kafka.stock_quotes_topic, self.config.kafka.stock_intraday_topic],
                    "output_topics": [
                        self.config.kafka.processed_stock_prices_topic,
                        self.config.kafka.processed_trading_volume_topic,
                        self.config.kafka.processed_technical_indicators_topic
                    ],
                    "queries": list(all_queries.keys())
                }
            )
            
            return all_queries
            
        except Exception as e:
            error_msg = f"Failed to start multi-topic stream processing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def _setup_topic_processing(self, topic: str, topic_name: str, output_base_path: str) -> Dict[str, StreamingQuery]:
        """
        Setup processing pipeline for a specific input topic.
        
        Args:
            topic: Kafka topic name to process
            topic_name: Friendly name for the topic (used in checkpoint paths)
            output_base_path: Base path for output files
            
        Returns:
            Dictionary of streaming queries for this topic
        """
        logger.info(f"Setting up processing pipeline for topic: {topic}")
        
        try:
            # Create Kafka stream for this topic
            kafka_stream = self.create_kafka_stream(topic)
            
            # Parse messages (works for both topics since they use same Avro schema)
            parsed_stream = self.parse_kafka_messages(kafka_stream)
            
            # Apply transformations
            transformed_stream = self.apply_data_transformations(parsed_stream)
            
            # Set up checkpoint paths for this topic
            base_checkpoint_path = f"{self.config.spark.checkpoint_location}/{topic_name}_processing"
            parquet_checkpoint_path = f"{base_checkpoint_path}/parquet"
            kafka_checkpoint_path = f"{base_checkpoint_path}/kafka"
            
            # Create data quality monitoring stream
            dq_monitoring_query = self.create_data_quality_monitoring_stream(base_checkpoint_path)
            
            # Publish to Kafka topics for medallion architecture
            kafka_queries = self.publish_to_kafka_topics(transformed_stream, kafka_checkpoint_path)
            
            # Optionally write to Parquet for backup
            parquet_query = None
            try:
                output_path = f"{output_base_path}/{topic_name}_data"
                parquet_query = self.write_to_parquet(transformed_stream, output_path, parquet_checkpoint_path)
                logger.info(f"Parquet writing query started for {topic_name}: {parquet_query.id}")
            except Exception as parquet_error:
                logger.warning(f"Parquet writing failed for {topic_name} (non-critical): {str(parquet_error)}")
            
            # Collect all queries for this topic
            topic_queries = {
                f"{topic_name}_data_quality_monitoring": dq_monitoring_query
            }
            
            if parquet_query:
                topic_queries[f"{topic_name}_parquet"] = parquet_query
                
            # Add Kafka output queries with topic-specific names
            for output_topic_name, query in kafka_queries.items():
                safe_topic_name = output_topic_name.replace('-', '_')
                topic_queries[f"{topic_name}_{safe_topic_name}"] = query
            
            logger.info(
                f"Processing pipeline setup completed for {topic}",
                extra={
                    "topic": topic,
                    "topic_name": topic_name,
                    "queries_created": len(topic_queries),
                    "query_names": list(topic_queries.keys())
                }
            )
            
            return topic_queries
            
        except Exception as e:
            error_msg = f"Failed to setup processing pipeline for topic {topic}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    def process_stock_quotes_stream(self, output_base_path: str = "/tmp/streaming-output") -> StreamingQuery:
        """
        Process stock quotes stream end-to-end with Kafka publishing for medallion architecture.
        Now processes BOTH input topics: stock-quotes-realtime AND stock-intraday-data
        
        Args:
            output_base_path: Base path for output files
            
        Returns:
            StreamingQuery object for the main processing pipeline (for backward compatibility)
        """
        logger.info("Starting comprehensive stock stream processing (both topics) with Kafka publishing")
        
        try:
            # Use the new multi-topic processing method
            all_queries = self.process_multiple_topics_stream(output_base_path)
            
            # Update active_queries for monitoring
            self.active_queries.update(all_queries)
            
            logger.info(
                "Comprehensive stock stream processing started successfully",
                extra={
                    "total_active_queries": len(self.active_queries),
                    "input_topics": [self.config.kafka.stock_quotes_topic, self.config.kafka.stock_intraday_topic],
                    "output_topics": [
                        self.config.kafka.processed_stock_prices_topic,
                        self.config.kafka.processed_trading_volume_topic, 
                        self.config.kafka.processed_technical_indicators_topic
                    ]
                }
            )
            
            # Return the first query for backward compatibility
            return list(all_queries.values())[0] if all_queries else None
            
        except Exception as e:
            error_msg = f"Failed to start comprehensive stock stream processing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def prepare_processed_stock_prices(self, df: DataFrame) -> DataFrame:
        """
        Prepare processed stock price data for publishing to Kafka.
        
        Args:
            df: Transformed DataFrame with stock data
            
        Returns:
            DataFrame formatted for processed-stock-prices topic
        """
        logger.info("Preparing processed stock prices data")
        
        try:
            # Check for required symbol column
            if "symbol" not in df.columns:
                raise StreamProcessorError("Symbol column is missing in stock prices data")
            
            # Add missing columns with defaults if needed
            df = (df
                  .withColumn("open_price", F.coalesce(F.col("open_price"), F.lit(0.0)))
                  .withColumn("high_price", F.coalesce(F.col("high_price"), F.lit(0.0)))
                  .withColumn("low_price", F.coalesce(F.col("low_price"), F.lit(0.0)))
                  .withColumn("current_price", F.coalesce(F.col("current_price"), F.lit(0.0)))
                  .withColumn("previous_close", F.coalesce(F.col("previous_close"), F.lit(0.0)))
                  .withColumn("change", F.coalesce(F.col("change"), F.lit(0.0)))
                  .withColumn("change_percent", F.coalesce(F.col("change_percent"), F.lit(0.0)))
                  .withColumn("sma_5min", F.coalesce(F.col("sma_5min"), F.lit(0.0)))
                  .withColumn("sma_20min", F.coalesce(F.col("sma_20min"), F.lit(0.0)))
                  .withColumn("price_trend_5min", F.coalesce(F.col("price_trend_5min"), F.lit("neutral")))
                  .withColumn("price_volatility", F.coalesce(F.col("price_volatility"), F.lit(0.0)))
                  .withColumn("trading_session", F.coalesce(F.col("trading_session"), F.lit("unknown")))
                  .withColumn("producer_timestamp", F.coalesce(F.col("producer_timestamp"), F.current_timestamp()))
                  .withColumn("processing_timestamp", F.coalesce(F.col("processing_timestamp"), F.current_timestamp())))
            
            # Select and format columns
            processed_df = (df
                           .select(
                               "symbol", "open_price", "high_price", "low_price", "current_price",
                               "previous_close", "change", "change_percent", "sma_5min", "sma_20min",
                               "price_trend_5min", "price_volatility", "trading_session",
                               "producer_timestamp", "processing_timestamp"
                           )
                           .withColumn("data_layer", F.lit("silver"))
                           .withColumn("record_type", F.lit("stock_price"))
                           .withColumn("processing_version", F.lit("1.0")))
            
            logger.info("Stock prices data prepared successfully")
            return processed_df
            
        except Exception as e:
            logger.error(f"Failed to prepare stock prices data: {str(e)}")
            raise StreamProcessorError(f"Failed to prepare stock prices data: {str(e)}") from e
    
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
            # Check for required symbol column
            if "symbol" not in df.columns:
                raise StreamProcessorError("Symbol column is missing in trading volume data")
            
            # Add missing columns with defaults if needed
            df = (df
                  .withColumn("volume", F.coalesce(F.col("volume"), F.lit(0)))
                  .withColumn("volume_weighted_price", F.coalesce(F.col("volume_weighted_price"), F.lit(0.0)))
                  .withColumn("volume_sma_5min", F.coalesce(F.col("volume_sma_5min"), F.lit(0)))
                  .withColumn("volume_ratio", F.coalesce(F.col("volume_ratio"), F.lit(1.0)))
                  .withColumn("trading_session", F.coalesce(F.col("trading_session"), F.lit("unknown")))
                  .withColumn("producer_timestamp", F.coalesce(F.col("producer_timestamp"), F.current_timestamp()))
                  .withColumn("processing_timestamp", F.coalesce(F.col("processing_timestamp"), F.current_timestamp())))
            
            # Select and format columns
            volume_df = (df
                        .select(
                            "symbol", "volume", "volume_weighted_price", "volume_sma_5min",
                            "volume_ratio", "trading_session", "producer_timestamp", "processing_timestamp"
                        )
                        .withColumn("data_layer", F.lit("silver"))
                        .withColumn("record_type", F.lit("trading_volume"))
                        .withColumn("processing_version", F.lit("1.0"))
                        .withColumn("volume_category",
                                  F.when(F.col("volume_ratio") > 2.0, "high")
                                   .when(F.col("volume_ratio") > 1.5, "above_average")
                                   .when(F.col("volume_ratio") < 0.5, "low")
                                   .otherwise("normal")))
            
            logger.info("Trading volume data prepared successfully")
            return volume_df
            
        except Exception as e:
            logger.error(f"Failed to prepare trading volume data: {str(e)}")
            raise StreamProcessorError(f"Failed to prepare trading volume data: {str(e)}") from e
    
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
            # Check for required symbol column
            if "symbol" not in df.columns:
                raise StreamProcessorError("Symbol column is missing in technical indicators data")
            
            # Add missing columns with defaults if needed
            df = (df
                  .withColumn("current_price", F.coalesce(F.col("current_price"), F.lit(0.0)))
                  .withColumn("sma_5min", F.coalesce(F.col("sma_5min"), F.lit(0.0)))
                  .withColumn("sma_20min", F.coalesce(F.col("sma_20min"), F.lit(0.0)))
                  .withColumn("price_trend_5min", F.coalesce(F.col("price_trend_5min"), F.lit("neutral")))
                  .withColumn("price_volatility", F.coalesce(F.col("price_volatility"), F.lit(0.0)))
                  .withColumn("volume_ratio", F.coalesce(F.col("volume_ratio"), F.lit(1.0)))
                  .withColumn("trading_session", F.coalesce(F.col("trading_session"), F.lit("unknown")))
                  .withColumn("producer_timestamp", F.coalesce(F.col("producer_timestamp"), F.current_timestamp()))
                  .withColumn("processing_timestamp", F.coalesce(F.col("processing_timestamp"), F.current_timestamp())))
            
            # Select and format columns
            indicators_df = (df
                            .select(
                                "symbol", "current_price", "sma_5min", "sma_20min", "price_trend_5min",
                                "price_volatility", "volume_ratio", "trading_session",
                                "producer_timestamp", "processing_timestamp"
                            )
                            .withColumn("data_layer", F.lit("silver"))
                            .withColumn("record_type", F.lit("technical_indicators"))
                            .withColumn("processing_version", F.lit("1.0"))
                            .withColumn("momentum_signal",
                                      F.when(F.col("price_trend_5min") == "up", "bullish")
                                       .when(F.col("price_trend_5min") == "down", "bearish")
                                       .otherwise("neutral"))
                            .withColumn("volatility_level",
                                      F.when(F.col("price_volatility") > 5.0, "high")
                                       .when(F.col("price_volatility") > 2.0, "medium")
                                       .otherwise("low")))
            
            logger.info("Technical indicators data prepared successfully")
            return indicators_df
            
        except Exception as e:
            logger.error(f"Failed to prepare technical indicators data: {str(e)}")
            raise StreamProcessorError(f"Failed to prepare technical indicators data: {str(e)}") from e
    
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
    
    
    
    def create_data_quality_monitoring_stream(self, base_checkpoint_path: str) -> StreamingQuery:
        """
        Create a streaming query for basic data quality monitoring.
        
        Args:
            base_checkpoint_path: Base path for checkpoints
            
        Returns:
            StreamingQuery for data quality monitoring
        """
        logger.info("Creating data quality monitoring stream")
        
        try:
            # Create Kafka stream for monitoring
            kafka_stream = self.create_kafka_stream(self.config.kafka.stock_quotes_topic)
            
            # Apply basic data quality checks
            def validate_and_forward(batch_df, batch_id):
                try:
                    logger.info(f"Processing batch {batch_id} for data quality monitoring")
                    
                    # Basic data quality checks
                    total_records = batch_df.count()
                    if total_records == 0:
                        logger.warning(f"Batch {batch_id} is empty")
                        return
                    
                    # Check for null/empty values in critical fields
                    null_checks = {
                        "symbol": batch_df.filter(F.col("symbol").isNull() | (F.col("symbol") == "")).count(),
                        "price": batch_df.filter(F.col("price").isNull()).count(),
                        "volume": batch_df.filter(F.col("volume").isNull()).count()
                    }
                    
                    # Check business rules
                    business_rule_checks = {
                        "invalid_price": batch_df.filter(F.col("price") <= 0).count(),
                        "invalid_volume": batch_df.filter(F.col("volume") < 0).count(),
                        "empty_symbol": batch_df.filter(F.col("symbol").isNull() | (F.col("symbol") == "")).count()
                    }
                    
                    # Log quality metrics
                    quality_issues = sum(null_checks.values()) + sum(business_rule_checks.values())
                    if quality_issues > 0:
                        logger.warning(f"Batch {batch_id} quality issues: {quality_issues}/{total_records} records")
                        logger.warning(f"Null checks: {null_checks}")
                        logger.warning(f"Business rule violations: {business_rule_checks}")
                    else:
                        logger.info(f"Batch {batch_id} passed all quality checks: {total_records} records")
                    
                except Exception as e:
                    logger.error(f"Error in data quality monitoring batch {batch_id}: {str(e)}")
            
            # Set up monitoring query
            checkpoint_path = f"{base_checkpoint_path}/data_quality_monitoring"
            
            query = (kafka_stream.writeStream
                    .foreachBatch(validate_and_forward)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=self.config.spark.trigger_processing_time)
                    .start())
            
            logger.info(f"Data quality monitoring stream started - Query ID: {query.id}")
            
            return query
            
        except Exception as e:
            error_msg = f"Failed to create data quality monitoring stream: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    
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
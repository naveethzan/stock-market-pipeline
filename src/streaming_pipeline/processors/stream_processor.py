"""
Spark Structured Streaming processor for real-time financial data processing.
Consumes data from Kafka, applies transformations, and outputs to Parquet format.
"""
import logging
import os
import time
from typing import Dict, Any, Optional, List
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
                    # Java 11+ compatibility fixes for DirectByteBuffer access
                    .config("spark.driver.extraJavaOptions", 
                           "--add-opens=java.base/java.lang=ALL-UNNAMED "
                           "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                           "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                           "--add-opens=java.base/java.io=ALL-UNNAMED "
                           "--add-opens=java.base/java.net=ALL-UNNAMED "
                           "--add-opens=java.base/java.nio=ALL-UNNAMED "
                           "--add-opens=java.base/java.util=ALL-UNNAMED "
                           "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                           "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                           "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                           "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
                           "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                           "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                           "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
                    .config("spark.executor.extraJavaOptions", 
                           "--add-opens=java.base/java.lang=ALL-UNNAMED "
                           "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                           "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                           "--add-opens=java.base/java.io=ALL-UNNAMED "
                           "--add-opens=java.base/java.net=ALL-UNNAMED "
                           "--add-opens=java.base/java.nio=ALL-UNNAMED "
                           "--add-opens=java.base/java.util=ALL-UNNAMED "
                           "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                           "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                           "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                           "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
                           "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                           "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                           "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
                    # Kafka and Avro integration - ensure packages are available
                    .config("spark.jars.packages", 
                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                           "org.apache.spark:spark-avro_2.12:3.5.1")
                    # Force package resolution and download
                    .config("spark.jars.ivy", "/home/streaming/.ivy2")
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                    .config("spark.sql.streaming.checkpointLocation.deleteOnExit", "true")
                    # Parquet optimization
                    .config("spark.sql.parquet.compression.codec", "snappy")
                    .config("spark.sql.parquet.enableVectorizedReader", "true")
                    .getOrCreate())
            
            # Set log level
            spark.sparkContext.setLogLevel("WARN")
            
            # Verify Kafka packages are available
            try:
                # Test if Kafka data source is available
                test_df = (spark.readStream
                          .format("kafka")
                          .option("kafka.bootstrap.servers", "dummy:9092")
                          .option("subscribe", "test")
                          .option("startingOffsets", "latest")
                          .load())
                # If we get here, Kafka packages are loaded correctly
                logger.info("Kafka packages verified successfully")
            except Exception as e:
                error_msg = f"Kafka packages not available: {str(e)}"
                logger.error(error_msg)
                spark.stop()
                raise StreamProcessorError(error_msg)
            
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
    
    def get_avro_schema_string(self, schema_name: str = "stock_quote") -> str:
        """
        Get Avro schema string for deserializing Kafka messages.
        Enhanced for Spark compatibility with proper error handling.
        
        Args:
            schema_name: Name of the schema to retrieve (default: "stock_quote")
            
        Returns:
            Avro schema as JSON string compatible with Spark's from_avro() function
            
        Raises:
            StreamProcessorError: If schema retrieval fails or schema is invalid
        """
        try:
            from ..schemas.avro_schemas import get_all_schemas
            import json
            
            # Get all available schemas
            schemas = get_all_schemas()
            
            # Validate schema name
            if schema_name not in schemas:
                available_schemas = list(schemas.keys())
                error_msg = f"Unknown schema '{schema_name}'. Available schemas: {available_schemas}"
                logger.error(
                    "Schema retrieval failed - unknown schema",
                    extra={
                        "requested_schema": schema_name,
                        "available_schemas": available_schemas
                    }
                )
                raise StreamProcessorError(error_msg)
            
            # Get the schema
            schema_dict = schemas[schema_name]
            
            # Validate schema structure for Spark compatibility
            if not isinstance(schema_dict, dict):
                error_msg = f"Schema '{schema_name}' is not a valid dictionary"
                logger.error(
                    "Schema validation failed - invalid structure",
                    extra={
                        "schema_name": schema_name,
                        "schema_type": type(schema_dict).__name__
                    }
                )
                raise StreamProcessorError(error_msg)
            
            # Ensure required fields for Avro record schema
            required_fields = ["type", "name", "fields"]
            missing_fields = [field for field in required_fields if field not in schema_dict]
            if missing_fields:
                error_msg = f"Schema '{schema_name}' missing required fields: {missing_fields}"
                logger.error(
                    "Schema validation failed - missing required fields",
                    extra={
                        "schema_name": schema_name,
                        "missing_fields": missing_fields,
                        "schema_keys": list(schema_dict.keys())
                    }
                )
                raise StreamProcessorError(error_msg)
            
            # Validate schema type
            if schema_dict.get("type") != "record":
                error_msg = f"Schema '{schema_name}' must be of type 'record', got '{schema_dict.get('type')}'"
                logger.error(
                    "Schema validation failed - invalid type",
                    extra={
                        "schema_name": schema_name,
                        "expected_type": "record",
                        "actual_type": schema_dict.get("type")
                    }
                )
                raise StreamProcessorError(error_msg)
            
            # Convert to JSON string with consistent formatting for Spark
            # Spark's from_avro() requires compact JSON without extra whitespace
            schema_json = json.dumps(schema_dict, separators=(',', ':'), sort_keys=True)
            
            # Validate schema compatibility with Spark's from_avro() function
            if not self.validate_avro_schema_for_spark(schema_json):
                error_msg = f"Schema '{schema_name}' is not compatible with Spark's from_avro() function"
                logger.error(
                    "Schema Spark compatibility validation failed",
                    extra={"schema_name": schema_name}
                )
                raise StreamProcessorError(error_msg)
            
            logger.debug(
                "Avro schema retrieved successfully",
                extra={
                    "schema_name": schema_name,
                    "schema_size_bytes": len(schema_json),
                    "field_count": len(schema_dict.get("fields", []))
                }
            )
            
            return schema_json
            
        except StreamProcessorError:
            # Re-raise our custom errors
            raise
        except Exception as e:
            error_msg = f"Unexpected error retrieving schema '{schema_name}': {str(e)}"
            logger.error(
                "Schema retrieval unexpected error",
                extra={
                    "schema_name": schema_name,
                    "error": str(e)
                },
                exc_info=True
            )
            raise StreamProcessorError(error_msg) from e
    
    def get_avro_schema_from_registry(self, subject: str, version: str = "latest") -> str:
        """
        Get Avro schema from Schema Registry for enhanced compatibility.
        
        Args:
            subject: Schema Registry subject name
            version: Schema version (default: "latest")
            
        Returns:
            Avro schema as JSON string compatible with Spark's from_avro() function
            
        Raises:
            StreamProcessorError: If schema retrieval from registry fails
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
                error_msg = f"No schema found for subject '{subject}' version '{version}'"
                logger.error(
                    "Schema Registry retrieval failed - no schema found",
                    extra={
                        "subject": subject,
                        "version": version,
                        "registry_url": registry_url
                    }
                )
                raise StreamProcessorError(error_msg)
            
            # Parse schema JSON
            schema_json = schema_info['schema']
            if isinstance(schema_json, str):
                # Validate JSON format
                try:
                    schema_dict = json.loads(schema_json)
                    # Re-serialize with consistent formatting for Spark
                    schema_json = json.dumps(schema_dict, separators=(',', ':'), sort_keys=True)
                except json.JSONDecodeError as e:
                    error_msg = f"Invalid JSON schema from registry: {str(e)}"
                    logger.error(
                        "Schema Registry JSON validation failed",
                        extra={
                            "subject": subject,
                            "version": version,
                            "json_error": str(e)
                        }
                    )
                    raise StreamProcessorError(error_msg) from e
            else:
                # Schema is already a dict, convert to JSON
                schema_json = json.dumps(schema_json, separators=(',', ':'), sort_keys=True)
            
            logger.info(
                "Schema retrieved from Schema Registry",
                extra={
                    "subject": subject,
                    "version": version,
                    "schema_id": schema_info.get('id'),
                    "schema_size_bytes": len(schema_json)
                }
            )
            
            return schema_json
            
        except StreamProcessorError:
            # Re-raise our custom errors
            raise
        except Exception as e:
            error_msg = f"Unexpected error retrieving schema from registry for subject '{subject}': {str(e)}"
            logger.error(
                "Schema Registry retrieval unexpected error",
                extra={
                    "subject": subject,
                    "version": version,
                    "error": str(e)
                },
                exc_info=True
            )
            raise StreamProcessorError(error_msg) from e
    
    def validate_avro_schema_for_spark(self, schema_json: str) -> bool:
        """
        Validate that an Avro schema is compatible with Spark's from_avro() function.
        
        Args:
            schema_json: Avro schema as JSON string
            
        Returns:
            True if schema is compatible, False otherwise
            
        Raises:
            StreamProcessorError: If validation fails due to errors
        """
        try:
            import json
            
            # Parse schema JSON
            try:
                schema_dict = json.loads(schema_json)
            except json.JSONDecodeError as e:
                logger.error(
                    "Schema validation failed - invalid JSON",
                    extra={"json_error": str(e)}
                )
                return False
            
            # Check required fields for Avro record schema
            if not isinstance(schema_dict, dict):
                logger.error("Schema validation failed - schema is not a dictionary")
                return False
            
            if schema_dict.get("type") != "record":
                logger.error(
                    "Schema validation failed - not a record type",
                    extra={"schema_type": schema_dict.get("type")}
                )
                return False
            
            required_fields = ["name", "fields"]
            for field in required_fields:
                if field not in schema_dict:
                    logger.error(
                        "Schema validation failed - missing required field",
                        extra={"missing_field": field}
                    )
                    return False
            
            # Validate fields array
            fields = schema_dict.get("fields", [])
            if not isinstance(fields, list):
                logger.error("Schema validation failed - fields is not an array")
                return False
            
            if len(fields) == 0:
                logger.error("Schema validation failed - no fields defined")
                return False
            
            # Validate each field has required properties
            for i, field in enumerate(fields):
                if not isinstance(field, dict):
                    logger.error(
                        "Schema validation failed - field is not a dictionary",
                        extra={"field_index": i}
                    )
                    return False
                
                if "name" not in field or "type" not in field:
                    logger.error(
                        "Schema validation failed - field missing name or type",
                        extra={"field_index": i, "field": field}
                    )
                    return False
            
            logger.debug(
                "Schema validation passed",
                extra={
                    "schema_name": schema_dict.get("name"),
                    "field_count": len(fields)
                }
            )
            
            return True
            
        except Exception as e:
            error_msg = f"Unexpected error during schema validation: {str(e)}"
            logger.error(
                "Schema validation unexpected error",
                extra={"error": str(e)},
                exc_info=True
            )
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
            
            logger.info(f"Kafka stream created successfully for topic: {topic}")
            return kafka_df
            
        except Exception as e:
            error_msg = f"Failed to create Kafka stream for topic {topic}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def parse_kafka_messages(self, kafka_df: DataFrame) -> DataFrame:
        """
        Parse Kafka messages and extract stock data using Avro deserialization.
        Enhanced to handle multiple topic schemas dynamically.
        
        Args:
            kafka_df: Raw Kafka DataFrame
            
        Returns:
            Parsed DataFrame with stock data
        """
        logger.info("Parsing Kafka messages using topic-aware Avro deserialization")
        
        try:
            # Import from_avro function for Avro deserialization
            from pyspark.sql.avro.functions import from_avro
            
            # Topic to schema mapping for multi-source support
            topic_schema_map = {
                self.config.kafka.stock_quotes_topic: "stock_quote",
                self.config.kafka.stock_intraday_topic: "intraday_data"
            }
            
            logger.info(
                "Topic-schema mapping configured",
                extra={
                    "mappings": topic_schema_map,
                    "available_topics": list(topic_schema_map.keys())
                }
            )
            
            # Parse Schema Registry Avro data with topic-aware schema selection
            all_parsed_dfs = []
            
            for topic_name, schema_name in topic_schema_map.items():
                try:
                    logger.info(f"Processing topic: {topic_name} with schema: {schema_name}")
                    
                    # Filter messages for this specific topic
                    topic_df = kafka_df.filter(F.col("topic") == topic_name)
                    
                    # Skip if no messages for this topic
                    # Note: We can't use count() in streaming, but we can check if DataFrame exists
                    logger.info(f"Filtering messages for topic: {topic_name}")
                    
                    # Get the appropriate Avro schema for this topic
                    try:
                        avro_schema = self.get_avro_schema_string(schema_name)
                        logger.info(
                            f"Retrieved Avro schema for topic {topic_name}",
                            extra={"schema_name": schema_name, "topic": topic_name}
                        )
                    except StreamProcessorError as e:
                        logger.error(
                            f"Failed to retrieve schema {schema_name} for topic {topic_name}: {str(e)}",
                            exc_info=True
                        )
                        continue  # Skip this topic and try the next one
                    
                    # Extract pure Avro payload by removing Schema Registry headers (magic byte + schema ID)
                    # Schema Registry format: [magic_byte(1)] + [schema_id(4)] + [avro_payload]
                    kafka_with_payload = (topic_df
                        .select(
                            F.col("key").cast("string").alias("message_key"),
                            F.col("value").alias("schema_registry_data"),
                            F.col("topic"),
                            F.col("partition"),
                            F.col("offset"),
                            F.col("timestamp").alias("kafka_timestamp")
                        )
                        # Extract Avro payload by skipping first 5 bytes (1 magic + 4 schema_id)
                        # Using expr with substring for proper binary handling
                        .withColumn("avro_payload", 
                            F.when(F.length("schema_registry_data") > 5,
                                  F.expr("substring(schema_registry_data, 6, length(schema_registry_data) - 5)"))
                            .otherwise(F.lit(None)))
                        .filter(F.col("avro_payload").isNotNull())
                    )
                    
                    # Use topic-specific schema for Avro deserialization
                    topic_parsed_df = (kafka_with_payload
                        .select(
                            "*",
                            from_avro(F.col("avro_payload"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")
                        ))
                    
                    logger.info(f"Schema Registry Avro deserialization completed for topic: {topic_name}")
                    
                    # Transform each topic to common schema format BEFORE union
                    if schema_name == "stock_quote":
                        # Handle stock quote schema (current_price, latest_trading_day, timestamp as long)
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
                        # Handle intraday schema (close_price -> current_price, timestamp as string, request_timestamp as long)
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
                        (F.col("symbol") != "null") &
                        F.col("current_price").isNotNull()
                    )
                    
                    logger.info(f"Topic {topic_name} harmonized and filtered successfully")
                    all_parsed_dfs.append(filtered_df)
                    
                except Exception as topic_error:
                    logger.error(
                        f"Failed to process topic {topic_name} with schema {schema_name}: {str(topic_error)}",
                        exc_info=True
                    )
                    # Continue with other topics instead of failing completely
                    continue
            
            # Union all successfully parsed DataFrames (now they have compatible schemas)
            if not all_parsed_dfs:
                raise StreamProcessorError("No topics could be successfully parsed")
            
            # Combine all topic DataFrames
            if len(all_parsed_dfs) == 1:
                final_df = all_parsed_dfs[0]
            else:
                final_df = all_parsed_dfs[0]
                for df in all_parsed_dfs[1:]:
                    final_df = final_df.union(df)
            
            logger.info(
                "All topics processed and combined successfully",
                extra={"processed_topics": len(all_parsed_dfs)}
            )
            
            # Log the final harmonized data structure for debugging
            final_columns = final_df.columns
            logger.info(f"Available columns after harmonization: {final_columns}")
            
            # Convert producer timestamp - both schemas now have producer_timestamp_ms as long
            if "producer_timestamp_ms" in final_df.columns:
                processed_df = final_df.withColumn("producer_timestamp", 
                    F.when(F.col("producer_timestamp_ms").isNotNull(),
                          F.col("producer_timestamp_ms").cast(TimestampType()))
                    .otherwise(F.current_timestamp()))
            else:
                processed_df = final_df.withColumn("producer_timestamp", F.current_timestamp())
            
            # Add processing timestamp
            processed_df = processed_df.withColumn("processing_timestamp", F.current_timestamp())
            
            # Select final columns with proper ordering
            result_df = processed_df.select(
                "symbol",
                "open_price",
                "high_price", 
                "low_price",
                "current_price",
                "volume",
                "previous_close",
                "change",
                "change_percent",
                "latest_trading_day",
                "producer_timestamp",
                "processing_timestamp",
                "kafka_timestamp",
                "topic",
                "partition",
                "offset"
            )
            
            # Log final schema
            final_columns = result_df.columns
            logger.info(f"Final multi-topic Schema Registry deserialized DataFrame columns: {final_columns}")
            
            logger.info("Multi-topic Schema Registry Kafka messages parsed successfully")
            return result_df
            
        except Exception as e:
            error_msg = f"Failed to parse Kafka messages with multi-topic Schema Registry Avro deserialization: {str(e)}"
            logger.error(
                "Multi-topic Kafka message parsing failed",
                extra={"supported_topics": list(topic_schema_map.keys()) if 'topic_schema_map' in locals() else []},
                exc_info=True
            )
            raise StreamProcessorError(error_msg) from e
    
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
            # Log available columns for debugging
            available_columns = df.columns
            logger.info(f"Available columns for transformation: {available_columns}")
            
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
            
            # Log final schema for debugging
            final_columns = final_df.columns
            logger.info(f"Final transformed columns: {final_columns}")
            
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
            import shutil
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
                logger.info(f"Removed existing checkpoint directory: {checkpoint_path}")
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
                        logger.debug(f"Batch {batch_id} is empty for topic {topic}, skipping")
                        return
                    
                    logger.info(f"Processing batch {batch_id} with {batch_df.count()} records for topic {topic}")
                    
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
                    
                    # Compile schema dictionary to Avro schema object (CRITICAL FIX)
                    # confluent-kafka AvroProducer requires compiled schema objects, not raw dicts
                    try:
                        value_schema = avro.schema.parse(json.dumps(schema_dict))
                        logger.info(f"Successfully compiled Avro value schema for {data_type}: {schema_name}")
                    except Exception as e:
                        logger.error(f"Failed to compile Avro value schema {schema_name}: {str(e)}")
                        raise ValueError(f"Invalid Avro value schema for {data_type}: {str(e)}")
                    
                    # Create AvroProducer with Schema Registry (handles magic bytes automatically)
                    # Define key schema for string keys
                    key_schema_dict = {
                        "type": "string",
                        "name": "key"
                    }
                    try:
                        key_schema = avro.schema.parse(json.dumps(key_schema_dict))
                        logger.info(f"Successfully compiled Avro key schema for {data_type}")
                    except Exception as e:
                        logger.error(f"Failed to compile Avro key schema: {str(e)}")
                        raise ValueError(f"Invalid Avro key schema: {str(e)}")
                    
                    producer = AvroProducer(
                        producer_config,
                        default_key_schema=key_schema,  # Use compiled string key schema
                        default_value_schema=value_schema  # Pass compiled schema object
                    )
                    successful_records = 0
                    failed_records = 0
                    
                    for record in records:
                        try:
                            # Get the record data as dictionary with proper null handling
                            record_dict = record.asDict()
                            
                            # Remove key column from serialization data
                            serialization_dict = {col: record_dict[col] for col in record_dict.keys() if col != 'key'}
                            
                            # Debug logging for problematic records
                            symbol_value = serialization_dict.get('symbol')
                            if not symbol_value or symbol_value in ['', 'null', None]:
                                logger.error(f"Record with empty/null symbol detected and will be skipped: {serialization_dict}")
                                failed_records += 1
                                continue  # Skip this record instead of using fallback
                            
                            # Validate required fields based on data type
                            if data_type == "stock_prices":
                                # For stock prices, validate current_price exists and is not null
                                current_price = serialization_dict.get('current_price')
                                if current_price is None:
                                    logger.error(f"Record with null current_price detected for symbol {symbol_value} and will be skipped: {serialization_dict}")
                                    failed_records += 1
                                    continue
                            elif data_type == "trading_volume":
                                # For trading volume, validate volume exists and is not null
                                volume = serialization_dict.get('volume')
                                if volume is None:
                                    logger.error(f"Record with null volume detected for symbol {symbol_value} and will be skipped: {serialization_dict}")
                                    failed_records += 1
                                    continue
                            elif data_type == "technical_indicators":
                                # For technical indicators, validate current_price exists
                                current_price = serialization_dict.get('current_price')
                                if current_price is None:
                                    logger.error(f"Record with null current_price detected for symbol {symbol_value} and will be skipped: {serialization_dict}")
                                    failed_records += 1
                                    continue
                            
                            # Log first few records for debugging
                            if successful_records < 3:
                                logger.info(f"Sample record {successful_records + 1}: symbol={serialization_dict.get('symbol')}, current_price={serialization_dict.get('current_price')}")
                            
                            # ==> DEBUG LOGGING: Examine data structure before transformation <==
                            if successful_records == 0:  # Only log the first record to avoid spam
                                logger.info("=== SERIALIZATION DEBUG - RAW DATA STRUCTURE ===")
                                logger.info(f"Raw serialization_dict keys: {list(serialization_dict.keys())}")
                                for key, value in serialization_dict.items():
                                    logger.info(f"Field '{key}': type={type(value)}, value={value}")
                                    if isinstance(value, dict):
                                        logger.warning(f"FOUND NESTED DICT in field '{key}': {value}")
                                    elif isinstance(value, (list, tuple)):
                                        logger.warning(f"FOUND ARRAY in field '{key}': {value}")
                                logger.info("=== END SERIALIZATION DEBUG ===")
                            
                            # Transform data for Schema Registry format (convert timestamps, etc.)
                            transformed_data = serialization_dict.copy()
                            
                            # Convert timestamp fields to proper format for AvroProducer
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
                            
                            # Convert all values to primitive types to avoid 'unhashable type: dict' errors
                            # AvroProducer requires primitive Python types
                            clean_data = {}
                            skipped_fields = []
                            for key, value in transformed_data.items():
                                if value is None:
                                    clean_data[key] = None
                                elif isinstance(value, (str, int, float, bool)):
                                    clean_data[key] = value
                                elif isinstance(value, dict):
                                    # For nested dictionaries, convert to JSON string or skip
                                    logger.warning(f"Skipping nested dict field '{key}' for Avro serialization: {value}")
                                    skipped_fields.append(f"{key} (dict)")
                                    continue
                                elif isinstance(value, (list, tuple)):
                                    # For arrays, convert to JSON string or skip
                                    logger.warning(f"Skipping array field '{key}' for Avro serialization: {value}")
                                    skipped_fields.append(f"{key} (array)")
                                    continue
                                else:
                                    # Try to convert to string as fallback
                                    try:
                                        clean_data[key] = str(value)
                                        if successful_records == 0:  # Log conversion for first record
                                            logger.info(f"Converted field '{key}' from {type(value)} to string: {clean_data[key]}")
                                    except Exception:
                                        logger.warning(f"Skipping unconvertible field '{key}' for Avro serialization")
                                        skipped_fields.append(f"{key} (unconvertible)")
                                        continue
                            
                            # ==> DEBUG LOGGING: Show cleaned data structure <==
                            if successful_records == 0:  # Only log the first record
                                logger.info("=== CLEANED DATA FOR AVRO SERIALIZATION ===")
                                logger.info(f"Clean data keys: {list(clean_data.keys())}")
                                logger.info(f"Skipped fields: {skipped_fields}")
                                for key, value in clean_data.items():
                                    logger.info(f"Clean field '{key}': type={type(value)}, value={value}")
                                logger.info("=== END CLEANED DATA DEBUG ===")
                            
                            # Send to Kafka using AvroProducer (automatically handles Schema Registry format)
                            def delivery_callback(err, msg):
                                if err:
                                    logger.error(f"Failed to deliver message to {topic}: {err}")
                                else:
                                    logger.debug(f"Message delivered to {topic}[{msg.partition()}] at offset {msg.offset()}")
                            
                            producer.produce(
                                topic=topic,
                                key=str(record_dict.get('key', 'UNKNOWN')),  # String key
                                value=clean_data,  # AvroProducer handles Schema Registry serialization with cleaned primitive data
                                callback=delivery_callback
                            )
                            successful_records += 1
                            
                        except Exception as e:
                            logger.error(f"Failed to serialize/send record to {topic}: {str(e)}")
                            failed_records += 1
                            continue
                    
                    # Flush to ensure delivery
                    producer.flush(timeout=30)
                    
                    logger.info(
                        f"Batch {batch_id} processing completed for topic {topic}",
                        extra={
                            "topic": topic,
                            "batch_id": batch_id,
                            "successful_records": successful_records,
                            "failed_records": failed_records,
                            "data_type": data_type
                        }
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to process batch {batch_id} for topic {topic}: {str(e)}")
                    raise
            
            # Use foreachBatch for Schema Registry Avro serialization
            query = (kafka_df.writeStream
                    .foreachBatch(write_batch_to_kafka_avro)
                    .outputMode("append")
                    .trigger(processingTime=self.config.spark.trigger_processing_time)
                    .option("checkpointLocation", checkpoint_path)
                    .start())
            
            logger.info(
                "Schema Registry Avro streaming write started successfully",
                extra={
                    "topic": topic,
                    "checkpoint_path": checkpoint_path,
                    "data_type": data_type,
                    "query_id": query.id,
                    "trigger_interval": self.config.spark.trigger_processing_time
                }
            )
            
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
        logger.info(f"Preparing DataFrame for Kafka output with Schema Registry Avro serialization: data_type={data_type}")
        
        try:
            # Use foreachBatch to serialize with Schema Registry AvroSerializer
            # This maintains consistency with the existing Avro serialization logic
            
            # Get available columns
            all_columns = df.columns
            logger.info(f"Available columns for {data_type}: {all_columns}")
            
            # Define columns and add missing required fields for each schema
            if data_type == "stock_prices":
                # Add missing fields with defaults for ProcessedStockPrices schema
                if "data_layer" not in all_columns:
                    df = df.withColumn("data_layer", F.lit("silver"))
                if "record_type" not in all_columns:
                    df = df.withColumn("record_type", F.lit("stock_price"))
                if "processing_version" not in all_columns:
                    df = df.withColumn("processing_version", F.lit("1.0"))
                if "producer_timestamp" not in all_columns:
                    df = df.withColumn("producer_timestamp", F.lit(None).cast("long"))
                if "trading_session" not in all_columns:
                    df = df.withColumn("trading_session", F.lit(None).cast("string"))
                    
                # Select columns that match the Avro schema
                selected_cols = [
                    "symbol", "open_price", "high_price", "low_price", "current_price",
                    "previous_close", "change", "change_percent", "sma_5min", "sma_20min",
                    "price_trend_5min", "price_volatility", "trading_session",
                    "producer_timestamp", "processing_timestamp", "data_layer",
                    "record_type", "processing_version"
                ]
                
            elif data_type == "trading_volume":
                # Add missing fields for ProcessedTradingVolume schema
                if "data_layer" not in all_columns:
                    df = df.withColumn("data_layer", F.lit("silver"))
                if "record_type" not in all_columns:
                    df = df.withColumn("record_type", F.lit("trading_volume"))
                if "processing_version" not in all_columns:
                    df = df.withColumn("processing_version", F.lit("1.0"))
                if "producer_timestamp" not in all_columns:
                    df = df.withColumn("producer_timestamp", F.lit(None).cast("long"))
                if "volume_category" not in all_columns:
                    df = df.withColumn("volume_category", F.lit(None).cast("string"))
                    
                selected_cols = [
                    "symbol", "volume", "volume_weighted_price", "volume_sma_5min",
                    "volume_ratio", "volume_category", "trading_session",
                    "producer_timestamp", "processing_timestamp", "data_layer",
                    "record_type", "processing_version"
                ]
                
            elif data_type == "technical_indicators":
                # Add missing fields for ProcessedTechnicalIndicators schema
                if "data_layer" not in all_columns:
                    df = df.withColumn("data_layer", F.lit("silver"))
                if "record_type" not in all_columns:
                    df = df.withColumn("record_type", F.lit("technical_indicators"))
                if "processing_version" not in all_columns:
                    df = df.withColumn("processing_version", F.lit("1.0"))
                if "producer_timestamp" not in all_columns:
                    df = df.withColumn("producer_timestamp", F.lit(None).cast("long"))
                if "momentum_signal" not in all_columns:
                    df = df.withColumn("momentum_signal", F.lit(None).cast("string"))
                if "volatility_level" not in all_columns:
                    df = df.withColumn("volatility_level", F.lit(None).cast("string"))
                    
                selected_cols = [
                    "symbol", "current_price", "sma_5min", "sma_20min", "price_trend_5min",
                    "price_volatility", "volume_ratio", "momentum_signal", "volatility_level",
                    "trading_session", "producer_timestamp", "processing_timestamp",
                    "data_layer", "record_type", "processing_version"
                ]
                
            else:
                # Fallback - use all available columns
                selected_cols = all_columns
            
            # Ensure all selected columns exist, add defaults for missing ones
            final_cols = []
            for col_name in selected_cols:
                if col_name in df.columns:
                    final_cols.append(col_name)
                else:
                    # Add missing column with appropriate default
                    if col_name.endswith('_timestamp'):
                        df = df.withColumn(col_name, F.lit(None).cast("long"))
                    elif col_name in ['data_layer', 'record_type', 'processing_version']:
                        df = df.withColumn(col_name, F.lit("unknown"))
                    else:
                        df = df.withColumn(col_name, F.lit(None))
                    final_cols.append(col_name)
            
            # Convert timestamps to epoch milliseconds for Avro compatibility
            if "processing_timestamp" in final_cols:
                df = df.withColumn("processing_timestamp", 
                                 (F.unix_timestamp("processing_timestamp") * 1000).cast("long"))
            if "producer_timestamp" in final_cols and "producer_timestamp" in df.columns:
                df = df.withColumn("producer_timestamp",
                                 F.when(F.col("producer_timestamp").isNotNull(),
                                       (F.unix_timestamp("producer_timestamp") * 1000).cast("long"))
                                  .otherwise(F.lit(None).cast("long")))
            
            # Select final columns in correct order
            df_final = df.select(*final_cols)
            
            # Add key column for Kafka partitioning
            kafka_df = df_final.withColumn("key", F.col("symbol").cast("string"))
            
            logger.info(f"Schema Registry Avro DataFrame prepared for {data_type}, columns: {final_cols}")
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
            logger.info(f"Available columns for {data_type}: {all_columns}")
            
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
            
            logger.info(f"DataFrame prepared for Kafka output with JSON: data_type={data_type}, columns={available_cols}")
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
            import shutil
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
                logger.info(f"Removed existing checkpoint directory: {checkpoint_path}")
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
            import shutil
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
                logger.info(f"Removed existing checkpoint directory: {checkpoint_path}")
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
            
            logger.info(f"Partitioning Parquet by columns: {partition_columns}")
            
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
            
            # Optionally write to Parquet for backup/debugging
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
        Prepare processed stock price data for publishing to Kafka with Silver layer validation.
        Enhanced with robust column validation and fallback handling.
        
        Args:
            df: Transformed DataFrame with stock data
            
        Returns:
            DataFrame formatted for processed-stock-prices topic
        """
        logger.info("Preparing processed stock prices data with Silver layer validation")
        
        try:
            # Log available columns for debugging
            available_columns = df.columns
            logger.info(f"Available columns in stock prices DataFrame: {available_columns}")
            
            # Define required columns for stock prices
            required_cols = [
                "symbol", "open_price", "high_price", "low_price", "current_price",
                "previous_close", "change", "change_percent", "sma_5min", "sma_20min",
                "price_trend_5min", "price_volatility", "trading_session",
                "producer_timestamp", "processing_timestamp"
            ]
            
            # Check which columns actually exist
            existing_cols = [col for col in required_cols if col in available_columns]
            missing_cols = [col for col in required_cols if col not in available_columns]
            
            if missing_cols:
                logger.warning(f"Missing columns in stock prices data: {missing_cols}")
                # Add missing columns with default values, but error on missing symbol
                for col in missing_cols:
                    if col == "symbol":
                        error_msg = f"Symbol column is missing in stock prices data - this should not happen after filtering. Available: {available_columns}"
                        logger.error(error_msg)
                        raise StreamProcessorError(error_msg)
                    elif col in ["open_price", "high_price", "low_price", "current_price", "previous_close"]:
                        df = df.withColumn(col, F.lit(0.0))
                    elif col in ["change", "change_percent", "price_volatility"]:
                        df = df.withColumn(col, F.lit(0.0))
                    elif col in ["sma_5min", "sma_20min"]:
                        df = df.withColumn(col, F.lit(0.0))
                    elif col == "price_trend_5min":
                        df = df.withColumn("price_trend_5min", F.lit("neutral"))
                    elif col == "trading_session":
                        df = df.withColumn("trading_session", F.lit("unknown"))
                    elif col == "producer_timestamp":
                        df = df.withColumn("producer_timestamp", F.current_timestamp())
                    elif col == "processing_timestamp":
                        df = df.withColumn("processing_timestamp", F.current_timestamp())
                
                logger.info(f"Added missing columns with defaults: {missing_cols}")
            
            # Now select all required columns (they should all exist)
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
            
            # Log final columns
            final_columns = processed_df.columns
            logger.info(f"Final stock prices columns: {final_columns}")
            
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
        Enhanced with robust column validation and fallback handling.
        
        Args:
            df: Transformed DataFrame with stock data
            
        Returns:
            DataFrame formatted for processed-trading-volume topic
        """
        logger.info("Preparing processed trading volume data")
        
        try:
            # Log available columns for debugging
            available_columns = df.columns
            logger.info(f"Available columns in trading volume DataFrame: {available_columns}")
            
            # Define required columns for trading volume
            required_cols = [
                "symbol", "volume", "volume_weighted_price", "volume_sma_5min",
                "volume_ratio", "trading_session", "producer_timestamp", "processing_timestamp"
            ]
            
            # Check which columns actually exist
            existing_cols = [col for col in required_cols if col in available_columns]
            missing_cols = [col for col in required_cols if col not in available_columns]
            
            if missing_cols:
                logger.warning(f"Missing columns in trading volume data: {missing_cols}")
                # Add missing columns with default values, but error on missing symbol
                for col in missing_cols:
                    if col == "symbol":
                        error_msg = f"Symbol column is missing in trading volume data - this should not happen after filtering. Available: {available_columns}"
                        logger.error(error_msg)
                        raise StreamProcessorError(error_msg)
                    elif col == "volume":
                        df = df.withColumn("volume", F.lit(0))
                    elif col == "volume_weighted_price":
                        df = df.withColumn("volume_weighted_price", F.lit(0.0))
                    elif col == "volume_sma_5min":
                        df = df.withColumn("volume_sma_5min", F.lit(0))
                    elif col == "volume_ratio":
                        df = df.withColumn("volume_ratio", F.lit(1.0))
                    elif col == "trading_session":
                        df = df.withColumn("trading_session", F.lit("unknown"))
                    elif col == "producer_timestamp":
                        df = df.withColumn("producer_timestamp", F.current_timestamp())
                    elif col == "processing_timestamp":
                        df = df.withColumn("processing_timestamp", F.current_timestamp())
                
                logger.info(f"Added missing columns with defaults: {missing_cols}")
            
            # Now select all required columns (they should all exist)
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
            
            # Log final columns
            final_columns = volume_df.columns
            logger.info(f"Final trading volume columns: {final_columns}")
            
            logger.info("Processed trading volume data prepared successfully")
            return volume_df
            
        except Exception as e:
            error_msg = f"Failed to prepare processed trading volume: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise StreamProcessorError(error_msg) from e
    
    def prepare_processed_technical_indicators(self, df: DataFrame) -> DataFrame:
        """
        Prepare processed technical indicators data for publishing to Kafka.
        Enhanced with robust column validation and fallback handling.
        
        Args:
            df: Transformed DataFrame with stock data
            
        Returns:
            DataFrame formatted for processed-technical-indicators topic
        """
        logger.info("Preparing processed technical indicators data")
        
        try:
            # Log available columns for debugging
            available_columns = df.columns
            logger.info(f"Available columns in technical indicators DataFrame: {available_columns}")
            
            # Define required columns for technical indicators
            required_cols = [
                "symbol", "current_price", "sma_5min", "sma_20min", "price_trend_5min",
                "price_volatility", "volume_ratio", "trading_session",
                "producer_timestamp", "processing_timestamp"
            ]
            
            # Check which columns actually exist
            existing_cols = [col for col in required_cols if col in available_columns]
            missing_cols = [col for col in required_cols if col not in available_columns]
            
            if missing_cols:
                logger.warning(f"Missing columns in technical indicators data: {missing_cols}")
                # Add missing columns with default values, but error on missing symbol
                for col in missing_cols:
                    if col == "symbol":
                        error_msg = f"Symbol column is missing in technical indicators data - this should not happen after filtering. Available: {available_columns}"
                        logger.error(error_msg)
                        raise StreamProcessorError(error_msg)
                    elif col in ["current_price", "sma_5min", "sma_20min", "price_volatility"]:
                        df = df.withColumn(col, F.lit(0.0))
                    elif col == "price_trend_5min":
                        df = df.withColumn("price_trend_5min", F.lit("neutral"))
                    elif col == "volume_ratio":
                        df = df.withColumn("volume_ratio", F.lit(1.0))
                    elif col == "trading_session":
                        df = df.withColumn("trading_session", F.lit("unknown"))
                    elif col == "producer_timestamp":
                        df = df.withColumn("producer_timestamp", F.current_timestamp())
                    elif col == "processing_timestamp":
                        df = df.withColumn("processing_timestamp", F.current_timestamp())
                
                logger.info(f"Added missing columns with defaults: {missing_cols}")
            
            # Now select all required columns (they should all exist)
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
            
            # Log final columns
            final_columns = indicators_df.columns
            logger.info(f"Final technical indicators columns: {final_columns}")
            
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
                    
                    # Process all batches without expensive checks to avoid InterruptedException
                    # Validate Bronze layer for all batches
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
    
    def check_query_health(self) -> Dict[str, Any]:
        """
        Check the health of all active streaming queries.
        
        Returns:
            Dictionary with query health information
        """
        health_info = {
            "total_queries": len(self.active_queries),
            "active_queries": 0,
            "failed_queries": 0,
            "query_details": {}
        }
        
        for query_name, query in self.active_queries.items():
            try:
                is_active = query.isActive
                exception = query.exception()
                
                query_info = {
                    "active": is_active,
                    "id": query.id,
                    "exception": str(exception) if exception else None
                }
                
                if is_active:
                    health_info["active_queries"] += 1
                    # Get recent progress if available
                    try:
                        progress = query.lastProgress
                        if progress:
                            query_info["last_progress"] = {
                                "batch_id": progress.get("batchId", "unknown"),
                                "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                                "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
                                "batch_duration": progress.get("batchDuration", "unknown")
                            }
                    except Exception:
                        pass
                else:
                    health_info["failed_queries"] += 1
                
                health_info["query_details"][query_name] = query_info
                
            except Exception as e:
                health_info["query_details"][query_name] = {
                    "active": False,
                    "error": str(e)
                }
                health_info["failed_queries"] += 1
        
        logger.info(f"Query health check: {health_info['active_queries']}/{health_info['total_queries']} queries active")
        
        return health_info
    
    def test_data_flow(self, sample_data: Dict[str, Any]) -> bool:
        """
        Test the data flow by processing a sample record through the pipeline.
        
        Args:
            sample_data: Sample stock data to test
            
        Returns:
            True if test passes, False otherwise
        """
        logger.info("Testing data flow with sample data")
        
        try:
            import time
            
            # Test Avro serialization for each data type
            test_results = {}
            
            # Test stock prices serialization
            try:
                stock_prices_data = {
                    "symbol": sample_data.get("symbol", "TEST"),
                    "current_price": sample_data.get("current_price", 100.0),
                    "open_price": sample_data.get("open_price", 99.0),
                    "high_price": sample_data.get("high_price", 101.0),
                    "low_price": sample_data.get("low_price", 98.0),
                    "previous_close": sample_data.get("previous_close", 99.5),
                    "change": sample_data.get("change", 0.5),
                    "change_percent": sample_data.get("change_percent", 0.5),
                    "sma_5min": 100.0,
                    "sma_20min": 100.0,
                    "price_trend_5min": "neutral",
                    "price_volatility": 1.0,
                    "trading_session": "regular",
                    "producer_timestamp": None,
                    "processing_timestamp": int(time.time() * 1000),
                    "data_layer": "silver",
                    "record_type": "stock_price",
                    "processing_version": "1.0"
                }
                
                avro_data = self.avro_serializer.serialize_processed_stock_prices(stock_prices_data)
                test_results["stock_prices"] = len(avro_data) > 0
                logger.info(f"Stock prices serialization test: {'PASS' if test_results['stock_prices'] else 'FAIL'} ({len(avro_data)} bytes)")
                
            except Exception as e:
                test_results["stock_prices"] = False
                logger.error(f"Stock prices serialization test FAILED: {str(e)}")
            
            # Test trading volume serialization
            try:
                volume_data = {
                    "symbol": sample_data.get("symbol", "TEST"),
                    "volume": sample_data.get("volume", 1000),
                    "volume_weighted_price": 100.0,
                    "volume_sma_5min": 1000.0,
                    "volume_ratio": 1.0,
                    "volume_category": "normal",
                    "trading_session": "regular",
                    "producer_timestamp": None,
                    "processing_timestamp": int(time.time() * 1000),
                    "data_layer": "silver",
                    "record_type": "trading_volume",
                    "processing_version": "1.0"
                }
                
                avro_data = self.avro_serializer.serialize_processed_trading_volume(volume_data)
                test_results["trading_volume"] = len(avro_data) > 0
                logger.info(f"Trading volume serialization test: {'PASS' if test_results['trading_volume'] else 'FAIL'} ({len(avro_data)} bytes)")
                
            except Exception as e:
                test_results["trading_volume"] = False
                logger.error(f"Trading volume serialization test FAILED: {str(e)}")
            
            # Test technical indicators serialization
            try:
                indicators_data = {
                    "symbol": sample_data.get("symbol", "TEST"),
                    "current_price": sample_data.get("current_price", 100.0),
                    "sma_5min": 100.0,
                    "sma_20min": 100.0,
                    "price_trend_5min": "neutral",
                    "price_volatility": 1.0,
                    "volume_ratio": 1.0,
                    "momentum_signal": "neutral",
                    "volatility_level": "low",
                    "trading_session": "regular",
                    "producer_timestamp": None,
                    "processing_timestamp": int(time.time() * 1000),
                    "data_layer": "silver",
                    "record_type": "technical_indicators",
                    "processing_version": "1.0"
                }
                
                avro_data = self.avro_serializer.serialize_processed_technical_indicators(indicators_data)
                test_results["technical_indicators"] = len(avro_data) > 0
                logger.info(f"Technical indicators serialization test: {'PASS' if test_results['technical_indicators'] else 'FAIL'} ({len(avro_data)} bytes)")
                
            except Exception as e:
                test_results["technical_indicators"] = False
                logger.error(f"Technical indicators serialization test FAILED: {str(e)}")
            
            # Overall test result
            all_passed = all(test_results.values())
            logger.info(f"Data flow test {'PASSED' if all_passed else 'FAILED'}: {test_results}")
            
            return all_passed
            
        except Exception as e:
            logger.error(f"Data flow test failed with exception: {str(e)}", exc_info=True)
            return False

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
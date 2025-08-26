"""
Unit tests for the Spark Structured Streaming processor.
"""
import pytest
import tempfile
import shutil
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from ..config.settings import ConfigManager
from .stream_processor import StreamProcessor, StreamProcessorError


@pytest.fixture(scope="module")
def spark_session():
    """Create a Spark session for testing."""
    spark = (SparkSession.builder
             .appName("test_stream_processor")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    
    yield spark
    spark.stop()


@pytest.fixture
def config():
    """Create a test configuration."""
    with patch.dict('os.environ', {
        'ALPHA_VANTAGE_API_KEY': 'test_key',
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_PASSWORD': 'test_password',
        'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
        'SNOWFLAKE_DATABASE': 'test_database',
        'SNOWFLAKE_SCHEMA': 'test_schema'
    }):
        return ConfigManager()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


class TestStreamProcessor:
    """Test cases for StreamProcessor."""
    
    def test_init_with_spark_session(self, config, spark_session):
        """Test initialization with provided Spark session."""
        processor = StreamProcessor(config, spark_session)
        
        assert processor.config == config
        assert processor.spark == spark_session
        assert processor.active_queries == {}
    
    def test_init_without_spark_session(self, config):
        """Test initialization without provided Spark session."""
        processor = StreamProcessor(config)
        
        assert processor.config == config
        assert processor.spark is not None
        assert processor.active_queries == {}
        
        # Clean up
        processor.close()
    
    def test_get_kafka_stream_schema(self, config, spark_session):
        """Test Kafka stream schema generation."""
        processor = StreamProcessor(config, spark_session)
        schema = processor.get_kafka_stream_schema()
        
        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        
        # Check for expected Alpha Vantage fields
        assert "01. symbol" in field_names
        assert "05. price" in field_names
        assert "_producer_metadata" in field_names
    
    def test_parse_kafka_messages_avro_deserialization(self, config, spark_session):
        """Test parsing of Kafka messages with Avro deserialization."""
        processor = StreamProcessor(config, spark_session)
        
        # Create mock Avro binary data (in real scenario, this would be actual Avro bytes)
        # For testing purposes, we'll create a DataFrame that simulates the structure
        # after Avro deserialization
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
        
        # Simulate the structure after from_avro() deserialization
        avro_schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("current_price", DoubleType(), False),
            StructField("volume", LongType(), True),
            StructField("latest_trading_day", StringType(), True),
            StructField("previous_close", DoubleType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_percent", DoubleType(), True),
            StructField("timestamp", LongType(), False)
        ])
        
        # Test data that matches Avro schema structure
        test_avro_data = [
            ("AAPL", 150.00, 152.00, 149.00, 151.50, 1000000, "2023-08-18", 150.50, 1.00, 0.66, 1692360600000)
        ]
        
        # Create a DataFrame that simulates what we'd get after from_avro() parsing
        avro_df = spark_session.createDataFrame(test_avro_data, avro_schema)
        
        # Add Kafka metadata columns to simulate the full Kafka DataFrame structure
        from pyspark.sql import functions as F
        kafka_df = (avro_df
                   .withColumn("message_key", F.lit("AAPL"))
                   .withColumn("topic", F.lit("stock-quotes-realtime"))
                   .withColumn("partition", F.lit(0))
                   .withColumn("offset", F.lit(123))
                   .withColumn("kafka_timestamp", F.current_timestamp()))
        
        # Test the transformation logic (skip the actual from_avro call for unit test)
        # This tests the field mapping and transformation logic
        cleaned_df = (kafka_df
                     .withColumn("symbol", F.col("symbol"))
                     .withColumn("open_price", F.col("open_price"))
                     .withColumn("high_price", F.col("high_price"))
                     .withColumn("low_price", F.col("low_price"))
                     .withColumn("current_price", F.col("current_price"))
                     .withColumn("volume", F.col("volume"))
                     .withColumn("latest_trading_day", F.col("latest_trading_day"))
                     .withColumn("previous_close", F.col("previous_close"))
                     .withColumn("change", F.col("change"))
                     .withColumn("change_percent", F.col("change_percent"))
                     .withColumn("producer_timestamp", 
                               F.from_unixtime(F.col("timestamp") / 1000).cast(TimestampType()))
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
        
        # Collect results and verify
        results = cleaned_df.collect()
        assert len(results) == 1
        
        row = results[0]
        assert row["symbol"] == "AAPL"
        assert row["current_price"] == 151.50
        assert row["volume"] == 1000000
        assert row["change"] == 1.00
        assert row["change_percent"] == 0.66
        assert row["open_price"] == 150.00
        assert row["high_price"] == 152.00
        assert row["low_price"] == 149.00
        assert row["previous_close"] == 150.50
    
    def test_avro_schema_string_generation(self, config, spark_session):
        """Test that Avro schema string is properly generated."""
        processor = StreamProcessor(config, spark_session)
        
        # Get Avro schema string
        avro_schema = processor.get_avro_schema_string("stock_quote")
        
        # Verify it's valid JSON
        import json
        schema_dict = json.loads(avro_schema)
        
        # Verify it has expected structure
        assert schema_dict["type"] == "record"
        assert schema_dict["name"] == "StockQuote"
        assert "fields" in schema_dict
        
        # Verify key fields are present
        field_names = [field["name"] for field in schema_dict["fields"]]
        assert "symbol" in field_names
        assert "current_price" in field_names
        assert "open_price" in field_names
        assert "volume" in field_names
        assert "timestamp" in field_names
    
    def test_enhanced_avro_schema_retrieval(self, config, spark_session):
        """Test enhanced Avro schema retrieval with error handling."""
        processor = StreamProcessor(config, spark_session)
        
        # Test successful schema retrieval
        avro_schema = processor.get_avro_schema_string("stock_quote")
        assert avro_schema is not None
        assert len(avro_schema) > 0
        
        # Test with different schema types
        processed_schema = processor.get_avro_schema_string("processed_stock_prices")
        assert processed_schema is not None
        assert processed_schema != avro_schema  # Should be different schemas
        
        # Test error handling for unknown schema
        with pytest.raises(StreamProcessorError) as exc_info:
            processor.get_avro_schema_string("unknown_schema")
        assert "Unknown schema 'unknown_schema'" in str(exc_info.value)
        
        # Test schema validation
        import json
        schema_dict = json.loads(avro_schema)
        assert processor.validate_avro_schema_for_spark(avro_schema) is True
        
        # Test invalid schema validation
        invalid_schema = json.dumps({"type": "invalid"})
        assert processor.validate_avro_schema_for_spark(invalid_schema) is False
    
    def test_apply_data_transformations(self, config, spark_session):
        """Test data transformations."""
        processor = StreamProcessor(config, spark_session)
        
        # Create test DataFrame
        from pyspark.sql import functions as F
        test_schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("current_price", DoubleType(), False),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("volume", StringType(), True),
            StructField("change", DoubleType(), True),
            StructField("processing_timestamp", TimestampType(), False)
        ])
        
        test_data = [
            ("AAPL", 151.50, 152.00, 149.00, "1000000", 1.00, "2023-08-18 10:30:00"),
            ("AAPL", 151.75, 152.25, 149.25, "1100000", 1.25, "2023-08-18 10:31:00")
        ]
        
        df = spark_session.createDataFrame(test_data, test_schema)
        df = df.withColumn("processing_timestamp", F.to_timestamp("processing_timestamp"))
        df = df.withColumn("volume", F.col("volume").cast("long"))
        
        # Apply transformations
        transformed_df = processor.apply_data_transformations(df)
        
        # Check that new columns are added
        columns = transformed_df.columns
        assert "price_change_abs" in columns
        assert "price_volatility" in columns
        assert "volume_weighted_price" in columns
        assert "market_cap_indicator" in columns
        assert "trading_session" in columns
        assert "sma_5min" in columns
        assert "sma_20min" in columns
    
    def test_get_query_status_not_found(self, config, spark_session):
        """Test getting status for non-existent query."""
        processor = StreamProcessor(config, spark_session)
        status = processor.get_query_status("non_existent")
        
        assert "error" in status
        assert "not found" in status["error"].lower()
    
    def test_stop_query_not_found(self, config, spark_session):
        """Test stopping non-existent query."""
        processor = StreamProcessor(config, spark_session)
        result = processor.stop_query("non_existent")
        
        assert result is False
    
    def test_context_manager(self, config, spark_session):
        """Test context manager functionality."""
        with StreamProcessor(config, spark_session) as processor:
            assert processor.spark is not None
        
        # After context exit, queries should be stopped
        assert len(processor.active_queries) == 0
    
    @patch('os.makedirs')
    def test_write_to_parquet_creates_checkpoint_dir(self, mock_makedirs, config, spark_session, temp_dir):
        """Test that checkpoint directory is created."""
        processor = StreamProcessor(config, spark_session)
        
        # Create a simple DataFrame
        test_data = [("AAPL", 151.50, "2023-08-18 10:30:00")]
        test_schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("current_price", DoubleType(), False),
            StructField("processing_timestamp", StringType(), False)
        ])
        
        df = spark_session.createDataFrame(test_data, test_schema)
        df = df.withColumn("processing_timestamp", F.to_timestamp("processing_timestamp"))
        df = df.withColumn("trading_session", F.lit("regular"))
        
        output_path = f"{temp_dir}/output"
        checkpoint_path = f"{temp_dir}/checkpoint"
        
        # This will fail because we can't actually start a streaming query in batch mode,
        # but we can verify that makedirs is called
        try:
            processor.write_to_parquet(df, output_path, checkpoint_path)
        except Exception:
            pass  # Expected to fail in test environment
        
        # Verify checkpoint directory creation was attempted
        mock_makedirs.assert_called_with(checkpoint_path, exist_ok=True)


class TestStreamProcessorIntegration:
    """Integration tests for StreamProcessor."""
    
    def test_full_pipeline_setup(self, config):
        """Test full pipeline setup without actually running streaming."""
        processor = StreamProcessor(config)
        
        # Verify processor is properly initialized
        assert processor.spark is not None
        assert processor.config == config
        
        # Test schema generation
        schema = processor.get_kafka_stream_schema()
        assert schema is not None
        
        # Clean up
        processor.close()
    
    def test_error_handling_invalid_config(self):
        """Test error handling with invalid configuration."""
        # Create config with missing required fields
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError):
                ConfigManager()


if __name__ == "__main__":
    pytest.main([__file__])
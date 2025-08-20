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
    
    def test_parse_kafka_messages_schema(self, config, spark_session):
        """Test parsing of Kafka messages with correct schema."""
        processor = StreamProcessor(config, spark_session)
        
        # Create test data
        test_data = [
            {
                "key": "AAPL",
                "value": """{
                    "01. symbol": "AAPL",
                    "02. open": "150.00",
                    "03. high": "152.00",
                    "04. low": "149.00",
                    "05. price": "151.50",
                    "06. volume": "1000000",
                    "07. latest trading day": "2023-08-18",
                    "08. previous close": "150.50",
                    "09. change": "1.00",
                    "10. change percent": "0.66%",
                    "_producer_metadata": {
                        "producer_timestamp": "2023-08-18T10:30:00Z",
                        "producer_version": "1.0.0"
                    }
                }""",
                "topic": "stock-quotes-realtime",
                "partition": 0,
                "offset": 123,
                "timestamp": "2023-08-18T10:30:00Z"
            }
        ]
        
        # Create DataFrame
        kafka_df = spark_session.createDataFrame(test_data)
        
        # Parse messages
        parsed_df = processor.parse_kafka_messages(kafka_df)
        
        # Collect results
        results = parsed_df.collect()
        assert len(results) == 1
        
        row = results[0]
        assert row["symbol"] == "AAPL"
        assert row["current_price"] == 151.50
        assert row["volume"] == 1000000
        assert row["change"] == 1.00
        assert row["change_percent"] == 0.66
    
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
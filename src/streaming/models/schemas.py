from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType

# Schema for raw streaming data from Kafka/S3
raw_stream_schema = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("volume", IntegerType(), nullable=False),
    StructField("exchange", StringType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False)
])

# Schema for aggregated streaming data
aggregated_stream_schema = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("open_price", DoubleType(), nullable=False),
    StructField("high_price", DoubleType(), nullable=False),
    StructField("low_price", DoubleType(), nullable=False),
    StructField("close_price", DoubleType(), nullable=False),
    StructField("total_volume", LongType(), nullable=False),
    StructField("vwap", DoubleType(), nullable=True),  # Volume Weighted Average Price
    StructField("price_change", DoubleType(), nullable=True),
    StructField("batch_id", StringType(), nullable=False),
    StructField("processing_timestamp", TimestampType(), nullable=False)
])

# Schema for streaming data quality metrics
data_quality_metrics_schema = StructType([
    StructField("check_timestamp", TimestampType(), nullable=False),
    StructField("check_name", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("description", StringType(), nullable=True),
    StructField("records_processed", LongType(), nullable=True),
    StructField("anomaly_count", LongType(), nullable=True),
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("batch_id", StringType(), nullable=False)
])

# Schema for anomaly detection results
anomaly_schema = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("predicted_price", DoubleType(), nullable=True),
    StructField("anomaly_score", DoubleType(), nullable=True),
    StructField("is_anomaly", IntegerType(), nullable=True),  # 0 or 1
    StructField("processing_timestamp", TimestampType(), nullable=False),
    StructField("batch_id", StringType(), nullable=False)
])

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType

# Schema for raw batch data from S3
raw_batch_schema = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("date", StringType(), nullable=False),  # Will be cast to DateType
    StructField("open", DoubleType(), nullable=True),
    StructField("high", DoubleType(), nullable=True),
    StructField("low", DoubleType(), nullable=True),
    StructField("close", DoubleType(), nullable=True),
    StructField("volume", LongType(), nullable=True),
    StructField("adj_close", DoubleType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False)
])

# Schema for transformed data
transformed_batch_schema = StructType([
    StructField("stock_id", StringType(), nullable=False),
    StructField("trading_date", StringType(), nullable=False),  # Will be cast to DateType
    StructField("open_price", DoubleType(), nullable=True),
    StructField("high_price", DoubleType(), nullable=True),
    StructField("low_price", DoubleType(), nullable=True),
    StructField("close_price", DoubleType(), nullable=True),
    StructField("volume", LongType(), nullable=True),
    StructField("adjusted_close", DoubleType(), nullable=True),
    StructField("daily_return", DoubleType(), nullable=True),
    StructField("sma_5", DoubleType(), nullable=True),  # 5-day Simple Moving Average
    StructField("sma_20", DoubleType(), nullable=True),  # 20-day Simple Moving Average
    StructField("rsi_14", DoubleType(), nullable=True),  # 14-day RSI
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("batch_id", StringType(), nullable=False)
])

# Schema for data quality checks
data_quality_schema = StructType([
    StructField("check_timestamp", TimestampType(), nullable=False),
    StructField("check_name", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("description", StringType(), nullable=True),
    StructField("records_processed", IntegerType(), nullable=True),
    StructField("records_failed", IntegerType(), nullable=True),
    StructField("batch_id", StringType(), nullable=False)
])

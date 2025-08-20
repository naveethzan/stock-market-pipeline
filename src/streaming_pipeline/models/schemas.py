"""
Data schemas for streaming pipeline.
Defines Spark SQL schemas for various data structures used in the pipeline.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, LongType, BooleanType
)


# Schema for Alpha Vantage real-time quote data from Kafka
ALPHA_VANTAGE_QUOTE_SCHEMA = StructType([
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


# Schema for Alpha Vantage intraday data from Kafka
ALPHA_VANTAGE_INTRADAY_SCHEMA = StructType([
    StructField("Meta Data", StructType([
        StructField("1. Information", StringType(), nullable=True),
        StructField("2. Symbol", StringType(), nullable=False),
        StructField("3. Last Refreshed", StringType(), nullable=True),
        StructField("4. Interval", StringType(), nullable=True),
        StructField("5. Output Size", StringType(), nullable=True),
        StructField("6. Time Zone", StringType(), nullable=True)
    ]), nullable=True),
    
    # Time series data - dynamic field names based on interval
    StructField("Time Series (1min)", StructType([
        # This will be dynamically handled in processing
    ]), nullable=True),
    
    # Producer metadata
    StructField("_producer_metadata", StructType([
        StructField("producer_timestamp", StringType(), nullable=True),
        StructField("producer_version", StringType(), nullable=True),
        StructField("serialization_format", StringType(), nullable=True)
    ]), nullable=True)
])


# Schema for processed stock data (after parsing and transformation)
PROCESSED_STOCK_SCHEMA = StructType([
    # Basic stock information
    StructField("symbol", StringType(), nullable=False),
    StructField("open_price", DoubleType(), nullable=True),
    StructField("high_price", DoubleType(), nullable=True),
    StructField("low_price", DoubleType(), nullable=True),
    StructField("current_price", DoubleType(), nullable=False),
    StructField("volume", LongType(), nullable=True),
    StructField("previous_close", DoubleType(), nullable=True),
    StructField("change", DoubleType(), nullable=True),
    StructField("change_percent", DoubleType(), nullable=True),
    
    # Calculated fields
    StructField("price_change_abs", DoubleType(), nullable=True),
    StructField("price_volatility", DoubleType(), nullable=True),
    StructField("volume_weighted_price", DoubleType(), nullable=True),
    StructField("market_cap_indicator", StringType(), nullable=True),
    StructField("trading_session", StringType(), nullable=True),
    
    # Moving averages
    StructField("sma_5min", DoubleType(), nullable=True),
    StructField("sma_20min", DoubleType(), nullable=True),
    StructField("volume_sma_5min", DoubleType(), nullable=True),
    StructField("price_trend_5min", StringType(), nullable=True),
    StructField("volume_ratio", DoubleType(), nullable=True),
    
    # Timestamps
    StructField("producer_timestamp", TimestampType(), nullable=True),
    StructField("processing_timestamp", TimestampType(), nullable=False),
    StructField("kafka_timestamp", TimestampType(), nullable=True),
    
    # Kafka metadata
    StructField("topic", StringType(), nullable=True),
    StructField("partition", IntegerType(), nullable=True),
    StructField("offset", LongType(), nullable=True)
])


# Schema for aggregated streaming data (windowed aggregations)
AGGREGATED_STREAM_SCHEMA = StructType([
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
    StructField("price_change_percent", DoubleType(), nullable=True),
    StructField("volatility", DoubleType(), nullable=True),
    StructField("trade_count", LongType(), nullable=True),
    StructField("processing_timestamp", TimestampType(), nullable=False)
])


# Schema for data quality metrics
DATA_QUALITY_METRICS_SCHEMA = StructType([
    StructField("check_timestamp", TimestampType(), nullable=False),
    StructField("symbol", StringType(), nullable=True),
    StructField("check_name", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),  # PASS, FAIL, WARNING
    StructField("description", StringType(), nullable=True),
    StructField("records_processed", LongType(), nullable=True),
    StructField("anomaly_count", LongType(), nullable=True),
    StructField("error_count", LongType(), nullable=True),
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("processing_timestamp", TimestampType(), nullable=False)
])


# Schema for market events
MARKET_EVENT_SCHEMA = StructType([
    StructField("event_type", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("data", StructType([
        StructField("market", StringType(), nullable=True),
        StructField("session", StringType(), nullable=True),
        StructField("symbols_affected", StringType(), nullable=True),  # JSON array as string
        StructField("description", StringType(), nullable=True)
    ]), nullable=True),
    StructField("processing_timestamp", TimestampType(), nullable=False)
])


# Schema for anomaly detection results
ANOMALY_DETECTION_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("predicted_price", DoubleType(), nullable=True),
    StructField("anomaly_score", DoubleType(), nullable=True),
    StructField("is_anomaly", BooleanType(), nullable=True),
    StructField("anomaly_type", StringType(), nullable=True),  # price_spike, volume_spike, etc.
    StructField("confidence", DoubleType(), nullable=True),
    StructField("processing_timestamp", TimestampType(), nullable=False)
])


# Schema for technical indicators
TECHNICAL_INDICATORS_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("volume", LongType(), nullable=True),
    
    # Moving averages
    StructField("sma_5", DoubleType(), nullable=True),
    StructField("sma_10", DoubleType(), nullable=True),
    StructField("sma_20", DoubleType(), nullable=True),
    StructField("sma_50", DoubleType(), nullable=True),
    StructField("ema_12", DoubleType(), nullable=True),
    StructField("ema_26", DoubleType(), nullable=True),
    
    # Technical indicators
    StructField("rsi_14", DoubleType(), nullable=True),
    StructField("macd", DoubleType(), nullable=True),
    StructField("macd_signal", DoubleType(), nullable=True),
    StructField("macd_histogram", DoubleType(), nullable=True),
    StructField("bollinger_upper", DoubleType(), nullable=True),
    StructField("bollinger_lower", DoubleType(), nullable=True),
    StructField("bollinger_middle", DoubleType(), nullable=True),
    
    # Volume indicators
    StructField("volume_sma_20", LongType(), nullable=True),
    StructField("volume_ratio", DoubleType(), nullable=True),
    
    StructField("processing_timestamp", TimestampType(), nullable=False)
])


def get_schema_by_name(schema_name: str) -> StructType:
    """
    Get schema by name.
    
    Args:
        schema_name: Name of the schema to retrieve
        
    Returns:
        StructType schema
        
    Raises:
        ValueError: If schema name is not found
    """
    schemas = {
        "alpha_vantage_quote": ALPHA_VANTAGE_QUOTE_SCHEMA,
        "alpha_vantage_intraday": ALPHA_VANTAGE_INTRADAY_SCHEMA,
        "processed_stock": PROCESSED_STOCK_SCHEMA,
        "aggregated_stream": AGGREGATED_STREAM_SCHEMA,
        "data_quality_metrics": DATA_QUALITY_METRICS_SCHEMA,
        "market_event": MARKET_EVENT_SCHEMA,
        "anomaly_detection": ANOMALY_DETECTION_SCHEMA,
        "technical_indicators": TECHNICAL_INDICATORS_SCHEMA
    }
    
    if schema_name not in schemas:
        available_schemas = ", ".join(schemas.keys())
        raise ValueError(f"Schema '{schema_name}' not found. Available schemas: {available_schemas}")
    
    return schemas[schema_name]


def validate_dataframe_schema(df, expected_schema: StructType, strict: bool = False) -> bool:
    """
    Validate that a DataFrame matches the expected schema.
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected StructType schema
        strict: If True, requires exact match. If False, allows additional columns
        
    Returns:
        True if schema is valid, False otherwise
    """
    try:
        df_fields = {field.name: field.dataType for field in df.schema.fields}
        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        
        if strict:
            return df_fields == expected_fields
        else:
            # Check that all expected fields are present with correct types
            for field_name, field_type in expected_fields.items():
                if field_name not in df_fields:
                    return False
                if df_fields[field_name] != field_type:
                    return False
            return True
            
    except Exception:
        return False
"""
Avro schemas for streaming pipeline data structures.
Defines schemas for stock quotes, intraday data, and market events.
"""
import json
from typing import Dict, Any


# Stock Quote Schema (Real-time quotes from GLOBAL_QUOTE)
STOCK_QUOTE_SCHEMA = {
    "type": "record",
    "name": "StockQuote",
    "namespace": "com.streaming.pipeline.stock",
    "doc": "Real-time stock quote from Alpha Vantage GLOBAL_QUOTE API",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol (e.g., AAPL)"
        },
        {
            "name": "open_price",
            "type": ["null", "double"],
            "default": None,
            "doc": "Opening price for the trading day"
        },
        {
            "name": "high_price", 
            "type": ["null", "double"],
            "default": None,
            "doc": "Highest price for the trading day"
        },
        {
            "name": "low_price",
            "type": ["null", "double"], 
            "default": None,
            "doc": "Lowest price for the trading day"
        },
        {
            "name": "current_price",
            "type": "double",
            "doc": "Current/latest price"
        },
        {
            "name": "volume",
            "type": ["null", "long"],
            "default": None,
            "doc": "Trading volume"
        },
        {
            "name": "latest_trading_day",
            "type": ["null", "string"],
            "default": None,
            "doc": "Latest trading day (YYYY-MM-DD)"
        },
        {
            "name": "previous_close",
            "type": ["null", "double"],
            "default": None,
            "doc": "Previous closing price"
        },
        {
            "name": "change",
            "type": ["null", "double"],
            "default": None,
            "doc": "Price change from previous close"
        },
        {
            "name": "change_percent",
            "type": ["null", "double"],
            "default": None,
            "doc": "Percentage change from previous close"
        },
        {
            "name": "timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            },
            "doc": "Timestamp when quote was retrieved (epoch millis)"
        },
        {
            "name": "producer_metadata",
            "type": {
                "type": "record",
                "name": "ProducerMetadata",
                "fields": [
                    {
                        "name": "producer_timestamp",
                        "type": "string",
                        "doc": "ISO timestamp when message was produced"
                    },
                    {
                        "name": "producer_version",
                        "type": "string",
                        "default": "1.0.0",
                        "doc": "Version of the producer"
                    },
                    {
                        "name": "data_source",
                        "type": "string",
                        "default": "alpha_vantage",
                        "doc": "Source of the data"
                    }
                ]
            },
            "doc": "Metadata about message production"
        }
    ]
}

# Intraday Data Point Schema
INTRADAY_DATA_POINT_SCHEMA = {
    "type": "record",
    "name": "IntradayDataPoint",
    "namespace": "com.streaming.pipeline.stock",
    "doc": "Single intraday data point from Alpha Vantage TIME_SERIES_INTRADAY",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol"
        },
        {
            "name": "timestamp",
            "type": "string",
            "doc": "Timestamp of the data point (YYYY-MM-DD HH:MM:SS)"
        },
        {
            "name": "open_price",
            "type": "double",
            "doc": "Opening price for the interval"
        },
        {
            "name": "high_price",
            "type": "double", 
            "doc": "Highest price for the interval"
        },
        {
            "name": "low_price",
            "type": "double",
            "doc": "Lowest price for the interval"
        },
        {
            "name": "close_price",
            "type": "double",
            "doc": "Closing price for the interval"
        },
        {
            "name": "volume",
            "type": "long",
            "doc": "Trading volume for the interval"
        },
        {
            "name": "interval",
            "type": "string",
            "doc": "Time interval (1min, 5min, etc.)"
        },
        {
            "name": "request_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            },
            "doc": "When this data was requested from API"
        },
        {
            "name": "producer_metadata",
            "type": {
                "type": "record",
                "name": "ProducerMetadata",
                "fields": [
                    {
                        "name": "producer_timestamp",
                        "type": "string",
                        "doc": "ISO timestamp when message was produced"
                    },
                    {
                        "name": "producer_version",
                        "type": "string",
                        "default": "1.0.0",
                        "doc": "Version of the producer"
                    },
                    {
                        "name": "data_source",
                        "type": "string",
                        "default": "alpha_vantage",
                        "doc": "Source of the data"
                    }
                ]
            },
            "doc": "Metadata about message production"
        }
    ]
}

# Market events removed - focusing only on stock data

# Processed Stock Prices Schema (Silver Layer)
PROCESSED_STOCK_PRICES_SCHEMA = {
    "type": "record",
    "name": "ProcessedStockPrices",
    "namespace": "com.streaming.pipeline.processed",
    "doc": "Processed stock price data for Silver layer",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol"
        },
        {
            "name": "open_price",
            "type": ["null", "double"],
            "default": None,
            "doc": "Opening price"
        },
        {
            "name": "high_price",
            "type": ["null", "double"],
            "default": None,
            "doc": "High price"
        },
        {
            "name": "low_price",
            "type": ["null", "double"],
            "default": None,
            "doc": "Low price"
        },
        {
            "name": "current_price",
            "type": "double",
            "doc": "Current price"
        },
        {
            "name": "previous_close",
            "type": ["null", "double"],
            "default": None,
            "doc": "Previous close price"
        },
        {
            "name": "change",
            "type": ["null", "double"],
            "default": None,
            "doc": "Price change"
        },
        {
            "name": "change_percent",
            "type": ["null", "double"],
            "default": None,
            "doc": "Percentage change"
        },
        {
            "name": "sma_5min",
            "type": ["null", "double"],
            "default": None,
            "doc": "5-minute simple moving average"
        },
        {
            "name": "sma_20min",
            "type": ["null", "double"],
            "default": None,
            "doc": "20-minute simple moving average"
        },
        {
            "name": "price_trend_5min",
            "type": ["null", "string"],
            "default": None,
            "doc": "Price trend over 5 minutes (up/down/neutral)"
        },
        {
            "name": "price_volatility",
            "type": ["null", "double"],
            "default": None,
            "doc": "Price volatility percentage"
        },
        {
            "name": "trading_session",
            "type": ["null", "string"],
            "default": None,
            "doc": "Trading session (regular/pre_market/after_hours)"
        },
        {
            "name": "producer_timestamp",
            "type": ["null", "long"],
            "default": None,
            "logicalType": "timestamp-millis",
            "doc": "Original producer timestamp"
        },
        {
            "name": "processing_timestamp",
            "type": "long",
            "logicalType": "timestamp-millis",
            "doc": "Processing timestamp"
        },
        {
            "name": "data_layer",
            "type": "string",
            "default": "silver",
            "doc": "Data layer (bronze/silver/gold)"
        },
        {
            "name": "record_type",
            "type": "string",
            "default": "stock_price",
            "doc": "Type of record"
        },
        {
            "name": "processing_version",
            "type": "string",
            "default": "1.0",
            "doc": "Processing version"
        }
    ]
}

# Processed Trading Volume Schema (Silver Layer)
PROCESSED_TRADING_VOLUME_SCHEMA = {
    "type": "record",
    "name": "ProcessedTradingVolume",
    "namespace": "com.streaming.pipeline.processed",
    "doc": "Processed trading volume data for Silver layer",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol"
        },
        {
            "name": "volume",
            "type": ["null", "long"],
            "default": None,
            "doc": "Trading volume"
        },
        {
            "name": "volume_weighted_price",
            "type": ["null", "double"],
            "default": None,
            "doc": "Volume weighted price"
        },
        {
            "name": "volume_sma_5min",
            "type": ["null", "double"],
            "default": None,
            "doc": "5-minute volume moving average"
        },
        {
            "name": "volume_ratio",
            "type": ["null", "double"],
            "default": None,
            "doc": "Volume ratio compared to average"
        },
        {
            "name": "volume_category",
            "type": ["null", "string"],
            "default": None,
            "doc": "Volume category (high/above_average/normal/low)"
        },
        {
            "name": "trading_session",
            "type": ["null", "string"],
            "default": None,
            "doc": "Trading session"
        },
        {
            "name": "producer_timestamp",
            "type": ["null", "long"],
            "default": None,
            "logicalType": "timestamp-millis",
            "doc": "Original producer timestamp"
        },
        {
            "name": "processing_timestamp",
            "type": "long",
            "logicalType": "timestamp-millis",
            "doc": "Processing timestamp"
        },
        {
            "name": "data_layer",
            "type": "string",
            "default": "silver",
            "doc": "Data layer"
        },
        {
            "name": "record_type",
            "type": "string",
            "default": "trading_volume",
            "doc": "Type of record"
        },
        {
            "name": "processing_version",
            "type": "string",
            "default": "1.0",
            "doc": "Processing version"
        }
    ]
}

# Processed Technical Indicators Schema (Silver Layer)
PROCESSED_TECHNICAL_INDICATORS_SCHEMA = {
    "type": "record",
    "name": "ProcessedTechnicalIndicators",
    "namespace": "com.streaming.pipeline.processed",
    "doc": "Processed technical indicators for Silver layer",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol"
        },
        {
            "name": "current_price",
            "type": "double",
            "doc": "Current price"
        },
        {
            "name": "sma_5min",
            "type": ["null", "double"],
            "default": None,
            "doc": "5-minute simple moving average"
        },
        {
            "name": "sma_20min",
            "type": ["null", "double"],
            "default": None,
            "doc": "20-minute simple moving average"
        },
        {
            "name": "price_trend_5min",
            "type": ["null", "string"],
            "default": None,
            "doc": "Price trend over 5 minutes"
        },
        {
            "name": "price_volatility",
            "type": ["null", "double"],
            "default": None,
            "doc": "Price volatility percentage"
        },
        {
            "name": "volume_ratio",
            "type": ["null", "double"],
            "default": None,
            "doc": "Volume ratio"
        },
        {
            "name": "momentum_signal",
            "type": ["null", "string"],
            "default": None,
            "doc": "Momentum signal (bullish/bearish/neutral)"
        },
        {
            "name": "volatility_level",
            "type": ["null", "string"],
            "default": None,
            "doc": "Volatility level (high/medium/low)"
        },
        {
            "name": "trading_session",
            "type": ["null", "string"],
            "default": None,
            "doc": "Trading session"
        },
        {
            "name": "producer_timestamp",
            "type": ["null", "long"],
            "default": None,
            "logicalType": "timestamp-millis",
            "doc": "Original producer timestamp"
        },
        {
            "name": "processing_timestamp",
            "type": "long",
            "logicalType": "timestamp-millis",
            "doc": "Processing timestamp"
        },
        {
            "name": "data_layer",
            "type": "string",
            "default": "silver",
            "doc": "Data layer"
        },
        {
            "name": "record_type",
            "type": "string",
            "default": "technical_indicators",
            "doc": "Type of record"
        },
        {
            "name": "processing_version",
            "type": "string",
            "default": "1.0",
            "doc": "Processing version"
        }
    ]
}

# Data Quality Alert Schema
DATA_QUALITY_ALERT_SCHEMA = {
    "type": "record",
    "name": "DataQualityAlert",
    "namespace": "com.streaming.pipeline.quality",
    "doc": "Data quality alert message",
    "fields": [
        {
            "name": "timestamp",
            "type": "long",
            "logicalType": "timestamp-millis",
            "doc": "Alert timestamp"
        },
        {
            "name": "layer",
            "type": "string",
            "doc": "Data layer (bronze/silver/gold)"
        },
        {
            "name": "rule_name",
            "type": "string",
            "doc": "Name of the validation rule"
        },
        {
            "name": "severity",
            "type": "string",
            "doc": "Alert severity (ERROR/WARNING/INFO)"
        },
        {
            "name": "message",
            "type": "string",
            "doc": "Alert message"
        },
        {
            "name": "failure_rate",
            "type": "double",
            "doc": "Failure rate (0.0 to 1.0)"
        },
        {
            "name": "failed_count",
            "type": "long",
            "doc": "Number of failed records"
        },
        {
            "name": "total_count",
            "type": "long",
            "doc": "Total number of records"
        },
        {
            "name": "topic",
            "type": ["null", "string"],
            "default": None,
            "doc": "Source topic if applicable"
        },
        {
            "name": "data_type",
            "type": ["null", "string"],
            "default": None,
            "doc": "Type of data being validated"
        }
    ]
}

# Schema registry mapping
SCHEMA_REGISTRY_SUBJECTS = {
    "stock-quotes-realtime-value": STOCK_QUOTE_SCHEMA,
    "stock-intraday-data-value": INTRADAY_DATA_POINT_SCHEMA,
    "processed-stock-prices-value": PROCESSED_STOCK_PRICES_SCHEMA,
    "processed-trading-volume-value": PROCESSED_TRADING_VOLUME_SCHEMA,
    "processed-technical-indicators-value": PROCESSED_TECHNICAL_INDICATORS_SCHEMA,
    "data-quality-alerts-value": DATA_QUALITY_ALERT_SCHEMA
}


def get_schema_json(schema_name: str) -> str:
    """
    Get schema as JSON string for Schema Registry registration.
    
    Args:
        schema_name: Name of the schema
        
    Returns:
        JSON string representation of the schema
    """
    schemas = get_all_schemas()
    
    if schema_name not in schemas:
        raise ValueError(f"Unknown schema: {schema_name}. Available: {list(schemas.keys())}")
    
    return json.dumps(schemas[schema_name], indent=2)


def get_all_schemas() -> Dict[str, Any]:
    """Get all schemas as a dictionary."""
    return {
        "stock_quote": STOCK_QUOTE_SCHEMA,
        "intraday_data": INTRADAY_DATA_POINT_SCHEMA,
        "processed_stock_prices": PROCESSED_STOCK_PRICES_SCHEMA,
        "processed_trading_volume": PROCESSED_TRADING_VOLUME_SCHEMA,
        "processed_technical_indicators": PROCESSED_TECHNICAL_INDICATORS_SCHEMA,
        "data_quality_alert": DATA_QUALITY_ALERT_SCHEMA
    }


# Schema evolution example removed for simplicity
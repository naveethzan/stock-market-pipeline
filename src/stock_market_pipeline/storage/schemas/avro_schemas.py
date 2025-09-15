"""
Avro schemas for streaming pipeline data structures.
Defines schemas for stock quotes, intraday data, and processed data.
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
            "type": "long",
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
                        "doc": "Data source identifier"
                    }
                ]
            },
            "doc": "Producer metadata"
        }
    ]
}

# Intraday Data Point Schema (Individual data points from TIME_SERIES_INTRADAY)
INTRADAY_DATA_POINT_SCHEMA = {
    "type": "record",
    "name": "IntradayDataPoint",
    "namespace": "com.streaming.pipeline.stock",
    "doc": "Individual intraday data point from Alpha Vantage TIME_SERIES_INTRADAY API",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol (e.g., AAPL)"
        },
        {
            "name": "timestamp",
            "type": "long",
            "doc": "Data point timestamp (epoch millis)"
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
            "doc": "Time interval (1min, 5min, 15min, 30min, 60min)"
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
                        "doc": "Data source identifier"
                    }
                ]
            },
            "doc": "Producer metadata"
        }
    ]
}

# Processed Stock Prices Schema (Silver layer - processed stock data)
PROCESSED_STOCK_PRICES_SCHEMA = {
    "type": "record",
    "name": "ProcessedStockPrices",
    "namespace": "com.streaming.pipeline.processed",
    "doc": "Processed stock prices with technical indicators",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol"
        },
        {
            "name": "timestamp",
            "type": "long",
            "doc": "Processing timestamp"
        },
        {
            "name": "open_price",
            "type": "double",
            "doc": "Opening price"
        },
        {
            "name": "high_price",
            "type": "double",
            "doc": "Highest price"
        },
        {
            "name": "low_price",
            "type": "double",
            "doc": "Lowest price"
        },
        {
            "name": "close_price",
            "type": "double",
            "doc": "Closing price"
        },
        {
            "name": "volume",
            "type": "long",
            "doc": "Trading volume"
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
            "doc": "Price volatility measure"
        },
        {
            "name": "trading_session",
            "type": ["null", "string"],
            "default": None,
            "doc": "Trading session identifier"
        },
        {
            "name": "producer_timestamp",
            "type": "long",
            "doc": "Original producer timestamp"
        },
        {
            "name": "processing_timestamp",
            "type": "long",
            "doc": "Processing timestamp"
        },
        {
            "name": "vwap",
            "type": ["null", "double"],
            "default": None,
            "doc": "Volume Weighted Average Price"
        },
        {
            "name": "price_change_abs",
            "type": ["null", "double"],
            "default": None,
            "doc": "Absolute price change"
        },
        {
            "name": "price_momentum",
            "type": ["null", "double"],
            "default": None,
            "doc": "Price momentum from previous close"
        },
        {
            "name": "data_quality_score",
            "type": ["null", "double"],
            "default": None,
            "doc": "Data quality score (0.0-1.0)"
        }
    ]
}

# Processed Trading Volume Schema (Silver layer - volume analysis)
PROCESSED_TRADING_VOLUME_SCHEMA = {
    "type": "record",
    "name": "ProcessedTradingVolume",
    "namespace": "com.streaming.pipeline.processed",
    "doc": "Processed trading volume analysis",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol"
        },
        {
            "name": "timestamp",
            "type": "long",
            "doc": "Processing timestamp"
        },
        {
            "name": "volume",
            "type": "long",
            "doc": "Trading volume"
        },
        {
            "name": "volume_ma_5min",
            "type": ["null", "double"],
            "default": None,
            "doc": "5-minute volume moving average"
        },
        {
            "name": "volume_ma_20min",
            "type": ["null", "double"],
            "default": None,
            "doc": "20-minute volume moving average"
        },
        {
            "name": "volume_trend",
            "type": ["null", "string"],
            "default": None,
            "doc": "Volume trend indicator"
        },
        {
            "name": "volume_anomaly",
            "type": ["null", "boolean"],
            "default": None,
            "doc": "Volume anomaly detection"
        },
        {
            "name": "producer_timestamp",
            "type": "long",
            "doc": "Original producer timestamp"
        },
        {
            "name": "processing_timestamp",
            "type": "long",
            "doc": "Processing timestamp"
        },
        {
            "name": "volume_ratio",
            "type": ["null", "double"],
            "default": None,
            "doc": "Volume ratio vs average"
        },
        {
            "name": "volume_weighted_price",
            "type": ["null", "double"],
            "default": None,
            "doc": "Volume weighted price"
        },
        {
            "name": "volume_category",
            "type": ["null", "string"],
            "default": None,
            "doc": "Volume category (high/medium/low)"
        }
    ]
}

# Processed Technical Indicators Schema (Silver layer - technical analysis)
PROCESSED_TECHNICAL_INDICATORS_SCHEMA = {
    "type": "record",
    "name": "ProcessedTechnicalIndicators",
    "namespace": "com.streaming.pipeline.processed",
    "doc": "Processed technical indicators",
    "fields": [
        {
            "name": "symbol",
            "type": "string",
            "doc": "Stock symbol"
        },
        {
            "name": "timestamp",
            "type": "long",
            "doc": "Processing timestamp"
        },
        {
            "name": "rsi_14",
            "type": ["null", "double"],
            "default": None,
            "doc": "14-period RSI"
        },
        {
            "name": "macd",
            "type": ["null", "double"],
            "default": None,
            "doc": "MACD line"
        },
        {
            "name": "macd_signal",
            "type": ["null", "double"],
            "default": None,
            "doc": "MACD signal line"
        },
        {
            "name": "macd_histogram",
            "type": ["null", "double"],
            "default": None,
            "doc": "MACD histogram"
        },
        {
            "name": "bollinger_upper",
            "type": ["null", "double"],
            "default": None,
            "doc": "Bollinger Bands upper"
        },
        {
            "name": "bollinger_lower",
            "type": ["null", "double"],
            "default": None,
            "doc": "Bollinger Bands lower"
        },
        {
            "name": "bollinger_middle",
            "type": ["null", "double"],
            "default": None,
            "doc": "Bollinger Bands middle"
        },
        {
            "name": "producer_timestamp",
            "type": "long",
            "doc": "Original producer timestamp"
        },
        {
            "name": "processing_timestamp",
            "type": "long",
            "doc": "Processing timestamp"
        }
    ]
}

SCHEMA_REGISTRY_SUBJECTS = {
    "stock-quotes-realtime-value": STOCK_QUOTE_SCHEMA,
    "stock-intraday-data-value": INTRADAY_DATA_POINT_SCHEMA,
    "processed-stock-prices-value": PROCESSED_STOCK_PRICES_SCHEMA,
    "processed-trading-volume-value": PROCESSED_TRADING_VOLUME_SCHEMA,
    "processed-technical-indicators-value": PROCESSED_TECHNICAL_INDICATORS_SCHEMA
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
        "processed_technical_indicators": PROCESSED_TECHNICAL_INDICATORS_SCHEMA
    }

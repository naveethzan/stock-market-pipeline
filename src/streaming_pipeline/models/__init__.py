"""
Data models package for streaming pipeline.
Contains schemas and transformations for streaming data processing.
"""

from .schemas import (
    ALPHA_VANTAGE_QUOTE_SCHEMA,
    ALPHA_VANTAGE_INTRADAY_SCHEMA,
    PROCESSED_STOCK_SCHEMA,
    AGGREGATED_STREAM_SCHEMA,
    DATA_QUALITY_METRICS_SCHEMA,
    MARKET_EVENT_SCHEMA,
    ANOMALY_DETECTION_SCHEMA,
    TECHNICAL_INDICATORS_SCHEMA,
    get_schema_by_name,
    validate_dataframe_schema
)

from .transformations import (
    StreamingTransformations,
    WindowedAggregations
)


__all__ = [
    # Schemas
    "ALPHA_VANTAGE_QUOTE_SCHEMA",
    "ALPHA_VANTAGE_INTRADAY_SCHEMA", 
    "PROCESSED_STOCK_SCHEMA",
    "AGGREGATED_STREAM_SCHEMA",
    "DATA_QUALITY_METRICS_SCHEMA",
    "MARKET_EVENT_SCHEMA",
    "ANOMALY_DETECTION_SCHEMA",
    "TECHNICAL_INDICATORS_SCHEMA",
    "get_schema_by_name",
    "validate_dataframe_schema",
    
    # Transformations
    "StreamingTransformations",
    "WindowedAggregations"
]
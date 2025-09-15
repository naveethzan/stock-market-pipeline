"""
Core constants for the stock market pipeline.
Centralizes all configuration values, magic strings, and business logic constants.
"""

from typing import Dict, List, Any


class APIConfig:
    """API configuration constants."""
    
    # Alpha Vantage API
    ALPHA_VANTAGE_BASE_URL: str = "https://www.alphavantage.co/query"
    ALPHA_VANTAGE_FUNCTIONS: Dict[str, str] = {
        "GLOBAL_QUOTE": "GLOBAL_QUOTE",
        "TIME_SERIES_INTRADAY": "TIME_SERIES_INTRADAY"
    }
    
    # API Rate Limits & Timeouts
    DEFAULT_RATE_LIMIT: int = 5
    DEFAULT_TIMEOUT: int = 30
    DEFAULT_RETRY_ATTEMPTS: int = 3
    DEFAULT_BACKOFF_FACTOR: float = 2.0
    
    # HTTP Methods
    HTTP_METHODS: List[str] = ["GET", "POST", "PUT", "DELETE"]


class KafkaConfig:
    """Kafka configuration constants."""
    
    # Security & Connection
    DEFAULT_SECURITY_PROTOCOL: str = "PLAINTEXT"
    DEFAULT_BOOTSTRAP_SERVERS: List[str] = ["localhost:9092"]
    
    # Producer Settings
    DEFAULT_PRODUCER_ACKS: str = "all"
    DEFAULT_PRODUCER_RETRIES: int = 3
    DEFAULT_PRODUCER_BATCH_SIZE: int = 16384
    DEFAULT_PRODUCER_LINGER_MS: int = 10
    DEFAULT_PRODUCER_COMPRESSION: str = "snappy"
    
    # Consumer Settings
    DEFAULT_CONSUMER_GROUP_ID: str = "streaming-pipeline"
    DEFAULT_CONSUMER_OFFSET_RESET: str = "latest"
    DEFAULT_CONSUMER_AUTO_COMMIT: bool = False
    DEFAULT_CONSUMER_MAX_POLL_RECORDS: int = 500


class Topics:
    """Kafka topic names."""
    
    # Bronze Layer Topics (Raw Data)
    STOCK_QUOTES: str = "stock-quotes-realtime"
    STOCK_INTRADAY: str = "stock-intraday-data"
    
    # Silver Layer Topics (Processed Data)
    PROCESSED_STOCK_PRICES: str = "processed-stock-prices"
    PROCESSED_TRADING_VOLUME: str = "processed-trading-volume"
    PROCESSED_TECHNICAL_INDICATORS: str = "processed-technical-indicators"
    
    # Gold Layer Topics (Alerts & Quality)
    DATA_QUALITY_ALERTS: str = "data-quality-alerts"


class SchemaNames:
    """Schema names for Avro and Spark schemas."""
    
    # Avro Schema Names
    STOCK_QUOTE: str = "stock_quote"
    INTRADAY_DATA: str = "intraday_data"
    PROCESSED_STOCK_PRICES: str = "processed_stock_prices"
    PROCESSED_TRADING_VOLUME: str = "processed_trading_volume"
    PROCESSED_TECHNICAL_INDICATORS: str = "processed_technical_indicators"
    DATA_QUALITY_ALERT: str = "data_quality_alert"
    
    # Spark Schema Names
    ALPHA_VANTAGE_QUOTE_SCHEMA: str = "alpha_vantage_quote"
    ALPHA_VANTAGE_INTRADAY_SCHEMA: str = "alpha_vantage_intraday"
    PROCESSED_STOCK_SCHEMA: str = "processed_stock"
    AGGREGATED_STREAM_SCHEMA: str = "aggregated_stream"
    DATA_QUALITY_METRICS_SCHEMA: str = "data_quality_metrics"
    MARKET_EVENT_SCHEMA: str = "market_event"
    ANOMALY_DETECTION_SCHEMA: str = "anomaly_detection"
    TECHNICAL_INDICATORS_SCHEMA: str = "technical_indicators"


class DataProcessing:
    """Data processing constants."""
    
    # Stock Symbols
    DEFAULT_STOCK_SYMBOLS: List[str] = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    
    # Processing Intervals
    INTERVALS: Dict[str, str] = {
        "1MIN": "1min",
        "5MIN": "5min",
        "20MIN": "20min"
    }
    
    # Data Layer Identifiers
    DATA_LAYERS: Dict[str, str] = {
        "BRONZE": "bronze",
        "SILVER": "silver",
        "GOLD": "gold"
    }
    
    # Record Types
    RECORD_TYPES: Dict[str, str] = {
        "STOCK_PRICE": "stock_price",
        "TRADING_VOLUME": "trading_volume",
        "TECHNICAL_INDICATORS": "technical_indicators",
        "DATA_QUALITY_ALERT": "data_quality_alert"
    }


class ServiceEndpoints:
    """Service endpoint constants."""
    
    # Schema Registry
    DEFAULT_SCHEMA_REGISTRY_URL: str = "http://schema-registry:8081"
    
    # Default URLs
    DEFAULT_KAFKA_URL: str = "localhost:9092"
    DEFAULT_REDSHIFT_PORT: int = 5439


class ErrorMessages:
    """Centralized error message constants."""
    
    # API Errors
    INVALID_SYMBOL: str = "Invalid stock symbol provided"
    API_RATE_LIMIT: str = "API rate limit exceeded"
    API_TIMEOUT: str = "API request timed out"
    API_CONNECTION_ERROR: str = "Failed to connect to API"
    
    # Data Validation Errors
    DATA_VALIDATION_FAILED: str = "Data validation failed"
    MISSING_REQUIRED_FIELD: str = "Missing required field"
    INVALID_DATA_TYPE: str = "Invalid data type"
    INVALID_DATA_FORMAT: str = "Invalid data format"
    
    # Processing Errors
    PROCESSING_FAILED: str = "Data processing failed"
    TRANSFORMATION_ERROR: str = "Data transformation error"
    AGGREGATION_ERROR: str = "Data aggregation error"
    
    # Storage Errors
    STORAGE_FAILED: str = "Data storage failed"
    CONNECTION_FAILED: str = "Connection to storage failed"
    SCHEMA_REGISTRY_ERROR: str = "Schema registry operation failed"
    
    # Configuration Errors
    CONFIGURATION_INVALID: str = "Configuration is invalid"
    MISSING_CONFIGURATION: str = "Required configuration missing"
    ENVIRONMENT_ERROR: str = "Environment configuration error"


class ProcessingConstants:
    """
    Processing-specific constants for stream processing operations.
    
    Contains time windows, thresholds, and business rules used throughout
    the data processing pipeline for technical indicators and aggregations.
    """
    
    # Time Windows
    WATERMARK_DELAY: str = "1 minute"
    TRIGGER_INTERVAL: str = "60 seconds"
    
    # Time windows (in seconds)
    SMA_5MIN_WINDOW: int = 300
    SMA_20MIN_WINDOW: int = 1200
    RSI_PERIOD: int = 840
    BB_PERIOD: int = 1200
    MACD_12_WINDOW: int = 720
    MACD_26_WINDOW: int = 1560
    MACD_9_WINDOW: int = 540
    ANOMALY_WINDOW: int = 1200
    
    # Thresholds
    Z_SCORE_THRESHOLD: float = 3.0
    MACD_CAP: float = 1000.0
    RSI_NEUTRAL: float = 50.0
    VOLUME_HIGH_THRESHOLD: int = 1000000
    VOLUME_MEDIUM_THRESHOLD: int = 100000
    VOLUME_RATIO_HIGH: float = 2.0
    VOLUME_RATIO_ABOVE_AVG: float = 1.5
    VOLUME_RATIO_LOW: float = 0.5
    
    # Default values
    PRICE_TREND_DEFAULT: str = "neutral"
    TRADING_SESSION_DEFAULT: str = "unknown"
    SYMBOL_UNKNOWN: str = "UNKNOWN"
    DATA_LAYER_DEFAULT: str = "silver"
    PROCESSING_VERSION: str = "1.0"
    RSI_NEUTRAL_DEFAULT: float = 50.0
    MACD_NEUTRAL: float = 0.0
    BOLLINGER_UPPER_MULTIPLIER: float = 1.02
    BOLLINGER_LOWER_MULTIPLIER: float = 0.98
    
    # Business rules
    MARKET_CAP_LARGE: int = 1000000
    MARKET_CAP_MEDIUM: int = 100000
    VOLUME_HIGH: int = 1000000
    VOLUME_MEDIUM: int = 100000
    PRICE_BUCKET_SIZE: float = 0.01
    
    # Batch Processing
    DEFAULT_BATCH_SIZE: int = 1000
    DEFAULT_WATERMARK_DELAY: str = "1 minute"
    DEFAULT_TRIGGER_INTERVAL: str = "60 seconds"
    
    # File Paths
    DEFAULT_CHECKPOINT_LOCATION: str = "/tmp/spark-checkpoints"
    DEFAULT_OUTPUT_BASE_PATH: str = "/tmp/streaming-output"


class ProcessingConfig:
    """
    Processing configuration helper class.
    
    Provides static methods to retrieve configuration dictionaries
    for time windows, thresholds, defaults, and business rules.
    """
    
    @staticmethod
    def get_time_windows() -> Dict[str, int]:
        """Get time window configurations."""
        return {
            "sma_5min": ProcessingConstants.SMA_5MIN_WINDOW,
            "sma_20min": ProcessingConstants.SMA_20MIN_WINDOW,
            "rsi_period": ProcessingConstants.RSI_PERIOD,
            "bb_period": ProcessingConstants.BB_PERIOD,
            "macd_12": ProcessingConstants.MACD_12_WINDOW,
            "macd_26": ProcessingConstants.MACD_26_WINDOW,
            "macd_9": ProcessingConstants.MACD_9_WINDOW,
            "anomaly_window": ProcessingConstants.ANOMALY_WINDOW
        }
    
    @staticmethod
    def get_thresholds() -> Dict[str, float]:
        """Get threshold configurations."""
        return {
            "z_score_threshold": ProcessingConstants.Z_SCORE_THRESHOLD,
            "macd_cap": ProcessingConstants.MACD_CAP,
            "rsi_neutral": ProcessingConstants.RSI_NEUTRAL,
            "volume_high_threshold": ProcessingConstants.VOLUME_HIGH_THRESHOLD,
            "volume_medium_threshold": ProcessingConstants.VOLUME_MEDIUM_THRESHOLD,
            "volume_ratio_high": ProcessingConstants.VOLUME_RATIO_HIGH,
            "volume_ratio_above_avg": ProcessingConstants.VOLUME_RATIO_ABOVE_AVG,
            "volume_ratio_low": ProcessingConstants.VOLUME_RATIO_LOW
        }
    
    @staticmethod
    def get_defaults() -> Dict[str, Any]:
        """Get default value configurations."""
        return {
            "price_trend": ProcessingConstants.PRICE_TREND_DEFAULT,
            "trading_session": ProcessingConstants.TRADING_SESSION_DEFAULT,
            "symbol_unknown": ProcessingConstants.SYMBOL_UNKNOWN,
            "data_layer": ProcessingConstants.DATA_LAYER_DEFAULT,
            "processing_version": ProcessingConstants.PROCESSING_VERSION,
            "rsi_neutral": ProcessingConstants.RSI_NEUTRAL_DEFAULT,
            "macd_neutral": ProcessingConstants.MACD_NEUTRAL,
            "bollinger_upper_multiplier": ProcessingConstants.BOLLINGER_UPPER_MULTIPLIER,
            "bollinger_lower_multiplier": ProcessingConstants.BOLLINGER_LOWER_MULTIPLIER
        }
    
    @staticmethod
    def get_business_rules() -> Dict[str, Any]:
        """Get business rule configurations."""
        return {
            "market_cap_large": ProcessingConstants.MARKET_CAP_LARGE,
            "market_cap_medium": ProcessingConstants.MARKET_CAP_MEDIUM,
            "volume_high": ProcessingConstants.VOLUME_HIGH,
            "volume_medium": ProcessingConstants.VOLUME_MEDIUM,
            "price_bucket_size": ProcessingConstants.PRICE_BUCKET_SIZE
        }


class SchemaRegistrySubjects:
    """Schema Registry subject names."""
    
    SUBJECTS: Dict[str, str] = {
        "STOCK_QUOTES_VALUE": "stock-quotes-realtime-value",
        "STOCK_INTRADAY_VALUE": "stock-intraday-data-value",
        "PROCESSED_STOCK_PRICES_VALUE": "processed-stock-prices-value",
        "PROCESSED_TRADING_VOLUME_VALUE": "processed-trading-volume-value",
        "PROCESSED_TECHNICAL_INDICATORS_VALUE": "processed-technical-indicators-value",
        "DATA_QUALITY_ALERTS_VALUE": "data-quality-alerts-value"
    }


class DataQuality:
    """Data quality constants."""
    
    # Validation Rules
    VALIDATION_RULES: Dict[str, str] = {
        "PRICE_VALIDATION": "price_validation",
        "VOLUME_VALIDATION": "volume_validation",
        "TIMESTAMP_VALIDATION": "timestamp_validation",
        "SYMBOL_VALIDATION": "symbol_validation"
    }
    
    # Alert Severities
    SEVERITIES: Dict[str, str] = {
        "ERROR": "ERROR",
        "WARNING": "WARNING",
        "INFO": "INFO"
    }
    
    # Quality Metrics
    METRICS: Dict[str, str] = {
        "SUCCESS_RATE": "success_rate",
        "FAILURE_RATE": "failure_rate",
        "ANOMALY_COUNT": "anomaly_count",
        "ERROR_COUNT": "error_count"
    }


class TechnicalIndicators:
    """Technical indicators constants."""
    
    # Moving Averages
    MOVING_AVERAGES: Dict[str, int] = {
        "SMA_5": 5,
        "SMA_10": 10,
        "SMA_20": 20,
        "SMA_50": 50,
        "EMA_12": 12,
        "EMA_26": 26
    }
    
    # Technical Indicator Names
    INDICATORS: Dict[str, str] = {
        "RSI": "rsi",
        "MACD": "macd",
        "BOLLINGER_BANDS": "bollinger_bands",
        "VOLUME_RATIO": "volume_ratio"
    }
    
    # Trend Categories
    TRENDS: Dict[str, str] = {
        "UP": "up",
        "DOWN": "down",
        "NEUTRAL": "neutral"
    }
    
    # Volatility Levels
    VOLATILITY_LEVELS: Dict[str, str] = {
        "HIGH": "high",
        "MEDIUM": "medium",
        "LOW": "low"
    }


class MockData:
    """Mock data constants for testing."""
    
    # Mock API Keys
    MOCK_API_KEY: str = "mock_api_key"
    
    # Mock Database Values
    MOCK_DATABASE: str = "mock_database"
    MOCK_USER: str = "mock_user"
    MOCK_PASSWORD: str = "mock_password"
    MOCK_ROLE: str = "mock_role"
    
    # Mock S3 Values
    MOCK_BUCKET: str = "mock_bucket"
    MOCK_PREFIX: str = "mock_prefix"
    MOCK_REGION: str = "us-east-1"


__all__ = [
    "APIConfig",
    "KafkaConfig", 
    "Topics",
    "SchemaNames",
    "DataProcessing",
    "ServiceEndpoints",
    "ErrorMessages",
    "ProcessingConstants",
    "ProcessingConfig",
    "SchemaRegistrySubjects",
    "DataQuality",
    "TechnicalIndicators",
    "MockData"
]

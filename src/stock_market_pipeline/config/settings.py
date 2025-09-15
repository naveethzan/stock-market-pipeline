"""
Configuration classes for the stock market pipeline.
Pure data structures with no business logic.
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class APIConfig:
    """
    API configuration for external data sources.
    
    Contains settings for Alpha Vantage API including authentication,
    rate limiting, timeouts, and retry behavior.
    """
    api_key: str
    base_url: str
    rate_limit: int
    timeout: int
    retry_attempts: int
    backoff_factor: float

@dataclass
class KafkaConfig:
    """
    Kafka configuration for message streaming.
    
    Contains producer and consumer settings for Apache Kafka
    including bootstrap servers, security, and performance tuning.
    """
    bootstrap_servers: List[str]
    security_protocol: str
    producer_acks: str
    producer_retries: int
    producer_batch_size: int
    producer_linger_ms: int
    producer_compression: str
    consumer_group_id: str
    consumer_offset_reset: str
    consumer_auto_commit: bool
    consumer_max_poll_records: int

@dataclass
class RedshiftConfig:
    """Redshift configuration."""
    endpoint: str
    database: str
    user: str
    password: str
    port: int
    connection_timeout: int
    network_timeout: int

@dataclass
class SparkConfig:
    """Spark configuration."""
    app_name: str
    master: str
    checkpoint_location: str
    trigger_interval: str
    watermark_delay: str
    driver_memory: str
    executor_memory: str
    executor_cores: int
    max_result_size: str

@dataclass
class S3Config:
    """S3 configuration."""
    bucket: str
    prefix: str
    region: str

@dataclass
class Config:
    """
    Main configuration container for the entire pipeline.
    
    Aggregates all service configurations including API, Kafka,
    Redshift, Spark, and S3 settings along with pipeline metadata.
    """
    mock_mode: bool
    api: APIConfig
    kafka: KafkaConfig
    redshift: RedshiftConfig
    spark: SparkConfig
    s3: S3Config
    stock_symbols: List[str]
    schema_registry_url: str

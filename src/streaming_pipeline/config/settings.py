"""
Configuration management for streaming pipeline.
Handles API keys, Kafka, and Redshift connections.
"""
import os
from typing import Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AlphaVantageConfig:
    """Alpha Vantage API configuration."""
    api_key: str
    base_url: str = "https://www.alphavantage.co/query"
    rate_limit_per_minute: int = 5
    timeout_seconds: int = 30
    retry_attempts: int = 3
    retry_backoff_factor: float = 2.0
    mock_mode: bool = False  # Enable mock responses for development/testing


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: List[str]
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    
    # Producer settings
    producer_acks: str = "all"
    producer_retries: int = 3
    producer_batch_size: int = 16384
    producer_linger_ms: int = 10
    producer_compression_type: str = "snappy"
    
    # Consumer settings
    consumer_group_id: str = "streaming-pipeline"
    consumer_auto_offset_reset: str = "latest"
    consumer_enable_auto_commit: bool = False
    consumer_max_poll_records: int = 500
    
    # Input Topics
    stock_quotes_topic: str = "stock-quotes-realtime"
    stock_intraday_topic: str = "stock-intraday-data"
    
    # Output Topics (Processed Data for Medallion Architecture)
    processed_stock_prices_topic: str = "processed-stock-prices"
    processed_trading_volume_topic: str = "processed-trading-volume"
    processed_technical_indicators_topic: str = "processed-technical-indicators"
    data_quality_alerts_topic: str = "data-quality-alerts"


@dataclass
class RedshiftConfig:
    """Redshift Serverless configuration."""
    endpoint: str
    database: str
    port: int = 5439
    user: str = "admin"
    password: str = ""
    iam_role: Optional[str] = None
    
    # Connection settings
    connection_timeout: int = 60
    network_timeout: int = 60
    
    # S3 settings for data loading
    s3_bucket: str = ""
    s3_prefix: str = "streaming-pipeline"
    s3_region: str = "us-east-1"


@dataclass
class SparkConfig:
    """Spark Structured Streaming configuration."""
    app_name: str
    master: str
    
    # Streaming settings
    checkpoint_location: str
    trigger_processing_time: str
    watermark_delay: str
    
    # Performance settings
    sql_adaptive_enabled: bool
    sql_adaptive_coalescePartitions_enabled: bool
    serializer: str
    
    # Memory settings
    driver_memory: str
    executor_memory: str
    executor_cores: int
    max_result_size: str


class ConfigManager:
    """Central configuration manager for the streaming pipeline."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or os.getenv("STREAMING_CONFIG_FILE")
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from environment variables and config file."""
        self.alpha_vantage = self._load_alpha_vantage_config()
        self.kafka = self._load_kafka_config()
        self.redshift = self._load_redshift_config()
        self.spark = self._load_spark_config()
        
        # Stock symbols to track
        self.stock_symbols = self._get_stock_symbols()
    
    def _load_alpha_vantage_config(self) -> AlphaVantageConfig:
        """Load Alpha Vantage configuration."""
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "mock_api_key")
        mock_mode = os.getenv("ALPHA_VANTAGE_MOCK_MODE", "false").lower() == "true"
        
        # If mock mode is enabled, API key is not required
        if not mock_mode and not api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required (unless ALPHA_VANTAGE_MOCK_MODE=true)")
        
        return AlphaVantageConfig(
            api_key=api_key,
            base_url=os.getenv("ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"),
            rate_limit_per_minute=int(os.getenv("ALPHA_VANTAGE_RATE_LIMIT", "5")),
            timeout_seconds=int(os.getenv("ALPHA_VANTAGE_TIMEOUT", "30")),
            retry_attempts=int(os.getenv("ALPHA_VANTAGE_RETRY_ATTEMPTS", "3")),
            retry_backoff_factor=float(os.getenv("ALPHA_VANTAGE_BACKOFF_FACTOR", "2.0")),
            mock_mode=mock_mode
        )
    
    def _load_kafka_config(self) -> KafkaConfig:
        """Load Kafka configuration."""
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
        
        return KafkaConfig(
            bootstrap_servers=bootstrap_servers,
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM"),
            sasl_username=os.getenv("KAFKA_SASL_USERNAME"),
            sasl_password=os.getenv("KAFKA_SASL_PASSWORD"),
            ssl_ca_location=os.getenv("KAFKA_SSL_CA_LOCATION"),
            ssl_certificate_location=os.getenv("KAFKA_SSL_CERTIFICATE_LOCATION"),
            ssl_key_location=os.getenv("KAFKA_SSL_KEY_LOCATION"),
            
            # Producer settings
            producer_acks=os.getenv("KAFKA_PRODUCER_ACKS", "all"),
            producer_retries=int(os.getenv("KAFKA_PRODUCER_RETRIES", "3")),
            producer_batch_size=int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", "16384")),
            producer_linger_ms=int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "10")),
            producer_compression_type=os.getenv("KAFKA_PRODUCER_COMPRESSION", "snappy"),
            
            # Consumer settings
            consumer_group_id=os.getenv("KAFKA_CONSUMER_GROUP_ID", "streaming-pipeline"),
            consumer_auto_offset_reset=os.getenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", "latest"),
            consumer_enable_auto_commit=os.getenv("KAFKA_CONSUMER_AUTO_COMMIT", "false").lower() == "true",
            consumer_max_poll_records=int(os.getenv("KAFKA_CONSUMER_MAX_POLL_RECORDS", "500")),
            
            # Input Topics
            stock_quotes_topic=os.getenv("KAFKA_STOCK_QUOTES_TOPIC", "stock-quotes-realtime"),
            stock_intraday_topic=os.getenv("KAFKA_STOCK_INTRADAY_TOPIC", "stock-intraday-data"),
            
            # Output Topics (Processed Data for Medallion Architecture)
            processed_stock_prices_topic=os.getenv("KAFKA_PROCESSED_STOCK_PRICES_TOPIC", "processed-stock-prices"),
            processed_trading_volume_topic=os.getenv("KAFKA_PROCESSED_TRADING_VOLUME_TOPIC", "processed-trading-volume"),
            processed_technical_indicators_topic=os.getenv("KAFKA_PROCESSED_TECHNICAL_INDICATORS_TOPIC", "processed-technical-indicators"),
            data_quality_alerts_topic=os.getenv("KAFKA_DATA_QUALITY_ALERTS_TOPIC", "data-quality-alerts")
        )
    
    def _load_redshift_config(self) -> RedshiftConfig:
        """Load Redshift configuration."""
        # If Alpha Vantage is in mock mode, make Redshift config optional
        mock_mode = os.getenv("ALPHA_VANTAGE_MOCK_MODE", "false").lower() == "true"
        
        # Core required fields
        required_fields = ["REDSHIFT_ENDPOINT", "REDSHIFT_DATABASE", "REDSHIFT_USER"]
        
        # Password is required for authentication
        if not os.getenv("REDSHIFT_PASSWORD"):
            required_fields.append("REDSHIFT_PASSWORD")
        
        missing_fields = [field for field in required_fields if not os.getenv(field)]
        
        # If in mock mode, Redshift config is optional
        if mock_mode:
            # If any fields are missing, use mock defaults
            if missing_fields:
                return RedshiftConfig(
                    endpoint="mock-endpoint.redshift-serverless.amazonaws.com",
                    database="mock_database",
                    port=5439,
                    user="mock_user",
                    password="mock_password",
                    iam_role="mock_role",
                    
                    connection_timeout=60,
                    network_timeout=60,
                    
                    s3_bucket="mock_bucket",
                    s3_prefix="mock_prefix",
                    s3_region="us-east-1"
                )
            # If all fields are present, use them even in mock mode
            else:
                return RedshiftConfig(
                    endpoint=os.getenv("REDSHIFT_ENDPOINT"),
                    database=os.getenv("REDSHIFT_DATABASE"),
                    port=int(os.getenv("REDSHIFT_PORT", "5439")),
                    user=os.getenv("REDSHIFT_USER"),
                    password=os.getenv("REDSHIFT_PASSWORD"),
                    iam_role=os.getenv("REDSHIFT_IAM_ROLE"),
                    
                    connection_timeout=int(os.getenv("REDSHIFT_CONNECTION_TIMEOUT", "60")),
                    network_timeout=int(os.getenv("REDSHIFT_NETWORK_TIMEOUT", "60")),
                    
                    s3_bucket=os.getenv("S3_BUCKET", ""),
                    s3_prefix=os.getenv("S3_PREFIX", "streaming-pipeline"),
                    s3_region=os.getenv("S3_REGION", "us-east-1")
                )
        else:
            # In production mode, all fields are required
            if missing_fields:
                raise ValueError(f"Missing required Redshift environment variables: {missing_fields}")
            
            return RedshiftConfig(
                endpoint=os.getenv("REDSHIFT_ENDPOINT"),
                database=os.getenv("REDSHIFT_DATABASE"),
                port=int(os.getenv("REDSHIFT_PORT", "5439")),
                user=os.getenv("REDSHIFT_USER"),
                password=os.getenv("REDSHIFT_PASSWORD"),
                iam_role=os.getenv("REDSHIFT_IAM_ROLE"),
                
                connection_timeout=int(os.getenv("REDSHIFT_CONNECTION_TIMEOUT", "60")),
                network_timeout=int(os.getenv("REDSHIFT_NETWORK_TIMEOUT", "60")),
                
                s3_bucket=os.getenv("S3_BUCKET", ""),
                s3_prefix=os.getenv("S3_PREFIX", "streaming-pipeline"),
                s3_region=os.getenv("S3_REGION", "us-east-1")
            )
    
    def _load_spark_config(self) -> SparkConfig:
        """Load Spark configuration."""
        return SparkConfig(
            app_name=os.getenv("SPARK_APP_NAME", "streaming-pipeline"),
            master=os.getenv("SPARK_MASTER", "local[*]"),
            
            checkpoint_location=os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-checkpoints"),
            trigger_processing_time=os.getenv("SPARK_TRIGGER_PROCESSING_TIME", "60 seconds"),
            watermark_delay=os.getenv("SPARK_WATERMARK_DELAY", "1 minute"),
            
            sql_adaptive_enabled=os.getenv("SPARK_SQL_ADAPTIVE_ENABLED", "true").lower() == "true",
            sql_adaptive_coalescePartitions_enabled=os.getenv("SPARK_SQL_ADAPTIVE_COALESCE", "true").lower() == "true",
            serializer=os.getenv("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer"),
            
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
            executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
            max_result_size=os.getenv("SPARK_MAX_RESULT_SIZE", "1g")
        )
    
    
    def _get_stock_symbols(self) -> List[str]:
        """Get list of stock symbols to track."""
        symbols_str = os.getenv("STOCK_SYMBOLS", "AAPL,GOOGL,MSFT,AMZN,TSLA")
        return [symbol.strip().upper() for symbol in symbols_str.split(",")]
    
    def get_kafka_producer_config(self) -> Dict[str, any]:
        """Get Kafka producer configuration dictionary."""
        config = {
            'bootstrap.servers': ','.join(self.kafka.bootstrap_servers),
            'security.protocol': self.kafka.security_protocol,
            'acks': self.kafka.producer_acks,
            'retries': self.kafka.producer_retries,
            'batch.size': self.kafka.producer_batch_size,
            'linger.ms': self.kafka.producer_linger_ms,
            'compression.type': self.kafka.producer_compression_type,
            'enable.idempotence': True,
            'max.in.flight.requests.per.connection': 5
        }
        
        # Add SASL configuration if provided
        if self.kafka.sasl_mechanism:
            config['sasl.mechanism'] = self.kafka.sasl_mechanism
            config['sasl.username'] = self.kafka.sasl_username
            config['sasl.password'] = self.kafka.sasl_password
        
        # Add SSL configuration if provided
        if self.kafka.ssl_ca_location:
            config['ssl.ca.location'] = self.kafka.ssl_ca_location
            config['ssl.certificate.location'] = self.kafka.ssl_certificate_location
            config['ssl.key.location'] = self.kafka.ssl_key_location
        
        return config
    
    def get_kafka_consumer_config(self) -> Dict[str, any]:
        """Get Kafka consumer configuration dictionary."""
        config = {
            'bootstrap.servers': ','.join(self.kafka.bootstrap_servers),
            'security.protocol': self.kafka.security_protocol,
            'group.id': self.kafka.consumer_group_id,
            'auto.offset.reset': self.kafka.consumer_auto_offset_reset,
            'enable.auto.commit': self.kafka.consumer_enable_auto_commit,
            'max.poll.records': self.kafka.consumer_max_poll_records
        }
        
        # Add SASL configuration if provided
        if self.kafka.sasl_mechanism:
            config['sasl.mechanism'] = self.kafka.sasl_mechanism
            config['sasl.username'] = self.kafka.sasl_username
            config['sasl.password'] = self.kafka.sasl_password
        
        # Add SSL configuration if provided
        if self.kafka.ssl_ca_location:
            config['ssl.ca.location'] = self.kafka.ssl_ca_location
            config['ssl.certificate.location'] = self.kafka.ssl_certificate_location
            config['ssl.key.location'] = self.kafka.ssl_key_location
        
        return config
    
    def get_redshift_connection_params(self) -> Dict[str, any]:
        """Get Redshift connection parameters."""
        params = {
            'host': self.redshift.endpoint,
            'port': self.redshift.port,
            'user': self.redshift.user,
            'password': self.redshift.password,
            'dbname': self.redshift.database,
            'connect_timeout': self.redshift.connection_timeout
        }
        
        if self.redshift.iam_role:
            params['iam_role'] = self.redshift.iam_role
        
        return params
    
    def get_stock_symbols(self) -> List[str]:
        """Get list of stock symbols to track."""
        return self.stock_symbols
    
    def get_production_interval(self) -> int:
        """Get production interval in seconds."""
        return int(os.getenv("PRODUCTION_INTERVAL_SECONDS", "60"))
    
    def get_output_base_path(self) -> str:
        """Get base path for output files."""
        return os.getenv("OUTPUT_BASE_PATH", "/tmp/streaming-output")


# Global configuration instance
config = ConfigManager()
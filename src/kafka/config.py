import os
from typing import Optional
from dataclasses import dataclass, field


@dataclass
class KafkaConfig:
    """Kafka configuration"""
    bootstrap_servers: str = field(default_factory=lambda: os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'))  # Changed default to match Docker Compose
    batch_topic: str = field(default_factory=lambda: os.getenv('KAFKA_BATCH_TOPIC', 'stock-data-batch'))
    stream_topic: str = field(default_factory=lambda: os.getenv('KAFKA_STREAM_TOPIC', 'stock-data-stream'))
    producer_timeout: int = field(default_factory=lambda: int(os.getenv('KAFKA_PRODUCER_TIMEOUT', '30')))
    consumer_timeout: int = field(default_factory=lambda: int(os.getenv('KAFKA_CONSUMER_TIMEOUT', '30')))
    group_id: str = field(default_factory=lambda: os.getenv('KAFKA_GROUP_ID', 'stock-batch-consumer'))


@dataclass
class AlphaVantageConfig:
    """Alpha Vantage API configuration"""
    api_key: Optional[str] = field(default_factory=lambda: os.getenv('ALPHA_VANTAGE_API_KEY'))
    base_url: str = field(default_factory=lambda: os.getenv('ALPHA_VANTAGE_BASE_URL', 'https://www.alphavantage.co/query'))


@dataclass
class YahooFinanceConfig:
    """Yahoo Finance configuration"""
    lookback_days: int = field(default_factory=lambda: int(os.getenv('BATCH_LOOKBACK_DAYS', '30')))
    default_period: str = field(default_factory=lambda: os.getenv('YAHOO_DEFAULT_PERIOD', '1mo'))


@dataclass
class S3Config:
    """AWS S3 configuration"""
    bucket_name: str = field(default_factory=lambda: os.getenv('S3_BUCKET_NAME', 'stock-market-pipeline-zan'))
    aws_region: str = field(default_factory=lambda: os.getenv('AWS_REGION', 'us-east-1'))
    aws_access_key_id: Optional[str] = field(default_factory=lambda: os.getenv('AWS_ACCESS_KEY_ID'))
    aws_secret_access_key: Optional[str] = field(default_factory=lambda: os.getenv('AWS_SECRET_ACCESS_KEY'))
    s3_endpoint_url: Optional[str] = field(default_factory=lambda: os.getenv('S3_ENDPOINT_URL'))  # For local testing with MinIO


@dataclass
class AppConfig:
    """Main application configuration"""
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    alpha_vantage: AlphaVantageConfig = field(default_factory=AlphaVantageConfig)
    yahoo_finance: YahooFinanceConfig = field(default_factory=YahooFinanceConfig)
    s3: S3Config = field(default_factory=S3Config)
    
    @classmethod
    def from_env(cls):
        """Create configuration from environment variables"""
        return cls()
    
    def validate(self):
        """Validate configuration"""
        if not self.kafka.bootstrap_servers:
            raise ValueError("Kafka bootstrap servers not configured")
        
        if not self.s3.bucket_name:
            raise ValueError("S3 bucket name not configured")
        
        # S3 credentials are optional (can use IAM roles, etc.)
        return True

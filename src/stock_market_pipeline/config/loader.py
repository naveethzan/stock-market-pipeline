"""
Configuration loading and management.
Handles environment variables, validation, and mode detection.
No hardcoded values - everything from constants or environment variables.
"""

import os
from typing import Dict, Any, List

from stock_market_pipeline.core.constants import (
    APIConfig as APIConstants,
    KafkaConfig as KafkaConstants,
    ServiceEndpoints,
    ProcessingConstants,
    MockData,
    DataProcessing
)
from stock_market_pipeline.core.exceptions import ConfigurationError
from .settings import Config, APIConfig, KafkaConfig, RedshiftConfig, SparkConfig, S3Config


class ConfigLoader:
    """
    Loads and manages configuration based on environment.
    
    Handles both mock and production configurations, automatically detecting
    the environment mode and loading appropriate settings from environment
    variables with fallback to default constants.
    """
    
    def __init__(self):
        """Initialize configuration loader."""
        self.is_mock = os.getenv("ENVIRONMENT", "mock").lower() == "mock"
        self.config = self._load_config()
    
    def _load_config(self) -> Config:
        """Load configuration based on mode."""
        if self.is_mock:
            return self._get_mock_config()
        else:
            return self._get_production_config()
    
    def _get_mock_config(self) -> Config:
        """
        Create mock configuration for local development.
        
        Uses mock data constants and local endpoints for testing
        without requiring external services.
        """
        return Config(
            mock_mode=True,
            api=APIConfig(
                api_key=os.getenv("ALPHA_VANTAGE_API_KEY", MockData.MOCK_API_KEY),
                base_url=os.getenv("ALPHA_VANTAGE_BASE_URL", APIConstants.ALPHA_VANTAGE_BASE_URL),
                rate_limit=int(os.getenv("ALPHA_VANTAGE_RATE_LIMIT", str(APIConstants.DEFAULT_RATE_LIMIT))),
                timeout=int(os.getenv("ALPHA_VANTAGE_TIMEOUT", str(APIConstants.DEFAULT_TIMEOUT))),
                retry_attempts=int(os.getenv("ALPHA_VANTAGE_RETRY_ATTEMPTS", str(APIConstants.DEFAULT_RETRY_ATTEMPTS))),
                backoff_factor=float(os.getenv("ALPHA_VANTAGE_BACKOFF_FACTOR", str(APIConstants.DEFAULT_BACKOFF_FACTOR)))
            ),
            kafka=KafkaConfig(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", ",".join(KafkaConstants.DEFAULT_BOOTSTRAP_SERVERS)).split(","),
                security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", KafkaConstants.DEFAULT_SECURITY_PROTOCOL),
                producer_acks=os.getenv("KAFKA_PRODUCER_ACKS", KafkaConstants.DEFAULT_PRODUCER_ACKS),
                producer_retries=int(os.getenv("KAFKA_PRODUCER_RETRIES", str(KafkaConstants.DEFAULT_PRODUCER_RETRIES))),
                producer_batch_size=int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", str(KafkaConstants.DEFAULT_PRODUCER_BATCH_SIZE))),
                producer_linger_ms=int(os.getenv("KAFKA_PRODUCER_LINGER_MS", str(KafkaConstants.DEFAULT_PRODUCER_LINGER_MS))),
                producer_compression=os.getenv("KAFKA_PRODUCER_COMPRESSION", KafkaConstants.DEFAULT_PRODUCER_COMPRESSION),
                consumer_group_id=os.getenv("KAFKA_CONSUMER_GROUP_ID", KafkaConstants.DEFAULT_CONSUMER_GROUP_ID),
                consumer_offset_reset=os.getenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", KafkaConstants.DEFAULT_CONSUMER_OFFSET_RESET),
                consumer_auto_commit=os.getenv("KAFKA_CONSUMER_AUTO_COMMIT", "false").lower() == "true",
                consumer_max_poll_records=int(os.getenv("KAFKA_CONSUMER_MAX_POLL_RECORDS", str(KafkaConstants.DEFAULT_CONSUMER_MAX_POLL_RECORDS)))
            ),
            redshift=RedshiftConfig(
                endpoint=os.getenv("REDSHIFT_ENDPOINT", "mock-endpoint.redshift-serverless.amazonaws.com"),
                database=os.getenv("REDSHIFT_DATABASE", MockData.MOCK_DATABASE),
                user=os.getenv("REDSHIFT_USER", MockData.MOCK_USER),
                password=os.getenv("REDSHIFT_PASSWORD", MockData.MOCK_PASSWORD),
                port=int(os.getenv("REDSHIFT_PORT", str(ServiceEndpoints.DEFAULT_REDSHIFT_PORT))),
                connection_timeout=int(os.getenv("REDSHIFT_CONNECTION_TIMEOUT", "60")),
                network_timeout=int(os.getenv("REDSHIFT_NETWORK_TIMEOUT", "60"))
            ),
            spark=SparkConfig(
                app_name=os.getenv("SPARK_APP_NAME", "stock-market-pipeline"),
                master=os.getenv("SPARK_MASTER", "local[*]"),
                checkpoint_location=os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-checkpoints-mock"),
                trigger_interval=os.getenv("SPARK_TRIGGER_PROCESSING_TIME", ProcessingConstants.DEFAULT_TRIGGER_INTERVAL),
                watermark_delay=os.getenv("SPARK_WATERMARK_DELAY", ProcessingConstants.DEFAULT_WATERMARK_DELAY),
                driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
                executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
                executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
                max_result_size=os.getenv("SPARK_MAX_RESULT_SIZE", "1g")
            ),
            s3=S3Config(
                bucket=os.getenv("S3_BUCKET", MockData.MOCK_BUCKET),
                prefix=os.getenv("S3_PREFIX", MockData.MOCK_PREFIX),
                region=os.getenv("S3_REGION", MockData.MOCK_REGION)
            ),
            stock_symbols=DataProcessing.DEFAULT_STOCK_SYMBOLS,
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", ServiceEndpoints.DEFAULT_SCHEMA_REGISTRY_URL)
        )
    
    def _get_production_config(self) -> Config:
        """
        Create production configuration for AWS deployment.
        
        Validates required environment variables and creates configuration
        using real AWS services (MSK, Redshift, S3) and production API keys.
        """
        # Check required variables for production
        required_vars = [
            "KAFKA_BOOTSTRAP_SERVERS",
            "REDSHIFT_ENDPOINT",
            "REDSHIFT_DATABASE",
            "REDSHIFT_USER", 
            "REDSHIFT_PASSWORD",
            "S3_BUCKET",
            "ALPHA_VANTAGE_API_KEY"
        ]
        
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ConfigurationError(f"Missing required environment variables: {missing}")
        
        return Config(
            mock_mode=False,
            api=APIConfig(
                api_key=os.getenv("ALPHA_VANTAGE_API_KEY"),
                base_url=os.getenv("ALPHA_VANTAGE_BASE_URL", APIConstants.ALPHA_VANTAGE_BASE_URL),
                rate_limit=int(os.getenv("ALPHA_VANTAGE_RATE_LIMIT", str(APIConstants.DEFAULT_RATE_LIMIT))),
                timeout=int(os.getenv("ALPHA_VANTAGE_TIMEOUT", str(APIConstants.DEFAULT_TIMEOUT))),
                retry_attempts=int(os.getenv("ALPHA_VANTAGE_RETRY_ATTEMPTS", str(APIConstants.DEFAULT_RETRY_ATTEMPTS))),
                backoff_factor=float(os.getenv("ALPHA_VANTAGE_BACKOFF_FACTOR", str(APIConstants.DEFAULT_BACKOFF_FACTOR)))
            ),
            kafka=KafkaConfig(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(","),
                security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", KafkaConstants.DEFAULT_SECURITY_PROTOCOL),
                producer_acks=os.getenv("KAFKA_PRODUCER_ACKS", KafkaConstants.DEFAULT_PRODUCER_ACKS),
                producer_retries=int(os.getenv("KAFKA_PRODUCER_RETRIES", str(KafkaConstants.DEFAULT_PRODUCER_RETRIES))),
                producer_batch_size=int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", str(KafkaConstants.DEFAULT_PRODUCER_BATCH_SIZE))),
                producer_linger_ms=int(os.getenv("KAFKA_PRODUCER_LINGER_MS", str(KafkaConstants.DEFAULT_PRODUCER_LINGER_MS))),
                producer_compression=os.getenv("KAFKA_PRODUCER_COMPRESSION", KafkaConstants.DEFAULT_PRODUCER_COMPRESSION),
                consumer_group_id=os.getenv("KAFKA_CONSUMER_GROUP_ID", KafkaConstants.DEFAULT_CONSUMER_GROUP_ID),
                consumer_offset_reset=os.getenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", KafkaConstants.DEFAULT_CONSUMER_OFFSET_RESET),
                consumer_auto_commit=os.getenv("KAFKA_CONSUMER_AUTO_COMMIT", "false").lower() == "true",
                consumer_max_poll_records=int(os.getenv("KAFKA_CONSUMER_MAX_POLL_RECORDS", str(KafkaConstants.DEFAULT_CONSUMER_MAX_POLL_RECORDS)))
            ),
            redshift=RedshiftConfig(
                endpoint=os.getenv("REDSHIFT_ENDPOINT"),
                database=os.getenv("REDSHIFT_DATABASE"),
                user=os.getenv("REDSHIFT_USER"),
                password=os.getenv("REDSHIFT_PASSWORD"),
                port=int(os.getenv("REDSHIFT_PORT", str(ServiceEndpoints.DEFAULT_REDSHIFT_PORT))),
                connection_timeout=int(os.getenv("REDSHIFT_CONNECTION_TIMEOUT", "60")),
                network_timeout=int(os.getenv("REDSHIFT_NETWORK_TIMEOUT", "60"))
            ),
            spark=SparkConfig(
                app_name=os.getenv("SPARK_APP_NAME", "stock-market-pipeline"),
                master=os.getenv("SPARK_MASTER", "yarn"),
                checkpoint_location=os.getenv("SPARK_CHECKPOINT_LOCATION", f"s3://{os.getenv('S3_BUCKET')}/checkpoints"),
                trigger_interval=os.getenv("SPARK_TRIGGER_PROCESSING_TIME", ProcessingConstants.DEFAULT_TRIGGER_INTERVAL),
                watermark_delay=os.getenv("SPARK_WATERMARK_DELAY", ProcessingConstants.DEFAULT_WATERMARK_DELAY),
                driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
                executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
                executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
                max_result_size=os.getenv("SPARK_MAX_RESULT_SIZE", "1g")
            ),
            s3=S3Config(
                bucket=os.getenv("S3_BUCKET"),
                prefix=os.getenv("S3_PREFIX", "streaming-pipeline"),
                region=os.getenv("S3_REGION", "us-east-1")
            ),
            stock_symbols=DataProcessing.DEFAULT_STOCK_SYMBOLS,
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", ServiceEndpoints.DEFAULT_SCHEMA_REGISTRY_URL)
        )
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """
        Get Kafka configuration dictionary.
        
        Returns:
            Dictionary with Kafka producer and consumer settings
            formatted for use with Kafka clients.
        """
        return {
            'bootstrap.servers': ','.join(self.config.kafka.bootstrap_servers),
            'security.protocol': self.config.kafka.security_protocol,
            'acks': self.config.kafka.producer_acks,
            'retries': self.config.kafka.producer_retries,
            'batch.size': self.config.kafka.producer_batch_size,
            'linger.ms': self.config.kafka.producer_linger_ms,
            'compression.type': self.config.kafka.producer_compression,
            'group.id': self.config.kafka.consumer_group_id,
            'auto.offset.reset': self.config.kafka.consumer_offset_reset,
            'enable.auto.commit': self.config.kafka.consumer_auto_commit,
            'max.poll.records': self.config.kafka.consumer_max_poll_records
        }
    
    def get_redshift_config(self) -> Dict[str, Any]:
        """
        Get Redshift configuration dictionary.
        
        Returns:
            Dictionary with Redshift connection parameters
            formatted for use with database clients.
        """
        return {
            'host': self.config.redshift.endpoint,
            'port': self.config.redshift.port,
            'user': self.config.redshift.user,
            'password': self.config.redshift.password,
            'dbname': self.config.redshift.database,
            'connect_timeout': self.config.redshift.connection_timeout
        }
    
    def get_spark_config(self) -> Dict[str, Any]:
        """
        Get Spark configuration dictionary.
        
        Returns:
            Dictionary with Spark application settings
            optimized for stream processing workloads.
        """
        return {
            'spark.app.name': self.config.spark.app_name,
            'spark.master': self.config.spark.master,
            'spark.sql.streaming.checkpointLocation': self.config.spark.checkpoint_location,
            'spark.sql.streaming.trigger.processingTime': self.config.spark.trigger_interval,
            'spark.sql.streaming.watermarkDelay': self.config.spark.watermark_delay,
            'spark.driver.memory': self.config.spark.driver_memory,
            'spark.executor.memory': self.config.spark.executor_memory,
            'spark.executor.cores': str(self.config.spark.executor_cores),
            'spark.sql.execution.arrow.maxRecordsPerBatch': '10000',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        }
    
    def get_s3_config(self) -> Dict[str, Any]:
        """
        Get S3 configuration dictionary.
        
        Returns:
            Dictionary with S3 bucket and region settings
            for data storage and checkpoint operations.
        """
        return {
            'bucket': self.config.s3.bucket,
            'prefix': self.config.s3.prefix,
            'region': self.config.s3.region
        }
    
    @property
    def is_mock_mode(self) -> bool:
        """Check if running in mock mode."""
        return self.config.mock_mode
    
    @property
    def is_production_mode(self) -> bool:
        """Check if running in production mode."""
        return not self.config.mock_mode
    
    @property
    def stock_symbols(self) -> List[str]:
        """Get stock symbols."""
        return self.config.stock_symbols
    
    def get_config(self) -> Config:
        """Get the current configuration."""
        return self.config


config = ConfigLoader()

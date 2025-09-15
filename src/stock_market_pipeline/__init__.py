"""
Stock Market Pipeline - Real-time streaming data pipeline.

A professional, enterprise-grade streaming pipeline for stock market data
processing using Kafka, Spark, and Redshift with Medallion Architecture.

This package provides a complete solution for:
- Real-time data ingestion from Alpha Vantage API
- Stream processing with Apache Spark Structured Streaming
- Data storage and analytics with AWS Redshift
- Data quality monitoring and validation
- Professional logging and error handling

Architecture:
- Bronze Layer: Raw data ingestion via Kafka
- Silver Layer: Stream processing and transformations
- Gold Layer: Analytics-ready data in Redshift

Usage:
    from stock_market_pipeline import ConfigManager
    from stock_market_pipeline.ingestion import AlphaVantageClient
    from stock_market_pipeline.processing import StreamProcessor
    
    # Initialize configuration
    config = ConfigManager('dev')
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"
__description__ = "Real-time Stock Market Streaming Pipeline"
__license__ = "MIT"

# ============================================================================
# CORE COMPONENTS
# ============================================================================

# Core Exceptions
from .core.exceptions import (
    StockMarketPipelineError,
    ConfigurationError,
    DataValidationError,
    ProcessingError,
    StorageError,
    IngestionError,
    AlphaVantageAPIError,
    KafkaProducerError,
    StreamProcessorError,
    SchemaRegistryError,
    AvroSerializationError
)

# Core Interfaces
from .core.interfaces import (
    DataClient,
    DataProducer,
    StreamProcessor,
    DataConnector,
    LifecycleManager,
    DataValidator
)

# Core Constants
from .core.constants import (
    APIConfig,
    KafkaConfig,
    Topics,
    SchemaNames,
    DataProcessing,
    ServiceEndpoints,
    ErrorMessages,
    ProcessingConstants,
    SchemaRegistrySubjects,
    DataQuality,
    TechnicalIndicators,
    MockData
)

# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

from .config import config, Config, APIConfig, KafkaConfig, RedshiftConfig, SparkConfig, S3Config

# ============================================================================
# UTILITY MODULES
# ============================================================================

from .utils import (
    PipelineLogger,
    get_logger,
    setup_logging
)

# ============================================================================
# LAYER MODULES
# ============================================================================

# Ingestion Layer
from . import ingestion

# Processing Layer  
from . import processing

# Storage Layer
from . import storage

# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    # Core Exceptions
    "StockMarketPipelineError",
    "ConfigurationError",
    "DataValidationError",
    "ProcessingError",
    "StorageError",
    "IngestionError",
    "AlphaVantageAPIError",
    "KafkaProducerError",
    "StreamProcessorError",
    "SchemaRegistryError",
    "AvroSerializationError",
    
    # Core Interfaces
    "DataClient",
    "DataProducer",
    "StreamProcessor",
    "DataConnector",
    "LifecycleManager",
    "DataValidator",
    
    # Configuration
    "config",
    "Config",
    "APIConfig",
    "KafkaConfig",
    "RedshiftConfig",
    "SparkConfig",
    "S3Config",
    
    # Constants
    "APIConfig",
    "KafkaConfig",
    "Topics",
    "SchemaNames",
    "DataProcessing",
    "ServiceEndpoints",
    "ErrorMessages",
    "ProcessingConstants",
    "SchemaRegistrySubjects",
    "DataQuality",
    "TechnicalIndicators",
    "MockData",
    
    # Utilities
    "PipelineLogger",
    "get_logger",
    "setup_logging",
    
    # Layer Modules
    "ingestion",
    "processing",
    "storage"
]

# ============================================================================
# PACKAGE METADATA
# ============================================================================

def get_version():
    """Get the package version."""
    return __version__

def get_author():
    """Get the package author."""
    return __author__

def get_description():
    """Get the package description."""
    return __description__

def get_license():
    """Get the package license."""
    return __license__

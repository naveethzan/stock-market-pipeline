"""
Core components for the stock market pipeline.
Contains exceptions, interfaces, and constants.
"""

from .exceptions import (
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
    AvroSerializationError,
    create_ingestion_error,
    create_processing_error,
    create_storage_error,
    create_validation_error
)

from .interfaces import (
    DataClient,
    DataProducer,
    StreamProcessor,
    DataConnector,
    LifecycleManager,
    DataValidator,
    validate_client_interface,
    validate_producer_interface,
    validate_processor_interface,
    validate_connector_interface
)

from .constants import (
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

__all__ = [
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
    "create_ingestion_error",
    "create_processing_error",
    "create_storage_error",
    "create_validation_error",
    "DataClient",
    "DataProducer",
    "StreamProcessor",
    "DataConnector",
    "LifecycleManager",
    "DataValidator",
    "validate_client_interface",
    "validate_producer_interface",
    "validate_processor_interface",
    "validate_connector_interface",
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
    "MockData"
]

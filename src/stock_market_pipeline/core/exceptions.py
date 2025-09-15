"""
Core exceptions for the stock market pipeline.
Provides centralized error handling across all components.
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional


class StockMarketPipelineError(Exception):
    """Base exception for all pipeline errors."""
    
    def __init__(self, message: str, component: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        """
        Initialize pipeline error.
        
        Args:
            message: Error message
            component: Component that raised the error
            context: Additional context information
        """
        super().__init__(message)
        self.message = message
        self.component = component
        self.context = context or {}
        self.timestamp = datetime.now(timezone.utc)
    
    def __str__(self) -> str:
        """Return formatted error message."""
        base_msg = f"[{self.component}] {self.message}" if self.component else self.message
        if self.context:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            return f"{base_msg} (Context: {context_str})"
        return base_msg


class ConfigurationError(StockMarketPipelineError):
    """Raised when configuration is invalid or missing."""
    pass


class DataValidationError(StockMarketPipelineError):
    """Raised when data validation fails."""
    pass


class ProcessingError(StockMarketPipelineError):
    """Raised when data processing fails."""
    pass


class StorageError(StockMarketPipelineError):
    """Raised when data storage operations fail."""
    pass


class IngestionError(StockMarketPipelineError):
    """Raised when data ingestion fails."""
    pass



class AlphaVantageAPIError(IngestionError):
    """Alpha Vantage API specific errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response_data: Optional[Dict[str, Any]] = None, 
                 component: str = "alpha_vantage_client", 
                 context: Optional[Dict[str, Any]] = None):
        """
        Initialize Alpha Vantage API error.
        
        Args:
            message: Error message
            status_code: HTTP status code
            response_data: API response data
            component: Component name
            context: Additional context
        """
        super().__init__(message, component, context)
        self.status_code = status_code
        self.response_data = response_data or {}


class KafkaProducerError(IngestionError):
    """Kafka producer specific errors."""
    
    def __init__(self, message: str, topic: Optional[str] = None,
                 component: str = "kafka_producer",
                 context: Optional[Dict[str, Any]] = None):
        """
        Initialize Kafka producer error.
        
        Args:
            message: Error message
            topic: Kafka topic name
            component: Component name
            context: Additional context
        """
        super().__init__(message, component, context)
        self.topic = topic


class StreamProcessorError(ProcessingError):
    """Spark stream processing errors."""
    
    def __init__(self, message: str, query_id: Optional[str] = None,
                 component: str = "stream_processor",
                 context: Optional[Dict[str, Any]] = None):
        """
        Initialize stream processor error.
        
        Args:
            message: Error message
            query_id: Spark query ID
            component: Component name
            context: Additional context
        """
        super().__init__(message, component, context)
        self.query_id = query_id


class SchemaRegistryError(StorageError):
    """Schema Registry errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None,
                 subject: Optional[str] = None,
                 component: str = "schema_registry",
                 context: Optional[Dict[str, Any]] = None):
        """
        Initialize Schema Registry error.
        
        Args:
            message: Error message
            status_code: HTTP status code
            subject: Schema subject
            component: Component name
            context: Additional context
        """
        super().__init__(message, component, context)
        self.status_code = status_code
        self.subject = subject


class AvroSerializationError(StorageError):
    """Avro serialization errors."""
    
    def __init__(self, message: str, schema_name: Optional[str] = None,
                 component: str = "avro_serializer",
                 context: Optional[Dict[str, Any]] = None):
        """
        Initialize Avro serialization error.
        
        Args:
            message: Error message
            schema_name: Avro schema name
            component: Component name
            context: Additional context
        """
        super().__init__(message, component, context)
        self.schema_name = schema_name


class ConnectorError(StorageError):
    """Kafka Connect connector errors."""
    
    def __init__(self, message: str, connector_name: Optional[str] = None,
                 component: str = "connector_manager",
                 context: Optional[Dict[str, Any]] = None):
        """
        Initialize connector error.
        
        Args:
            message: Error message
            connector_name: Connector name
            component: Component name
            context: Additional context
        """
        super().__init__(message, component, context)
        self.connector_name = connector_name



def create_ingestion_error(message: str, component: str, **context) -> IngestionError:
    """
    Create an ingestion error with context.
    
    Args:
        message: Error message
        component: Component that raised the error
        **context: Additional context information
        
    Returns:
        IngestionError instance with context
    """
    return IngestionError(message, component, context)


def create_processing_error(message: str, component: str, **context) -> ProcessingError:
    """
    Create a processing error with context.
    
    Args:
        message: Error message
        component: Component that raised the error
        **context: Additional context information
        
    Returns:
        ProcessingError instance with context
    """
    return ProcessingError(message, component, context)


def create_storage_error(message: str, component: str, **context) -> StorageError:
    """
    Create a storage error with context.
    
    Args:
        message: Error message
        component: Component that raised the error
        **context: Additional context information
        
    Returns:
        StorageError instance with context
    """
    return StorageError(message, component, context)


def create_validation_error(message: str, field: str = None, **context) -> DataValidationError:
    """
    Create a data validation error with context.
    
    Args:
        message: Error message
        field: Field that failed validation (optional)
        **context: Additional context information
        
    Returns:
        DataValidationError instance with context
    """
    if field:
        context['field'] = field
    return DataValidationError(message, "data_validator", context)

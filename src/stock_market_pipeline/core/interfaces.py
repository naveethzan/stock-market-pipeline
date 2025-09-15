"""
Core interfaces for the stock market pipeline.
Defines contracts for all major components following the Medallion Architecture.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime


class DataClient(ABC):
    """
    Interface for data clients (API clients, mock clients, etc.).

    Represents the Bronze Layer (Ingestion) - fetching data from external sources.
    """

    @abstractmethod
    def fetch_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch data for a given symbol.

        Args:
            symbol: Stock symbol to fetch data for

        Returns:
            Dictionary containing the fetched data

        Raises:
            IngestionError: If data fetching fails
        """
        pass

    @abstractmethod
    def is_healthy(self) -> bool:
        """
        Check if client is healthy and ready to serve requests.

        Returns:
            True if client is healthy, False otherwise
        """
        pass

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get client performance metrics.

        Returns:
            Dictionary containing metrics like request count, success rate, etc.
        """
        pass


class DataProducer(ABC):
    """
    Interface for data producers (Kafka producers, etc.).

    Represents the Bronze Layer (Ingestion) - publishing data to message queues.
    """

    @abstractmethod
    def produce(self, data: Dict[str, Any]) -> bool:
        """
        Produce data to the target system.

        Args:
            data: Data dictionary to produce

        Returns:
            True if data was successfully produced, False otherwise

        Raises:
            IngestionError: If data production fails
        """
        pass

    @abstractmethod
    def is_healthy(self) -> bool:
        """
        Check if producer is healthy and ready to produce data.

        Returns:
            True if producer is healthy, False otherwise
        """
        pass

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get producer performance metrics.

        Returns:
            Dictionary containing metrics like messages sent, success rate, etc.
        """
        pass


class StreamProcessor(ABC):
    """
    Interface for stream processors.

    Represents the Silver Layer (Processing) - real-time data transformation.
    """

    @abstractmethod
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process streaming data.

        Args:
            data: Input data dictionary

        Returns:
            Processed data dictionary

        Raises:
            ProcessingError: If data processing fails
        """
        pass

    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate input data before processing.

        Args:
            data: Data dictionary to validate

        Returns:
            True if data is valid, False otherwise

        Raises:
            DataValidationError: If data validation fails
        """
        pass

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get processor performance metrics.

        Returns:
            Dictionary containing metrics like processed records, processing time, etc.
        """
        pass


class DataConnector(ABC):
    """
    Interface for data connectors (Kafka Connect, database connectors, etc.).

    Represents the Gold Layer (Storage) - persisting data to storage systems.
    """

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to target system.

        Returns:
            True if connection was successful, False otherwise

        Raises:
            StorageError: If connection fails
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close connection to target system.

        Raises:
            StorageError: If disconnection fails
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if connector is connected to target system.

        Returns:
            True if connected, False otherwise
        """
        pass

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get connector performance metrics.

        Returns:
            Dictionary containing metrics like connection status, data transferred, etc.
        """
        pass




class LifecycleManager(ABC):
    """
    Optional interface for components that support lifecycle management.
    """

    @abstractmethod
    def start(self) -> None:
        """
        Start the component.

        Raises:
            ConfigurationError: If component cannot be started
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the component.

        Raises:
            ProcessingError: If component cannot be stopped gracefully
        """
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """
        Check if component is currently running.

        Returns:
            True if running, False otherwise
        """
        pass


class DataValidator(ABC):
    """
    Optional interface for components that perform data validation.
    """

    @abstractmethod
    def validate_schema(self, data: Dict[str, Any], schema_name: str) -> bool:
        """
        Validate data against a specific schema.

        Args:
            data: Data to validate
            schema_name: Name of the schema to validate against

        Returns:
            True if data is valid, False otherwise

        Raises:
            DataValidationError: If validation fails
        """
        pass

    @abstractmethod
    def get_validation_errors(self, data: Dict[str, Any], schema_name: str) -> list:
        """
        Get detailed validation errors for data.

        Args:
            data: Data to validate
            schema_name: Name of the schema to validate against

        Returns:
            List of validation error messages
        """
        pass




def validate_client_interface(instance: Any) -> bool:
    """
    Validate that an instance implements the DataClient interface.

    Args:
        instance: Object to validate

    Returns:
        True if instance implements DataClient interface
    """
    required_methods = ["fetch_data", "is_healthy", "get_metrics"]
    return all(
        hasattr(instance, method) and callable(getattr(instance, method))
        for method in required_methods
    )


def validate_producer_interface(instance: Any) -> bool:
    """
    Validate that an instance implements the DataProducer interface.

    Args:
        instance: Object to validate

    Returns:
        True if instance implements DataProducer interface
    """
    required_methods = ["produce", "is_healthy", "get_metrics"]
    return all(
        hasattr(instance, method) and callable(getattr(instance, method))
        for method in required_methods
    )


def validate_processor_interface(instance: Any) -> bool:
    """
    Validate that an instance implements the StreamProcessor interface.

    Args:
        instance: Object to validate

    Returns:
        True if instance implements StreamProcessor interface
    """
    required_methods = ["process", "validate", "get_metrics"]
    return all(
        hasattr(instance, method) and callable(getattr(instance, method))
        for method in required_methods
    )


def validate_connector_interface(instance: Any) -> bool:
    """
    Validate that an instance implements the DataConnector interface.

    Args:
        instance: Object to validate

    Returns:
        True if instance implements DataConnector interface
    """
    required_methods = ["connect", "disconnect", "is_connected", "get_metrics"]
    return all(
        hasattr(instance, method) and callable(getattr(instance, method))
        for method in required_methods
    )

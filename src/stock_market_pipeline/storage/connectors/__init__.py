"""
Kafka Connect Connector Management Package

This package provides enhanced connector management for the stock market pipeline.
It handles the three main connectors:
- Bronze S3 Connector: Raw data to S3 (Avro format)
- Silver S3 Connector: Processed data to S3 (Parquet format)  
- Redshift Connector: Analytics data to Redshift (JSON format)

Components:
- ConnectorManager: Main connector management class
- ConnectorConfig: Configuration management for connectors
- ConnectorHealth: Health monitoring and status checking

Usage:
    from stock_market_pipeline.storage.connectors import ConnectorManager
    
    # Create connector manager
    manager = ConnectorManager()
    
    # List all connectors
    connectors = manager.list_connectors()
    
    # Get connector status
    status = manager.get_connector_status("bronze-s3-sink-connector")
    
    # Create connector from config file
    success = manager.create_connector_from_file("scripts/connectors/configs/bronze/bronze-s3-connector.json")
"""

from .connector_manager import ConnectorManager

__all__ = [
    'ConnectorManager'
]

__version__ = '1.0.0'
__author__ = 'Stock Market Pipeline Team'
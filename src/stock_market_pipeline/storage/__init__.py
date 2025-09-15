"""
Data storage layer for the stock market pipeline.

This layer handles the Gold tier of the Medallion Architecture:
- Data persistence to AWS Redshift
- Schema management and evolution
- Data connectors and integrations
- Analytics-ready data preparation

Components:
- connectors: Database and service connectors
- schemas: Avro schema definitions and management

Usage:
    from stock_market_pipeline.storage import RedshiftConnector
    from stock_market_pipeline.storage import SchemaRegistryClient
    
    # Initialize connector
    connector = RedshiftConnector(connection_string="your_connection")
    
    # Initialize schema client
    schema_client = SchemaRegistryClient(registry_url="http://localhost:8081")
"""

from .connectors import *
from .schemas import *

__all__ = ["connectors", "schemas"]

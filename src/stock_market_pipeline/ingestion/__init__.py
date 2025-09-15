"""
Data ingestion layer for the stock market pipeline.

This layer handles the Bronze tier of the Medallion Architecture:
- Raw data ingestion from external sources (Alpha Vantage API)
- Data publishing to Kafka topics
- Schema validation and serialization
- Error handling and retry logic

Components:
- clients: API clients for data fetching
- producers: Kafka producers for data publishing

Usage:
    from stock_market_pipeline.ingestion import AlphaVantageClient
    from stock_market_pipeline.ingestion import KafkaAvroProducer
    
    # Initialize client
    client = AlphaVantageClient(api_key="your_key")
    
    # Initialize producer
    producer = KafkaAvroProducer(topic="stock-quotes")
"""

from .clients import *
from .producers import *

__all__ = ["clients", "producers"]

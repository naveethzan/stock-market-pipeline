"""
Data ingestion clients for the stock market pipeline.
Provides Alpha Vantage API clients for real-time and mock data.
"""

from .base_client import BaseDataClient
from .alpha_vantage_client import AlphaVantageClient
from .mock_alpha_vantage_client import MockAlphaVantageClient

__all__ = [
    "BaseDataClient",
    "AlphaVantageClient", 
    "MockAlphaVantageClient"
]
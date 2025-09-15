"""
Output Producers

This package contains output producers for the stock market pipeline.
Each producer handles data preparation and output for specific output topics.
"""

from .stock_prices_producer import StockPricesProducer
from .trading_volume_producer import TradingVolumeProducer
from .technical_indicators_producer import TechnicalIndicatorsProducer

# Package metadata
__version__ = "1.0.0"
__author__ = "Stock Market Pipeline Team"

# Public API
__all__ = [
    "StockPricesProducer",
    "TradingVolumeProducer",
    "TechnicalIndicatorsProducer"
]





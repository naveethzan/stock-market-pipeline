"""
Stock Market Pipeline - Processing Layer (Silver Tier)

This package provides the stream processing layer for the stock market pipeline,
handling real-time data transformation and processing using Spark Structured Streaming.

Architecture:
- StreamConsumer: Main consumer for stream processing (consumes from Kafka)
- Transformations: Common transformation utilities and technical indicators
- Output Producers: Schema-specific producers for different output topics
  - StockPricesProducer: Produces to processed-stock-prices topic
  - TradingVolumeProducer: Produces to processed-trading-volume topic
  - TechnicalIndicatorsProducer: Produces to processed-technical-indicators topic

Usage:
    from stock_market_pipeline.processing import StreamConsumer
    from stock_market_pipeline.processing import StockPricesProducer
    from stock_market_pipeline.processing import calculate_moving_averages
"""

# Core processing components
from .core import StreamConsumer, StreamProcessingService
from .transformations import (
    calculate_price_metrics,
    calculate_moving_averages,
    calculate_technical_indicators,
    calculate_volume_metrics,
    classify_market_data,
    detect_price_anomalies
)

# Output producers
from .producers import (
    StockPricesProducer,
    TradingVolumeProducer,
    TechnicalIndicatorsProducer
)

# Package metadata
__version__ = "1.0.0"
__author__ = "Stock Market Pipeline Team"
__email__ = "team@stockmarketpipeline.com"

# Public API
__all__ = [
    # Core components
    "StreamConsumer",
    "StreamProcessingService",
    
    # Transformations
    "calculate_price_metrics",
    "calculate_moving_averages", 
    "calculate_technical_indicators",
    "calculate_volume_metrics",
    "classify_market_data",
    "detect_price_anomalies",
    
    # Output producers
    "StockPricesProducer",
    "TradingVolumeProducer", 
    "TechnicalIndicatorsProducer",
]

# Package information
def get_version():
    """Get the package version."""
    return __version__

def get_author():
    """Get the package author."""
    return __author__

def get_package_info():
    """Get comprehensive package information."""
    return {
        "name": "stock_market_pipeline.processing",
        "version": __version__,
        "author": __author__,
        "email": __email__,
        "description": "Stream processing layer for stock market pipeline",
        "components": {
            "core": ["StreamConsumer", "StreamProcessingService"],
            "transformations": [
                "calculate_price_metrics",
                "calculate_moving_averages",
                "calculate_technical_indicators",
                "calculate_volume_metrics", 
                "classify_market_data",
                "detect_price_anomalies"
            ],
            "output_producers": [
                "StockPricesProducer",
                "TradingVolumeProducer",
                "TechnicalIndicatorsProducer"
            ]
        }
    }
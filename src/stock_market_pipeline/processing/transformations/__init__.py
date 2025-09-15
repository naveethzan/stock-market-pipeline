"""
Data Transformations

This package contains data transformation functions for the stock market pipeline.
Includes technical indicators, price metrics, and data enrichment functions.
"""

from .transformations import (
    calculate_price_metrics,
    calculate_moving_averages,
    calculate_technical_indicators,
    calculate_volume_metrics,
    classify_market_data,
    detect_price_anomalies
)

# Package metadata
__version__ = "1.0.0"
__author__ = "Stock Market Pipeline Team"

# Public API
__all__ = [
    "calculate_price_metrics",
    "calculate_moving_averages",
    "calculate_technical_indicators",
    "calculate_volume_metrics",
    "classify_market_data",
    "detect_price_anomalies"
]





"""
Core Processing Components

This package contains the core processing components for the stock market pipeline.
Includes the main stream consumer and service orchestration.
"""

from .stream_consumer import StreamConsumer
from .stream_processing_service import StreamProcessingService

# Package metadata
__version__ = "1.0.0"
__author__ = "Stock Market Pipeline Team"

# Public API
__all__ = [
    "StreamConsumer",
    "StreamProcessingService"
]





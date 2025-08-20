"""
Streaming processors package.
Contains Spark Structured Streaming processors for real-time data processing.
"""

from .stream_processor import StreamProcessor, StreamProcessorError

__all__ = [
    "StreamProcessor",
    "StreamProcessorError"
]
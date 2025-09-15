"""
Schema management for the stock market pipeline.
Provides Avro schema management and serialization.
"""

from .schema_manager import SchemaManager
from .avro_serializer import AvroSerializer

__all__ = [
    "SchemaManager",
    "AvroSerializer"
]
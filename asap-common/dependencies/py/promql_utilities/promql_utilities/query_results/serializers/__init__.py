"""
Streaming serialization interfaces for query results.

This module provides multiple serialization formats for query results:
- JSONL + gzip: Compressed streaming format, human-readable
- Parquet: Columnar format for analytics, high compression
- Backward compatibility with pickle format
"""

from .base import ResultsSerializer
from .factory import SerializerFactory, get_available_formats

__all__ = ["ResultsSerializer", "SerializerFactory", "get_available_formats"]

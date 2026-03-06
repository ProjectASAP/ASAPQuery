"""
Factory for creating serializers with automatic format detection.
"""

import os
import logging
from typing import List, Optional
from .base import ResultsSerializer


logger = logging.getLogger(__name__)


def get_available_formats() -> List[str]:
    """Get list of available serialization formats.

    Returns:
        List of format names that can be used
    """
    return ["jsonl", "parquet"]


class SerializerFactory:
    """Factory for creating results serializers."""

    @staticmethod
    def create(format_name: str, output_dir: str, **kwargs) -> ResultsSerializer:
        """Create a serializer for the specified format.

        Args:
            format_name: Format name ('jsonl', 'parquet', or 'auto')
            output_dir: Directory for output files
            **kwargs: Additional arguments passed to serializer

        Returns:
            ResultsSerializer instance

        Raises:
            ValueError: If format is not supported
            ImportError: If required dependencies are missing
        """
        if format_name == "auto":
            format_name = SerializerFactory._detect_format(output_dir)

        if format_name == "jsonl":
            from .jsonl_serializer import JSONLResultsSerializer

            return JSONLResultsSerializer(output_dir, **kwargs)

        elif format_name == "parquet":
            from .parquet_serializer import ParquetResultsSerializer

            return ParquetResultsSerializer(output_dir, **kwargs)

        else:
            available = get_available_formats()
            raise ValueError(
                f"Unsupported format '{format_name}'. Available formats: {available}"
            )

    @staticmethod
    def _detect_format(output_dir: str) -> str:
        """Auto-detect format based on existing files.

        Args:
            output_dir: Directory to check for existing files

        Returns:
            Detected format name, defaults to 'jsonl' if none found
        """
        if not os.path.exists(output_dir):
            return "jsonl"  # Default for new directories

        # Check for Parquet files first (they indicate intent for analytics)
        parquet_files = ["query_results.parquet", "query_latencies.parquet"]

        if any(os.path.exists(os.path.join(output_dir, f)) for f in parquet_files):
            return "parquet"

        # Check for JSONL files
        jsonl_files = [
            "query_results.jsonl.gz",
            "query_results.jsonl",
            "query_latencies.jsonl.gz",
            "query_latencies.jsonl",
        ]

        if any(os.path.exists(os.path.join(output_dir, f)) for f in jsonl_files):
            return "jsonl"

        # Default to JSONL for new directories
        logger.debug(
            f"No existing format detected in {output_dir}, defaulting to JSONL"
        )
        return "jsonl"

    @staticmethod
    def create_from_existing(output_dir: str) -> Optional[ResultsSerializer]:
        """Create serializer by detecting format from existing files.

        Args:
            output_dir: Directory containing existing results

        Returns:
            ResultsSerializer instance, or None if no results found
        """
        if not os.path.exists(output_dir):
            return None

        detected_format = SerializerFactory._detect_format(output_dir)

        try:
            serializer = SerializerFactory.create(detected_format, output_dir)
            if serializer.exists():
                return serializer
        except (ValueError, ImportError) as e:
            logger.warning(
                f"Could not create serializer for detected format {detected_format}: {e}"
            )

        return None

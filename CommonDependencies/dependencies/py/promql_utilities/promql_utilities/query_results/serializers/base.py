"""
Abstract base class for results serializers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict
from ..classes import QueryResultAcrossTime, LatencyResultAcrossTime, QueryResult


class ResultsSerializer(ABC):
    """Abstract interface for query results serialization."""

    def __init__(self, output_dir: str):
        """Initialize serializer with output directory.

        Args:
            output_dir: Directory where results will be written
        """
        self.output_dir = output_dir

    @abstractmethod
    def write_results(
        self, results_across_servers: Dict[str, Dict[int, QueryResultAcrossTime]]
    ) -> None:
        """Write query results to storage.

        Args:
            results_across_servers: Nested dict of server -> query_idx -> QueryResultAcrossTime
        """
        pass

    @abstractmethod
    def read_results(self) -> Dict[str, Dict[int, QueryResultAcrossTime]]:
        """Read query results from storage.

        Returns:
            Nested dict of server -> query_idx -> QueryResultAcrossTime
        """
        pass

    @abstractmethod
    def exists(self) -> bool:
        """Check if serialized results exist.

        Returns:
            True if results exist and can be read
        """
        pass

    @abstractmethod
    def streaming_write_start(self, metadata: Dict[str, Any]) -> None:
        """Initialize streaming write session with experiment metadata.

        Args:
            metadata: Experiment metadata containing queries, servers, repetitions, etc.
        """
        pass

    @abstractmethod
    def streaming_write_result(self, query_result: QueryResult) -> None:
        """Write a single query result incrementally.

        Args:
            query_result: Individual query result to write
        """
        pass

    @abstractmethod
    def streaming_write_end(self) -> None:
        """Finalize streaming write session and close any open resources."""
        pass

    def cleanup(self) -> None:
        """Clean up any resources. Override if needed."""
        pass

    @abstractmethod
    def read_latencies_only(self) -> Dict[str, Dict[int, LatencyResultAcrossTime]]:
        """Read only latency data without loading full results.

        Returns:
            Nested dict of server -> query_idx -> LatencyResultAcrossTime
        """
        pass

"""
JSONL+gzip streaming serializer for query results.
"""

import json
import gzip
import os
import threading
from typing import Any, Dict, Iterator
from .base import ResultsSerializer
from ..classes import (
    QueryResult,
    QueryResultAcrossTime,
    LatencyResult,
    LatencyResultAcrossTime,
)


class JSONLResultsSerializer(ResultsSerializer):
    """JSONL+gzip streaming serializer for query results."""

    def __init__(self, output_dir: str, use_compression: bool = True):
        """Initialize JSONL serializer.

        Args:
            output_dir: Directory for output files
            use_compression: Whether to use gzip compression
        """
        super().__init__(output_dir)
        self.use_compression = use_compression
        self.results_file = os.path.join(output_dir, "query_results.jsonl")
        self.latency_file = os.path.join(output_dir, "query_latencies.jsonl")
        self.metadata_file = os.path.join(output_dir, "experiment_metadata.json")

        if use_compression:
            self.results_file += ".gz"
            self.latency_file += ".gz"

        os.makedirs(output_dir, exist_ok=True)

        # Streaming write state
        self._streaming_results_file = None
        self._streaming_latency_file = None
        self._streaming_metadata = None

        # Thread safety for streaming writes
        self._write_lock = threading.Lock()

    def _open_for_write(self, filepath: str):
        """Open file for writing with optional compression."""
        if self.use_compression:
            return gzip.open(filepath, "wt", encoding="utf-8")
        return open(filepath, "w", encoding="utf-8")

    def _open_for_read(self, filepath: str):
        """Open file for reading with optional compression."""
        if self.use_compression:
            return gzip.open(filepath, "rt", encoding="utf-8")
        return open(filepath, "r", encoding="utf-8")

    def write_results(
        self, results_across_servers: Dict[str, Dict[int, QueryResultAcrossTime]]
    ) -> None:
        """Write query results to JSONL files.

        Args:
            results_across_servers: Nested dict of server -> query_idx -> QueryResultAcrossTime
        """
        # Write metadata
        self._write_metadata(results_across_servers)

        # Write results and latencies
        with self._open_for_write(self.results_file) as results_f, self._open_for_write(
            self.latency_file
        ) as latency_f:
            for server_name, server_results in results_across_servers.items():
                for query_idx, query_result_across_time in server_results.items():
                    for query_result in query_result_across_time.query_results:
                        # Write result record
                        if query_result.result:
                            for frozenset_key, value in query_result.result.items():
                                result_record = {
                                    "server_name": server_name,
                                    "query": query_result.query,
                                    "query_idx": query_idx,
                                    "repetition_idx": query_result.repetition_idx,
                                    "result_labels": self._serialize_frozenset_key(
                                        frozenset_key
                                    ),
                                    "result_value": value,
                                }
                                results_f.write(json.dumps(result_record) + "\n")

                        # Write latency record
                        latency_record = {
                            "server_name": server_name,
                            "query_idx": query_idx,
                            "repetition_idx": query_result.repetition_idx,
                            "latency": query_result.latency,
                            "cumulative_latency": query_result.cumulative_latency,
                        }
                        latency_f.write(json.dumps(latency_record) + "\n")

    def read_results(self) -> Dict[str, Dict[int, QueryResultAcrossTime]]:
        """Read query results from JSONL files.

        Returns:
            Nested dict of server -> query_idx -> QueryResultAcrossTime
        """
        if not self.exists():
            raise FileNotFoundError(f"No results found in {self.output_dir}")

        # Read metadata
        metadata = self._read_metadata()

        # Handle both old and new metadata formats
        if "query_groups" in metadata:
            # New format with query groups
            all_queries = []
            query_idx_to_repetitions = {}
            global_query_idx = 0

            for qg in metadata["query_groups"]:
                for query in qg["queries"]:
                    all_queries.append(query)
                    query_idx_to_repetitions[global_query_idx] = qg["repetitions"]
                    global_query_idx += 1

            servers = metadata["servers"]
        else:
            # Old format (backward compatible)
            all_queries = metadata["queries"]
            servers = metadata["servers"]
            query_idx_to_repetitions = {
                i: metadata["repetitions"] for i in range(len(all_queries))
            }

        # Initialize nested structure
        results = {}
        for server in servers:
            results[server] = {}
            for query_idx, query in enumerate(all_queries):
                results[server][query_idx] = QueryResultAcrossTime(
                    server,
                    query,
                    query_idx,
                    query_idx_to_repetitions[query_idx],
                )

        # Read latencies into lookup table
        latencies = {}
        if os.path.exists(self.latency_file):
            with self._open_for_read(self.latency_file) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        latency_record = json.loads(line)
                        key = (
                            latency_record["server_name"],
                            latency_record["query_idx"],
                            latency_record["repetition_idx"],
                        )
                        latencies[key] = (
                            latency_record["latency"],
                            latency_record["cumulative_latency"],
                        )

        # Read results and reconstruct QueryResult objects
        query_results = {}  # (server, query_idx, repetition_idx) -> partial QueryResult

        if os.path.exists(self.results_file):
            with self._open_for_read(self.results_file) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        result_record = json.loads(line)

                        key = (
                            result_record["server_name"],
                            result_record["query_idx"],
                            result_record["repetition_idx"],
                        )

                        # Check if this is a raw_text_result (SQL/ClickHouse) record
                        is_raw_text = "raw_text_result" in result_record

                        # Initialize QueryResult if not exists
                        if key not in query_results:
                            latency, cumulative_latency = latencies.get(
                                key, (None, None)
                            )
                            query_results[key] = QueryResult(
                                server_name=result_record["server_name"],
                                query=result_record["query"],
                                query_idx=result_record["query_idx"],
                                repetition_idx=result_record["repetition_idx"],
                                result=None,  # Will be populated below for Prometheus
                                latency=latency,
                                cumulative_latency=cumulative_latency,
                                query_group_idx=result_record.get("query_group_idx", 0),
                                raw_text_result=None,  # Will be populated for SQL
                            )
                            if not is_raw_text:
                                query_results[key].result = {}

                        if is_raw_text:
                            # SQL/ClickHouse raw text result
                            query_results[key].raw_text_result = result_record[
                                "raw_text_result"
                            ]
                        else:
                            # Prometheus-style result
                            frozenset_key = self._deserialize_frozenset_key(
                                result_record["result_labels"]
                            )
                            query_results[key].result[frozenset_key] = result_record[
                                "result_value"
                            ]

        # Add QueryResult objects to the nested structure
        for (
            server_name,
            query_idx,
            repetition_idx,
        ), query_result in query_results.items():
            results[server_name][query_idx].add_result(query_result)

        # Handle cases where we have latencies but no results
        for (server_name, query_idx, repetition_idx), (
            latency,
            cumulative_latency,
        ) in latencies.items():
            if (server_name, query_idx, repetition_idx) not in query_results:
                # Create empty QueryResult with just latency data
                empty_result = QueryResult(
                    server_name=server_name,
                    query=all_queries[query_idx],
                    query_idx=query_idx,
                    repetition_idx=repetition_idx,
                    result=None,
                    latency=latency,
                    cumulative_latency=cumulative_latency,
                    query_group_idx=0,  # Default for backward compatibility
                )
                results[server_name][query_idx].add_result(empty_result)

        return results

    def exists(self) -> bool:
        """Check if serialized results exist.

        Returns:
            True if results exist and can be read
        """
        return os.path.exists(self.metadata_file) and (
            os.path.exists(self.results_file) or os.path.exists(self.latency_file)
        )

    def streaming_write_start(self, metadata: Dict[str, Any]) -> None:
        """Initialize streaming write session with experiment metadata.

        Args:
            metadata: Experiment metadata containing queries, servers, repetitions, etc.
        """
        if (
            self._streaming_results_file is not None
            or self._streaming_latency_file is not None
        ):
            raise RuntimeError("Streaming write session already active")

        self._streaming_metadata = metadata
        self._streaming_results_file = self._open_for_write(self.results_file)
        self._streaming_latency_file = self._open_for_write(self.latency_file)

    def streaming_write_result(self, query_result: QueryResult) -> None:
        """Write a single query result incrementally.

        Args:
            query_result: Individual query result to write
        """
        if self._streaming_results_file is None or self._streaming_latency_file is None:
            raise RuntimeError("Streaming write session not started")

        with self._write_lock:
            # Write result records - handle both Prometheus (result) and SQL (raw_text_result)
            if query_result.result:
                # Prometheus-style normalized results
                for frozenset_key, value in query_result.result.items():
                    result_record = {
                        "query_group_idx": query_result.query_group_idx,
                        "server_name": query_result.server_name,
                        "query": query_result.query,
                        "query_idx": query_result.query_idx,
                        "repetition_idx": query_result.repetition_idx,
                        "result_labels": self._serialize_frozenset_key(frozenset_key),
                        "result_value": value,
                    }
                    self._streaming_results_file.write(json.dumps(result_record) + "\n")
            elif query_result.raw_text_result is not None:
                # SQL/ClickHouse raw text result
                result_record = {
                    "query_group_idx": query_result.query_group_idx,
                    "server_name": query_result.server_name,
                    "query": query_result.query,
                    "query_idx": query_result.query_idx,
                    "repetition_idx": query_result.repetition_idx,
                    "raw_text_result": query_result.raw_text_result,
                }
                self._streaming_results_file.write(json.dumps(result_record) + "\n")

            # Write latency record
            latency_record = {
                "query_group_idx": query_result.query_group_idx,
                "server_name": query_result.server_name,
                "query_idx": query_result.query_idx,
                "repetition_idx": query_result.repetition_idx,
                "latency": query_result.latency,
                "cumulative_latency": query_result.cumulative_latency,
            }
            self._streaming_latency_file.write(json.dumps(latency_record) + "\n")

    def streaming_write_end(self) -> None:
        """Finalize streaming write session and close any open resources."""
        if self._streaming_results_file is not None:
            self._streaming_results_file.close()
            self._streaming_results_file = None

        if self._streaming_latency_file is not None:
            self._streaming_latency_file.close()
            self._streaming_latency_file = None

        # Write metadata at the end
        if self._streaming_metadata is not None:
            with open(self.metadata_file, "w") as f:
                json.dump(self._streaming_metadata, f, indent=2)
            self._streaming_metadata = None

    def stream_results(self) -> Iterator[Dict]:
        """Stream read query results one record at a time.

        Yields:
            Dict containing result record data
        """
        if not os.path.exists(self.results_file):
            return

        with self._open_for_read(self.results_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)

    def stream_latencies(self) -> Iterator[Dict]:
        """Stream read latency data one record at a time.

        Yields:
            Dict containing latency record data
        """
        if not os.path.exists(self.latency_file):
            return

        with self._open_for_read(self.latency_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)

    def _write_metadata(
        self, results_across_servers: Dict[str, Dict[int, QueryResultAcrossTime]]
    ):
        """Write experiment metadata."""
        if not results_across_servers:
            return

        servers = list(results_across_servers.keys())
        queries = []
        repetitions = 0

        if servers:
            first_server = servers[0]
            if results_across_servers[first_server]:
                query_indices = sorted(results_across_servers[first_server].keys())
                queries = [
                    results_across_servers[first_server][i].query for i in query_indices
                ]
                if query_indices:
                    repetitions = results_across_servers[first_server][
                        query_indices[0]
                    ].num_repetitions

        metadata = {
            "queries": queries,
            "servers": servers,
            "repetitions": repetitions,
            "total_queries": len(queries),
        }

        with open(self.metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

    def _read_metadata(self) -> Dict:
        """Read experiment metadata."""
        with open(self.metadata_file, "r") as f:
            return json.load(f)

    def _serialize_frozenset_key(self, frozenset_key: frozenset) -> str:
        """Convert frozenset key to JSON string.

        Args:
            frozenset_key: frozenset of (key, value) tuples

        Returns:
            JSON string representation
        """
        # Convert to dict and serialize as JSON with sorted keys for consistency
        labels_dict = dict(frozenset_key)
        return json.dumps(labels_dict, sort_keys=True)

    def _deserialize_frozenset_key(self, json_str: str) -> frozenset:
        """Convert JSON string back to frozenset key.

        Args:
            json_str: JSON string representation

        Returns:
            frozenset of (key, value) tuples
        """
        labels_dict = json.loads(json_str)
        return frozenset(labels_dict.items())

    def read_latencies_only(self) -> Dict[str, Dict[int, LatencyResultAcrossTime]]:
        """Read only latency data without loading full results.

        Returns:
            Nested dict of server -> query_idx -> LatencyResultAcrossTime
        """
        if not self.exists():
            raise FileNotFoundError(f"No results found in {self.output_dir}")

        # Read metadata
        metadata = self._read_metadata()

        # Handle both old and new metadata formats
        if "query_groups" in metadata:
            # New format with query groups
            all_queries = []
            query_idx_to_repetitions = {}
            global_query_idx = 0

            for qg in metadata["query_groups"]:
                for query in qg["queries"]:
                    all_queries.append(query)
                    query_idx_to_repetitions[global_query_idx] = qg["repetitions"]
                    global_query_idx += 1

            servers = metadata["servers"]
        else:
            # Old format (backward compatible)
            all_queries = metadata["queries"]
            servers = metadata["servers"]
            query_idx_to_repetitions = {
                i: metadata["repetitions"] for i in range(len(all_queries))
            }

        # Initialize nested structure
        latencies = {}
        for server in servers:
            latencies[server] = {}
            for query_idx, query in enumerate(all_queries):
                latencies[server][query_idx] = LatencyResultAcrossTime(
                    server,
                    query,
                    query_idx,
                    query_idx_to_repetitions[query_idx],
                )

        # Read only latency data
        if os.path.exists(self.latency_file):
            with self._open_for_read(self.latency_file) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        latency_record = json.loads(line)

                        latency_result = LatencyResult(
                            server_name=latency_record["server_name"],
                            query=all_queries[latency_record["query_idx"]],
                            query_idx=latency_record["query_idx"],
                            repetition_idx=latency_record["repetition_idx"],
                            latency=latency_record["latency"],
                            cumulative_latency=latency_record["cumulative_latency"],
                            query_group_idx=latency_record.get("query_group_idx", 0),
                        )

                        latencies[latency_record["server_name"]][
                            latency_record["query_idx"]
                        ].add_latency_result(latency_result)

        return latencies

"""
Parquet serializer for query results using JSON columns for labels.
"""

import json
import os
import threading
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from .base import ResultsSerializer
from ..classes import (
    QueryResult,
    QueryResultAcrossTime,
    LatencyResult,
    LatencyResultAcrossTime,
)


class ParquetResultsSerializer(ResultsSerializer):
    """Parquet serializer for query results with JSON column for labels."""

    def __init__(
        self, output_dir: str, compression: str = "snappy", batch_size: int = 1000
    ):
        """Initialize Parquet serializer.

        Args:
            output_dir: Directory for output files
            compression: Compression algorithm ('snappy', 'gzip', 'lz4', etc.)
            batch_size: Number of records to batch before writing to parquet
        """
        super().__init__(output_dir)
        self.compression = compression
        self.batch_size = batch_size
        self.results_file = os.path.join(output_dir, "query_results.parquet")
        self.latency_file = os.path.join(output_dir, "query_latencies.parquet")
        self.metadata_file = os.path.join(output_dir, "experiment_metadata.json")

        os.makedirs(output_dir, exist_ok=True)

        # Streaming write state
        self._streaming_results_writer: Optional[pq.ParquetWriter] = None
        self._streaming_latency_writer: Optional[pq.ParquetWriter] = None
        self._results_batch: List[Dict] = []
        self._latency_batch: List[Dict] = []
        self._streaming_metadata = None

        # Define schemas for streaming
        self._results_schema = pa.schema(
            [
                ("query_group_idx", pa.int64()),
                ("server_name", pa.string()),
                ("query", pa.string()),
                ("query_idx", pa.int64()),
                ("repetition_idx", pa.int64()),
                ("result_labels", pa.string()),
                ("result_value", pa.float64()),
            ]
        )

        self._latency_schema = pa.schema(
            [
                ("query_group_idx", pa.int64()),
                ("server_name", pa.string()),
                ("query_idx", pa.int64()),
                ("repetition_idx", pa.int64()),
                ("latency", pa.float64()),
                ("cumulative_latency", pa.float64()),
            ]
        )

        # Thread safety for streaming writes
        self._write_lock = threading.Lock()

    def write_results(
        self, results_across_servers: Dict[str, Dict[int, QueryResultAcrossTime]]
    ) -> None:
        """Write query results to Parquet files.

        Args:
            results_across_servers: Nested dict of server -> query_idx -> QueryResultAcrossTime
        """
        # Write metadata
        self._write_metadata(results_across_servers)

        results_rows = []
        latency_rows = []

        for server_name, server_results in results_across_servers.items():
            for query_idx, query_result_across_time in server_results.items():
                query = query_result_across_time.query

                for query_result in query_result_across_time.query_results:
                    # Process query results
                    if query_result.result:
                        for frozenset_key, value in query_result.result.items():
                            # Convert frozenset to JSON string
                            labels_dict = dict(frozenset_key)
                            labels_json = json.dumps(labels_dict, sort_keys=True)

                            results_rows.append(
                                {
                                    "server_name": server_name,
                                    "query": query,
                                    "query_idx": query_idx,
                                    "repetition_idx": query_result.repetition_idx,
                                    "result_labels": labels_json,
                                    "result_value": value,
                                }
                            )

                    # Process latency data separately
                    latency_rows.append(
                        {
                            "server_name": server_name,
                            "query_idx": query_idx,
                            "repetition_idx": query_result.repetition_idx,
                            "latency": query_result.latency,
                            "cumulative_latency": query_result.cumulative_latency,
                        }
                    )

        # Write results DataFrame
        if results_rows:
            results_df = pd.DataFrame(results_rows)
            results_df.to_parquet(
                self.results_file, compression=self.compression, index=False
            )

        # Write latencies DataFrame
        if latency_rows:
            latency_df = pd.DataFrame(latency_rows)
            latency_df.to_parquet(
                self.latency_file, compression=self.compression, index=False
            )

    def read_results(self) -> Dict[str, Dict[int, QueryResultAcrossTime]]:
        """Read query results from Parquet files.

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

        # Read latencies
        latencies = {}
        if os.path.exists(self.latency_file):
            latency_df = pd.read_parquet(self.latency_file)
            for _, row in latency_df.iterrows():
                key = (row["server_name"], row["query_idx"], row["repetition_idx"])
                latencies[key] = (row["latency"], row["cumulative_latency"])

        # Read results and reconstruct QueryResult objects
        query_results = {}  # (server, query_idx, repetition_idx) -> QueryResult

        if os.path.exists(self.results_file):
            results_df = pd.read_parquet(self.results_file)

            for _, row in results_df.iterrows():
                key = (row["server_name"], row["query_idx"], row["repetition_idx"])

                # Initialize QueryResult if not exists
                if key not in query_results:
                    latency, cumulative_latency = latencies.get(key, (None, None))
                    query_results[key] = QueryResult(
                        server_name=row["server_name"],
                        query=row["query"],
                        query_idx=row["query_idx"],
                        repetition_idx=row["repetition_idx"],
                        result=None,  # Will be populated below
                        latency=latency,
                        cumulative_latency=cumulative_latency,
                        query_group_idx=row.get("query_group_idx", 0),
                    )
                    query_results[key].result = {}

                # Parse labels back to frozenset
                labels_dict = json.loads(row["result_labels"])
                frozenset_key = frozenset(labels_dict.items())
                query_results[key].result[frozenset_key] = row["result_value"]

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
            self._streaming_results_writer is not None
            or self._streaming_latency_writer is not None
        ):
            raise RuntimeError("Streaming write session already active")

        self._streaming_metadata = metadata
        self._results_batch = []
        self._latency_batch = []

        # Initialize ParquetWriter instances with schemas
        self._streaming_results_writer = pq.ParquetWriter(
            self.results_file, schema=self._results_schema, compression=self.compression
        )
        self._streaming_latency_writer = pq.ParquetWriter(
            self.latency_file, schema=self._latency_schema, compression=self.compression
        )

    def streaming_write_result(self, query_result: QueryResult) -> None:
        """Write a single query result incrementally.

        Args:
            query_result: Individual query result to write
        """
        if (
            self._streaming_results_writer is None
            or self._streaming_latency_writer is None
        ):
            raise RuntimeError("Streaming write session not started")

        with self._write_lock:
            # Add result records to batch
            if query_result.result:
                for frozenset_key, value in query_result.result.items():
                    labels_dict = dict(frozenset_key)
                    labels_json = json.dumps(labels_dict, sort_keys=True)

                    self._results_batch.append(
                        {
                            "query_group_idx": query_result.query_group_idx,
                            "server_name": query_result.server_name,
                            "query": query_result.query,
                            "query_idx": query_result.query_idx,
                            "repetition_idx": query_result.repetition_idx,
                            "result_labels": labels_json,
                            "result_value": value,
                        }
                    )

            # Add latency record to batch
            self._latency_batch.append(
                {
                    "query_group_idx": query_result.query_group_idx,
                    "server_name": query_result.server_name,
                    "query_idx": query_result.query_idx,
                    "repetition_idx": query_result.repetition_idx,
                    "latency": query_result.latency,
                    "cumulative_latency": query_result.cumulative_latency,
                }
            )

            # Flush batches if they reach batch_size
            if len(self._results_batch) >= self.batch_size:
                self._flush_results_batch()
            if len(self._latency_batch) >= self.batch_size:
                self._flush_latency_batch()

    def streaming_write_end(self) -> None:
        """Finalize streaming write session and close any open resources."""
        # Flush any remaining batches
        if self._results_batch:
            self._flush_results_batch()
        if self._latency_batch:
            self._flush_latency_batch()

        # Close writers
        if self._streaming_results_writer is not None:
            self._streaming_results_writer.close()
            self._streaming_results_writer = None

        if self._streaming_latency_writer is not None:
            self._streaming_latency_writer.close()
            self._streaming_latency_writer = None

        # Write metadata at the end
        if self._streaming_metadata is not None:
            with open(self.metadata_file, "w") as f:
                json.dump(self._streaming_metadata, f, indent=2)
            self._streaming_metadata = None

    def _flush_results_batch(self) -> None:
        """Write current results batch to parquet."""
        if self._results_batch and self._streaming_results_writer is not None:
            results_df = pd.DataFrame(self._results_batch)
            table = pa.Table.from_pandas(results_df, schema=self._results_schema)
            self._streaming_results_writer.write_table(table)
            self._results_batch = []

    def _flush_latency_batch(self) -> None:
        """Write current latency batch to parquet."""
        if self._latency_batch and self._streaming_latency_writer is not None:
            latency_df = pd.DataFrame(self._latency_batch)
            table = pa.Table.from_pandas(latency_df, schema=self._latency_schema)
            self._streaming_latency_writer.write_table(table)
            self._latency_batch = []

    def query_results(self, filters=None, columns=None) -> pd.DataFrame:
        """Query results with optional filtering and column selection.

        Args:
            filters: PyArrow filters for row selection
            columns: List of column names to read

        Returns:
            Pandas DataFrame with query results
        """
        if not os.path.exists(self.results_file):
            return pd.DataFrame()

        return pd.read_parquet(self.results_file, filters=filters, columns=columns)

    def query_latencies(self, filters=None, columns=None) -> pd.DataFrame:
        """Query latencies with optional filtering and column selection.

        Args:
            filters: PyArrow filters for row selection
            columns: List of column names to read

        Returns:
            Pandas DataFrame with latency data
        """
        if not os.path.exists(self.latency_file):
            return pd.DataFrame()

        return pd.read_parquet(self.latency_file, filters=filters, columns=columns)

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
            latency_df = pd.read_parquet(self.latency_file)
            for _, row in latency_df.iterrows():
                latency_result = LatencyResult(
                    server_name=row["server_name"],
                    query=all_queries[row["query_idx"]],
                    query_idx=row["query_idx"],
                    repetition_idx=row["repetition_idx"],
                    latency=row["latency"],
                    cumulative_latency=row["cumulative_latency"],
                    query_group_idx=row.get("query_group_idx", 0),
                )

                latencies[row["server_name"]][row["query_idx"]].add_latency_result(
                    latency_result
                )

        return latencies

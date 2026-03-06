import numpy as np
from typing import List, Dict, Optional, Set


class TimeSeries:
    def __init__(self, key: frozenset, values: List[Optional[float]]):
        self.key = key
        self.values = np.array(values)


class QueryResult:
    def __init__(
        self,
        server_name: str,
        query: str,
        query_idx: int,
        repetition_idx: int,
        result: Optional[List[Dict]],
        latency: Optional[float],
        cumulative_latency: Optional[float],
        query_group_idx: int = 0,
        raw_text_result: Optional[str] = None,
    ):
        self.server_name = server_name
        self.query = query
        self.query_idx = query_idx
        self.repetition_idx = repetition_idx
        self.query_group_idx = query_group_idx
        self.latency = latency
        self.cumulative_latency = cumulative_latency
        self.raw_text_result = raw_text_result

        self.result: Optional[Dict[frozenset, float]] = None
        if result:
            self.result = {
                frozenset(result_per_key["metric"].items()): float(
                    result_per_key["value"][1]
                )
                for result_per_key in result
            }


class QueryResultAcrossTime:
    def __init__(self, server_name, query, query_idx, num_repetitions):
        self.server_name = server_name
        self.query = query
        self.query_idx = query_idx
        self.num_repetitions = num_repetitions
        self.query_results: List[QueryResult] = []

    def add_result(self, query_result: QueryResult):
        self.query_results.append(query_result)

    def get_all_timeseries(self) -> Dict[frozenset, TimeSeries]:
        keys: Set[frozenset] = set()
        for query_result in self.query_results:
            if query_result.result:
                keys.update(query_result.result.keys())

        assert len(self.query_results) == self.num_repetitions
        ret: Dict[frozenset, TimeSeries] = {}
        intermediate_ret: Dict[frozenset, List[Optional[float]]] = {
            k: [None for _ in range(self.num_repetitions)] for k in keys
        }

        for k in keys:
            for repetition_idx, result in enumerate(self.query_results):
                if result.result:
                    intermediate_ret[k][repetition_idx] = result.result[k]

            ret[k] = TimeSeries(k, intermediate_ret[k])

        return ret


class LatencyResult:
    """Represents latency data for a single query execution."""

    def __init__(
        self,
        server_name: str,
        query: str,
        query_idx: int,
        repetition_idx: int,
        latency: Optional[float],
        cumulative_latency: Optional[float],
        query_group_idx: int = 0,
    ):
        self.server_name = server_name
        self.query = query
        self.query_idx = query_idx
        self.repetition_idx = repetition_idx
        self.query_group_idx = query_group_idx
        self.latency = latency
        self.cumulative_latency = cumulative_latency


class LatencyResultAcrossTime:
    """Represents latency data for a query across multiple repetitions."""

    def __init__(
        self, server_name: str, query: str, query_idx: int, num_repetitions: int
    ):
        self.server_name = server_name
        self.query = query
        self.query_idx = query_idx
        self.num_repetitions = num_repetitions
        self.latency_results: List[LatencyResult] = []

    def add_latency_result(self, latency_result: LatencyResult):
        """Add a latency result for a specific repetition."""
        self.latency_results.append(latency_result)

    def get_latencies(self) -> List[Optional[float]]:
        """Get list of latencies across all repetitions."""
        return [lr.latency for lr in self.latency_results]

    def get_cumulative_latencies(self) -> List[Optional[float]]:
        """Get list of cumulative latencies across all repetitions."""
        return [lr.cumulative_latency for lr in self.latency_results]

    @classmethod
    def from_query_result_across_time(
        cls, qrat: "QueryResultAcrossTime"
    ) -> "LatencyResultAcrossTime":
        """Create LatencyResultAcrossTime from existing QueryResultAcrossTime."""
        latency_result_across_time = cls(
            qrat.server_name, qrat.query, qrat.query_idx, qrat.num_repetitions
        )

        for query_result in qrat.query_results:
            latency_result = LatencyResult(
                server_name=query_result.server_name,
                query=query_result.query,
                query_idx=query_result.query_idx,
                repetition_idx=query_result.repetition_idx,
                latency=query_result.latency,
                cumulative_latency=query_result.cumulative_latency,
                query_group_idx=query_result.query_group_idx,
            )
            latency_result_across_time.add_latency_result(latency_result)

        return latency_result_across_time

import re

import numpy as np
from typing import List, Dict, Optional, Set

# Parens, commas and the keywords that bound a SELECT list.
_SELECT_LIST_TOKEN = re.compile(r"\(|\)|,|\bSELECT\b|\bFROM\b", re.IGNORECASE)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _outer_select_items(query: str) -> List[str]:
    """Items of the outermost SELECT list.

    e.g. "SELECT job, QUANTILE(0.9, value) FROM t GROUP BY job"
    -> ["job", "QUANTILE(0.9, value)"]
    """
    depth = 0
    item_start: Optional[int] = None
    items: List[str] = []
    for token in _SELECT_LIST_TOKEN.finditer(query):
        word = token.group().upper()
        if word == "(":
            depth += 1
        elif word == ")":
            depth -= 1
        elif depth > 0:
            continue
        elif item_start is None:
            if word == "SELECT":
                item_start = token.end()
        elif word in (",", "FROM"):
            items.append(query[item_start : token.start()].strip())
            if word == "FROM":
                return items
            item_start = token.end()
    raise ValueError(f"No top-level SELECT ... FROM in query: {query!r}")


def _sql_column_label_names(query: str) -> List[Optional[str]]:
    """Label name for each result column, None for the single value column.

    Bare column names in the SELECT list are the GROUP BY labels; the one
    other item is the aggregate. Names come from the query, not the result,
    because TabSeparated output has no header.
    """
    names = [
        item if _IDENTIFIER.fullmatch(item) else None
        for item in _outer_select_items(query)
    ]
    if names.count(None) != 1:
        raise ValueError(
            f"Expected exactly one non-label column in SELECT list of {query!r}"
        )
    return names


def parse_tabseparated_result(query: str, raw_text: str) -> Dict[frozenset, float]:
    """Key ClickHouse TabSeparated rows like `QueryResult.result`.

    e.g. for "SELECT job, SUM(value) FROM t GROUP BY job", the row "api\t3"
    becomes {frozenset({("job", "api")}): 3.0}. Columns are read in SELECT
    list order, as ClickHouse returns them.
    """
    names = _sql_column_label_names(query)
    value_idx = names.index(None)
    result: Dict[frozenset, float] = {}
    for row in raw_text.splitlines():
        fields = row.split("\t")
        if len(fields) != len(names):
            raise ValueError(
                f"Row {row!r} has {len(fields)} columns, query {query!r} "
                f"selects {len(names)}"
            )
        key = frozenset(
            (name, field) for name, field in zip(names, fields) if name is not None
        )
        if key in result:
            raise ValueError(f"Duplicate label set {sorted(key)} in SQL result")
        result[key] = float(fields[value_idx])
    return result


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

    def keyed_result(self) -> Optional[Dict[frozenset, float]]:
        """`result`, or for SQL queries the parsed `raw_text_result`."""
        if self.raw_text_result is None:
            return self.result
        return parse_tabseparated_result(self.query, self.raw_text_result)


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
        keyed_results = [qr.keyed_result() for qr in self.query_results]
        keys: Set[frozenset] = set()
        for keyed_result in keyed_results:
            if keyed_result:
                keys.update(keyed_result.keys())

        assert len(self.query_results) == self.num_repetitions
        ret: Dict[frozenset, TimeSeries] = {}
        intermediate_ret: Dict[frozenset, List[Optional[float]]] = {
            k: [None for _ in range(self.num_repetitions)] for k in keys
        }

        for k in keys:
            for repetition_idx, keyed_result in enumerate(keyed_results):
                # A key absent from one repetition (e.g. a series that appears
                # mid-run) stays None for that repetition.
                if keyed_result:
                    intermediate_ret[k][repetition_idx] = keyed_result.get(k)

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

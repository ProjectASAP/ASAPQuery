"""Type aliases for PrometheusClient codebase."""

from typing import Dict, Any, Callable
from typing_extensions import TypeAlias

# Server and connection types
ServerName: TypeAlias = str
ServerURL: TypeAlias = str
ServerDict: TypeAlias = Dict[str, Any]  # Dict of server_name -> QueryClient

# Query related types
Query: TypeAlias = str
QueryIndex: TypeAlias = int
RepetitionIndex: TypeAlias = int
UnixTimestamp: TypeAlias = int

# Result types
ResultDict: TypeAlias = Dict[
    str, Dict[int, Any]
]  # Dict[server_name][query_idx] -> QueryResultAcrossTime
SimilarityScores: TypeAlias = Dict[
    str, Dict[str, float]
]  # Dict[function_name][query] -> score

# Configuration types
QueryStartTimes: TypeAlias = Dict[str, float]  # Dict[query] -> start_time
AggregationConfig: TypeAlias = Dict[str, Any]
QueryEngineConfig: TypeAlias = Dict[str, Any]

# Function types
SimilarityFunction: TypeAlias = Callable[[Any, Any], float]

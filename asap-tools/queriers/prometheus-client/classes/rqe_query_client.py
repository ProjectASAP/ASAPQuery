import json
import os
from typing import Any, Dict, Optional, Tuple

import requests

from .prometheus_query_client import fetch_runtime_info, normalize_vector
from .query_client import QueryClient, QueryResponse

# The `protocol` of a config server answered by RqeQueryClient.
RQE_PROTOCOL = "rqe"
RQE_QUERY_PATH = "/api/v1/rqe_query"
# The only result type the route answers with; results compare as vectors.
VECTOR_RESULT_TYPE = "vector"
# (connect, read): the engine answers from precomputed sketches, but a dead
# server must still fail instead of hanging the querier.
RQE_QUERY_TIMEOUT_SECS: Tuple[float, float] = (10, 300)


def load_rqe_exprs(path: str) -> Dict[str, Any]:
    """Read a side file: a JSON object mapping config query text to its RQE
    expression tree."""
    with open(path) as f:
        exprs = json.load(f)
    if not isinstance(exprs, dict):
        raise ValueError(f"{path}: expected a JSON object of query text -> expr")
    return exprs


def resolve_rqe_exprs_file(config_file: str, rqe_exprs_file: str) -> str:
    """A relative side-file path is relative to the client config's directory,
    so the two travel (and are mounted) together."""
    return os.path.join(os.path.dirname(os.path.abspath(config_file)), rqe_exprs_file)


class RqeQueryClient(QueryClient):
    """Sends each query's RQE expression tree to ASAPQuery's RQE route.

    Queries keep their PromQL/SQL text in the config; the side file maps that
    text to the tree, so results are keyed and scored as for PromQL/SQL. The
    key is the config's unrendered text (`config_query`), not the query sent:
    templated SQL renders differently on every run (its `{{ end_time }}`), so
    only the config text is stable across runs.
    """

    def __init__(
        self,
        server_url: str,
        server_name: str,
        rqe_exprs_file: str,
        timeout: Tuple[float, float] = RQE_QUERY_TIMEOUT_SECS,
        **kwargs: Any,
    ):
        super().__init__(server_url, server_name)
        self.rqe_exprs_file = rqe_exprs_file
        self._exprs = load_rqe_exprs(rqe_exprs_file)
        self.timeout = timeout
        self._session = requests.Session()

    @property
    def protocol_name(self) -> str:
        return RQE_PROTOCOL

    @property
    def session(self) -> requests.Session:
        """Access to underlying requests Session for mounting debug adapters."""
        return self._session

    def execute_query(
        self,
        query: str,
        query_time: Optional[int] = None,
        config_query: Optional[str] = None,
    ) -> QueryResponse:
        """
        POST the tree for `config_query` (else `query`) evaluated at `query_time`.

        Returns:
            QueryResponse with normalized data; raw_response is the
            Prometheus-format result list, as from PrometheusQueryClient
        """
        key = query if config_query is None else config_query
        if key not in self._exprs:
            return QueryResponse(
                success=False,
                data=None,
                error_message=f"no RQE expression for query {key!r} in "
                f"{self.rqe_exprs_file}",
            )
        if query_time is None:
            return QueryResponse(
                success=False,
                data=None,
                error_message="RQE queries need an evaluation time",
            )
        try:
            response = self._session.post(
                f"{self.server_url}{RQE_QUERY_PATH}",
                json={"expr": self._exprs[key], "time": float(query_time)},
                timeout=self.timeout,
            )
            if response.status_code != 200:
                return QueryResponse(
                    success=False,
                    data=None,
                    error_message=f"HTTP {response.status_code}: {response.text}",
                    raw_response=response.text,
                )
            body = response.json()
            if body.get("status") != "success":
                return QueryResponse(
                    success=False,
                    data=None,
                    error_message=f"RQE query failed: {body}",
                    raw_response=body,
                )
            result_type = body["data"].get("resultType")
            if result_type != VECTOR_RESULT_TYPE:
                return QueryResponse(
                    success=False,
                    data=None,
                    error_message=f"unexpected result type {result_type!r}, "
                    f"expected {VECTOR_RESULT_TYPE!r}",
                    raw_response=body,
                )
            raw_result = body["data"]["result"]
            return QueryResponse(
                success=True,
                data=normalize_vector(raw_result),
                raw_response=raw_result,
            )
        except Exception as e:
            return QueryResponse(
                success=False,
                data=None,
                error_message=f"{type(e).__name__}: {e}",
            )

    def get_runtime_info(self) -> Optional[Dict[str, Any]]:
        return fetch_runtime_info(self.server_url)

from typing import Any, Dict, Optional
import requests

from prometheus_api_client import PrometheusConnect
from .query_client import QueryClient, QueryResponse


def normalize_vector(raw_result: list) -> Dict[frozenset, float]:
    """
    Convert a Prometheus instant-vector result to normalized format.

    Prometheus response format:
        [{"metric": {"label1": "value1", ...}, "value": [timestamp, "value_str"]}, ...]

    Returns:
        Dict mapping frozenset of labels to float value
    """
    result = {}
    for item in raw_result:
        metric_labels = frozenset(item.get("metric", {}).items())
        value = item.get("value", [None, None])
        if len(value) >= 2 and value[1] is not None:
            try:
                result[metric_labels] = float(value[1])
            except (ValueError, TypeError):
                # Skip non-numeric values (e.g., NaN represented as string)
                pass
    return result


RUNTIME_INFO_PATH = "/api/v1/status/runtimeinfo"
RUNTIME_INFO_TIMEOUT_SECS = 10


def fetch_runtime_info(server_url: str) -> Dict[str, Any]:
    """Query SketchDB/Prometheus runtime info endpoint. Raises on a connection
    error, a non-200 status or a body without `data`."""
    response = requests.get(
        f"{server_url}{RUNTIME_INFO_PATH}", timeout=RUNTIME_INFO_TIMEOUT_SECS
    )
    response.raise_for_status()
    return response.json()["data"]


class PrometheusQueryClient(QueryClient):
    """Query client for Prometheus HTTP API."""

    def __init__(
        self,
        server_url: str,
        server_name: str,
        disable_ssl: bool = True,
        retry: Optional[Any] = None,
        **kwargs: Any,
    ):
        super().__init__(server_url, server_name)
        self._client = PrometheusConnect(
            url=server_url,
            disable_ssl=disable_ssl,
            retry=retry,
            **kwargs,
        )

    @property
    def protocol_name(self) -> str:
        return "prometheus"

    @property
    def underlying_client(self) -> PrometheusConnect:
        """Access to underlying PrometheusConnect for advanced usage (e.g., mounting HTTP adapters)."""
        return self._client

    @property
    def session(self) -> requests.Session:
        """Access to underlying requests Session for mounting debug adapters."""
        return self._client._session

    def execute_query(
        self,
        query: str,
        query_time: Optional[int] = None,
        config_query: Optional[str] = None,
    ) -> QueryResponse:
        """
        Execute PromQL query via Prometheus HTTP API.

        Args:
            query: PromQL query string
            query_time: Optional Unix timestamp for point-in-time query
            config_query: Unused; PromQL is sent as rendered

        Returns:
            QueryResponse with normalized data
        """
        try:
            if query_time:
                raw_result = self._client.custom_query(
                    query=query, params={"time": query_time}
                )
            else:
                raw_result = self._client.custom_query(query=query)

            # Normalize to Dict[frozenset, float]
            normalized = normalize_vector(raw_result)
            return QueryResponse(
                success=True,
                data=normalized,
                raw_response=raw_result,
            )
        except Exception as e:
            return QueryResponse(
                success=False,
                data=None,
                error_message=str(e),
            )

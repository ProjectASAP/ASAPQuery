from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from dataclasses import dataclass
import requests


@dataclass
class QueryResponse:
    """Normalized response from any query backend."""

    success: bool
    data: Optional[Dict[frozenset, float]]  # metric_labels -> value
    error_message: Optional[str] = None
    raw_response: Optional[Any] = None  # For debugging


class QueryClient(ABC):
    """Abstract base class for query protocol adapters."""

    def __init__(self, server_url: str, server_name: str):
        self.server_url = server_url
        self.server_name = server_name

    @abstractmethod
    def execute_query(
        self,
        query: str,
        query_time: Optional[int] = None,
        config_query: Optional[str] = None,
    ) -> QueryResponse:
        """
        Execute a query and return normalized response.

        Args:
            query: The query string (PromQL, SQL, etc.), templates rendered
            query_time: Optional Unix timestamp for point-in-time queries
            config_query: The query as the config lists it, before template
                rendering; None means `query` is that text. Clients that
                look queries up by text (rqe) use it, since the rendered
                text changes every run

        Returns:
            QueryResponse with normalized data
        """
        pass

    @property
    @abstractmethod
    def protocol_name(self) -> str:
        """Return the protocol name (e.g., 'prometheus', 'clickhouse')."""
        pass

    @property
    @abstractmethod
    def session(self) -> requests.Session:
        """Access to underlying requests Session for mounting debug adapters."""
        pass

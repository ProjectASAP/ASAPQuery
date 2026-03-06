import requests
from typing import Any, Dict, Optional
from requests.adapters import HTTPAdapter

from .query_client import QueryClient, QueryResponse


class ClickHouseQueryClient(QueryClient):
    """Query client for ClickHouse HTTP API."""

    def __init__(
        self,
        server_url: str,
        server_name: str,
        database: str = "default",
        user: str = "default",
        password: str = "",
        timeout: int = 30,
        **kwargs: Any,
    ):
        super().__init__(server_url, server_name)
        self.database = database
        self.user = user
        self.password = password
        self.timeout = timeout

        self._session = requests.Session()
        if user and password:
            self._session.auth = (user, password)

    @property
    def protocol_name(self) -> str:
        return "clickhouse"

    @property
    def session(self) -> requests.Session:
        """Access to underlying requests Session for mounting debug adapters."""
        return self._session

    def mount_adapter(self, prefix: str, adapter: HTTPAdapter) -> None:
        """Mount an HTTP adapter (e.g., for debug logging)."""
        self._session.mount(prefix, adapter)

    def execute_query(
        self,
        query: str,
        query_time: Optional[int] = None,
    ) -> QueryResponse:
        """
        Execute SQL query via ClickHouse HTTP interface.

        Args:
            query: SQL query string (may contain template variables already substituted)
            query_time: Not directly used - time filtering should be done via
                        template substitution before calling this method

        Returns:
            QueryResponse with normalized data
        """
        try:
            params = {"database": self.database}

            formatted_query = query.strip()

            # Reject queries with FORMAT clause - we need raw TSV for parsing
            if self._has_format_clause(formatted_query):
                return QueryResponse(
                    success=False,
                    data=None,
                    error_message="Queries must not contain FORMAT clause - raw TSV output is required for parsing",
                )

            response = self._session.post(
                self.server_url,
                params=params,
                data=formatted_query.encode("utf-8"),
                timeout=self.timeout,
            )

            if response.status_code != 200:
                return QueryResponse(
                    success=False,
                    data=None,
                    error_message=f"HTTP {response.status_code}: {response.text}",
                    raw_response=response.text,
                )

            # Return raw TSV text - will be stored in QueryResult.raw_text_result
            return QueryResponse(
                success=True,
                data=None,
                raw_response=response.text,
            )

        except requests.exceptions.Timeout:
            return QueryResponse(
                success=False,
                data=None,
                error_message=f"Request timed out after {self.timeout}s",
            )
        except Exception as e:
            return QueryResponse(
                success=False,
                data=None,
                error_message=f"{type(e).__name__}: {e}",
            )

    def _has_format_clause(self, query: str) -> bool:
        """Check if query already has a FORMAT clause."""
        # Simple check - look for FORMAT keyword followed by format name
        upper_query = query.upper()
        return " FORMAT " in upper_query or "\nFORMAT " in upper_query

    def get_runtime_info(self) -> Optional[Dict[str, Any]]:
        """Check ClickHouse availability via ping endpoint."""
        try:
            response = self._session.get(
                f"{self.server_url}/ping",
                timeout=5,
            )
            if response.status_code == 200:
                return {"status": "ok", "response": response.text.strip()}
        except Exception:
            pass
        return None

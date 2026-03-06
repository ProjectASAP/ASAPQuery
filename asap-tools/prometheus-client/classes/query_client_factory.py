from typing import Any, Dict, List, Type

from .query_client import QueryClient
from .prometheus_query_client import PrometheusQueryClient
from .clickhouse_query_client import ClickHouseQueryClient


class QueryClientFactory:
    """Factory for creating protocol-specific query clients."""

    _registry: Dict[str, Type[QueryClient]] = {
        "prometheus": PrometheusQueryClient,
        "clickhouse": ClickHouseQueryClient,
    }

    @classmethod
    def register(cls, protocol: str, client_class: Type[QueryClient]) -> None:
        """
        Register a new protocol handler.

        Args:
            protocol: Protocol name (e.g., 'influxdb')
            client_class: QueryClient subclass to handle this protocol
        """
        cls._registry[protocol] = client_class

    @classmethod
    def create(
        cls,
        protocol: str,
        server_url: str,
        server_name: str,
        **kwargs: Any,
    ) -> QueryClient:
        """
        Create a query client for the specified protocol.

        Args:
            protocol: Protocol name ('prometheus', 'clickhouse', etc.)
            server_url: Backend server URL
            server_name: Logical name for the server
            **kwargs: Protocol-specific options passed to the client constructor

        Returns:
            QueryClient instance

        Raises:
            ValueError: If protocol is not supported
        """
        if protocol not in cls._registry:
            supported = ", ".join(sorted(cls._registry.keys()))
            raise ValueError(
                f"Unsupported protocol '{protocol}'. "
                f"Supported protocols: {supported}"
            )

        client_class = cls._registry[protocol]
        return client_class(server_url, server_name, **kwargs)

    @classmethod
    def supported_protocols(cls) -> List[str]:
        """Return list of supported protocol names."""
        return sorted(cls._registry.keys())

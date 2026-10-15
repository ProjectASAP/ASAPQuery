from typing import List, Dict, Any, Optional

from .rqe_query_client import RQE_PROTOCOL


class ServerConfig:
    def __init__(
        self,
        name: str,
        url: str,
        protocol: Optional[str],
        # ClickHouse-specific options
        database: Optional[str],
        user: Optional[str],
        password: Optional[str],
        # rqe-specific: JSON side file of query text -> RQE expression tree,
        # relative to the client config's directory
        rqe_exprs_file: Optional[str] = None,
    ):
        if protocol == RQE_PROTOCOL and not rqe_exprs_file:
            raise ValueError(f"server {name!r}: the rqe protocol needs rqe_exprs_file")
        self.name = name
        self.url = url
        self.protocol = protocol
        self.database = database
        self.user = user
        self.password = password
        self.rqe_exprs_file = rqe_exprs_file

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ServerConfig":
        return cls(
            name=data["name"],
            url=data["url"],
            protocol=data.get("protocol"),
            database=data.get("database"),
            user=data.get("user"),
            password=data.get("password"),
            rqe_exprs_file=data.get("rqe_exprs_file"),
        )


class QueryGroupConfig:
    def __init__(
        self,
        id: int,
        queries: List[str],
        # repetitions: int,
        repetition_delay_ms: int,
        options: Dict[str, Any],
        time_window_seconds: Optional[int],
        # starting_delay: int,
        # options: Dict[str, Any],
    ):
        # set defaults
        self.starting_delay = 0
        self.repetitions = None

        self.id = id
        self.queries = queries
        # self.repetitions = repetitions
        self.repetition_delay_ms = repetition_delay_ms
        self.time_window_seconds = time_window_seconds
        self.__dict__.update(options)
        # self.starting_delay = starting_delay
        # self.options = options

        assert self.repetitions is not None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueryGroupConfig":
        return cls(
            id=data["id"],
            queries=data["queries"],
            repetition_delay_ms=data["repetition_delay_ms"],
            options=data["client_options"],
            time_window_seconds=data.get("time_window_seconds"),
        )


class Config:
    def __init__(
        self, servers: List[ServerConfig], query_groups: List[QueryGroupConfig]
    ):
        self.servers = servers
        self.query_groups = query_groups

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        servers = [ServerConfig.from_dict(server) for server in data["servers"]]
        query_groups = [
            QueryGroupConfig.from_dict(group) for group in data["query_groups"]
        ]
        return cls(servers=servers, query_groups=query_groups)

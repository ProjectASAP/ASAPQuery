from typing import List, Dict, Any, Optional


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
    ):
        self.name = name
        self.url = url
        self.protocol = protocol
        self.database = database
        self.user = user
        self.password = password

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ServerConfig":
        return cls(
            name=data["name"],
            url=data["url"],
            protocol=data.get("protocol"),
            database=data.get("database"),
            user=data.get("user"),
            password=data.get("password"),
        )


class QueryGroupConfig:
    def __init__(
        self,
        id: int,
        queries: List[str],
        # repetitions: int,
        repetition_delay: int,
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
        self.repetition_delay = repetition_delay
        self.time_window_seconds = time_window_seconds
        self.__dict__.update(options)
        # self.starting_delay = starting_delay
        # self.options = options

        assert self.repetitions is not None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueryGroupConfig":
        # return cls(
        #     id=data["id"],
        #     repetitions=data["repetitions"],
        #     repetition_delay=data["repetition_delay"],
        #     starting_delay=data["starting_delay"] if "starting_delay" in data else 0,
        #     options=data["options"],
        #     queries=data["queries"],
        # )
        return cls(
            id=data["id"],
            queries=data["queries"],
            repetition_delay=data["repetition_delay"],
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

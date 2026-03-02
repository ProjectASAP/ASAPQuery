import os
import yaml
import time
import requests
import argparse
import datetime
import logging

# import urllib3
from loguru import logger
from typing import Dict, Set, Optional, List, Any
from type_aliases import (
    ServerDict,
    Query,
    QueryIndex,
    RepetitionIndex,
    UnixTimestamp,
    ResultDict,
    QueryStartTimes,
    QueryEngineConfig,
)
import threading
import subprocess
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from classes.config import Config
from classes.QueryLatencyExporter import QueryLatencyExporter
from classes.query_client import QueryClient
from classes.query_client_factory import QueryClientFactory
from classes.query_template import QueryTemplate
from promql_utilities.query_results.classes import QueryResult, QueryResultAcrossTime
from promql_utilities.query_results.serializers import SerializerFactory


class PrometheusDebugRetry(Retry):
    def __init__(self, *args: Any, server_name: str = "", **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.server_name = server_name

    def new(self, **kw: Any) -> "PrometheusDebugRetry":
        """Override new() to preserve server_name when creating new instances."""
        new_retry = super().new(**kw)
        new_retry.server_name = self.server_name
        return new_retry

    def increment(
        self,
        method: Optional[str] = None,
        url: Optional[str] = None,
        response: Optional[Any] = None,
        error: Optional[Exception] = None,
        _pool: Optional[Any] = None,
        _stacktrace: Optional[Any] = None,
    ) -> "PrometheusDebugRetry":
        # Calculate current attempt number
        assert self.total is not None
        current_retries = self.total - (
            self.total if hasattr(self, "history") and self.history else 0
        )
        attempt_num = (3 - current_retries) + 1  # Assuming max 3 retries

        if response:
            logger.bind(module="http_debug").debug(
                f"RETRY ATTEMPT {attempt_num} for {self.server_name}: "
                f"{method} {url} -> HTTP {response.status} "
                f"(will retry: {response.status in self.status_forcelist})"
            )
        elif error:
            logger.bind(module="http_debug").debug(
                f"RETRY ATTEMPT {attempt_num} for {self.server_name}: "
                f"{method} {url} -> ERROR: {type(error).__name__}: {error}"
            )

        result = super().increment(method, url, response, error, _pool, _stacktrace)
        assert isinstance(result, PrometheusDebugRetry)
        return result


class PrometheusDebugHTTPAdapter(HTTPAdapter):
    def __init__(self, server_name: str, *args: Any, **kwargs: Any) -> None:
        self.server_name = server_name
        super().__init__(*args, **kwargs)

    def send(self, request: Any, *args: Any, **kwargs: Any) -> Any:
        logger.bind(module="http_debug").debug(
            f"HTTP REQUEST START for {self.server_name}: "
            f"{request.method} {request.url}"
        )
        start_time = time.time()

        try:
            response = super().send(request, *args, **kwargs)
            elapsed = time.time() - start_time

            logger.bind(module="http_debug").debug(
                f"HTTP REQUEST END for {self.server_name}: "
                f"{request.method} {request.url} -> HTTP {response.status_code} "
                f"({elapsed:.3f}s, {len(response.content)} bytes)"
            )
            return response
        except Exception as e:
            elapsed = time.time() - start_time
            logger.bind(module="http_debug").error(
                f"HTTP REQUEST FAILED for {self.server_name}: "
                f"{request.method} {request.url} -> {type(e).__name__}: {e} "
                f"(after {elapsed:.3f}s)"
            )
            raise


def create_loggers(logging_dir: str, log_level: str) -> None:
    logger.remove(None)  # remove default loggers

    logger.add("{}/prometheus_client.log".format(logging_dir), filter="__main__")

    logger.add(  # add latency exporter logger
        "{}/query_latency_exporter.log".format(logging_dir),
        level=log_level,
        filter=lambda record: record["extra"].get("module") == "query_latency_exporter",
    )

    # NEW: HTTP request debugging logger
    logger.add(
        "{}/http_requests.log".format(logging_dir),
        level="DEBUG",
        filter=lambda record: record["extra"].get("module") == "http_debug",
    )

    # Enable urllib3 debug logging for connection-level details
    urllib3_logger = logging.getLogger("urllib3.connectionpool")
    urllib3_logger.setLevel(logging.DEBUG)
    urllib3_handler = logging.FileHandler("{}/urllib3_debug.log".format(logging_dir))
    urllib3_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    urllib3_logger.addHandler(urllib3_handler)


def get_query_unix_time(
    query: Query,
    query_unix_time: UnixTimestamp,
    query_start_times: Optional[QueryStartTimes],
    repetition_delay: int,
) -> UnixTimestamp:
    if query_start_times is None or query not in query_start_times:
        return query_unix_time

    query_alignment_time = query_start_times[query]
    # we want the latest timestamp that is query_aligment_time + N * repetition_delay
    query_unix_time = int(
        query_unix_time - (query_unix_time - query_alignment_time) % repetition_delay
    )
    return query_unix_time


def execute_single_query(
    server_name: str,
    server_object: QueryClient,
    query: Query,
    query_idx: QueryIndex,
    repetition_idx: RepetitionIndex,
    query_unix_time: Optional[UnixTimestamp],
    dry_run: bool,
    query_group_idx: int,
    time_window_seconds: Optional[int],
) -> QueryResult:
    """Execute a single query and return the result with latency information."""
    logger.debug(
        f"Running query {query} on server {server_name} at time {query_unix_time}"
    )

    # Handle template substitution for queries with time variables
    template = QueryTemplate(query)
    if template.has_time_variables:
        if time_window_seconds is None:
            raise ValueError(
                f"Query contains time template variables but time_window_seconds is not set: {query[:100]}"
            )
        if query_unix_time is None:
            raise ValueError(
                f"Query contains time template variables but query_unix_time is not set: {query[:100]}"
            )
        time_range = QueryTemplate.calculate_time_range(
            current_time=query_unix_time,
            window_seconds=time_window_seconds,
        )
        rendered_query = template.render(time_range)
        logger.debug(f"Rendered query template: {rendered_query}")
    else:
        rendered_query = query

    # Enhanced HTTP debug logging for query start
    logger.bind(module="http_debug").info(
        f"QUERY START - Server: {server_name}, Query: {rendered_query[:100]}{'...' if len(rendered_query) > 100 else ''}, "
        f"QueryIdx: {query_idx}, QueryGroupIdx: {query_group_idx}, Rep: {repetition_idx}, Time: {query_unix_time}"
    )

    empty_query_result = QueryResult(
        server_name,
        query,  # Store original query template, not rendered
        query_idx,
        repetition_idx,
        result=None,
        latency=None,
        cumulative_latency=None,
        query_group_idx=query_group_idx,
    )

    if dry_run:
        logger.bind(module="http_debug").debug(
            f"DRY RUN - Skipping actual HTTP request for {server_name}"
        )
        return empty_query_result

    try:
        query_start_time = time.time()
        # Use the QueryClient abstraction
        response = server_object.execute_query(
            query=rendered_query,
            query_time=query_unix_time,
        )
        query_end_time = time.time()

        latency = query_end_time - query_start_time
        logger.debug("Latency: {}", latency)

        if not response.success:
            logger.error(f"Query failed: {response.error_message}")
            logger.bind(module="http_debug").error(
                f"QUERY ERROR - Server: {server_name}, Error: {response.error_message}"
            )
            return empty_query_result

        # Determine result type based on response format
        if isinstance(response.raw_response, str):
            # ClickHouse/SQL - raw text result
            query_result_data = None
            raw_text_result = response.raw_response
            result_count = (
                len(response.raw_response.strip().split("\n"))
                if response.raw_response
                else 0
            )
        else:
            # Prometheus - list of dicts
            query_result_data = response.raw_response
            raw_text_result = None
            result_count = len(response.raw_response) if response.raw_response else 0

        # Enhanced HTTP debug logging for query success
        logger.debug("Query result: {}", response.raw_response)
        logger.bind(module="http_debug").info(
            f"QUERY SUCCESS - Server: {server_name}, Total latency: {latency:.3f}s, "
            f"Results: {result_count} data points"
        )

    except Exception as e:
        logger.error(f"Error running query: {str(e)}")

        # Enhanced HTTP debug logging for query error
        logger.bind(module="http_debug").error(
            f"QUERY ERROR - Server: {server_name}, Error: {type(e).__name__}: {e}"
        )
        return empty_query_result

    return QueryResult(
        server_name,
        query,  # Store original query template
        query_idx,
        repetition_idx,
        result=query_result_data,
        latency=latency,
        cumulative_latency=None,
        query_group_idx=query_group_idx,
        raw_text_result=raw_text_result,
    )


def handle_query_group(
    servers: ServerDict,
    query_group: Any,
    query_group_idx: int,
    query_start_times: Optional[QueryStartTimes],
    dry_run: bool,
    parallel: bool = False,
    latency_exporter: Optional[Any] = None,
    streaming_serializer: Optional[Any] = None,
) -> ResultDict:
    logger.debug(f"Starting query group {query_group.id}")
    if query_group.starting_delay:
        logger.debug(
            f"Waiting for {query_group.starting_delay} seconds before starting"
        )
        time.sleep(query_group.starting_delay)

    logger.debug("Query start times: {}", query_start_times)

    current_time = None
    query_unix_time = None

    # Calculate global query indices (combining group offset with local index)
    global_query_idx_start: int = query_group._global_query_idx_start

    result = {
        server_name: {
            global_query_idx_start
            + local_query_idx: QueryResultAcrossTime(
                server_name,
                query,
                global_query_idx_start + local_query_idx,
                query_group.repetitions,
            )
            for local_query_idx, query in enumerate(query_group.queries)
        }
        for server_name in servers
    }

    for repetition_idx in range(query_group.repetitions):
        current_time = datetime.datetime.now()
        logger.debug("Current unix time: {}", int(current_time.timestamp()))

        if hasattr(query_group, "query_time_offset"):
            current_time = current_time - datetime.timedelta(
                seconds=query_group.query_time_offset
            )
            logger.debug(
                "Offsetting query time by {} seconds", query_group.query_time_offset
            )

        query_unix_time = int(current_time.timestamp())
        logger.debug("Unix time after query_time_offset: {}", query_unix_time)

        if parallel:
            # Execute queries in parallel
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for local_query_idx, query in enumerate(query_group.queries):
                    global_query_idx = global_query_idx_start + local_query_idx
                    current_query_unix_time = get_query_unix_time(
                        query,
                        query_unix_time,
                        query_start_times,
                        query_group.repetition_delay,
                    )

                    for server_name, server_object in servers.items():
                        futures.append(
                            executor.submit(
                                execute_single_query,
                                server_name,
                                server_object,
                                query,
                                global_query_idx,
                                repetition_idx,
                                current_query_unix_time,
                                dry_run,
                                query_group_idx,
                                query_group.time_window_seconds,
                            )
                        )

                # Collect results
                for future in concurrent.futures.as_completed(futures):
                    query_result = future.result()
                    server_name = query_result.server_name
                    query_idx = query_result.query_idx

                    query_result.cumulative_latency = query_result.latency

                    result[server_name][query_idx].add_result(query_result)

                    # Stream result immediately if streaming serializer is provided
                    if streaming_serializer is not None and not dry_run:
                        streaming_serializer.streaming_write_result(query_result)
        else:
            # Reset cumulative latency for each repetition
            cumulative_latency = {server_name: 0.0 for server_name in servers}

            # Serial execution - use the same execute_single_query function
            for local_query_idx, query in enumerate(query_group.queries):
                global_query_idx = global_query_idx_start + local_query_idx
                current_query_unix_time = get_query_unix_time(
                    query,
                    query_unix_time,
                    query_start_times,
                    query_group.repetition_delay,
                )

                logger.debug("Unix time for query: {}", current_query_unix_time)

                for server_name, server_object in servers.items():
                    query_result = execute_single_query(
                        server_name,
                        server_object,
                        query,
                        global_query_idx,
                        repetition_idx,
                        current_query_unix_time,
                        dry_run,
                        query_group_idx,
                        query_group.time_window_seconds,
                    )

                    # Update cumulative latency for this repetition
                    if query_result.latency is not None:
                        cumulative_latency[server_name] += query_result.latency

                    query_result.cumulative_latency = cumulative_latency[server_name]

                    try:
                        result[server_name][global_query_idx].add_result(query_result)
                    except Exception as e:
                        logger.error(
                            f"{type(e).__name__} accessing result dict: {e}, "
                            f"server_name={server_name}, "
                            f"global_query_idx={global_query_idx}, "
                            f"local_query_idx={local_query_idx}, "
                            f"query_group_idx={query_group_idx}, "
                            f"available_keys={list(result[server_name].keys())}"
                        )
                        raise

                    # Stream result immediately if streaming serializer is provided
                    if streaming_serializer is not None and not dry_run:
                        streaming_serializer.streaming_write_result(query_result)

        if latency_exporter is not None:
            latency_exporter.export_repetition(repetition_idx, result)

        if repetition_idx < query_group.repetitions - 1:
            time.sleep(query_group.repetition_delay)

    if latency_exporter is not None:
        latency_exporter.shutdown()

    return result


def get_query_start_times(
    server_url: str, query_engine_config: QueryEngineConfig
) -> QueryStartTimes:
    aggregation_id_start_time_map = {}
    query_aggregation_id_map = {}
    query_start_time_map = {}

    required_aggregation_ids: Set[int] = set()
    for query_yaml in query_engine_config["queries"]:
        # add all aggregation IDs from the query YAML to the required_aggregation_ids set
        required_aggregation_ids.update(
            int(aggregation["aggregation_id"])
            for aggregation in query_yaml["aggregations"]
        )
        # assert len(query_yaml["aggregations"]) == 1
        # required_aggregation_ids.add(
        #    int(query_yaml["aggregations"][0]["aggregation_id"])
        # )
    logger.debug("Required aggregation IDs: {}", required_aggregation_ids)

    # wait for all required aggregation IDs to be present
    while True:
        server_response = requests.get(
            server_url + "/api/v1/status/runtimeinfo",
            headers={"Content-Type": "application/json"},
        )
        server_response.raise_for_status()
        server_response_json = server_response.json()
        logger.debug("Server response: {}", server_response_json)
        aggregation_id_start_time_map = server_response_json["data"][
            "earliest_timestamp_per_aggregation_id"
        ]

        # change all keys from string to int
        aggregation_id_start_time_map = {
            int(k): v for k, v in aggregation_id_start_time_map.items()
        }

        if not set(aggregation_id_start_time_map.keys()).issuperset(
            required_aggregation_ids
        ):
            logger.debug(
                "Waiting for aggregation IDs {} to be present",
                required_aggregation_ids - set(aggregation_id_start_time_map.keys()),
            )
            time.sleep(10)
        else:
            break

    # TODO: make this more robust. What happens if there are multiple aggregations with
    # different tumbling windows? How long do we wait here? What happens with multiple query groups?

    # get query to aggregate ID mapping from query_engine_config
    for query_yaml in query_engine_config["queries"]:
        # TODO: this assert will fail if there are multiple aggregations in a query YAML, including for DeltaSet, so commenting it out
        # assert len(query_yaml["aggregations"]) == 1
        # for now, just take the first aggregation ID
        # TODO: make this more robust, eg for cases where aggregations for the same query have different tumbling windows or start times
        query_aggregation_id_map[query_yaml["query"]] = int(
            query_yaml["aggregations"][0]["aggregation_id"]
        )

    for query, aggregation_id in query_aggregation_id_map.items():
        # aggregation_id_start_time_map is in milliseconds, convert to seconds
        query_start_time_map[query] = (
            # aggregation_id_start_time_map[str(aggregation_id)] / 1000
            aggregation_id_start_time_map[aggregation_id]
            / 1000
        )

    return query_start_time_map


def check_args(args: Any) -> None:
    if args.align_query_time and args.query_engine_config_file is None:
        raise ValueError(
            "If align_query_time is set, query_engine_config_file must be provided"
        )


def start_query_engine_profiler(
    pid: int, output_dir: str, starting_delay: int, duration: int
) -> None:
    """
    Create and start a subprocess to run py-spy on the specified process.

    Args:
        pid: Process ID of the query engine
        output_dir: Directory to save the profile output
        duration: Duration in seconds to run the profiler

    Returns:
        subprocess.Popen: The created subprocess
    """
    output_file = os.path.join(output_dir, "query_engine_profile.svg")
    logger.debug(f"Waiting for {starting_delay} seconds before starting profiler")
    time.sleep(starting_delay)
    logger.debug(f"Starting py-spy profiling of PID {pid} for {duration} seconds")

    try:
        cmd = "bash --login -c 'sudo env \"PATH=$PATH\" py-spy record --pid {} -o {} --duration {} --idle'".format(
            str(pid), output_file, str(duration)
        )
        logger.info(f"Running command: {cmd}")

        subprocess.run(cmd, shell=True)
    except Exception as e:
        logger.error(f"Error starting profiler: {str(e)}")
        raise e


def start_prometheus_profiler(
    output_dir: str, starting_delay: int, duration: int
) -> None:
    output_file = os.path.join(output_dir, "prometheus_profile.pprof")
    logger.debug(f"Waiting for {starting_delay} seconds before starting profiler")
    time.sleep(starting_delay)
    logger.debug(f"Starting pprof profiling of Prometheus for {duration} seconds")

    try:
        # cmd = "go tool pprof -seconds {} -output {} http://localhost:9090/debug/pprof/profile".format(
        cmd = "curl -o {} http://localhost:9090/debug/pprof/profile?seconds={}".format(
            output_file,
            str(duration),
        )
        logger.info(f"Running command: {cmd}")

        subprocess.run(cmd, shell=True)
    except Exception as e:
        logger.error(f"Error starting profiler: {str(e)}")


def main(args: Any) -> None:
    check_args(args)
    os.makedirs(args.output_dir, exist_ok=True)

    create_loggers(args.output_dir, "DEBUG")

    if args.dry_run:
        logger.info("Running in dry-run mode")

    if args.parallel:
        logger.info("Running queries in parallel mode")

    with open(args.config_file, "r") as file:
        config_data = yaml.safe_load(file)

    query_engine_config = None
    if args.query_engine_config_file:
        with open(args.query_engine_config_file, "r") as file:
            query_engine_config = yaml.safe_load(file)

    config = Config.from_dict(config_data)

    logger.debug("Read config")

    # Calculate global query indices for each query group
    global_query_idx = 0
    for query_group in config.query_groups:
        query_group._global_query_idx_start = global_query_idx  # type: ignore[attr-defined]
        global_query_idx += len(query_group.queries)

    server_url_for_alignment = None

    servers: Dict[str, QueryClient] = {}
    for server in config.servers:
        # Determine protocol (default to prometheus for backward compatibility)
        protocol = server.protocol if server.protocol else "prometheus"

        if protocol == "prometheus":
            # Create custom retry adapter with debug logging
            debug_retry = PrometheusDebugRetry(
                server_name=server.name,
                total=3,
                backoff_factor=1,
                status_forcelist=[408, 429, 500, 502, 503, 504],
            )

            client = QueryClientFactory.create(
                protocol=protocol,
                server_url=server.url,
                server_name=server.name,
                disable_ssl=True,
                retry=debug_retry,
            )

            # Mount debug adapter for HTTP request logging
            debug_adapter = PrometheusDebugHTTPAdapter(server.name)
            client.session.mount("http://", debug_adapter)
            client.session.mount("https://", debug_adapter)
        else:
            # ClickHouse or other protocols
            client = QueryClientFactory.create(
                protocol=protocol,
                server_url=server.url,
                server_name=server.name,
                database=server.database if server.database else "default",
                user=server.user if server.user else "default",
                password=server.password if server.password else "",
            )

            # Mount debug adapter for HTTP request logging
            debug_adapter = PrometheusDebugHTTPAdapter(server.name)
            client.session.mount("http://", debug_adapter)
            client.session.mount("https://", debug_adapter)

        servers[server.name] = client
        logger.debug(
            "Connected to server {} ({}) with HTTP debug logging enabled",
            server.name,
            protocol,
        )

        if args.align_query_time and server.name == args.server_for_alignment:
            server_url_for_alignment = server.url

    query_start_times = None
    if args.align_query_time:
        assert server_url_for_alignment is not None
        assert query_engine_config is not None
        query_start_times = get_query_start_times(
            server_url_for_alignment, query_engine_config
        )
        logger.debug("Got query start times")

    # Calculate profiler timing based on all query groups
    min_starting_delay = min(qg.starting_delay for qg in config.query_groups)
    max_duration = 0
    for query_group in config.query_groups:
        assert query_group.repetitions is not None
        assert query_group.repetition_delay is not None
        duration = (
            query_group.repetition_delay * query_group.repetitions
            + query_group.starting_delay
            - min_starting_delay
        )
        max_duration = max(max_duration, duration)

    query_engine_profiler_thread = None
    if args.profile_query_engine_pid:
        query_engine_profiler_thread = threading.Thread(
            target=start_query_engine_profiler,
            args=(
                args.profile_query_engine_pid,
                args.output_dir,
                min_starting_delay,
                max_duration,
            ),
        )
        if query_engine_profiler_thread:
            logger.debug("Starting query engine profiler thread...")
            query_engine_profiler_thread.daemon = True
            query_engine_profiler_thread.start()

    prometheus_profiler_thread = None
    if args.profile_prometheus_time is not None:
        prometheus_profiler_thread = threading.Thread(
            target=start_prometheus_profiler,
            args=(
                args.output_dir,
                min_starting_delay,
                args.profile_prometheus_time,
            ),
        )
        if prometheus_profiler_thread:
            prometheus_profiler_thread.daemon = True
            prometheus_profiler_thread.start()

    if args.export_latencies_for_prometheus is not None:
        exporter_socket_addr = args.export_latencies_for_prometheus.split(sep=":")
        exporter_ip = exporter_socket_addr[0]
        exporter_port = int(exporter_socket_addr[1])
        latency_exporter = QueryLatencyExporter(addr=exporter_ip, port=exporter_port)
        logger.debug(
            f"Running with query latency exporter at {args.export_latencies_for_prometheus}"
        )
        latency_exporter.launch()
    else:
        latency_exporter = None

    # Initialize streaming serializer if not in dry run mode
    streaming_serializer = None
    if not args.dry_run:
        streaming_serializer = SerializerFactory.create(
            args.serialization_format, args.output_dir
        )

        # Prepare metadata for streaming - include per-group information
        query_groups_metadata = []
        for query_group_idx, query_group in enumerate(config.query_groups):
            query_groups_metadata.append(
                {
                    "query_group_idx": query_group_idx,
                    "query_group_id": query_group.id,
                    "queries": query_group.queries,
                    "repetitions": query_group.repetitions,
                }
            )

        metadata = {
            "query_groups": query_groups_metadata,
            "servers": list(servers.keys()),
        }
        streaming_serializer.streaming_write_start(metadata)

    # Spawn threads for each query group
    query_group_threads = []
    results_per_group: List[Optional[ResultDict]] = [None] * len(config.query_groups)

    def run_query_group(query_group_idx: int, query_group: Any) -> None:
        """Wrapper function to run a query group and store results."""
        try:
            results = handle_query_group(
                servers,
                query_group,
                query_group_idx,
                query_start_times,
                args.dry_run,
                args.parallel,
                latency_exporter,
                streaming_serializer,
            )
            results_per_group[query_group_idx] = results
        except Exception as e:
            logger.error(
                f"Query group {query_group_idx} (id={query_group.id}) failed with "
                f"{type(e).__name__}: {e}",
                exc_info=True,
            )
            results_per_group[query_group_idx] = None
            raise  # Re-raise to ensure it's logged but thread still terminates

    for query_group_idx, query_group in enumerate(config.query_groups):
        thread = threading.Thread(
            target=run_query_group,
            args=(query_group_idx, query_group),
        )
        query_group_threads.append(thread)
        thread.start()
        logger.debug(f"Started thread for query group {query_group_idx}")

    # Wait for all query group threads to complete
    for idx, thread in enumerate(query_group_threads):
        thread.join()
        logger.debug(f"Query group {idx} thread completed")

    # Merge results from all query groups into single structure
    results_across_servers: Dict[str, Dict[int, Any]] = {}
    for server_name in servers.keys():
        results_across_servers[server_name] = {}

    for group_results in results_per_group:
        if group_results:
            for server_name, server_results in group_results.items():
                results_across_servers[server_name].update(server_results)

    if not args.dry_run and streaming_serializer is not None:
        # Finalize streaming write
        streaming_serializer.streaming_write_end()

        # deprecated: save results in a pickle file
        # with open(os.path.join(args.output_dir, args.result_output_file), "wb") as fout:
        #    pickle.dump(results_across_servers, fout)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--config_file", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--output_file", type=str, required=True)
    # deprecated:
    # parser.add_argument("--result_output_file", type=str, default="results.pkl")

    parser.add_argument("--query_engine_config_file", type=str, required=False)
    parser.add_argument("--align_query_time", action="store_true", required=False)
    parser.add_argument("--server_for_alignment", type=str, default="sketchdb")

    parser.add_argument("--dry_run", action="store_true", required=False)
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Execute queries in parallel",
        required=False,
    )

    parser.add_argument("--profile_query_engine_pid", type=int, required=False)
    parser.add_argument("--profile_prometheus_time", type=int, required=False)

    parser.add_argument(
        "--export_latencies_for_prometheus",
        type=str,
        help="Run prometheus query latency exporter at <IP:PORT>",
        required=False,
    )

    parser.add_argument(
        "--serialization_format",
        type=str,
        choices=["jsonl", "parquet"],
        default="jsonl",
        help="Format for serializing query results (jsonl or parquet)",
        required=False,
    )

    args = parser.parse_args()
    main(args)

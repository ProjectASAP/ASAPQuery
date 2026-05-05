"""
Query Engine service management for experiments.
"""

import os
import subprocess

import yaml

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class BaseQueryEngineService(BaseService):
    """Base class for query engine services."""

    def __init__(
        self,
        provider: InfrastructureProvider,
        use_container: bool,
        node_offset: int,
    ):
        """
        Initialize base query engine service.

        Args:
            provider: Infrastructure provider for node communication and management
            use_container: Whether to use containerized deployment
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.use_container = use_container
        self.node_offset = node_offset
        self.container_name = None
        self.compose_file = None

    def get_monitoring_keyword(self) -> str:
        pass

    def get_http_port(self) -> int:
        """Get the HTTP port for QueryEngine."""
        return 8088


class QueryEngineService(BaseQueryEngineService):
    """Service for managing the Python query engine process."""

    def __init__(
        self,
        provider: InfrastructureProvider,
        use_container: bool,
        node_offset: int,
    ):
        """
        Initialize Python Query Engine service.

        Args:
            provider: Infrastructure provider for node communication and management
            use_container: Whether to use containerized deployment
            node_offset: Starting node index offset
        """
        super().__init__(provider, use_container, node_offset)
        self.container_name = constants.QUERY_ENGINE_PY_CONTAINER_NAME

    def start(
        self,
        experiment_output_dir: str,
        flink_output_format: str,
        prometheus_scrape_interval: int,
        log_level: str,
        profile_query_engine: bool,
        manual: bool,
        streaming_engine: str,
        forward_unsupported_queries: bool,
        controller_remote_output_dir: str,
        compress_json: bool,
        dump_precomputes: bool,
        **kwargs,
    ) -> None:
        """
        Start the query engine.

        Args:
            experiment_output_dir: Directory for experiment output
            flink_output_format: Format of data from Flink
            prometheus_scrape_interval: Prometheus scraping interval
            log_level: Logging level
            profile_query_engine: Whether to enable profiling
            manual: Whether to run in manual mode
            streaming_engine: Type of streaming engine (flink/arroyo/precompute)
            forward_unsupported_queries: Whether to forward unsupported queries
            controller_remote_output_dir: Controller output directory
            compress_json: Whether JSON is compressed
            dump_precomputes: Whether to dump precomputed values
            **kwargs: Additional configuration
        """
        if dump_precomputes:
            raise ValueError(
                "dump_precomputes is not supported by the Python query engine. Use the Rust query engine instead."
            )
        if self.use_container:
            prometheus_host = kwargs.get(
                "prometheus_host", self.provider.get_node_ip(self.node_offset)
            )
            self._start_containerized(
                experiment_output_dir,
                flink_output_format,
                prometheus_scrape_interval,
                log_level,
                profile_query_engine,
                manual,
                streaming_engine,
                forward_unsupported_queries,
                controller_remote_output_dir,
                compress_json,
                prometheus_host,
                dump_precomputes,
            )
        else:
            self._start_bare_metal(
                experiment_output_dir,
                flink_output_format,
                prometheus_scrape_interval,
                log_level,
                profile_query_engine,
                manual,
                streaming_engine,
                forward_unsupported_queries,
                controller_remote_output_dir,
                compress_json,
                dump_precomputes,
            )

    def _start_bare_metal(
        self,
        experiment_output_dir: str,
        flink_output_format: str,
        prometheus_scrape_interval: int,
        log_level: str,
        profile_query_engine: bool,
        manual: bool,
        streaming_engine: str,
        forward_unsupported_queries: bool,
        controller_remote_output_dir: str,
        compress_json: bool,
        dump_precomputes: bool,
    ) -> None:
        """Start QueryEngine using bare metal deployment (original implementation)."""
        output_dir = os.path.join(experiment_output_dir, "query_engine_output")

        cmd = (
            "mkdir -p {}; python3 -u main_query_engine.py "
            "--kafka_topic {} "
            "--input_format {} "
            "--config {}/inference_config.yaml "
            "--streaming_config {}/streaming_config.yaml "
            "--prometheus_scrape_interval {} "
            "--delete_existing_db "
            "--log_level {} "
            "--output_dir {} "
            "{} "
            "--streaming_engine {} "
        ).format(
            output_dir,
            constants.FLINK_OUTPUT_TOPIC,
            flink_output_format,
            controller_remote_output_dir,
            controller_remote_output_dir,
            prometheus_scrape_interval,
            log_level,
            output_dir,
            "--decompress_json" if compress_json else "",
            streaming_engine,
        )

        if profile_query_engine:
            cmd += "--do_profiling "
        if forward_unsupported_queries:
            cmd += "--forward_unsupported_queries "
        cmd += "> {}/main_query_engine.out 2>&1 &".format(output_dir)

        cmd_dir = os.path.join(self.provider.get_home_dir(), "code", "QueryEngine")
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=True,
            popen=False,
            ignore_errors=False,
            manual=manual,
        )

    def _start_containerized(
        self,
        experiment_output_dir: str,
        flink_output_format: str,
        prometheus_scrape_interval: int,
        log_level: str,
        profile_query_engine: bool,
        manual: bool,
        streaming_engine: str,
        forward_unsupported_queries: bool,
        controller_remote_output_dir: str,
        compress_json: bool,
        prometheus_host: str,
        dump_precomputes: bool,
    ) -> None:
        """Start QueryEngine using containerized deployment with Jinja template."""
        output_dir = os.path.join(experiment_output_dir, "query_engine_output")

        # Paths on remote CloudLab node
        queryengine_dir = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "QueryEngine"
        )
        template_path = os.path.join(queryengine_dir, "docker-compose.yml.j2")
        remote_compose_file = os.path.join(output_dir, "docker-compose.yml")
        helper_script = os.path.join(
            constants.CLOUDLAB_HOME_DIR,
            "code",
            "asap-tools",
            "experiments",
            "generate_queryengine_compose.py",
        )
        self.compose_file = remote_compose_file

        # Build command to generate docker-compose file using helper script
        generate_cmd = f"python3 {helper_script}"
        generate_cmd += f" --template-path '{template_path}'"
        generate_cmd += f" --output-path '{remote_compose_file}'"
        generate_cmd += f" --queryengine-dir '{queryengine_dir}'"
        generate_cmd += f" --container-name '{self.container_name}'"
        generate_cmd += f" --experiment-output-dir '{output_dir}'"
        generate_cmd += (
            f" --controller-remote-output-dir '{controller_remote_output_dir}'"
        )
        generate_cmd += f" --kafka-topic '{constants.FLINK_OUTPUT_TOPIC}'"
        generate_cmd += f" --input-format '{flink_output_format}'"
        generate_cmd += f" --prometheus-scrape-interval '{prometheus_scrape_interval}'"
        generate_cmd += f" --log-level '{log_level}'"
        generate_cmd += f" --streaming-engine '{streaming_engine}'"
        generate_cmd += f" --kafka-host '{self.provider.get_node_ip(self.node_offset)}'"
        generate_cmd += f" --prometheus-host '{prometheus_host}'"

        # Add optional flags
        if compress_json:
            generate_cmd += " --compress-json"
        if profile_query_engine:
            generate_cmd += " --profile-query-engine"
        if forward_unsupported_queries:
            generate_cmd += " --forward-unsupported-queries"
        if dump_precomputes:
            generate_cmd += " --dump-precomputes"
        if manual:
            generate_cmd += " --manual"

        cmd = f"mkdir -p {output_dir}; {generate_cmd}; docker compose -f {remote_compose_file} up --no-build -d"

        if manual:
            print(f"Directory to run command: {queryengine_dir}")
            print(f"Manual mode: Run command: {cmd}")
            input("Press Enter to continue...")
        else:
            try:
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=queryengine_dir,
                    nohup=False,
                    popen=False,
                    ignore_errors=False,
                )
            except Exception as e:
                print(f"Failed to start QueryEngine container: {e}")
                raise

    def stop(self, **kwargs) -> None:
        """
        Stop the query engine process.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        if self.use_container:
            self._stop_containerized()
        else:
            self._stop_bare_metal()

    def _stop_bare_metal(self) -> None:
        """Stop QueryEngine using bare metal deployment (original implementation)."""
        cmd = "pkill -f main_query_engine.py"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def _stop_containerized(self) -> None:
        """Stop QueryEngine using containerized deployment."""
        try:
            if self.compose_file:
                # Stop using docker compose command on remote node
                cmd = f"docker compose -f {self.compose_file} down"
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=False,
                    ignore_errors=True,
                )
                self.compose_file = None
            else:
                # Fallback: stop by container name on remote node
                cmd = f"docker stop {self.container_name}; docker rm {self.container_name}"
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=False,
                    ignore_errors=True,
                )
        except Exception as e:
            print(f"Error stopping QueryEngine container: {e}")

    def is_healthy(self) -> bool:
        """
        Check if query engine is healthy by checking if process is running.

        Returns:
            True if query engine process is running
        """
        if self.use_container:
            return self._is_healthy_containerized()
        else:
            return self._is_healthy_bare_metal()

    def _is_healthy_bare_metal(self) -> bool:
        """Check if QueryEngine is healthy using bare metal deployment."""
        try:
            cmd = "pgrep -f main_query_engine.py"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            import subprocess

            assert isinstance(result, subprocess.CompletedProcess)
            return result.stdout.strip() != ""
        except Exception:
            return False

    def _is_healthy_containerized(self) -> bool:
        """Check if QueryEngine is healthy using containerized deployment."""
        try:
            # Check if container is running
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", self.container_name],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip() == "true"
        except subprocess.CalledProcessError:
            return False
        except Exception:
            return False

    def get_monitoring_keyword(self) -> str:
        """
        Get the keyword to use for process monitoring.

        Returns:
            Container name if using containers, otherwise process name
        """
        if self.use_container:
            return self.container_name
        else:
            return constants.QUERY_ENGINE_PY_PROCESS_KEYWORD


class QueryEngineRustService(BaseQueryEngineService):
    """Service for managing the Rust query engine process."""

    def __init__(
        self,
        provider: InfrastructureProvider,
        use_container: bool,
        node_offset: int,
    ):
        """
        Initialize Rust Query Engine service.

        Args:
            provider: Infrastructure provider for node communication and management
            use_container: Whether to use containerized deployment
            node_offset: Starting node index offset
        """
        super().__init__(provider, use_container, node_offset)
        self.container_name = constants.QUERY_ENGINE_RS_CONTAINER_NAME

    # ------------------------------------------------------------------
    # Config-file helpers (used by both bare-metal and containerized paths)
    # ------------------------------------------------------------------

    def _build_engine_config(
        self,
        output_dir: str,
        flink_output_format: str,
        prometheus_scrape_interval: int,
        log_level: str,
        streaming_engine: str,
        forward_unsupported_queries: bool,
        controller_config_dir: str,
        compress_json: bool,
        prometheus_server: str,
        http_port: int,
        remote_write_port: int,
        dump_precomputes: bool,
        query_language: str,
        lock_strategy: str,
        profile_query_engine: bool,
        kafka_broker: str,
    ) -> dict:
        """
        Build an EngineConfig dict matching asap-query-engine's engine_config.rs schema.

        Args:
            output_dir: Output directory path (remote host path or container-internal path)
            flink_output_format: Kafka input_format when streaming_engine=arroyo
            prometheus_scrape_interval: Prometheus scraping interval in seconds
            log_level: Logging level
            streaming_engine: 'arroyo' (Kafka ingest) or 'precompute' (HTTP remote write)
            forward_unsupported_queries: Whether to forward unsupported queries to backend
            controller_config_dir: Directory containing inference_config.yaml and streaming_config.yaml
            compress_json: Whether incoming JSON is gzip-compressed (arroyo/Kafka only)
            prometheus_server: Full Prometheus URL, e.g. http://host:9090
            http_port: Port for the query engine's HTTP API server
            remote_write_port: Port to listen on for Prometheus remote write (precompute only);
                               should match streaming.remote_write.base_port in the Hydra config
            dump_precomputes: Whether to dump received precomputes to output_dir for debugging
            query_language: 'PROMQL' → prometheus backend, 'SQL' → clickhouse backend
            lock_strategy: Lock strategy for SimpleMapStore ('global' or 'per-key')
            profile_query_engine: Whether to enable do_profiling in the engine
            kafka_broker: Kafka broker address, e.g. '10.10.1.1:9092' (arroyo only)

        Returns:
            Dict matching the EngineConfig YAML schema
        """
        # Map query_language to the backend type (determines PromQL vs SQL API)
        if query_language.upper() == "PROMQL":
            backend: dict = {
                "type": "prometheus",
                "server": prometheus_server,
                "forward_unsupported_queries": forward_unsupported_queries,
            }
        else:
            # SQL mode: use clickhouse backend with the same host as prometheus_server
            backend = {
                "type": "clickhouse",
                "url": prometheus_server,
                "forward_unsupported_queries": forward_unsupported_queries,
            }

        # Ingest config depends on the streaming engine
        if streaming_engine == "arroyo":
            ingest: dict = {
                "type": "kafka",
                "broker": kafka_broker,
                "topic": constants.FLINK_OUTPUT_TOPIC,
                "input_format": flink_output_format,
                "decompress_json": compress_json,
            }
        elif streaming_engine == "precompute":
            ingest = {
                "type": "http_remote_write",
                "port": remote_write_port,
            }
        else:
            raise ValueError(
                f"streaming_engine='{streaming_engine}' is not supported by the Rust query engine. "
                "Use 'arroyo' or 'precompute'."
            )

        return {
            "output_dir": output_dir,
            "log_level": log_level,
            "prometheus_scrape_interval": prometheus_scrape_interval,
            "streaming_engine": streaming_engine,
            "do_profiling": profile_query_engine,
            "http_server": {"port": http_port},
            "backend": backend,
            "store": {"lock_strategy": lock_strategy},
            "ingest": ingest,
            "precompute_engine": {"dump_precomputes": dump_precomputes},
            "inference_config": os.path.join(
                controller_config_dir, "inference_config.yaml"
            ),
            "streaming_config": os.path.join(
                controller_config_dir, "streaming_config.yaml"
            ),
        }

    def _write_engine_config_to_remote(
        self, config_dict: dict, local_path: str, remote_path: str
    ) -> None:
        """
        Write the engine config YAML locally then rsync it to the remote node.

        Follows the same local-write + rsync pattern used for controller configs
        (see sync.rsync_controller_client_configs).

        Args:
            config_dict: Engine config dict as returned by _build_engine_config
            local_path: Local path to write the YAML file to
            remote_path: Absolute path on the remote node where the file should land
        """
        import utils  # top-level module in the experiments/ tree

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        config_yaml = yaml.dump(
            config_dict, default_flow_style=False, allow_unicode=True
        )
        with open(local_path, "w") as f:
            f.write(config_yaml)

        hostname = f"node{self.node_offset}.{self.provider.hostname_suffix}"
        # Ensure the remote directory exists before rsyncing
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"mkdir -p {os.path.dirname(remote_path)}",
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=False,
        )
        cmd = 'rsync -azh -e "ssh {}" {} {}@{}:{}'.format(
            constants.SSH_OPTIONS,
            local_path,
            self.provider.username,
            hostname,
            remote_path,
        )
        utils.run_cmd_with_retry(cmd, popen=False, ignore_errors=False)

    def start(
        self,
        experiment_output_dir: str,
        local_experiment_dir: str,
        flink_output_format: str,
        prometheus_scrape_interval: int,
        log_level: str,
        profile_query_engine: bool,
        manual: bool,
        streaming_engine: str,
        forward_unsupported_queries: bool,
        controller_remote_output_dir: str,
        compress_json: bool,
        dump_precomputes: bool,
        lock_strategy: str,
        query_language: str = "PROMQL",
        **kwargs,
    ) -> None:
        """
        Start the Rust query engine.

        Args:
            experiment_output_dir: Remote directory for experiment output
            local_experiment_dir: Local experiment directory (used to write the engine
                                  config YAML locally before rsyncing to remote)
            flink_output_format: Format of data from Flink (used as Kafka input_format
                                 when streaming_engine=arroyo)
            prometheus_scrape_interval: Prometheus scraping interval
            log_level: Logging level
            profile_query_engine: Whether to enable profiling
            manual: Whether to run in manual mode
            streaming_engine: Type of streaming engine ('arroyo' or 'precompute')
            forward_unsupported_queries: Whether to forward unsupported queries
            controller_remote_output_dir: Controller output directory
            compress_json: Whether JSON is compressed (arroyo/Kafka only)
            dump_precomputes: Whether to dump precomputed values
            lock_strategy: Lock strategy for SimpleMapStore (global or per-key)
            query_language: Query language (SQL or PROMQL), defaults to PROMQL
            **kwargs: Additional configuration.
                      Required: prometheus_port, http_port.
                      Optional: prometheus_host (defaults to coordinator node IP),
                                remote_write_port (port the precompute engine listens on
                                for Prometheus remote write; should match
                                streaming.remote_write.base_port, defaults to 8080).
        """
        # Extract prometheus configuration
        prometheus_host = kwargs.get(
            "prometheus_host", self.provider.get_node_ip(self.node_offset)
        )
        prometheus_port = kwargs["prometheus_port"]  # Required, no default
        http_port = kwargs["http_port"]  # Required, no default
        # Port the precompute engine listens on for Prometheus remote write.
        # Should match streaming.remote_write.base_port in the Hydra config.
        remote_write_port = kwargs.get("remote_write_port", 8080)

        if self.use_container:
            self._start_containerized(
                experiment_output_dir,
                local_experiment_dir,
                flink_output_format,
                prometheus_scrape_interval,
                log_level,
                profile_query_engine,
                manual,
                streaming_engine,
                forward_unsupported_queries,
                controller_remote_output_dir,
                compress_json,
                prometheus_host,
                prometheus_port,
                http_port,
                remote_write_port,
                dump_precomputes,
                query_language,
                lock_strategy,
            )
        else:
            self._start_bare_metal(
                experiment_output_dir,
                local_experiment_dir,
                flink_output_format,
                prometheus_scrape_interval,
                log_level,
                profile_query_engine,
                manual,
                streaming_engine,
                forward_unsupported_queries,
                controller_remote_output_dir,
                compress_json,
                prometheus_host,
                prometheus_port,
                http_port,
                remote_write_port,
                dump_precomputes,
                query_language,
                lock_strategy,
            )

    def _start_bare_metal(
        self,
        experiment_output_dir: str,
        local_experiment_dir: str,
        flink_output_format: str,
        prometheus_scrape_interval: int,
        log_level: str,
        profile_query_engine: bool,
        manual: bool,
        streaming_engine: str,
        forward_unsupported_queries: bool,
        controller_remote_output_dir: str,
        compress_json: bool,
        prometheus_host: str,
        prometheus_port: int,
        http_port: int,
        remote_write_port: int,
        dump_precomputes: bool,
        query_language: str,
        lock_strategy: str,
    ) -> None:
        """Start Rust QueryEngine using bare metal deployment."""
        output_dir = os.path.join(experiment_output_dir, "query_engine_output")
        local_output_dir = os.path.join(local_experiment_dir, "query_engine_output")

        config = self._build_engine_config(
            output_dir=output_dir,
            flink_output_format=flink_output_format,
            prometheus_scrape_interval=prometheus_scrape_interval,
            log_level=log_level,
            streaming_engine=streaming_engine,
            forward_unsupported_queries=forward_unsupported_queries,
            controller_config_dir=controller_remote_output_dir,
            compress_json=compress_json,
            prometheus_server=f"http://{prometheus_host}:{prometheus_port}",
            http_port=http_port,
            remote_write_port=remote_write_port,
            dump_precomputes=dump_precomputes,
            query_language=query_language,
            lock_strategy=lock_strategy,
            profile_query_engine=profile_query_engine,
            kafka_broker=f"{self.provider.get_node_ip(self.node_offset)}:9092",
        )
        self._write_engine_config_to_remote(
            config_dict=config,
            local_path=os.path.join(local_output_dir, "engine_config.yaml"),
            remote_path=os.path.join(output_dir, "engine_config.yaml"),
        )

        cmd_dir = os.path.join(
            self.provider.get_home_dir(), "code", "asap-query-engine"
        )
        cmd = (
            f"../target/release/query_engine_rust"
            f" --config-file {output_dir}/engine_config.yaml"
            f" > {output_dir}/query_engine_rust.out 2>&1 &"
        )
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=True,
            popen=False,
            ignore_errors=False,
            manual=manual,
        )

    def _start_containerized(
        self,
        experiment_output_dir: str,
        local_experiment_dir: str,
        flink_output_format: str,
        prometheus_scrape_interval: int,
        log_level: str,
        profile_query_engine: bool,
        manual: bool,
        streaming_engine: str,
        forward_unsupported_queries: bool,
        controller_remote_output_dir: str,
        compress_json: bool,
        prometheus_host: str,
        prometheus_port: int,
        http_port: int,
        remote_write_port: int,
        dump_precomputes: bool,
        query_language: str,
        lock_strategy: str,
    ) -> None:
        """Start Rust QueryEngine using containerized deployment with Jinja template."""
        output_dir = os.path.join(experiment_output_dir, "query_engine_output")
        local_output_dir = os.path.join(local_experiment_dir, "query_engine_output")

        # Inside the container, outputs are mounted at /app/outputs and controller
        # configs are mounted at /app/controller_output (read-only).  All paths
        # written into the config dict must be container-internal paths.
        container_output_dir = "/app/outputs"
        container_controller_dir = "/app/controller_output"

        config = self._build_engine_config(
            output_dir=container_output_dir,
            flink_output_format=flink_output_format,
            prometheus_scrape_interval=prometheus_scrape_interval,
            log_level=log_level,
            streaming_engine=streaming_engine,
            forward_unsupported_queries=forward_unsupported_queries,
            controller_config_dir=container_controller_dir,
            compress_json=compress_json,
            prometheus_server=f"http://{prometheus_host}:{prometheus_port}",
            http_port=http_port,
            remote_write_port=remote_write_port,
            dump_precomputes=dump_precomputes,
            query_language=query_language,
            lock_strategy=lock_strategy,
            profile_query_engine=profile_query_engine,
            kafka_broker=f"{self.provider.get_node_ip(self.node_offset)}:9092",
        )
        # Write the config to the host path that is volume-mounted as /app/outputs,
        # so the container finds it at /app/outputs/engine_config.yaml.
        self._write_engine_config_to_remote(
            config_dict=config,
            local_path=os.path.join(local_output_dir, "engine_config.yaml"),
            remote_path=os.path.join(output_dir, "engine_config.yaml"),
        )

        # Paths on remote CloudLab node
        queryengine_dir = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "asap-query-engine"
        )
        template_path = os.path.join(queryengine_dir, "docker-compose.yml.j2")
        remote_compose_file = os.path.join(output_dir, "docker-compose.yml")
        helper_script = os.path.join(
            constants.CLOUDLAB_HOME_DIR,
            "code",
            "asap-tools",
            "experiments",
            "generate_queryengine_compose.py",
        )
        self.compose_file = remote_compose_file

        # Build command to generate docker-compose file using helper script
        generate_cmd = f"python3 {helper_script}"
        generate_cmd += f" --template-path '{template_path}'"
        generate_cmd += f" --output-path '{remote_compose_file}'"
        generate_cmd += f" --queryengine-dir '{queryengine_dir}'"
        generate_cmd += f" --container-name '{self.container_name}'"
        generate_cmd += f" --experiment-output-dir '{output_dir}'"
        generate_cmd += (
            f" --controller-remote-output-dir '{controller_remote_output_dir}'"
        )
        generate_cmd += f" --log-level '{log_level}'"
        generate_cmd += f" --http-port '{http_port}'"
        if manual:
            generate_cmd += " --manual"

        cmd = f"mkdir -p {output_dir}; {generate_cmd}; docker compose -f {remote_compose_file} up --no-build -d"

        if manual:
            print(f"Directory to run command: {queryengine_dir}")
            print(f"Manual mode: Run command: {cmd}")
            input("Press Enter to continue...")
        else:
            try:
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=queryengine_dir,
                    nohup=False,
                    popen=False,
                    ignore_errors=False,
                )
            except Exception as e:
                print(f"Failed to start Rust QueryEngine container: {e}")
                raise

    def stop(self, **kwargs) -> None:
        """
        Stop the Rust query engine process.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        if self.use_container:
            self._stop_containerized()
        else:
            self._stop_bare_metal()

    def _stop_bare_metal(self) -> None:
        """Stop Rust QueryEngine using bare metal deployment."""
        cmd = "pkill -f query_engine_rust"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def _stop_containerized(self) -> None:
        """Stop Rust QueryEngine using containerized deployment."""
        try:
            if self.compose_file:
                # Stop using docker compose command on remote node
                cmd = f"docker compose -f {self.compose_file} down"
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=False,
                    ignore_errors=True,
                )
                self.compose_file = None
            else:
                # Fallback: stop by container name on remote node
                cmd = f"docker stop {self.container_name}; docker rm {self.container_name}"
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=False,
                    ignore_errors=True,
                )
        except Exception as e:
            print(f"Error stopping Rust QueryEngine container: {e}")

    def is_healthy(self) -> bool:
        """
        Check if Rust query engine is healthy by checking if process is running.

        Returns:
            True if Rust query engine process is running
        """
        if self.use_container:
            return self._is_healthy_containerized()
        else:
            return self._is_healthy_bare_metal()

    def _is_healthy_bare_metal(self) -> bool:
        """Check if Rust QueryEngine is healthy using bare metal deployment."""
        try:
            cmd = "pgrep -f query_engine_rust"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            import subprocess

            assert isinstance(result, subprocess.CompletedProcess)
            return result.stdout.strip() != ""
        except Exception:
            return False

    def _is_healthy_containerized(self) -> bool:
        """Check if Rust QueryEngine is healthy using containerized deployment."""
        try:
            # Check if container is running
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", self.container_name],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip() == "true"
        except subprocess.CalledProcessError:
            return False
        except Exception:
            return False

    def get_monitoring_keyword(self) -> str:
        """
        Get the keyword to use for process monitoring.

        Returns:
            Container name if using containers, otherwise process name
        """
        if self.use_container:
            return self.container_name
        else:
            return constants.QUERY_ENGINE_RS_PROCESS_KEYWORD


class QueryEngineServiceFactory:
    """Factory for creating appropriate query engine services."""

    @staticmethod
    def create_query_engine_service(
        language: str,
        provider: InfrastructureProvider,
        use_container: bool,
        node_offset: int,
    ) -> BaseQueryEngineService:
        """
        Create a query engine service based on language.

        Args:
            language: Programming language ("python" or "rust")
            provider: Infrastructure provider for node communication and management
            use_container: Whether to use containerized deployment
            node_offset: Starting node index offset

        Returns:
            Appropriate query engine service instance

        Raises:
            ValueError: If language is not supported
        """
        if language == "python":
            return QueryEngineService(provider, use_container, node_offset)
        elif language == "rust":
            return QueryEngineRustService(provider, use_container, node_offset)
        else:
            raise ValueError(
                f"Invalid query engine language: {language}. Supported languages are 'python' and 'rust'"
            )

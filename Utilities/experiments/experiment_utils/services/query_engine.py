"""
Query Engine service management for experiments.
"""

import os
import subprocess

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
            streaming_engine: Type of streaming engine (flink/arroyo)
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
            "Utilities",
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
        lock_strategy: str,
        query_language: str = "PROMQL",
        **kwargs,
    ) -> None:
        """
        Start the Rust query engine.

        Args:
            experiment_output_dir: Directory for experiment output
            flink_output_format: Format of data from Flink
            prometheus_scrape_interval: Prometheus scraping interval
            log_level: Logging level
            profile_query_engine: Whether to enable profiling
            manual: Whether to run in manual mode
            streaming_engine: Type of streaming engine (flink/arroyo)
            forward_unsupported_queries: Whether to forward unsupported queries
            controller_remote_output_dir: Controller output directory
            compress_json: Whether JSON is compressed
            dump_precomputes: Whether to dump precomputed values
            lock_strategy: Lock strategy for SimpleMapStore (global or per-key)
            query_language: Query language (SQL or PROMQL), defaults to PROMQL
            **kwargs: Additional configuration (requires prometheus_port, http_port)
        """
        # Extract prometheus configuration
        prometheus_host = kwargs.get(
            "prometheus_host", self.provider.get_node_ip(self.node_offset)
        )
        prometheus_port = kwargs["prometheus_port"]  # Required, no default
        http_port = kwargs["http_port"]  # Required, no default

        if self.use_container:
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
                prometheus_port,
                http_port,
                dump_precomputes,
                query_language,
                lock_strategy,
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
                prometheus_host,
                prometheus_port,
                http_port,
                dump_precomputes,
                query_language,
                lock_strategy,
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
        prometheus_host: str,
        prometheus_port: int,
        http_port: int,
        dump_precomputes: bool,
        query_language: str,
        lock_strategy: str,
    ) -> None:
        """Start Rust QueryEngine using bare metal deployment."""
        output_dir = os.path.join(experiment_output_dir, "query_engine_output")
        prometheus_server = f"http://{prometheus_host}:{prometheus_port}"

        cmd = (
            "mkdir -p {}; ../target/release/query_engine_rust "
            "--kafka-topic {} "
            "--input-format {} "
            "--config {}/inference_config.yaml "
            "--streaming-config {}/streaming_config.yaml "
            "--prometheus-scrape-interval {} "
            "--prometheus-server {} "
            "--http-port {} "
            "--delete-existing-db "
            "--log-level {} "
            "--output-dir {} "
            "{} "
            "--streaming-engine {} "
            "--query-language {} "
            "--lock-strategy {} "
        ).format(
            output_dir,
            constants.FLINK_OUTPUT_TOPIC,
            flink_output_format,
            controller_remote_output_dir,
            controller_remote_output_dir,
            prometheus_scrape_interval,
            prometheus_server,
            http_port,
            log_level,
            output_dir,
            "--decompress-json" if compress_json else "",
            streaming_engine,
            query_language,
            lock_strategy,
        )

        if profile_query_engine:
            cmd += "--do-profiling "
        if forward_unsupported_queries:
            cmd += "--forward-unsupported-queries "
        if dump_precomputes:
            cmd += "--dump-precomputes "
        cmd += "> {}/query_engine_rust.out 2>&1 &".format(output_dir)

        cmd_dir = os.path.join(self.provider.get_home_dir(), "code", "QueryEngineRust")
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
        prometheus_port: int,
        http_port: int,
        dump_precomputes: bool,
        query_language: str,
        lock_strategy: str,
    ) -> None:
        """Start Rust QueryEngine using containerized deployment with Jinja template."""
        output_dir = os.path.join(experiment_output_dir, "query_engine_output")

        # Paths on remote CloudLab node
        queryengine_dir = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "QueryEngineRust"
        )
        template_path = os.path.join(queryengine_dir, "docker-compose.yml.j2")
        remote_compose_file = os.path.join(output_dir, "docker-compose.yml")
        helper_script = os.path.join(
            constants.CLOUDLAB_HOME_DIR,
            "code",
            "Utilities",
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
        generate_cmd += f" --query-language '{query_language}'"
        generate_cmd += f" --lock-strategy '{lock_strategy}'"
        generate_cmd += f" --kafka-host '{self.provider.get_node_ip(self.node_offset)}'"
        generate_cmd += f" --prometheus-host '{prometheus_host}'"
        generate_cmd += f" --prometheus-port '{prometheus_port}'"
        generate_cmd += f" --http-port '{http_port}'"

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

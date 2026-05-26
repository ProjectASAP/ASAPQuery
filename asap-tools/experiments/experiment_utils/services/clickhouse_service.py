"""
ClickHouse Docker service management and data loading for SQL experiment infrastructure.
"""

import os
import shlex
import subprocess
from typing import Optional
from jinja2 import Template

from .base import BaseService, DockerServiceBase
from experiment_utils.providers.base import InfrastructureProvider
import constants
import utils


class ClickHouseService(DockerServiceBase):
    """Manages a ClickHouse Docker container on a remote CloudLab node."""

    CONTAINER_NAME = "clickhouse-server"
    DEFAULT_HTTP_PORT = 8123
    DEFAULT_NATIVE_PORT = 9000
    DEFAULT_IMAGE_TAG = "latest"
    DEFAULT_DATABASE = "default"

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        super().__init__(provider, num_nodes, node_offset)
        self.compose_file: Optional[str] = None
        self._http_port = self.DEFAULT_HTTP_PORT

    def get_container_name(self) -> str:
        return self.CONTAINER_NAME

    def get_service_url(self) -> str:
        return f"http://localhost:{self._http_port}"

    def get_health_endpoint(self) -> str:
        # ClickHouse HTTP interface responds with "Ok." on /ping when ready
        return "/ping"

    def get_http_port(self) -> int:
        return self._http_port

    def is_healthy(self) -> bool:
        """ClickHouse is ready only when /ping returns exactly 'Ok.'"""
        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"curl -s http://localhost:{self._http_port}/ping",
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )
        if not isinstance(result, subprocess.CompletedProcess):
            return False
        return result.returncode == 0 and result.stdout.strip() == "Ok."

    def start(
        self,
        experiment_output_dir: str,
        local_experiment_dir: str,
        http_port: int = DEFAULT_HTTP_PORT,
        native_port: int = DEFAULT_NATIVE_PORT,
        database: str = DEFAULT_DATABASE,
        image_tag: str = DEFAULT_IMAGE_TAG,
        cpu_limit: Optional[float] = None,
        memory_limit: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Render the Jinja2 compose template, rsync it to the node, and start ClickHouse.

        Args:
            experiment_output_dir: Remote directory for data and config storage
            local_experiment_dir: Local directory to write the rendered compose file
            http_port: ClickHouse HTTP interface port (default 8123)
            native_port: ClickHouse native TCP port (default 9000)
            database: Default database name
            image_tag: Docker image tag for clickhouse/clickhouse-server
            cpu_limit: Optional CPU limit (e.g. 4.0)
            memory_limit: Optional memory limit (e.g. "8g")
        """
        self._http_port = http_port
        self._force_cleanup_container()

        data_dir = os.path.join(experiment_output_dir, "clickhouse_data")
        log_dir = os.path.join(experiment_output_dir, "clickhouse_logs")

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"mkdir -p {data_dir} {log_dir} && chmod 777 {data_dir} {log_dir}",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        template_path = os.path.join(
            os.path.dirname(__file__), "docker-compose.clickhouse.yml.j2"
        )
        with open(template_path, "r") as f:
            template = Template(f.read())

        compose_content = template.render(
            container_name=self.CONTAINER_NAME,
            image_tag=image_tag,
            database=database,
            data_dir=data_dir,
            log_dir=log_dir,
            cpu_limit=str(cpu_limit) if cpu_limit is not None else None,
            memory_limit=memory_limit,
        )

        local_compose_file = os.path.join(
            local_experiment_dir, "docker-compose.clickhouse.yml"
        )
        os.makedirs(os.path.dirname(local_compose_file), exist_ok=True)
        with open(local_compose_file, "w") as f:
            f.write(compose_content)

        remote_compose_file = os.path.join(
            experiment_output_dir, "docker-compose.clickhouse.yml"
        )
        self.compose_file = remote_compose_file

        hostname = f"node{self.node_offset}.{self.provider.hostname_suffix}"
        rsync_cmd = 'rsync -azh -e "ssh {}" {} {}@{}:{}'.format(
            constants.SSH_OPTIONS,
            local_compose_file,
            self.provider.username,
            hostname,
            remote_compose_file,
        )
        utils.run_cmd_with_retry(rsync_cmd, popen=False, ignore_errors=False)

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"docker compose -f {remote_compose_file} up -d",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        self._wait_for_service_ready()

    def stop(self, **kwargs) -> None:
        """Stop and remove the ClickHouse container."""
        if self.compose_file:
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=f"docker compose -f {self.compose_file} down",
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            self.compose_file = None
        else:
            self._force_cleanup_container()


class ClickHouseDataLoaderService(BaseService):
    """Loads datasets into ClickHouse for SQL experiment infrastructure.

    Supports ClickBench (JSON-lines) and H2O GroupBy (CSV) out of the box, plus
    custom JSON-lines datasets. Always drops and recreates the target table before
    loading to guarantee a clean state.

    Built-in DDL and the H2O loader script live alongside this file in
    ``experiment_utils/services/`` and are rsynced to the remote node at runtime:
    - ``clickbench_init.sql`` — hits table schema
    - ``h2o_init.sql`` — h2o_groupby table schema
    - ``h2o_clickhouse_loader.py`` — standalone H2O CSV → ClickHouse loader

    Typical usage::

        loader = ClickHouseDataLoaderService(provider, num_nodes=1, node_offset=0)
        loader.prepare(local_data_file="/local/hits.json.gz", remote_dir="/scratch/data")
        loader.start(dataset_name="clickbench")
    """

    # Directory containing this file; used to locate sibling DDL / script assets.
    _ASSETS_DIR = os.path.dirname(os.path.abspath(__file__))

    # Built-in DDL file names (relative to _ASSETS_DIR).
    BUILTIN_DDL_FILES = {
        "clickbench": "clickbench/init.sql",
        "h2o": "h2o/init.sql",
    }

    # H2O loader script (relative to _ASSETS_DIR).
    H2O_LOADER_SCRIPT = "h2o/loader.py"

    H2O_BATCH_SIZE = 50_000

    DEFAULT_TABLES = {
        "clickbench": "hits",
        "h2o": "h2o_groupby",
    }

    def __init__(
        self,
        provider: InfrastructureProvider,
        num_nodes: int,
        node_offset: int,
        clickhouse_http_port: int = 8123,
    ):
        super().__init__(provider)
        self.num_nodes = num_nodes
        self.node_offset = node_offset
        self.clickhouse_http_port = clickhouse_http_port
        self.remote_data_file: Optional[str] = None

    def prepare(self, local_data_file: str, remote_dir: str) -> str:
        """Rsync a local data file to the remote node.

        Args:
            local_data_file: Absolute path to the local data file.
            remote_dir: Remote directory to place the file in.

        Returns:
            The full remote path to the rsynced file.
        """
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"mkdir -p {remote_dir}",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )
        hostname = f"node{self.node_offset}.{self.provider.hostname_suffix}"
        rsync_cmd = 'rsync -azh --progress -e "ssh {}" {} {}@{}:{}/'.format(
            constants.SSH_OPTIONS,
            local_data_file,
            self.provider.username,
            hostname,
            remote_dir,
        )
        utils.run_cmd_with_retry(rsync_cmd, popen=False, ignore_errors=False)
        self.remote_data_file = os.path.join(
            remote_dir, os.path.basename(local_data_file)
        )
        return self.remote_data_file

    def start(
        self,
        dataset_name: str,
        remote_data_file: Optional[str] = None,
        table: Optional[str] = None,
        batch_size: int = H2O_BATCH_SIZE,
        init_sql_file: Optional[str] = None,
        max_rows: int = 0,
        **kwargs,
    ) -> None:
        """Drop, recreate, and load data into ClickHouse.

        Args:
            dataset_name: One of ``'clickbench'``, ``'h2o'``, or ``'custom'``.
            remote_data_file: Path on the remote node. Defaults to the path
                stored by the most recent :meth:`prepare` call.
            table: Target table name. Defaults to the dataset's standard table
                (``hits`` for clickbench, ``h2o_groupby`` for h2o).
            batch_size: INSERT batch size for H2O loading (default 50 000).
            init_sql_file: Path to a DDL SQL file *already on the remote node*.
                When ``None``, the built-in ``*_init.sql`` is rsynced and used.
            max_rows: Maximum rows to load (0 = all).
        """
        if remote_data_file is None:
            remote_data_file = self.remote_data_file
        if remote_data_file is None:
            raise ValueError(
                "remote_data_file not set; call prepare() first or pass remote_data_file"
            )

        table = table or self.DEFAULT_TABLES.get(dataset_name)
        if table is None:
            raise ValueError(
                f"table must be specified for dataset_name={dataset_name!r}"
            )

        url = f"http://localhost:{self.clickhouse_http_port}/"

        print(f"Dropping table {table!r}...")
        self._exec_sql(f"DROP TABLE IF EXISTS {table}", url)

        if init_sql_file is not None:
            print(f"Running init SQL from {init_sql_file!r}...")
            self._exec_sql_file(init_sql_file, url)
        elif dataset_name in self.BUILTIN_DDL_FILES:
            local_ddl = os.path.join(
                self._ASSETS_DIR, self.BUILTIN_DDL_FILES[dataset_name]
            )
            print(f"Initializing schema for {dataset_name!r} from {local_ddl!r}...")
            remote_ddl = f"/tmp/{dataset_name}_init_{os.getpid()}.sql"
            self._rsync_to_remote(local_ddl, remote_ddl)
            try:
                self._exec_sql_file(remote_ddl, url)
            finally:
                self._remote_rm(remote_ddl)
        elif dataset_name != "custom":
            raise ValueError(
                f"No built-in DDL for dataset_name={dataset_name!r}; pass init_sql_file"
            )

        if dataset_name == "clickbench":
            self._load_clickbench(remote_data_file, url, table, max_rows)
        elif dataset_name == "h2o":
            self._load_h2o(remote_data_file, url, batch_size, max_rows)
        elif dataset_name == "custom":
            self._load_custom(remote_data_file, table, url, max_rows)
        else:
            raise ValueError(
                f"Unsupported dataset_name={dataset_name!r}; "
                "expected 'clickbench', 'h2o', or 'custom'"
            )

        count = self._check_row_count(table, url)
        print(f"Loaded {count:,} rows into ClickHouse ({table!r})")

    def stop(self, **kwargs) -> None:
        pass

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _rsync_to_remote(self, local_path: str, remote_path: str) -> None:
        """Rsync a single local file to an exact remote path."""
        hostname = f"node{self.node_offset}.{self.provider.hostname_suffix}"
        cmd = 'rsync -azh -e "ssh {}" {} {}@{}:{}'.format(
            constants.SSH_OPTIONS,
            local_path,
            self.provider.username,
            hostname,
            remote_path,
        )
        utils.run_cmd_with_retry(cmd, popen=False, ignore_errors=False)

    def _remote_rm(self, remote_path: str) -> None:
        """Remove a file on the remote node, ignoring errors."""
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"rm -f {shlex.quote(remote_path)}",
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def _exec_sql(self, sql: str, url: str) -> None:
        """Execute a single SQL statement via curl on the remote node."""
        cmd = "curl -sS {} --data {}".format(shlex.quote(url), shlex.quote(sql))
        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )
        if isinstance(result, subprocess.CompletedProcess):
            if result.returncode != 0:
                raise RuntimeError(
                    f"SQL execution failed (exit {result.returncode}): "
                    f"{result.stderr.strip()[:200]}"
                )
            if result.stdout and "Code:" in result.stdout:
                print(f"  WARN ClickHouse: {result.stdout.strip()[:200]}")
        short = sql.strip()[:80].replace("\n", " ")
        print(f"  SQL OK: {short}")

    def _exec_sql_file(self, remote_sql_file: str, url: str) -> None:
        """Read a SQL file on the remote node and execute each semicolon-delimited statement."""
        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd="cat {}".format(shlex.quote(remote_sql_file)),
            cmd_dir=None,
            nohup=False,
            popen=False,
        )
        assert isinstance(result, subprocess.CompletedProcess)
        for stmt in (s.strip() for s in result.stdout.split(";") if s.strip()):
            self._exec_sql(stmt, url)

    def _check_row_count(self, table: str, url: str) -> int:
        """Return the row count for a table on the remote node, or 0 on error."""
        cmd = "curl -sS {} --data {}".format(
            shlex.quote(url),
            shlex.quote(f"SELECT count() FROM {table}"),
        )
        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )
        if isinstance(result, subprocess.CompletedProcess) and result.returncode == 0:
            try:
                return int(result.stdout.strip())
            except (ValueError, TypeError):
                pass
        return 0

    def _load_clickbench(
        self, remote_data_file: str, url: str, table: str, max_rows: int
    ) -> None:
        """Stream a JSON-lines file (optionally gzipped) into ClickHouse."""
        print(f"Loading ClickBench data from {remote_data_file!r}...")
        file_lower = remote_data_file.lower()
        is_gz = file_lower.endswith(".json.gz") or file_lower.endswith(".jsonl.gz")
        insert_sql = shlex.quote(f"INSERT INTO {table} FORMAT JSONEachRow")

        if is_gz:
            if max_rows > 0:
                reader = "zcat {} | head -n {}".format(
                    shlex.quote(remote_data_file), max_rows
                )
            else:
                reader = "zcat {}".format(shlex.quote(remote_data_file))
        else:
            if max_rows > 0:
                reader = "head -n {} {}".format(max_rows, shlex.quote(remote_data_file))
            else:
                reader = "cat {}".format(shlex.quote(remote_data_file))

        cmd = (
            "{} | docker exec -i clickhouse-server clickhouse-client --query {}".format(
                reader, insert_sql
            )
        )

        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )
        if isinstance(result, subprocess.CompletedProcess) and result.returncode != 0:
            raise RuntimeError(
                f"ClickBench data load failed: {result.stderr.strip()[:200]}"
            )

    def _load_h2o(
        self, remote_data_file: str, url: str, batch_size: int, max_rows: int
    ) -> None:
        """Rsync h2o_clickhouse_loader.py to the remote node and execute it."""
        print(f"Loading H2O data from {remote_data_file!r}...")
        local_script = os.path.join(self._ASSETS_DIR, self.H2O_LOADER_SCRIPT)
        remote_script = f"/tmp/h2o_loader_{os.getpid()}.py"
        self._rsync_to_remote(local_script, remote_script)
        try:
            cmd = "python3 {} --data-file {} --url {} --batch-size {} --max-rows {}".format(
                shlex.quote(remote_script),
                shlex.quote(remote_data_file),
                shlex.quote(url),
                batch_size,
                max_rows,
            )
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
            )
            if (
                isinstance(result, subprocess.CompletedProcess)
                and result.returncode != 0
            ):
                raise RuntimeError(
                    f"H2O data load failed: {result.stderr.strip()[:200]}"
                )
        finally:
            self._remote_rm(remote_script)

    def _load_custom(
        self, remote_data_file: str, table: str, url: str, max_rows: int
    ) -> None:
        """Stream a custom JSON-lines file (plain or gzipped) into ClickHouse."""
        print(f"Loading custom data from {remote_data_file!r} into {table!r}...")
        file_lower = remote_data_file.lower()
        is_gz = file_lower.endswith(".json.gz") or file_lower.endswith(".jsonl.gz")
        is_json = file_lower.endswith(".json") or file_lower.endswith(".jsonl")
        insert_sql = shlex.quote(f"INSERT INTO {table} FORMAT JSONEachRow")

        if is_gz:
            if max_rows > 0:
                reader = "zcat {} | head -n {}".format(
                    shlex.quote(remote_data_file), max_rows
                )
            else:
                reader = "zcat {}".format(shlex.quote(remote_data_file))
        elif is_json:
            if max_rows > 0:
                reader = "head -n {} {}".format(max_rows, shlex.quote(remote_data_file))
            else:
                reader = "cat {}".format(shlex.quote(remote_data_file))
        else:
            raise ValueError(
                f"Unsupported file format for {remote_data_file!r}. "
                "Use dataset_name='h2o' for CSV files."
            )

        cmd = (
            "{} | docker exec -i clickhouse-server clickhouse-client --query {}".format(
                reader, insert_sql
            )
        )

        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )
        if isinstance(result, subprocess.CompletedProcess) and result.returncode != 0:
            raise RuntimeError(
                f"Custom data load failed: {result.stderr.strip()[:200]}"
            )

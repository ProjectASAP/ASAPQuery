"""
Grafana dashboard configuration module using Grafana Foundation SDK.

This module provides functionality to generate Grafana dashboards from experiment
configurations using the Grafana Foundation SDK's builder pattern.
"""

import json
import urllib.parse
import os
import sys
import typing
import requests
import re
from dataclasses import dataclass
from typing import List, Dict, Optional, Any
from omegaconf import DictConfig, OmegaConf

from grafana_foundation_sdk.builders.dashboard import Dashboard
from grafana_foundation_sdk.builders.prometheus import Dataquery as PrometheusQuery
from grafana_foundation_sdk.builders.timeseries import Panel as Timeseries
from grafana_foundation_sdk.cog.encoder import JSONEncoder
from grafana_foundation_sdk.models.dashboard import Dashboard as DashboardModel

from promql_parser import parse as parse_promql

import hydra
import constants

# Register the same resolver as experiment_run_e2e.py
OmegaConf.register_new_resolver(
    "local_experiment_dir", lambda: constants.LOCAL_EXPERIMENT_DIR
)


@dataclass
class ServerConfig:
    """Configuration for a server datasource."""

    name: str
    url: str


@dataclass
class QueryConfig:
    """Configuration for a query with timing parameters."""

    query: str
    repetition_delay: int
    query_time_offset: int


class GrafanaConfig:
    host: str
    user: str
    password: str

    def __init__(self, host: str = "", user: str = "", password: str = ""):
        self.host = host
        self.user = user
        self.password = password

    @classmethod
    def from_env(cls) -> typing.Self:
        return cls(
            host=os.environ.get("GRAFANA_HOST", "localhost:3000"),
            user=os.environ.get("GRAFANA_USER", "admin"),
            password=os.environ.get("GRAFANA_PASSWORD", "admin"),
        )

    @classmethod
    def from_config(cls, grafana_cfg: DictConfig) -> typing.Self:
        return cls(
            host=grafana_cfg.get("host", "localhost:3000"),
            user=grafana_cfg.get("user", "admin"),
            password=grafana_cfg.get("password", "admin"),
        )


class GrafanaClient:
    config: GrafanaConfig

    def __init__(self, config: GrafanaConfig):
        self.config = config

    def find_or_create_folder(self, name: str) -> str:
        auth = (self.config.user, self.config.password)
        response = requests.get(
            f"http://{self.config.host}/api/search?type=dash-folder&query={urllib.parse.quote_plus(name)}",
            auth=auth,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"could not fetch folders list: expected 200, got {response.status_code}"
            )

        # The folder exists.
        response_json = response.json()
        if len(response_json) == 1:
            return response_json[0]["uid"]

        # The folder doesn't exist: we create it.
        response = requests.post(
            f"http://{self.config.host}/api/folders",
            auth=auth,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"title": name}),
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"could not create new folder: expected 200, got {response.status_code}"
            )

        return response.json()["uid"]

    def persist_dashboard(self, dashboard: DashboardModel):
        auth = (self.config.user, self.config.password)
        response = requests.post(
            f"http://{self.config.host}/api/dashboards/db",
            auth=auth,
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {
                    "dashboard": dashboard,
                    "overwrite": True,
                },
                cls=JSONEncoder,
            ),
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"could not persist dashboard: expected 200, got {response.status_code}"
            )

    def find_datasource_by_name(self, name: str) -> typing.Optional[dict]:
        """Find a datasource by name. Returns the datasource dict or None if not found."""
        auth = (self.config.user, self.config.password)
        response = requests.get(
            f"http://{self.config.host}/api/datasources",
            auth=auth,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"could not fetch datasources list: expected 200, got {response.status_code}"
            )

        datasources = response.json()
        for ds in datasources:
            if ds.get("name") == name:
                return ds
        return None

    def create_or_update_datasource(self, datasource_config: dict) -> dict:
        """Create a new datasource or update an existing one by name."""
        auth = (self.config.user, self.config.password)

        # Check if datasource already exists
        existing_ds = self.find_datasource_by_name(datasource_config.get("name", ""))

        if existing_ds:
            # Update existing datasource
            datasource_id = existing_ds["id"]
            # Preserve the ID and version for updates
            update_config = {**datasource_config, "id": datasource_id}
            if "version" in existing_ds:
                update_config["version"] = existing_ds["version"]

            response = requests.put(
                f"http://{self.config.host}/api/datasources/{datasource_id}",
                auth=auth,
                headers={"Content-Type": "application/json"},
                data=json.dumps(update_config),
            )
            if response.status_code != 200:
                raise RuntimeError(
                    f"could not update datasource: expected 200, got {response.status_code}, response: {response.text}"
                )
        else:
            # Create new datasource
            response = requests.post(
                f"http://{self.config.host}/api/datasources",
                auth=auth,
                headers={"Content-Type": "application/json"},
                data=json.dumps(datasource_config),
            )
            if response.status_code not in [200, 201]:
                raise RuntimeError(
                    f"could not create datasource: expected 200 or 201, got {response.status_code}, response: {response.text}"
                )

        return response.json()

    def test_datasource_health(self, datasource_uid: str) -> bool:
        """
        Test datasource health/connectivity using Grafana's health check API.

        Args:
            datasource_uid: UID of the datasource to test

        Returns:
            True if datasource is healthy, False otherwise
        """
        auth = (self.config.user, self.config.password)

        try:
            response = requests.get(
                f"http://{self.config.host}/api/datasources/uid/{datasource_uid}/health",
                auth=auth,
                timeout=10,  # 10 second timeout for health checks
            )

            if response.status_code == 200:
                result = response.json()
                # Check if the health check was successful
                # Different plugins may return different structures, but 'status' is common
                return result.get("status") == "success" or result.get("status") == "OK"
            else:
                print(
                    f"Health check failed with status {response.status_code}: {response.text}"
                )
                return False

        except requests.exceptions.Timeout:
            print(f"Health check timed out for datasource {datasource_uid}")
            return False
        except requests.exceptions.RequestException as e:
            print(f"Health check failed for datasource {datasource_uid}: {e}")
            return False
        except Exception as e:
            print(
                f"Unexpected error during health check for datasource {datasource_uid}: {e}"
            )
            return False


@dataclass
class ExperimentDashboardConfig:
    """
    Parsed experiment configuration for dashboard generation.

    This class extracts and structures the relevant parts of the experiment
    configuration needed for dashboard creation.
    """

    experiment_name: str
    servers: List[ServerConfig]
    queries: List[QueryConfig]
    metric_names: List[str]

    @classmethod
    def from_experiment_config(cls, cfg: DictConfig) -> "ExperimentDashboardConfig":
        """
        Create ExperimentDashboardConfig from experiment configuration.

        Args:
            cfg: Hydra experiment configuration (can be partial or complete)

        Returns:
            Parsed dashboard configuration

        Raises:
            ValueError: If required configuration is missing or invalid
        """
        # Handle both complete configs (with experiment.name) and partial configs
        experiment_name = None
        if hasattr(cfg, "experiment") and hasattr(cfg.experiment, "name"):
            experiment_name = cfg.experiment.name
        else:
            # For partial configs, generate a name from the config structure
            experiment_name = "experiment_dashboard"

        # Look for experiment_params or use the root config if it contains servers/query_groups directly
        if hasattr(cfg, "experiment_params"):
            experiment_params = cfg.experiment_params
        elif hasattr(cfg, "servers") and hasattr(cfg, "query_groups"):
            # This is a partial config file - use the root level
            experiment_params = cfg
        else:
            raise ValueError(
                "Neither experiment_params nor direct server/query configuration found"
            )

        # Extract servers
        if not hasattr(experiment_params, "servers") or not experiment_params.servers:
            raise ValueError(
                "experiment_params.servers is required and cannot be empty"
            )

        servers = []
        for server in experiment_params.servers:
            if not hasattr(server, "name") or not hasattr(server, "url"):
                raise ValueError("Each server must have 'name' and 'url' fields")
            servers.append(ServerConfig(name=server.name, url=server.url))

        # Extract queries
        if (
            not hasattr(experiment_params, "query_groups")
            or not experiment_params.query_groups
        ):
            raise ValueError(
                "experiment_params.query_groups is required and cannot be empty"
            )

        queries = []
        for query_group in experiment_params.query_groups:
            if not hasattr(query_group, "queries") or not query_group.queries:
                continue

            repetition_delay = getattr(query_group, "repetition_delay", 30)
            query_time_offset = 0

            if hasattr(query_group, "client_options"):
                client_options = query_group.client_options
                query_time_offset = getattr(client_options, "query_time_offset", 0)

            for query_str in query_group.queries:
                queries.append(
                    QueryConfig(
                        query=query_str,
                        repetition_delay=repetition_delay,
                        query_time_offset=query_time_offset,
                    )
                )

        if not queries:
            raise ValueError("No valid queries found in query_groups")

        # Extract metric names
        metric_names = []
        if hasattr(experiment_params, "metrics") and experiment_params.metrics:
            for metric in experiment_params.metrics:
                if hasattr(metric, "metric"):
                    metric_names.append(metric.metric)

        if not metric_names:
            raise ValueError("No metrics found in experiment_params.metrics")

        return cls(
            experiment_name=experiment_name,
            servers=servers,
            queries=queries,
            metric_names=metric_names,
        )


class GrafanaDashboardBuilder:
    """
    Main class for building Grafana dashboards from experiment configurations.

    This class uses the Grafana Foundation SDK to programmatically generate
    dashboard configurations that can be deployed to Grafana.
    """

    def __init__(self):
        """Initialize the dashboard builder."""
        pass

    def build_dashboard_from_config(
        self, experiment_config: DictConfig
    ) -> Optional[str]:
        """
        Build a Grafana dashboard from experiment configuration.

        Args:
            experiment_config: Hydra experiment configuration

        Returns:
            Dashboard JSON string, or None if building failed
        """
        # Parse experiment config
        config = ExperimentDashboardConfig.from_experiment_config(experiment_config)

        # Calculate refresh interval (minimum repetition_delay)
        refresh_interval = self._calculate_refresh_interval(config.queries)

        # Calculate time range with offset
        time_from, time_to = self._calculate_time_range(config.queries)

        # Create dashboard builder
        dashboard_title = f"Experiment Dashboard - {config.experiment_name}"
        dashboard_uid = f"exp-{config.experiment_name}"

        builder = (
            Dashboard(dashboard_title)
            .uid(dashboard_uid)
            .tags(["experiment", config.experiment_name])
            .refresh(refresh_interval)
            .time(time_from, time_to)
            .timezone("browser")
        )

        # Create panels
        panels = self._create_panels_from_queries(config)
        for panel in panels:
            builder.with_panel(panel)

        # Build and export dashboard
        dashboard_obj = builder.build()
        encoder = JSONEncoder(sort_keys=True, indent=2)
        return encoder.encode(dashboard_obj)

    def configure_grafana(
        self,
        experiment_config: DictConfig,
        grafana_config: Optional[GrafanaConfig] = None,
    ) -> bool:
        """
        Configure Grafana with datasources and dashboard from experiment configuration.

        Args:
            experiment_config: Hydra experiment configuration
            grafana_config: Grafana connection configuration (uses experiment_config.grafana if None)

        Returns:
            True if configuration succeeded, False otherwise
        """
        try:
            # Parse experiment config
            config = ExperimentDashboardConfig.from_experiment_config(experiment_config)

            # Use provided config or create from Hydra config
            if grafana_config is None:
                grafana_config = GrafanaConfig.from_config(experiment_config.grafana)

            client = GrafanaClient(grafana_config)

            # Create datasources
            datasources = self.create_datasources_config(config.servers)
            for datasource in datasources:
                print(f"Creating/updating datasource: {datasource['name']}")
                result = client.create_or_update_datasource(datasource)

                # Test datasource health if it has a UID
                datasource_uid = result.get("uid")
                if datasource_uid:
                    print(f"Testing datasource health: {datasource['name']}")
                    is_healthy = client.test_datasource_health(datasource_uid)
                    if is_healthy:
                        print(f"✓ Datasource {datasource['name']} is healthy")
                    else:
                        print(f"⚠ Datasource {datasource['name']} health check failed")
                else:
                    print(
                        f"⚠ No UID returned for datasource {datasource['name']}, skipping health check"
                    )

            # Build and deploy dashboard
            dashboard_obj = self._build_dashboard_object(config)
            print(f"Deploying dashboard: {config.experiment_name}")
            client.persist_dashboard(dashboard_obj)

            print("Grafana configuration completed successfully!")
            return True

        except Exception as e:
            print(f"Error configuring Grafana: {e}")
            return False

    def _build_dashboard_object(
        self, config: ExperimentDashboardConfig
    ) -> DashboardModel:
        """Build dashboard object for API deployment."""
        # Calculate refresh interval
        refresh_interval = self._calculate_refresh_interval(config.queries)

        # Calculate time range with offset
        time_from, time_to = self._calculate_time_range(config.queries)

        # Create dashboard builder
        dashboard_title = f"Experiment Dashboard - {config.experiment_name}"
        dashboard_uid = f"exp-{config.experiment_name}"

        builder = (
            Dashboard(dashboard_title)
            .uid(dashboard_uid)
            .tags(["experiment", config.experiment_name])
            .refresh(refresh_interval)
            .time(time_from, time_to)
            .timezone("browser")
        )

        # Create panels
        panels = self._create_panels_from_queries(config)
        for panel in panels:
            builder.with_panel(panel)

        return builder.build()

    def _calculate_refresh_interval(self, queries: List[QueryConfig]) -> str:
        """
        Calculate dashboard refresh interval from query configurations.

        Args:
            queries: List of query configurations

        Returns:
            Refresh interval string (e.g., "30s")
        """
        if not queries:
            return "30s"

        min_delay = min(q.repetition_delay for q in queries)
        return f"{min_delay}s"

    def _calculate_time_range(self, queries: List[QueryConfig]) -> tuple[str, str]:
        """
        Calculate dashboard time range based on query time offsets.

        Args:
            queries: List of query configurations

        Returns:
            Tuple of (time_from, time_to) strings
        """
        if not queries:
            return "now-1h", "now"

        # Find the maximum time offset to determine the time range
        max_offset = max(q.query_time_offset for q in queries)

        if max_offset <= 0:
            return "now-1h", "now"

        # Create time range that accommodates the offset
        time_from = f"now-1h-{max_offset}s"
        time_to = f"now-{max_offset}s"

        return time_from, time_to

    def _create_panels_from_queries(
        self, config: ExperimentDashboardConfig
    ) -> List[Any]:
        """
        Create dashboard panels from queries and servers.

        Args:
            config: Parsed experiment configuration

        Returns:
            List of panel objects
        """
        panels = []
        panel_id = 1

        for query_config in config.queries:
            for server in config.servers:
                panel = self._create_single_panel(
                    panel_id=panel_id, query_config=query_config, server=server
                )
                panels.append(panel)
                panel_id += 1

        return panels

    def _create_single_panel(
        self, panel_id: int, query_config: QueryConfig, server: ServerConfig
    ) -> Any:
        """
        Create a single dashboard panel.

        Args:
            panel_id: Unique panel ID
            query_config: Query configuration
            server: Server configuration

        Returns:
            Panel object
        """
        # Use the original query without PromQL offset (time offset handled by dashboard time range)
        # Create prometheus target for instant query
        target = (
            PrometheusQuery()
            .expr(query_config.query)
            .legend_format("")
            .ref_id("A")
            .instant()
        )

        # Create panel with correct method signatures
        panel_title = f"{query_config.query} - {server.name}"
        panel = (
            Timeseries().title(panel_title).datasource(server.name).with_target(target)
        )

        return panel

    def _apply_query_time_offset(self, query: str, offset: int) -> str:
        """
        Apply time offset to PromQL query.

        Args:
            query: Original PromQL query
            offset: Time offset in seconds

        Returns:
            Modified query with time offset applied
        """
        if offset <= 0:
            return query

        # For time offset, we append the offset modifier to the entire query
        # This is the standard PromQL way to apply time offsets
        return f"{query} offset {offset}s"

    def _apply_query_time_offset_regex(
        self, query: str, offset: int, metric_names: List[str]
    ) -> str:
        """
        Apply time offset to PromQL query using regex to find metric selectors.

        This method uses regex pattern matching to identify metric selectors and applies
        the offset modifier in the correct location according to PromQL syntax rules.

        The offset modifier placement rules:
        1. For instant queries: metric{labels} offset Xs
        2. For range queries: metric{labels}[range] offset Xs

        Algorithm:
        1. For each known metric name, create a regex pattern that matches the complete selector
        2. Check if the metric already has an offset to avoid double-application
        3. Reconstruct the selector with offset in the correct position
        4. Validate the result using promql_parser to ensure syntactic correctness

        Args:
            query: Original PromQL query string
            offset: Time offset in seconds (must be > 0)
            metric_names: List of known metric names from experiment configuration

        Returns:
            Modified query string with offset applied to all metric selectors

        Raises:
            ValueError: If the resulting query is malformed according to PromQL syntax
        """
        if offset <= 0:
            return query

        modified_query = query

        # Apply offset to each known metric name found in the query
        for metric_name in metric_names:
            # Regex pattern explanation:
            # \b{re.escape(metric_name)} - Match exact metric name with word boundaries
            # (\{[^}]*\})? - Optional capture group for label selectors like {job="test", instance="host1"}
            # (\[[^\]]*\])? - Optional capture group for range selectors like [5m] or [1h]
            # (?!\w) - Negative lookahead to prevent partial matches (e.g., cpu_total shouldn't match cpu)
            pattern = rf"\b{re.escape(metric_name)}(\{{[^}}]*\}})?(\[[^\]]*\])?(?!\w)"

            def replace_metric(match):
                full_match = match.group(0)  # Complete matched text
                metric_part = match.group(0)  # Same as full_match
                labels_part = (
                    match.group(1) or ""
                )  # Captured label selectors or empty string
                range_part = (
                    match.group(2) or ""
                )  # Captured range selector or empty string

                # Avoid double-applying offset by checking what follows the match
                remaining_text = query[match.end() :]
                if remaining_text.strip().startswith("offset"):
                    return full_match  # Already has offset, don't modify

                # Extract just the metric name (remove labels and range from full match)
                metric_name_only = metric_part.replace(labels_part, "").replace(
                    range_part, ""
                )

                if range_part:
                    # Range query: place offset after range selector
                    # Example: cpu_usage{mode="idle"}[5m] -> cpu_usage{mode="idle"}[5m] offset 10s
                    return (
                        f"{metric_name_only}{labels_part}{range_part} offset {offset}s"
                    )
                else:
                    # Instant query: place offset after label selector (if any)
                    # Example: cpu_usage{mode="idle"} -> cpu_usage{mode="idle"} offset 10s
                    return f"{metric_name_only}{labels_part} offset {offset}s"

            # Apply the replacement function to all matches of this metric in the query
            modified_query = re.sub(pattern, replace_metric, modified_query)

        # Validate the modified query is well-formed
        try:
            parse_promql(modified_query)
        except Exception as e:
            raise ValueError(
                f"Generated malformed PromQL query '{modified_query}' from original '{query}': {e}"
            )

        return modified_query

    def create_datasources_config(
        self, servers: List[ServerConfig]
    ) -> List[Dict[str, Any]]:
        """
        Create datasource configurations for Grafana API.

        Args:
            servers: List of server configurations

        Returns:
            List of datasource configuration dictionaries
        """
        datasources = []

        for server in servers:
            datasource = {
                "name": server.name,
                "type": "prometheus",
                "url": server.url,
                "access": "proxy",
                "isDefault": False,
                "jsonData": {"httpMethod": "POST"},
            }
            datasources.append(datasource)

        return datasources


def build_dashboard_from_config(experiment_config: DictConfig) -> Optional[str]:
    """
    Convenience function to build dashboard from experiment configuration.

    Args:
        experiment_config: Hydra experiment configuration

    Returns:
        Dashboard JSON string, or None if building failed
    """
    builder = GrafanaDashboardBuilder()
    return builder.build_dashboard_from_config(experiment_config)


def create_datasources_config(
    experiment_config: DictConfig,
) -> Optional[List[Dict[str, Any]]]:
    """
    Convenience function to create datasource configurations.

    Args:
        experiment_config: Hydra experiment configuration

    Returns:
        List of datasource configurations, or None if parsing failed
    """
    config = ExperimentDashboardConfig.from_experiment_config(experiment_config)
    builder = GrafanaDashboardBuilder()
    return builder.create_datasources_config(config.servers)


def configure_grafana(
    experiment_config: DictConfig, grafana_config: Optional[GrafanaConfig] = None
) -> bool:
    """
    Convenience function to configure Grafana with datasources and dashboard.

    Args:
        experiment_config: Hydra experiment configuration
        grafana_config: Grafana connection configuration (uses experiment_config.grafana if None)

    Returns:
        True if configuration succeeded, False otherwise
    """
    builder = GrafanaDashboardBuilder()
    return builder.configure_grafana(experiment_config, grafana_config)


def parse_grafana_args():
    """Parse command line arguments for Grafana configuration mode."""
    configure_mode = False
    if "--configure" in sys.argv:
        configure_mode = True
        # Remove --configure from args so Hydra doesn't see it
        sys.argv = [arg for arg in sys.argv if arg != "--configure"]

    return configure_mode


@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg: DictConfig):
    """
    Main function using Hydra for configuration management.

    Usage examples:
      # Generate dashboard JSON
      python grafana_config.py experiment_type=cloud_demo experiment.name=test_dash

      # Configure Grafana directly
      python grafana_config.py experiment_type=cloud_demo experiment.name=test_dash --configure

      # With additional overrides
      python grafana_config.py experiment_type=collapsable cloudlab.num_nodes=2 experiment.name=pc_test_3 --configure
    """
    # Check if we're in configure mode (determined before Hydra processed args)
    configure_mode = hasattr(main, "_configure_mode") and main._configure_mode

    if configure_mode:
        print("Configuring Grafana...")
        # Use Grafana config from Hydra configuration
        grafana_config = GrafanaConfig.from_config(cfg.grafana)
        success = configure_grafana(cfg, grafana_config)
        if success:
            print("Grafana configuration completed successfully!")
        else:
            print("Failed to configure Grafana")
            sys.exit(1)
    else:
        dashboard_json = build_dashboard_from_config(cfg)
        if dashboard_json:
            print("Dashboard JSON generated successfully:")
            print(dashboard_json)
        else:
            print("Failed to generate dashboard JSON")

            sys.exit(1)


if __name__ == "__main__":
    # Parse our custom args before Hydra processes them
    configure_mode = parse_grafana_args()
    main._configure_mode = configure_mode

    # Print usage if no args provided

    if len(sys.argv) == 1:
        print("Usage: python grafana_config.py [hydra_overrides...] [--configure]")
        print("")
        print("Examples:")
        print("  # Generate dashboard JSON")
        print(
            "  python grafana_config.py experiment_type=cloud_demo experiment.name=test_dash"
        )
        print("")
        print("  # Configure Grafana directly")
        print(
            "  python grafana_config.py experiment_type=cloud_demo experiment.name=test_dash --configure"
        )
        print("")
        print("  # With additional overrides")
        print(
            "  python grafana_config.py experiment_type=collapsable cloudlab.num_nodes=2 experiment.name=pc_test_3 --configure"
        )
        print("")
        print("Environment variables for Grafana configuration:")
        print("  GRAFANA_HOST (default: localhost:3000)")
        print("  GRAFANA_USER (default: admin)")
        print("  GRAFANA_PASSWORD (default: admin)")
        sys.exit(1)

    main()

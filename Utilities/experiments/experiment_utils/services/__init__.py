"""
Service management package for experiments.

This package contains service classes for managing various components
of the experiment infrastructure with consistent start/stop interfaces.
"""

from .base import BaseService, DockerServiceBase
from .kafka import KafkaService
from .flink import FlinkService
from .query_engine import (
    QueryEngineService,
    QueryEngineRustService,
    QueryEngineServiceFactory,
)
from .monitoring import MonitoringService
from .fake_exporters import (
    ExporterServiceFactory,
    PythonExporterService,
    RustExporterService,
    AvalancheExporterService,
)
from .cluster_data_exporter import (
    ClusterDataExporterService,
    DataExporterFactory,
)
from .system_exporters import SystemExportersService
from .prometheus import PrometheusService
from .prometheus_kafka_adapter import PrometheusKafkaAdapterService
from .prometheus_client_service import PrometheusClientService
from .remote_monitor_service import RemoteMonitorService
from .docker_prometheus import DockerPrometheusService
from .docker_victoriametrics import DockerVictoriaMetricsService
from .arroyo import ArroyoService
from .arroyo_throughput_monitor import ArroyoThroughputMonitor
from .prometheus_throughput_monitor import PrometheusThroughputMonitor
from .prometheus_health_monitor import PrometheusHealthMonitor
from .misc import (
    DeathstarService,
    ControllerService,
    DumbKafkaConsumerService,
)
from .grafana import GrafanaService


def create_prometheus_service(cfg, provider, num_nodes: int, node_offset: int):
    """
    Create appropriate Prometheus service based on configuration.

    Args:
        cfg: Hydra configuration object
        provider: Infrastructure provider
        num_nodes: Number of nodes
        node_offset: Starting node index offset

    Returns:
        Appropriate Prometheus/VictoriaMetrics service instance

    Raises:
        ValueError: If configuration is invalid or missing
    """
    # Check for deprecated docker_resources without proper monitoring config
    if hasattr(cfg.experiment_params, "docker_resources"):
        if not hasattr(cfg.experiment_params, "monitoring"):
            raise ValueError(
                "ERROR: 'docker_resources' found but 'monitoring' section is missing. "
                "Please update your experiment config to use the new 'monitoring' section. "
                "See CONFIG_PARAMETERS_REFERENCE.md for details."
            )

    # Require explicit monitoring configuration
    if not hasattr(cfg.experiment_params, "monitoring"):
        raise ValueError(
            "ERROR: 'monitoring' section is required in experiment_type config. "
            "Please specify monitoring.tool and monitoring.deployment_mode. "
            "See CONFIG_PARAMETERS_REFERENCE.md for examples."
        )

    monitoring = cfg.experiment_params.monitoring

    # Validate required fields
    if not hasattr(monitoring, "tool"):
        raise ValueError(
            "ERROR: monitoring.tool is required (prometheus | victoriametrics)"
        )
    if not hasattr(monitoring, "deployment_mode"):
        raise ValueError(
            "ERROR: monitoring.deployment_mode is required (bare_metal | containerized)"
        )

    # Validate deployment_mode value
    if monitoring.deployment_mode not in ["bare_metal", "containerized"]:
        raise ValueError(
            f"ERROR: Invalid monitoring.deployment_mode='{monitoring.deployment_mode}'. "
            "Must be 'bare_metal' or 'containerized'"
        )

    # Validate tool value
    if monitoring.tool not in ["prometheus", "victoriametrics"]:
        raise ValueError(
            f"ERROR: Invalid monitoring.tool='{monitoring.tool}'. "
            "Must be 'prometheus' or 'victoriametrics'"
        )

    # Validate resource_limits only used with containerized mode
    if (
        hasattr(monitoring, "resource_limits")
        and monitoring.deployment_mode == "bare_metal"
    ):
        raise ValueError(
            "ERROR: monitoring.resource_limits can only be used with deployment_mode: containerized"
        )

    # Create appropriate service based on deployment mode
    if monitoring.deployment_mode == "containerized":
        if monitoring.tool == "prometheus":
            return DockerPrometheusService(provider, num_nodes, node_offset)
        elif monitoring.tool == "victoriametrics":
            return DockerVictoriaMetricsService(provider, num_nodes, node_offset)
    else:  # bare_metal
        if monitoring.tool == "victoriametrics":
            raise ValueError(
                "ERROR: VictoriaMetrics only supports containerized deployment. "
                "Use tool: prometheus for bare_metal deployment."
            )
        return PrometheusService(provider, num_nodes, node_offset)


__all__ = [
    "BaseService",
    "DockerServiceBase",
    "KafkaService",
    "FlinkService",
    "QueryEngineService",
    "QueryEngineRustService",
    "QueryEngineServiceFactory",
    "MonitoringService",
    "ExporterServiceFactory",
    "PythonExporterService",
    "RustExporterService",
    "AvalancheExporterService",
    "ClusterDataExporterService",
    "DataExporterFactory",
    "SystemExportersService",
    "PrometheusService",
    "PrometheusKafkaAdapterService",
    "PrometheusClientService",
    "RemoteMonitorService",
    "DockerPrometheusService",
    "DockerVictoriaMetricsService",
    "ArroyoService",
    "ArroyoThroughputMonitor",
    "PrometheusThroughputMonitor",
    "PrometheusHealthMonitor",
    "DeathstarService",
    "ControllerService",
    "DumbKafkaConsumerService",
    "GrafanaService",
    "create_prometheus_service",
]

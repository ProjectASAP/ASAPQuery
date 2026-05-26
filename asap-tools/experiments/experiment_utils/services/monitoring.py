"""
Monitoring service management for experiments.
"""

from .base import BaseService
from .system_exporters import SystemExportersService
from .prometheus import PrometheusService
from experiment_utils.providers.base import InfrastructureProvider


class MonitoringService(BaseService):
    """Service for managing monitoring across nodes."""

    def __init__(self, provider: InfrastructureProvider, args):
        """
        Initialize Monitoring service.

        Args:
            provider: Infrastructure provider for node communication and management
            args: Experiment args object providing get_node_range() and get_coordinator_node()
        """
        super().__init__(provider)
        self.system_exporters_service = SystemExportersService(provider, args)
        self.prometheus_service = PrometheusService(
            provider, args.num_nodes, args.node_offset
        )

    def start(self, experiment_params, experiment_output_dir: str, **kwargs) -> None:
        """
        Start monitoring service for the experiment.

        Args:
            experiment_params: Experiment configuration parameters
            experiment_output_dir: Directory for experiment output
            **kwargs: Additional configuration
        """
        # Convert experiment_params to dict if needed
        from omegaconf import OmegaConf

        if hasattr(experiment_params, "_content"):
            experiment_params_dict = OmegaConf.to_container(experiment_params)
        else:
            experiment_params_dict = experiment_params

        # Start system exporters (node_exporter, blackbox_exporter, cadvisor)
        self.system_exporters_service.start(experiment_params_dict)

        # Start Prometheus
        self.prometheus_service.start(experiment_output_dir)

    def stop(self, **kwargs) -> None:
        """
        Stop monitoring services across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        self.system_exporters_service.stop()
        self.prometheus_service.stop()

    def is_healthy(self) -> bool:
        """
        Check if monitoring services are healthy.

        Returns:
            True if monitoring is running
        """
        # Basic health check - could be enhanced to check actual monitoring processes
        return True

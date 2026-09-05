"""
Nuclear teardown script - stops ALL services and containers regardless of configuration.

This script is useful when:
- experiment_run_e2e.py was run with no_teardown=True
- experiment_run_grafana_demo.py was run and left services running
- You want to clean up everything without knowing the exact experiment configuration

It attempts to stop all possible services, ignoring errors if services aren't running.
"""

import hydra
from omegaconf import DictConfig, OmegaConf

import constants
from experiment_utils import config
from experiment_utils.services import (
    QueryEngineRustService,
    ExporterServiceFactory,
    DeathstarService,
    ControllerService,
    PrometheusClientService,
    RemoteMonitorService,
    AvalancheExporterService,
    create_prometheus_service,
    SystemExportersService,
    GrafanaService,
)

# Register custom resolver for LOCAL_EXPERIMENT_DIR before Hydra processes config
OmegaConf.register_new_resolver(
    "local_experiment_dir", lambda: constants.LOCAL_EXPERIMENT_DIR
)


@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg: DictConfig):
    """
    Nuclear teardown - stops all services regardless of experiment configuration.

    Usage:
        python experiment_teardown_everything.py experiment_type=<type> experiment_name=<name>

    The experiment_type and experiment_name are only used to initialize the provider.
    All services will be stopped regardless of what was actually running.
    """
    # Validate configuration (minimal validation for provider setup)
    config.validate_config(cfg)
    args = config.Args(cfg)
    provider = args.provider

    num_nodes_in_experiment = args.num_nodes

    print(f"Provider: {type(provider).__name__}")
    print(f"Nodes: {num_nodes_in_experiment}")

    query_engine_service_container = QueryEngineRustService(
        provider, use_container=True, node_offset=args.node_offset
    )
    query_engine_service_native = QueryEngineRustService(
        provider, use_container=False, node_offset=args.node_offset
    )

    system_exporters_service = SystemExportersService(provider, args)
    prometheus_service = create_prometheus_service(
        cfg, provider, num_nodes_in_experiment, args.node_offset
    )

    deathstar_service = DeathstarService(provider, args)

    controller_service_container = ControllerService(
        provider, use_container=True, node_offset=args.node_offset
    )
    controller_service_native = ControllerService(
        provider, use_container=False, node_offset=args.node_offset
    )

    prometheus_client_service_container = PrometheusClientService(
        provider, use_container=True, node_offset=args.node_offset
    )
    prometheus_client_service_native = PrometheusClientService(
        provider, use_container=False, node_offset=args.node_offset
    )

    remote_monitor_service = RemoteMonitorService(provider, args.node_offset)

    grafana_service = GrafanaService(
        provider, num_nodes_in_experiment, args.node_offset
    )

    avalanche_service = AvalancheExporterService(provider, args, use_container=False)

    # Initialize both exporter languages
    fake_exporter_service_rust = ExporterServiceFactory.create_exporter_service(
        "rust", provider, args, use_container=True
    )
    fake_exporter_service_python = ExporterServiceFactory.create_exporter_service(
        "python", provider, args, use_container=True
    )
    fake_exporter_service_rust_native = ExporterServiceFactory.create_exporter_service(
        "rust", provider, args, use_container=False
    )
    fake_exporter_service_python_native = (
        ExporterServiceFactory.create_exporter_service(
            "python", provider, args, use_container=False
        )
    )

    services_to_stop = [
        ("Prometheus Client (container)", prometheus_client_service_container),
        ("Prometheus Client (native)", prometheus_client_service_native),
        ("Remote Monitor", remote_monitor_service),
        ("Query Engine (container)", query_engine_service_container),
        ("Query Engine (native)", query_engine_service_native),
        ("System Exporters", system_exporters_service),
        ("Prometheus", prometheus_service),
        ("Fake Exporter Rust (container)", fake_exporter_service_rust),
        ("Fake Exporter Python (container)", fake_exporter_service_python),
        ("Fake Exporter Rust (native)", fake_exporter_service_rust_native),
        ("Fake Exporter Python (native)", fake_exporter_service_python_native),
        ("Avalanche", avalanche_service),
        ("Deathstar", deathstar_service),
        ("Controller (container)", controller_service_container),
        ("Controller (native)", controller_service_native),
        ("Grafana", grafana_service),
    ]

    for service_name, service in services_to_stop:
        try:
            print(f"Stopping {service_name}...", end=" ")
            service.stop()
        except Exception as e:
            print(f"Error in stopping {service_name}: {e}")

    # Reset Prometheus
    print("Resetting Prometheus")
    try:
        prometheus_service.reset()
    except Exception as e:
        print(f"Error in resetting Prometheus: {e}")
    print("Teardown complete.")


if __name__ == "__main__":
    main()  # type: ignore

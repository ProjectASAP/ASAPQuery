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
from experiment_utils.providers.factory import create_provider
from experiment_utils.services import (
    KafkaService,
    FlinkService,
    QueryEngineServiceFactory,
    ExporterServiceFactory,
    PrometheusKafkaAdapterService,
    ArroyoService,
    DeathstarService,
    ControllerService,
    DumbKafkaConsumerService,
    PrometheusClientService,
    RemoteMonitorService,
    AvalancheExporterService,
    create_prometheus_service,
    SystemExportersService,
    GrafanaService,
)

KAFKA_NUM_TRIES = 5

# Register custom resolver for LOCAL_EXPERIMENT_DIR before Hydra processes config
OmegaConf.register_new_resolver(
    "local_experiment_dir", lambda: constants.LOCAL_EXPERIMENT_DIR
)

# Register custom resolver for remote write IP based on node_offset
OmegaConf.register_new_resolver(
    "remote_write_ip", lambda node_offset: f"10.10.1.{node_offset + 1}"
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

    # Create infrastructure provider
    provider = create_provider(cfg)

    num_nodes_in_experiment = args.num_nodes

    print(f"Provider: {type(provider).__name__}")
    print(f"Nodes: {num_nodes_in_experiment}")

    kafka_service = KafkaService(provider, args.node_offset, num_tries=KAFKA_NUM_TRIES)
    flink_service = FlinkService(provider, args.node_offset)

    # Initialize both query engine languages
    query_engine_service_rust = QueryEngineServiceFactory.create_query_engine_service(
        "rust", provider, use_container=True, node_offset=args.node_offset
    )
    query_engine_service_python = QueryEngineServiceFactory.create_query_engine_service(
        "python", provider, use_container=True, node_offset=args.node_offset
    )
    query_engine_service_rust_native = (
        QueryEngineServiceFactory.create_query_engine_service(
            "rust", provider, use_container=False, node_offset=args.node_offset
        )
    )
    query_engine_service_python_native = (
        QueryEngineServiceFactory.create_query_engine_service(
            "python", provider, use_container=False, node_offset=args.node_offset
        )
    )

    system_exporters_service = SystemExportersService(
        provider, num_nodes_in_experiment, args.node_offset
    )
    prometheus_service = create_prometheus_service(
        cfg, provider, num_nodes_in_experiment, args.node_offset
    )
    prometheus_kafka_adapter_service = PrometheusKafkaAdapterService(
        provider, args.node_offset
    )

    arroyo_service_container = ArroyoService(
        provider, use_container=True, node_offset=args.node_offset
    )
    arroyo_service_native = ArroyoService(
        provider, use_container=False, node_offset=args.node_offset
    )

    deathstar_service = DeathstarService(
        provider, num_nodes_in_experiment, args.node_offset
    )

    controller_service_container = ControllerService(
        provider, use_container=True, node_offset=args.node_offset
    )
    controller_service_native = ControllerService(
        provider, use_container=False, node_offset=args.node_offset
    )

    dumb_consumer_service = DumbKafkaConsumerService(provider, args.node_offset)

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

    avalanche_service = AvalancheExporterService(
        provider,
        num_nodes_in_experiment,
        use_container=False,
        node_offset=args.node_offset,
    )

    # Initialize both exporter languages
    fake_exporter_service_rust = ExporterServiceFactory.create_exporter_service(
        "rust",
        provider,
        num_nodes_in_experiment,
        use_container=True,
        node_offset=args.node_offset,
    )
    fake_exporter_service_python = ExporterServiceFactory.create_exporter_service(
        "python",
        provider,
        num_nodes_in_experiment,
        use_container=True,
        node_offset=args.node_offset,
    )
    fake_exporter_service_rust_native = ExporterServiceFactory.create_exporter_service(
        "rust",
        provider,
        num_nodes_in_experiment,
        use_container=False,
        node_offset=args.node_offset,
    )
    fake_exporter_service_python_native = (
        ExporterServiceFactory.create_exporter_service(
            "python",
            provider,
            num_nodes_in_experiment,
            use_container=False,
            node_offset=args.node_offset,
        )
    )

    services_to_stop = [
        ("Prometheus Client (container)", prometheus_client_service_container),
        ("Prometheus Client (native)", prometheus_client_service_native),
        ("Remote Monitor", remote_monitor_service),
        ("Query Engine Rust (container)", query_engine_service_rust),
        ("Query Engine Python (container)", query_engine_service_python),
        ("Query Engine Rust (native)", query_engine_service_rust_native),
        ("Query Engine Python (native)", query_engine_service_python_native),
        ("Kafka", kafka_service),
        ("Prometheus-Kafka Adapter", prometheus_kafka_adapter_service),
        ("System Exporters", system_exporters_service),
        ("Prometheus", prometheus_service),
        ("Fake Exporter Rust (container)", fake_exporter_service_rust),
        ("Fake Exporter Python (container)", fake_exporter_service_python),
        ("Fake Exporter Rust (native)", fake_exporter_service_rust_native),
        ("Fake Exporter Python (native)", fake_exporter_service_python_native),
        ("Avalanche", avalanche_service),
        ("Deathstar", deathstar_service),
        ("Dumb Consumer", dumb_consumer_service),
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

    # Stop all Flink jobs
    print("Stopping all Flink jobs")
    try:
        flink_service.stop_all_jobs()
    except Exception as e:
        print(f"Error in stopping Flink jobs: {e}")

    # Stop all Arroyo jobs (both container and native)
    print("Stopping all Arroyo jobs (container)")
    try:
        arroyo_service_container.stop_all_jobs()
    except Exception as e:
        print(f"Error in stopping Arroyo jobs (container): {e}")

    print("Stopping all Arroyo jobs (native)")
    try:
        arroyo_service_native.stop_all_jobs()
    except Exception as e:
        print(f"Error in stopping Arroyo jobs (native): {e}")

    # Stop all Java processes (for local Flink)
    print("Stopping all Flink Java processes")
    try:
        flink_service.stop_all_java_processes()
    except Exception as e:
        print(f"Error in stopping Flink Java processes: {e}")

    # Delete Kafka topics
    print("Deleting Kafka topics")
    try:
        kafka_service.delete_topics()
    except Exception as e:
        print(f"Error in deleting Kafka topics: {e}")

    # Stop Arroyo services
    print("Stopping Arroyo service (container)")
    try:
        arroyo_service_container.stop()
    except Exception as e:
        print(f"Error in stopping Arroyo service (container): {e}")

    print("Stopping Arroyo service (native)")
    try:
        arroyo_service_native.stop()
    except Exception as e:
        print(f"Error in stopping Arroyo service (native): {e}")

    # Stop Flink service
    print("Stopping Flink service")
    try:
        flink_service.stop()
    except Exception as e:
        print(f"Error in stopping Flink service: {e}")

    # Reset Prometheus
    print("Resetting Prometheus")
    try:
        prometheus_service.reset()
    except Exception as e:
        print(f"Error in resetting Prometheus: {e}")
    print("Teardown complete.")


if __name__ == "__main__":
    main()  # type: ignore

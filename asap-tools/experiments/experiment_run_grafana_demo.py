import os
import json
import time
import sys
import hydra
from omegaconf import DictConfig, OmegaConf

import constants
import experiment_utils
from experiment_utils import sync, config
from experiment_utils.services import (
    QueryEngineRustService,
    resolve_backend_config,
    ExporterServiceFactory,
    PrometheusThroughputMonitor,
    DeathstarService,
    ControllerService,
    RemoteMonitorService,
    AvalancheExporterService,
    create_prometheus_service,
    PrometheusService,
    DockerPrometheusService,
    DockerVictoriaMetricsService,
    SystemExportersService,
    GrafanaService,
)
from experiment_utils.services.misc import DiscoveryBackend

GRAFANA_ADMIN_PASSWORD = "admin"

CONTROLLER_LOCAL_OUTPUT_DIR = None
CONTROLLER_REMOTE_OUTPUT_DIR = None

REMOTE_PROCESS_POLLING_INTERVAL = 10

# Register custom resolver for LOCAL_EXPERIMENT_DIR before Hydra processes config
OmegaConf.register_new_resolver(
    "local_experiment_dir", lambda: constants.LOCAL_EXPERIMENT_DIR
)


@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg: DictConfig):
    # Validate configuration
    config.validate_config(cfg)
    # Validate experiment configuration
    config.validate_experiment_config(cfg.experiment_params)
    # Convert config to args-like object for backward compatibility
    # (also constructs the infrastructure provider, exposed as args.provider)
    args = config.Args(cfg)
    provider = args.provider

    args.forward_unsupported_queries = True
    print("Forcing forward_unsupported_queries to True for Grafana demo")

    local_experiment_root_dir = os.path.join(
        constants.LOCAL_EXPERIMENT_DIR, args.experiment_name
    )
    os.makedirs(local_experiment_root_dir, exist_ok=True)

    # dump config to a file
    with open(os.path.join(local_experiment_root_dir, "hydra_config.yaml"), "w") as f:
        OmegaConf.save(cfg, f)

    # Also dump args to a file for backward compatibility
    with open(os.path.join(local_experiment_root_dir, "cmdline_args.txt"), "w") as f:
        json.dump(args.to_dict(), f)

    experiment_root_output_dir = (
        f"{constants.CLOUDLAB_HOME_DIR}/experiment_outputs/{args.experiment_name}"
    )

    global CONTROLLER_REMOTE_OUTPUT_DIR, CONTROLLER_LOCAL_OUTPUT_DIR
    CONTROLLER_LOCAL_OUTPUT_DIR = os.path.join(
        local_experiment_root_dir, "controller_output"
    )
    CONTROLLER_REMOTE_OUTPUT_DIR = os.path.join(
        experiment_root_output_dir, "controller_output"
    )

    provider.execute_command(
        node_idx=args.get_coordinator_node(),
        cmd="mkdir -p {} {}".format(
            os.path.dirname(constants.CLOUDLAB_QUERY_LOG_FILE),
            experiment_root_output_dir,
        ),
        cmd_dir="",
        nohup=False,
        popen=False,
    )

    num_nodes_in_experiment = args.num_nodes

    workloads_config = config.read_workloads_config(cfg.experiment_params)
    if workloads_config is None:
        print("-" * 40)
        print("WARN: No workloads specified in the experiment configuration")
        print("-" * 40)

    exporter_config, rejection_reason = experiment_utils.read_exporter_config(
        cfg.experiment_params
    )
    if exporter_config is None:
        raise ValueError("Invalid exporter config: {}".format(rejection_reason))

    prometheus_throughput_monitor = None

    # Initialize services
    query_engine_service = QueryEngineRustService(
        provider,
        use_container=args.use_container_query_engine,
        node_offset=args.node_offset,
    )
    system_exporters_service = SystemExportersService(provider, args)
    prometheus_service = create_prometheus_service(
        cfg, provider, args.num_nodes, args.node_offset
    )
    deathstar_service = DeathstarService(provider, args)
    controller_service = ControllerService(
        provider,
        use_container=args.use_container_controller,
        node_offset=args.node_offset,
    )
    # TODO: QueryLatencyExporter is part of PrometheusClientService. How do we export latencies if we don't use PrometheusClientService?
    # prometheus_client_service = PrometheusClientService(
    #     args.cloudlab_username,
    #     args.hostname_suffix,
    #     use_container=args.use_container_prometheus_client,
    # )
    remote_monitor_service = RemoteMonitorService(provider, args.node_offset)
    grafana_service = GrafanaService(
        provider, num_nodes_in_experiment, args.node_offset
    )
    avalanche_service = AvalancheExporterService(provider, args, use_container=False)

    # Initialize exporter service based on language
    exporter_service = ExporterServiceFactory.create_exporter_service(
        args.fake_exporter_language,
        provider,
        args,
        use_container=args.use_container_fake_exporter,
    )

    sync.copy_experiment_config(cfg.experiment_params, local_experiment_root_dir)
    experiment_modes, metrics_to_remote_write = (
        config.generate_controller_client_configs(
            cfg.experiment_params,
            local_experiment_root_dir,
            cfg.aggregate_cleanup,
            cfg.get("sketch_parameters", None),
            cfg.get("windowing", None),
        )
    )
    sync.rsync_controller_client_configs(
        provider,
        experiment_root_output_dir,
        local_experiment_root_dir,
        node_offset=args.node_offset,
    )
    minimum_experiment_running_time = config.get_minimum_experiment_running_time(
        cfg.experiment_params
    )

    # Fixed to sketchdb mode for Grafana demo
    experiment_mode = constants.SKETCHDB_EXPERIMENT_NAME
    print(f"Running fixed experiment mode for Grafana demo: {experiment_mode}")
    experiment_output_dir = os.path.join(
        experiment_root_output_dir,
        experiment_mode,
    )
    local_experiment_dir = os.path.join(local_experiment_root_dir, experiment_mode)
    provider.execute_command_parallel(
        node_idxs=args.get_node_range(include_coordinator=True),
        cmd=f"mkdir -p {experiment_output_dir}",
        cmd_dir="",
        nohup=False,
        popen=True,
        wait=True,
    )

    controller_input_config = os.path.join(
        experiment_root_output_dir,
        "controller_client_configs",
        f"{experiment_mode}_controller_input.yaml",
    )

    if args.streaming_engine != "precompute":
        raise NotImplementedError(
            "Only streaming_engine=precompute is supported for Grafana demo"
        )

    # prometheus_client_service.stop()
    remote_monitor_service.stop()
    query_engine_service.stop()
    system_exporters_service.stop()
    prometheus_service.stop()
    exporter_service.stop()
    deathstar_service.stop()
    prometheus_service.reset()

    # Also stop avalanche exporters if they were started
    if config.check_exporter_and_queries_exist("avalanche", cfg.experiment_params):
        avalanche_service.stop()

    prometheus_config_output_dir = os.path.join(
        local_experiment_dir, constants.PROMETHEUS_CONFIG_DIR
    )
    os.makedirs(prometheus_config_output_dir, exist_ok=True)

    config.generate_and_copy_prometheus_config(
        num_nodes_in_experiment,
        local_experiment_dir,
        prometheus_config_output_dir,
        experiment_mode,
        cfg,
        cfg.prometheus,
        args.node_offset,
        constants.SKETCHDB_EXPERIMENT_NAME,
        provider,
    )
    sync.rsync_prometheus_config(
        provider,
        experiment_output_dir,
        prometheus_config_output_dir,
        node_offset=args.node_offset,
    )
    data_ingestion_interval_ms = config.get_prometheus_data_ingestion_interval_ms(
        cfg.prometheus
    )

    if config.check_exporter_and_queries_exist("fake_exporter", cfg.experiment_params):
        # this DOES NOT block
        exporter_service.start(
            config=exporter_config["exporter_list"]["fake_exporter"],
            experiment_output_dir=experiment_output_dir,
            local_experiment_dir=local_experiment_dir,
        )

    # Handle avalanche exporter for vertical scalability testing
    if config.check_exporter_and_queries_exist("avalanche", cfg.experiment_params):
        avalanche_service.start(
            config=exporter_config["exporter_list"]["avalanche"],
            experiment_output_dir=experiment_output_dir,
            local_experiment_dir=local_experiment_dir,
        )

    if (
        workloads_config is not None
        and "deathstar" in workloads_config
        and workloads_config["deathstar"] is not None
        and workloads_config["deathstar"]["use"] is True
    ):
        deathstar_service.start()

    # Start system exporters (node_exporter, blackbox_exporter, cadvisor)
    system_exporters_service.start(cfg.experiment_params)

    # Start Prometheus service based on deployment mode
    monitoring = cfg.experiment_params.monitoring

    if monitoring.deployment_mode == "containerized":
        # Containerized deployment (DockerPrometheusService or DockerVictoriaMetricsService)
        assert isinstance(
            prometheus_service, (DockerPrometheusService, DockerVictoriaMetricsService)
        ), f"Expected Docker-based service but got {type(prometheus_service).__name__}"

        # Check if resource limits are specified
        if hasattr(monitoring, "resource_limits"):
            prometheus_service.start(
                experiment_output_dir=experiment_output_dir,
                local_experiment_dir=local_experiment_dir,
                experiment_mode=experiment_mode,
                cpu_limit=monitoring.resource_limits.cpu_limit,
                memory_limit=monitoring.resource_limits.memory_limit,
            )
        else:
            # Containerized without resource limits
            prometheus_service.start(
                experiment_output_dir=experiment_output_dir,
                local_experiment_dir=local_experiment_dir,
                experiment_mode=experiment_mode,
            )
    else:  # bare_metal
        # Bare-metal deployment (PrometheusService)
        assert isinstance(
            prometheus_service, PrometheusService
        ), f"Expected PrometheusService but got {type(prometheus_service).__name__}"
        prometheus_service.start(experiment_output_dir)

    if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
        prometheus_url = (
            f"http://localhost:{prometheus_service.get_query_endpoint_port()}"
        )

        prometheus_service.wait_until_ready()

        label_discovery_wait_ms = data_ingestion_interval_ms * 2
        print(
            f"Waiting {label_discovery_wait_ms / 1000}s for Prometheus to scrape initial data "
            f"before running controller label inference..."
        )
        time.sleep(label_discovery_wait_ms / 1000)

        controller_service.start(
            controller_input_file=controller_input_config,
            data_ingestion_interval_ms=data_ingestion_interval_ms,
            streaming_engine=args.streaming_engine,
            controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
            punting=args.controller_punting,
            discovery_backend=DiscoveryBackend(
                type="prometheus",
                url=prometheus_url,
                database=None,
            ),
        )
        sync.rsync_controller_config_remote_to_local(
            provider,
            CONTROLLER_REMOTE_OUTPUT_DIR,
            CONTROLLER_LOCAL_OUTPUT_DIR,
            node_offset=args.node_offset,
        )
    if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
        if args.streaming_engine != "precompute":
            raise ValueError(
                "Invalid streaming engine: {}. Only 'precompute' is supported.".format(
                    args.streaming_engine
                )
            )

        # Start Prometheus throughput monitoring if enabled
        if args.throughput_prometheus:
            prometheus_throughput_monitor = PrometheusThroughputMonitor(
                provider,
                node_offset=args.node_offset,
            )
            prometheus_throughput_monitor.start(
                experiment_output_dir=experiment_output_dir
            )
        # Get http port from query engine service
        http_port = query_engine_service.get_http_port()

        # forward_unsupported_queries is forced True for the Grafana demo (line 63)
        backend_config = resolve_backend_config(
            args.backend,
            prometheus_service,
            provider,
            args.node_offset,
            args.forward_unsupported_queries,
        )

        query_engine_service.start(
            experiment_output_dir=experiment_output_dir,
            local_experiment_dir=local_experiment_dir,
            data_ingestion_interval_ms=data_ingestion_interval_ms,
            log_level=args.log_level,
            profile_query_engine=args.profile_query_engine,
            manual=args.manual_query_engine,
            streaming_engine=args.streaming_engine,
            controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
            lock_strategy=args.lock_strategy,
            backend_config=backend_config,
            http_port=http_port,
            remote_write_port=args.remote_write_base_port,
        )

    # this DOES NOT block
    if (
        workloads_config is not None
        and "deathstar" in workloads_config
        and workloads_config["deathstar"] is not None
        and workloads_config["deathstar"]["use"] is True
    ):
        deathstar_service.run_workload(
            experiment_output_dir=experiment_output_dir,
            local_experiment_dir=local_experiment_dir,
            minimum_experiment_running_time=minimum_experiment_running_time,
            random_params=False,
        )

    time.sleep(args.steady_state_wait)

    # Start and configure Grafana
    print("Starting Grafana service...")
    grafana_service.start(admin_password=GRAFANA_ADMIN_PASSWORD)
    grafana_service._wait_for_service_ready()

    print("Configuring Grafana datasources and dashboard...")
    # Get experiment_type from Hydra overrides (it's not in the final config)
    from hydra.core.hydra_config import HydraConfig

    hydra_cfg = HydraConfig.get()

    experiment_type = None
    for override in hydra_cfg.overrides.task:
        if override.startswith("experiment_type="):
            experiment_type = override.split("=")[1]
            break

    if experiment_type is None:
        raise ValueError(
            "experiment_type parameter is required but not found in command line overrides"
        )

    success = grafana_service.configure_dashboard(experiment_type, args.experiment_name)
    if not success:
        print("ERROR: Failed to configure Grafana")
        sys.exit(1)

    print(
        f"✓ Grafana dashboard available at: {grafana_service.get_dashboard_url(args.experiment_name)}"
    )


if __name__ == "__main__":
    main()

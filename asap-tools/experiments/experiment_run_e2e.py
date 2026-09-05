import os
import json
import time

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
    PrometheusHealthMonitor,
    DeathstarService,
    ControllerService,
    PrometheusClientService,
    RemoteMonitorService,
    AvalancheExporterService,
    DataExporterFactory,
    create_prometheus_service,
    PrometheusService,
    DockerPrometheusService,
    DockerVictoriaMetricsService,
    SystemExportersService,
)
from experiment_utils.services.misc import DiscoveryBackend

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

    if provider.is_remote():
        local_experiment_root_dir = os.path.join(
            constants.LOCAL_EXPERIMENT_DIR, args.experiment_name
        )
        experiment_root_output_dir = os.path.join(
            provider.get_home_dir(), "experiment_outputs", args.experiment_name
        )
    else:
        # Local mode: the "remote" and "local" roots are the same filesystem
        # path by construction, so every rsync/scp call downstream becomes a
        # no-op rather than something to opportunistically skip.
        local_experiment_root_dir = os.path.join(
            provider.get_home_dir(), "experiment_outputs", args.experiment_name
        )
        experiment_root_output_dir = local_experiment_root_dir
    os.makedirs(local_experiment_root_dir, exist_ok=True)

    # dump config to a file
    with open(os.path.join(local_experiment_root_dir, "hydra_config.yaml"), "w") as f:
        OmegaConf.save(cfg, f)

    # Also dump args to a file for backward compatibility
    with open(os.path.join(local_experiment_root_dir, "cmdline_args.txt"), "w") as f:
        json.dump(args.to_dict(), f)

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
            os.path.dirname(provider.get_query_log_file()),
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

    skip_querying = cfg.experiment_params.get("skip_querying", False)
    if skip_querying:
        print("-" * 40)
        print("Skip querying mode ENABLED")
        print(
            f"Experiment will run for {cfg.experiment_params.experiment_duration} seconds without queries"
        )
        print("-" * 40)

    exporter_config, rejection_reason = experiment_utils.read_exporter_config(
        cfg.experiment_params
    )
    if exporter_config is None:
        raise ValueError("Invalid exporter config: {}".format(rejection_reason))

    prometheus_throughput_monitor = None
    prometheus_health_monitor = None

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
    prometheus_client_service = PrometheusClientService(
        provider,
        use_container=args.use_container_prometheus_client,
        node_offset=args.node_offset,
    )
    remote_monitor_service = RemoteMonitorService(provider, args.node_offset)
    avalanche_service = AvalancheExporterService(provider, args, use_container=False)

    # Initialize exporter service based on language
    exporter_service = ExporterServiceFactory.create_exporter_service(
        args.fake_exporter_language,
        provider,
        args,
        use_container=args.use_container_fake_exporter,
    )

    # Initialize cluster data exporter service if configured
    cluster_data_service = None
    if exporter_config and "cluster_data_exporter" in exporter_config.get(
        "exporter_list", {}
    ):
        cluster_data_directory = cfg.get(
            "cluster_data_directory", "/data/cluster_traces"
        )
        cluster_data_service = DataExporterFactory.create_data_exporter_service(
            "cluster_data",
            provider,
            node_offset=args.node_offset,
            data_directory=cluster_data_directory,
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

    for experiment_mode in experiment_modes:
        print(f"Running experiment mode: {experiment_mode}")
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

        controller_client_config = os.path.join(
            experiment_root_output_dir,
            "controller_client_configs",
            f"{experiment_mode}.yaml",
        )
        # Stripped to the fields ControllerConfig accepts (deny_unknown_fields).
        # The full config above is still used by the prometheus_client.
        controller_input_config = os.path.join(
            experiment_root_output_dir,
            "controller_client_configs",
            f"{experiment_mode}_controller_input.yaml",
        )

        prometheus_client_service.stop()
        remote_monitor_service.stop(
            execution_mode="timed" if skip_querying else "prometheus_client",
            experiment_output_dir=experiment_output_dir,
        )
        query_engine_service.stop()
        system_exporters_service.stop()
        prometheus_service.stop()
        exporter_service.stop()
        deathstar_service.stop()
        prometheus_service.reset()

        # Also stop avalanche exporters if they were started
        if config.check_exporter_and_queries_exist("avalanche", cfg.experiment_params):
            avalanche_service.stop()

        # Also stop cluster data exporter if it was started
        if cluster_data_service and config.check_exporter_and_queries_exist(
            "cluster_data_exporter", cfg.experiment_params
        ):
            cluster_data_service.stop()

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

        # copy_controller_client_config(args.controller_client_config, local_experiment_dir)
        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            if config.check_exporter_and_queries_exist(
                "fake_exporter", cfg.experiment_params
            ):
                exporter_service.start(
                    config=exporter_config["exporter_list"]["fake_exporter"],
                    experiment_output_dir=experiment_output_dir,
                    local_experiment_dir=local_experiment_dir,
                )
            if cluster_data_service and config.check_exporter_and_queries_exist(
                "cluster_data_exporter", cfg.experiment_params
            ):
                cluster_data_service.start(
                    config=exporter_config["exporter_list"]["cluster_data_exporter"],
                    experiment_output_dir=experiment_output_dir,
                    local_experiment_dir=local_experiment_dir,
                    num_nodes=num_nodes_in_experiment,
                )
            prometheus_service.start(
                experiment_output_dir=experiment_output_dir,
                local_experiment_dir=local_experiment_dir,
                experiment_mode=experiment_mode,
            )
            # Poll until Prometheus is actually accepting connections before sleeping
            # for scrape data. Prometheus takes a few seconds to bind the port after
            # its process starts, so a fixed sleep alone can race.
            prometheus_service.wait_until_ready()

            # Config files already contain metric label hints. Only wait for
            # scrape data when runtime label discovery has been explicitly
            # enabled; otherwise the planner uses those hints directly.
            discovery_backend = None
            if args.controller_auto_discover_labels:
                label_discovery_wait_ms = data_ingestion_interval_ms * 2
                print(
                    f"Waiting {label_discovery_wait_ms / 1000}s for Prometheus to scrape initial data "
                    f"before running controller label inference..."
                )
                time.sleep(label_discovery_wait_ms / 1000)
                discovery_backend = DiscoveryBackend(
                    type="prometheus",
                    url=f"http://localhost:{prometheus_service.get_query_endpoint_port()}",
                    database=None,
                )
            else:
                print(
                    "Using metric labels from the controller config; "
                    "Prometheus label discovery is disabled."
                )

            controller_service.start(
                controller_input_file=controller_input_config,
                streaming_engine=args.streaming_engine,
                controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
                punting=args.controller_punting,
                discovery_backend=discovery_backend,
                data_ingestion_interval_ms=data_ingestion_interval_ms,
            )
            sync.rsync_controller_config_remote_to_local(
                provider,
                CONTROLLER_REMOTE_OUTPUT_DIR,
                CONTROLLER_LOCAL_OUTPUT_DIR,
                node_offset=args.node_offset,
            )

        if (
            config.check_exporter_and_queries_exist(
                "fake_exporter", cfg.experiment_params
            )
            and experiment_mode != constants.SKETCHDB_EXPERIMENT_NAME
        ):
            # this DOES NOT block
            # (SKETCHDB mode already started the exporter early for label discovery)
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

        # Handle cluster data exporter for replaying cluster traces
        if (
            cluster_data_service
            and experiment_mode != constants.SKETCHDB_EXPERIMENT_NAME
            and config.check_exporter_and_queries_exist(
                "cluster_data_exporter", cfg.experiment_params
            )
        ):
            cluster_data_service.start(
                config=exporter_config["exporter_list"]["cluster_data_exporter"],
                experiment_output_dir=experiment_output_dir,
                local_experiment_dir=local_experiment_dir,
                num_nodes=num_nodes_in_experiment,
            )

        if (
            workloads_config is not None
            and "deathstar" in workloads_config
            and workloads_config["deathstar"] is not None
            and workloads_config["deathstar"]["use"] is True
        ):
            deathstar_service.start()

        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            if args.streaming_engine != "precompute":
                raise ValueError(
                    "Invalid streaming engine: {}. Only 'precompute' is supported.".format(
                        args.streaming_engine
                    )
                )

            http_port = query_engine_service.get_http_port()

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
            # For precompute mode the query engine IS the Prometheus remote-write
            # target (port remote_write_base_port). Prometheus is already running
            # and retrying writes against that port, so block until the query
            # engine's HTTP server is accepting connections before proceeding.
            if args.streaming_engine == "precompute":
                query_engine_service.wait_until_ready()

        # Start system exporters (node_exporter, blackbox_exporter, cadvisor)
        system_exporters_service.start(cfg.experiment_params)

        # Start Prometheus service based on deployment mode
        # (SKETCHDB mode already started Prometheus early for label discovery)
        monitoring = cfg.experiment_params.monitoring

        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            pass  # already started before the controller
        elif monitoring.deployment_mode == "containerized":
            # Containerized deployment (DockerPrometheusService or DockerVictoriaMetricsService)
            assert isinstance(
                prometheus_service,
                (DockerPrometheusService, DockerVictoriaMetricsService),
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

        # Start Prometheus throughput monitoring if enabled
        if args.throughput_prometheus:
            prometheus_throughput_monitor = PrometheusThroughputMonitor(
                provider,
                node_offset=args.node_offset,
            )
            prometheus_throughput_monitor.start(
                experiment_output_dir=experiment_output_dir
            )

        # Start Prometheus health check monitoring if enabled
        if args.health_check_prometheus:
            prometheus_health_monitor = PrometheusHealthMonitor(
                provider,
                node_offset=args.node_offset,
            )
            prometheus_health_monitor.start(experiment_output_dir=experiment_output_dir)

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

        if not skip_querying:
            time.sleep(args.steady_state_wait)
        else:
            print("Skipping steady_state_wait in skip_querying mode")

        # TODO: rename this function and remote_monitor.py
        # run_remote_monitor(
        remote_monitor_service.start(
            controller_client_config,
            experiment_output_dir,
            experiment_mode,
            args.profile_query_engine,
            args.profile_prometheus_time,
            args.manual_remote_monitor,
            args.streaming_engine,
            query_engine_service,
            controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
            use_container_prometheus_client=args.use_container_prometheus_client,
            prometheus_client_parallel=args.prometheus_client_parallel,
            backend_protocol="prometheus",
            pre_query_wait_seconds=0,
            monitor_interval_seconds=float(cfg.flow.monitor_interval_seconds),
            backend_tool=cfg.experiment_params.monitoring.tool,
            timed_duration=minimum_experiment_running_time if skip_querying else None,
        )

        if not args.manual_remote_monitor and constants.AVOID_REMOTE_MONITOR_LONG_SSH:
            # we need to wait here and keep checking if the remote monitor has finished
            remote_monitor_service.wait_for_remote_monitor_to_finish(
                minimum_experiment_running_time=minimum_experiment_running_time,
                polling_interval=REMOTE_PROCESS_POLLING_INTERVAL,
                execution_mode="timed" if skip_querying else "prometheus_client",
                experiment_output_dir=experiment_output_dir,
            )

        # Containerized Prometheus service mounts a volume on the remote experiment directory
        # Bare-metal Prometheus stores data locally, so we need to copy it back
        if (
            cfg.experiment_params.monitoring.deployment_mode == "bare_metal"
            and not cfg.flow.get("skip_copy_prometheus_data", False)
        ):
            sync.copy_prometheus_data(provider, local_experiment_dir, args.node_offset)

        # Skip teardown if the no_teardown flag is set
        if not args.no_teardown:
            if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
                query_engine_service.stop()

                # Stop Prometheus throughput monitoring if it was started
                if args.throughput_prometheus:
                    if prometheus_throughput_monitor is None:
                        raise RuntimeError(
                            "Prometheus throughput monitoring was enabled but monitor is None"
                        )
                    prometheus_throughput_monitor.stop()

                # Stop Prometheus health check monitoring if it was started
                if args.health_check_prometheus:
                    if prometheus_health_monitor is None:
                        raise RuntimeError(
                            "Prometheus health check monitoring was enabled but monitor is None"
                        )
                    prometheus_health_monitor.stop()

            system_exporters_service.stop()
            prometheus_service.stop()
            controller_service.stop()  # only does something if controller is containerized
            exporter_service.stop()
            deathstar_service.stop()
            prometheus_service.reset()

            # Also stop avalanche exporters if they were started
            if config.check_exporter_and_queries_exist(
                "avalanche", cfg.experiment_params
            ):
                avalanche_service.stop()

            # Also stop cluster data exporter if it was started
            if cluster_data_service and config.check_exporter_and_queries_exist(
                "cluster_data_exporter", cfg.experiment_params
            ):
                cluster_data_service.stop()

        sync.rsync_experiment_data(
            provider,
            experiment_output_dir,
            local_experiment_dir,
            node_offset=args.node_offset,
        )


if __name__ == "__main__":
    main()

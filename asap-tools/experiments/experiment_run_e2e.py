import os
import json
import time

import hydra
from omegaconf import DictConfig, OmegaConf

import constants
import experiment_utils
from experiment_utils import sync, config
from experiment_utils.providers.factory import create_provider
from experiment_utils.services import (
    KafkaService,
    FlinkService,
    QueryEngineServiceFactory,
    ExporterServiceFactory,
    PrometheusKafkaAdapterService,
    ArroyoService,
    ArroyoThroughputMonitor,
    PrometheusThroughputMonitor,
    PrometheusHealthMonitor,
    DeathstarService,
    ControllerService,
    DumbKafkaConsumerService,
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

COMPRESS_JSON = True

CONTROLLER_LOCAL_OUTPUT_DIR = None
CONTROLLER_REMOTE_OUTPUT_DIR = None

REMOTE_PROCESS_POLLING_INTERVAL = 10
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
    # Validate configuration
    config.validate_config(cfg)
    # Validate experiment configuration
    config.validate_experiment_config(cfg.experiment_params)
    # Convert config to args-like object for backward compatibility
    args = config.Args(cfg)

    # Create infrastructure provider
    provider = create_provider(cfg)

    local_experiment_root_dir = os.path.join(
        constants.LOCAL_EXPERIMENT_DIR, args.experiment_name
    )
    os.makedirs(local_experiment_root_dir, exist_ok=True)

    # dump config to a file
    with open(os.path.join(local_experiment_root_dir, "hydra_config.yaml"), "w") as f:
        OmegaConf.save(cfg, f)

    # Also dump args to a file for backward compatibility
    with open(os.path.join(local_experiment_root_dir, "cmdline_args.txt"), "w") as f:
        json.dump(vars(args), f)

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

    flinksketch_job_id = None
    flinksketch_popen = None
    flink_pids = None
    arroyo_pids = None
    arroyosketch_pipeline_id = None
    arroyo_throughput_monitor = None
    prometheus_throughput_monitor = None
    prometheus_health_monitor = None

    # Initialize services
    kafka_service = KafkaService(provider, args.node_offset, num_tries=KAFKA_NUM_TRIES)
    flink_service = FlinkService(provider, args.node_offset)
    # Initialize query engine service based on language
    query_engine_service = QueryEngineServiceFactory.create_query_engine_service(
        args.query_engine_language,
        provider,
        use_container=args.use_container_query_engine,
        node_offset=args.node_offset,
    )
    system_exporters_service = SystemExportersService(
        provider, args.num_nodes, args.node_offset
    )
    prometheus_service = create_prometheus_service(
        cfg, provider, args.num_nodes, args.node_offset
    )
    prometheus_kafka_adapter_service = PrometheusKafkaAdapterService(
        provider, args.node_offset
    )
    arroyo_service = ArroyoService(
        provider,
        use_container=args.use_container_arroyo,
        node_offset=args.node_offset,
    )
    deathstar_service = DeathstarService(
        provider, num_nodes_in_experiment, args.node_offset
    )
    controller_service = ControllerService(
        provider,
        use_container=args.use_container_controller,
        node_offset=args.node_offset,
    )
    dumb_consumer_service = DumbKafkaConsumerService(provider, args.node_offset)
    prometheus_client_service = PrometheusClientService(
        provider,
        use_container=args.use_container_prometheus_client,
        node_offset=args.node_offset,
    )
    remote_monitor_service = RemoteMonitorService(provider, args.node_offset)
    avalanche_service = AvalancheExporterService(
        provider,
        num_nodes_in_experiment,
        use_container=False,
        node_offset=args.node_offset,
    )

    # Initialize exporter service based on language
    exporter_service = ExporterServiceFactory.create_exporter_service(
        args.fake_exporter_language,
        provider,
        num_nodes_in_experiment,
        use_container=args.use_container_fake_exporter,
        node_offset=args.node_offset,
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

        if (
            experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME
            and args.streaming_engine == "flink"
            and not args.do_local_flink
        ):
            flink_service.start()

        if args.do_local_flink:
            flink_service.stop()

        if (
            experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME
            and args.streaming_engine == "arroyo"
        ):
            arroyo_service.stop()
            time.sleep(10)
            arroyo_service.start(
                experiment_output_dir=experiment_output_dir,
                remote_write_base_port=args.remote_write_base_port,
                parallelism=args.parallelism,
            )

        prometheus_client_service.stop()
        remote_monitor_service.stop()
        flink_service.stop_all_jobs()
        arroyo_service.stop_all_jobs()
        if args.do_local_flink:
            flink_service.stop_all_java_processes()
        query_engine_service.stop()
        kafka_service.stop()
        prometheus_kafka_adapter_service.stop()
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
        prometheus_scrape_interval = config.get_prometheus_scrape_interval(
            cfg.prometheus
        )

        # copy_controller_client_config(args.controller_client_config, local_experiment_dir)
        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            # The controller queries Prometheus to auto-infer metric labels, so
            # exporters and Prometheus must be up and have at least one scrape
            # worth of data before the controller runs.
            if config.check_exporter_and_queries_exist(
                "fake_exporter", cfg.experiment_params
            ):
                exporter_service.start(
                    config=exporter_config["exporter_list"]["fake_exporter"],
                    experiment_output_dir=experiment_output_dir,
                    local_experiment_dir=local_experiment_dir,
                )
            prometheus_service.start(
                experiment_output_dir=experiment_output_dir,
                local_experiment_dir=local_experiment_dir,
                experiment_mode=experiment_mode,
            )
            # Wait for two scrape intervals so Prometheus has series to return.
            label_discovery_wait = prometheus_scrape_interval * 2
            print(
                f"Waiting {label_discovery_wait}s for Prometheus to scrape initial data "
                f"before running controller label inference..."
            )
            time.sleep(label_discovery_wait)

            prometheus_url = (
                f"http://localhost:{prometheus_service.get_query_endpoint_port()}"
            )
            controller_service.start(
                controller_input_file=controller_client_config,
                prometheus_scrape_interval=prometheus_scrape_interval,
                streaming_engine=args.streaming_engine,
                controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
                punting=args.controller_punting,
                prometheus_url=prometheus_url,
            )
            sync.rsync_controller_config_remote_to_local(
                provider,
                CONTROLLER_REMOTE_OUTPUT_DIR,
                CONTROLLER_LOCAL_OUTPUT_DIR,
                node_offset=args.node_offset,
            )
            kafka_service.start()
            kafka_service.wait_until_ready()
            kafka_service.delete_topics()
            kafka_service.create_topics()

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
        if cluster_data_service and config.check_exporter_and_queries_exist(
            "cluster_data_exporter", cfg.experiment_params
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
            if args.use_kafka_ingest:
                prometheus_kafka_adapter_service.start(
                    flink_input_format=args.flink_input_format
                )
            if args.streaming_engine == "flink":
                flinksketch_job_id, flinksketch_popen = flink_service.run_flinksketch(
                    experiment_output_dir=experiment_output_dir,
                    flink_input_format=args.flink_input_format,
                    flink_output_format=args.flink_output_format,
                    enable_object_reuse=args.enable_object_reuse,
                    do_local_flink=args.do_local_flink,
                    controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
                    compress_json=COMPRESS_JSON,
                )

                if args.profile_flink or args.do_local_flink:
                    while flink_pids is None:
                        flink_pids = flink_service.get_flink_pids(args.do_local_flink)
                        print(
                            "Waiting for Flink pids to be available. Sleeping for 10 seconds"
                        )
                        time.sleep(5)
            elif args.streaming_engine == "arroyo":
                arroyosketch_pipeline_id = arroyo_service.run_arroyosketch(
                    experiment_name=args.experiment_name,
                    experiment_output_dir=experiment_output_dir,
                    flink_input_format=args.flink_input_format,
                    flink_output_format=args.flink_output_format,
                    controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
                    remote_write_ip=args.remote_write_ip,
                    remote_write_base_port=args.remote_write_base_port,
                    remote_write_path=args.remote_write_path,
                    parallelism=args.parallelism,
                    use_kafka_ingest=args.use_kafka_ingest,
                    enable_optimized_remote_write=cfg.streaming.remote_write.enable_optimized_source,
                    avoid_long_ssh=constants.AVOID_RUN_ARROYOSKETCH_LONG_SSH,
                )
                print("ArroyoSketch pipeline ID: {}".format(arroyosketch_pipeline_id))

                if args.profile_arroyo:
                    while arroyo_pids is None:
                        arroyo_pids = arroyo_service.get_arroyo_pids()
                        print(
                            "Waiting for Arroyo pids to be available. Sleeping for 5 seconds"
                        )
                        time.sleep(5)

                # Start throughput monitoring if enabled
                if args.throughput_arroyo:
                    arroyo_throughput_monitor = ArroyoThroughputMonitor(
                        provider,
                        node_offset=args.node_offset,
                    )
                    arroyo_throughput_monitor.start(
                        pipeline_id=arroyosketch_pipeline_id,
                        experiment_output_dir=experiment_output_dir,
                    )
            else:
                raise ValueError(
                    "Invalid streaming engine: {}. Supported engines are 'flink' and 'arroyo'".format(
                        args.streaming_engine
                    )
                )

            # in case we want to run query engine manually
            if not cfg.flow.replace_query_engine_with_dumb_consumer:
                # Get prometheus port from prometheus service
                prometheus_port = prometheus_service.get_query_endpoint_port()
                # Get http port from query engine service
                http_port = query_engine_service.get_http_port()

                query_engine_service.start(
                    experiment_output_dir=experiment_output_dir,
                    flink_output_format=args.flink_output_format,
                    prometheus_scrape_interval=prometheus_scrape_interval,
                    log_level=args.log_level,
                    profile_query_engine=args.profile_query_engine,
                    manual=args.manual_query_engine,
                    streaming_engine=args.streaming_engine,
                    forward_unsupported_queries=args.forward_unsupported_queries,
                    controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
                    compress_json=COMPRESS_JSON,
                    dump_precomputes=args.dump_precomputes,
                    lock_strategy=args.lock_strategy,
                    query_language=args.query_language,
                    prometheus_port=prometheus_port,
                    http_port=http_port,
                )

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

        if cfg.flow.replace_query_engine_with_dumb_consumer:
            dumb_consumer_service.start(experiment_output_dir=experiment_output_dir)

        # TODO: rename this function and remote_monitor.py
        # run_remote_monitor(
        remote_monitor_service.start(
            controller_client_config,
            experiment_output_dir,
            experiment_mode,
            args.profile_query_engine,
            args.profile_prometheus_time,
            args.profile_flink,
            flink_pids,
            args.profile_arroyo,
            arroyo_pids,
            args.manual_remote_monitor,
            args.do_local_flink,
            args.streaming_engine,
            query_engine_service,
            arroyo_service,
            controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
            use_container_prometheus_client=args.use_container_prometheus_client,
            prometheus_client_parallel=args.prometheus_client_parallel,
            monitoring_tool=cfg.experiment_params.monitoring.tool,
            timed_duration=minimum_experiment_running_time if skip_querying else None,
        )

        if not args.manual_remote_monitor and constants.AVOID_REMOTE_MONITOR_LONG_SSH:
            # we need to wait here and keep checking if the remote monitor has finished
            remote_monitor_service.wait_for_remote_monitor_to_finish(
                minimum_experiment_running_time=minimum_experiment_running_time,
                polling_interval=REMOTE_PROCESS_POLLING_INTERVAL,
            )

        if cfg.flow.replace_query_engine_with_dumb_consumer:
            dumb_consumer_service.stop()

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
                if args.streaming_engine == "flink":
                    flink_service.stop_flinksketch(
                        job_id=flinksketch_job_id,
                        popen=flinksketch_popen,
                        flink_pids=flink_pids,
                        do_local_flink=args.do_local_flink,
                    )
                elif args.streaming_engine == "arroyo":
                    # Stop throughput monitoring if it was started
                    if args.throughput_arroyo:
                        if arroyo_throughput_monitor is None:
                            raise RuntimeError(
                                "Throughput monitoring was enabled but monitor is None"
                            )
                        arroyo_throughput_monitor.stop()

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

                if args.streaming_engine == "arroyo":
                    assert (
                        arroyosketch_pipeline_id is not None
                    ), "ArroyoSketch pipeline ID is None"
                    arroyo_service.stop_arroyosketch(arroyosketch_pipeline_id)
                    arroyo_service.stop()
                if args.use_kafka_ingest:
                    prometheus_kafka_adapter_service.stop()
                kafka_service.delete_topics()
                kafka_service.stop()

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

import os
import json
import time
import sys
import hydra
from omegaconf import DictConfig, OmegaConf

import constants
import experiment_utils
from experiment_utils import sync, config
from experiment_utils.providers.factory import create_provider
from experiment_utils.services import (
    KafkaService,
    QueryEngineServiceFactory,
    ExporterServiceFactory,
    ArroyoService,
    ArroyoThroughputMonitor,
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

COMPRESS_JSON = True
GRAFANA_ADMIN_PASSWORD = "admin"

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

    exporter_config, rejection_reason = experiment_utils.read_exporter_config(
        cfg.experiment_params
    )
    if exporter_config is None:
        raise ValueError("Invalid exporter config: {}".format(rejection_reason))

    arroyo_pids = None
    arroyosketch_pipeline_id = None
    arroyo_throughput_monitor = None
    prometheus_throughput_monitor = None

    # Initialize services
    kafka_service = KafkaService(provider, args.node_offset, num_tries=KAFKA_NUM_TRIES)
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

    if cfg.flow.replace_query_engine_with_dumb_consumer:
        raise NotImplementedError(
            "Dumb consumer is not supported in Grafana demo experiments"
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

    controller_client_config = os.path.join(
        experiment_root_output_dir,
        "controller_client_configs",
        f"{experiment_mode}.yaml",
    )

    if args.streaming_engine == "flink":
        raise NotImplementedError("Flink is not supported for Grafana demo")

    # if (
    #     experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME
    #     and args.streaming_engine == "flink"
    #     and not args.do_local_flink
    # ):
    #     flink_service.start()

    # if args.do_local_flink:
    #     flink_service.stop()

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

    # prometheus_client_service.stop()
    remote_monitor_service.stop()
    # flink_service.stop_all_jobs()
    arroyo_service.stop_all_jobs()
    # if args.do_local_flink:
    #     flink_service.stop_all_java_processes()
    query_engine_service.stop()
    kafka_service.stop()
    # prometheus_kafka_adapter_service.stop()
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
    prometheus_scrape_interval = config.get_prometheus_scrape_interval(cfg.prometheus)

    # copy_controller_client_config(args.controller_client_config, local_experiment_dir)
    if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
        controller_service.start(
            controller_input_file=controller_client_config,
            prometheus_scrape_interval=prometheus_scrape_interval,
            streaming_engine=args.streaming_engine,
            controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
            punting=args.controller_punting,
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

    if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
        if args.use_kafka_ingest:
            # prometheus_kafka_adapter_service.start(
            #     flink_input_format=args.flink_input_format
            # )
            raise NotImplementedError("Prometheus-Kafka adapter is not supported")
        # if args.streaming_engine == "flink":
        #     flinksketch_job_id, flinksketch_popen = flink_service.run_flinksketch(
        #         experiment_output_dir=experiment_output_dir,
        #         flink_input_format=args.flink_input_format,
        #         flink_output_format=args.flink_output_format,
        #         enable_object_reuse=args.enable_object_reuse,
        #         do_local_flink=args.do_local_flink,
        #         controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
        #         compress_json=COMPRESS_JSON,
        #     )

        #     if args.profile_flink or args.do_local_flink:
        #         while flink_pids is None:
        #             flink_pids = flink_service.get_flink_pids(args.do_local_flink)
        #             print(
        #                 "Waiting for Flink pids to be available. Sleeping for 10 seconds"
        #             )
        #             time.sleep(5)
        # elif args.streaming_engine == "arroyo":
        if args.streaming_engine == "arroyo":
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

        # Start Prometheus throughput monitoring if enabled
        if args.throughput_prometheus:
            prometheus_throughput_monitor = PrometheusThroughputMonitor(
                provider,
                node_offset=args.node_offset,
            )
            prometheus_throughput_monitor.start(
                experiment_output_dir=experiment_output_dir
            )
        # in case we want to run query engine manually
        if not cfg.flow.replace_query_engine_with_dumb_consumer:
            # Get prometheus port from prometheus service
            prometheus_port = prometheus_service.get_query_endpoint_port()

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
            )

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

    # if cfg.flow.replace_query_engine_with_dumb_consumer:
    #     dumb_consumer_service.start(experiment_output_dir=experiment_output_dir)

    # TODO: rename this function and remote_monitor.py
    # run_remote_monitor(
    # remote_monitor_service.start(
    #     controller_client_config,
    #     experiment_output_dir,
    #     experiment_mode,
    #     args.profile_query_engine,
    #     args.profile_prometheus_time,
    #     args.profile_flink,
    #     flink_pids,
    #     args.profile_arroyo,
    #     arroyo_pids,
    #     args.manual_remote_monitor,
    #     args.do_local_flink,
    #     args.streaming_engine,
    #     query_engine_service,
    #     arroyo_service,
    #     controller_remote_output_dir=CONTROLLER_REMOTE_OUTPUT_DIR,
    #     use_container_prometheus_client=args.use_container_prometheus_client,
    #     prometheus_client_parallel=args.prometheus_client_parallel,
    #     monitoring_tool=cfg.experiment_params.monitoring.tool,
    #     timed_duration=None,
    # )

    # if not args.manual_remote_monitor and constants.AVOID_REMOTE_MONITOR_LONG_SSH:
    #     # we need to wait here and keep checking if the remote monitor has finished
    #     remote_monitor_service.wait_for_remote_monitor_to_finish(
    #         minimum_experiment_running_time=minimum_experiment_running_time,
    #         polling_interval=REMOTE_PROCESS_POLLING_INTERVAL,
    #     )

    # if cfg.flow.replace_query_engine_with_dumb_consumer:
    #     dumb_consumer_service.stop()

    # sync.copy_prometheus_data(
    #     args.cloudlab_username, args.hostname_suffix, local_experiment_dir, args.node_offset
    # )

    # # Skip teardown if the no_teardown flag is set
    # if not args.no_teardown:
    #     if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
    #         query_engine_service.stop()
    #         if args.streaming_engine == "flink":
    #             flink_service.stop_flinksketch(
    #                 job_id=flinksketch_job_id,
    #                 popen=flinksketch_popen,
    #                 flink_pids=flink_pids,
    #                 do_local_flink=args.do_local_flink,
    #             )
    #         elif args.streaming_engine == "arroyo":
    #             # Stop throughput monitoring if it was started
    #             if args.throughput_arroyo:
    #                 if arroyo_throughput_monitor is None:
    #                     raise RuntimeError(
    #                         "Throughput monitoring was enabled but monitor is None"
    #                     )
    #                 arroyo_throughput_monitor.stop()

    #         # Stop Prometheus throughput monitoring if it was started
    #         if args.throughput_prometheus:
    #             if prometheus_throughput_monitor is None:
    #                 raise RuntimeError(
    #                     "Prometheus throughput monitoring was enabled but monitor is None"
    #                 )
    #             prometheus_throughput_monitor.stop()

    #         if args.streaming_engine == "arroyo":
    #             assert (
    #                 arroyosketch_pipeline_id is not None
    #             ), "ArroyoSketch pipeline ID is None"
    #             arroyo_service.stop_arroyosketch(arroyosketch_pipeline_id)
    #             arroyo_service.stop()
    #         if args.use_kafka_ingest:
    #             prometheus_kafka_adapter_service.stop()
    #         kafka_service.delete_topics()
    #         kafka_service.stop()

    #     system_exporters_service.stop()
    #     prometheus_service.stop()
    #     controller_service.stop()  # only does something if controller is containerized
    #     exporter_service.stop()
    #     deathstar_service.stop()
    #     prometheus_service.reset()

    #     # Also stop avalanche exporters if they were started
    #     if config.check_exporter_and_queries_exist(
    #         "avalanche", cfg.experiment_params
    #     ):
    #         avalanche_service.stop()

    # sync.rsync_experiment_data(
    #     args.cloudlab_username,
    #     args.hostname_suffix,
    #     experiment_output_dir,
    #     local_experiment_dir,
    #     node_offset=args.node_offset,
    # )


if __name__ == "__main__":
    main()

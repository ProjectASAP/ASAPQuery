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
    ExporterServiceFactory,
    SystemExportersService,
    create_prometheus_service,
    PrometheusService,
    DockerPrometheusService,
    DockerVictoriaMetricsService,
    ArroyoService,
    ArroyoThroughputMonitor,
    PrometheusThroughputMonitor,
    PrometheusHealthMonitor,
    ControllerService,
)

# Register custom resolver for LOCAL_EXPERIMENT_DIR before Hydra processes config
OmegaConf.register_new_resolver(
    "local_experiment_dir", lambda: constants.LOCAL_EXPERIMENT_DIR
)

# Register custom resolver for remote write IP based on node_offset
OmegaConf.register_new_resolver(
    "remote_write_ip", lambda node_offset: f"10.10.1.{node_offset + 1}"
)

KAFKA_NUM_TRIES = 5
CONTROLLER_LOCAL_OUTPUT_DIR = None
CONTROLLER_REMOTE_OUTPUT_DIR = None


@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg: DictConfig):
    # Validate configuration
    config.validate_config(cfg)
    # Validate experiment configuration (queries not required for ingest path experiments)
    config.validate_experiment_config(cfg.experiment_params, require_queries=False)

    # Check that experiment_duration is specified
    if not hasattr(cfg.experiment_params, "experiment_duration"):
        raise ValueError(
            "experiment_duration must be specified in experiment config. "
            "Add it as a CLI override: experiment_duration=300"
        )

    experiment_duration = cfg.experiment_params.experiment_duration
    print(f"Experiment duration: {experiment_duration} seconds")

    # Determine experiment mode
    if (
        not hasattr(cfg.experiment_params, "experiment")
        or not cfg.experiment_params.experiment
    ):
        raise ValueError("experiment mode must be specified in experiment config")

    experiment_mode = cfg.experiment_params.experiment[0].get("mode", "")
    if experiment_mode not in [
        constants.BASELINE_EXPERIMENT_NAME,
        constants.SKETCHDB_EXPERIMENT_NAME,
    ]:
        raise ValueError(
            f"Invalid experiment mode: {experiment_mode}. "
            f"Must be '{constants.BASELINE_EXPERIMENT_NAME}' (V1) or '{constants.SKETCHDB_EXPERIMENT_NAME}' (V2)"
        )

    print(f"Experiment mode: {experiment_mode}")
    # V2 uses sketchdb mode (enables remote_write), V1 uses prometheus mode
    is_v2 = experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME

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

    # Create output directory on coordinator node
    provider.execute_command(
        node_idx=args.get_coordinator_node(),
        cmd=f"mkdir -p {experiment_root_output_dir}",
        cmd_dir="",
        nohup=False,
        popen=False,
    )

    num_nodes_in_experiment = args.num_nodes

    # Read exporter configuration
    exporter_config, rejection_reason = experiment_utils.read_exporter_config(
        cfg.experiment_params
    )
    if exporter_config is None:
        raise ValueError("Invalid exporter config: {}".format(rejection_reason))

    # Initialize services
    system_exporters_service = SystemExportersService(
        provider, args.num_nodes, args.node_offset
    )
    prometheus_service = create_prometheus_service(
        cfg, provider, args.num_nodes, args.node_offset
    )

    # Initialize exporter service based on language
    exporter_service = ExporterServiceFactory.create_exporter_service(
        args.fake_exporter_language,
        provider,
        num_nodes_in_experiment,
        use_container=args.use_container_fake_exporter,
        node_offset=args.node_offset,
    )

    # Initialize V2-specific services (always initialize to allow cleanup from previous runs)
    arroyo_throughput_monitor = None
    arroyosketch_pipeline_id = None

    print("Initializing services (including V2 services for cleanup)...")
    kafka_service = KafkaService(provider, args.node_offset, num_tries=KAFKA_NUM_TRIES)
    arroyo_service = ArroyoService(
        provider,
        use_container=args.use_container_arroyo,
        node_offset=args.node_offset,
    )
    controller_service = ControllerService(
        provider,
        use_container=args.use_container_controller,
        node_offset=args.node_offset,
    )

    if is_v2:
        global CONTROLLER_LOCAL_OUTPUT_DIR, CONTROLLER_REMOTE_OUTPUT_DIR
        CONTROLLER_LOCAL_OUTPUT_DIR = os.path.join(
            local_experiment_root_dir, "controller_output"
        )
        CONTROLLER_REMOTE_OUTPUT_DIR = os.path.join(
            experiment_root_output_dir, "controller_output"
        )

    # Stop any existing services to ensure clean state
    print("Stopping any existing services...")
    system_exporters_service.stop()
    prometheus_service.stop()
    exporter_service.stop()
    prometheus_service.reset()
    kafka_service.stop()
    arroyo_service.stop()
    controller_service.stop()
    # Create local and remote experiment directories
    experiment_output_dir = os.path.join(experiment_root_output_dir, experiment_mode)
    local_experiment_dir = os.path.join(local_experiment_root_dir, experiment_mode)

    provider.execute_command_parallel(
        node_idxs=args.get_node_range(include_coordinator=True),
        cmd=f"mkdir -p {experiment_output_dir}",
        cmd_dir="",
        nohup=False,
        popen=True,
        wait=True,
    )

    # Generate and copy Prometheus configuration
    prometheus_config_output_dir = os.path.join(
        local_experiment_dir, constants.PROMETHEUS_CONFIG_DIR
    )
    os.makedirs(prometheus_config_output_dir, exist_ok=True)

    # For V2, we need to generate controller configs even though we're not running queries
    if is_v2:
        print("Generating controller and client configs for V2...")
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

    # Generate Prometheus config (with or without remote_write based on mode)
    # Remote write is enabled when experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME
    # V1: experiment_mode=BASELINE_EXPERIMENT_NAME != SKETCHDB_EXPERIMENT_NAME -> no remote_write
    # V2: experiment_mode=SKETCHDB_EXPERIMENT_NAME == SKETCHDB_EXPERIMENT_NAME -> remote_write enabled
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

    # Start V2-specific infrastructure before Prometheus
    if is_v2:
        print("Starting V2 infrastructure (Controller, Kafka, Arroyo)...")

        # Start controller to generate sketch configs
        controller_client_config = os.path.join(
            experiment_root_output_dir,
            "controller_client_configs",
            f"{experiment_mode}.yaml",
        )
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

        # Start Kafka
        kafka_service.start()
        kafka_service.wait_until_ready()
        kafka_service.delete_topics()
        kafka_service.create_topics()

        # Start Arroyo
        arroyo_service.stop()
        time.sleep(10)
        arroyo_service.start(
            experiment_output_dir=experiment_output_dir,
            remote_write_base_port=args.remote_write_base_port,
            parallelism=args.parallelism,
        )

    # Start fake exporter if configured
    if config.check_exporter_and_queries_exist("fake_exporter", cfg.experiment_params):
        print("Starting fake exporter...")
        exporter_service.start(
            config=exporter_config["exporter_list"]["fake_exporter"],
            experiment_output_dir=experiment_output_dir,
            local_experiment_dir=local_experiment_dir,
        )

    # Start system exporters (node_exporter, blackbox_exporter, cadvisor)
    print("Starting system exporters...")
    system_exporters_service.start(cfg.experiment_params)

    # Start Prometheus service based on deployment mode
    print("Starting Prometheus...")
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
            )
    else:  # bare_metal
        # Bare-metal deployment (PrometheusService)
        assert isinstance(
            prometheus_service, PrometheusService
        ), f"Expected PrometheusService but got {type(prometheus_service).__name__}"
        prometheus_service.start(experiment_output_dir)

    # Start V2-specific: Run ArroyoSketch pipeline
    if is_v2:
        print("Starting ArroyoSketch pipeline...")
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
        print(f"ArroyoSketch pipeline ID: {arroyosketch_pipeline_id}")

    # Start monitoring services
    print("Starting monitoring services...")

    # Prometheus throughput monitoring
    prometheus_throughput_monitor = PrometheusThroughputMonitor(
        provider,
        node_offset=args.node_offset,
    )
    prometheus_throughput_monitor.start(experiment_output_dir=experiment_output_dir)

    # Prometheus health monitoring
    prometheus_health_monitor = PrometheusHealthMonitor(
        provider,
        node_offset=args.node_offset,
    )
    prometheus_health_monitor.start(experiment_output_dir=experiment_output_dir)

    # Start Arroyo throughput monitoring if V2
    if is_v2 and arroyosketch_pipeline_id:
        print("Starting Arroyo throughput monitoring...")
        arroyo_throughput_monitor = ArroyoThroughputMonitor(
            provider,
            node_offset=args.node_offset,
        )
        arroyo_throughput_monitor.start(
            pipeline_id=arroyosketch_pipeline_id,
            experiment_output_dir=experiment_output_dir,
        )

    # Start resource cost monitoring via remote_monitor.py
    print("Starting resource cost monitoring (CPU/memory)...")
    start_resource_monitoring(
        provider,
        args.node_offset,
        experiment_output_dir,
        local_experiment_dir,
        experiment_duration,
        is_v2,
    )

    print("-" * 60)
    print("All services started successfully!")
    print(f"Experiment: {args.experiment_name}")
    print(f"Mode: {experiment_mode}")
    print(f"Duration: {experiment_duration} seconds")
    print(f"Output directory: {experiment_output_dir}")
    print("-" * 60)

    # Wait for experiment duration
    print(f"\nWaiting {experiment_duration} seconds for experiment to run...")
    time.sleep(experiment_duration)

    print("\nExperiment duration complete. Stopping services...")

    # Stop monitoring services
    print("Stopping monitoring services...")
    prometheus_throughput_monitor.stop()
    prometheus_health_monitor.stop()

    if is_v2 and arroyo_throughput_monitor:
        arroyo_throughput_monitor.stop()

    # Note: remote_monitor.py will stop automatically after the timed duration

    # Stop V2-specific services
    if is_v2:
        print("Stopping V2 services...")
        if arroyosketch_pipeline_id:
            arroyo_service.stop_arroyosketch(arroyosketch_pipeline_id)
        arroyo_service.stop()
        kafka_service.delete_topics()
        kafka_service.stop()
        controller_service.stop()

    # Stop core services
    print("Stopping core services...")
    system_exporters_service.stop()
    prometheus_service.stop()
    exporter_service.stop()

    # Sync data to local
    print("\nSyncing Prometheus data...")
    sync.copy_prometheus_data(provider, local_experiment_dir, args.node_offset)

    print("Syncing experiment data...")
    sync.rsync_experiment_data(
        provider,
        experiment_output_dir,
        local_experiment_dir,
        node_offset=args.node_offset,
    )
    prometheus_service.reset()

    print("-" * 60)
    print("Experiment completed successfully!")
    print(f"Local output: {local_experiment_dir}")
    print("-" * 60)


def start_resource_monitoring(
    provider,
    node_offset: int,
    experiment_output_dir: str,
    local_experiment_dir: str,
    duration: int,
    is_v2: bool,
):
    """
    Start resource monitoring using remote_monitor.py in timed mode.

    Args:
        provider: Infrastructure provider
        node_offset: Node offset
        experiment_output_dir: Remote output directory for monitoring data
        local_experiment_dir: Local experiment directory
        duration: Duration to run monitoring in seconds
        is_v2: Whether this is V2 (includes Arroyo monitoring)
    """
    import yaml

    # Determine keywords for process/container monitoring
    keywords = ["prometheus"]  # Will match prometheus container or process

    if is_v2:
        keywords.append("arroyo")  # Will match arroyo worker containers/processes

    # Create minimal config file locally (remote_monitor.py needs this)
    local_monitor_config_dir = os.path.join(
        local_experiment_dir, "remote_monitor_config"
    )
    os.makedirs(local_monitor_config_dir, exist_ok=True)

    local_config_path = os.path.join(local_monitor_config_dir, "monitor_config.yaml")
    minimal_config = {"export_cost_and_latency": False}

    # Write config file locally
    with open(local_config_path, "w") as f:
        yaml.dump(minimal_config, f)

    # Rsync config to remote
    remote_monitor_config_dir = os.path.join(
        experiment_output_dir, "remote_monitor_config"
    )
    hostname = f"node{node_offset}.{provider.hostname_suffix}"
    rsync_cmd = 'rsync -azh -e "ssh {}" {} {}@{}:{}/'.format(
        constants.SSH_OPTIONS,
        local_monitor_config_dir,
        provider.username,
        hostname,
        os.path.dirname(remote_monitor_config_dir),
    )

    import utils

    utils.run_cmd(rsync_cmd, popen=False, ignore_errors=False)

    config_file_path = os.path.join(remote_monitor_config_dir, "monitor_config.yaml")

    # Build remote_monitor.py command
    cmd = (
        "python3 -u remote_monitor.py "
        "--execution_mode timed "
        "--experiment_mode ingest_path "
        f'--keywords "{",".join(keywords)}" '
        f"--config_file {config_file_path} "
        f"--experiment_output_dir {experiment_output_dir} "
        "--monitor_output_file monitor_output.json "
        f"--time_to_run {duration} "
        f"--node_offset {node_offset} "
    )

    cmd_dir = os.path.join(provider.get_home_dir(), "code", "Utilities", "experiments")

    cmd += f" > {experiment_output_dir}/remote_monitor.out 2>&1 &"

    print(f"Starting resource monitoring with command: {cmd}")

    provider.execute_command(
        node_idx=node_offset,
        cmd=cmd,
        cmd_dir=cmd_dir,
        nohup=True,
        popen=False,
    )


if __name__ == "__main__":
    main()

import os
import json

import hydra
from omegaconf import DictConfig, OmegaConf

import constants
import experiment_utils
from experiment_utils import sync, config
from experiment_utils.providers.factory import create_provider
from experiment_utils.services import (
    ExporterServiceFactory,
    SystemExportersService,
    create_prometheus_service,
    PrometheusService,
    DockerPrometheusService,
    DockerVictoriaMetricsService,
)

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

    # Stop any existing services to ensure clean state
    system_exporters_service.stop()
    prometheus_service.stop()
    exporter_service.stop()
    prometheus_service.reset()

    # Create local and remote experiment directories
    experiment_output_dir = experiment_root_output_dir
    local_experiment_dir = local_experiment_root_dir

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

    experiment_mode = (
        constants.BASELINE_EXPERIMENT_NAME
    )  # This script runs in baseline mode
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
                experiment_mode=experiment_mode,
            )
    else:  # bare_metal
        # Bare-metal deployment (PrometheusService)
        assert isinstance(
            prometheus_service, PrometheusService
        ), f"Expected PrometheusService but got {type(prometheus_service).__name__}"
        prometheus_service.start(experiment_output_dir)

    print("-" * 60)
    print("Services started successfully!")
    print(f"Experiment: {args.experiment_name}")
    print(f"Output directory: {experiment_output_dir}")
    print("-" * 60)

    # Check no_teardown flag
    no_teardown = getattr(args, "no_teardown", False)

    if no_teardown:
        print("No teardown mode: Services will keep running.")
        print("To stop services manually, use the appropriate stop commands.")
    else:
        print("Press Enter to stop services and teardown...")
        input()

        print("\nStopping services...")
        system_exporters_service.stop()
        prometheus_service.stop()
        exporter_service.stop()
        prometheus_service.reset()

        # print("Syncing Prometheus data...")
        # sync.copy_prometheus_data(provider, local_experiment_dir, args.node_offset)

        # print("Syncing experiment data...")
        # sync.rsync_experiment_data(
        #     provider,
        #     experiment_output_dir,
        #     local_experiment_dir,
        #     node_offset=args.node_offset,
        # )

        print("-" * 60)
        print("Experiment completed successfully!")
        print(f"Local output: {local_experiment_dir}")
        print("-" * 60)


if __name__ == "__main__":
    main()

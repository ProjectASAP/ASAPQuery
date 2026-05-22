"""
Experiment runner for ClickHouse/SQL experiments — baseline mode.

Flow:
  rsync dataset file → node
  ClickHouseService.start()
  ClickHouseDataLoaderService.start()   (once, before mode loop; DROP + reload)

  for experiment_mode in ["baseline"]:
      run prometheus-client in ClickHouse SQL mode (blocking)
      rsync results back
      teardown if not no_teardown

Usage:
  python experiment_run_clickhouse.py \\
    experiment_type=clickhouse \\
    experiment.name=my_bench \\
    cloudlab.num_nodes=1 \\
    cloudlab.username=myuser \\
    cloudlab.hostname_suffix=myexp.cloudlab.us \\
    experiment_params.dataset.name=clickbench \\
    experiment_params.dataset.local_data_file=/path/to/hits.json \\
    'experiment_params.query_groups[0].sql_file=/path/to/queries.sql'
"""

import json
import os
from urllib.parse import urlparse

import hydra
from omegaconf import DictConfig, OmegaConf

import constants
from experiment_utils import config, sync
from experiment_utils.providers.factory import create_provider
from experiment_utils.services import ClickHouseDataLoaderService, ClickHouseService

# Register resolvers used by config.yaml interpolation.
OmegaConf.register_new_resolver(
    "local_experiment_dir", lambda: constants.LOCAL_EXPERIMENT_DIR
)
OmegaConf.register_new_resolver(
    "remote_write_ip", lambda node_offset: f"10.10.1.{node_offset + 1}"
)


def _run_query_client(
    provider,
    node_offset: int,
    config_file: str,
    output_dir: str,
    use_container: bool,
    parallel: bool,
) -> None:
    """SSH to the node and run the prometheus-client, blocking until done.

    For bare-metal: runs main_prometheus_client.py directly.
    For containerized: generates a docker-compose file then runs
    `docker compose up --no-build` (foreground, exits when container exits).
    """
    home_dir = provider.get_home_dir()
    prometheus_client_dir = os.path.join(
        home_dir, "code", "asap-tools", "queriers", "prometheus-client"
    )

    if use_container:
        helper_script = os.path.join(
            home_dir,
            "code",
            "asap-tools",
            "experiments",
            "generate_prometheus_client_compose.py",
        )
        template_path = os.path.join(prometheus_client_dir, "docker-compose.yml.j2")
        remote_compose_file = os.path.join(
            output_dir, "prometheus-client-docker-compose.yml"
        )
        node_ip = provider.get_node_ip(node_offset)

        gen_compose_cmd = (
            f"python3 {helper_script}"
            f" --template-path {template_path}"
            f" --compose-output-path {remote_compose_file}"
            f" --prometheusclient-dir {prometheus_client_dir}"
            f" --container-name sketchdb-prometheusclient"
            f" --experiment-output-dir {output_dir}"
            f" --config-file {config_file}"
            f" --client-output-dir {output_dir}"
            f" --client-output-file prometheus_client_output.txt"
            f" --prometheus-host {node_ip}"
            f" --sketchdb-host {node_ip}"
        )
        if parallel:
            gen_compose_cmd += " --parallel"

        # docker compose up without -d: foreground, blocks until container exits
        cmd = (
            f"mkdir -p {output_dir}; "
            f"{gen_compose_cmd}; "
            f"docker compose -f {remote_compose_file} up --no-build"
        )
    else:
        cmd = (
            f"python3 -u main_prometheus_client.py"
            f" --config_file {config_file}"
            f" --output_dir {output_dir}"
            f" --output_file prometheus_client_output.txt"
        )
        if parallel:
            cmd += " --parallel"

    provider.execute_command(
        node_idx=node_offset,
        cmd=cmd,
        cmd_dir=prometheus_client_dir,
        nohup=False,
        popen=False,
    )


@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg: DictConfig) -> None:
    config.validate_basic_config(
        cfg,
        required_params=[
            ("experiment.name", "Human-readable experiment name"),
            ("cloudlab.num_nodes", "Number of CloudLab nodes to use"),
            ("cloudlab.username", "Your CloudLab username"),
            ("cloudlab.hostname_suffix", "CloudLab experiment hostname suffix"),
        ],
        script_name="experiment_run_clickhouse",
    )
    config.validate_experiment_config(cfg.experiment_params)

    provider = create_provider(cfg)

    experiment_name = cfg.experiment.name
    node_offset = cfg.cloudlab.node_offset
    no_teardown = cfg.flow.no_teardown
    skip_querying = cfg.experiment_params.get("skip_querying", False)
    use_container = cfg.use_container.prometheus_client
    parallel = cfg.prometheus_client.parallel

    local_experiment_root_dir = os.path.join(
        constants.LOCAL_EXPERIMENT_DIR, experiment_name
    )
    os.makedirs(local_experiment_root_dir, exist_ok=True)

    with open(os.path.join(local_experiment_root_dir, "hydra_config.yaml"), "w") as f:
        OmegaConf.save(cfg, f)
    with open(os.path.join(local_experiment_root_dir, "cmdline_args.txt"), "w") as f:
        json.dump({"experiment_name": experiment_name, "node_offset": node_offset}, f)

    experiment_root_output_dir = (
        f"{constants.CLOUDLAB_HOME_DIR}/experiment_outputs/{experiment_name}"
    )
    provider.execute_command(
        node_idx=node_offset,
        cmd=f"mkdir -p {experiment_root_output_dir}",
        cmd_dir="",
        nohup=False,
        popen=False,
    )

    sync.copy_experiment_config(cfg.experiment_params, local_experiment_root_dir)

    # --- dataset config ---
    ep = cfg.experiment_params
    dataset_cfg = ep.dataset
    dataset_name = str(dataset_cfg.name)
    local_data_file = str(dataset_cfg.local_data_file)
    table = dataset_cfg.get("table") or None
    init_sql_file = dataset_cfg.get("init_sql_file") or None
    max_rows = int(dataset_cfg.get("max_rows", 0))

    # --- ClickHouse connection ---
    clickhouse_url = str(cfg.clickhouse.url)
    clickhouse_database = str(cfg.clickhouse.database)
    clickhouse_http_port = urlparse(clickhouse_url).port or 8123

    # --- generate prometheus-client config YAMLs for each experiment mode ---
    if not skip_querying:
        mode_server_urls = {constants.BASELINE_EXPERIMENT_NAME: clickhouse_url}
        experiment_modes = config.generate_clickhouse_client_configs(
            query_groups=ep.query_groups,
            local_experiment_dir=local_experiment_root_dir,
            mode_server_urls=mode_server_urls,
            clickhouse_database=clickhouse_database,
        )
        sync.rsync_controller_client_configs(
            provider,
            experiment_root_output_dir,
            local_experiment_root_dir,
            node_offset=node_offset,
        )
    else:
        print("-" * 40)
        print("skip_querying=True: no SQL queries will be executed")
        print("-" * 40)
        experiment_modes = [constants.BASELINE_EXPERIMENT_NAME]

    # --- rsync dataset file to node ---
    remote_data_dir = os.path.join(experiment_root_output_dir, "data")
    remote_data_file = sync.rsync_dataset_file(
        provider, local_data_file, remote_data_dir, node_offset
    )

    # --- start ClickHouse (persists across all modes) ---
    clickhouse_service = ClickHouseService(
        provider, num_nodes=cfg.cloudlab.num_nodes, node_offset=node_offset
    )
    clickhouse_service.start(
        experiment_output_dir=experiment_root_output_dir,
        local_experiment_dir=local_experiment_root_dir,
        http_port=clickhouse_http_port,
        database=clickhouse_database,
    )

    # --- load data once before the mode loop (DROP + reload) ---
    data_loader = ClickHouseDataLoaderService(
        provider,
        num_nodes=cfg.cloudlab.num_nodes,
        node_offset=node_offset,
        clickhouse_http_port=clickhouse_http_port,
    )
    data_loader.start(
        dataset_name=dataset_name,
        remote_data_file=remote_data_file,
        table=table,
        init_sql_file=init_sql_file,
        max_rows=max_rows,
    )

    # --- mode loop ---
    for experiment_mode in experiment_modes:
        print(f"Running experiment mode: {experiment_mode}")

        experiment_output_dir = os.path.join(
            experiment_root_output_dir, experiment_mode
        )
        local_experiment_dir = os.path.join(local_experiment_root_dir, experiment_mode)
        provider.execute_command(
            node_idx=node_offset,
            cmd=f"mkdir -p {experiment_output_dir}",
            cmd_dir="",
            nohup=False,
            popen=False,
        )
        os.makedirs(local_experiment_dir, exist_ok=True)

        if not skip_querying:
            controller_client_config = os.path.join(
                experiment_root_output_dir,
                "controller_client_configs",
                f"{experiment_mode}.yaml",
            )
            _run_query_client(
                provider=provider,
                node_offset=node_offset,
                config_file=controller_client_config,
                output_dir=experiment_output_dir,
                use_container=use_container,
                parallel=parallel,
            )

        sync.rsync_experiment_data(
            provider,
            experiment_output_dir,
            local_experiment_dir,
            node_offset=node_offset,
        )

    # --- teardown ---
    if not no_teardown:
        clickhouse_service.stop()

    print("Experiment complete.")


if __name__ == "__main__":
    main()

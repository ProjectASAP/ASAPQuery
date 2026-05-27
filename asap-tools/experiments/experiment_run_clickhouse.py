"""
Experiment runner for ClickHouse/SQL experiments.

Supports two modes, selected automatically based on config:

  baseline  — queries go directly to ClickHouse (:8123).
  sketchdb  — queries go to the ASAP precompute engine (:8088), which serves
              approximate results from KLL sketches and forwards unsupported
              queries to ClickHouse as a fallback.

────────────────────────────────────────────────────────────
Baseline-only flow
────────────────────────────────────────────────────────────
  rsync dataset file → node
  ClickHouseService.start()
  ClickHouseDataLoaderService.start()   (DROP + reload)

  for experiment_mode in ["baseline"]:
      run prometheus-client → ClickHouse :8123
      rsync results back

────────────────────────────────────────────────────────────
Sketchdb flow  (enabled when experiment list contains mode: sketchdb)
────────────────────────────────────────────────────────────
  (same rsync + ClickHouse start + data load as above)

  for experiment_mode in ["baseline", "sketchdb"]:
      if sketchdb:
          rsync streaming_config.yaml → node
          query_engine_rust starts (precompute engine + JSON ingest + CH fallback)
          wait until process is up
          sleep flow.steady_state_wait  (ingest completes during this window)
          run prometheus-client → ASAP :8088
          rsync results back
          stop query_engine_rust

────────────────────────────────────────────────────────────
Pre-run steps for sketchdb mode
────────────────────────────────────────────────────────────
1. Generate the SQL query file (once per dataset/window configuration):

     python asap-tools/execution-utilities/benchmark/generate_queries.py \\
       --table-name hits \\
       --ts-column EventTime \\
       --value-column ResolutionWidth \\
       --group-by-columns RegionID,OS,UserAgent,TraficSourceID \\
       --window-size 600 \\
       --stride-seconds 600 \\
       --output-prefix /path/to/output/clickhouse_quantile_queries \\
       --auto-detect-timestamps \\
       --data-file /path/to/hits.json \\
       --data-file-format jsonl

   The same SQL file is used for both baseline (load tester) and sketchdb
   (load tester + planner input).

2. Ensure the release binaries are built on the CloudLab node:
     cargo build --release   (in ~/code/asap-query-engine)
     cargo build --release   (in ~/code/asap-planner-rs)

   asap-planner runs automatically in SQL mode during the experiment to
   generate streaming_config.yaml and inference_config.yaml — no manual
   config file authoring needed.

────────────────────────────────────────────────────────────
Usage
────────────────────────────────────────────────────────────
Baseline only:
  python experiment_run_clickhouse.py \\
    experiment_type=clickhouse \\
    experiment.name=my_bench \\
    cloudlab.num_nodes=1 \\
    cloudlab.username=myuser \\
    cloudlab.hostname_suffix=myexp.cloudlab.us \\
    experiment_params.dataset.local_data_file=/path/to/hits.json \\
    'experiment_params.query_groups[0].sql_file=/path/to/clickbench.sql'

Baseline + sketchdb:
  python experiment_run_clickhouse.py \\
    experiment_type=clickhouse \\
    experiment.name=my_bench \\
    cloudlab.num_nodes=1 \\
    cloudlab.username=myuser \\
    cloudlab.hostname_suffix=myexp.cloudlab.us \\
    experiment_params.dataset.local_data_file=/path/to/hits.json \\
    'experiment_params.query_groups[0].sql_file=/path/to/clickbench.sql'

  Sketchdb mode runs when experiment_params.experiment contains an entry
  with mode: sketchdb (mirrors the Prometheus experiment config structure).
  Remove that entry to run baseline only.
"""

import json
import os
import time
from urllib.parse import urlparse

import hydra
import yaml
from omegaconf import DictConfig, OmegaConf

import constants
from experiment_utils import config, sync
from experiment_utils.providers.factory import create_provider
from experiment_utils.services import (
    ClickHouseDataLoaderService,
    ClickHouseService,
    PrometheusClientService,
)
from experiment_utils.services.misc import ControllerService
from experiment_utils.services.query_engine import QueryEngineRustService

CLICKHOUSE_DATABASE = "default"


def _inline_sql_queries_in_experiment_config(local_experiment_root_dir: str) -> None:
    """Enrich the saved experiment_params.yaml by inlining SQL from sql_file references.

    Downstream analysis scripts expect query_groups[i]["queries"] to be a list
    of query strings.  The clickhouse config stores sql_file paths instead, so
    we read each file and add the queries in-place before the scripts run.
    """
    config_path = os.path.join(
        local_experiment_root_dir, "experiment_config", "experiment_params.yaml"
    )
    if not os.path.exists(config_path):
        return
    with open(config_path) as f:
        data = yaml.safe_load(f)

    def _expand_groups(groups):
        if not groups:
            return
        for group in groups:
            sql_file = group.get("sql_file")
            if sql_file and "queries" not in group:
                with open(sql_file) as fq:
                    content = fq.read()
                group["queries"] = [s.strip() for s in content.split(";") if s.strip()]

    _expand_groups(data.get("query_groups"))

    with open(config_path, "w") as f:
        yaml.dump(data, f, allow_unicode=True)


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
    _inline_sql_queries_in_experiment_config(local_experiment_root_dir)

    # --- dataset config ---
    ep = cfg.experiment_params
    dataset_cfg = ep.dataset
    dataset_name = str(dataset_cfg.name)
    local_data_file = str(dataset_cfg.local_data_file)
    table = dataset_cfg.table
    init_sql_file = dataset_cfg.init_sql_file
    max_rows = int(dataset_cfg.max_rows)

    # --- experiment modes and server URLs from config (mirrors Prometheus structure) ---
    experiment_cfg = OmegaConf.to_container(ep.experiment, resolve=True)
    servers_by_name = {
        s["name"]: s["url"] for s in OmegaConf.to_container(ep.servers, resolve=True)
    }
    mode_server_urls = {m["mode"]: servers_by_name[m["server"]] for m in experiment_cfg}
    clickhouse_url = servers_by_name["clickhouse"]
    clickhouse_http_port = urlparse(clickhouse_url).port
    data_ingestion_interval = int(ep.data_ingestion_interval)

    # --- generate prometheus-client config YAMLs for each experiment mode ---
    experiment_modes = config.generate_clickhouse_client_configs(
        query_groups=ep.query_groups,
        local_experiment_dir=local_experiment_root_dir,
        mode_server_urls=mode_server_urls,
        clickhouse_database=CLICKHOUSE_DATABASE,
    )
    sync.rsync_controller_client_configs(
        provider,
        experiment_root_output_dir,
        local_experiment_root_dir,
        node_offset=node_offset,
    )

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
        database=CLICKHOUSE_DATABASE,
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
    prometheus_client_service = PrometheusClientService(
        provider, use_container=use_container, node_offset=node_offset
    )

    for experiment_mode in experiment_modes:
        print(f"Running experiment mode: {experiment_mode}")

        # Clean up any leftover prometheus-client container from the previous mode,
        # mirroring the prometheus_client_service.stop() call at the top of the
        # e2e mode loop.
        prometheus_client_service.stop()

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

        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            # --- sketchdb mode: precompute engine + JSON ingest + ClickHouse fallback ---
            asap_http_port = urlparse(mode_server_urls[experiment_mode]).port
            # Kill any leftover query_engine_rust from a previous run (mirrors
            # query_engine_service.stop() at the top of the e2e mode loop).
            QueryEngineRustService(
                provider=provider, use_container=False, node_offset=node_offset
            ).stop()
            # Mirrors experiment_run_e2e.py: planner runs first and generates
            # streaming_config.yaml + inference_config.yaml into controller_output_dir,
            # then the query engine starts reading from that same directory.

            local_controller_dir = os.path.join(
                local_experiment_root_dir, "controller_output"
            )
            remote_controller_dir = os.path.join(
                experiment_root_output_dir, "controller_output"
            )
            os.makedirs(local_controller_dir, exist_ok=True)

            # Generate and rsync the planner input config to the node
            planner_input_yaml = config.generate_sql_planner_input(
                ep.query_groups, dataset_cfg
            )
            local_planner_input = os.path.join(
                local_controller_dir, "planner_input.yaml"
            )
            with open(local_planner_input, "w") as _f:
                _f.write(planner_input_yaml)
            sync.rsync_streaming_configs(
                provider, local_controller_dir, remote_controller_dir, node_offset
            )

            # Run asap-planner (SQL mode) — writes streaming_config.yaml + inference_config.yaml
            controller_service = ControllerService(
                provider=provider, use_container=False, node_offset=node_offset
            )
            controller_service.start(
                controller_input_file=os.path.join(
                    remote_controller_dir, "planner_input.yaml"
                ),
                streaming_engine="precompute",
                controller_remote_output_dir=remote_controller_dir,
                punting=False,
                query_language="sql",
                data_ingestion_interval=data_ingestion_interval,
            )
            sync.rsync_controller_config_remote_to_local(
                provider, remote_controller_dir, local_controller_dir, node_offset
            )

            # Start query engine (precompute + JSON ingest + ClickHouse fallback)
            query_engine_service = QueryEngineRustService(
                provider=provider,
                use_container=False,
                node_offset=node_offset,
            )
            dataset_precompute_cfg = dataset_cfg.precompute
            query_engine_service.start(
                experiment_output_dir=experiment_output_dir,
                local_experiment_dir=local_experiment_dir,
                flink_output_format="json",
                prometheus_scrape_interval=15,
                log_level="INFO",
                profile_query_engine=False,
                manual=False,
                streaming_engine="precompute",
                controller_remote_output_dir=remote_controller_dir,
                compress_json=False,
                dump_precomputes=False,
                lock_strategy="per-key",
                backend_config={
                    "type": "clickhouse",
                    "url": clickhouse_url,
                    "database": CLICKHOUSE_DATABASE,
                    "forward_unsupported_queries": True,
                },
                http_port=asap_http_port,
                ingest_json_config={
                    "path": remote_data_file,
                    "metric_name": str(dataset_cfg.metric_name),
                    "value_col": str(dataset_precompute_cfg.value_col),
                    "label_cols": list(dataset_precompute_cfg.label_cols),
                    "timestamp_col": str(dataset_precompute_cfg.timestamp_col),
                    "timestamp_unit": "seconds",
                    "batch_size": 1000,
                },
            )

            query_engine_service.wait_until_ready()

            steady_state_wait = int(cfg.flow.steady_state_wait)
            print(f"Waiting {steady_state_wait}s for precompute ingest to complete...")
            time.sleep(steady_state_wait)

            controller_client_config = os.path.join(
                experiment_root_output_dir,
                "controller_client_configs",
                f"{experiment_mode}.yaml",
            )
            _run_query_client(
                provider=provider,
                node_offset=node_offset,
                config_file=controller_client_config,
                output_dir=os.path.join(
                    experiment_output_dir, "prometheus_client_output"
                ),
                use_container=use_container,
                parallel=parallel,
            )

            sync.rsync_experiment_data(
                provider,
                experiment_output_dir,
                local_experiment_dir,
                node_offset=node_offset,
            )

            if not no_teardown:
                query_engine_service.stop()

        else:
            # --- baseline mode ---
            controller_client_config = os.path.join(
                experiment_root_output_dir,
                "controller_client_configs",
                f"{experiment_mode}.yaml",
            )
            _run_query_client(
                provider=provider,
                node_offset=node_offset,
                config_file=controller_client_config,
                output_dir=os.path.join(
                    experiment_output_dir, "prometheus_client_output"
                ),
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

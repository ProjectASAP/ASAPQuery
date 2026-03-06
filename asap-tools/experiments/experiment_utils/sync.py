"""
File synchronization and data management utilities for experiments.
Contains functions for syncing data between local and remote machines.
"""

import os

import utils
import constants
from .providers.base import InfrastructureProvider


def copy_prometheus_data(
    provider: InfrastructureProvider, experiment_name: str, node_offset: int
):
    """Copy Prometheus data from remote to local machine."""
    remote_prometheus_home_dir = os.path.join(provider.get_home_dir(), "prometheus")
    data_to_copy = [
        f"{remote_prometheus_home_dir}/data",
        # f"{remote_prometheus_home_dir}/queries.log",
    ]
    local_destination_dir = os.path.join(
        constants.LOCAL_EXPERIMENT_DIR, experiment_name, "prometheus_data"
    )
    os.makedirs(local_destination_dir, exist_ok=True)

    for data_path in data_to_copy:
        cmd = f'rsync -azh -e "ssh {constants.SSH_OPTIONS}" {provider.username}@node{node_offset}.{provider.hostname_suffix}:{data_path} {local_destination_dir}/'
        utils.run_cmd_with_retry(cmd, popen=False, ignore_errors=False)


def rsync_experiment_data(
    provider: InfrastructureProvider,
    experiment_output_dir: str,
    local_experiment_dir: str,
    node_offset: int,
):
    """Sync experiment data from remote to local machine."""
    cmd = 'rsync -azh -e "ssh {}" {}@node{}.{}:{}/ {}/'.format(
        constants.SSH_OPTIONS,
        provider.username,
        node_offset,
        provider.hostname_suffix,
        experiment_output_dir,
        local_experiment_dir,
    )
    utils.run_cmd_with_retry(cmd, popen=False, ignore_errors=False)


def rsync_prometheus_config(
    provider: InfrastructureProvider,
    experiment_output_dir: str,
    prometheus_config_output_dir: str,
    node_offset: int,
):
    """
    Sync Prometheus configuration directory to remote machine.

    Syncs the entire prometheus_config/ directory which contains:
    - prometheus.yml (Prometheus config)
    - vmagent_scrape.yml (VictoriaMetrics scrape config)
    - vmagent_remote_write.yml (VictoriaMetrics remote write config)
    """
    remote_prometheus_dir = os.path.join(
        experiment_output_dir, constants.PROMETHEUS_CONFIG_DIR
    )
    cmd = "mkdir -p {}".format(remote_prometheus_dir)
    provider.execute_command(
        node_idx=node_offset, cmd=cmd, cmd_dir=None, nohup=False, popen=False
    )

    hostname = f"node{node_offset}.{provider.hostname_suffix}"
    # Sync entire directory (note the trailing slash to sync contents)
    cmd = 'rsync -azh -e "ssh {}" {}/ {}@{}:{}/'.format(
        constants.SSH_OPTIONS,
        prometheus_config_output_dir,
        provider.username,
        hostname,
        remote_prometheus_dir,
    )
    utils.run_cmd_with_retry(cmd, popen=False, ignore_errors=False)


def rsync_controller_client_configs(
    provider: InfrastructureProvider,
    experiment_output_dir: str,
    local_experiment_dir: str,
    node_offset: int,
):
    """Sync controller client configurations to remote machine."""
    hostname = f"node{node_offset}.{provider.hostname_suffix}"
    cmd = 'rsync -azh -e "ssh {}" {} {}@{}:{}/'.format(
        constants.SSH_OPTIONS,
        os.path.join(local_experiment_dir, "controller_client_configs"),
        provider.username,
        hostname,
        os.path.join(experiment_output_dir),
    )
    utils.run_cmd_with_retry(cmd, popen=False, ignore_errors=False)


def rsync_controller_config_remote_to_local(
    provider: InfrastructureProvider,
    controller_remote_output_dir: str,
    controller_local_output_dir: str,
    node_offset: int,
):
    """Sync controller configuration from remote to local machine."""
    hostname = f"node{node_offset}.{provider.hostname_suffix}"
    cmd = 'rsync -azh -e "ssh {}" {}@{}:{}/ {}/'.format(
        constants.SSH_OPTIONS,
        provider.username,
        hostname,
        controller_remote_output_dir,
        controller_local_output_dir,
    )
    utils.run_cmd_with_retry(cmd, popen=False, ignore_errors=False)


def copy_experiment_config(experiment_params, local_experiment_dir: str):
    """Save the experiment config to local directory for reference."""
    os.makedirs(os.path.join(local_experiment_dir, "experiment_config"), exist_ok=True)

    # Handle both file paths and DictConfig objects
    if hasattr(experiment_params, "__dict__") or hasattr(experiment_params, "_content"):
        # It's a DictConfig object
        from omegaconf import OmegaConf

        config_file_path = os.path.join(
            local_experiment_dir, "experiment_config", "experiment_params.yaml"
        )
        with open(config_file_path, "w") as f:
            OmegaConf.save(experiment_params, f)
    else:
        # It's a file path
        cmd = "cp {} {}/".format(
            experiment_params, os.path.join(local_experiment_dir, "experiment_config")
        )
        utils.run_cmd_with_retry(cmd, popen=False, ignore_errors=False)

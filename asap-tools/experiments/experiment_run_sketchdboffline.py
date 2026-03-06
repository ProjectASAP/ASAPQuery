import os
import json
import shlex
import subprocess

import hydra
from omegaconf import DictConfig, OmegaConf

import constants
from classes import process_monitor

BINARY_PATH = "/scratch/sketch_db_for_prometheus/code/SketchDBOfflinePOCRust/target/release/SketchDBOfflinePOCRust"
RESOURCES = ["cpu_percent", "memory_info"]

# Register custom resolver for LOCAL_EXPERIMENT_DIR before Hydra processes config
OmegaConf.register_new_resolver(
    "local_experiment_dir", lambda: constants.LOCAL_EXPERIMENT_DIR
)

# Register custom resolver for remote write IP based on node_offset
OmegaConf.register_new_resolver(
    "remote_write_ip", lambda node_offset: f"10.10.1.{node_offset + 1}"
)


def run_sketchdboffline(args, output_dir) -> subprocess.Popen:
    cmd = (
        f"{BINARY_PATH}"
        f" {args.experiment_dir}/prometheus/prometheus_data/exported_data/fake_metric_total.csv"
        f" --labels {','.join(args.labels)}"
        f" --ignore __name__"
        f" --slide-iterations 30"
        f" --slide-time 10"
        f" --slide-range 600"
        f" --groupby {','.join(args.groupby)}"
        f" --output-dir {output_dir}"
        f" --aggregation {args.aggregation}"
    )

    print(cmd)

    # redirect stderr to stdout
    stderr = subprocess.STDOUT
    process = subprocess.Popen(
        shlex.split(cmd), shell=False, stdout=subprocess.PIPE, stderr=stderr
    )
    return process


def validate_config(cfg: DictConfig):
    """
    Validate configuration parameters for sketchdboffline experiment.
    """
    # Check for required parameters that must be provided via command line
    required_params = [
        (
            "experiment_variants.sketchdboffline.experiment_dir",
            "Path to experiment data directory",
        ),
        ("experiment_variants.sketchdboffline.groupby", "List of labels to group by"),
        (
            "experiment_variants.sketchdboffline.aggregation",
            "Aggregation function to apply",
        ),
    ]

    missing_params = []
    for param_path, description in required_params:
        try:
            value = OmegaConf.select(cfg, param_path)
            if value is None or (isinstance(value, str) and value == "???"):
                missing_params.append((param_path, description))
        except Exception:
            missing_params.append((param_path, description))

    if missing_params:
        error_msg = "Required parameters must be provided via command line:\n\n"
        for param_path, description in missing_params:
            error_msg += f"  {param_path}: {description}\n"

        error_msg += "\nExample usage:\n"
        error_msg += "python experiment_run_sketchdboffline.py \\\n"
        error_msg += (
            "  experiment_variants.sketchdboffline.experiment_dir=/path/to/data \\\n"
        )
        error_msg += (
            "  experiment_variants.sketchdboffline.groupby=[label_0,instance] \\\n"
        )
        error_msg += "  experiment_variants.sketchdboffline.aggregation=avg\n"

        raise ValueError(error_msg)


class Args:
    """Helper class to convert Hydra config to argparse-like namespace"""

    def __init__(self, cfg: DictConfig):
        # Offline analysis configuration
        offline_cfg = cfg.experiment_variants.sketchdboffline
        self.experiment_dir = offline_cfg.experiment_dir
        self.labels = offline_cfg.labels
        self.groupby = offline_cfg.groupby
        self.aggregation = offline_cfg.aggregation


def main(args):
    output_dir = os.path.join(
        args.experiment_dir,
        BINARY_PATH.split("/")[-1],
        "groupby_{}".format(".".join(args.groupby)),
    )

    os.makedirs(output_dir, exist_ok=True)
    popen = run_sketchdboffline(args, output_dir)

    monitor, control_pipe, monitor_pipe = process_monitor.start_monitor(
        [popen.pid],
        [BINARY_PATH.split("/")[-1]],  # Use the binary name as the process name
        1,
        RESOURCES,
        include_children=True,
    )

    # wait for the process to finish
    popen.wait()
    # dump the output to a file
    assert popen.stdout is not None, "Process did not return stdout"
    with open(os.path.join(output_dir, "sketchdboffline_output.txt"), "w") as fout:
        for line in iter(popen.stdout.readline, b""):
            fout.write(line.decode("utf-8"))
    monitor_info = process_monitor.stop_monitor(monitor, control_pipe, monitor_pipe)

    assert monitor_info is not None, "Monitor process did not return data"

    with open(os.path.join(output_dir, "monitor_output.json"), "w") as fout:
        json.dump(monitor_info, fout)


@hydra.main(version_base=None, config_path="config", config_name="config")
def hydra_main(cfg: DictConfig):
    # Validate configuration
    validate_config(cfg)

    # Convert config to args-like object for backward compatibility
    args = Args(cfg)

    # Create output directory structure
    output_dir = os.path.join(
        args.experiment_dir,
        BINARY_PATH.split("/")[-1],
        "groupby_{}".format(".".join(args.groupby)),
    )
    os.makedirs(output_dir, exist_ok=True)

    # Dump config to a file
    with open(os.path.join(output_dir, "hydra_config.yaml"), "w") as f:
        OmegaConf.save(cfg, f)

    # Also dump args to a file for backward compatibility
    with open(os.path.join(output_dir, "cmdline_args.txt"), "w") as f:
        json.dump(vars(args), f)

    print(f"Running sketchdboffline experiment with aggregation: {args.aggregation}")
    print(f"Group by: {args.groupby}")
    print(f"Output directory: {output_dir}")
    main(args)


if __name__ == "__main__":
    hydra_main()

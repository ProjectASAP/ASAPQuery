#!/usr/bin/env python3
"""
Script to plot data scale vs cost and latency across multiple experiments.
X-axis: Data scale (metrics/sec) in log scale
Y-axes: Left = Cost (CPU %), Right = Latency (ms)
"""

import argparse
import os
import sys
import json
import subprocess
import yaml
import matplotlib.pyplot as plt
import numpy as np

POST_EXPERIMENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SINGLE_EXPERIMENT_DIR = os.path.join(POST_EXPERIMENT_DIR, "single_experiment")

sys.path.append(os.path.dirname(POST_EXPERIMENT_DIR))
import constants  # noqa: E402

# Configuration
# EXPERIMENT_NAMES = [
#    "non_quantile_1s_4queries_10valuesperlabel_2",
#    "non_quantile_1s_4queries_20valuesperlabel_2",
#    "non_quantile_1s_4queries_30valuesperlabel_2",
#    "non_quantile_1s_4queries_40valuesperlabel_2",
# ]
EXPERIMENT_NAMES = [
    "quantile_1s_10queries_10valuesperlabel_2",
    "quantile_1s_10queries_20valuesperlabel_2",
    "quantile_1s_10queries_30valuesperlabel_2",
    "quantile_1s_10queries_40valuesperlabel_2",
    #    "quantile_1s_10queries_50valuesperlabel_2",
]
# EXPERIMENT_NAMES = [
#    "quantile_1s_10queries_20valuesperlabel_2labels",
#    "quantile_1s_10queries_40valuesperlabel_2labels",
#    "quantile_1s_10queries_60valuesperlabel_2labels",
#    "quantile_1s_10queries_80valuesperlabel_2labels",
#    "quantile_1s_10queries_100valuesperlabel_2labels",
# ]

FONTSIZE = 20


def calculate_data_scale(experiment_name):
    """
    Calculate data scale (metrics/sec) from experiment config.
    Formula: num_ports_per_server * (num_labels ^ num_values_per_label)

    Args:
        experiment_name: Name of the experiment

    Returns:
        Data scale in metrics/sec, or None if config not found
    """
    experiment_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, experiment_name)
    config_file = os.path.join(
        experiment_dir, "experiment_config", "experiment_params.yaml"
    )

    if not os.path.exists(config_file):
        print(f"Warning: Config file not found for {experiment_name}: {config_file}")
        return None

    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        # Extract fake_exporter parameters
        fake_exporter = config["exporters"]["exporter_list"]["fake_exporter"]
        num_ports = fake_exporter["num_ports_per_server"]
        num_labels = fake_exporter["num_labels"]
        num_values_per_label = fake_exporter["num_values_per_label"]

        # Calculate data scale
        data_scale = num_ports * (num_values_per_label**num_labels)

        return data_scale

    except Exception as e:
        print(f"Error parsing config for {experiment_name}: {e}")
        return None


def _run_json(script, args):
    """Run a single_experiment script with --machine-readable and parse its JSON.

    Returns None if the script fails (e.g. missing experiment); a missing JSON
    key raises instead, so a schema change can't silently drop data.
    """
    try:
        result = subprocess.run(
            ["python3", os.path.join(SINGLE_EXPERIMENT_DIR, script)]
            + args
            + ["--machine-readable"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error running {script} {args}: {e.stderr.strip()[-300:]}")
        return None
    return json.loads(result.stdout)


def _compare_costs(experiment_name, experiment_mode):
    return _run_json(
        "compare_costs.py",
        [
            "--experiment_name",
            experiment_name,
            "--experiment_mode",
            experiment_mode,
            "--print",
        ],
    )


def get_latency_p95(experiment_name, experiment_mode="baseline"):
    """p95 latency pooled over all queries (baseline = exact, sketchdb = estimate)."""
    data = _run_json(
        "compare_latencies.py",
        [
            "--experiment_name",
            experiment_name,
            "--exact_experiment_mode",
            "baseline",
            "--estimate_experiment_mode",
            "sketchdb",
        ],
    )
    if data is None:
        return None
    side = "exact" if experiment_mode == "baseline" else "estimate"
    return data["results"]["-1"][side]["p95"]


def get_cost_p95(experiment_name, experiment_mode="baseline"):
    """p95 of total CPU % (sum over all monitored processes: ingest + query)."""
    data = _compare_costs(experiment_name, experiment_mode)
    if data is None:
        return None
    return data["experiment_modes"][experiment_mode]["processes"]["all_all"][
        "cpu_percent"
    ]["p95"]


def get_query_cost_95(experiment_name, experiment_mode="baseline"):
    """p95 of query CPU % (see compare_costs.calculate_query_cpu)."""
    data = _compare_costs(experiment_name, experiment_mode)
    if data is None:
        return None
    return data["query_cpu"][experiment_mode]["p95"]


def get_query_cost_sum(experiment_name, experiment_mode="baseline"):
    """Sum of query CPU % over the run; depends on run length."""
    data = _compare_costs(experiment_name, experiment_mode)
    if data is None:
        return None
    return data["query_cpu"][experiment_mode]["sum"]


def cost_label_for(use_query_cost_sum=False, use_query_cost_95=False):
    if use_query_cost_sum:
        return "Query CPU sum (%)"
    if use_query_cost_95:
        return "Query CPU p95 (%)"
    return "Total CPU p95 (%)"


def print_data_summary(experiments, data_scales, latencies, costs, cost_label):
    """Print summary of the data."""
    cost_json_key = cost_label
    print("\nData Summary:")
    print("=" * 100)
    print(
        f"{'Experiment':<50} {'Data Scale':<20} {'Latency P95 (s)':<20} {cost_label:<20}"
    )
    print("-" * 100)

    for exp, scale, lat, cost in zip(experiments, data_scales, latencies, costs):
        scale_str = f"{scale:.2e}" if scale is not None else "N/A"
        lat_str = f"{lat:.4f}" if lat is not None else "N/A"
        cost_str = f"{cost:.2f}" if cost is not None else "N/A"
        print(f"{exp:<50} {scale_str:<20} {lat_str:<20} {cost_str:<20}")

    print("=" * 100)

    # Print json-like structure also
    print("\nJSON-like Data Structure:")
    data_list = []
    for exp, scale, lat, cost in zip(experiments, data_scales, latencies, costs):
        data_list.append(
            {
                "experiment": exp,
                "data_scale_metrics_per_sec": scale,
                "latency_p95_seconds": lat,
                cost_json_key: cost,
            }
        )

    print(json.dumps(data_list, indent=4))


def plot_scale_vs_metrics(
    experiments,
    data_scales,
    latencies,
    costs,
    cost_label,
    save_file=None,
    show=False,
):
    """
    Plot data scale vs cost and latency.

    Args:
        experiments: List of experiment names
        data_scales: List of data scale values (metrics/sec)
        latencies: List of p95 latency values (seconds)
        costs: List of CPU cost values (%)
        cost_label: Axis label naming the cost definition (see cost_label_for)
        save_file: Filename to save the plot (if None, doesn't save)
        show: Whether to display the plot

    Returns:
        matplotlib figure object
    """
    # Filter out None values and sort by data scale
    valid_data = [
        (s, l, c, e)
        for s, l, c, e in zip(data_scales, latencies, costs, experiments)
        if s is not None and l is not None and c is not None
    ]

    if not valid_data:
        print("Error: No valid data points to plot")
        return None

    valid_data.sort(key=lambda x: x[0])  # Sort by data scale
    data_scales_sorted, latencies_sorted, costs_sorted, experiments_sorted = zip(
        *valid_data
    )

    # Convert to numpy arrays
    data_scales_arr = np.array(data_scales_sorted)
    # latencies_arr = np.array(latencies_sorted) * 1000  # Convert to milliseconds
    latencies_arr = np.array(latencies_sorted)  # Keep as seconds
    costs_arr = np.array(costs_sorted)

    # Create the plot with two y-axes
    fig, ax1 = plt.subplots(figsize=(12, 6))

    cost_ylabel = cost_legend = cost_label

    # Plot cost on left y-axis
    color_cost = "#1f77b4"
    ax1.set_xlabel("Data Scale (metrics/sec)", fontsize=FONTSIZE, fontweight="bold")
    ax1.set_ylabel(cost_ylabel, fontsize=FONTSIZE, fontweight="bold", color=color_cost)
    line1 = ax1.plot(
        data_scales_arr,
        costs_arr,
        "o-",
        color=color_cost,
        linewidth=2,
        markersize=8,
        label=cost_legend,
    )
    ax1.tick_params(axis="y", labelcolor=color_cost, labelsize=FONTSIZE)
    ax1.tick_params(axis="x", labelsize=FONTSIZE)
    ax1.set_xscale("log")
    ax1.grid(True, alpha=0.3, which="both")

    # Create second y-axis for latency
    ax2 = ax1.twinx()
    color_latency = "#ff7f0e"
    # ax2.set_ylabel('Latency (ms, p95)', fontsize=FONTSIZE, fontweight='bold', color=color_latency)
    ax2.set_ylabel(
        "p95 Latency (s)", fontsize=FONTSIZE, fontweight="bold", color=color_latency
    )
    line2 = ax2.plot(
        data_scales_arr,
        latencies_arr,
        "s-",
        color=color_latency,
        linewidth=2,
        markersize=8,
        label="p95 Latency (s)",
    )
    ax2.tick_params(axis="y", labelcolor=color_latency, labelsize=FONTSIZE)

    # Add title
    plt.title(
        "Data Scale vs Cost and Latency",
        fontsize=FONTSIZE + 2,
        fontweight="bold",
        pad=20,
    )

    # Add legend
    lines = line1 + line2
    labels = [line_foo.get_label() for line_foo in lines]
    ax1.legend(lines, labels, loc="upper left", fontsize=FONTSIZE)

    # Adjust layout
    fig.tight_layout()

    # Save if requested
    if save_file:
        plt.savefig(save_file, dpi=300, bbox_inches="tight")
        print(f"Plot saved as '{save_file}'")

    # Show if requested
    if show:
        plt.show()
    else:
        plt.close(fig)

    return fig


def main():
    parser = argparse.ArgumentParser(
        description="Plot data scale vs cost and latency across experiments",
        epilog="""
Examples:
  # Print data summary only
  python3 plot_scale_vs_metrics.py --print

  # Plot and save to file
  python3 plot_scale_vs_metrics.py --plot --save scale_metrics.png

  # Plot and show interactively
  python3 plot_scale_vs_metrics.py --plot --show

  # Both print and plot
  python3 plot_scale_vs_metrics.py --print --plot --save output.png --show

  # Use query cost sum instead of p95 cost
  python3 plot_scale_vs_metrics.py --print --use-query-cost-sum
  python3 plot_scale_vs_metrics.py --plot --save query_cost.png --use-query-cost-sum
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--print", action="store_true", help="Print data summary")
    parser.add_argument("--plot", action="store_true", help="Generate plot")
    parser.add_argument(
        "--save",
        type=str,
        metavar="FILENAME",
        help="Save plot to file (provide filename)",
    )
    parser.add_argument("--show", action="store_true", help="Display plot")
    parser.add_argument(
        "--use-query-cost-sum",
        action="store_true",
        help="Use query CPU sum instead of total CPU p95",
    )
    parser.add_argument(
        "--use-query-cost-95",
        action="store_true",
        help="Use query CPU p95 instead of total CPU p95",
    )
    parser.add_argument(
        "--experiment_mode",
        type=str,
        choices=["baseline", "sketchdb"],
        default="baseline",
        help="Experiment mode (baseline or sketchdb)",
    )

    args = parser.parse_args()
    cost_label = cost_label_for(args.use_query_cost_sum, args.use_query_cost_95)

    # Validate arguments
    if args.plot and not (args.save or args.show):
        parser.error("--plot requires either --save or --show (or both)")

    if not args.print and not args.plot:
        parser.error("At least one of --print or --plot must be specified")

    # Collect data for all experiments
    print(f"Processing {len(EXPERIMENT_NAMES)} experiments...")

    data_scales = []
    latencies = []
    costs = []

    for exp_name in EXPERIMENT_NAMES:
        print(f"\nProcessing: {exp_name}")

        # Calculate data scale
        scale = calculate_data_scale(exp_name)
        data_scales.append(scale)
        if scale is not None:
            print(f"  Data scale: {scale:.2e} metrics/sec")

        # Get latency p95
        latency = get_latency_p95(exp_name, args.experiment_mode)
        latencies.append(latency)
        if latency is not None:
            print(f"  Latency p95: {latency:.4f} seconds")

        if args.use_query_cost_sum:
            cost = get_query_cost_sum(exp_name, args.experiment_mode)
        elif args.use_query_cost_95:
            cost = get_query_cost_95(exp_name, args.experiment_mode)
        else:
            cost = get_cost_p95(exp_name, args.experiment_mode)
        if cost is not None:
            print(f"  {cost_label}: {cost:.2f}")
        costs.append(cost)

    # Print summary if requested
    if args.print:
        print_data_summary(EXPERIMENT_NAMES, data_scales, latencies, costs, cost_label)

    # Generate plot if requested
    if args.plot:
        plot_scale_vs_metrics(
            experiments=EXPERIMENT_NAMES,
            data_scales=data_scales,
            latencies=latencies,
            costs=costs,
            cost_label=cost_label,
            save_file=args.save,
            show=args.show,
        )

    return 0


if __name__ == "__main__":
    exit(main())

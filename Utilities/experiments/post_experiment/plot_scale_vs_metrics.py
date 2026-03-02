#!/usr/bin/env python3
"""
Script to plot data scale vs cost and latency across multiple experiments.
X-axis: Data scale (metrics/sec) in log scale
Y-axes: Left = Cost (CPU %), Right = Latency (ms)
"""

import argparse
import os
import sys
import re
import json
import subprocess
import yaml
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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


def get_latency_p95(experiment_name):
    """
    Get p95 latency by running ./run_compare_latencies.sh

    Args:
        experiment_name: Name of the experiment

    Returns:
        p95 latency value (exact), or None if failed
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, "run_compare_latencies.sh")

    try:
        result = subprocess.run(
            [script_path, experiment_name],
            capture_output=True,
            text=True,
            check=True,
            cwd=script_dir,
        )

        # Parse output to extract p95 from exact
        # Looking for: exact: {'median': X, 'p95': Y, ...}
        output = result.stdout + result.stderr

        # Find the "exact:" line
        exact_match = re.search(r"exact:\s*\{([^}]+)\}", output)
        if exact_match:
            exact_dict_str = exact_match.group(1)
            # Extract p95 value
            p95_match = re.search(r"'p95':\s*([\d.]+)", exact_dict_str)
            if p95_match:
                return float(p95_match.group(1))

        print(f"Warning: Could not parse p95 latency from output for {experiment_name}")
        return None

    except subprocess.CalledProcessError as e:
        print(f"Error running latency comparison for {experiment_name}: {e}")
        return None
    except Exception as e:
        print(f"Error getting latency for {experiment_name}: {e}")
        return None


def get_cost_p95(experiment_name):
    """
    Get p95 CPU cost by running compare_costs.py

    Args:
        experiment_name: Name of the experiment

    Returns:
        p95 CPU percentage, or None if failed
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    compare_costs_path = os.path.join(script_dir, "compare_costs.py")

    try:
        result = subprocess.run(
            [
                "python3",
                compare_costs_path,
                "--experiment_name",
                experiment_name,
                "--experiment_mode",
                "baseline",
                "--print",
            ],
            capture_output=True,
            text=True,
            check=True,
            cwd=script_dir,
        )

        # Parse output to extract p95 CPU from "prometheus prometheus.yml cpu_percent p95"
        # or "prometheus prometheus cpu_percent p95"
        output = result.stdout + result.stderr

        # Look for lines matching the pattern
        for line in output.split("\n"):
            if re.search(
                r"prometheus\s+prometheus.*cpu_percent\s+p95\s+([\d.]+)", line
            ):
                match = re.search(
                    r"prometheus\s+prometheus.*cpu_percent\s+p95\s+([\d.]+)", line
                )
                if match:
                    return float(match.group(1))

        print(
            f"Warning: Could not parse p95 CPU cost from output for {experiment_name}"
        )
        return None

    except subprocess.CalledProcessError as e:
        print(f"Error running cost comparison for {experiment_name}: {e}")
        return None
    except Exception as e:
        print(f"Error getting cost for {experiment_name}: {e}")
        return None


def get_query_cost_95(experiment_name):
    """
    Get query CPU cost p95 by running compare_costs.py

    Args:
        experiment_name: Name of the experiment

    Returns:
        Query CPU cost p95 percentage, or None if failed
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    compare_costs_path = os.path.join(script_dir, "compare_costs.py")

    try:
        result = subprocess.run(
            [
                "python3",
                compare_costs_path,
                "--experiment_name",
                experiment_name,
                "--experiment_mode",
                "baseline",
                "--print",
            ],
            capture_output=True,
            text=True,
            check=True,
            cwd=script_dir,
        )

        # Parse output to extract query CPU sum from "Query CPU Statistics" section
        # Looking for pattern like:
        # prometheus:
        #   p95: 1122837.55%
        output = result.stdout + result.stderr

        # Look for the Query CPU Statistics section
        in_query_section = False
        in_prometheus_subsection = False
        for line in output.split("\n"):
            if "Query CPU Statistics" in line:
                in_query_section = True
                continue

            if in_query_section:
                # Check if we're in the prometheus subsection
                if line.strip().startswith("prometheus:"):
                    in_prometheus_subsection = True
                    continue

                # If we're in prometheus subsection, look for sum
                if in_prometheus_subsection:
                    match = re.search(r"p95:\s+([\d.]+)%", line)
                    if match:
                        return float(match.group(1))
                    # If we hit another section, stop
                    if line.strip() and not line.strip().startswith(
                        ("sum:", "max:", "median:", "p95:", "p99:")
                    ):
                        break

        print(
            f"Warning: Could not parse query CPU cost sum from output for {experiment_name}"
        )
        return None

    except subprocess.CalledProcessError as e:
        print(f"Error running cost comparison for {experiment_name}: {e}")
        return None
    except Exception as e:
        print(f"Error getting query cost sum for {experiment_name}: {e}")
        return None


def get_query_cost_sum(experiment_name):
    """
    Get query CPU cost sum by running compare_costs.py

    Args:
        experiment_name: Name of the experiment

    Returns:
        Query CPU cost sum percentage, or None if failed
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    compare_costs_path = os.path.join(script_dir, "compare_costs.py")

    try:
        result = subprocess.run(
            [
                "python3",
                compare_costs_path,
                "--experiment_name",
                experiment_name,
                "--experiment_mode",
                "baseline",
                "--print",
            ],
            capture_output=True,
            text=True,
            check=True,
            cwd=script_dir,
        )

        # Parse output to extract query CPU sum from "Query CPU Statistics" section
        # Looking for pattern like:
        # prometheus:
        #   sum: 1122837.55%
        output = result.stdout + result.stderr

        # Look for the Query CPU Statistics section
        in_query_section = False
        in_prometheus_subsection = False
        for line in output.split("\n"):
            if "Query CPU Statistics" in line:
                in_query_section = True
                continue

            if in_query_section:
                # Check if we're in the prometheus subsection
                if line.strip().startswith("prometheus:"):
                    in_prometheus_subsection = True
                    continue

                # If we're in prometheus subsection, look for sum
                if in_prometheus_subsection:
                    match = re.search(r"sum:\s+([\d.]+)%", line)
                    if match:
                        return float(match.group(1))
                    # If we hit another section, stop
                    if line.strip() and not line.strip().startswith(
                        ("sum:", "max:", "median:", "p95:", "p99:")
                    ):
                        break

        print(
            f"Warning: Could not parse query CPU cost sum from output for {experiment_name}"
        )
        return None

    except subprocess.CalledProcessError as e:
        print(f"Error running cost comparison for {experiment_name}: {e}")
        return None
    except Exception as e:
        print(f"Error getting query cost sum for {experiment_name}: {e}")
        return None


def print_data_summary(
    experiments, data_scales, latencies, costs, use_query_cost_sum=False
):
    """Print summary of the data."""
    cost_label = "Query Cost Sum (CPU %)" if use_query_cost_sum else "Cost P95 (CPU %)"
    cost_json_key = (
        "query_cost_sum_cpu_percent" if use_query_cost_sum else "cost_p95_cpu_percent"
    )

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
    save_file=None,
    show=False,
    use_query_cost_sum=False,
):
    """
    Plot data scale vs cost and latency.

    Args:
        experiments: List of experiment names
        data_scales: List of data scale values (metrics/sec)
        latencies: List of p95 latency values (seconds)
        costs: List of CPU cost values (% - either p95 or query sum)
        save_file: Filename to save the plot (if None, doesn't save)
        show: Whether to display the plot
        use_query_cost_sum: Whether cost values represent query cost sum instead of p95

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

    # Determine cost label based on type
    cost_ylabel = (
        "Query Cost (CPU %, sum)" if use_query_cost_sum else "p95 CPU usage (%)"
    )
    cost_legend = "Query Cost (CPU %)" if use_query_cost_sum else "p95 CPU Usage (%)"

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
        help="Use query CPU cost sum instead of p95 CPU cost",
    )
    parser.add_argument(
        "--use-query-cost-95",
        action="store_true",
        help="Use query CPU cost p95 instead of p95 CPU cost",
    )

    args = parser.parse_args()

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
        latency = get_latency_p95(exp_name)
        latencies.append(latency)
        if latency is not None:
            print(f"  Latency p95: {latency:.4f} seconds")

        # Get cost (either p95 or query cost sum based on flag)
        if args.use_query_cost_sum:
            cost = get_query_cost_sum(exp_name)
            if cost is not None:
                print(f"  Query cost sum: {cost:.2f} CPU %")
        elif args.use_query_cost_95:
            cost = get_query_cost_95(exp_name)
            if cost is not None:
                print(f"  Query cost p95: {cost:.2f} CPU %")
        else:
            cost = get_cost_p95(exp_name)
            if cost is not None:
                print(f"  Cost p95: {cost:.2f} CPU %")
        costs.append(cost)

    # Print summary if requested
    if args.print:
        print_data_summary(
            EXPERIMENT_NAMES,
            data_scales,
            latencies,
            costs,
            use_query_cost_sum=args.use_query_cost_sum,
        )

    # Generate plot if requested
    if args.plot:
        plot_scale_vs_metrics(
            experiments=EXPERIMENT_NAMES,
            data_scales=data_scales,
            latencies=latencies,
            costs=costs,
            save_file=args.save,
            show=args.show,
            use_query_cost_sum=args.use_query_cost_sum,
        )

    return 0


if __name__ == "__main__":
    exit(main())

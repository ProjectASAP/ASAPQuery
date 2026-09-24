import os
import sys
import json
import glob
import argparse
import subprocess
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, Tuple

POST_EXPERIMENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SINGLE_EXPERIMENT_DIR = os.path.join(POST_EXPERIMENT_DIR, "single_experiment")

sys.path.append(os.path.dirname(POST_EXPERIMENT_DIR))
import constants  # noqa: E402


def run_compare_costs(experiment_name: str) -> Dict:
    """Run compare_costs.py with machine-readable output."""
    script_path = os.path.join(SINGLE_EXPERIMENT_DIR, "compare_costs.py")

    cmd = [
        "python3",
        script_path,
        "--experiment_name",
        experiment_name,
        "--all_experiment_modes",
        "--print",
        "--machine-readable",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"compare_costs.py failed for {experiment_name}: {result.stderr}"
        )

    return json.loads(result.stdout)


def run_compare_latencies(
    experiment_name: str, exact_mode: str, estimate_mode: str
) -> Dict:
    """Run compare_latencies.py with machine-readable output."""
    script_path = os.path.join(SINGLE_EXPERIMENT_DIR, "compare_latencies.py")

    cmd = [
        "python3",
        script_path,
        "--experiment_name",
        experiment_name,
        "--exact_experiment_mode",
        exact_mode,
        "--estimate_experiment_mode",
        estimate_mode,
        "--machine-readable",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"compare_latencies.py failed for {experiment_name}: {result.stderr}"
        )

    return json.loads(result.stdout)


def extract_metrics(
    experiment_name: str,
    latency_metric: str,
    cost_metric: str,
    exact_mode: str,
    estimate_mode: str,
) -> Tuple[float, float, float, float, float, float]:
    """Extract latency and cost metrics for both modes.

    Returns:
        (exact_latency, exact_cost, estimate_latency, estimate_cost,
         exact_total_cpu, estimate_total_cpu)
    """
    # Get cost data
    cost_data = run_compare_costs(experiment_name)

    if "query_cpu" not in cost_data:
        raise ValueError(f"No query_cpu data found for {experiment_name}")

    if exact_mode not in cost_data["query_cpu"]:
        raise ValueError(
            f"Mode {exact_mode} not found in query_cpu data for {experiment_name}"
        )
    if estimate_mode not in cost_data["query_cpu"]:
        raise ValueError(
            f"Mode {estimate_mode} not found in query_cpu data for {experiment_name}"
        )

    exact_cost = cost_data["query_cpu"][exact_mode][cost_metric]
    estimate_cost = cost_data["query_cpu"][estimate_mode][cost_metric]

    # Total CPU across all monitored processes ("all" pseudo-process in compare_costs.py)
    def total_cpu(mode):
        return cost_data["experiment_modes"][mode]["processes"]["all_all"][
            "cpu_percent"
        ][cost_metric]

    exact_total_cpu = total_cpu(exact_mode)
    estimate_total_cpu = total_cpu(estimate_mode)

    # Get latency data
    latency_data = run_compare_latencies(experiment_name, exact_mode, estimate_mode)

    if "results" not in latency_data:
        raise ValueError(f"No results found for {experiment_name}")

    # Use aggregate results (key "-1" as string since JSON converts int keys to strings)
    if "-1" not in latency_data["results"]:
        raise ValueError(f"No aggregate results found for {experiment_name}")

    exact_latency = latency_data["results"]["-1"]["exact"][latency_metric]
    estimate_latency = latency_data["results"]["-1"]["estimate"][latency_metric]

    return (
        exact_latency,
        exact_cost,
        estimate_latency,
        estimate_cost,
        exact_total_cpu,
        estimate_total_cpu,
    )


def plot_latency_cost_tradeoff(
    data_points: Dict[str, Tuple[float, float, float, float]],
    latency_metric: str,
    cost_metric: str,
    args,
):
    """Plot latency-cost tradeoff.

    Args:
        data_points: Dict mapping experiment_name to (exact_latency, exact_cost, estimate_latency, estimate_cost)
        latency_metric: Name of latency metric (e.g., "median")
        cost_metric: Name of cost metric (e.g., "p99")
        args: Command-line arguments
    """
    plt.rcParams.update({"font.size": 24})

    fig, ax = plt.subplots(figsize=(12, 8))

    prometheus_latencies = []
    prometheus_costs = []
    turboprom_latencies = []
    turboprom_costs = []
    experiment_names = []

    for exp_name, (exact_lat, exact_cost, est_lat, est_cost) in data_points.items():
        prometheus_latencies.append(exact_lat)
        prometheus_costs.append(exact_cost)
        turboprom_latencies.append(est_lat)
        turboprom_costs.append(est_cost)
        experiment_names.append(exp_name)

    # Plot prometheus points
    ax.scatter(
        prometheus_latencies,
        prometheus_costs,
        color="red",
        marker="o",
        s=100,
        alpha=0.6,
        label="Prometheus",
    )

    # Plot turboprom points
    ax.scatter(
        turboprom_latencies,
        turboprom_costs,
        color="blue",
        marker="s",
        s=100,
        alpha=0.6,
        label="ASAPOlly",
    )

    # Optionally label points with experiment names
    if args.label_points:
        for i, exp_name in enumerate(experiment_names):
            # Label prometheus point
            ax.annotate(
                exp_name,
                (prometheus_latencies[i], prometheus_costs[i]),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8,
                alpha=0.7,
            )

    # Calculate and draw benefit arrows
    median_prom_latency = np.median(prometheus_latencies)
    median_prom_cost = np.median(prometheus_costs)
    median_turbo_latency = np.median(turboprom_latencies)
    median_turbo_cost = np.median(turboprom_costs)

    latency_benefit = median_prom_latency / median_turbo_latency
    cost_benefit = median_prom_cost / median_turbo_cost

    # Draw horizontal arrow for latency benefit
    # Position it at a Y coordinate between the minimum and median cost
    min_cost = min(min(prometheus_costs), min(turboprom_costs))
    max_cost = max(max(prometheus_costs), max(turboprom_costs))
    arrow_y_latency = min_cost + (max_cost - min_cost) * 0.15
    ax.annotate(
        "",
        xy=(median_turbo_latency, arrow_y_latency),
        xytext=(median_prom_latency, arrow_y_latency),
        arrowprops=dict(
            arrowstyle="<->", color="green", lw=2.5, alpha=0.8, shrinkA=0, shrinkB=0
        ),
    )
    # Label the latency benefit arrow
    ax.text(
        (median_prom_latency + median_turbo_latency) / 2,
        arrow_y_latency,
        f"{latency_benefit:.1f}×",
        ha="center",
        va="top",
        fontsize=24,
        fontweight="bold",
        color="green",
        bbox=dict(
            boxstyle="round,pad=0.5", facecolor="white", edgecolor="green", alpha=0.8
        ),
    )

    # Draw vertical arrow for cost benefit
    # Position it at an X coordinate between the minimum and median latency
    min_latency = min(min(prometheus_latencies), min(turboprom_latencies))
    max_latency = max(max(prometheus_latencies), max(turboprom_latencies))
    arrow_x_cost = min_latency + (max_latency - min_latency) * 0.15
    ax.annotate(
        "",
        xy=(arrow_x_cost, median_turbo_cost),
        xytext=(arrow_x_cost, median_prom_cost),
        arrowprops=dict(
            arrowstyle="<->", color="purple", lw=2.5, alpha=0.8, shrinkA=0, shrinkB=0
        ),
    )
    # Label the cost benefit arrow
    ax.text(
        arrow_x_cost,
        (median_prom_cost + median_turbo_cost) / 2,
        f"{cost_benefit:.1f}×",
        ha="right",
        va="center",
        fontsize=24,
        fontweight="bold",
        color="purple",
        bbox=dict(
            boxstyle="round,pad=0.5", facecolor="white", edgecolor="purple", alpha=0.8
        ),
    )

    ax.set_xlabel(f"Latency ({latency_metric}) [s]")
    ax.set_ylabel(f"{args.cpu_type.capitalize()} CPU Cost ({cost_metric}) [%]")
    ax.set_title("Latency-Cost Tradeoff: Prometheus vs ASAPOlly")
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Save or show
    if args.save:
        output_path = args.output_file
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Saved plot to {output_path}")

    if args.show:
        plt.show()
    else:
        plt.close()


def main(args):
    if not args.show and not args.save:
        raise ValueError("Must specify either --show or --save")

    # Find matching experiment directories
    experiment_dirs = glob.glob(
        os.path.join(constants.LOCAL_EXPERIMENT_DIR, args.experiment_glob)
    )

    if not experiment_dirs:
        raise ValueError(f"No experiments found matching glob: {args.experiment_glob}")

    # Extract experiment names
    experiment_names = [os.path.basename(exp_dir) for exp_dir in experiment_dirs]

    print(
        f"Found {len(experiment_names)} experiments matching glob: {args.experiment_glob}"
    )
    print(f"Experiments: {experiment_names}")

    # Collect data for each experiment
    data_points = {}
    failed_experiments = []

    for exp_name in experiment_names:
        try:
            print(f"\nProcessing experiment: {exp_name}")
            (
                exact_lat,
                exact_cost,
                est_lat,
                est_cost,
                exact_total,
                est_total,
            ) = extract_metrics(
                exp_name,
                args.latency_metric,
                args.cost_metric,
                args.exact_mode,
                args.estimate_mode,
            )
            if args.cpu_type == "total":
                exact_cost, est_cost = exact_total, est_total
            data_points[exp_name] = (exact_lat, exact_cost, est_lat, est_cost)
            print(
                f"  Prometheus: latency={exact_lat:.2f}s, {args.cpu_type}_cpu={exact_cost:.2f}%"
            )
            print(
                f"  TurboProm: latency={est_lat:.2f}s, {args.cpu_type}_cpu={est_cost:.2f}%"
            )
        except Exception as e:
            print(f"  Failed to process {exp_name}: {e}")
            failed_experiments.append(exp_name)

    if not data_points:
        raise ValueError("No valid data points collected")

    if failed_experiments:
        print(f"\nWarning: Failed to process {len(failed_experiments)} experiments:")
        for exp in failed_experiments:
            print(f"  - {exp}")

    # Plot the data
    plot_latency_cost_tradeoff(data_points, args.latency_metric, args.cost_metric, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Plot latency-cost tradeoff for multiple experiments"
    )
    parser.add_argument(
        "--experiment_glob",
        type=str,
        required=True,
        help="Glob pattern to match experiment names (e.g., 'quantile_*')",
    )
    parser.add_argument(
        "--latency_metric",
        type=str,
        default="median",
        choices=["median", "mean", "p95", "p99"],
        help="Latency metric to use (default: median)",
    )
    parser.add_argument(
        "--cost_metric",
        type=str,
        default="p99",
        choices=["median", "mean", "p95", "p99", "sum"],
        help="Cost metric to use (default: p99)",
    )
    parser.add_argument(
        "--cpu_type",
        type=str,
        required=True,
        choices=["query", "total"],
        help="CPU to print/plot: query-attributed CPU or total CPU across all processes",
    )
    parser.add_argument(
        "--exact_mode",
        type=str,
        default="baseline",
        help="Name of exact/baseline experiment mode (default: baseline)",
    )
    parser.add_argument(
        "--estimate_mode",
        type=str,
        default="sketchdb",
        help="Name of estimate/optimized experiment mode (default: sketchdb)",
    )
    parser.add_argument("--show", action="store_true", help="Show the plot")
    parser.add_argument("--save", action="store_true", help="Save the plot to a file")
    parser.add_argument(
        "--output_file",
        type=str,
        default="latency_cost_tradeoff.png",
        help="Output file path (default: latency_cost_tradeoff.png)",
    )
    parser.add_argument(
        "--label_points",
        action="store_true",
        help="Label points with experiment names",
    )

    args = parser.parse_args()
    main(args)

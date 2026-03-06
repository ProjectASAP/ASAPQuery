import os
import sys
import yaml
import json
import argparse
import numpy as np
import matplotlib.pyplot as plt

from typing import List, Optional, Dict

from promql_utilities.query_results.classes import LatencyResultAcrossTime

# TODO: make this more robust
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import constants  # noqa: E402


def calculate_percentiles(latencies: List[float]) -> Dict[int, float]:
    """Calculate percentiles from p0 to p100 in steps of 5."""
    if not latencies:
        return {p: 0.0 for p in range(0, 101, 5)}

    percentiles = {}
    for p in range(0, 101, 5):
        percentiles[p] = float(np.percentile(latencies, p))

    return percentiles


def collect_all_latencies(
    results: Dict[int, LatencyResultAcrossTime],
    all_queries: List[str],
) -> List[float]:
    """Collect all latencies across all queries."""
    all_latencies = []

    for query_idx in range(len(all_queries)):
        latencies = [
            latency
            for latency in results[query_idx].get_latencies()
            if latency is not None
        ]
        all_latencies.extend(latencies)

    return all_latencies


def plot_latency_distribution(
    exact_percentiles: Dict[int, float],
    estimate_percentiles: Dict[int, float],
    exact_name: str,
    estimate_name: str,
    output_path: Optional[str] = None,
    show: bool = False,
) -> None:
    """Plot latency distribution across percentiles."""
    percentile_values = sorted(exact_percentiles.keys())
    exact_latencies = [exact_percentiles[p] for p in percentile_values]
    estimate_latencies = [estimate_percentiles[p] for p in percentile_values]

    plt.figure(figsize=(12, 6))
    plt.plot(
        percentile_values, exact_latencies, marker="o", label=exact_name, linewidth=2
    )
    plt.plot(
        percentile_values,
        estimate_latencies,
        marker="s",
        label=estimate_name,
        linewidth=2,
    )

    plt.xlabel("Percentile", fontsize=12)
    plt.ylabel("Latency (seconds)", fontsize=12)
    plt.title("Query Latency Distribution", fontsize=14)
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)

    # Set x-axis ticks to show all percentiles
    plt.xticks(
        percentile_values, [f"p{p}" for p in percentile_values], rotation=45, ha="right"
    )

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Plot saved to {output_path}")

    if show:
        plt.show()
    else:
        plt.close()


def print_percentile_data(
    exact_percentiles: Dict[int, float],
    estimate_percentiles: Dict[int, float],
    exact_name: str,
    estimate_name: str,
) -> None:
    """Print percentile data in a readable format."""
    print(f"\nLatency Distribution: {exact_name} vs {estimate_name}")
    print("-" * 80)
    print(
        f"{'Percentile':<15} {exact_name:<20} {estimate_name:<20} {'Ratio (E/S)':<15}"
    )
    print("-" * 80)

    for p in sorted(exact_percentiles.keys()):
        exact_val = exact_percentiles[p]
        estimate_val = estimate_percentiles[p]

        if exact_val > 0 and estimate_val > 0:
            ratio = exact_val / estimate_val
        elif exact_val == 0 and estimate_val == 0:
            ratio = 1.0
        elif exact_val > 0 and estimate_val == 0:
            ratio = float("inf")
        else:
            ratio = 0.0

        print(f"p{p:<14} {exact_val:<20.6f} {estimate_val:<20.6f} {ratio:<15.4f}")

    print("-" * 80)


def main(args):
    experiment_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, args.experiment_name)

    from results_loader import load_latencies_only, get_server_name_for_mode

    if not args.exact_experiment_server_name:
        args.exact_experiment_server_name = get_server_name_for_mode(
            experiment_dir, args.exact_experiment_mode
        )
    if not args.estimate_experiment_server_name:
        args.estimate_experiment_server_name = get_server_name_for_mode(
            experiment_dir, args.estimate_experiment_mode
        )
    import logging

    # Suppress debug logging if not printing
    if not args.print:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(level=logging.DEBUG)

    exact_dir = os.path.join(
        experiment_dir, args.exact_experiment_mode, "prometheus_client_output"
    )
    estimate_dir = os.path.join(
        experiment_dir, args.estimate_experiment_mode, "prometheus_client_output"
    )

    try:
        exact_latencies = load_latencies_only(exact_dir)
        exact_results = exact_latencies[args.exact_experiment_server_name]
    except (FileNotFoundError, KeyError) as e:
        print(f"Error loading exact latencies: {e}")
        raise

    try:
        estimate_latencies = load_latencies_only(estimate_dir)
        estimate_results = estimate_latencies[args.estimate_experiment_server_name]
    except (FileNotFoundError, KeyError) as e:
        print(f"Error loading estimate latencies: {e}")
        raise

    # Load query configuration
    config_files = os.listdir(os.path.join(experiment_dir, "experiment_config"))
    if len(config_files) != 1:
        raise ValueError(
            f"Expected exactly one config file in {experiment_dir}, but found {len(config_files)}"
        )

    with open(
        os.path.join(experiment_dir, "experiment_config", config_files[0]), "r"
    ) as f:
        config = yaml.safe_load(f)
        query_group_config = config["query_groups"]

    if len(query_group_config) != 1:
        raise ValueError(
            f"Expected exactly one query group in {experiment_dir}, but found {len(query_group_config)}"
        )

    query_group = query_group_config[0]
    all_queries = query_group["queries"]

    # Collect all latencies
    all_exact_latencies = collect_all_latencies(exact_results, all_queries)
    all_estimate_latencies = collect_all_latencies(estimate_results, all_queries)

    # Calculate percentiles
    exact_percentiles = calculate_percentiles(all_exact_latencies)
    estimate_percentiles = calculate_percentiles(all_estimate_latencies)

    # Print percentile data if requested
    if args.print:
        print_percentile_data(
            exact_percentiles,
            estimate_percentiles,
            args.exact_experiment_server_name,
            args.estimate_experiment_server_name,
        )

    # Output machine-readable JSON if requested
    if args.machine_readable:
        output = {
            "experiment_name": args.experiment_name,
            "exact_experiment_mode": args.exact_experiment_mode,
            "estimate_experiment_mode": args.estimate_experiment_mode,
            "exact_experiment_server_name": args.exact_experiment_server_name,
            "estimate_experiment_server_name": args.estimate_experiment_server_name,
            "exact_percentiles": exact_percentiles,
            "estimate_percentiles": estimate_percentiles,
        }
        print(json.dumps(output, indent=2))

    # Generate plot if requested
    if args.plot or args.show or args.save:
        output_path = None
        if args.save:
            if args.output:
                output_path = args.output
            else:
                output_path = os.path.join(
                    experiment_dir,
                    f"latency_distribution_{args.exact_experiment_mode}_vs_{args.estimate_experiment_mode}.png",
                )

        plot_latency_distribution(
            exact_percentiles,
            estimate_percentiles,
            args.exact_experiment_server_name,
            args.estimate_experiment_server_name,
            output_path=output_path,
            show=args.show,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Plot query latency distribution across percentiles"
    )
    parser.add_argument(
        "--experiment_name", type=str, required=True, help="Name of the experiment"
    )
    parser.add_argument(
        "--exact_experiment_mode",
        type=str,
        required=True,
        help="Exact experiment mode name",
    )
    parser.add_argument(
        "--estimate_experiment_mode",
        type=str,
        required=True,
        help="Estimate experiment mode name",
    )
    parser.add_argument(
        "--exact_experiment_server_name",
        type=str,
        required=False,
        help="Server name for exact experiment (defaults to experiment mode)",
    )
    parser.add_argument(
        "--estimate_experiment_server_name",
        type=str,
        required=False,
        help="Server name for estimate experiment (defaults to experiment mode)",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        default=False,
        help="Print percentile data to console",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        default=False,
        help="Generate plot (use with --show and/or --save)",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        default=False,
        help="Display the plot interactively",
    )
    parser.add_argument(
        "--save", action="store_true", default=False, help="Save the plot to a file"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=False,
        help="Output file path (defaults to experiment_dir/latency_distribution_<modes>.png)",
    )
    parser.add_argument(
        "--machine-readable",
        action="store_true",
        default=False,
        help="Output results in machine-readable JSON format",
    )

    args = parser.parse_args()

    # If no action is specified, default to printing
    if not (args.print or args.plot or args.show or args.save or args.machine_readable):
        args.print = True

    main(args)

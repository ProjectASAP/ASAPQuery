import os
import sys
import yaml
import json
import argparse
import numpy as np

from typing import List, Optional, Dict, Any

from promql_utilities.query_results.classes import LatencyResultAcrossTime

# TODO: make this more robust
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import constants  # noqa: E402


def calculate_latency_stats(latencies: List[float]) -> Dict[str, float]:
    """Calculate latency statistics for a list of latencies."""
    if not latencies:
        return {
            "median": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "sum": 0.0,
            "mean": 0.0,
        }

    return {
        "median": float(np.median(latencies)),
        "p95": float(np.percentile(latencies, 95)),
        "p99": float(np.percentile(latencies, 99)),
        "sum": float(np.sum(latencies)),
        "mean": float(np.mean(latencies)),
    }


def calculate_ratios(
    exact_stats: Dict[str, float], estimate_stats: Dict[str, float]
) -> Dict[str, float]:
    """Calculate ratios of exact to estimate stats."""
    ratios = {}
    for metric in ["median", "p95", "p99", "sum", "mean"]:
        if exact_stats[metric] > 0 and estimate_stats[metric] > 0:
            ratios[metric] = round(exact_stats[metric] / estimate_stats[metric], 2)
        elif exact_stats[metric] == 0 and estimate_stats[metric] == 0:
            ratios[metric] = 1.0
        elif exact_stats[metric] > 0 and estimate_stats[metric] == 0:
            ratios[metric] = float("inf")
        else:
            ratios[metric] = 0.0
    return ratios


def compare_latencies(
    exact_results: Dict[int, LatencyResultAcrossTime],
    estimate_results: Dict[int, LatencyResultAcrossTime],
    all_queries: List[str],
) -> Dict[int, Dict[str, Any]]:
    """Compare latencies between exact and estimate results."""

    results = {}

    all_exact_latencies = []
    all_estimate_latencies = []

    for query_idx, query in enumerate(all_queries):
        exact_latencies = [
            latency
            for latency in exact_results[query_idx].get_latencies()
            if latency is not None
        ]
        estimate_latencies = [
            latency
            for latency in estimate_results[query_idx].get_latencies()
            if latency is not None
        ]
        all_exact_latencies.extend(exact_latencies)
        all_estimate_latencies.extend(estimate_latencies)

        exact_stats = calculate_latency_stats(exact_latencies)
        estimate_stats = calculate_latency_stats(estimate_latencies)

        # Calculate ratio (estimate/exact) for each metric
        ratios = calculate_ratios(exact_stats, estimate_stats)

        results[query_idx] = {
            "query": query,
            "exact": exact_stats,
            "estimate": estimate_stats,
            "ratios": ratios,
        }

    results[-1] = {
        "query": "All",
        "exact": calculate_latency_stats(all_exact_latencies),
        "estimate": calculate_latency_stats(all_estimate_latencies),
    }
    results[-1]["ratios"] = calculate_ratios(
        results[-1]["exact"], results[-1]["estimate"]
    )

    return results


def print_comparison_results(
    comparison_results: Dict[int, Dict[str, Any]], exact_name: str, estimate_name: str
) -> None:
    """Print comparison results in a readable format."""
    print(f"\nLatency Comparison: {exact_name} vs {estimate_name}")
    print("-" * 100)

    headers = [
        "Query",
        "Metric",
        f"{exact_name}",
        f"{estimate_name}",
        "Ratio (exact/estimate)",
    ]
    print(
        f"{headers[0]:<5} {headers[1]:<10} {headers[2]:<15} {headers[3]:<15} {headers[4]:<15}"
    )
    print("-" * 100)

    for query_idx, data in sorted(comparison_results.items()):
        query_display = f"Q{query_idx}"

        for metric in ["median", "p95", "p99", "sum", "mean"]:
            exact_val = data["exact"][metric]
            estimate_val = data["estimate"][metric]
            ratio = data["ratios"][metric]

            print(
                f"{query_display:<5} {metric:<10} {exact_val:<15.4f} {estimate_val:<15.4f} {ratio:<15.4f}"
            )

        print("-" * 100)

    # Print aggregate statistics
    print("\nAggregate Statistics (across all queries):")
    print("-" * 100)

    avg_ratios = {
        metric: np.mean(
            [data["ratios"][metric] for data in comparison_results.values()]
        )
        for metric in ["median", "p95", "p99", "sum", "mean"]
    }

    for metric, avg_ratio in avg_ratios.items():
        print(f"Average {metric} ratio: {avg_ratio:.4f}")


def main(args):
    experiment_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, args.experiment_name)

    exact_results: Optional[Dict[int, LatencyResultAcrossTime]] = None
    estimate_results: Optional[Dict[int, LatencyResultAcrossTime]] = None

    from results_loader import load_latencies_only, get_server_name_for_mode
    import logging

    # Suppress debug logging in machine-readable mode
    if args.machine_readable:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(level=logging.DEBUG)

    if not args.exact_experiment_server_name:
        args.exact_experiment_server_name = get_server_name_for_mode(
            experiment_dir, args.exact_experiment_mode
        )
    if not args.estimate_experiment_server_name:
        args.estimate_experiment_server_name = get_server_name_for_mode(
            experiment_dir, args.estimate_experiment_mode
        )

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

    query_group_config = None
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

    # Flatten queries from all query groups
    all_queries = []
    for query_group in query_group_config:
        all_queries.extend(query_group["queries"])

    # Compare latencies
    assert exact_results is not None
    assert estimate_results is not None
    comparison_results = compare_latencies(exact_results, estimate_results, all_queries)

    # Output results based on machine-readable flag
    if args.machine_readable:
        # Convert comparison_results to a serializable format
        output = {
            "experiment_name": args.experiment_name,
            "exact_experiment_mode": args.exact_experiment_mode,
            "estimate_experiment_mode": args.estimate_experiment_mode,
            "exact_experiment_server_name": args.exact_experiment_server_name,
            "estimate_experiment_server_name": args.estimate_experiment_server_name,
            "results": comparison_results,
        }
        print(json.dumps(output, indent=2))
    else:
        # Print results for each query
        if args.print_per_query:
            print_comparison_results(
                comparison_results,
                args.exact_experiment_server_name,
                args.estimate_experiment_server_name,
            )

        # Print summary results across queries
        print("\nSummary Results:")
        print("-" * 100)
        for k in comparison_results[-1]:
            print(f"{k}: {comparison_results[-1][k]}")
        print("-" * 100)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--exact_experiment_mode", type=str, required=True)
    parser.add_argument("--estimate_experiment_mode", type=str, required=True)
    parser.add_argument("--exact_experiment_server_name", type=str, required=False)
    parser.add_argument("--estimate_experiment_server_name", type=str, required=False)
    parser.add_argument("--print_per_query", action="store_true", default=False)
    parser.add_argument(
        "--machine-readable",
        action="store_true",
        default=False,
        help="Output results in machine-readable JSON format",
    )
    args = parser.parse_args()
    main(args)

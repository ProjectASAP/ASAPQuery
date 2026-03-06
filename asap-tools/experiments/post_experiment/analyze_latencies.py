import os
import sys
import yaml
import argparse
import numpy as np

from typing import List, Dict, Any

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


def analyze_latencies(
    results: Dict[int, LatencyResultAcrossTime],
    all_queries: List[str],
) -> Dict[int, Dict[str, Any]]:
    """Analyze latencies for a single experiment."""

    analysis_results = {}

    all_latencies = []

    for query_idx, query in enumerate(all_queries):
        latencies = [
            latency
            for latency in results[query_idx].get_latencies()
            if latency is not None
        ]
        all_latencies.extend(latencies)

        stats = calculate_latency_stats(latencies)

        analysis_results[query_idx] = {
            "query": query,
            "stats": stats,
            "num_samples": len(latencies),
        }

    # Add aggregate statistics across all queries
    analysis_results[-1] = {
        "query": "All",
        "stats": calculate_latency_stats(all_latencies),
        "num_samples": len(all_latencies),
    }

    return analysis_results


def print_analysis_results(
    analysis_results: Dict[int, Dict[str, Any]], experiment_name: str
) -> None:
    """Print analysis results in a readable format."""
    print(f"\nLatency Analysis: {experiment_name}")
    print("-" * 100)

    headers = [
        "Query",
        "Samples",
        "Metric",
        "Value (s)",
    ]
    print(f"{headers[0]:<10} {headers[1]:<10} {headers[2]:<10} {headers[3]:<15}")
    print("-" * 100)

    for query_idx, data in sorted(analysis_results.items()):
        query_display = f"Q{query_idx}" if query_idx >= 0 else "All"
        num_samples = data["num_samples"]

        for metric in ["median", "p95", "p99", "mean", "sum"]:
            val = data["stats"][metric]

            if metric == "sum":
                # For sum, show first without samples count
                print(f"{query_display:<10} {'':<10} {metric:<10} {val:<15.4f}")
            else:
                # For other metrics, show samples on first line only
                sample_str = str(num_samples) if metric == "median" else ""
                print(f"{query_display:<10} {sample_str:<10} {metric:<10} {val:<15.4f}")

        print("-" * 100)


def main(args):
    experiment_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, args.experiment_name)

    from results_loader import load_latencies_only, get_server_name_for_mode

    if not args.experiment_server_name:
        args.experiment_server_name = get_server_name_for_mode(
            experiment_dir, args.experiment_mode
        )
    import logging

    logging.basicConfig(level=logging.DEBUG)

    experiment_mode_dir = os.path.join(
        experiment_dir, args.experiment_mode, "prometheus_client_output"
    )

    try:
        latencies = load_latencies_only(experiment_mode_dir)
        results = latencies[args.experiment_server_name]
    except (FileNotFoundError, KeyError) as e:
        print(f"Error loading latencies: {e}")
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

    # Analyze latencies
    assert results is not None
    analysis_results = analyze_latencies(results, all_queries)

    # Print results for each query
    if args.print_per_query:
        print_analysis_results(analysis_results, args.experiment_server_name)

    # Print summary results across queries
    print("\nSummary Results (All Queries):")
    print("-" * 100)
    for k in analysis_results[-1]:
        print(f"{k}: {analysis_results[-1][k]}")
    print("-" * 100)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze latencies for a single experiment mode"
    )
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--experiment_mode", type=str, required=True)
    parser.add_argument("--experiment_server_name", type=str, required=False)
    parser.add_argument("--print_per_query", action="store_true", default=False)
    args = parser.parse_args()
    main(args)

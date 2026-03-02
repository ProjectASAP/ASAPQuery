"""
Unified results loader with backward compatibility.

This module provides a unified interface for loading query results
that automatically detects and handles both new streaming formats
(JSONL, Parquet) and legacy pickle format.
"""

import os
import pickle
import logging
import yaml
from typing import Dict

from promql_utilities.query_results.classes import (
    QueryResultAcrossTime,
    LatencyResultAcrossTime,
)
from promql_utilities.query_results.serializers import SerializerFactory

logger = logging.getLogger(__name__)


def get_server_name_for_mode(experiment_dir: str, mode: str) -> str:
    """Return the server name used for a given experiment mode.

    Reads controller_client_configs/{mode}.yaml from the experiment directory.
    Raises FileNotFoundError if the config is missing, ValueError if it has no servers.
    """
    config_path = os.path.join(
        experiment_dir, "controller_client_configs", f"{mode}.yaml"
    )
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"No controller client config for mode '{mode}': {config_path}"
        )
    with open(config_path) as f:
        config = yaml.safe_load(f)
    servers = config.get("servers", [])
    if not servers:
        raise ValueError(
            f"No servers listed in controller client config for mode '{mode}': {config_path}"
        )
    return servers[0]["name"]


def load_results(experiment_dir: str) -> Dict[str, Dict[int, QueryResultAcrossTime]]:
    """Load query results with automatic format detection and fallback.

    This function tries to load results in the following order:
    1. New streaming formats (JSONL, Parquet) - auto-detected
    2. Legacy pickle format (results.pkl)
    3. Raises error if no format is found

    Args:
        experiment_dir: Directory containing experiment results

    Returns:
        Nested dict of server_name -> query_idx -> QueryResultAcrossTime

    Raises:
        FileNotFoundError: If no results are found in any format
        Exception: If results exist but cannot be loaded
    """
    if not os.path.exists(experiment_dir):
        raise FileNotFoundError(
            f"Experiment directory does not exist: {experiment_dir}"
        )

    # Try new formats first
    try:
        serializer = SerializerFactory.create_from_existing(experiment_dir)
        if serializer is not None:
            logger.debug(f"Loading results using {serializer} format")
            return serializer.read_results()
    except Exception as e:
        logger.warning(f"Failed to load with new formats: {e}")

    # Fall back to pickle format
    pickle_path = os.path.join(experiment_dir, "results.pkl")
    if os.path.exists(pickle_path):
        try:
            logger.debug("Loading results using legacy pickle format")
            with open(pickle_path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"Failed to load pickle format: {e}")
            raise

    # No results found in any format
    raise FileNotFoundError(
        f"No results found in {experiment_dir}. "
        f"Expected either new format files (experiment_metadata.json + results files) "
        f"or legacy results.pkl"
    )


def save_results(
    results_across_servers: Dict[str, Dict[int, QueryResultAcrossTime]],
    output_dir: str,
    format_name: str = "jsonl",
    keep_pickle: bool = True,
) -> None:
    """Save query results in the specified format.

    Args:
        results_across_servers: Nested dict of results to save
        output_dir: Directory where results will be written
        format_name: Format to use ('jsonl', 'parquet', or 'auto')
        keep_pickle: Whether to also save legacy pickle format for compatibility

    Raises:
        ValueError: If format is not supported
        Exception: If saving fails
    """
    # Save in new format
    try:
        serializer = SerializerFactory.create(format_name, output_dir)
        logger.debug(f"Saving results using {serializer} format")
        serializer.write_results(results_across_servers)
    except Exception as e:
        logger.error(f"Failed to save in {format_name} format: {e}")
        raise

    # Also save in pickle format for backward compatibility if requested
    if keep_pickle:
        try:
            pickle_path = os.path.join(output_dir, "results.pkl")
            with open(pickle_path, "wb") as f:
                pickle.dump(results_across_servers, f)
            logger.debug("Also saved results in legacy pickle format for compatibility")
        except Exception as e:
            logger.warning(f"Failed to save pickle format: {e}")


# Legacy compatibility aliases
def load_results_legacy(
    experiment_dir: str,
) -> Dict[str, Dict[int, QueryResultAcrossTime]]:
    """Legacy function name - use load_results() instead."""
    import warnings

    warnings.warn(
        "load_results_legacy() is deprecated, use load_results() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return load_results(experiment_dir)


# Convenience functions for common use cases
def load_exact_and_estimate_results(
    experiment_dir: str, exact_mode: str, estimate_mode: str
) -> tuple:
    """Load both exact and estimate results for comparison.

    Args:
        experiment_dir: Base experiment directory
        exact_mode: Subdirectory name for exact results
        estimate_mode: Subdirectory name for estimate results

    Returns:
        Tuple of (exact_results, estimate_results)
    """
    exact_dir = os.path.join(experiment_dir, exact_mode)
    estimate_dir = os.path.join(experiment_dir, estimate_mode)

    exact_results = load_results(exact_dir)
    estimate_results = load_results(estimate_dir)

    return exact_results, estimate_results


def load_latencies_only(
    experiment_dir: str,
) -> Dict[str, Dict[int, LatencyResultAcrossTime]]:
    """Load only latency information without query results.

    Args:
        experiment_dir: Directory containing experiment results

    Returns:
        Nested dict: server_name -> query_idx -> LatencyResultAcrossTime

    Raises:
        FileNotFoundError: If no latency data is found in any format
    """
    if not os.path.exists(experiment_dir):
        raise FileNotFoundError(
            f"Experiment directory does not exist: {experiment_dir}"
        )

    # Try new formats first
    try:
        serializer = SerializerFactory.create_from_existing(experiment_dir)
        if serializer is not None:
            logger.debug(f"Loading latencies using {serializer} format")
            return serializer.read_latencies_only()
    except Exception as e:
        logger.warning(f"Failed to load latencies with new formats: {e}")

    # Fall back to pickle format
    pickle_path = os.path.join(experiment_dir, "results.pkl")
    if os.path.exists(pickle_path):
        try:
            logger.debug("Loading latencies from legacy pickle format")
            with open(pickle_path, "rb") as f:
                full_results = pickle.load(f)
            return _extract_latencies_from_full_results(full_results)
        except Exception as e:
            logger.error(f"Failed to load latencies from pickle format: {e}")
            raise

    # No results found in any format
    raise FileNotFoundError(f"No latency data found in {experiment_dir}")


def _extract_latencies_from_full_results(
    full_results: Dict[str, Dict[int, QueryResultAcrossTime]]
) -> Dict[str, Dict[int, LatencyResultAcrossTime]]:
    """Extract latency data from full QueryResultAcrossTime structure."""
    latencies = {}

    for server_name, server_results in full_results.items():
        latencies[server_name] = {}
        for query_idx, query_result_across_time in server_results.items():
            # Use the class method to convert from QueryResultAcrossTime
            latencies[server_name][query_idx] = (
                LatencyResultAcrossTime.from_query_result_across_time(
                    query_result_across_time
                )
            )

    return latencies

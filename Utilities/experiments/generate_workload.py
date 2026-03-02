#!/usr/bin/env python3
"""
Workload Generator Script

Generates experiment config YAML files with randomized query workloads based on
building blocks and distribution patterns.

Usage:
    python generate_workload.py --num-queries 20 --distribution uniform --num-configs 5
    python generate_workload.py --num-queries 50 --distribution heavy_tailed --favor-blocks 1,3,5 --num-configs 3 --seed 42
"""

import argparse
import random
import re
import yaml
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import copy

import constants


# ============================================================================
# EXTENSIBLE PARAMETER FUNCTIONS
# These functions return hardcoded values for now but can be extended later
# ============================================================================


def get_aggregation_label() -> str:
    """Returns the label to use for 'by' aggregations.

    Currently hardcoded to 'label_0'. Modify this function to support
    multiple labels or different selection logic.
    """
    return "label_0"


def get_time_range() -> str:
    """Returns the time range for _over_time queries.

    Currently hardcoded to '15m'. Modify this function to support
    variable time ranges.
    """
    return "15m"


def get_quantile_values() -> List[float]:
    """Returns possible quantile values for quantile queries.

    Modify this function to add/remove quantile options.
    """
    return [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99]


def get_metric_name() -> str:
    """Returns the metric name to use in queries.

    Currently returns 'fake_metric_total' (counter type).
    Modify this function to support different metric types.
    """
    return "fake_metric_total"


def get_metric_type() -> str:
    """Returns the metric type (gauge or counter).

    Currently hardcoded to 'counter'. Modify to support gauge.
    """
    return "counter"


def parse_time_range_to_seconds(time_range: str) -> int:
    """Converts a time range string to seconds.

    Args:
        time_range: Time range string (e.g., '15m', '1h', '30s')

    Returns:
        Time range in seconds

    Raises:
        ValueError: If format is invalid
    """
    time_range = time_range.strip()
    if not time_range:
        raise ValueError("Empty time range")

    # Parse number and unit
    if time_range[-1] == "s":
        value = int(time_range[:-1])
        return value
    elif time_range[-1] == "m":
        return int(time_range[:-1]) * 60
    elif time_range[-1] == "h":
        return int(time_range[:-1]) * 3600
    elif time_range[-1] == "d":
        return int(time_range[:-1]) * 86400
    else:
        raise ValueError(f"Unknown time unit in '{time_range}'")


def get_query_lookback(query: str) -> int:
    """Determines the lookback period (T_lookback) for a query.

    Args:
        query: PromQL query string

    Returns:
        Lookback period in seconds:
        - For temporal queries: parsed time range value
        - For spatial queries: 1
    """
    # Check if query contains a time range (e.g., [15m])
    match = re.search(r"\[(\d+[smhd])\]", query)

    if match:
        # Temporal query - parse the time range
        time_range = match.group(1)
        return parse_time_range_to_seconds(time_range)
    else:
        # Spatial query - return 1
        return 1


# ============================================================================
# QUERY BUILDING BLOCKS (B1-B6)
# Each function generates a random query of that type
# ============================================================================


def generate_b1_query() -> str:
    """B1: quantile by () query

    Example: quantile by (label_0) (0.95, fake_metric_total)
    """
    quantile = random.choice(get_quantile_values())
    label = get_aggregation_label()
    metric = get_metric_name()
    return f"quantile by ({label}) ({quantile}, {metric})"


def generate_b2_query() -> str:
    """B2: sum by or count by query

    Example: sum by (label_0) (fake_metric_total)
    """
    aggregation = random.choice(["sum", "count"])
    label = get_aggregation_label()
    metric = get_metric_name()
    return f"{aggregation} by ({label}) ({metric})"


def generate_b3_query() -> str:
    """B3: quantile_over_time query

    Example: quantile_over_time(0.95, fake_metric_total[15m])
    """
    quantile = random.choice(get_quantile_values())
    time_range = get_time_range()
    metric = get_metric_name()
    return f"quantile_over_time({quantile}, {metric}[{time_range}])"


def generate_b4_query() -> str:
    """B4: sum_over_time OR count_over_time

    Example: sum_over_time(fake_metric_total[15m])
    """
    aggregation = random.choice(["sum_over_time", "count_over_time"])
    time_range = get_time_range()
    metric = get_metric_name()
    return f"{aggregation}({metric}[{time_range}])"


def generate_b5_query() -> str:
    """B5: rate/increase

    Example: rate(fake_metric_total[15m])
    """
    function = random.choice(["rate", "increase"])
    time_range = get_time_range()
    metric = get_metric_name()
    return f"{function}({metric}[{time_range}])"


def generate_b6_query() -> str:
    """B6: sum by () (sum_over_time) or sum by () (count_over_time)

    Example: sum by (label_0) (sum_over_time(fake_metric_total[15m]))
    """
    outer_agg = "sum"
    inner_agg = random.choice(["sum_over_time", "count_over_time"])
    label = get_aggregation_label()
    time_range = get_time_range()
    metric = get_metric_name()
    return f"{outer_agg} by ({label}) ({inner_agg}({metric}[{time_range}]))"


# Map block IDs to generator functions
BLOCK_GENERATORS = {
    1: generate_b1_query,
    2: generate_b2_query,
    3: generate_b3_query,
    4: generate_b4_query,
    5: generate_b5_query,
    6: generate_b6_query,
}


# ============================================================================
# DISTRIBUTION FUNCTIONS
# ============================================================================


def distribute_uniform(
    num_queries: int, num_blocks: int = None, select_blocks: List[int] = None
) -> List[int]:
    """Distributes queries uniformly across blocks.

    Args:
        num_queries: Total number of queries to generate
        num_blocks: Number of building blocks (if None, uses len(BLOCK_GENERATORS))
        select_blocks: Optional list of block IDs to use (1-indexed)

    Returns:
        List of query counts per block [B1_count, B2_count, ..., BN_count]
    """
    if num_blocks is None:
        num_blocks = len(BLOCK_GENERATORS)
    # Determine which blocks to use
    if select_blocks:
        active_blocks = [b for b in select_blocks if 1 <= b <= num_blocks]
        num_active = len(active_blocks)
    else:
        active_blocks = list(range(1, num_blocks + 1))
        num_active = num_blocks

    if num_active == 0:
        return [0] * num_blocks

    base_count = num_queries // num_active
    remainder = num_queries % num_active

    # Initialize all counts to 0
    counts = [0] * num_blocks

    # Distribute base count to active blocks
    for block_id in active_blocks:
        counts[block_id - 1] = base_count

    # Distribute remainder randomly among active blocks
    remainder_blocks = random.sample(active_blocks, remainder)
    for block_id in remainder_blocks:
        counts[block_id - 1] += 1

    return counts


def distribute_heavy_tailed(
    num_queries: int,
    favor_blocks: List[int] = None,
    num_blocks: int = None,
    select_blocks: List[int] = None,
) -> List[int]:
    """Distributes queries with ordered exponential decay.

    Args:
        num_queries: Total number of queries to generate
        favor_blocks: List of block IDs in preference order (1-indexed)
                     First block gets most queries, then exponential decay
        num_blocks: Number of building blocks (if None, uses len(BLOCK_GENERATORS))
        select_blocks: Optional list of block IDs to use (1-indexed)

    Returns:
        List of query counts per block [B1_count, B2_count, ..., BN_count]
    """
    if num_blocks is None:
        num_blocks = len(BLOCK_GENERATORS)
    # Determine which blocks are available
    if select_blocks:
        available_blocks = [b for b in select_blocks if 1 <= b <= num_blocks]
    else:
        available_blocks = list(range(1, num_blocks + 1))

    if not available_blocks:
        return [0] * num_blocks

    # If favor_blocks not specified, use all available blocks
    if favor_blocks is None:
        favor_blocks = available_blocks

    # Create ranking: favored blocks first (in order), then others
    # Only include blocks that are both favored AND available
    ranking = []
    for block_id in favor_blocks:
        if block_id in available_blocks:
            ranking.append(block_id)

    # Add remaining available blocks not in favor list
    for block_id in available_blocks:
        if block_id not in ranking:
            ranking.append(block_id)

    num_active = len(ranking)
    if num_active == 0:
        return [0] * num_blocks

    # Generate exponential weights (decay factor = 2)
    weights = []
    for i in range(num_active):
        weights.append(1.0 / (2**i))

    # Normalize weights to sum to 1
    total_weight = sum(weights)
    probabilities = [w / total_weight for w in weights]

    # Assign counts based on probabilities
    counts_dict = {}
    remaining = num_queries

    for i in range(num_active - 1):
        block_id = ranking[i]
        count = round(probabilities[i] * num_queries)
        counts_dict[block_id] = count
        remaining -= count

    # Last block gets remainder to ensure exact total
    counts_dict[ranking[-1]] = max(0, remaining)

    # Convert to list ordered by block ID
    counts = [counts_dict.get(i, 0) for i in range(1, num_blocks + 1)]

    return counts


# ============================================================================
# CONFIG TEMPLATE AND GENERATION
# ============================================================================


def get_base_config() -> Dict:
    """Returns the base configuration template.

    Modify this function to change default exporter settings,
    monitoring configuration, etc.
    """
    return {
        "experiment": [
            {"mode": constants.SKETCHDB_EXPERIMENT_NAME, "server": "sketchdb"},
            {"mode": constants.BASELINE_EXPERIMENT_NAME, "server": "prometheus"},
        ],
        "monitoring": {"tool": "prometheus", "deployment_mode": "bare_metal"},
        "servers": [
            {"name": "prometheus", "url": "http://localhost:9090"},
            {"name": "sketchdb", "url": "http://localhost:8088"},
        ],
        "exporters": {
            "only_start_if_queries_exist": True,
            "exporter_list": {
                "node_exporter": {
                    "port": 9100,
                    "extra_flags": "--collector.disable-defaults --collector.cpu",
                },
                "fake_exporter": {
                    "num_ports_per_server": 1,
                    "start_port": 50000,
                    "dataset": "zipf",
                    "synthetic_data_value_scale": 10000,
                    "num_labels": 3,
                    "num_values_per_label": 20,
                    "metric_type": get_metric_type(),
                },
            },
        },
        "query_groups": [
            {
                "id": 1,
                "queries": [],  # Will be populated
                "repetition_delay": 10,
                "client_options": {
                    "repetitions": 10,
                    "query_time_offset": 10,
                    "starting_delay": 60,
                },
                "controller_options": {"accuracy_sla": 0.99, "latency_sla": 1},
            }
        ],
        "metrics": [
            {
                "metric": get_metric_name(),
                "labels": ["instance", "job", "label_0", "label_1", "label_2"],
                "exporter": "fake_exporter",
            }
        ],
    }


def generate_queries(
    distribution: str,
    num_queries: int,
    favor_blocks: List[int],
    allow_duplicates: bool,
    select_blocks: List[int] = None,
) -> List[str]:
    """Generates queries based on distribution and parameters.

    Args:
        distribution: 'uniform' or 'heavy_tailed'
        num_queries: Total number of queries to generate
        favor_blocks: Block IDs to favor (for heavy_tailed distribution)
        allow_duplicates: If False, ensures all queries are unique
        select_blocks: Optional list of block IDs to use (1-indexed)

    Returns:
        List of query strings
    """
    # Get distribution of queries across blocks
    if distribution == "uniform":
        block_counts = distribute_uniform(num_queries, select_blocks=select_blocks)
    elif distribution == "heavy_tailed":
        block_counts = distribute_heavy_tailed(
            num_queries, favor_blocks, select_blocks=select_blocks
        )
    else:
        raise ValueError(f"Unknown distribution: {distribution}")

    # Generate queries
    queries = []
    seen_queries = set()

    for block_id, count in enumerate(block_counts, start=1):
        if count == 0:
            continue

        generator = BLOCK_GENERATORS[block_id]
        generated = 0
        max_attempts = count * 100  # Prevent infinite loops
        attempts = 0

        while generated < count and attempts < max_attempts:
            query = generator()
            attempts += 1

            if allow_duplicates or query not in seen_queries:
                queries.append(query)
                seen_queries.add(query)
                generated += 1

    # Shuffle queries to mix building blocks
    random.shuffle(queries)

    return queries


def generate_config(
    distribution: str,
    num_queries: int,
    favor_blocks: List[int] = None,
    allow_duplicates: bool = False,
    select_blocks: List[int] = None,
) -> Dict:
    """Generates a complete experiment configuration.

    Args:
        distribution: 'uniform' or 'heavy_tailed'
        num_queries: Total number of queries
        favor_blocks: Block IDs to favor (for heavy_tailed)
        allow_duplicates: Allow duplicate queries
        select_blocks: Optional list of block IDs to use (1-indexed)

    Returns:
        Complete configuration dictionary
    """
    config = get_base_config()
    queries = generate_queries(
        distribution, num_queries, favor_blocks, allow_duplicates, select_blocks
    )
    config["query_groups"][0]["queries"] = queries

    # Calculate max T_lookback across all queries
    max_lookback = max(get_query_lookback(q) for q in queries) if queries else 1

    # Calculate starting_delay = query_time_offset + max(T_lookback)
    query_time_offset = config["query_groups"][0]["client_options"]["query_time_offset"]
    starting_delay = query_time_offset + max_lookback

    # Update starting_delay in config
    config["query_groups"][0]["client_options"]["starting_delay"] = starting_delay

    return config


def create_victoriametrics_variant(config: Dict) -> Dict:
    """Creates a VictoriaMetrics variant of the configuration.

    Args:
        config: Base Prometheus configuration

    Returns:
        VictoriaMetrics configuration variant
    """
    vm_config = copy.deepcopy(config)

    # Update monitoring settings
    vm_config["monitoring"]["tool"] = "victoriametrics"
    vm_config["monitoring"]["deployment_mode"] = "containerized"

    # Update prometheus server URL to VictoriaMetrics port
    for server in vm_config["servers"]:
        if server["name"] == "prometheus":
            server["url"] = "http://localhost:8428"

    return vm_config


def save_config(
    config: Dict, output_dir: Path, index: int, variant: str = "prometheus"
) -> Path:
    """Saves configuration to YAML file.

    Args:
        config: Configuration dictionary
        output_dir: Output directory path
        index: Config file index
        variant: Config variant ('prometheus' or 'victoriametrics')

    Returns:
        Path to saved file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"generated_workload_{variant}_{timestamp}_{index}.yaml"
    filepath = output_dir / filename

    # Add @package directive at the top
    yaml_content = "# @package experiment_params\n"
    yaml_content += yaml.dump(config, default_flow_style=False, sort_keys=False)

    with open(filepath, "w") as f:
        f.write(yaml_content)

    return filepath


# ============================================================================
# CLI INTERFACE
# ============================================================================


def parse_block_list(block_str: str, param_name: str = "Block") -> List[int]:
    """Parses comma-separated block IDs.

    Args:
        block_str: Comma-separated block IDs (e.g., "1,3,5")
        param_name: Name of parameter for error messages

    Returns:
        List of integer block IDs
    """
    if not block_str:
        return None

    try:
        blocks = [int(x.strip()) for x in block_str.split(",")]
        # Validate block IDs
        max_blocks = len(BLOCK_GENERATORS)
        for block_id in blocks:
            if block_id < 1 or block_id > max_blocks:
                raise ValueError(
                    f"{param_name} ID must be between 1 and {max_blocks}, got {block_id}"
                )
        return blocks
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid block IDs: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate experiment workload configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 5 uniform workloads with 20 queries each
  python generate_workload.py --num-queries 20 --distribution uniform --num-configs 5

  # Generate heavy-tailed workload favoring blocks 1, 3, and 5
  python generate_workload.py --num-queries 50 --distribution heavy_tailed \\
      --favor-blocks 1,3,5 --num-configs 3 --seed 42

  # Only generate queries from blocks 1 and 3 (quantile queries only)
  python generate_workload.py --num-queries 30 --distribution uniform \\
      --select-blocks 1,3 --num-configs 2

  # Allow duplicate queries
  python generate_workload.py --num-queries 30 --distribution uniform \\
      --num-configs 2 --allow-duplicates

Building Blocks:
  B1: quantile by () query
  B2: sum by / count by query
  B3: quantile_over_time query
  B4: sum_over_time / count_over_time
  B5: rate / increase
  B6: sum by () (sum_over_time / count_over_time)
        """,
    )

    # Required arguments
    parser.add_argument(
        "--num-queries",
        type=int,
        required=True,
        help="Total number of queries per config",
    )
    parser.add_argument(
        "--distribution",
        type=str,
        required=True,
        choices=["uniform", "heavy_tailed"],
        help="Distribution type for queries across blocks",
    )
    parser.add_argument(
        "--num-configs",
        type=int,
        required=True,
        help="Number of config files to generate",
    )

    # Optional arguments
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (optional)",
    )
    parser.add_argument(
        "--favor-blocks",
        type=str,
        default=None,
        help='Comma-separated block IDs to favor (e.g., "1,3,5") for heavy_tailed',
    )
    parser.add_argument(
        "--select-blocks",
        type=str,
        default=None,
        help='Comma-separated block IDs to use (e.g., "1,3") to only generate from specific blocks',
    )
    parser.add_argument(
        "--allow-duplicates",
        action="store_true",
        help="Allow duplicate queries in a config (default: enforce uniqueness)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="config/experiment_type/generated",
        help="Output directory for generated configs",
    )

    args = parser.parse_args()

    # Validate and parse arguments
    favor_blocks = None
    if args.distribution == "heavy_tailed" and args.favor_blocks:
        favor_blocks = parse_block_list(args.favor_blocks, "Favor-block")

    select_blocks = None
    if args.select_blocks:
        select_blocks = parse_block_list(args.select_blocks, "Select-block")

    # Set up output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating {args.num_configs} workload configs...")
    print(f"  Queries per config: {args.num_queries}")
    print(f"  Distribution: {args.distribution}")
    if select_blocks:
        print(f"  Selected blocks: {select_blocks}")
    if favor_blocks:
        print(f"  Favored blocks: {favor_blocks}")
    print(f"  Allow duplicates: {args.allow_duplicates}")
    print(f"  Seed: {args.seed}")
    print(f"  Output directory: {output_dir}")
    print()

    generated_files = []

    for i in range(args.num_configs):
        # Use incremental seeds for reproducibility with variation
        random.seed(args.seed + i)

        # Generate base config
        config = generate_config(
            distribution=args.distribution,
            num_queries=args.num_queries,
            favor_blocks=favor_blocks,
            allow_duplicates=args.allow_duplicates,
            select_blocks=select_blocks,
        )

        # Save Prometheus config
        prometheus_filepath = save_config(config, output_dir, i + 1, "prometheus")
        generated_files.append(prometheus_filepath)
        print(f"Generated [{2*i+1}/{2*args.num_configs}]: {prometheus_filepath.name}")

        # Create and save VictoriaMetrics variant
        vm_config = create_victoriametrics_variant(config)
        vm_filepath = save_config(vm_config, output_dir, i + 1, "victoriametrics")
        generated_files.append(vm_filepath)
        print(f"Generated [{2*i+2}/{2*args.num_configs}]: {vm_filepath.name}")

    print()
    print(
        f"Successfully generated {len(generated_files)} config files ({args.num_configs} Prometheus + {args.num_configs} VictoriaMetrics) in {output_dir}"
    )


if __name__ == "__main__":
    main()

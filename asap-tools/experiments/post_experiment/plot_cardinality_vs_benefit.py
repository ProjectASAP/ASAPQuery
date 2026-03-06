#!/usr/bin/env python3
"""
Plot latency benefit vs lookback period, with one line per cardinality.

This script analyzes experiments following the naming pattern:
    <query_type>_<lookback>_1_card_2_<exp>

For example: qot_30m_1_card_2_5
    - query_type: qot (quantile_over_time)
    - lookback: 30m
    - cardinality: 2^5 = 32

The script plots:
    - X-axis: Lookback period (log scale, in minutes)
    - Y-axis: Latency benefit ratio (prometheus/sketchdb)
    - Lines: One per cardinality level (2^0 through 2^9)
"""

import os
import sys
import re
import glob
import yaml
import argparse
import subprocess
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional

# plotnine imports
from plotnine import (
    ggplot,
    aes,
    geom_line,
    geom_point,
    geom_hline,
    scale_color_discrete,
    scale_x_continuous,
    scale_y_continuous,
    labs,
    theme_minimal,
    theme,
    element_text,
    ggsave,
)

# Add parent directories to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import constants  # noqa: E402
from post_experiment.results_loader import (  # noqa: E402
    load_latencies_only,
    get_server_name_for_mode,
)
from post_experiment.compare_latencies import calculate_latency_stats  # noqa: E402

# Metric mapping for cost benefit (compare_costs.py doesn't have 'mean')
METRIC_TO_CPU_STAT = {
    "median": "median",
    "p95": "p95",
    "p99": "p99",
    "sum": "sum",
    "max": "max",
}


def parse_lookback_to_minutes(lookback_str: str) -> float:
    """
    Convert lookback string to minutes.

    Examples:
        '5m' -> 5.0
        '1h' -> 60.0
        '90s' -> 1.5
        '2h30m' -> 150.0 (if needed in future)
    """
    lookback_str = lookback_str.strip().lower()

    # Simple patterns first
    if lookback_str.endswith("m"):
        return float(lookback_str[:-1])
    elif lookback_str.endswith("h"):
        return float(lookback_str[:-1]) * 60
    elif lookback_str.endswith("s"):
        return float(lookback_str[:-1]) / 60

    # Fallback: try to parse as just a number (assume minutes)
    try:
        return float(lookback_str)
    except ValueError:
        raise ValueError(f"Cannot parse lookback string: {lookback_str}")


def parse_experiment_name(exp_name: str) -> Optional[Dict[str, Any]]:
    """
    Parse experiment name to extract metadata.

    Expected format: <query_type>_<lookback>_1_card_2_<exp>
    Example: qot_30m_1_card_2_5

    Returns:
        dict with keys: query_type, lookback_str, lookback_minutes, card_exp
        or None if name doesn't match pattern
    """
    # Pattern: word_lookback_1_card_2_digit
    pattern = r"^(?P<query_type>\w+)_(?P<lookback>\d+\w+)_1_card_2_(?P<card_exp>\d+)$"
    match = re.match(pattern, exp_name)

    if not match:
        return None

    data = match.groupdict()
    lookback_str = data["lookback"]

    return {
        "query_type": data["query_type"],
        "lookback_str": lookback_str,
        "lookback_minutes": parse_lookback_to_minutes(lookback_str),
        "card_exp": int(data["card_exp"]),
    }


def calculate_data_scale_from_config(config: Dict[str, Any]) -> int:
    """
    Calculate data scale from config: num_ports_per_server * num_values_per_label.
    """
    fake_exporter_config = config["exporters"]["exporter_list"]["fake_exporter"]

    num_ports_per_server = fake_exporter_config["num_ports_per_server"]
    num_values_per_label = fake_exporter_config["num_values_per_label"]

    return num_ports_per_server * num_values_per_label


def load_experiment_config(exp_dir: str) -> Dict[str, Any]:
    """Load experiment configuration YAML."""
    config_dir = os.path.join(exp_dir, "experiment_config")

    if not os.path.exists(config_dir):
        raise FileNotFoundError(f"Config directory not found: {config_dir}")

    config_files = [f for f in os.listdir(config_dir) if f.endswith(".yaml")]
    if len(config_files) != 1:
        raise ValueError(f"Expected exactly one config file, found {len(config_files)}")

    config_path = os.path.join(config_dir, config_files[0])
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def extract_experiment_data(
    exp_name: str, metric: str = "p95", verify_scale: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Extract data from a single experiment.

    Args:
        exp_name: Experiment name
        metric: Latency metric to use (median, p95, p99, mean)
        verify_scale: If True, verify data scale matches expected 2^card_exp

    Returns:
        dict with experiment data or None if extraction fails
    """
    # Parse experiment name
    metadata = parse_experiment_name(exp_name)
    if metadata is None:
        print(f"Warning: Skipping {exp_name} (doesn't match naming pattern)")
        return None

    exp_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, exp_name)

    if not os.path.exists(exp_dir):
        print(f"Warning: Experiment directory not found: {exp_dir}")
        return None

    try:
        # Load config
        config = load_experiment_config(exp_dir)

        # Calculate data scale from config
        actual_scale = calculate_data_scale_from_config(config)
        expected_scale = 2 ** metadata["card_exp"]

        if verify_scale and actual_scale != expected_scale:
            print(
                f"Warning: {exp_name} has scale {actual_scale} but expected {expected_scale}"
            )

        # Load latencies for both servers
        latencies = {}
        for server_type in ["baseline", "sketchdb"]:
            server_dir = os.path.join(exp_dir, server_type, "prometheus_client_output")

            if not os.path.exists(server_dir):
                print(f"Warning: {server_type} directory not found for {exp_name}")
                return None

            try:
                actual_server = get_server_name_for_mode(exp_dir, server_type)
                server_latencies = load_latencies_only(server_dir)
                if actual_server not in server_latencies:
                    print(f"Warning: No {server_type} data in results for {exp_name}")
                    return None

                # Aggregate latencies across all queries
                all_latencies = []
                for query_idx, latency_result in server_latencies[
                    actual_server
                ].items():
                    query_latencies = [
                        lat for lat in latency_result.get_latencies() if lat is not None
                    ]
                    all_latencies.extend(query_latencies)

                if not all_latencies:
                    print(f"Warning: No latency data for {server_type} in {exp_name}")
                    return None

                stats = calculate_latency_stats(all_latencies)
                latencies[server_type] = stats

            except Exception as e:
                print(f"Warning: Failed to load {server_type} data for {exp_name}: {e}")
                return None

        # Calculate benefit ratio
        if "baseline" not in latencies or "sketchdb" not in latencies:
            print(f"Warning: Missing server data for {exp_name}")
            return None

        baseline_latency = latencies["baseline"][metric]
        sketchdb_latency = latencies["sketchdb"][metric]

        if sketchdb_latency > 0:
            benefit_ratio = baseline_latency / sketchdb_latency
        elif baseline_latency > 0:
            benefit_ratio = float("inf")
        else:
            benefit_ratio = 1.0

        return {
            "experiment_name": exp_name,
            "query_type": metadata["query_type"],
            "lookback_str": metadata["lookback_str"],
            "lookback_minutes": metadata["lookback_minutes"],
            "card_exp": metadata["card_exp"],
            "data_scale": actual_scale,
            "baseline_latency": baseline_latency,
            "sketchdb_latency": sketchdb_latency,
            "benefit_ratio": benefit_ratio,
            "metric": metric,
        }

    except Exception as e:
        print(f"Warning: Failed to process {exp_name}: {e}")
        return None


def extract_cost_benefit_data(
    exp_name: str, metric: str = "p95", verify_scale: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Extract cost benefit data from a single experiment.

    Runs compare_costs.py and parses Query CPU Benefit output.

    Args:
        exp_name: Experiment name
        metric: CPU metric to use (median, p95, p99, sum, max)
        verify_scale: If True, verify data scale matches expected 2^card_exp

    Returns:
        dict with experiment data or None if extraction fails
    """
    # Parse experiment name
    metadata = parse_experiment_name(exp_name)
    if metadata is None:
        print(f"Warning: Skipping {exp_name} (doesn't match naming pattern)")
        return None

    exp_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, exp_name)

    if not os.path.exists(exp_dir):
        print(f"Warning: Experiment directory not found: {exp_dir}")
        return None

    try:
        # Verify scale if requested
        actual_scale = None
        if verify_scale:
            config = load_experiment_config(exp_dir)
            actual_scale = calculate_data_scale_from_config(config)
            expected_scale = 2 ** metadata["card_exp"]
            if actual_scale != expected_scale:
                print(
                    f"Warning: {exp_name} has scale {actual_scale} but expected {expected_scale}"
                )

        # Run compare_costs.py
        script_dir = os.path.dirname(os.path.abspath(__file__))
        compare_costs_path = os.path.join(script_dir, "compare_costs.py")

        result = subprocess.run(
            [
                "python3",
                compare_costs_path,
                "--experiment_name",
                exp_name,
                "--all_experiment_modes",
                "--print",
            ],
            capture_output=True,
            text=True,
            check=True,
            cwd=script_dir,
        )

        # Parse output for Query CPU Benefit section
        output = result.stdout + result.stderr
        cpu_stat = METRIC_TO_CPU_STAT.get(metric, "p95")

        # Look for pattern: "  <stat>: <value>x" in Query CPU Benefit section
        pattern = rf"Query CPU Benefit.*?^\s+{cpu_stat}:\s+([\d.]+)x"
        match = re.search(pattern, output, re.MULTILINE | re.DOTALL)

        if not match:
            print(
                f"Warning: Could not find Query CPU Benefit '{cpu_stat}' for {exp_name}"
            )
            return None

        benefit_ratio = float(match.group(1))

        return {
            "experiment_name": exp_name,
            "query_type": metadata["query_type"],
            "lookback_str": metadata["lookback_str"],
            "lookback_minutes": metadata["lookback_minutes"],
            "card_exp": metadata["card_exp"],
            "data_scale": actual_scale,
            "benefit_ratio": benefit_ratio,
            "metric": metric,
        }

    except subprocess.CalledProcessError as e:
        print(f"Warning: compare_costs.py failed for {exp_name}: {e}")
        return None
    except Exception as e:
        print(f"Warning: Failed to process {exp_name}: {e}")
        return None


def extract_experiments_from_patterns(
    patterns: List[str],
    metric: str = "p95",
    cardinalities: Optional[List[int]] = None,
    benefit_type: str = "latency",
) -> pd.DataFrame:
    """
    Extract data from experiments matching glob patterns.

    Args:
        patterns: List of glob patterns for experiment names
        metric: Metric to use (latency or CPU stat depending on benefit_type)
        cardinalities: Optional list of cardinality exponents to include
        benefit_type: Type of benefit to extract ('latency' or 'cost')

    Returns:
        DataFrame with experiment data
    """
    # Find all matching experiment directories
    exp_names = set()
    for pattern in patterns:
        pattern_path = os.path.join(constants.LOCAL_EXPERIMENT_DIR, pattern)
        for path in glob.glob(pattern_path):
            if os.path.isdir(path):
                exp_names.add(os.path.basename(path))

    if not exp_names:
        raise ValueError(f"No experiments found matching patterns: {patterns}")

    print(f"Found {len(exp_names)} experiments matching patterns")

    # Extract data from each experiment - dispatch based on benefit type
    data_list = []
    for exp_name in sorted(exp_names):
        if benefit_type == "latency":
            exp_data = extract_experiment_data(exp_name, metric=metric)
        elif benefit_type == "cost":
            exp_data = extract_cost_benefit_data(exp_name, metric=metric)
        else:
            raise ValueError(f"Unknown benefit_type: {benefit_type}")

        if exp_data is not None:
            # Filter by cardinality if specified
            if cardinalities is None or exp_data["card_exp"] in cardinalities:
                data_list.append(exp_data)

    if not data_list:
        raise ValueError("No valid experiment data extracted")

    print(f"Successfully extracted data from {len(data_list)} experiments")

    df = pd.DataFrame(data_list)

    # Add log-scale transformation: log2(T/15) where T is in minutes
    # This makes lookback periods equally spaced on the plot
    df["lookback_log2"] = np.log2(df["lookback_minutes"] / 15.0)

    return df


def create_plot(
    df: pd.DataFrame, metric: str = "p95", benefit_type: str = "latency"
) -> "ggplot":
    """
    Create benefit vs lookback plot with log2(T/15) x-axis.

    Args:
        df: DataFrame with experiment data
        metric: Metric being plotted (latency or CPU stat)
        benefit_type: Type of benefit ('latency' or 'cost')

    Returns:
        plotnine ggplot object
    """
    # Create labels for legend
    card_exps = sorted(df["card_exp"].unique())
    color_labels = [f"2^{exp}" for exp in card_exps]

    # Get unique lookback values for x-axis breaks and labels
    lookback_data = df[
        ["lookback_minutes", "lookback_log2", "lookback_str"]
    ].drop_duplicates()
    lookback_data = lookback_data.sort_values("lookback_minutes")

    x_breaks = lookback_data["lookback_log2"].tolist()
    x_labels = lookback_data["lookback_str"].tolist()

    # Get y-axis range and create breaks
    y_min = df["benefit_ratio"].min()
    y_max = df["benefit_ratio"].max()

    # Create y-axis breaks: use multiples of 10, plus explicitly include 1.0
    y_breaks = [1.0]  # Start with 1.0
    step = 10
    current = step
    while current <= y_max:
        y_breaks.append(float(current))
        current += step

    # Add 0 if needed (if y_min < 1)
    if y_min < 1.0:
        y_breaks.insert(0, 0.0)

    y_breaks = sorted(list(set(y_breaks)))  # Remove duplicates and sort

    # Dynamic Y-axis label based on benefit type
    y_label = "Latency Benefit" if benefit_type == "latency" else "Cost Benefit (CPU)"

    p = (
        ggplot(
            df,
            aes(
                x="lookback_log2",
                y="benefit_ratio",
                color="factor(card_exp)",
                group="factor(card_exp)",
            ),
        )
        + geom_hline(yintercept=1.0, linetype="dashed", color="gray", alpha=0.5)
        + geom_line(size=1.2)
        + geom_point(size=3)
        + scale_x_continuous(name="Data Scale", breaks=x_breaks, labels=x_labels)
        + scale_y_continuous(breaks=y_breaks)
        + scale_color_discrete(name="Data Cardinality", labels=color_labels)
        + labs(
            # title=f'{benefit_type.capitalize()} Benefit vs Lookback Period ({metric.upper()})',
            y=y_label
        )
        + theme_minimal()
        + theme(
            legend_position="right",
            # plot_title=element_text(size=14, weight='bold'),
            axis_title_x=element_text(size=12),
            axis_title_y=element_text(
                size=12, rotation=0, ha="left", va="center", margin={"r": 20}
            ),
            legend_title=element_text(size=11),
            plot_margin=0.1,
        )
    )

    return p


def print_summary_table(
    df: pd.DataFrame, metric: str = "p95", benefit_type: str = "latency"
):
    """Print summary table of experiment data."""
    # Dynamic header based on benefit type
    if benefit_type == "latency":
        header = f"Latency Benefit Analysis Summary ({metric.upper()} metric)"
    else:
        cpu_stat = METRIC_TO_CPU_STAT.get(metric, metric)
        header = f"Cost Benefit Analysis Summary (CPU {cpu_stat.upper()})"

    print("\n" + "=" * 100)
    print(header)
    print("=" * 100)

    # Group by query type
    for query_type in df["query_type"].unique():
        query_df = df[df["query_type"] == query_type]
        print(f"\nQuery Type: {query_type}")
        print("-" * 100)

        # Pivot table: rows = cardinality, columns = lookback
        pivot = query_df.pivot_table(
            index="card_exp",
            columns="lookback_str",
            values="benefit_ratio",
            aggfunc="mean",
        )

        # Sort columns by lookback minutes
        lookback_order = (
            query_df.groupby("lookback_str")["lookback_minutes"].first().sort_values()
        )
        pivot = pivot[lookback_order.index]

        # Format with data scale labels
        pivot.index = [f"2^{exp} ({2**exp})" for exp in pivot.index]

        print(pivot.to_string(float_format=lambda x: f"{x:.2f}"))

    print("\n" + "=" * 100)
    print(f"Total experiments: {len(df)}")
    print(f"Cardinalities: {sorted(df['card_exp'].unique())}")
    print(f"Lookback periods: {sorted(df['lookback_str'].unique())}")
    print(f"Query types: {sorted(df['query_type'].unique())}")

    # Summary statistics
    print("\nBenefit Ratio Statistics:")
    print(f"  Mean:   {df['benefit_ratio'].mean():.2f}")
    print(f"  Median: {df['benefit_ratio'].median():.2f}")
    print(f"  Min:    {df['benefit_ratio'].min():.2f}")
    print(f"  Max:    {df['benefit_ratio'].max():.2f}")
    print("=" * 100 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Plot latency or cost benefit vs lookback period, one line per cardinality",
        epilog="""
Examples:
  # Print latency benefit summary (default)
  python plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --print

  # Plot and save latency benefit
  python plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --plot --save latency_benefit.png

  # Plot cost benefit with p99 CPU metric
  python plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --benefit-type cost --metric p99 --plot --save cost_benefit_p99.png

  # Plot cost benefit with max CPU metric
  python plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --benefit-type cost --metric max --plot --save cost_benefit_max.png

  # Plot specific cardinalities
  python plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --plot --save benefit.png --cardinalities 0 2 4 6 8

  # Print cost benefit summary table
  python plot_cardinality_vs_benefit.py "qot_*_1_card_2_*" --benefit-type cost --metric p95 --print
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "patterns",
        nargs="+",
        help='Glob patterns for experiment names (e.g., "qot_*_1_card_2_*")',
    )
    parser.add_argument(
        "--metric",
        type=str,
        default="p95",
        choices=["median", "p95", "p99", "mean", "sum", "max"],
        help="Metric to plot (default: p95). Note: 'mean' only available for latency benefit",
    )
    parser.add_argument(
        "--benefit-type",
        type=str,
        default="latency",
        choices=["latency", "cost"],
        help="Type of benefit to plot: latency or cost (CPU) (default: latency)",
    )
    parser.add_argument(
        "--cardinalities",
        type=int,
        nargs="+",
        help="Filter to specific cardinality exponents (e.g., 0 2 4 6 8)",
    )
    parser.add_argument(
        "--print", action="store_true", dest="print_summary", help="Print summary table"
    )
    parser.add_argument("--plot", action="store_true", help="Generate plot")
    parser.add_argument("--save", type=str, help="Save plot to file (provide filename)")
    parser.add_argument("--show", action="store_true", help="Display plot")

    args = parser.parse_args()

    # Validate arguments
    if args.plot and not (args.save or args.show):
        parser.error("--plot requires either --save or --show (or both)")

    if args.save and not args.plot:
        parser.error("--save requires --plot")

    if not args.print_summary and not args.plot:
        parser.error("Must specify at least one of --print or --plot")

    # Validate metric compatibility with benefit type
    if args.benefit_type == "cost" and args.metric == "mean":
        parser.error(
            "'mean' metric is not available for cost benefit. Use median, p95, p99, sum, or max"
        )

    # Extract experiment data
    print(
        f"Extracting {args.benefit_type} benefit data from experiments matching: {args.patterns}"
    )
    df = extract_experiments_from_patterns(
        args.patterns,
        metric=args.metric,
        cardinalities=args.cardinalities,
        benefit_type=args.benefit_type,
    )

    # Print summary if requested
    if args.print_summary:
        print_summary_table(df, metric=args.metric, benefit_type=args.benefit_type)

    # Generate plot if requested
    if args.plot:
        print("\nGenerating plot...")
        plot = create_plot(df, metric=args.metric, benefit_type=args.benefit_type)

        if args.save:
            ggsave(plot, args.save, dpi=300, width=10, height=6)
            print(f"Plot saved to: {args.save}")

        if args.show:
            print(plot)

    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Modular plotting script for query latency metrics against data scale.

This script plots various latency metrics (median, p95, p99, mean, sum) against
data scale for multiple experiments, comparing different servers (prometheus vs sketchdb).
"""

import os
import sys
import yaml
import argparse
import pandas as pd
from typing import List, Dict, Any
from abc import ABC, abstractmethod

# plotnine imports
from plotnine import (
    ggplot,
    aes,
    geom_line,
    geom_point,
    scale_color_discrete,
    scale_linetype_discrete,
    labs,
    theme_minimal,
    theme,
    facet_wrap,
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


class DataExtractor:
    """Extracts experiment data and calculates data scale."""

    def __init__(self, experiment_names: List[str]):
        self.experiment_names = experiment_names
        self.base_dir = constants.LOCAL_EXPERIMENT_DIR

    def extract_experiment_data(self) -> List[Dict[str, Any]]:
        """Extract data from all experiments."""
        experiment_data = []

        for exp_name in self.experiment_names:
            try:
                data = self._extract_single_experiment(exp_name)
                experiment_data.append(data)
            except Exception as e:
                print(f"Warning: Failed to extract data from {exp_name}: {e}")

        return experiment_data

    def _extract_single_experiment(self, exp_name: str) -> Dict[str, Any]:
        """Extract data from a single experiment."""
        exp_dir = os.path.join(self.base_dir, exp_name)

        if not os.path.exists(exp_dir):
            raise FileNotFoundError(f"Experiment directory not found: {exp_dir}")

        # Load experiment config
        config = self._load_experiment_config(exp_dir)

        # Calculate data scale
        data_scale = self._calculate_data_scale(config)

        # Load latency data for both servers
        latency_data = {}
        for server_type in ["baseline", "sketchdb"]:
            try:
                server_dir = os.path.join(
                    exp_dir, server_type, "prometheus_client_output"
                )
                if os.path.exists(server_dir):
                    actual_server = get_server_name_for_mode(exp_dir, server_type)
                    server_latencies = load_latencies_only(server_dir)
                    if (
                        actual_server in server_latencies
                        and actual_server != server_type
                    ):
                        server_latencies = {
                            server_type: server_latencies[actual_server]
                        }
                    latency_data[server_type] = server_latencies
            except Exception as e:
                print(f"Warning: Failed to load {server_type} data for {exp_name}: {e}")

        return {
            "experiment_name": exp_name,
            "data_scale": data_scale,
            "config": config,
            "latency_data": latency_data,
        }

    def _load_experiment_config(self, exp_dir: str) -> Dict[str, Any]:
        """Load experiment configuration."""
        config_dir = os.path.join(exp_dir, "experiment_config")

        if not os.path.exists(config_dir):
            raise FileNotFoundError(f"Config directory not found: {config_dir}")

        config_files = [f for f in os.listdir(config_dir) if f.endswith(".yaml")]
        if len(config_files) != 1:
            raise ValueError(
                f"Expected exactly one config file, found {len(config_files)}"
            )

        config_path = os.path.join(config_dir, config_files[0])
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def _calculate_data_scale(self, config: Dict[str, Any]) -> float:
        """
        Calculate data scale as: num_ports_per_server * instances * (cardinality^num_labels)
        """
        # Extract parameters from config
        fake_exporter_config = config["exporters"]["exporter_list"]["fake_exporter"]

        num_ports_per_server = fake_exporter_config["num_ports_per_server"]
        num_values_per_label = fake_exporter_config[
            "num_values_per_label"
        ]  # cardinality
        num_labels = fake_exporter_config["num_labels"]

        # For now, assume 1 instance (could be extracted from server count if needed)
        num_instances = 1

        data_scale = (
            num_ports_per_server * num_instances * (num_values_per_label**num_labels)
        )

        return float(data_scale)


class DataProcessor:
    """Processes latency data for plotting."""

    def process_for_plotting(
        self,
        experiment_data: List[Dict[str, Any]],
        individual_queries: bool = False,
        show_benefit: bool = False,
    ) -> pd.DataFrame:
        """Process experiment data into format suitable for plotting."""
        plot_data = []

        for exp_data in experiment_data:
            exp_name = exp_data["experiment_name"]
            data_scale = exp_data["data_scale"]
            latency_data = exp_data["latency_data"]
            config = exp_data["config"]

            # Get query information - flatten queries from all query groups
            query_groups = config["query_groups"]
            queries = []
            for query_group in query_groups:
                queries.extend(query_group["queries"])

            # Process data for each server
            for server_name, server_data in latency_data.items():
                if server_name not in server_data:
                    print(
                        f"Warning: No data found for server {server_name} in {exp_name}"
                    )
                    continue

                server_latencies = server_data[server_name]

                if individual_queries:
                    # Process individual queries
                    for query_idx, query in enumerate(queries):
                        if query_idx not in server_latencies:
                            continue

                        latencies = [
                            latency
                            for latency in server_latencies[query_idx].get_latencies()
                            if latency is not None
                        ]
                        stats = calculate_latency_stats(latencies)

                        for metric, value in stats.items():
                            plot_data.append(
                                {
                                    "experiment_name": exp_name,
                                    "data_scale": data_scale,
                                    "server": server_name,
                                    "query_idx": query_idx,
                                    "query": query,
                                    "metric": metric,
                                    "latency": value,
                                }
                            )
                else:
                    # Process aggregated data across all queries
                    all_latencies = []
                    for query_idx in server_latencies:
                        latencies = [
                            latency
                            for latency in server_latencies[query_idx].get_latencies()
                            if latency is not None
                        ]
                        all_latencies.extend(latencies)

                    stats = calculate_latency_stats(all_latencies)

                    for metric, value in stats.items():
                        plot_data.append(
                            {
                                "experiment_name": exp_name,
                                "data_scale": data_scale,
                                "server": server_name,
                                "query_idx": -1,  # Indicates aggregated
                                "query": "All Queries",
                                "metric": metric,
                                "latency": value,
                            }
                        )

        df = pd.DataFrame(plot_data)

        # Calculate benefit (prometheus/sketchdb ratio) if requested
        if show_benefit and not df.empty:
            df = self._calculate_benefits(df)

        return df

    def _calculate_benefits(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate benefit ratios (prometheus latency / sketchdb latency)."""
        benefit_data = []

        # Group by experiment, query, and metric
        for (exp_name, query_idx, metric), group in df.groupby(
            ["experiment_name", "query_idx", "metric"]
        ):
            servers_data = {}
            data_scale = None
            query_name = None

            for _, row in group.iterrows():
                servers_data[row["server"]] = row["latency"]
                data_scale = row["data_scale"]
                query_name = row["query"]

            # Calculate benefit ratio if both servers have data
            if "baseline" in servers_data and "sketchdb" in servers_data:
                baseline_latency = servers_data["baseline"]
                sketchdb_latency = servers_data["sketchdb"]

                if sketchdb_latency > 0:
                    benefit_ratio = baseline_latency / sketchdb_latency
                else:
                    benefit_ratio = float("inf") if baseline_latency > 0 else 1.0

                benefit_data.append(
                    {
                        "experiment_name": exp_name,
                        "data_scale": data_scale,
                        "server": "benefit_ratio",  # New "server" for benefit
                        "query_idx": query_idx,
                        "query": query_name,
                        "metric": metric,
                        "latency": benefit_ratio,
                    }
                )

        return pd.DataFrame(benefit_data)


class BasePlotter(ABC):
    """Base class for plotting implementations."""

    @abstractmethod
    def create_plot(self, data: pd.DataFrame, **kwargs) -> "ggplot":
        """Create a plot from the data."""
        pass

    def save_plot(self, plot: "ggplot", filename: str, **kwargs):
        """Save plot to file."""
        ggsave(plot, filename, **kwargs)

    def show_plot(self, plot: "ggplot"):
        """Display plot."""
        print(plot)


class LatencyVsScalePlotter(BasePlotter):
    """Plotter for latency vs data scale."""

    def create_plot(self, data: pd.DataFrame, **kwargs) -> "ggplot":
        """Create latency vs scale plot."""
        # Determine if we're plotting benefit ratios
        is_benefit_plot = "benefit_ratio" in data["server"].unique()

        if is_benefit_plot:
            # Benefit ratio plot
            p = (
                ggplot(data, aes(x="data_scale", y="latency", color="metric"))
                + geom_line(size=1.2)
                + geom_point(size=2)
                + scale_color_discrete(name="Latency Metric")
                + labs(
                    title="Latency Benefit (Prometheus/SketchDB Ratio) vs Data Scale",
                    x="Data Scale (ports × instances × cardinality^labels)",
                    y="Latency Benefit Ratio (prometheus/sketchdb)",
                )
                + theme_minimal()
                + theme(
                    legend_position="right",
                    plot_title=element_text(size=14, weight="bold"),
                    axis_title=element_text(size=12),
                    legend_title=element_text(size=11),
                )
            )
        else:
            # Regular latency plot
            p = (
                ggplot(
                    data,
                    aes(x="data_scale", y="latency", color="metric", linetype="server"),
                )
                + geom_line(size=1.2)
                + geom_point(size=2)
                + scale_color_discrete(name="Latency Metric")
                + scale_linetype_discrete(name="Server")
                + labs(
                    title="Query Latency vs Data Scale",
                    x="Data Scale (ports × instances × cardinality^labels)",
                    y="Latency (seconds)",
                )
                + theme_minimal()
                + theme(
                    legend_position="right",
                    plot_title=element_text(size=14, weight="bold"),
                    axis_title=element_text(size=12),
                    legend_title=element_text(size=11),
                )
            )

        # Add faceting for individual queries if requested
        if "individual_queries" in kwargs and kwargs["individual_queries"]:
            if len(data["query_idx"].unique()) > 1:
                p = p + facet_wrap("query", scales="free")

        return p


def print_data_summary(data: pd.DataFrame, experiment_data: List[Dict[str, Any]]):
    """Print summary of the extracted data."""
    print("\nData Summary:")
    print("=" * 80)

    # Print experiment info
    print(f"Experiments analyzed: {len(experiment_data)}")
    for exp_data in experiment_data:
        exp_name = exp_data["experiment_name"]
        data_scale = exp_data["data_scale"]
        servers = list(exp_data["latency_data"].keys())
        print(f"  {exp_name}: scale={data_scale:,.0f}, servers={servers}")

    print(f"\nTotal data points: {len(data)}")
    print(f"Servers: {data['server'].unique().tolist()}")
    print(f"Metrics: {data['metric'].unique().tolist()}")

    if len(data["query_idx"].unique()) > 1:
        print(f"Queries: {len(data['query_idx'].unique())} individual queries")
    else:
        print("Data: Aggregated across all queries")

    # Check if this is benefit data or raw latency data
    is_benefit_data = "benefit_ratio" in data["server"].unique()

    if is_benefit_data:
        print("\nBenefit Summary (prometheus/sketchdb ratio):")
        summary = data.groupby(["metric"])["latency"].agg(["mean", "std"]).round(2)
    else:
        print("\nLatency Summary (seconds):")
        summary = (
            data.groupby(["server", "metric"])["latency"].agg(["mean", "std"]).round(4)
        )

    print(summary)


def main():
    parser = argparse.ArgumentParser(
        description="Plot query latency metrics against data scale",
        epilog="""
Examples:
  # Print raw latency summary
  python3 plot_latency_metrics.py exp1 exp2 --print --aggregated

  # Plot and save benefit ratios
  python3 plot_latency_metrics.py exp1 exp2 exp3 --plot --save benefit.png --aggregated --benefit

  # Individual queries with raw latencies
  python3 plot_latency_metrics.py exp1 exp2 --plot --show --individual-queries
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "experiments", nargs="+", help="List of experiment names to analyze"
    )
    parser.add_argument("--print", action="store_true", help="Print data summary")
    parser.add_argument("--plot", action="store_true", help="Generate plots")
    parser.add_argument("--save", type=str, help="Save plot to file (provide filename)")
    parser.add_argument("--show", action="store_true", help="Display plot")
    parser.add_argument(
        "--individual-queries",
        action="store_true",
        help="Plot individual query latencies",
    )
    parser.add_argument(
        "--aggregated",
        action="store_true",
        help="Plot aggregated latencies across all queries",
    )
    parser.add_argument(
        "--benefit",
        action="store_true",
        help="Show benefit ratios (prometheus/sketchdb) instead of raw latencies",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.plot and not (args.save or args.show):
        parser.error("--plot requires either --save or --show (or both)")

    if not (args.individual_queries or args.aggregated):
        # Default to aggregated if neither specified
        args.aggregated = True

    # Extract data
    print(f"Extracting data from {len(args.experiments)} experiments...")
    extractor = DataExtractor(args.experiments)
    experiment_data = extractor.extract_experiment_data()

    if not experiment_data:
        print("Error: No valid experiment data found")
        return 1

    # Process data
    processor = DataProcessor()

    # Process and optionally plot for different configurations
    if args.individual_queries:
        print("\nProcessing individual query data...")
        data_individual = processor.process_for_plotting(
            experiment_data, individual_queries=True, show_benefit=args.benefit
        )

        if args.print:
            data_type = (
                "Individual Query Benefit Data"
                if args.benefit
                else "Individual Query Data"
            )
            print(f"\n--- {data_type} ---")
            print_data_summary(data_individual, experiment_data)

        if args.plot and not data_individual.empty:
            plotter = LatencyVsScalePlotter()
            plot = plotter.create_plot(
                data_individual, individual_queries=True, show_benefit=args.benefit
            )

            if args.save:
                prefix = "benefit_individual" if args.benefit else "individual"
                filename = args.save if not args.aggregated else f"{prefix}_{args.save}"
                plotter.save_plot(plot, filename)
                print(f"Individual queries plot saved to: {filename}")

            if args.show:
                plotter.show_plot(plot)

    if args.aggregated:
        print("\nProcessing aggregated data...")
        data_aggregated = processor.process_for_plotting(
            experiment_data, individual_queries=False, show_benefit=args.benefit
        )

        if args.print:
            data_type = "Aggregated Benefit Data" if args.benefit else "Aggregated Data"
            print(f"\n--- {data_type} ---")
            print_data_summary(data_aggregated, experiment_data)

        if args.plot and not data_aggregated.empty:
            plotter = LatencyVsScalePlotter()
            plot = plotter.create_plot(
                data_aggregated, individual_queries=False, show_benefit=args.benefit
            )

            if args.save:
                prefix = "benefit_aggregated" if args.benefit else "aggregated"
                filename = (
                    args.save
                    if not args.individual_queries
                    else f"{prefix}_{args.save}"
                )
                plotter.save_plot(plot, filename)
                print(f"Aggregated plot saved to: {filename}")

            if args.show:
                plotter.show_plot(plot)

    return 0


if __name__ == "__main__":
    sys.exit(main())

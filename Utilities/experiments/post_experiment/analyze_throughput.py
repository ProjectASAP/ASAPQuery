#!/usr/bin/env python3
"""
Analyze throughput from Prometheus and Arroyo experiment outputs.

This script calculates throughput rates from cumulative sample counts and
provides stable throughput measurements by averaging over multiple time windows.
"""
import os
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class ThroughputAnalyzer:
    """Analyzes throughput from prometheus metrics."""

    def __init__(self, window_duration: int = 30, num_windows: int = 10):
        """
        Initialize the throughput analyzer.

        Args:
            window_duration: Duration in seconds for each rate measurement window
            num_windows: Number of windows to average for stable throughput calculation
        """
        self.window_duration = window_duration
        self.num_windows = num_windows

    def load_prometheus_metrics(self, file_path: Path) -> Dict:
        """Load prometheus throughput metrics from JSON file."""
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            raise

    def extract_timeseries(
        self,
        data: Dict,
        metric_name: str,
        label_filter: Optional[Dict[str, str]] = None,
    ) -> List[Tuple[float, float]]:
        """
        Extract timeseries data from prometheus metrics.

        Args:
            data: Loaded prometheus metrics data
            metric_name: Name of the metric to extract
            label_filter: Dict of label key-value pairs to filter on (e.g., {"type": "float"})

        Returns:
            List of (timestamp_seconds, value) tuples, sorted by timestamp
        """
        if (
            label_filter is None
            and metric_name == "prometheus_tsdb_head_samples_appended_total"
        ):
            label_filter = {"type": "float"}  # Default to float type only

        timeseries = []
        collection_start = datetime.fromisoformat(data["collection_start"])

        for measurement in data.get("measurements", []):
            if metric_name not in measurement.get("metrics", {}):
                continue

            metric_entries = measurement["metrics"][metric_name]

            for entry in metric_entries:
                # Skip entries with errors or null values
                if entry.get("error") or entry.get("value") is None:
                    continue

                # Check if labels match filter
                entry_labels = entry.get("labels", {})
                if label_filter is None or all(
                    entry_labels.get(k) == v for k, v in label_filter.items()
                ):
                    timestamp = datetime.fromisoformat(entry["timestamp"])
                    timestamp_seconds = (timestamp - collection_start).total_seconds()
                    timeseries.append((timestamp_seconds, float(entry["value"])))

        # Sort by timestamp and remove duplicates
        timeseries.sort(key=lambda x: x[0])

        if not timeseries:
            logger.warning(
                f"No data found for metric {metric_name} with filter {label_filter}"
            )

        return timeseries

    def calculate_rates(
        self,
        timeseries: List[Tuple[float, float]],
        window_duration: Optional[int] = None,
    ) -> List[Tuple[float, float]]:
        """
        Calculate rate (samples/sec) between measurements.

        Args:
            timeseries: List of (timestamp, cumulative_value) tuples
            window_duration: If provided, only calculate rates for pairs separated by
                           approximately this duration (in seconds)

        Returns:
            List of (timestamp, rate) tuples where timestamp is the end of the interval
        """
        if len(timeseries) < 2:
            logger.warning("Not enough data points to calculate rates")
            return []

        rates = []

        if window_duration is None:
            # Calculate rate between consecutive points
            for i in range(1, len(timeseries)):
                t1, v1 = timeseries[i - 1]
                t2, v2 = timeseries[i]
                time_diff = t2 - t1

                if time_diff > 0:
                    rate = (v2 - v1) / time_diff
                    rates.append((t2, rate))
        else:
            # Calculate rates over specific window durations
            # For each point, find the closest point approximately window_duration seconds earlier
            tolerance = window_duration * 0.2  # 20% tolerance

            for i in range(len(timeseries)):
                t_current, v_current = timeseries[i]
                target_time = t_current - window_duration

                # Find closest earlier point to target_time
                best_idx = None
                best_diff = float("inf")

                for j in range(i):
                    t_prev, _ = timeseries[j]
                    diff = abs(t_prev - target_time)

                    if (
                        diff < best_diff
                        and t_current - t_prev >= window_duration - tolerance
                    ):
                        best_diff = diff
                        best_idx = j

                if best_idx is not None:
                    t_prev, v_prev = timeseries[best_idx]
                    time_diff = t_current - t_prev

                    if time_diff > 0:
                        rate = (v_current - v_prev) / time_diff
                        rates.append((t_current, rate))

        return rates

    def calculate_stable_throughput(
        self, rates: List[Tuple[float, float]], num_windows: Optional[int] = None
    ) -> float:
        """
        Calculate stable throughput by averaging the last N rate measurements.

        Args:
            rates: List of (timestamp, rate) tuples
            num_windows: Number of last measurements to average (defaults to self.num_windows)

        Returns:
            Average rate over the last num_windows measurements
        """
        if num_windows is None:
            num_windows = self.num_windows

        if len(rates) < num_windows:
            logger.warning(
                f"Only {len(rates)} rate measurements available, less than requested {num_windows}"
            )
            num_windows = len(rates)

        if num_windows == 0:
            return 0.0

        last_rates = [rate for _, rate in rates[-num_windows:]]
        return sum(last_rates) / len(last_rates)

    def analyze_prometheus(self, file_path: Path) -> Dict:
        """
        Analyze prometheus throughput from metrics file.

        Returns:
            Dict containing analysis results
        """
        logger.info(f"Loading prometheus metrics from {file_path}")
        data = self.load_prometheus_metrics(file_path)

        logger.info("Extracting timeseries data...")

        metrics = [
            "prometheus_tsdb_head_samples_appended_total",
            "prometheus_remote_storage_samples_total",
        ]

        results = {}

        for metric in metrics:
            logger.info(f"Trying to extract metric: {metric}")
            timeseries = self.extract_timeseries(data, metric_name=metric)

            if not timeseries:
                logger.error("No valid timeseries data found")
                return {"error": "No valid data"}

            logger.info(
                f"Found {len(timeseries)} data points spanning {timeseries[-1][0] - timeseries[0][0]:.1f} seconds"
            )

            # Calculate instant rates (between consecutive measurements)
            instant_rates = self.calculate_rates(timeseries, window_duration=None)

            # Calculate windowed rates
            windowed_rates = self.calculate_rates(
                timeseries, window_duration=self.window_duration
            )

            # Calculate stable throughput
            stable_throughput = self.calculate_stable_throughput(
                windowed_rates, num_windows=self.num_windows
            )

            results[metric] = {
                "file": str(file_path),
                "data_points": len(timeseries),
                "duration_seconds": (
                    timeseries[-1][0] - timeseries[0][0] if timeseries else 0
                ),
                "instant_rates": instant_rates,
                "windowed_rates": windowed_rates,
                "window_duration": self.window_duration,
                "num_windows_for_stable": self.num_windows,
                "stable_throughput_samples_per_sec": stable_throughput,
            }

        return results

    def print_results(self, results: Dict):
        """Print analysis results in a readable format."""
        if "error" in results:
            print(f"\nError: {results['error']}")
            return

        print("\n" + "=" * 60)
        print("PROMETHEUS THROUGHPUT ANALYSIS")
        print("=" * 60)
        print(f"\nFile: {results['file']}")
        print(f"Data points: {results['data_points']}")
        print(f"Duration: {results['duration_seconds']:.1f} seconds")
        print(f"\nRate calculation window: {results['window_duration']} seconds")
        print(
            f"Number of windows for stable throughput: {results['num_windows_for_stable']}"
        )

        windowed_rates = results["windowed_rates"]
        if windowed_rates:
            rates_only = [rate for _, rate in windowed_rates]
            print("\nWindowed rate statistics:")
            print(f"  Min: {min(rates_only):,.1f} samples/sec")
            print(f"  Max: {max(rates_only):,.1f} samples/sec")
            print(f"  Mean: {sum(rates_only)/len(rates_only):,.1f} samples/sec")
            print(
                f"  Last {min(len(rates_only), results['num_windows_for_stable'])} windows: {', '.join(f'{r:,.1f}' for r in rates_only[-results['num_windows_for_stable']:])}"
            )

        print(f"\n{'='*60}")
        print(
            f"STABLE THROUGHPUT: {results['stable_throughput_samples_per_sec']:,.2f} samples/sec"
        )
        print(f"{'='*60}\n")

    def plot_throughput(self, results: Dict, output_file: Optional[Path] = None):
        """
        Plot throughput over time.

        Args:
            results: Analysis results dict
            output_file: If provided, save plot to this file. Otherwise, display interactively.
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            logger.error(
                "matplotlib is required for plotting. Install with: pip install matplotlib"
            )
            return

        if "error" in results:
            logger.error("Cannot plot: analysis contains errors")
            return

        instant_rates = results["instant_rates"]
        windowed_rates = results["windowed_rates"]
        stable_throughput = results["stable_throughput_samples_per_sec"]

        if not instant_rates and not windowed_rates:
            logger.error("No rate data to plot")
            return

        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot instant rates (lighter, more volatile)
        if instant_rates:
            times_instant, rates_instant = zip(*instant_rates)
            ax.plot(
                times_instant,
                rates_instant,
                "o-",
                alpha=0.3,
                markersize=2,
                label="Instant rate (consecutive points)",
                color="lightblue",
            )

        # Plot windowed rates (darker, smoother)
        if windowed_rates:
            times_windowed, rates_windowed = zip(*windowed_rates)
            ax.plot(
                times_windowed,
                rates_windowed,
                "o-",
                alpha=0.7,
                markersize=4,
                label=f'{results["window_duration"]}s window rate',
                color="blue",
                linewidth=2,
            )

        # Plot stable throughput line
        ax.axhline(
            y=stable_throughput,
            color="red",
            linestyle="--",
            linewidth=2,
            label=f'Stable throughput (last {results["num_windows_for_stable"]} windows): {stable_throughput:,.0f} samples/sec',
        )

        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        ax.set_ylabel("Throughput (samples/sec)", fontsize=12)
        ax.set_title("Prometheus Throughput Over Time", fontsize=14, fontweight="bold")
        ax.legend(loc="best")
        ax.grid(True, alpha=0.3)

        # Format y-axis with thousand separators
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x):,}"))

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=150, bbox_inches="tight")
            logger.info(f"Plot saved to {output_file}")
        else:
            plt.show()


class ArroyoThroughputAnalyzer(ThroughputAnalyzer):
    """Analyzes throughput from Arroyo metrics."""

    def load_arroyo_metrics(self, file_path: Path) -> Dict:
        """Load Arroyo throughput metrics from JSON file."""
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            raise

    def find_pipeline_graph_file(self, metrics_file_path: Path) -> Optional[Path]:
        """
        Find the pipeline graph JSON file in the same directory as the metrics file.

        Args:
            metrics_file_path: Path to the throughput metrics file

        Returns:
            Path to the pipeline graph file, or None if not found
        """
        metrics_dir = metrics_file_path.parent
        # Look for pipeline_graph_*.json files
        graph_files = list(metrics_dir.glob("pipeline_graph_*.json"))

        if not graph_files:
            logger.warning(f"No pipeline graph file found in {metrics_dir}")
            return None

        if len(graph_files) > 1:
            logger.warning(
                f"Multiple pipeline graph files found, using {graph_files[0]}"
            )

        return graph_files[0]

    def find_node_by_description(
        self, pipeline_graph_path: Path, description_pattern: str
    ) -> Optional[int]:
        """
        Find a node ID by searching for a description pattern in the pipeline graph.

        Args:
            pipeline_graph_path: Path to the pipeline graph JSON file
            description_pattern: String pattern to search for in node descriptions

        Returns:
            Node ID if found, None otherwise
        """
        try:
            with open(pipeline_graph_path, "r") as f:
                graph_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading pipeline graph: {e}")
            return None

        nodes = graph_data.get("nodes", {})

        for node_id_str, node_info in nodes.items():
            description = node_info.get("description", "")
            if description_pattern in description:
                logger.info(
                    f"Found node matching '{description_pattern}': node_id={node_id_str}, description='{description}'"
                )
                return int(node_id_str)

        logger.warning(
            f"No node found with description containing '{description_pattern}'"
        )
        return None

    def extract_arroyo_timeseries(
        self,
        data: Dict,
        metric_type: str = "messages_sent",
        node_id: Optional[int] = None,
        aggregate_nodes: bool = True,
    ) -> List[Tuple[float, float]]:
        """
        Extract timeseries data from Arroyo metrics.

        Args:
            data: Loaded Arroyo metrics data
            metric_type: Type of metric to extract (messages_sent, messages_recv, bytes_sent, bytes_recv)
            node_id: If provided, only extract data for this specific node
            aggregate_nodes: If True, sum values across all nodes at each timestamp

        Returns:
            List of (timestamp_seconds, value) tuples, sorted by timestamp
        """
        # Track data points: map from time (microseconds) -> {node_id -> value}
        time_to_nodes: Dict[int, Dict[int, float]] = {}

        # Find the earliest timestamp to use as reference
        first_measurement = (
            data.get("measurements", [])[0] if data.get("measurements") else None
        )
        if not first_measurement:
            logger.warning("No measurements found in Arroyo data")
            return []

        reference_time = datetime.fromisoformat(first_measurement["timestamp"])

        for measurement in data.get("measurements", []):
            for job in measurement.get("jobs", []):
                metrics_data = job.get("metrics", {}).get(metric_type, [])

                for entry in metrics_data:
                    # Filter by node_id if specified
                    if node_id is not None and entry.get("node_id") != node_id:
                        continue

                    time_micros = entry.get("time")
                    value = entry.get("value", 0.0)
                    entry_node_id = entry.get("node_id")

                    if time_micros is None or entry_node_id is None:
                        continue

                    if time_micros not in time_to_nodes:
                        time_to_nodes[time_micros] = {}

                    time_to_nodes[time_micros][entry_node_id] = value

        # Convert to timeseries
        timeseries = []
        for time_micros in sorted(time_to_nodes.keys()):
            nodes_data = time_to_nodes[time_micros]

            if aggregate_nodes:
                # Sum across all nodes
                total_value = sum(nodes_data.values())
            else:
                # If we're not aggregating and no specific node was requested, skip
                if node_id is None:
                    continue
                total_value = nodes_data.get(node_id, 0.0)

            # Convert microseconds to seconds relative to reference time
            timestamp_seconds = (time_micros / 1_000_000.0) - reference_time.timestamp()
            timeseries.append((timestamp_seconds, total_value))

        if not timeseries:
            logger.warning(
                f"No data found for metric {metric_type}"
                + (f" and node {node_id}" if node_id is not None else "")
            )

        return timeseries

    def calculate_instantaneous_average(
        self,
        timeseries: List[Tuple[float, float]],
        window_duration: Optional[int] = None,
    ) -> List[Tuple[float, float]]:
        """
        Calculate average of instantaneous values over windows.

        Args:
            timeseries: List of (timestamp, instantaneous_value) tuples
            window_duration: If provided, average values over this window duration

        Returns:
            List of (timestamp, average_value) tuples
        """
        if len(timeseries) < 2:
            logger.warning("Not enough data points to calculate averages")
            return timeseries

        if window_duration is None:
            # Return as-is for instantaneous values
            return timeseries

        # Calculate windowed averages
        averages = []
        tolerance = window_duration * 0.2  # 20% tolerance

        for i in range(len(timeseries)):
            t_current, _ = timeseries[i]

            # Find all points in the window
            window_values = []
            for j in range(i + 1):
                t_prev, v_prev = timeseries[j]
                if t_current - t_prev <= window_duration + tolerance:
                    window_values.append(v_prev)

            if window_values:
                avg_value = sum(window_values) / len(window_values)
                averages.append((t_current, avg_value))

        return averages

    def analyze_arroyo(
        self,
        file_path: Path,
        metric_type: str = "messages_recv",
        node_description: str = "prometheus_8080_fake_metric_total -> watermark",
        node_id: Optional[int] = None,
    ) -> Dict:
        """
        Analyze Arroyo throughput from metrics file.

        Args:
            file_path: Path to Arroyo metrics JSON file
            metric_type: Type of metric to analyze (messages_recv/bytes_recv are instantaneous,
                        messages_sent/bytes_sent are cumulative)
            node_description: Description pattern to search for in pipeline graph (ignored if node_id is provided)
            node_id: Specific node ID to analyze (if None, will search by node_description)

        Returns:
            Dict containing analysis results
        """
        logger.info(f"Loading Arroyo metrics from {file_path}")
        data = self.load_arroyo_metrics(file_path)

        pipeline_id = data.get("pipeline_id", "unknown")
        logger.info(f"Pipeline ID: {pipeline_id}")

        # Find node_id if not provided
        if node_id is None:
            graph_file = self.find_pipeline_graph_file(file_path)
            if graph_file:
                node_id = self.find_node_by_description(graph_file, node_description)
                if node_id is None:
                    logger.error(
                        f"Could not find node with description containing '{node_description}'"
                    )
                    return {"error": f"Node not found: {node_description}"}
            else:
                logger.error("Could not find pipeline graph file")
                return {"error": "Pipeline graph file not found"}

        logger.info(
            f"Extracting timeseries data for metric: {metric_type}, node_id: {node_id}..."
        )

        timeseries = self.extract_arroyo_timeseries(
            data, metric_type=metric_type, node_id=node_id, aggregate_nodes=False
        )

        if not timeseries:
            logger.error("No valid timeseries data found")
            return {"error": "No valid data"}

        logger.info(
            f"Found {len(timeseries)} data points spanning {timeseries[-1][0] - timeseries[0][0]:.1f} seconds"
        )

        # Determine if this is an instantaneous or cumulative metric
        # messages_recv and bytes_recv are instantaneous rates
        is_instantaneous = metric_type in ["messages_recv", "bytes_recv"]

        if is_instantaneous:
            logger.info(f"Treating {metric_type} as instantaneous throughput")
            # For instantaneous metrics, use values directly
            instant_rates = timeseries  # Already instantaneous values

            # Calculate windowed averages
            windowed_rates = self.calculate_instantaneous_average(
                timeseries, window_duration=self.window_duration
            )
        else:
            logger.info(
                f"Treating {metric_type} as cumulative metric, calculating rates"
            )
            # Calculate instant rates (between consecutive measurements)
            instant_rates = self.calculate_rates(timeseries, window_duration=None)

            # Calculate windowed rates
            windowed_rates = self.calculate_rates(
                timeseries, window_duration=self.window_duration
            )

        # Calculate stable throughput
        stable_throughput = self.calculate_stable_throughput(
            windowed_rates, num_windows=self.num_windows
        )

        return {
            "file": str(file_path),
            "pipeline_id": pipeline_id,
            "metric_type": metric_type,
            "node_id": node_id,
            "node_description": node_description,
            "is_instantaneous": is_instantaneous,
            "data_points": len(timeseries),
            "duration_seconds": (
                timeseries[-1][0] - timeseries[0][0] if timeseries else 0
            ),
            "instant_rates": instant_rates,
            "windowed_rates": windowed_rates,
            "window_duration": self.window_duration,
            "num_windows_for_stable": self.num_windows,
            "stable_throughput_per_sec": stable_throughput,
        }

    def print_arroyo_results(self, results: Dict):
        """Print Arroyo analysis results in a readable format."""
        if "error" in results:
            print(f"\nError: {results['error']}")
            return

        metric_type = results.get("metric_type", "messages_sent")
        is_instantaneous = results.get("is_instantaneous", False)
        unit = "messages/sec" if "messages" in metric_type else "bytes/sec"

        print("\n" + "=" * 60)
        print("ARROYO THROUGHPUT ANALYSIS")
        print("=" * 60)
        print(f"\nFile: {results['file']}")
        print(f"Pipeline ID: {results.get('pipeline_id', 'unknown')}")
        print(f"Node ID: {results.get('node_id', 'N/A')}")
        print(f"Node Description: {results.get('node_description', 'N/A')}")
        print(
            f"Metric type: {metric_type} ({'instantaneous' if is_instantaneous else 'cumulative'})"
        )
        print(f"Data points: {results['data_points']}")
        print(f"Duration: {results['duration_seconds']:.1f} seconds")

        if is_instantaneous:
            print(f"\nAveraging window: {results['window_duration']} seconds")
        else:
            print(f"\nRate calculation window: {results['window_duration']} seconds")

        print(
            f"Number of windows for stable throughput: {results['num_windows_for_stable']}"
        )

        windowed_rates = results["windowed_rates"]
        if windowed_rates:
            rates_only = [rate for _, rate in windowed_rates]

            if is_instantaneous:
                print("\nWindowed average statistics:")
            else:
                print("\nWindowed rate statistics:")

            print(f"  Min: {min(rates_only):,.1f} {unit}")
            print(f"  Max: {max(rates_only):,.1f} {unit}")
            print(f"  Mean: {sum(rates_only)/len(rates_only):,.1f} {unit}")
            print(
                f"  Last {min(len(rates_only), results['num_windows_for_stable'])} windows: {', '.join(f'{r:,.1f}' for r in rates_only[-results['num_windows_for_stable']:])}"
            )

        print(f"\n{'='*60}")
        print(f"STABLE THROUGHPUT: {results['stable_throughput_per_sec']:,.2f} {unit}")
        print(f"{'='*60}\n")

    def plot_arroyo_throughput(self, results: Dict, output_file: Optional[Path] = None):
        """
        Plot Arroyo throughput over time.

        Args:
            results: Analysis results dict
            output_file: If provided, save plot to this file. Otherwise, display interactively.
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            logger.error(
                "matplotlib is required for plotting. Install with: pip install matplotlib"
            )
            return

        if "error" in results:
            logger.error("Cannot plot: analysis contains errors")
            return

        metric_type = results.get("metric_type", "messages_sent")
        is_instantaneous = results.get("is_instantaneous", False)
        unit = "messages/sec" if "messages" in metric_type else "bytes/sec"

        instant_rates = results["instant_rates"]
        windowed_rates = results["windowed_rates"]
        stable_throughput = results["stable_throughput_per_sec"]

        if not instant_rates and not windowed_rates:
            logger.error("No rate data to plot")
            return

        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot instant values/rates (lighter, more volatile)
        if instant_rates:
            times_instant, rates_instant = zip(*instant_rates)
            instant_label = (
                "Instantaneous values"
                if is_instantaneous
                else "Instant rate (consecutive points)"
            )
            ax.plot(
                times_instant,
                rates_instant,
                "o-",
                alpha=0.3,
                markersize=2,
                label=instant_label,
                color="lightblue",
            )

        # Plot windowed values/rates (darker, smoother)
        if windowed_rates:
            times_windowed, rates_windowed = zip(*windowed_rates)
            windowed_label = (
                f'{results["window_duration"]}s window average'
                if is_instantaneous
                else f'{results["window_duration"]}s window rate'
            )
            ax.plot(
                times_windowed,
                rates_windowed,
                "o-",
                alpha=0.7,
                markersize=4,
                label=windowed_label,
                color="blue",
                linewidth=2,
            )

        # Plot stable throughput line
        ax.axhline(
            y=stable_throughput,
            color="red",
            linestyle="--",
            linewidth=2,
            label=f'Stable throughput (last {results["num_windows_for_stable"]} windows): {stable_throughput:,.0f} {unit}',
        )

        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        ax.set_ylabel(f"Throughput ({unit})", fontsize=12)

        node_info = f"Node {results.get('node_id', 'N/A')}"
        if results.get("node_description"):
            node_info += f": {results['node_description']}"

        ax.set_title(
            f"Arroyo Throughput Over Time\n{metric_type} - {node_info}",
            fontsize=14,
            fontweight="bold",
        )
        ax.legend(loc="best")
        ax.grid(True, alpha=0.3)

        # Format y-axis with thousand separators
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x):,}"))

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=150, bbox_inches="tight")
            logger.info(f"Plot saved to {output_file}")
        else:
            plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze throughput from Prometheus and Arroyo experiment outputs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic Prometheus analysis
  %(prog)s /path/to/prometheus_throughput_metrics.json

  # With custom window parameters
  %(prog)s --window-duration 60 --num-windows 5 /path/to/metrics.json

  # Generate plot
  %(prog)s --plot /path/to/metrics.json

  # Save plot to file
  %(prog)s --plot --plot-output throughput_plots /path/to/metrics.json

  # Analyze Arroyo (auto-detects watermark node, uses instantaneous messages_recv)
  %(prog)s --type arroyo /path/to/arroyo_metrics.json

  # Analyze Arroyo with cumulative metric type
  %(prog)s --type arroyo --metric-type messages_sent /path/to/arroyo_metrics.json

  # Analyze specific Arroyo node by description (e.g., output sink)
  %(prog)s --type arroyo --node-description "KafkaSink" /path/to/arroyo_metrics.json

  # Analyze specific Arroyo node by ID
  %(prog)s --type arroyo --node-id 27 /path/to/arroyo_metrics.json

  # Arroyo analysis with plot
  %(prog)s --type arroyo --plot --plot-output arroyo_plots /path/to/arroyo_metrics.json
        """,
    )

    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to prometheus or arroyo throughput metrics JSON file",
    )

    parser.add_argument(
        "--type",
        choices=["prometheus", "arroyo"],
        default="prometheus",
        help="Type of metrics to analyze (default: prometheus)",
    )

    parser.add_argument(
        "--window-duration",
        type=int,
        default=30,
        help="Duration in seconds for each rate measurement window (default: 30)",
    )

    parser.add_argument(
        "--num-windows",
        type=int,
        default=10,
        help="Number of windows to average for stable throughput (default: 10)",
    )

    parser.add_argument(
        "--plot", action="store_true", help="Generate a plot of throughput over time"
    )

    parser.add_argument(
        "--plot-output",
        type=Path,
        help="Save plot to this file (if not provided, plot is displayed interactively)",
    )

    parser.add_argument(
        "--metric-type",
        choices=["messages_sent", "messages_recv", "bytes_sent", "bytes_recv"],
        default="messages_recv",
        help="For Arroyo: which metric type to analyze. messages_recv and bytes_recv are instantaneous, messages_sent and bytes_sent are cumulative (default: messages_recv)",
    )

    parser.add_argument(
        "--node-description",
        type=str,
        default="prometheus_8080_fake_metric_total -> watermark",
        help="For Arroyo: node description pattern to search for (default: 'prometheus_8080_fake_metric_total -> watermark')",
    )

    parser.add_argument(
        "--node-id",
        type=int,
        default=None,
        help="For Arroyo: specific node ID to analyze (overrides --node-description if provided)",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate input file exists
    if not args.input_file.exists():
        logger.error(f"Input file does not exist: {args.input_file}")
        sys.exit(1)

    (
        os.makedirs(args.plot_output, exist_ok=True)
        if args.plot and args.plot_output
        else None
    )

    # Analyze based on type
    if args.type == "prometheus":
        analyzer = ThroughputAnalyzer(
            window_duration=args.window_duration, num_windows=args.num_windows
        )
        results = analyzer.analyze_prometheus(args.input_file)
        for k, v in results.items():
            analyzer.print_results(v)

            if args.plot:
                analyzer.plot_throughput(
                    v,
                    output_file=os.path.join(args.plot_output, f"_{k}_throughput.png"),
                )

    elif args.type == "arroyo":
        analyzer = ArroyoThroughputAnalyzer(
            window_duration=args.window_duration, num_windows=args.num_windows
        )
        results = analyzer.analyze_arroyo(
            args.input_file,
            metric_type=args.metric_type,
            node_description=args.node_description,
            node_id=args.node_id,
        )
        analyzer.print_arroyo_results(results)

        if args.plot:
            plot_file = None
            if args.plot_output:
                plot_file = os.path.join(
                    args.plot_output, f"arroyo_{args.metric_type}_throughput.png"
                )
            analyzer.plot_arroyo_throughput(results, output_file=plot_file)

    else:
        logger.error(f"Unknown type: {args.type}")
        sys.exit(1)


if __name__ == "__main__":
    main()

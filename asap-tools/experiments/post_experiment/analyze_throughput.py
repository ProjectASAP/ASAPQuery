#!/usr/bin/env python3
"""
Analyze throughput from Prometheus experiment outputs.

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


def main():
    parser = argparse.ArgumentParser(
        description="Analyze throughput from Prometheus experiment outputs",
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
        """,
    )

    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to prometheus throughput metrics JSON file",
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


if __name__ == "__main__":
    main()

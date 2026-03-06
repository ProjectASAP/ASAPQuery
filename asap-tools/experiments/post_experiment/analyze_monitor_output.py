#!/usr/bin/env python3
"""
Analyze monitor output JSON files to compute aggregate CPU and memory statistics.

For each keyword, this script:
1. Sums CPU and memory usage across all PIDs at each timestamp
2. Computes p95, p99, and max statistics across time
3. Prints the time series and summary statistics
4. Optionally plots CPU and memory usage with PIDs grouped by keyword
"""

import json
import sys
import argparse
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict

# Line styles for different keywords
LINE_STYLES = ["-", "--", "-.", ":", (0, (3, 1, 1, 1)), (0, (5, 2, 1, 2))]


def get_line_style_for_keyword(keyword, keyword_to_style):
    """
    Assign a consistent line style to each keyword.

    Args:
        keyword: The keyword to assign a style to
        keyword_to_style: Dictionary mapping keywords to line styles

    Returns:
        Line style string for matplotlib
    """
    if keyword not in keyword_to_style:
        keyword_to_style[keyword] = LINE_STYLES[
            len(keyword_to_style) % len(LINE_STYLES)
        ]
    return keyword_to_style[keyword]


def plot_resource_usage(data, file_path, args):
    """
    Plot CPU and memory usage with one line per PID.
    PIDs with the same keyword share the same line style.

    Args:
        data: Dictionary with PIDs as keys and monitoring data as values
        file_path: Path to the monitor_output.json file
        args: Command-line arguments
    """
    # Set global font size
    plt.rcParams.update({"font.size": 22})

    # Group PIDs by keyword and assign line styles
    keyword_to_style = {}
    keyword_to_pids = defaultdict(list)

    for pid, pid_info in data.items():
        keyword = pid_info["keyword"]
        keyword_to_pids[keyword].append(pid)
        get_line_style_for_keyword(keyword, keyword_to_style)

    # Create plots for each resource type
    resources = [
        ("cpu_percent", "CPU Usage (%)", "cpu"),
        ("memory_info", "Memory Usage (MB)", "memory"),
    ]

    for resource_key, resource_label, resource_name in resources:
        plt.figure(figsize=(20, 8))

        # Plot data for each PID
        for pid, pid_info in data.items():
            keyword = pid_info["keyword"]
            line_style = keyword_to_style[keyword]

            # Convert memory to MB if needed
            if resource_key == "memory_info":
                y_values = np.array(pid_info[resource_key]) / (1024 * 1024)
            else:
                y_values = pid_info[resource_key]

            x_values = list(range(len(y_values)))

            plt.plot(
                x_values,
                y_values,
                linestyle=line_style,
                label=f"{keyword} (PID: {pid})",
                linewidth=2,
            )

        # Add labels and title
        plt.ylabel(resource_label)
        plt.xlabel("Time (samples)")
        plt.title(f"{resource_name.upper()} Usage by PID")
        plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        # Save or show based on args
        if args.save:
            output_filename = f"{Path(file_path).stem}_{resource_name}.png"
            if args.save_to_experiment_dir:
                output_dir = Path(file_path).parent
            elif args.output_dir:
                output_dir = Path(args.output_dir)
            else:
                output_dir = Path.cwd()

            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / output_filename
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            print(f"Saved plot to {output_path}")

        if args.show:
            plt.show()
        else:
            plt.close()


def analyze_monitor_output(file_path: str, args=None):
    """
    Analyze monitor output JSON file and compute statistics.

    Args:
        file_path: Path to the monitor_output.json file
        args: Command-line arguments (optional)
    """
    # Load the JSON file
    with open(file_path, "r") as f:
        data = json.load(f)

    # Generate plots if requested
    if args and args.plot:
        plot_resource_usage(data, file_path, args)

    # Group PIDs by keyword
    keyword_data = {}
    for pid, pid_info in data.items():
        keyword = pid_info["keyword"]
        if keyword not in keyword_data:
            keyword_data[keyword] = {"cpu_percent": [], "memory_info": []}
        keyword_data[keyword]["cpu_percent"].append(pid_info["cpu_percent"])
        keyword_data[keyword]["memory_info"].append(pid_info["memory_info"])

    # Skip printing if --print not specified
    if not args or not args.print:
        return

    # Process each keyword
    for keyword, metrics in keyword_data.items():
        print(f"\n{'='*80}")
        print(f"Keyword: {keyword}")
        print(f"{'='*80}")

        # Convert to numpy arrays for easier manipulation
        cpu_arrays = [np.array(cpu) for cpu in metrics["cpu_percent"]]
        mem_arrays = [np.array(mem) for mem in metrics["memory_info"]]

        # Find the maximum length across all PIDs
        max_len = max(len(arr) for arr in cpu_arrays)

        # Pad arrays to the same length (with zeros for missing data)
        # This handles cases where different PIDs have different numbers of samples
        cpu_padded = np.zeros((len(cpu_arrays), max_len))
        mem_padded = np.zeros((len(mem_arrays), max_len))

        for i, arr in enumerate(cpu_arrays):
            cpu_padded[i, : len(arr)] = arr
        for i, arr in enumerate(mem_arrays):
            mem_padded[i, : len(arr)] = arr

        # Sum across PIDs at each timestamp
        cpu_sum = np.sum(cpu_padded, axis=0)
        mem_sum = np.sum(mem_padded, axis=0)

        # Convert memory from bytes to MB for readability
        mem_sum_mb = mem_sum / (1024 * 1024)

        # Compute statistics
        cpu_median = np.median(cpu_sum)
        cpu_p95 = np.percentile(cpu_sum, 95)
        cpu_p99 = np.percentile(cpu_sum, 99)
        cpu_max = np.max(cpu_sum)

        mem_median = np.median(mem_sum_mb)
        mem_p95 = np.percentile(mem_sum_mb, 95)
        mem_p99 = np.percentile(mem_sum_mb, 99)
        mem_max = np.max(mem_sum_mb)

        # Print CPU statistics
        print(f"\nCPU Usage (sum across {len(cpu_arrays)} PIDs):")
        print(f"  Median: {cpu_median:.2f}%")
        print(f"  P95: {cpu_p95:.2f}%")
        print(f"  P99: {cpu_p99:.2f}%")
        print(f"  Max: {cpu_max:.2f}%")

        print("\nCPU time series (sum across PIDs):")
        print(f"  Samples: {len(cpu_sum)}")
        print(f"  First 10 values: {cpu_sum[:10]}")
        if len(cpu_sum) > 20:
            print(f"  Last 10 values: {cpu_sum[-10:]}")
        else:
            print(f"  All values: {cpu_sum}")

        # Print Memory statistics
        print(f"\nMemory Usage in MB (sum across {len(mem_arrays)} PIDs):")
        print(f"  Median: {mem_median:.2f} MB")
        print(f"  P95: {mem_p95:.2f} MB")
        print(f"  P99: {mem_p99:.2f} MB")
        print(f"  Max: {mem_max:.2f} MB")

        print("\nMemory time series in MB (sum across PIDs):")
        print(f"  Samples: {len(mem_sum_mb)}")
        print(f"  First 10 values: {mem_sum_mb[:10]}")
        if len(mem_sum_mb) > 20:
            print(f"  Last 10 values: {mem_sum_mb[-10:]}")
        else:
            print(f"  All values: {mem_sum_mb}")

        # Optional: Print full time series to a separate file
        # output_dir = Path(file_path).parent
        # keyword_clean = keyword.replace('/', '_').replace('\\', '_')
        #
        # cpu_output = output_dir / f"{keyword_clean}_cpu_timeseries.txt"
        # mem_output = output_dir / f"{keyword_clean}_memory_timeseries.txt"
        #
        # with open(cpu_output, 'w') as f:
        #     f.write(f"# CPU Usage (%) - Sum across {len(cpu_arrays)} PIDs for keyword: {keyword}\n")
        #     f.write(f"# Timestamp_index\tCPU_percent\n")
        #     for i, val in enumerate(cpu_sum):
        #         f.write(f"{i}\t{val:.2f}\n")
        #
        # with open(mem_output, 'w') as f:
        #     f.write(f"# Memory Usage (MB) - Sum across {len(mem_arrays)} PIDs for keyword: {keyword}\n")
        #     f.write(f"# Timestamp_index\tMemory_MB\n")
        #     for i, val in enumerate(mem_sum_mb):
        #         f.write(f"{i}\t{val:.2f}\n")
        #
        # print(f"\nTime series data written to:")
        # print(f"  CPU: {cpu_output}")
        # print(f"  Memory: {mem_output}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze monitor output JSON files and compute/plot statistics"
    )
    parser.add_argument(
        "file_path", type=str, help="Path to the monitor_output.json file"
    )
    parser.add_argument(
        "--print", action="store_true", help="Print statistics to stdout"
    )
    parser.add_argument(
        "--plot", action="store_true", help="Generate plots for CPU and memory usage"
    )
    parser.add_argument(
        "--save", action="store_true", help="Save plots to files (requires --plot)"
    )
    parser.add_argument(
        "--show", action="store_true", help="Display plots on screen (requires --plot)"
    )
    parser.add_argument(
        "--output-dir", type=str, help="Directory to save plots (requires --save)"
    )
    parser.add_argument(
        "--save-to-experiment-dir",
        action="store_true",
        help="Save plots to the same directory as the input file (requires --save)",
    )

    args = parser.parse_args()

    # Validation
    if not args.print and not args.plot:
        parser.error("Must specify at least one of --print or --plot")

    if args.plot and not args.show and not args.save:
        parser.error("When using --plot, must specify at least one of --show or --save")

    if args.save and not args.plot:
        parser.error("--save requires --plot")

    if args.save and not args.save_to_experiment_dir and not args.output_dir:
        parser.error(
            "When using --save, must specify either --save-to-experiment-dir or --output-dir"
        )

    if not Path(args.file_path).exists():
        print(f"Error: File not found: {args.file_path}")
        sys.exit(1)

    analyze_monitor_output(args.file_path, args)


if __name__ == "__main__":
    main()

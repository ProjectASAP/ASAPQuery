#!/usr/bin/env python3
"""
Script to plot a bar graph comparing latency or cost reduction across different queries
against Prometheus and VictoriaMetrics.
"""

import argparse
import matplotlib.pyplot as plt
import numpy as np

# Configuration
QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5"]  # X-axis labels
METRIC_NAME = "Latency Reduction"  # Y-axis label (can be "Cost Reduction")
Y_LABEL = "Reduction Factor (log scale)"
FONTSIZE = 16

# Data: Dictionary of {query: {system: value}}
# Values represent the reduction factor (e.g., 31.02 means 31.02X reduction)
DATA = {
    # result_1_quantile_1s_1
    # result_1_quantile_1s_4_vm
    "Q1": {"Prometheus": 3805.7, "VictoriaMetrics": 3576.6},
    # result_1_non_quantile_1s_1
    "Q2": {"Prometheus": 1341.1, "VictoriaMetrics": 1.0},
    "Q3": {"Prometheus": 31.02, "VictoriaMetrics": 1.0},
    "Q4": {"Prometheus": 1.0, "VictoriaMetrics": 1.0},
    # result_1_collapsable_3
    # result_1_collapsable_3_vm_2
    "Q5": {"Prometheus": 28.3, "VictoriaMetrics": 23.2},
}


def print_data_summary(queries, data, metric_name):
    """Print summary of the data."""
    print("\nData Summary:")
    print("=" * 80)
    print(f"Metric: {metric_name}")
    print(f"Queries: {', '.join(queries)}")
    print("\nReduction Factors:")
    print(f"{'Query':<10} {'Prometheus':<20} {'VictoriaMetrics':<20}")
    print("-" * 50)

    for query in queries:
        prom_val = data[query]["Prometheus"]
        vm_val = data[query]["VictoriaMetrics"]
        print(f"{query:<10} {prom_val:<20.2f} {vm_val:<20.2f}")

    print("\nStatistics:")
    prom_values = [data[q]["Prometheus"] for q in queries]
    vm_values = [data[q]["VictoriaMetrics"] for q in queries]

    print(
        f"  Prometheus - Mean: {np.mean(prom_values):.2f}, Max: {np.max(prom_values):.2f}, Min: {np.min(prom_values):.2f}"
    )
    print(
        f"  VictoriaMetrics - Mean: {np.mean(vm_values):.2f}, Max: {np.max(vm_values):.2f}, Min: {np.min(vm_values):.2f}"
    )
    print("=" * 80)


def plot_comparison_bars(
    queries=QUERIES,
    data=DATA,
    metric_name=METRIC_NAME,
    y_label=Y_LABEL,
    save_file=None,
    show=False,
):
    """
    Plot a bar graph comparing reduction factors across queries and systems.

    Args:
        queries: List of query names for x-axis
        data: Dictionary of {query: {system: value}}
        metric_name: Name of the metric being plotted (for title)
        y_label: Label for y-axis
        save_file: Filename to save the plot (if None, doesn't save)
        show: Whether to display the plot

    Returns:
        matplotlib figure object
    """
    # Extract data for plotting
    prom_values = [data[q]["Prometheus"] for q in queries]
    vm_values = [data[q]["VictoriaMetrics"] for q in queries]

    # Set up the bar positions
    x = np.arange(len(queries))
    width = 0.35  # Width of bars

    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 6))

    # Create bars
    bars1 = ax.bar(
        x - width / 2,
        prom_values,
        width,
        label="Compared to Prometheus",
        color="#1f77b4",
        alpha=0.8,
    )
    bars2 = ax.bar(
        x + width / 2,
        vm_values,
        width,
        label="Compared to VictoriaMetrics",
        color="#ff7f0e",
        alpha=0.8,
    )

    # Set y-axis to log scale
    ax.set_yscale("log")

    # Customize the plot
    ax.set_xlabel("Query", fontsize=FONTSIZE, fontweight="bold")
    ax.set_ylabel(y_label, fontsize=FONTSIZE, fontweight="bold")
    ax.set_title(f"{metric_name} Comparison", fontsize=FONTSIZE, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(queries, fontsize=FONTSIZE)
    ax.legend(loc="upper right", fontsize=FONTSIZE)
    ax.grid(True, alpha=0.3, axis="y")

    # Set tick label font sizes
    ax.tick_params(axis="both", which="major", labelsize=FONTSIZE)

    # Add value labels on top of bars (optional, can be removed if cluttered)
    def add_value_labels(bars):
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{height:.2f}X" if height != 1.0 else "1X",
                ha="center",
                va="bottom",
                fontsize=FONTSIZE,
                fontweight="bold",
            )

    add_value_labels(bars1)
    add_value_labels(bars2)

    # Adjust layout
    plt.tight_layout()

    # Save if requested
    if save_file:
        plt.savefig(save_file, dpi=300, bbox_inches="tight")
        print(f"Plot saved as '{save_file}'")

    # Show if requested
    if show:
        plt.show()
    else:
        plt.close(fig)

    return fig


def main():
    parser = argparse.ArgumentParser(
        description="Plot bar graph comparing latency or cost reduction across queries",
        epilog="""
Examples:
  # Print data summary only
  python3 plot_comparison_bars.py --print

  # Plot and save to file
  python3 plot_comparison_bars.py --plot --save comparison_bars.png

  # Plot and show interactively
  python3 plot_comparison_bars.py --plot --show

  # Both print and plot
  python3 plot_comparison_bars.py --print --plot --save output.png --show
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--print", action="store_true", help="Print data summary")
    parser.add_argument("--plot", action="store_true", help="Generate plot")
    parser.add_argument(
        "--save",
        type=str,
        metavar="FILENAME",
        help="Save plot to file (provide filename)",
    )
    parser.add_argument("--show", action="store_true", help="Display plot")

    args = parser.parse_args()

    # Validate arguments
    if args.plot and not (args.save or args.show):
        parser.error("--plot requires either --save or --show (or both)")

    if not args.print and not args.plot:
        parser.error("At least one of --print or --plot must be specified")

    # Print summary if requested
    if args.print:
        print_data_summary(QUERIES, DATA, METRIC_NAME)

    # Generate plot if requested
    if args.plot:
        plot_comparison_bars(
            queries=QUERIES,
            data=DATA,
            metric_name=METRIC_NAME,
            y_label=Y_LABEL,
            save_file=args.save,
            show=args.show,
        )

    return 0


if __name__ == "__main__":
    exit(main())

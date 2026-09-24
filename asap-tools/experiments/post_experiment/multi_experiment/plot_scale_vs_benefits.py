#!/usr/bin/env python3
"""
Script to plot data scale vs benefits (prometheus/sketchdb ratios).
Shows how much faster and cheaper sketchdb is compared to prometheus.
X-axis: Data scale (metrics/sec) in log scale
Y-axes: Left = Latency Benefit (ratio), Right = Cost Benefit (ratio)
"""

import argparse
import json
import matplotlib.pyplot as plt
import numpy as np

# Import functions from the other script
from plot_scale_vs_metrics import (
    calculate_data_scale,
    get_latency_p95,
    get_cost_p95,
    get_query_cost_sum,
    get_query_cost_95,
)

# Configuration
EXPERIMENT_NAMES = [
    "quantile_1s_10queries_10valuesperlabel_2",
    "quantile_1s_10queries_20valuesperlabel_2",
    "quantile_1s_10queries_30valuesperlabel_2",
    "quantile_1s_10queries_40valuesperlabel_2",
]

FONTSIZE = 24


def print_benefits_summary(
    experiments,
    data_scales,
    latency_benefits,
    cost_benefits,
    use_query_cost_sum,
    use_query_cost_95,
):
    """Print summary of the benefits data."""
    if use_query_cost_sum:
        cost_label = "Query Cost Sum Benefit (ratio)"
        cost_json_key = "query_cost_sum_benefit_ratio"
    elif use_query_cost_95:
        cost_label = "Query Cost P95 Benefit (ratio)"
        cost_json_key = "query_cost_p95_benefit_ratio"
    else:
        cost_label = "Total CPU P95 Benefit (ratio)"
        cost_json_key = "total_cpu_p95_benefit_ratio"

    print("\nBenefits Summary (Prometheus / SketchDB):")
    print("=" * 110)
    print(
        f"{'Experiment':<50} {'Data Scale':<20} {'Latency Benefit':<20} {cost_label:<20}"
    )
    print("-" * 110)

    for exp, scale, lat_benefit, cost_benefit in zip(
        experiments, data_scales, latency_benefits, cost_benefits
    ):
        scale_str = f"{scale:.2e}" if scale is not None else "N/A"
        lat_str = f"{lat_benefit:.2f}x" if lat_benefit is not None else "N/A"
        cost_str = f"{cost_benefit:.2f}x" if cost_benefit is not None else "N/A"
        print(f"{exp:<50} {scale_str:<20} {lat_str:<20} {cost_str:<20}")

    print("=" * 110)

    # Print json-like structure also
    print("\nJSON-like Data Structure:")
    data_list = []
    for exp, scale, lat_benefit, cost_benefit in zip(
        experiments, data_scales, latency_benefits, cost_benefits
    ):
        data_list.append(
            {
                "experiment": exp,
                "data_scale_metrics_per_sec": scale,
                "latency_benefit_ratio": lat_benefit,
                cost_json_key: cost_benefit,
            }
        )

    print(json.dumps(data_list, indent=4))


def plot_scale_vs_benefits(
    experiments,
    data_scales,
    latency_benefits,
    cost_benefits,
    use_query_cost_sum,
    use_query_cost_95,
    save_file=None,
    show=False,
):
    """
    Plot data scale vs benefits (prometheus/sketchdb ratios).

    Args:
        experiments: List of experiment names
        data_scales: List of data scale values (metrics/sec)
        latency_benefits: List of latency benefit ratios (prometheus/sketchdb)
        cost_benefits: List of cost benefit ratios (prometheus/sketchdb)
        save_file: Filename to save the plot (if None, doesn't save)
        show: Whether to display the plot
        use_query_cost_sum: Whether cost values represent query cost sum
        use_query_cost_95: Whether cost values represent query cost p95

    Returns:
        matplotlib figure object
    """
    # Filter out None values and sort by data scale
    valid_data = [
        (s, l, c, e)
        for s, l, c, e in zip(data_scales, latency_benefits, cost_benefits, experiments)
        if s is not None and l is not None and c is not None
    ]

    if not valid_data:
        print("Error: No valid data points to plot")
        return None

    valid_data.sort(key=lambda x: x[0])  # Sort by data scale
    data_scales_sorted, latency_benefits_sorted, cost_benefits_sorted, _ = zip(
        *valid_data
    )

    # Convert to numpy arrays
    data_scales_arr = np.array(data_scales_sorted)
    latency_benefits_arr = np.array(latency_benefits_sorted)
    cost_benefits_arr = np.array(cost_benefits_sorted)

    # Create the plot with two y-axes
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Determine cost label based on type
    if use_query_cost_sum:
        cost_ylabel = "Query CPU Sum Benefit (ratio)"
        cost_legend = "Query CPU Sum Benefit"
    elif use_query_cost_95:
        cost_ylabel = "Query CPU P95 Benefit (ratio)"
        cost_legend = "Query CPU P95 Benefit"
    else:
        cost_ylabel = "Total CPU P95 Benefit (ratio)"
        cost_legend = "Total CPU P95 Benefit"

    # Plot cost benefit on left y-axis
    color_cost = "#1f77b4"
    # ax1.set_xlabel("Data Scale (metrics/sec)", fontsize=FONTSIZE, fontweight="bold")
    ax1.set_xlabel("Data Cardinality", fontsize=FONTSIZE, fontweight="bold")
    ax1.set_ylabel(cost_ylabel, fontsize=FONTSIZE, fontweight="bold", color=color_cost)
    line1 = ax1.plot(
        data_scales_arr,
        cost_benefits_arr,
        "o-",
        color=color_cost,
        linewidth=2,
        markersize=8,
        label=cost_legend,
    )
    ax1.tick_params(axis="y", labelcolor=color_cost, labelsize=FONTSIZE)
    ax1.tick_params(axis="x", labelsize=FONTSIZE)
    ax1.set_xscale("log")
    ax1.grid(True, alpha=0.3, which="both")

    # Create second y-axis for latency benefit
    ax2 = ax1.twinx()
    color_latency = "#ff7f0e"
    ax2.set_ylabel(
        "Latency Benefit (ratio)",
        fontsize=FONTSIZE,
        fontweight="bold",
        color=color_latency,
    )
    line2 = ax2.plot(
        data_scales_arr,
        latency_benefits_arr,
        "s-",
        color=color_latency,
        linewidth=2,
        markersize=8,
        label="Latency Benefit",
    )
    ax2.tick_params(axis="y", labelcolor=color_latency, labelsize=FONTSIZE)

    # Add title
    plt.title(
        "TurboProm's Benefits vs Data Cardinality",
        fontsize=FONTSIZE + 2,
        fontweight="bold",
        pad=20,
    )

    # Add vertical dotted lines and annotations for each data point
    # Calculate the middle position for annotations (in data coordinates)
    y1_min, y1_max = ax1.get_ylim()
    annotation_y = y1_min + (y1_max - y1_min) * 0.5  # Center vertically

    for i, x in enumerate(data_scales_arr):
        # Format the data scale nicely
        if x < 1000:
            scale_label = f"{int(x)}"
        elif x < 1000000:
            scale_label = f"{int(x/1000)}K"
        else:
            scale_label = f"{x/1000000:.1f}M"

        # Draw vertical dotted line
        ax1.axvline(x=x, color="gray", linestyle=":", alpha=0.5, linewidth=1.5)

        # Add annotation at the center of the plot
        ax1.text(
            x,
            annotation_y,
            scale_label,
            ha="center",
            va="center",
            fontsize=FONTSIZE - 4,
            bbox=dict(
                boxstyle="round,pad=0.4", facecolor="white", edgecolor="gray", alpha=0.8
            ),
        )

    # Add legend
    lines = line1 + line2
    labels = [line.get_label() for line in lines]
    ax1.legend(lines, labels, loc="upper left", fontsize=FONTSIZE)

    # Adjust layout
    fig.tight_layout()

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
        description="Plot data scale vs benefits (prometheus/sketchdb ratios)",
        epilog="""
Examples:
  # Print benefits summary only
  python3 plot_scale_vs_benefits.py --print

  # Plot and save to file
  python3 plot_scale_vs_benefits.py --plot --save scale_benefits.png

  # Plot and show interactively
  python3 plot_scale_vs_benefits.py --plot --show

  # Both print and plot
  python3 plot_scale_vs_benefits.py --print --plot --save output.png --show

  # Use query cost sum instead of p95 cost
  python3 plot_scale_vs_benefits.py --print --use-query-cost-sum
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--print", action="store_true", help="Print benefits summary")
    parser.add_argument("--plot", action="store_true", help="Generate plot")
    parser.add_argument(
        "--save",
        type=str,
        metavar="FILENAME",
        help="Save plot to file (provide filename)",
    )
    parser.add_argument("--show", action="store_true", help="Display plot")
    parser.add_argument(
        "--use-query-cost-sum",
        action="store_true",
        help="Use query CPU sum instead of total CPU p95",
    )
    parser.add_argument(
        "--use-query-cost-95",
        action="store_true",
        help="Use query CPU p95 instead of total CPU p95",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.plot and not (args.save or args.show):
        parser.error("--plot requires either --save or --show (or both)")

    if not args.print and not args.plot:
        parser.error("At least one of --print or --plot must be specified")

    # Collect data for all experiments
    print(f"Processing {len(EXPERIMENT_NAMES)} experiments...")

    data_scales = []
    latency_benefits = []
    cost_benefits = []

    for exp_name in EXPERIMENT_NAMES:
        print(f"\nProcessing: {exp_name}")

        # Calculate data scale
        scale = calculate_data_scale(exp_name)
        data_scales.append(scale)
        if scale is not None:
            print(f"  Data scale: {scale:.2e} metrics/sec")

        # Get latency for both prometheus and sketchdb
        latency_prometheus = get_latency_p95(exp_name, "baseline")
        latency_sketchdb = get_latency_p95(exp_name, "sketchdb")

        if latency_prometheus is not None and latency_sketchdb is not None:
            latency_benefit = latency_prometheus / latency_sketchdb
            latency_benefits.append(latency_benefit)
            print(
                f"  Latency benefit: {latency_benefit:.2f}x (prometheus: {latency_prometheus:.4f}s, sketchdb: {latency_sketchdb:.4f}s)"
            )
        else:
            latency_benefits.append(None)
            print(
                f"  Latency benefit: N/A (prometheus: {latency_prometheus}, sketchdb: {latency_sketchdb})"
            )

        # Get cost for both prometheus and sketchdb
        if args.use_query_cost_sum:
            cost_prometheus = get_query_cost_sum(exp_name, "baseline")
            cost_sketchdb = get_query_cost_sum(exp_name, "sketchdb")
            cost_type = "Query cost sum"
        elif args.use_query_cost_95:
            cost_prometheus = get_query_cost_95(exp_name, "baseline")
            cost_sketchdb = get_query_cost_95(exp_name, "sketchdb")
            cost_type = "Query cost p95"
        else:
            cost_prometheus = get_cost_p95(exp_name, "baseline")
            cost_sketchdb = get_cost_p95(exp_name, "sketchdb")
            cost_type = "Total CPU p95"

        if cost_prometheus is not None and cost_sketchdb is not None:
            cost_benefit = cost_prometheus / cost_sketchdb
            cost_benefits.append(cost_benefit)
            print(
                f"  {cost_type} benefit: {cost_benefit:.2f}x (prometheus: {cost_prometheus:.2f}%, sketchdb: {cost_sketchdb:.2f}%)"
            )
        else:
            cost_benefits.append(None)
            print(
                f"  {cost_type} benefit: N/A (prometheus: {cost_prometheus}, sketchdb: {cost_sketchdb})"
            )

    # Print summary if requested
    if args.print:
        print_benefits_summary(
            EXPERIMENT_NAMES,
            data_scales,
            latency_benefits,
            cost_benefits,
            use_query_cost_sum=args.use_query_cost_sum,
            use_query_cost_95=args.use_query_cost_95,
        )

    # Generate plot if requested
    if args.plot:
        plot_scale_vs_benefits(
            experiments=EXPERIMENT_NAMES,
            data_scales=data_scales,
            latency_benefits=latency_benefits,
            cost_benefits=cost_benefits,
            save_file=args.save,
            show=args.show,
            use_query_cost_sum=args.use_query_cost_sum,
            use_query_cost_95=args.use_query_cost_95,
        )

    return 0


if __name__ == "__main__":
    exit(main())

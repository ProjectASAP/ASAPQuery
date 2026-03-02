import os
import sys
import json
import argparse
import humanize
import numpy as np
import matplotlib.pyplot as plt
from typing import List
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import constants  # noqa: E402

RESOURCES = ["cpu_percent", "memory_info"]
PROMETHEUS_PROCESS_KEYWORD = "prometheus.yml"

relevant_stats = {
    "sum": lambda x: sum(x),
    "max": lambda x: max(x),
    "median": lambda x: np.median(x),
    "p95": lambda x: np.percentile(x, 95),
    "p99": lambda x: np.percentile(x, 99),
}


def pretty_print(key, value):
    if "memory" in key:
        print(key, humanize.naturalsize(value))
    else:
        print(key, round(value, 2))


def calculate_query_cpu(monitor_info, experiment_mode):
    """
    Calculate Query CPU timeseries for the given experiment mode.

    Args:
        monitor_info: Dictionary with PIDs as keys and monitoring data as values
        experiment_mode: Name of the current experiment mode

    Returns:
        List of Query CPU values (timeseries)
    """
    pids = [pid for pid in monitor_info.keys() if pid != "all"]

    if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
        # Sum CPU across all PIDs except those with keyword="prometheus"
        query_cpu = [0 for _ in range(len(monitor_info[pids[0]]["cpu_percent"]))]

        for pid in pids:
            keyword = monitor_info[pid]["keyword"]
            if keyword != PROMETHEUS_PROCESS_KEYWORD:
                for i in range(len(monitor_info[pid]["cpu_percent"])):
                    query_cpu[i] += monitor_info[pid]["cpu_percent"][i]

        return query_cpu

    elif experiment_mode == constants.BASELINE_EXPERIMENT_NAME:
        # Find prometheus PID(s) and get their CPU timeseries
        prometheus_cpu = None
        for pid in pids:
            if monitor_info[pid]["keyword"] == PROMETHEUS_PROCESS_KEYWORD:
                prometheus_cpu = monitor_info[pid]["cpu_percent"][:]
                break

        if prometheus_cpu is None:
            raise ValueError(f"No prometheus PID found in mode {experiment_mode}")

        # Calculate 5th percentile (ingestion cost)
        prometheus_ingestion_cost = np.percentile(prometheus_cpu, 5)

        # Subtract ingestion cost from each time point
        query_cpu = [cpu - prometheus_ingestion_cost for cpu in prometheus_cpu]

        return query_cpu

    else:
        raise AssertionError(
            f"Query CPU calculation not supported for mode: {experiment_mode}"
        )


def plot_resource_usage(monitor_info, experiment_mode, args):
    """
    Plot raw resource usage data for each resource type.

    Args:
        monitor_info: Dictionary with PIDs as keys and monitoring data as values
        experiment_mode: Name of the current experiment mode
        args: Command-line arguments
    """
    # Set global font size to 24
    plt.rcParams.update({"font.size": 22})

    for resource in RESOURCES:
        plt.figure(figsize=(20, 8))

        # Plot data for each PID/keyword
        for pid, data in monitor_info.items():
            keyword = data["keyword"]
            y_values = data[resource]
            x_values = list(range(len(y_values)))

            plt.plot(x_values, y_values, label=f"{keyword} (PID: {pid})")

        # Add labels and title
        resource_label = (
            "Memory Usage (bytes)" if resource == "memory_info" else "CPU Usage (%)"
        )
        plt.ylabel(resource_label)
        plt.xlabel("Time (samples)")
        plt.title(f"{experiment_mode}: {resource} Raw Data")
        plt.legend()

        # Save or show based on args
        if args.save:
            # Make plot fullscreen before saving
            mng = plt.get_current_fig_manager()
            try:
                mng.full_screen_toggle()  # For Qt backend
            except AttributeError:
                try:
                    mng.window.showMaximized()  # For TkAgg backend
                except Exception:
                    try:
                        mng.frame.Maximize(True)  # For WX backend
                    except Exception:
                        try:
                            mng.resize(*mng.window.maxsize())  # For other backends
                        except Exception:
                            print("Warning: Could not maximize figure window")

            output_filename = f"mode_{experiment_mode}_{resource}.png"
            output_path = os.path.join(args.output_dir, output_filename)
            os.makedirs(args.output_dir, exist_ok=True)
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            print(f"Saved plot to {output_path}")

        if args.show:
            plt.show()
        else:
            plt.close()


def plot_query_cpu(query_cpu, experiment_mode, args):
    """
    Plot Query CPU timeseries.

    Args:
        query_cpu: List of Query CPU values (timeseries)
        experiment_mode: Name of the current experiment mode
        args: Command-line arguments
    """
    # Set global font size to 24
    plt.rcParams.update({"font.size": 22})

    plt.figure(figsize=(20, 8))

    # Plot Query CPU timeseries
    x_values = list(range(len(query_cpu)))
    plt.plot(x_values, query_cpu, label="Query CPU", linewidth=2)

    # Add labels and title
    plt.ylabel("Query CPU Usage (%)")
    plt.xlabel("Time (samples)")
    plt.title(f"{experiment_mode}: Query CPU")
    plt.legend()
    plt.grid(True, alpha=0.3)

    # Save or show based on args
    if args.save:
        # Make plot fullscreen before saving
        mng = plt.get_current_fig_manager()
        try:
            mng.full_screen_toggle()  # For Qt backend
        except AttributeError:
            try:
                mng.window.showMaximized()  # For TkAgg backend
            except Exception:
                try:
                    mng.frame.Maximize(True)  # For WX backend
                except Exception:
                    try:
                        mng.resize(*mng.window.maxsize())  # For other backends
                    except Exception:
                        print("Warning: Could not maximize figure window")

        output_filename = f"mode_{experiment_mode}_query_cpu.png"
        output_path = os.path.join(args.output_dir, output_filename)
        os.makedirs(args.output_dir, exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Saved Query CPU plot to {output_path}")

    if args.show:
        plt.show()
    else:
        plt.close()


def main(args):
    if not args.print and not args.plot:
        raise ValueError("Must specify either --print or --plot")
    if args.all_experiment_modes and args.experiment_mode:
        raise ValueError(
            "Cannot specify both --all_experiment_modes and --experiment_mode"
        )
    elif not args.all_experiment_modes and not args.experiment_mode:
        raise ValueError(
            "Must specify either --all_experiment_modes or --experiment_mode"
        )

    if args.plot:
        if not args.show and not args.save:
            raise ValueError("Must specify either --show or --save when using --plot")
        if args.save:
            if not args.save_to_experiment_dir and not args.output_dir:
                raise ValueError(
                    "Must specify --save_to_experiment_dir or --output_dir when using --save"
                )

    experiment_dir = os.path.join(constants.LOCAL_EXPERIMENT_DIR, args.experiment_name)

    if not os.path.exists(experiment_dir):
        raise ValueError(f"Experiment directory {experiment_dir} does not exist")

    if args.plot and args.save:
        if args.save_to_experiment_dir:
            args.output_dir = experiment_dir

    # Initialize machine-readable output structure
    machine_readable_output = {
        "experiment_name": args.experiment_name,
        "experiment_modes": {},
    }

    experiment_modes: List[str] = []
    if args.all_experiment_modes:
        # experiment_modes = os.listdir(experiment_dir)
        # experiment_modes = [
        #     mode
        #     for mode in experiment_modes
        #     if os.path.exists(os.path.join(experiment_dir, mode, "monitor_output.json"))
        # ]

        # find all directories in experiment_dir recursively that contain monitor_output.json
        experiment_modes = [
            d
            for d, _, files in os.walk(experiment_dir)
            if "monitor_output.json" in files and d.endswith("remote_monitor_output")
        ]
        # for each directory, experiment mode is the parent directory relative to experiment_dir
        experiment_modes = [
            os.path.dirname(os.path.relpath(d, experiment_dir))
            for d in experiment_modes
        ]
        # remove the experiment_dir prefix
        experiment_modes = [
            mode for mode in experiment_modes if mode != "." and mode != ""
        ]
    else:
        experiment_modes = [args.experiment_mode]

    if not args.machine_readable:
        print(f"Experiment modes to analyze: {experiment_modes}")

    experiment_mode_to_overall_resource_usage = {}
    experiment_mode_to_query_cpu = {}

    for experiment_mode in experiment_modes:
        if not args.machine_readable:
            print("-" * 20 + f" Mode: {experiment_mode} " + "-" * 20)

        monitor_info_file = os.path.join(
            experiment_dir,
            experiment_mode,
            "remote_monitor_output",
            "monitor_output.json",
        )

        monitor_info = None
        with open(monitor_info_file, "r") as f:
            monitor_info = json.load(f)

        resources_across_pids = defaultdict(list)

        pids = list(monitor_info.keys())
        # verify that all pids have the same length
        assert all(
            len(monitor_info[pid]["cpu_percent"])
            == len(monitor_info[pids[0]]["cpu_percent"])
            for pid in pids
        ), "All PIDs must have the same length of CPU percent data"
        assert all(
            len(monitor_info[pid]["memory_info"])
            == len(monitor_info[pids[0]]["memory_info"])
            for pid in pids
        ), "All PIDs must have the same length of memory info data"

        # for pid in pids:
        #     for resource in RESOURCES:
        #         monitor_info[pid][resource] = monitor_info[pid][resource][610:]

        monitor_info["all"] = {
            "keyword": "all",
            "cpu_percent": [
                0 for _ in range(len(monitor_info[pids[0]]["cpu_percent"]))
            ],
            "memory_info": [
                0 for _ in range(len(monitor_info[pids[0]]["memory_info"]))
            ],
        }

        # Add the CPU and memory data to the "all" entry
        for pid in pids:
            for i in range(len(monitor_info[pid]["cpu_percent"])):
                monitor_info["all"]["cpu_percent"][i] += monitor_info[pid][
                    "cpu_percent"
                ][i]
                monitor_info["all"]["memory_info"][i] += monitor_info[pid][
                    "memory_info"
                ][i]

        experiment_mode_to_overall_resource_usage[experiment_mode] = monitor_info["all"]

        # Calculate Query CPU for this experiment mode
        try:
            query_cpu = calculate_query_cpu(monitor_info, experiment_mode)
            experiment_mode_to_query_cpu[experiment_mode] = query_cpu
        except (AssertionError, ValueError) as e:
            if not args.machine_readable:
                print(f"Skipping Query CPU calculation for {experiment_mode}: {e}")

        # Initialize mode data for machine-readable output
        if args.machine_readable:
            machine_readable_output["experiment_modes"][experiment_mode] = {
                "processes": {},
                "overall": {},
            }

        for pid in monitor_info.keys():
            keyword = monitor_info[pid]["keyword"]
            if not args.machine_readable:
                print(pid, keyword)

            # Collect statistics for machine-readable output
            if args.machine_readable:
                process_stats = {}
                for resource in RESOURCES:
                    process_stats[resource] = {}
                    for stat, agg_func in relevant_stats.items():
                        value = agg_func(monitor_info[pid][resource])
                        process_stats[resource][stat] = value

                machine_readable_output["experiment_modes"][experiment_mode][
                    "processes"
                ][f"{pid}_{keyword}"] = process_stats

            for resource in RESOURCES:
                resources_across_pids[resource].extend(monitor_info[pid][resource])

                if args.print and not args.machine_readable:
                    for stat, agg_func in relevant_stats.items():
                        pretty_print(
                            f"{experiment_mode} {keyword} {resource} {stat}",
                            agg_func(monitor_info[pid][resource]),
                        )

        # if args.print:
        #     for resource, values in resources_across_pids.items():
        #         for stat, agg_func in relevant_stats.items():
        #             pretty_print(f"Overall {resource} {stat}", agg_func(values))

        # Generate plots if requested
        if args.plot:
            plot_resource_usage(monitor_info, experiment_mode, args)

            # Plot Query CPU if available
            if experiment_mode in experiment_mode_to_query_cpu:
                plot_query_cpu(
                    experiment_mode_to_query_cpu[experiment_mode], experiment_mode, args
                )

    # Calculate and output benefit statistics
    if (
        constants.BASELINE_EXPERIMENT_NAME in experiment_mode_to_overall_resource_usage
        and constants.SKETCHDB_EXPERIMENT_NAME
        in experiment_mode_to_overall_resource_usage
    ):
        benefit_stats = {}
        for resource in RESOURCES:
            benefit_stats[resource] = {}
            for stat, agg_func in relevant_stats.items():
                # divide prometheus agg_func(resource) by sketchdb agg_func(resource)
                prometheus_value = agg_func(
                    experiment_mode_to_overall_resource_usage[
                        constants.BASELINE_EXPERIMENT_NAME
                    ][resource]
                )
                sketchdb_value = agg_func(
                    experiment_mode_to_overall_resource_usage[
                        constants.SKETCHDB_EXPERIMENT_NAME
                    ][resource]
                )
                benefit = prometheus_value / sketchdb_value
                benefit_stats[resource][stat] = benefit

                if args.print and not args.machine_readable:
                    print(f"Benefit for {stat}({resource}): {benefit}")

        if args.machine_readable:
            machine_readable_output["benefit"] = benefit_stats

    # Handle Query CPU statistics
    if experiment_mode_to_query_cpu:
        if not args.machine_readable and args.print:
            print("\n" + "=" * 60)
            print("Query CPU Statistics")
            print("=" * 60)

        query_cpu_stats = {}
        for experiment_mode, query_cpu in experiment_mode_to_query_cpu.items():
            query_cpu_stats[experiment_mode] = {}
            if not args.machine_readable and args.print:
                print(f"\n{experiment_mode}:")

            for stat, agg_func in relevant_stats.items():
                value = agg_func(query_cpu)
                query_cpu_stats[experiment_mode][stat] = value
                if not args.machine_readable and args.print:
                    print(f"  {stat}: {round(value, 2)}%")

        if args.machine_readable:
            machine_readable_output["query_cpu"] = query_cpu_stats

        # Calculate benefit if both prometheus and sketchdb are present
        if (
            constants.BASELINE_EXPERIMENT_NAME in experiment_mode_to_query_cpu
            and constants.SKETCHDB_EXPERIMENT_NAME in experiment_mode_to_query_cpu
        ):
            query_cpu_benefit = {}
            if not args.machine_readable and args.print:
                print("\nQuery CPU Benefit (prometheus / sketchdb):")

            for stat, agg_func in relevant_stats.items():
                prometheus_value = agg_func(
                    experiment_mode_to_query_cpu[constants.BASELINE_EXPERIMENT_NAME]
                )
                sketchdb_value = agg_func(
                    experiment_mode_to_query_cpu[constants.SKETCHDB_EXPERIMENT_NAME]
                )
                benefit = prometheus_value / sketchdb_value
                query_cpu_benefit[stat] = benefit
                if not args.machine_readable and args.print:
                    print(f"  {stat}: {round(benefit, 2)}x")

            if args.machine_readable:
                machine_readable_output["query_cpu_benefit"] = query_cpu_benefit

    # Output machine-readable results
    if args.machine_readable:
        print(json.dumps(machine_readable_output, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--all_experiment_modes", action="store_true")
    parser.add_argument("--experiment_mode", type=str, required=False)
    parser.add_argument("--print", action="store_true", help="Print the results")
    parser.add_argument("--plot", action="store_true", help="Plot the results")
    parser.add_argument(
        "--save", action="store_true", help="Save the results to a file"
    )
    parser.add_argument(
        "--show", action="store_true", help="Show the results on the screen"
    )
    parser.add_argument(
        "--output_dir", type=str, required=False, help="Directory to save the output"
    )
    parser.add_argument(
        "--save_to_experiment_dir",
        action="store_true",
        help="Save to experiment directory",
    )
    parser.add_argument(
        "--machine-readable",
        action="store_true",
        default=False,
        help="Output results in machine-readable JSON format",
    )
    args = parser.parse_args()
    main(args)

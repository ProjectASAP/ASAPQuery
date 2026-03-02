#!/usr/bin/env python3
"""
Vertical Scalability Grid Search for Prometheus and VictoriaMetrics

This script runs a comprehensive grid search over different CPU and memory
configurations to test the vertical scalability of Prometheus and VictoriaMetrics.
It uses Docker resource constraints and the existing experiment infrastructure.

Usage:
    python vertical_scalability_grid_search.py \\
        --base-config vertical_scalability_test \\
        --output-dir /path/to/results \\
        --tools prometheus,victoriametrics \\
        --cpu-configs 1,2,4,8 \\
        --memory-configs 1g,2g,4g,8g
"""

import argparse
import os
import subprocess
import time
from typing import List, Dict, Any
import itertools
import json
from datetime import datetime

# Default configurations
DEFAULT_CPU_CONFIGS = [1.0, 2.0, 4.0, 8.0]
DEFAULT_MEMORY_CONFIGS = ["1g", "2g", "4g", "8g"]
DEFAULT_TOOLS = ["prometheus", "victoriametrics"]


def run_single_experiment(
    base_config: str,
    tool: str,
    cpu_limit: float,
    memory_limit: str,
    output_dir: str,
    baseline: bool = False,
) -> Dict[str, Any]:
    """
    Run a single vertical scalability experiment.

    Args:
        base_config: Base configuration name (e.g., "vertical_scalability_test")
        tool: Tool to test ("prometheus" or "victoriametrics")
        cpu_limit: CPU limit (e.g., 4.0)
        memory_limit: Memory limit (e.g., "8g")
        output_dir: Output directory for results
        baseline: Whether this is a baseline test (no resource limits)

    Returns:
        Dictionary with experiment metadata and results path
    """
    # Generate experiment name
    if baseline:
        experiment_name = f"baseline_{tool}"
    else:
        experiment_name = f"scalability_{tool}_{cpu_limit}cpu_{memory_limit}"

    print(f"Starting experiment: {experiment_name}")

    # Prepare command arguments
    cmd = [
        "python",
        "experiment_run_e2e.py",
        f"experiment_name={experiment_name}",
        f"experiment_params={base_config}",
    ]

    # Add Docker resource configuration (only if not baseline)
    if not baseline:
        cmd.extend(
            [
                f"experiment_params.docker_resources.tool={tool}",
                f"experiment_params.docker_resources.cpu_limit={cpu_limit}",
                f"experiment_params.docker_resources.memory_limit={memory_limit}",
            ]
        )

    # Override tool in experiment mode if needed
    cmd.append(f"experiment_params.docker_resources.tool={tool}")

    # Record start time
    start_time = datetime.now()

    try:
        # Run experiment
        print(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Prepare experiment metadata
        experiment_metadata = {
            "experiment_name": experiment_name,
            "tool": tool,
            "cpu_limit": cpu_limit if not baseline else None,
            "memory_limit": memory_limit if not baseline else None,
            "baseline": baseline,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "return_code": result.returncode,
            "success": result.returncode == 0,
            "command": " ".join(cmd),
        }

        # Log stdout and stderr
        if result.stdout:
            experiment_metadata["stdout"] = result.stdout
        if result.stderr:
            experiment_metadata["stderr"] = result.stderr

        if result.returncode == 0:
            print(
                f"✅ Experiment {experiment_name} completed successfully in {duration:.1f}s"
            )
        else:
            print(
                f"❌ Experiment {experiment_name} failed with return code {result.returncode}"
            )
            if result.stderr:
                print(f"Error output: {result.stderr[:500]}")

        return experiment_metadata

    except subprocess.TimeoutExpired:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"⏰ Experiment {experiment_name} timed out after {duration:.1f}s")

        return {
            "experiment_name": experiment_name,
            "tool": tool,
            "cpu_limit": cpu_limit if not baseline else None,
            "memory_limit": memory_limit if not baseline else None,
            "baseline": baseline,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "return_code": -1,
            "success": False,
            "error": "timeout",
            "command": " ".join(cmd),
        }

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"💥 Experiment {experiment_name} failed with exception: {e}")

        return {
            "experiment_name": experiment_name,
            "tool": tool,
            "cpu_limit": cpu_limit if not baseline else None,
            "memory_limit": memory_limit if not baseline else None,
            "baseline": baseline,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "return_code": -2,
            "success": False,
            "error": str(e),
            "command": " ".join(cmd),
        }


def generate_experiment_plan(
    tools: List[str],
    cpu_configs: List[float],
    memory_configs: List[str],
    include_baseline: bool = True,
) -> List[Dict[str, Any]]:
    """
    Generate a list of experiments to run.

    Args:
        tools: List of tools to test
        cpu_configs: List of CPU configurations
        memory_configs: List of memory configurations
        include_baseline: Whether to include baseline tests

    Returns:
        List of experiment configurations
    """
    experiments = []

    # Add baseline experiments (no resource limits)
    if include_baseline:
        for tool in tools:
            experiments.append(
                {
                    "tool": tool,
                    "cpu_limit": None,
                    "memory_limit": None,
                    "baseline": True,
                }
            )

    # Add resource-constrained experiments
    for tool, cpu_limit, memory_limit in itertools.product(
        tools, cpu_configs, memory_configs
    ):
        experiments.append(
            {
                "tool": tool,
                "cpu_limit": cpu_limit,
                "memory_limit": memory_limit,
                "baseline": False,
            }
        )

    return experiments


def save_results_summary(
    experiments_results: List[Dict[str, Any]], output_dir: str
) -> None:
    """
    Save a summary of all experiment results.

    Args:
        experiments_results: List of experiment result dictionaries
        output_dir: Output directory for summary
    """
    summary_file = os.path.join(output_dir, "grid_search_summary.json")

    # Overall summary statistics
    total_experiments = len(experiments_results)
    successful_experiments = sum(1 for exp in experiments_results if exp["success"])
    failed_experiments = total_experiments - successful_experiments

    total_duration = sum(exp.get("duration_seconds", 0) for exp in experiments_results)

    summary = {
        "grid_search_metadata": {
            "total_experiments": total_experiments,
            "successful_experiments": successful_experiments,
            "failed_experiments": failed_experiments,
            "success_rate": (
                successful_experiments / total_experiments
                if total_experiments > 0
                else 0
            ),
            "total_duration_seconds": total_duration,
            "total_duration_hours": total_duration / 3600,
            "generated_at": datetime.now().isoformat(),
        },
        "experiments": experiments_results,
    }

    # Save to file
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)

    print("\n📊 Grid Search Summary:")
    print(f"   Total experiments: {total_experiments}")
    print(f"   Successful: {successful_experiments}")
    print(f"   Failed: {failed_experiments}")
    print(f"   Success rate: {successful_experiments/total_experiments*100:.1f}%")
    print(f"   Total duration: {total_duration/3600:.1f} hours")
    print(f"   Results saved to: {summary_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Run vertical scalability grid search for Prometheus and VictoriaMetrics"
    )
    parser.add_argument(
        "--base-config",
        default="vertical_scalability_test",
        help="Base configuration template name",
    )
    parser.add_argument(
        "--output-dir", required=True, help="Output directory for grid search results"
    )
    parser.add_argument(
        "--tools",
        default="prometheus,victoriametrics",
        help="Comma-separated list of tools to test",
    )
    parser.add_argument(
        "--cpu-configs",
        default="1,2,4,8",
        help="Comma-separated list of CPU configurations (vCPUs)",
    )
    parser.add_argument(
        "--memory-configs",
        default="1g,2g,4g,8g",
        help="Comma-separated list of memory configurations",
    )
    parser.add_argument(
        "--no-baseline",
        action="store_true",
        help="Skip baseline tests (no resource limits)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print experiment plan without running experiments",
    )

    args = parser.parse_args()

    # Parse configurations
    tools = [tool.strip() for tool in args.tools.split(",")]
    cpu_configs = [float(cpu.strip()) for cpu in args.cpu_configs.split(",")]
    memory_configs = [mem.strip() for mem in args.memory_configs.split(",")]

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Generate experiment plan
    experiments = generate_experiment_plan(
        tools=tools,
        cpu_configs=cpu_configs,
        memory_configs=memory_configs,
        include_baseline=not args.no_baseline,
    )

    print("🧪 Vertical Scalability Grid Search")
    print(f"   Tools: {tools}")
    print(f"   CPU configs: {cpu_configs}")
    print(f"   Memory configs: {memory_configs}")
    print(f"   Total experiments: {len(experiments)}")
    print(f"   Output directory: {args.output_dir}")

    if args.dry_run:
        print("\n📋 Experiment Plan (Dry Run):")
        for i, exp in enumerate(experiments, 1):
            if exp["baseline"]:
                print(f"   {i:2d}. Baseline {exp['tool']}")
            else:
                print(
                    f"   {i:2d}. {exp['tool']} - {exp['cpu_limit']} CPU, {exp['memory_limit']} memory"
                )
        return

    # Run experiments
    print("\n🚀 Starting grid search...")
    start_time = time.time()

    results = []
    for i, exp in enumerate(experiments, 1):
        print(f"\n--- Experiment {i}/{len(experiments)} ---")

        result = run_single_experiment(
            base_config=args.base_config,
            tool=exp["tool"],
            cpu_limit=exp.get("cpu_limit"),
            memory_limit=exp.get("memory_limit"),
            output_dir=args.output_dir,
            baseline=exp["baseline"],
        )

        results.append(result)

        # Add a small delay between experiments
        time.sleep(5)

    end_time = time.time()
    total_duration = end_time - start_time

    print(f"\n🏁 Grid search completed in {total_duration/3600:.1f} hours")

    # Save results summary
    save_results_summary(results, args.output_dir)


if __name__ == "__main__":
    main()

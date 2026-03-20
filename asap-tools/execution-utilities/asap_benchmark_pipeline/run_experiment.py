#!/usr/bin/env python3
"""
Run baseline and ASAP experiments, then generate comparison graphs.

Usage:
  # Full run from scratch (starts all infrastructure):
  python3 run_experiment.py --runs 3 --load-data

  # Infra already running, data already loaded:
  python3 run_experiment.py --runs 3 --skip-infra

  # Skip baseline (use existing baseline_results.csv):
  python3 run_experiment.py --runs 3 --skip-infra --skip-baseline

  # Quick single-run comparison:
  python3 run_experiment.py --runs 1 --skip-infra
"""

import argparse
import subprocess
import sys
import csv
from pathlib import Path
from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np


def run_pipeline(mode, output_file, load_data=False, skip_infra=False):
    """Invoke run_pipeline.sh and return True on success."""
    script_dir = Path(__file__).parent
    cmd = [
        "bash", str(script_dir / "run_pipeline.sh"),
        "--mode", mode,
        "--output", str(output_file),
    ]
    if load_data:
        cmd.append("--load-data")
    if skip_infra:
        cmd.append("--skip-infra")

    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'='*60}\n")

    result = subprocess.run(cmd, check=False)
    return result.returncode == 0


def load_csv(csv_file):
    """Return (query_ids, latencies_ms, errors) from a benchmark CSV."""
    query_ids, latencies, errors = [], [], []
    try:
        with open(csv_file, newline="") as f:
            for row in csv.DictReader(f):
                query_ids.append(row["query_id"])
                try:
                    latencies.append(float(row["latency_ms"]))
                except (ValueError, KeyError):
                    latencies.append(0.0)
                errors.append(bool(row.get("error", "").strip()))
    except FileNotFoundError:
        print(f"  Warning: {csv_file} not found")
    return query_ids, latencies, errors


def generate_comparison_graphs(baseline_file, asap_files, output_dir, timestamp):
    """Generate side-by-side latency comparison and per-run overlay graphs."""
    b_ids, b_lat, b_err = load_csv(baseline_file)
    if not b_lat:
        print("No baseline data; skipping graphs")
        return

    asap_runs = []
    for f in asap_files:
        _, lats, _ = load_csv(f)
        if lats:
            asap_runs.append(lats)

    if not asap_runs:
        print("No ASAP run data; skipping graphs")
        return

    n = len(b_lat)
    x = np.arange(1, n + 1)
    asap_matrix = np.array([r[:n] for r in asap_runs])  # shape (runs, queries)
    asap_mean = asap_matrix.mean(axis=0)
    asap_std = asap_matrix.std(axis=0) if len(asap_runs) > 1 else np.zeros(n)

    # --- Figure 1: Baseline vs ASAP mean latency (grouped bars) ---
    fig, axes = plt.subplots(2, 1, figsize=(14, 12))
    fig.suptitle(
        f"ASAP vs Baseline — {len(asap_runs)} ASAP run(s) — {timestamp}",
        fontsize=14, fontweight="bold"
    )

    ax = axes[0]
    bar_w = 0.38
    ax.bar(x - bar_w / 2, b_lat, bar_w, label="Baseline (exact)", color="#1f77b4", alpha=0.85, edgecolor="black")
    ax.bar(x + bar_w / 2, asap_mean, bar_w, label=f"ASAP avg (n={len(asap_runs)})", color="#ff7f0e", alpha=0.85, edgecolor="black")
    if len(asap_runs) > 1:
        ax.errorbar(x + bar_w / 2, asap_mean, yerr=asap_std, fmt="none", color="black", capsize=2)
    ax.set_xlabel("Query Number")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency per Query")
    ax.set_xticks(np.arange(0, n + 1, 5))
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    # --- Subplot 2: Speedup ratio ---
    ax2 = axes[1]
    with np.errstate(divide="ignore", invalid="ignore"):
        speedup = np.where(asap_mean > 0, np.array(b_lat[:n]) / asap_mean, 0.0)
    colors = ["#2ca02c" if s >= 1.0 else "#d62728" for s in speedup]
    ax2.bar(x, speedup, color=colors, alpha=0.85, edgecolor="black")
    ax2.axhline(1.0, color="black", linestyle="--", linewidth=1, label="1x (no speedup)")
    ax2.axhline(2.0, color="gray", linestyle=":", linewidth=1, label="2x target")
    ax2.set_xlabel("Query Number")
    ax2.set_ylabel("Speedup (Baseline / ASAP)")
    ax2.set_title("ASAP Speedup Factor per Query  (green = faster, red = slower)")
    ax2.set_xticks(np.arange(0, n + 1, 5))
    ax2.legend()
    ax2.grid(axis="y", linestyle="--", alpha=0.5)

    out1 = output_dir / f"comparison_{timestamp}.png"
    plt.tight_layout()
    plt.savefig(out1, dpi=150)
    plt.close()
    print(f"Saved: {out1}")

    # --- Figure 2: All ASAP runs overlaid (consistency check) ---
    if len(asap_runs) > 1:
        fig2, ax3 = plt.subplots(figsize=(14, 6))
        colors_runs = plt.cm.tab10.colors
        for i, run_lats in enumerate(asap_runs):
            ax3.plot(x, run_lats[:n], marker="o", markersize=3, linewidth=1,
                     label=f"ASAP run {i + 1}", color=colors_runs[i % 10], alpha=0.7)
        ax3.plot(x, b_lat, marker="s", markersize=3, linewidth=1.5,
                 label="Baseline", color="black", linestyle="--")
        ax3.set_xlabel("Query Number")
        ax3.set_ylabel("Latency (ms)")
        ax3.set_title("ASAP Run Consistency — All Runs Overlaid")
        ax3.set_xticks(np.arange(0, n + 1, 5))
        ax3.legend()
        ax3.grid(linestyle="--", alpha=0.4)
        out2 = output_dir / f"asap_runs_overlay_{timestamp}.png"
        plt.tight_layout()
        plt.savefig(out2, dpi=150)
        plt.close()
        print(f"Saved: {out2}")

    # --- Summary statistics ---
    valid_b = [v for v in b_lat if v > 0]
    valid_a = [v for v in asap_mean if v > 0]
    if valid_b and valid_a:
        print(f"\nSummary Statistics:")
        print(f"  Baseline  — mean: {np.mean(valid_b):7.1f}ms  median: {np.median(valid_b):7.1f}ms  p95: {np.percentile(valid_b, 95):7.1f}ms")
        print(f"  ASAP avg  — mean: {np.mean(valid_a):7.1f}ms  median: {np.median(valid_a):7.1f}ms  p95: {np.percentile(valid_a, 95):7.1f}ms")
        mean_speedup = np.mean(valid_b) / np.mean(valid_a)
        median_speedup = np.median(valid_b) / np.median(valid_a)
        print(f"  Mean speedup:   {mean_speedup:.2f}x")
        print(f"  Median speedup: {median_speedup:.2f}x")
        if mean_speedup < 2.0:
            print(f"  WARNING: Mean speedup {mean_speedup:.2f}x is below the 2x target.")
            print("  Check /tmp/query_engine.log to confirm ASAP is serving from sketches,")
            print("  not falling back to ClickHouse for every query.")


def main():
    parser = argparse.ArgumentParser(description="Run ASAP vs baseline experiments")
    parser.add_argument("--runs", type=int, default=3, help="Number of ASAP runs (default: 3)")
    parser.add_argument("--load-data", action="store_true", help="Download and load H2O dataset")
    parser.add_argument("--skip-baseline", action="store_true", help="Skip baseline; use existing baseline_results.csv")
    parser.add_argument("--skip-infra", action="store_true", help="Skip starting Kafka/ClickHouse (assume already running)")
    parser.add_argument("--output-dir", default=".", help="Directory for output files (default: .)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    baseline_file = output_dir / "baseline_results.csv"
    asap_files = []

    # ---- Step 1: Baseline ----
    if not args.skip_baseline:
        print("\n" + "="*60)
        print("STEP 1: Baseline run")
        print("="*60)
        ok = run_pipeline(
            "baseline", baseline_file,
            load_data=args.load_data,
            skip_infra=args.skip_infra,
        )
        if not ok:
            print("WARNING: Baseline run reported a non-zero exit code")
    else:
        print(f"Skipping baseline; using: {baseline_file}")

    # ---- Steps 2..N+1: ASAP runs ----
    for i in range(1, args.runs + 1):
        print(f"\n{'='*60}")
        print(f"STEP {1 + i}: ASAP run {i}/{args.runs}")
        print("="*60)

        asap_file = output_dir / f"asap_results_run{i}_{timestamp}.csv"

        # Load data on the first ASAP run only (it also loads into MergeTree via
        # the materialized view, so baseline can reuse that data afterwards if needed)
        load_this_run = (i == 1) and args.load_data

        ok = run_pipeline(
            "asap", asap_file,
            load_data=load_this_run,
            skip_infra=args.skip_infra,
        )
        if ok or asap_file.exists():
            asap_files.append(asap_file)
        else:
            print(f"WARNING: ASAP run {i} produced no output")

    # ---- Graphs ----
    print(f"\n{'='*60}")
    print("Generating comparison graphs")
    print("="*60)

    if baseline_file.exists() and asap_files:
        generate_comparison_graphs(baseline_file, asap_files, output_dir, timestamp)

        # Run existing value-accuracy comparison for first ASAP run
        script_dir = Path(__file__).parent
        values_png = output_dir / f"value_comparison_{timestamp}.png"
        subprocess.run([
            "python3", str(script_dir / "compare_values.py"),
            "--baseline", str(baseline_file),
            "--asap", str(asap_files[0]),
            "--output", str(values_png),
        ], check=False)
    else:
        missing = []
        if not baseline_file.exists():
            missing.append(str(baseline_file))
        if not asap_files:
            missing.append("(no ASAP results)")
        print(f"Missing files, skipping graphs: {', '.join(missing)}")

    print("\nExperiment complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

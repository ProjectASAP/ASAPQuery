#!/usr/bin/env python3
"""Cost-latency scatter from collate_batch.py's CSV: one panel per workload
set, one point per (workload, system); x = p95 query latency, y = total
CPU-seconds, both log. Lower left is better. A run collate_batch.py marks
invalid (failed queries, or answers that disagree with the reference) is
drawn hollow: its latency and CPU don't stand for a correct answer.

Writes <out_dir>/cost_latency_<sets>_<YYYYmmdd-HHMMSS>.png; never overwrites.

Usage: python3 plot_cost_latency.py collated.csv out_dir
"""

import csv
import datetime
import sys
from pathlib import Path
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Validated categorical slots 1-5 (fixed order) and a marker per system, so
# identity never rests on color alone.
SYSTEMS = [
    ("asap", "LiteOlly", "#2a78d6", "o"),
    ("prom", "Prometheus", "#eb6834", "s"),
    ("vm", "VictoriaMetrics", "#1baf7a", "^"),
    ("ch", "ClickHouse", "#eda100", "D"),
    ("chmv", "ClickHouse+MV", "#e87ba4", "v"),
]
SURFACE, INK, MUTED = "#fcfcfb", "#0b0b0b", "#52514e"


def main(csv_path, out_dir):
    rows = list(csv.DictReader(open(csv_path)))
    by_set = defaultdict(list)
    for r in rows:
        by_set[r["set"]].append(r)
    sets = sorted(by_set)
    fig, axes = plt.subplots(
        1, len(sets), figsize=(7 * len(sets), 6), facecolor=SURFACE, squeeze=False
    )
    for ax, set_name in zip(axes[0], sets):
        ax.set_facecolor(SURFACE)
        workloads = sorted({r["workload"] for r in by_set[set_name]})
        for key, label, color, marker in SYSTEMS:
            pts = [r for r in by_set[set_name] if r["arm"] == key]
            xs = [float(r["latency_p95"]) for r in pts]
            ys = [float(r["cpu_secs"]) for r in pts]
            valid = [r["valid"] == "True" for r in pts]
            ax.scatter(
                [x for x, v in zip(xs, valid) if v],
                [y for y, v in zip(ys, valid) if v],
                s=128,
                color=color,
                marker=marker,
                label=label,
                edgecolors=SURFACE,
                linewidths=1,
                zorder=3,
            )
            ax.scatter(
                [x for x, v in zip(xs, valid) if not v],
                [y for y, v in zip(ys, valid) if not v],
                s=112,
                facecolors="none",
                edgecolors=color,
                marker=marker,
                linewidths=2.5,
                zorder=3,
            )
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("p95 query latency (s)", color=MUTED, fontsize=14)
        ax.set_ylabel(
            "total CPU (CPU-seconds, warm-up + queries)", color=MUTED, fontsize=14
        )
        title = (
            f"{set_name}: {len(workloads)} workload(s), lower left is better"
            "\nhollow: invalid run (failed queries or wrong answers)"
        )
        ax.set_title(title, color=INK, fontsize=14, loc="left")
        ax.grid(True, which="major", color="#e6e5e1", linewidth=0.8)
        ax.tick_params(colors=MUTED, labelsize=13)
        for spine in ax.spines.values():
            spine.set_color("#d4d3cf")
    axes[0][0].legend(frameon=False, fontsize=13, loc="best")
    fig.tight_layout()
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = Path(out_dir) / f"cost_latency_{'+'.join(sets)}_{stamp}.png"
    with open(out_path, "xb") as f:
        fig.savefig(f, dpi=150, facecolor=SURFACE, format="png")
    print(out_path)


if __name__ == "__main__":
    main(*sys.argv[1:3])

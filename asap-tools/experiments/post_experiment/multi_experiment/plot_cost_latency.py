#!/usr/bin/env python3
"""Cost-latency scatter from collate_batch.py's CSV: one panel per workload
set, one point per (workload, system); x = p95 query latency, y = total
CPU-seconds, both log. Lower left is better. A run collate_batch.py marks
invalid (failed queries, or answers that disagree with the reference) is
drawn hollow: its latency and CPU don't stand for a correct answer.

Usage: python3 plot_cost_latency.py collated.csv out.png
"""

import csv
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Validated categorical slots 1-5 (fixed order) and a marker per system, so
# identity never rests on color alone.
SYSTEMS = [
    ("asap", "ASAPQuery", "#2a78d6", "o", (8, 6)),
    ("prom", "Prometheus", "#eb6834", "s", (8, 6)),
    ("vm", "VictoriaMetrics", "#1baf7a", "^", (8, -14)),
    ("ch", "ClickHouse", "#eda100", "D", (8, 6)),
    ("chmv", "ClickHouse+MV", "#e87ba4", "v", (-30, 12)),
]
SURFACE, INK, MUTED = "#fcfcfb", "#0b0b0b", "#52514e"


def main(csv_path, out_path):
    rows = list(csv.DictReader(open(csv_path)))
    by_set = defaultdict(list)
    for r in rows:
        by_set[r["set"]].append(r)
    sets = sorted(by_set)
    fig, axes = plt.subplots(
        1, len(sets), figsize=(6 * len(sets), 5), facecolor=SURFACE, squeeze=False
    )
    for ax, set_name in zip(axes[0], sets):
        ax.set_facecolor(SURFACE)
        workloads = sorted({r["workload"] for r in by_set[set_name]})
        for key, label, color, marker, offset in SYSTEMS:
            pts = [r for r in by_set[set_name] if r["arm"] == key]
            xs = [float(r["latency_p95"]) for r in pts]
            ys = [float(r["cpu_secs"]) for r in pts]
            valid = [r["valid"] == "True" for r in pts]
            ax.scatter(
                [x for x, v in zip(xs, valid) if v],
                [y for y, v in zip(ys, valid) if v],
                s=64,
                color=color,
                marker=marker,
                label=label,
                edgecolors=SURFACE,
                linewidths=2,
                zorder=3,
            )
            ax.scatter(
                [x for x, v in zip(xs, valid) if not v],
                [y for y, v in zip(ys, valid) if not v],
                s=56,
                facecolors="none",
                edgecolors=color,
                marker=marker,
                linewidths=1.5,
                zorder=3,
            )
            if xs:
                # Direct label at the system's median point.
                mx, my = sorted(xs)[len(xs) // 2], sorted(ys)[len(ys) // 2]
                ax.annotate(
                    label,
                    (mx, my),
                    xytext=offset,
                    textcoords="offset points",
                    fontsize=9,
                    color=INK,
                )
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("p95 query latency (s)", color=MUTED)
        ax.set_ylabel("total CPU (CPU-seconds, warm-up + queries)", color=MUTED)
        title = (
            f"{set_name}: {len(workloads)} workload(s), lower left is better"
            "\nhollow: invalid run (failed queries or wrong answers)"
        )
        ax.set_title(title, color=INK, fontsize=10, loc="left")
        ax.grid(True, which="major", color="#e6e5e1", linewidth=0.8)
        ax.tick_params(colors=MUTED)
        for spine in ax.spines.values():
            spine.set_color("#d4d3cf")
    axes[0][0].legend(frameon=False, fontsize=9, loc="upper left")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, facecolor=SURFACE)


if __name__ == "__main__":
    main(*sys.argv[1:3])

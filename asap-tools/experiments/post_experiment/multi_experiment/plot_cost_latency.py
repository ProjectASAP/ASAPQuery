#!/usr/bin/env python3
"""Cost-latency scatter from collate_batch.py's CSV: one panel per workload
set, one point per (workload, system); x = total CPU-seconds, y = p95 query
latency, both log. Lower left is better. A run collate_batch.py marks
invalid (failed queries, or answers that disagree with the reference) is
drawn hollow: its latency and CPU don't stand for a correct answer.

Two arrows from LiteOlly's geometric-mean point show its improvement in CPU
(horizontal) and p95 latency (vertical) over the worst baseline for that
metric: collate_batch.py's geometric mean of baseline / LiteOlly over
workloads where both are valid. Arrow lengths are those ratios.

An oval encloses each baseline's valid runs (all its runs if none is valid).

Writes <out_dir>/cost_latency_<sets>_<YYYYmmdd-HHMMSS>.png; never overwrites.

Usage: python3 plot_cost_latency.py collated.csv out_dir
"""

import csv
import datetime
import statistics
import sys
from pathlib import Path
from collections import defaultdict

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import ticker  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from collate_batch import ratios  # noqa: E402

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


def oval(ax, xs, ys, color):
    """Ellipse in log space along the points' covariance, scaled to enclose
    them all with a fixed margin (in decades) for the markers."""
    pts = np.log10(np.column_stack([xs, ys]))
    center = pts.mean(axis=0)
    # The small diagonal keeps one or collinear points drawable.
    cov = np.cov(pts.T, bias=True) + np.eye(2) * 1e-4
    inv = np.linalg.inv(cov)
    d = pts - center
    reach = np.sqrt(np.einsum("ij,jk,ik->i", d, inv, d).max())
    vals, vecs = np.linalg.eigh(cov)
    t = np.linspace(0, 2 * np.pi, 200)
    ring = (vecs * (np.sqrt(vals) * reach + 0.05)) @ np.vstack([np.cos(t), np.sin(t)])
    ax.plot(*10 ** (ring + center[:, None]), color=color, linewidth=1.5, zorder=2)


def arrows(ax, rows, improvement, set_name):
    """LiteOlly's improvement over the worst baseline per metric, drawn from
    LiteOlly's geometric-mean point (over its valid runs, else all)."""
    names = {key: label for key, label, *_ in SYSTEMS}
    lite = [r for r in rows if r["arm"] == "asap"]
    lite = [r for r in lite if r["valid"] == "True"] or lite
    lx = statistics.geometric_mean(float(r["cpu_secs"]) for r in lite)
    ly = statistics.geometric_mean(float(r["latency_p95"]) for r in lite)
    worst = {}
    for metric in ("cpu_secs", "latency_p95"):
        worst[metric] = max(
            ((arm, m[metric]) for (s, arm), m in improvement.items() if s == set_name),
            key=lambda am: am[1],
        )
    style = dict(arrowstyle="<->", color=INK, linewidth=2, mutation_scale=22)
    arm, lat = worst["latency_p95"]
    x = lx / 1.5
    ax.annotate("", (x, ly), xytext=(x, ly * lat), arrowprops=style, zorder=4)
    ax.text(
        x * 1.08,
        ly * lat**0.75,
        f"{lat:.1f}x lower p95\nlatency vs\n{names[arm]}",
        color=INK,
        fontsize=13,
        va="center",
    )
    arm, cpu = worst["cpu_secs"]
    y = ly / 1.4
    ax.annotate("", (lx, y), xytext=(lx * cpu, y), arrowprops=style, zorder=4)
    ax.text(
        lx * cpu**0.5,
        y / 1.08,
        f"{cpu:.1f}x less CPU vs {names[arm]}",
        color=INK,
        fontsize=13,
        ha="center",
        va="top",
    )
    left, right = ax.get_xlim()
    ax.set_xlim(min(left, lx / 2), right)
    bottom, top = ax.get_ylim()
    ax.set_ylim(min(bottom, ly / 2), top)


def main(csv_path, out_dir):
    rows = list(csv.DictReader(open(csv_path)))
    by_set = defaultdict(list)
    for r in rows:
        by_set[r["set"]].append(r)
    sets = sorted(by_set)
    numeric = ("cpu_secs", "latency_p50", "latency_p95", "latency_sum")
    improvement = ratios(
        [
            {**r, "valid": r["valid"] == "True", **{k: float(r[k]) for k in numeric}}
            for r in rows
        ]
    )
    fig, axes = plt.subplots(
        1, len(sets), figsize=(7 * len(sets), 6), facecolor=SURFACE, squeeze=False
    )
    for ax, set_name in zip(axes[0], sets):
        ax.set_facecolor(SURFACE)
        for key, label, color, marker in SYSTEMS:
            pts = [r for r in by_set[set_name] if r["arm"] == key]
            xs = [float(r["cpu_secs"]) for r in pts]
            ys = [float(r["latency_p95"]) for r in pts]
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
            # Invalid runs don't stand for the system, so they don't stretch its oval.
            fit = [(x, y) for x, y, v in zip(xs, ys, valid) if v] or list(zip(xs, ys))
            if key != "asap" and fit:
                oval(ax, *zip(*fit), color)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("Total Compute Cost", color=MUTED, fontsize=14)
        ax.set_ylabel("p95 Query Latency", color=MUTED, fontsize=14)
        for axis in (ax.xaxis, ax.yaxis):
            axis.set_major_locator(ticker.LogLocator(subs=(1, 2, 5)))
            axis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:g}"))
            axis.set_minor_formatter(ticker.NullFormatter())
        ax.grid(True, which="major", color="#e6e5e1", linewidth=0.8)
        ax.tick_params(colors=MUTED, labelsize=13, width=2)
        for spine in ax.spines.values():
            spine.set_color(INK)
            spine.set_linewidth(2)
        arrows(ax, by_set[set_name], improvement, set_name)
    fig.suptitle(
        "Cost-latency tradeoffs of LiteOlly vs. baselines", color=INK, fontsize=16
    )
    fig.legend(
        *axes[0][0].get_legend_handles_labels(),
        frameon=False,
        fontsize=13,
        loc="lower center",
        ncol=min(len(SYSTEMS), 3 * len(sets)),
    )
    # A one-panel figure fits three legend entries a row, so two rows.
    fig.tight_layout(rect=(0, 0.07 if len(sets) > 1 else 0.12, 1, 1))
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_path = Path(out_dir) / f"cost_latency_{'+'.join(sets)}_{stamp}.png"
    with open(out_path, "xb") as f:
        fig.savefig(f, dpi=150, facecolor=SURFACE, format="png")
    print(out_path)


if __name__ == "__main__":
    main(*sys.argv[1:3])

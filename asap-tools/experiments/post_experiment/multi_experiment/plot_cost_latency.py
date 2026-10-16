#!/usr/bin/env python3
"""Cost-latency scatter from collate_batch.py's CSV: one panel per workload
set, one point per (workload, system); x = p95 query latency, y = total
CPU-seconds, both log. Lower left is better. A run collate_batch.py marks
invalid (failed queries, or answers that disagree with the reference) is
drawn hollow: its latency and CPU don't stand for a correct answer. A run
whose ingest fell behind is invalid too, and VictoriaMetrics counts as exact
(its 1 s windows may hold one boundary sample Prometheus's don't).

Two arrows from LiteOlly's geometric-mean point show its improvement in CPU
(vertical) and p95 latency (horizontal) over the worst baseline for that
metric: collate_batch.py's geometric mean of baseline / LiteOlly over
workloads where both are valid. Arrow lengths are those ratios; labels give
the ratios against Prometheus and ClickHouse.

Each system's runs are drawn faint; a large marker sits at their geometric
mean, with whiskers spanning the runs' range on each axis (valid runs, all if
none is valid), labelled with the geometric mean of their peak memory.

Writes <out_dir>/cost_latency_<sets>_<YYYYmmdd-HHMMSS>.png; never overwrites.

Usage: python3 plot_cost_latency.py collated.csv out_dir [legend suffix for baselines]
"""

import csv
import datetime
import statistics
import sys
from pathlib import Path
from collections import defaultdict

import matplotlib

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
    # ClickHouse+MV with approximate quantiles, when a CSV includes it (slot 8).
    ("chmvaq", "ClickHouse+MV (approx.)", "#e34948", "<"),
]
SURFACE, INK = "#fcfcfb", "#0b0b0b"


# Memory label offset (points) and alignment; VictoriaMetrics sits just left
# of ClickHouse, so its label goes up and left; LiteOlly's goes up, clear
# of the latency arrow below it.
MEM_LABEL = {"vm": ((-10, 10), "right", "bottom"), "asap": ((10, 8), "left", "bottom")}


def summary(ax, xs, ys, mems, color, marker, label, key):
    """Geometric-mean marker with min-max whiskers on both axes, labelled with
    the geometric-mean peak memory."""
    gx, gy = statistics.geometric_mean(xs), statistics.geometric_mean(ys)
    ax.errorbar(
        gx,
        gy,
        xerr=[[gx - min(xs)], [max(xs) - gx]],
        yerr=[[gy - min(ys)], [max(ys) - gy]],
        fmt="none",
        ecolor=color,
        elinewidth=2,
        capsize=4,
        zorder=4,
    )
    ax.scatter(
        gx,
        gy,
        s=260,
        color=color,
        marker=marker,
        label=label,
        edgecolors=INK,
        linewidths=1.5,
        zorder=5,
    )
    offset, ha, va = MEM_LABEL.get(key, ((9, -9), "left", "top"))
    ax.annotate(
        f"{statistics.geometric_mean(mems):.2g} GiB",
        (gx, gy),
        xytext=offset,
        textcoords="offset points",
        color=INK,
        fontsize=16,
        ha=ha,
        va=va,
        zorder=6,
        bbox=dict(boxstyle="round,pad=0.15", fc=SURFACE, ec="none", alpha=0.8),
    )


def arrows(ax, rows, improvement, set_name):
    """LiteOlly's improvement over the worst baseline per metric, drawn from
    LiteOlly's geometric-mean point (over its valid runs, else all) and
    labelled with its ratios against Prometheus and ClickHouse."""
    names = {key: label for key, label, *_ in SYSTEMS}
    lite = [r for r in rows if r["arm"] == "asap"]
    leftmost = min(float(r["latency_p95"]) for r in lite)
    lite = [r for r in lite if r["valid"] == "True"] or lite
    lx = statistics.geometric_mean(float(r["latency_p95"]) for r in lite)
    ly = statistics.geometric_mean(float(r["cpu_secs"]) for r in lite)
    worst = {}
    for metric in ("cpu_secs", "latency_p95"):
        worst[metric] = max(
            ((arm, m[metric]) for (s, arm), m in improvement.items() if s == set_name),
            key=lambda am: am[1],
        )

    present = [a for a in ("prom", "ch", "chmv") if (set_name, a) in improvement]

    def vs(metric, word):
        """e.g. "6.2x cheaper vs. ClickHouse,\n4.9x vs. Prometheus", largest first."""
        (a, ra), (b, rb) = sorted(
            ((a, improvement[(set_name, a)][metric]) for a in present[:2]),
            key=lambda ar: -ar[1],
        )
        return f"{ra:.1f}x {word} vs. {names[a]},\n{rb:.1f}x vs. {names[b]}"

    style = dict(arrowstyle="<->", color=INK, linewidth=2, mutation_scale=22)
    arm, cpu = worst["cpu_secs"]
    # Left of every LiteOlly point; its label sits above the arrow's top.
    x = leftmost / 1.3
    ax.annotate("", (x, ly), xytext=(x, ly * cpu), arrowprops=style, zorder=4)
    ax.text(
        x / 1.15,
        ly * cpu * 1.12,
        vs("cpu_secs", "cheaper"),
        color=INK,
        fontsize=17,
        ha="left",
        va="bottom",
    )
    arm, lat = worst["latency_p95"]
    y = ly / 1.5
    ax.annotate("", (lx, y), xytext=(lx * lat, y), arrowprops=style, zorder=4)
    ax.text(
        lx * lat**0.5,
        y / 1.08,
        vs("latency_p95", "faster"),
        color=INK,
        fontsize=17,
        ha="center",
        va="top",
    )
    left, right = ax.get_xlim()
    # Room on the right for the slowest system's memory label.
    ax.set_xlim(min(left, x / 1.5), right * 1.8)
    bottom, top = ax.get_ylim()
    ax.set_ylim(min(bottom, ly / 3.2), max(top, ly * cpu * 2.2))


def main(csv_path, out_dir, suffix=""):
    rows = list(csv.DictReader(open(csv_path)))
    for r in rows:
        counted = r["valid"] == "True" or r["arm"] == "vm"
        r["valid"] = str(counted and r["ingest_kept_up"] != "False")
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
    for i, (ax, set_name) in enumerate(zip(axes[0], sets)):
        ax.set_facecolor(SURFACE)
        if len(sets) > 1:
            ax.set_title(f"{set_name} dataset", color=INK, fontsize=21)
        for key, label, color, marker in SYSTEMS:
            label = label if key == "asap" else label + suffix
            pts = [r for r in by_set[set_name] if r["arm"] == key]
            xs = [float(r["latency_p95"]) for r in pts]
            ys = [float(r["cpu_secs"]) for r in pts]
            valid = [r["valid"] == "True" for r in pts]
            mems = [float(r["peak_memory_gib"]) for r in pts]
            ax.scatter(
                [x for x, v in zip(xs, valid) if v],
                [y for y, v in zip(ys, valid) if v],
                s=60,
                color=color,
                marker=marker,
                alpha=0.35,
                edgecolors="none",
                zorder=3,
            )
            ax.scatter(
                [x for x, v in zip(xs, valid) if not v],
                [y for y, v in zip(ys, valid) if not v],
                s=60,
                facecolors="none",
                edgecolors=color,
                marker=marker,
                alpha=0.6,
                linewidths=1.5,
                zorder=3,
            )
            # Invalid runs don't stand for the system, so they don't move its summary.
            fit = [(x, y, m) for x, y, m, v in zip(xs, ys, mems, valid) if v] or list(
                zip(xs, ys, mems)
            )
            if fit:
                summary(ax, *zip(*fit), color, marker, label, key)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("p95 Query Latency (s)", color=INK, fontsize=19)
        if i == 0:
            ax.set_ylabel("Total Compute Cost (CPU-sec)", color=INK, fontsize=19)
        for axis in (ax.xaxis, ax.yaxis):
            axis.set_major_locator(ticker.LogLocator(subs=(1,)))
            axis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:g}"))
            axis.set_minor_formatter(ticker.NullFormatter())
        ax.grid(True, which="major", color="#e6e5e1", linewidth=0.8)
        ax.tick_params(colors=INK, labelsize=18, width=2)
        for spine in ax.spines.values():
            spine.set_color(INK)
            spine.set_linewidth(2)
        arrows(ax, by_set[set_name], improvement, set_name)
    # Several panels are titled by dataset; a single one gets a figure title.
    if len(sets) == 1:
        fig.suptitle(
            (
                f"LiteOlly vs. baselines{suffix}: cost and latency"
                if suffix
                else "Cost-latency tradeoffs of LiteOlly vs. baselines"
            ),
            color=INK,
            fontsize=18,
        )
    fig.legend(
        *axes[0][0].get_legend_handles_labels(),
        frameon=False,
        fontsize=18,
        loc="lower center",
        # Suffixed labels are longer: two a row.
        ncol=min(len(SYSTEMS), (2 if suffix else 3) * len(sets)),
    )
    # A one-panel figure fits three legend entries a row, so two rows.
    fig.tight_layout(rect=(0, 0.1 if len(sets) > 1 else 0.17, 1, 1))
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    name = "+".join(s.replace(" ", "-") for s in sets)
    out_path = Path(out_dir) / f"cost_latency_{name}_{stamp}.png"
    with open(out_path, "xb") as f:
        fig.savefig(f, dpi=150, facecolor=SURFACE, format="png", bbox_inches="tight")
    print(out_path)


if __name__ == "__main__":
    main(*sys.argv[1:4])

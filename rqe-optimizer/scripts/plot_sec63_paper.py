#!/usr/bin/env python3
"""The paper's §6.3 figures, from the committed AutoSketch-vs-ASAP results.

Writes into OUT (PDF and PNG each):
  sec63_cost_bars     each method's cost at AutoSketch-Adapted's batch latency
                      (CPU only, log scale), per workload (panels) at base,
                      8 users and 16 metrics; a baseline's bar also names how
                      many times ASAP's cost it is. Drawn at a figure*'s
                      7 in; sec63_cost_bars_column stacks the panels at one
                      column's 3.4 in
  sec63_frontier      cost vs. batch latency (ASAP and PerQuery swept over
                      latency bounds; AutoSketch one point), CPU and Fargate
  sec63_rollups       the roll-up ablation: ASAP's cheapest plan with and
                      without roll-ups (CPU only, log scale)

Every cost is absolute (vCPU or $/hour).

Usage: scripts/plot_sec63_paper.py RESULTS_DIR OUT
  RESULTS_DIR: rqe-optimizer/results/autosketch-vs-asap-hydra
"""

import json
import os
import sys

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

plt.rcParams.update({"font.size": 8, "pdf.fonttype": 42})

WORKLOADS = [
    ("classic", "Aggregations"),
    ("multigroup", "Roll-ups"),
    ("all", "Aggregations + roll-ups"),
]
VARIANTS = [
    ("shared1", "base"),
    ("shared8", "8 users"),
    ("shared1-metrics16", "16 metrics"),
]
METHODS = [
    ("autosketch", "AutoSketch-Adapted", "C2"),
    ("perquery", "PerQuery", "C1"),
    ("asap", "ASAP", "C0"),
]


def load(results, table):
    with open(os.path.join(results, f"synthetic-templates{table}-tp95.json")) as f:
        return json.load(f)


def plans(data, method, weights):
    return [
        r
        for r in data["results"]
        if r["weights"] == weights and r["method"] == method and "objective" in r
    ]


def cheapest(data, method, weights="cpu", max_latency_ms=None):
    return min(
        (
            r
            for r in plans(data, method, weights)
            if max_latency_ms is None or r["latency_ms"] <= max_latency_ms + 1e-6
        ),
        key=lambda r: r["objective"],
    )["objective"]


def save(fig, out, name, tight=False):
    """PDF and PNG; `tight` trims the page to the drawing (2 pt margin)."""
    extra = {"bbox_inches": "tight", "pad_inches": 0.02} if tight else {}
    fig.savefig(os.path.join(out, f"{name}.pdf"), **extra)
    fig.savefig(os.path.join(out, f"{name}.png"), dpi=150, **extra)
    plt.close(fig)


def short(cost):
    """2.7k, 161, 4.5, 0.32: three significant figures at most."""
    if cost >= 1000:
        return f"{cost / 1000:.3g}k"
    return f"{cost:.3g}" if cost >= 1 else f"{cost:.2g}"


# Darker shades than matplotlib's defaults, so white labels inside the bars
# read clearly.
WIDE_COLORS = {"autosketch": "#2e7d32", "perquery": "#d35400", "asap": "#1f5fa8"}


def cost_bars_wide(results, out):
    """The figure* version (7.1 in, drawn at print size). ASAP's cost sits
    right on top of its bar; each baseline's multiple of it sits inside the
    baseline's bar in white, or just above the bar when the bar is too short
    to hold it. The page is trimmed to the drawing."""
    sizes = {
        "font.size": 9.5,
        "axes.titlesize": 11,
        "axes.labelsize": 10.5,
        "xtick.labelsize": 9.5,
        "ytick.labelsize": 9.5,
        "legend.fontsize": 10.5,
    }
    label_pt = 8.5
    with plt.rc_context(sizes):
        fig, axes = plt.subplots(1, len(WORKLOADS), figsize=(7.1, 3.0))
        width, gap = 0.3, 1.15
        labels_to_place = []
        for ax, (templates, title) in zip(axes, WORKLOADS):
            ticks = []
            for i, (variant, name) in enumerate(VARIANTS):
                data = load(results, f"{templates}-{variant}")
                limit = data["frontier"]["autosketch_latency_ms"]
                cost = {
                    m: cheapest(data, m, max_latency_ms=limit) for m, _, _ in METHODS
                }
                for k, (method, label, _) in enumerate(METHODS):
                    x = i * gap + (k - 1) * width
                    ax.bar(
                        x,
                        cost[method],
                        width * 0.95,
                        color=WIDE_COLORS[method],
                        label=label if i == 0 else None,
                    )
                    if method == "asap":
                        # Vertical, so it stays within its bar's width.
                        ax.text(
                            x,
                            cost[method] * 1.08,
                            short(cost[method]),
                            ha="center",
                            va="bottom",
                            rotation=90,
                            fontsize=label_pt,
                            weight="bold",
                            color=WIDE_COLORS["asap"],
                        )
                    else:
                        ratio = cost[method] / cost["asap"]
                        text = f"{ratio:.0f}×" if ratio >= 10 else f"{ratio:.1f}×"
                        labels_to_place.append(
                            (ax, x, cost[method], text, WIDE_COLORS[method])
                        )
                ticks.append(f"{name}\n{data['rqes']:,}")
            ax.set_yscale("log")
            ax.set_xticks([i * gap for i in range(len(VARIANTS))])
            ax.set_xticklabels(ticks, fontsize=8.5, linespacing=1.0)
            ax.set_xlim(-0.6, (len(VARIANTS) - 1) * gap + 0.6)
            ax.set_ylim(top=ax.get_ylim()[1] * 4)
            ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(numticks=4))
            ax.grid(axis="y", alpha=0.3, lw=0.4)
            ax.tick_params(axis="x", length=0)
            ax.set_title(title)
            ax.set_xlabel("workload scale (# RQEs)")
        axes[0].set_ylabel("cost (vCPU)")
        handles, names = axes[0].get_legend_handles_labels()
        fig.legend(
            handles,
            names,
            loc="upper center",
            ncol=len(METHODS),
            frameon=False,
            handlelength=1.2,
            columnspacing=1.2,
            bbox_to_anchor=(0.5, 1.02),
        )
        fig.tight_layout(rect=(0, 0, 1, 0.9), w_pad=0.3)
        # With the axes final: a label goes inside its bar, written
        # vertically in white, if the bar is tall enough (in points) to hold
        # it; else vertically just above the bar, in the bar's colour.
        fig.canvas.draw()
        for ax, x, top, text, color in labels_to_place:
            bottom = ax.get_ylim()[0]
            to_pt = ax.transData.transform
            height_pt = (to_pt((x, top))[1] - to_pt((x, bottom))[1]) * 72 / fig.dpi
            if height_pt > label_pt * 0.62 * len(text) + 6:
                ax.text(
                    x,
                    top / 1.12,
                    text,
                    ha="center",
                    va="top",
                    rotation=90,
                    fontsize=label_pt,
                    weight="bold",
                    color="white",
                )
            else:
                ax.text(
                    x,
                    top * 1.08,
                    text,
                    ha="center",
                    va="bottom",
                    rotation=90,
                    fontsize=label_pt,
                    weight="bold",
                    color=color,
                )
        save(fig, out, "sec63_cost_bars_wide", tight=True)


# The paper's names: the system, its no-sharing ablation, and the baseline.
SINGLE_METHODS = [
    ("asap", "LiteOlly", "#1f5fa8"),
    ("perquery", "LiteOllyPerQuery", "#8fb3de"),
    ("autosketch", "AutoSketch baseline", "#2e7d32"),
]
SINGLE_WORKLOADS = [
    ("classic", "W-RangeHeavy"),
    ("multigroup", "W-RollupHeavy"),
    ("all", "W-Mixed"),
]
SINGLE_VARIANTS = [("shared1", "base"), ("shared8", "8U"), ("shared1-metrics16", "16M")]


def cost_bars_single(results, out, weights="cpu"):
    """One set of axes at one column's width (3.4 in, drawn at print size):
    every workload and scale as a group of three bars (LiteOlly,
    LiteOllyPerQuery, AutoSketch baseline), each at AutoSketch's batch
    latency, log scale, under `weights` (CPU only, or Fargate's prices). LiteOlly's cost sits on its bar; each other
    bar names how many times LiteOlly's cost it is, inside the bar in white
    when it fits, else just above it. The page is trimmed to the drawing."""
    sizes = {
        "font.size": 8,
        "axes.labelsize": 8.5,
        "xtick.labelsize": 7.5,
        "ytick.labelsize": 8,
        "legend.fontsize": 8,
    }
    label_pt = 6.5
    with plt.rc_context(sizes):
        fig, ax = plt.subplots(figsize=(3.4, 2.5))
        width, gap, block = (
            0.32,
            1.02,
            0.35,
        )  # bar, group spacing, extra between workloads
        x0, ticks, ticklabels, spans, to_place = 0.0, [], [], [], []
        for templates, title in SINGLE_WORKLOADS:
            start = x0
            for variant, name in SINGLE_VARIANTS:
                data = load(results, f"{templates}-{variant}")
                limit = data["frontier"]["autosketch_latency_ms"]
                cost = {
                    m: cheapest(data, m, weights, max_latency_ms=limit)
                    for m, _, _ in SINGLE_METHODS
                }
                for k, (method, label, color) in enumerate(SINGLE_METHODS):
                    x = x0 + (k - 1) * width
                    ax.bar(
                        x,
                        cost[method],
                        width * 0.92,
                        color=color,
                        label=label if not ticks else None,
                    )
                    if method == "asap":
                        ax.text(
                            x,
                            cost[method] * 1.1,
                            short(cost[method]),
                            ha="center",
                            va="bottom",
                            rotation=90,
                            fontsize=label_pt,
                            weight="bold",
                            color=color,
                        )
                    else:
                        ratio = cost[method] / cost["asap"]
                        text = f"{ratio:.0f}×" if ratio >= 10 else f"{ratio:.1f}×"
                        to_place.append((x, cost[method], text, color))
                ticks.append(x0)
                ticklabels.append(name)
                x0 += gap
            spans.append((start, x0 - gap, title))
            x0 += block
        ax.set_yscale("log")
        ax.set_xticks(ticks)
        ax.set_xticklabels(ticklabels, rotation=0)
        ax.set_xlim(-0.55, x0 - block - gap + 0.55)
        ax.set_ylim(top=ax.get_ylim()[1] * 6)
        ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(numticks=5))
        ax.grid(axis="y", alpha=0.3, lw=0.4)
        ax.tick_params(axis="x", length=0, pad=2)
        ax.set_ylabel("cost (vCPU)" if weights == "cpu" else "cost ($/hour)")
        # Workload names under their three groups; thin separators between them.
        for start, end, title in spans:
            ax.text(
                (start + end) / 2,
                -0.11,
                title,
                transform=ax.get_xaxis_transform(),
                ha="center",
                va="top",
                fontsize=8,
            )
        for (_, end, _), (start, _, _) in zip(spans, spans[1:]):
            ax.axvline((end + start) / 2, color="0.6", lw=0.6, ls=":")
        handles, names = ax.get_legend_handles_labels()
        fig.tight_layout(rect=(0, 0, 1, 0.9))
        # Placed after the layout, so it does not narrow the axes.
        fig.legend(
            handles,
            names,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.0),
            ncol=3,
            frameon=False,
            fontsize=7.5,
            handlelength=0.9,
            handletextpad=0.4,
            columnspacing=0.7,
        )
        fig.canvas.draw()
        for x, top, text, color in to_place:
            bottom = ax.get_ylim()[0]
            to_pt = ax.transData.transform
            height_pt = (to_pt((x, top))[1] - to_pt((x, bottom))[1]) * 72 / fig.dpi
            inside = height_pt > label_pt * 0.62 * len(text) + 5
            ax.text(
                x,
                top / 1.12 if inside else top * 1.1,
                text,
                ha="center",
                va="top" if inside else "bottom",
                rotation=90,
                fontsize=label_pt,
                weight="bold",
                # White inside dark bars; the light LiteOllyPerQuery bar
                # takes dark blue inside and above it, where its own
                # colour would not read.
                color=(
                    "#0b2e59" if color == "#8fb3de" else "white" if inside else color
                ),
            )
        save(
            fig,
            out,
            "sec63_cost_bars" + ("" if weights == "cpu" else f"_{weights}"),
            tight=True,
        )


def cost_bars(results, out, layout):
    """`wide`: three panels side by side at a two-column figure*'s width
    (7 in). `column`: stacked, at one column's width (3.4 in). Both are drawn
    at their printed size, so fonts are what the paper shows."""
    wide = layout == "wide"
    sizes = (
        {
            "font.size": 9.5,
            "axes.titlesize": 11,
            "axes.labelsize": 10,
            "xtick.labelsize": 9.5,
            "ytick.labelsize": 9.5,
            "legend.fontsize": 10.5,
        }
        if wide
        else {
            "font.size": 8,
            "axes.titlesize": 9,
            "axes.labelsize": 8.5,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8.5,
        }
    )
    with plt.rc_context(sizes):
        fig, axes = (
            plt.subplots(1, len(WORKLOADS), figsize=(7.1, 3.1))
            if wide
            else plt.subplots(len(WORKLOADS), 1, figsize=(3.4, 5.8))
        )
        width = 0.3 if wide else 0.28
        gap = 1.15 if wide else 1.0  # between variant groups
        for ax, (templates, title) in zip(axes, WORKLOADS):
            labels = []
            for i, (variant, name) in enumerate(VARIANTS):
                data = load(results, f"{templates}-{variant}")
                limit = data["frontier"]["autosketch_latency_ms"]
                cost = {
                    m: cheapest(data, m, max_latency_ms=limit) for m, _, _ in METHODS
                }
                for k, (method, label, color) in enumerate(METHODS):
                    x = i * gap + (k - 1) * width
                    ax.bar(
                        x,
                        cost[method],
                        width * 0.95,
                        color=color,
                        label=label if i == 0 else None,
                    )
                    ratio = cost[method] / cost["asap"]
                    multiple = f"{ratio:.0f}×" if ratio >= 10 else f"{ratio:.1f}×"
                    if wide:
                        # One short label per bar: ASAP's cost, each
                        # baseline's multiple of it (its cost is on the axis).
                        text = short(cost[method]) if method == "asap" else multiple
                    else:
                        text = short(cost[method]) + (
                            "" if method == "asap" else "\n" + multiple
                        )
                    # Wide: when ASAP's bar is about as tall as PerQuery's, its
                    # label rises above PerQuery's, so neighbours never share a
                    # row (one label is about a factor 2.5 of height here).
                    y = cost[method] * 1.2
                    if wide and method == "asap":
                        y = max(y, cost["perquery"] * 1.2 * 2.6)
                    ax.text(
                        x,
                        y,
                        text,
                        ha="center",
                        va="bottom",
                        fontsize=8 if wide else 6.5,
                        linespacing=0.95,
                        weight="bold" if wide and method == "asap" else "normal",
                    )
                labels.append(
                    f"{name}\n{data['rqes']:,}"
                    if wide
                    else f"{name}\n{data['rqes']:,} RQEs"
                )
            ax.set_yscale("log")
            ax.set_xticks([i * gap for i in range(len(VARIANTS))])
            ax.set_xticklabels(labels, fontsize=8.5 if wide else 8, linespacing=1.0)
            ax.set_xlim(-0.62, (len(VARIANTS) - 1) * gap + 0.62)
            ax.set_title(title)
            ax.set_ylim(top=ax.get_ylim()[1] * (12 if wide else 25))
            ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(numticks=4))
            ax.grid(axis="y", alpha=0.3, lw=0.4)
            ax.tick_params(axis="x", length=0)
            if wide or ax is axes[len(WORKLOADS) // 2]:
                ax.set_ylabel("cost (vCPU)")
            if wide:
                ax.set_xlabel("workload scale (# RQEs)", fontsize=8.5)
            if wide and ax is not axes[0]:
                ax.set_ylabel("")
        handles, names = axes[0].get_legend_handles_labels()
        fig.legend(
            handles,
            names,
            loc="upper center",
            ncol=len(METHODS),
            frameon=False,
            handlelength=1.2,
            columnspacing=1.2,
        )
        fig.tight_layout(rect=(0, 0, 1, 0.89 if wide else 0.95), h_pad=1.4, w_pad=0.3)
        save(fig, out, "sec63_cost_bars" + ("" if wide else "_column"))


def frontier(results, out):
    lines = [("asap", "ASAP", "C0", "o"), ("perquery", "PerQuery", "C1", "s")]
    units = [("cpu", "cost (vCPU)"), ("fargate", "cost ($/hour)")]
    fig, axes = plt.subplots(2, len(WORKLOADS), figsize=(7, 4))
    for j, (templates, title) in enumerate(WORKLOADS):
        data = load(results, f"{templates}-shared1")
        for i, (weights, unit) in enumerate(units):
            ax = axes[i, j]
            for method, label, color, marker in lines:
                rs = sorted(plans(data, method, weights), key=lambda r: r["latency_ms"])
                ax.plot(
                    [r["latency_ms"] / 1e3 for r in rs],
                    [r["objective"] for r in rs],
                    "-",
                    color=color,
                    marker=marker,
                    ms=2.5,
                    lw=1,
                    label=label,
                )
            auto = plans(data, "autosketch", weights)[0]
            ax.scatter(
                [auto["latency_ms"] / 1e3],
                [auto["objective"]],
                marker="D",
                s=20,
                color="C2",
                label="AutoSketch-Adapted",
                zorder=5,
            )
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.xaxis.set_major_locator(matplotlib.ticker.LogLocator(subs=(1, 2, 5)))
            ax.xaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter("%g"))
            ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
            ax.grid(alpha=0.3, which="both", lw=0.3)
            if j == 0:
                ax.set_ylabel(unit)
            if i == 0:
                ax.set_title(f"{title} ({data['rqes']} RQEs)", fontsize=8)
            if i == len(units) - 1:
                ax.set_xlabel("batch latency (s)")
    handles, names = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, names, loc="upper center", ncol=3, fontsize=7, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    save(fig, out, "sec63_frontier")


def rollups(results, out):
    fig, ax = plt.subplots(figsize=(7, 2.3))
    x, ticks, labels = 0, [], []
    for templates, title in WORKLOADS:
        for variant, name in VARIANTS:
            data = load(results, f"{templates}-{variant}")
            for k, (method, label, color) in enumerate(
                [("asap-norollup", "ASAP w/o roll-ups", "C9"), ("asap", "ASAP", "C0")]
            ):
                cost = cheapest(data, method)
                ax.bar(
                    x + (k - 0.5) * 0.38,
                    cost,
                    0.38,
                    color=color,
                    label=label if x == 0 else None,
                    hatch="//" if k == 0 else None,
                    edgecolor="white",
                    lw=0,
                )
                ax.text(
                    x + (k - 0.5) * 0.38,
                    cost * 1.08,
                    f"{cost:.3g}",
                    ha="center",
                    fontsize=6,
                )
            ticks.append(x)
            labels.append(f"{name}\n({data['rqes']} RQEs)")
            x += 1
        ax.text(x - 2, 250, title, ha="center", fontsize=8, weight="bold")
        x += 0.6
    ax.set_yscale("log")
    ax.set_ylim(0.1, 900)
    ax.set_xticks(ticks)
    ax.set_xticklabels(labels, fontsize=6.5)
    ax.set_ylabel("cheapest plan (vCPU)")
    ax.legend(
        fontsize=7, loc="lower left", bbox_to_anchor=(0, 1), frameon=False, ncol=2
    )
    ax.grid(axis="y", alpha=0.3, lw=0.3)
    fig.tight_layout()
    save(fig, out, "sec63_rollups")


def main():
    results, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    cost_bars_wide(results, out)
    cost_bars_single(results, out)
    cost_bars_single(results, out, "fargate")
    cost_bars(results, out, "column")
    frontier(results, out)
    rollups(results, out)


if __name__ == "__main__":
    main()

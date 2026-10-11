"""Plot sweep.py results.

    python plot.py --input results/main.jsonl --experiment main --out main.pdf
"""

import argparse
import json
from collections import defaultdict

import matplotlib.pyplot as plt

# Fixed per pipeline (color follows the entity, not its rank). Validated
# colorblind-safe; low-contrast slots are backed by markers + legend.
STYLE = {
    "kll": ("#2a78d6", "o", "KLL"),
    "ddsketch": ("#eb6834", "s", "DDSketch"),
    "cms_heap": ("#1baf7a", "^", "CMS+Heap"),
    "hydra_kll": ("#eda100", "D", "HydraKLL"),
    "raw": ("#8a8a85", "x", "No sketch (raw)"),
}
MSAMPLES = 1e6
COLUMN_WIDTH_IN = 3.4  # one column of a typical two-column paper


def load(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def style(label):
    """Variant labels (e.g. kll_sliding) inherit their base pipeline's style."""
    base = max((k for k in STYLE if label.startswith(k)), key=len)
    return STYLE[base]


def series_title(n):
    return f"{n // 1_000_000}M series" if n >= 1_000_000 else f"{n // 1_000}K series"


def plot_main(rows, out):
    """2x2 grid sized for one column of a two-column paper."""
    by_series = defaultdict(lambda: defaultdict(list))
    for r in rows:
        by_series[r["num_series"]][r["label"]].append(r)
    plt.rcParams.update({"font.size": 8, "axes.titlesize": 8, "legend.fontsize": 8})
    fig, axes = plt.subplots(
        2, 2, figsize=(COLUMN_WIDTH_IN, 3.2), sharex=True, sharey=True
    )
    for ax, series in zip(axes.flat, sorted(by_series)):
        for label, pts in by_series[series].items():
            # Offered-load order; the open-loop saturation run (target 0) is last.
            pts.sort(key=lambda r: r["target_rate"] or float("inf"))
            color, marker, name = style(label)
            ax.plot(
                [r["engine_cpu_cores"] for r in pts],
                [r["achieved_rate"] / MSAMPLES for r in pts],
                color=color, marker=marker, linewidth=1.5, markersize=3.5, label=name,
            )  # fmt: skip
        ax.set_title(series_title(series), pad=3)
    ymax = max(r["achieved_rate"] for r in rows) / MSAMPLES
    max_cores = max(r["engine_cpu_cores"] for r in rows)
    axes[0][0].set_xlim(0, max_cores * 1.05)
    axes[0][0].set_xticks(range(0, int(max_cores) + 1, 2))
    axes[0][0].set_ylim(0, ymax * 1.08)
    fig.supxlabel("CPU cores", fontsize=8)
    fig.supylabel("Throughput (M samples/s)", fontsize=8)
    handles, names = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, names, loc="upper center", ncol=2, frameon=False,
               handlelength=1.5, columnspacing=1.0, bbox_to_anchor=(0.5, 1.06))  # fmt: skip
    finish(fig, axes.flat, out)


def plot_bars(rows, out):
    rows = sorted(rows, key=lambda r: r["label"])
    fig, ax = plt.subplots(figsize=(5, 0.45 * len(rows) + 1))
    ys = range(len(rows))
    ax.barh(
        ys,
        [r["achieved_rate"] / MSAMPLES for r in rows],
        color=[style(r["label"])[0] for r in rows],
    )
    ax.set_yticks(list(ys), [r["label"] for r in rows])
    for y, r in zip(ys, rows):
        ax.annotate(
            f" {r['engine_cpu_cores']:.1f} cores",
            (r["achieved_rate"] / MSAMPLES, y),
            va="center",
            fontsize=8,
        )
    ax.set_xlabel("Saturation throughput (M samples/s)")
    ax.margins(x=0.25, y=0.1)  # room for the cores annotations
    finish(fig, [ax], out)


def plot_scaling(rows, out):
    by_label = defaultdict(list)
    for r in rows:
        by_label[r["label"]].append(r)
    fig, ax = plt.subplots(figsize=(4, 3))
    for label, pts in by_label.items():
        pts.sort(key=lambda r: r["workers"])
        color, marker, name = style(label)
        ax.plot(
            [r["workers"] for r in pts], [r["achieved_rate"] / MSAMPLES for r in pts],
            color=color, marker=marker, linewidth=2, markersize=6, label=name,
        )  # fmt: skip
    ax.set_xscale("log", base=2)
    ax.set_xticks([r["workers"] for r in rows], [str(r["workers"]) for r in rows])
    ax.set_xlabel("Engine workers")
    ax.set_ylabel("Saturation throughput (M samples/s)")
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False)
    finish(fig, [ax], out)


def finish(fig, axes, out):
    for ax in axes:
        ax.grid(alpha=0.3, linewidth=0.5)
        ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout(rect=(0, 0, 1, 0.9) if fig.legends else None)
    fig.savefig(out, bbox_inches="tight", dpi=300)
    print(f"wrote {out}")


PLOTS = {
    "main": plot_main,
    "fanin": plot_bars,
    "window": plot_bars,
    "scaling": plot_scaling,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--experiment", required=True, choices=PLOTS)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    PLOTS[args.experiment](load(args.input), args.out)

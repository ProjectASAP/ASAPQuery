#!/usr/bin/env python3
"""AutoSketch vs. ASAP across accuracy targets (90%, 95%, 99%).

Reads `autosketch_vs_asap` results named `<table>-t<level>.json`, with level
`acc90`, `p95` (the committed 95% runs) or `acc99`, and writes into OUT:

  fig_cost_vs_target.png    each method's cheapest plan (absolute cost) vs.
                            the accuracy target, one panel per workload
                            (columns) and weight setting (rows)
  fig_cost_vs_target_at_autosketch_latency.png
                            the same, every method held to AutoSketch's
                            batch latency at that target
  summary_targets.md        per workload, weights and target: each method's
                            cheapest cost, latency and deployments, RQEs
                            dropped, roll-ups

Usage: rqe-optimizer/scripts/plot_autosketch_vs_asap_targets.py OUT RESULT.json...
"""

import json
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

LEVELS = {"acc90": 90, "p95": 95, "acc95": 95, "acc99": 99}
METHODS = ["asap", "asap-norollup", "perquery", "autosketch"]
LABELS = {
    "asap": "ASAP",
    "asap-norollup": "ASAP (no roll-ups)",
    "perquery": "PerQuery-CostAware",
    "autosketch": "AutoSketch-Adapted",
}
COLORS = {
    "asap": "#1f77b4",
    "asap-norollup": "#4a3aa7",
    "perquery": "#ff7f0e",
    "autosketch": "#2ca02c",
}
STYLES = {"asap-norollup": "--"}
MARKERS = {"asap": "o", "asap-norollup": "^", "perquery": "s", "autosketch": "D"}
UNITS = {"cpu": "vCPU", "fargate": "$/hour"}
WEIGHTS = ["cpu", "fargate"]
TEMPLATES = {
    "classic": "mixed",
    "multigroup": "multi-grouping",
    "all": "mixed + multi-grouping",
}
NAME = re.compile(
    r"synthetic-templates(\w+)-shared(\d+)(?:-metrics(\d+))?-t(\w+)\.json$"
)


def workload_title(templates, shared, metrics):
    parts = [TEMPLATES.get(templates, templates)]
    if shared > 1:
        parts.append(f"{shared} users")
    if metrics > 1:
        parts.append(f"{metrics} metrics")
    return ", ".join(parts)


def cheapest(data, method, weights, max_latency=None):
    plans = [
        r
        for r in data["results"]
        if r["method"] == method
        and r["weights"] == weights
        and "objective" in r
        and (max_latency is None or r["latency_ms"] <= max_latency + 1e-6)
    ]
    return min(plans, key=lambda r: r["objective"]) if plans else None


def load(paths):
    """{(templates, shared, metrics): {accuracy %: result}}."""
    runs = {}
    for path in paths:
        m = NAME.search(os.path.basename(path))
        if not m or m.group(4) not in LEVELS:
            continue
        key = (m.group(1), int(m.group(2)), int(m.group(3) or 1))
        with open(path) as f:
            runs.setdefault(key, {})[LEVELS[m.group(4)]] = json.load(f)
    return runs


def plot(runs, keys, out, at_autosketch_latency):
    fig, axes = plt.subplots(
        len(WEIGHTS),
        len(keys),
        figsize=(3.2 * len(keys), 2.8 * len(WEIGHTS)),
        squeeze=False,
    )
    for j, key in enumerate(keys):
        for i, weights in enumerate(WEIGHTS):
            ax = axes[i, j]
            for method in METHODS:
                xs, ys = [], []
                for acc, data in sorted(runs[key].items()):
                    limit = (
                        data["frontier"]["autosketch_latency_ms"]
                        if at_autosketch_latency
                        else None
                    )
                    plan = cheapest(data, method, weights, limit)
                    if plan:
                        xs.append(acc)
                        ys.append(plan["objective"])
                ax.plot(
                    xs,
                    ys,
                    STYLES.get(method, "-"),
                    color=COLORS[method],
                    marker=MARKERS[method],
                    ms=4,
                    lw=1.2,
                    label=LABELS[method],
                )
                for x, y in zip(xs, ys):
                    ax.annotate(
                        f"{y:.3g}",
                        (x, y),
                        xytext=(3, 2),
                        textcoords="offset points",
                        fontsize=6,
                        color=COLORS[method],
                    )
            ax.set_yscale("log")
            ax.set_xticks([90, 95, 99])
            ax.set_xticklabels(["90%", "95%", "99%"])
            ax.grid(alpha=0.3, lw=0.3, which="both")
            if i == 0:
                ax.set_title(
                    f"{workload_title(*key)} ({next(iter(runs[key].values()))['rqes']} RQEs)",
                    fontsize=8,
                )
            if i == len(WEIGHTS) - 1:
                ax.set_xlabel("accuracy target")
            if j == 0:
                ax.set_ylabel(f"cost by use ({UNITS[weights]})")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        ncol=len(METHODS),
        fontsize=8,
        frameon=False,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(out, dpi=150)
    plt.close(fig)


def summary(runs, out):
    lines = [
        "# Accuracy target sweep",
        "",
        "Cheapest plan of each method (no latency bound). Cost by use: CPU only in "
        "vCPU, Fargate in $/hour. `dropped`: RQEs ASAP could not serve at the target.",
        "",
    ]
    for key in sorted(runs):
        lines += [
            f"## {workload_title(*key)}",
            "",
            "| Target | Weights | RQEs | dropped | ASAP (latency, deployments, rolled up) "
            "| ASAP no roll-ups | PerQuery | AutoSketch (latency) |",
            "|---|---|---|---|---|---|---|---|",
        ]

        def cell(r):
            return f"{r['objective']:.4g}" if r else "—"

        for acc, data in sorted(runs[key].items()):
            for weights in WEIGHTS:
                p = {m: cheapest(data, m, weights) for m in METHODS}
                a, au = p["asap"], p["autosketch"]
                lines.append(
                    f"| {acc}% | {weights} | {data['rqes']} | "
                    f"{len(data['dropped_unservable']['asap'])} | "
                    f"{cell(a)} ({a['latency_ms'] / 1e3:.2f} s, {a['active_deployments']}, "
                    f"{a.get('rolled_up_rqes', 0)}) | {cell(p['asap-norollup'])} | "
                    f"{cell(p['perquery'])} | {cell(au)} ({au['latency_ms']:.0f} ms) |"
                )
        lines.append("")
    with open(out, "w") as f:
        f.write("\n".join(lines))


def main():
    out, paths = sys.argv[1], sys.argv[2:]
    runs = load(paths)
    # One column per template set at its base point; the scaled points are in
    # the summary.
    keys = [
        k for k in [("classic", 1, 1), ("multigroup", 1, 1), ("all", 1, 1)] if k in runs
    ]
    plot(runs, keys, os.path.join(out, "fig_cost_vs_target.png"), False)
    plot(
        runs,
        keys,
        os.path.join(out, "fig_cost_vs_target_at_autosketch_latency.png"),
        True,
    )
    summary(runs, os.path.join(out, "summary_targets.md"))


if __name__ == "__main__":
    main()

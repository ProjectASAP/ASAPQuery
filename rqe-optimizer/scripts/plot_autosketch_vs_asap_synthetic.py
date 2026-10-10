#!/usr/bin/env python3
"""Plot the synthetic AutoSketch vs. ASAP evaluation (ProjectASAP/ASAPQuery#777,
section 7) from the JSON files of `autosketch_vs_asap synthetic`.

Every plan is priced by use (sketch-bench docs/rqe_optimizer_cost_model.md,
"Cost by use and batch latency"): w_cpu * AUC(CPU) + w_mem * AUC(memory), with
CPU elastic, and its query latency (the longest chain) is reported. ASAP and
PerQuery-CostAware are solved without a bound and for a sweep of latency
bounds (their cost-latency frontiers); AutoSketch-Adapted is one plan.

Writes, next to the inputs:
  fig_frontier.png        cost vs. query latency, one column per workload, one
                          row per weight setting; ASAP and PerQuery frontiers
                          as lines, AutoSketch as a point; every point labeled
                          with its cost
  fig_planning_time.png   planning time vs. RQEs over the metrics dimension;
                          AutoSketch's is its search plus its measured
                          benchmark (60 s per probe only as a labeled reference)
  summary_synthetic.md    every workload and weight setting: each method's
                          unbounded plan, its cost at AutoSketch's latency, and
                          the frontier points

Usage: scripts/plot_autosketch_vs_asap_synthetic.py DIR
"""

import json
import pathlib
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

METHODS = ["asap", "perquery", "autosketch"]
LABELS = {"asap": "ASAP", "perquery": "PerQuery-CostAware", "autosketch": "AutoSketch-Adapted"}
# Categorical slots 1-3 of the dataviz reference palette, with marker shapes as
# a second encoding.
COLORS = {"asap": "#2a78d6", "perquery": "#eb6834", "autosketch": "#1baf7a"}
MARKERS = {"asap": "o", "perquery": "s", "autosketch": "D"}
UNITS = {"cpu": "vCPU", "fargate": "$/hour"}
WEIGHT_TITLES = {"cpu": "CPU only", "fargate": "Fargate prices"}


def point(data):
    """(shared, metrics) of a result's workload name."""
    m = re.search(r"shared=(\d+)(?:/metrics=(\d+))?", data["workload"])
    return int(m.group(1)), int(m.group(2) or 1)


def title(key):
    shared, metrics = key
    if shared > 1:
        return f"mixed, r = {shared}"
    if metrics > 1:
        return f"mixed, m = {metrics}"
    return "mixed"


def load(directory):
    runs = {}
    for path in sorted(pathlib.Path(directory).glob("synthetic-*.json")):
        data = json.loads(path.read_text())
        runs[point(data)] = data
    return dict(sorted(runs.items()))


def records(data, method, weights):
    return [r for r in data["results"]
            if r["method"] == method and r["weights"] == weights and "objective" in r]


def unbounded(data, method, weights):
    return next(r for r in records(data, method, weights) if r["bound_ms"] is None)


def at_autosketch_latency(data, method, weights):
    """The method's plan at the bound closest to AutoSketch's latency."""
    target = data["frontier"]["autosketch_latency_ms"]
    bounded = [r for r in records(data, method, weights) if r["bound_ms"] is not None]
    return min(bounded, key=lambda r: abs(r["bound_ms"] - target)) if bounded else None


def fmt(x, digits=3):
    return "—" if x is None else f"{x:.{digits}g}"


def frontier(data, method, weights):
    """Distinct (latency, cost) points of the method, by latency."""
    return sorted({(r["latency_ms"], r["objective"]) for r in records(data, method, weights)})


def fig_frontier(runs, out):
    weights = [w["name"] for w in next(iter(runs.values()))["weights"]]
    fig, axes = plt.subplots(len(weights), len(runs),
                             figsize=(3.6 * len(runs), 3.2 * len(weights)), squeeze=False)
    for col, (key, data) in enumerate(runs.items()):
        for row, w in enumerate(weights):
            ax = axes[row][col]
            for m in METHODS:
                pts = frontier(data, m, w)
                style = dict(color=COLORS[m], marker=MARKERS[m], markersize=4, linewidth=1.5,
                             label=LABELS[m])
                if m == "autosketch":
                    ax.plot(*zip(*pts), linestyle="none", **style)
                else:
                    ax.plot(*zip(*pts), **style)
                for x, y in pts:
                    ax.annotate(fmt(y), (x, y), textcoords="offset points", xytext=(3, 3),
                                fontsize=5, color="#52514e")
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.grid(True, which="major", color="#e5e4e0", linewidth=0.5)
            ax.tick_params(labelsize=6)
            if row == 0:
                ax.set_title(f"{title(key)} ({data['rqes']} RQEs)", fontsize=8)
            if row == len(weights) - 1:
                ax.set_xlabel("query latency (ms)", fontsize=7)
            if col == 0:
                ax.set_ylabel(f"{WEIGHT_TITLES.get(w, w)}: cost by use ({UNITS.get(w, w)})",
                              fontsize=7)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(out / "fig_frontier.png", dpi=150)
    plt.close(fig)


def fig_planning_time(runs, out):
    """Over the metrics dimension (shared = 1), CPU-only weights."""
    series = {"asap": [], "perquery": [], "search": [], "measured": [], "paper": []}
    for (shared, _), data in runs.items():
        if shared != 1:
            continue
        n = data["rqes"]
        a = data["autosketch"]
        auto = unbounded(data, "autosketch", "cpu")
        series["asap"].append((n, unbounded(data, "asap", "cpu")["planning_secs"]))
        series["perquery"].append((n, unbounded(data, "perquery", "cpu")["planning_secs"]))
        series["search"].append((n, auto["planning_secs"]))
        if "benchmark_secs_measured" in a:
            series["measured"].append((n, auto["planning_secs"] + a["benchmark_secs_measured"]))
        series["paper"].append((n, auto["planning_secs"] + a["benchmark_secs_paper_rate"]))
    fig, ax = plt.subplots(figsize=(6.0, 3.8))
    lines = [
        ("asap", "ASAP (candidates + MILP)", COLORS["asap"], MARKERS["asap"], "-"),
        ("perquery", "PerQuery-CostAware", COLORS["perquery"], MARKERS["perquery"], "-"),
        ("measured", "AutoSketch-Adapted: search + measured benchmark", COLORS["autosketch"],
         MARKERS["autosketch"], "-"),
        ("search", "AutoSketch-Adapted: search only", COLORS["autosketch"],
         MARKERS["autosketch"], ":"),
        ("paper", "reference: AutoSketch search + 60 s per probe (paper rate)", "#898781",
         "x", "--"),
    ]
    for key, label, color, marker, style in lines:
        pts = sorted(series[key])
        if not pts:
            continue
        ax.plot(*zip(*pts), color=color, marker=marker, markersize=4, linestyle=style,
                linewidth=1.5, label=label)
        for x, y in pts:
            ax.annotate(fmt(y) + " s", (x, y), textcoords="offset points", xytext=(3, 3),
                        fontsize=5, color="#52514e")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.grid(True, which="major", color="#e5e4e0", linewidth=0.5)
    ax.set_xlabel("RQEs (mixed set, m = 1, 8, 16 metrics)", fontsize=8)
    ax.set_ylabel("planning time (s)", fontsize=8)
    ax.tick_params(labelsize=7)
    ax.legend(fontsize=6, loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=2)
    fig.tight_layout()
    fig.savefig(out / "fig_planning_time.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def summary(runs, out):
    lines = ["# Synthetic mixed set: AutoSketch vs. ASAP, cost by use\n",
             "Cost by use: `w_cpu · AUC(CPU) + w_mem · AUC(memory)` (CPU only, in vCPU; "
             "Fargate prices, in $/hour). Latency: a plan's query latency (its longest "
             "chain, compaction then query, CPU elastic), and the median over its RQEs. "
             "AutoSketch's planning time is its search plus its measured benchmark.\n"]
    for key, data in runs.items():
        a = data["autosketch"]
        lines.append(f"## {title(key)}\n")
        lines.append(
            f"{data['rqes']} RQEs on {data['streams']} streams; dropped: "
            f"{sum(len(v) for v in data['dropped_unservable'].values())}; sanity violations: "
            f"{len(data['sanity_violations'])}. AutoSketch: {a['probes']} probes, "
            f"{a['distinct_probes']} distinct (metric, config); measured benchmark "
            f"{fmt(a.get('benchmark_secs_measured'))} s at N = {fmt(a.get('benchmark_n'))} "
            f"(paper rate {fmt(a['benchmark_secs_paper_rate'])} s). AutoSketch latency "
            f"{fmt(data['frontier']['autosketch_latency_ms'])} ms; tightest feasible bound "
            f"{fmt(data['frontier']['asap_tightest_bound_ms'])} ms.\n")
        lines.append("| weights | method | cost (unbounded) | CPU (vCPU) | GiB | latency (ms) | "
                     "median latency (ms) | planning (s) | cost at AutoSketch's latency |")
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for w in [x["name"] for x in data["weights"]]:
            for m in METHODS:
                r = unbounded(data, m, w)
                planning = r.get("planning_secs")
                if m == "autosketch" and "benchmark_secs_measured" in a:
                    planning = planning + a["benchmark_secs_measured"]
                at = at_autosketch_latency(data, m, w)
                at_cost = r["objective"] if m == "autosketch" else (at and at["objective"])
                lines.append(
                    f"| {w} | {LABELS[m]} | {fmt(r['objective'])} | {fmt(r['cpu'])} | "
                    f"{fmt(r['gib'])} | {fmt(r['latency_ms'])} | {fmt(r['median_latency_ms'])} | "
                    f"{fmt(planning)} | {fmt(at_cost)} |")
        lines.append("")
        lines.append("Frontier points (latency ms, cost), by weights:\n")
        for w in [x["name"] for x in data["weights"]]:
            for m in ("asap", "perquery"):
                pts = ", ".join(f"({fmt(x)}, {fmt(y)})" for x, y in frontier(data, m, w))
                lines.append(f"- {w}, {LABELS[m]}: {pts}")
        lines.append("")
    (out / "summary_synthetic.md").write_text("\n".join(lines))


def main():
    out = pathlib.Path(sys.argv[1])
    runs = load(out)
    fig_frontier(runs, out)
    fig_planning_time(runs, out)
    summary(runs, out)


if __name__ == "__main__":
    main()

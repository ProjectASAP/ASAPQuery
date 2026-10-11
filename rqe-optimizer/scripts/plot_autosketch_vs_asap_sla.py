#!/usr/bin/env python3
"""Plot version 2 (a batch latency SLA) of the AutoSketch vs. ASAP evaluation
(ProjectASAP/ASAPQuery#777; sketch-bench docs/rqe_optimizer_cost_model.md,
"Cost by use and batch latency") from the `sla_results` of
`autosketch_vs_asap synthetic` runs.

Writes, into OUT:
  fig_cost_vs_sla.png  one panel per workload (columns) and weight setting
                       (rows): cost by use (absolute, labeled) at each SLA,
                       ASAP and PerQuery as lines over the SLAs they can meet,
                       AutoSketch as a horizontal reference at its one plan's
                       cost, a filled marker where its latency meets the SLA
                       and a cross where it misses
  summary_sla.md       per workload, weights and SLA: each method's cost,
                       latency, CPU and GiB; AutoSketch's planning time as
                       search + measured benchmark

Usage: scripts/plot_autosketch_vs_asap_sla.py OUT RESULT.json...
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
COLORS = {"asap": "#1f77b4", "perquery": "#ff7f0e", "autosketch": "#2ca02c"}
UNITS = {"cpu": "vCPU", "fargate": "$/hour"}


def title(data):
    """`mixed, r = 8` from `synthetic/templates=classic/shared=8/tp95`."""
    m = re.search(r"templates=(\w+)/shared=(\d+)(?:/metrics=(\d+))?", data["workload"])
    if not m:
        return data["workload"]
    name = {"classic": "mixed", "all": "mixed + multi-grouping"}.get(m.group(1), m.group(1))
    if m.group(3):
        return f"{name}, m = {m.group(3)}"
    if m.group(2) != "1":
        return f"{name}, r = {m.group(2)}"
    return name


def rows(data, method, weights):
    return sorted((r for r in data["sla_results"]
                   if r["method"] == method and r["weights"] == weights),
                  key=lambda r: r["sla_ms"])


def fmt(x, digits=3):
    return "—" if x is None else f"{x:.{digits}g}"


def autosketch_planning_secs(data):
    """Search + measured benchmark (scripts/autosketch_benchmark_time.py)."""
    a = data["autosketch"]
    measured = a.get("benchmark_secs_measured")
    return None if measured is None else a["search_secs"] + measured


def figure(runs, out):
    weights = [w["name"] for w in runs[0]["weights"]]
    fig, axes = plt.subplots(len(weights), len(runs), figsize=(3.6 * len(runs), 3.0 * len(weights)),
                             squeeze=False)
    for col, data in enumerate(runs):
        for row, w in enumerate(weights):
            ax = axes[row][col]
            for m in ("asap", "perquery"):
                pts = [(r["sla_ms"], r["objective"]) for r in rows(data, m, w)
                       if not r.get("infeasible") and "objective" in r]
                if not pts:
                    continue
                ax.plot(*zip(*pts), marker="o", markersize=4, linewidth=2, color=COLORS[m],
                        label=LABELS[m])
                for x, y in pts:
                    ax.annotate(fmt(y), (x, y), textcoords="offset points", xytext=(3, 3),
                                fontsize=6, color="#333333")
            auto = rows(data, "autosketch", w)
            if auto:
                cost = auto[0]["objective"]
                ax.axhline(cost, color=COLORS["autosketch"], linewidth=1.5, linestyle="--",
                           label=LABELS["autosketch"])
                for r in auto:
                    ax.plot(r["sla_ms"], cost, marker="o" if r["meets_sla"] else "x",
                            markersize=8 if r["meets_sla"] else 7,
                            color=COLORS["autosketch"], linestyle="none")
                ax.annotate(f"{fmt(cost)} (latency {fmt(auto[0]['latency_ms'])} ms)",
                            (auto[0]["sla_ms"], cost), textcoords="offset points",
                            xytext=(0, -11), fontsize=6, color="#333333")
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.tick_params(labelsize=7)
            ax.grid(True, which="major", color="#e5e5e5", linewidth=0.6)
            if row == 0:
                ax.set_title(f"{title(data)} ({data['rqes']} RQEs)", fontsize=9)
            if row == len(weights) - 1:
                ax.set_xlabel("batch latency SLA (ms)", fontsize=8)
            if col == 0:
                ax.set_ylabel(f"cost by use ({UNITS.get(w, w)})", fontsize=8)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, fontsize=8,
               title="AutoSketch: ● meets the SLA, × misses it", title_fontsize=7)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(out / "fig_cost_vs_sla.png", dpi=150)
    plt.close(fig)


def summary(runs, out):
    lines = ["# Version 2: cost by use under a batch latency SLA\n",
             "Each method's cost by use (`w_cpu · AUC(CPU) + w_mem · AUC(memory)`) at each "
             "SLA; ASAP and PerQuery plan for the SLA (the cheapest plan whose batch latency, "
             "the longest chain, is at most it), AutoSketch's one plan ignores it.\n"]
    for data in runs:
        planning = autosketch_planning_secs(data)
        lines.append(f"## {title(data)}: {data['rqes']} RQEs\n")
        lines.append(f"Sanity violations: {len(data['sanity_violations'])}. Tightest feasible "
                     f"SLA: ASAP {fmt(data['frontier']['asap_tightest_bound_ms'])} ms, PerQuery "
                     f"{fmt(data['frontier']['perquery_tightest_bound_ms'])} ms. AutoSketch "
                     f"latency {fmt(data['frontier']['autosketch_latency_ms'])} ms; planning "
                     f"time (search + measured benchmark) {fmt(planning)} s.\n")
        lines.append("| weights | SLA (ms) | method | cost | latency (ms) | CPU (vCPU) | GiB | "
                     "meets SLA |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for w in [x["name"] for x in data["weights"]]:
            for sla in data["sla_grid_ms"]:
                for m in METHODS:
                    r = next((r for r in rows(data, m, w) if r["sla_ms"] == sla), None)
                    if r is None:
                        continue
                    if r.get("infeasible"):
                        lines.append(f"| {w} | {fmt(sla)} | {LABELS[m]} | infeasible (tightest "
                                     f"{fmt(r['tightest_bound_ms'])} ms) |||||")
                        continue
                    meets = "yes" if m != "autosketch" or r["meets_sla"] else "**no**"
                    lines.append(f"| {w} | {fmt(sla)} | {LABELS[m]} | {fmt(r['objective'])} | "
                                 f"{fmt(r['latency_ms'])} | {fmt(r['cpu'])} | {fmt(r['gib'])} | "
                                 f"{meets} |")
        lines.append("")
    (out / "summary_sla.md").write_text("\n".join(lines))


def main():
    out = pathlib.Path(sys.argv[1])
    runs = [json.loads(pathlib.Path(p).read_text()) for p in sys.argv[2:]]
    runs.sort(key=lambda d: d["rqes"])
    figure(runs, out)
    summary(runs, out)


if __name__ == "__main__":
    main()

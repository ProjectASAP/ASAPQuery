#!/usr/bin/env python3
"""Collate a random_workload_batch.py batch: one row per (set, workload, arm)
with total CPU-seconds (warm-up + queries), peak memory, the median, p95 and
sum of every answered query's latency, and whether the run is valid:

- no query failed or timed out;
- its answers agree with the reference arm (Prometheus, which keeps up, or
  else ClickHouse+MV): exact arms to EXACT_REL_ERR, ASAPQuery's sketches to
  their bounds. In a pooled set (`*-pooled`) the quantiles are pooled, which
  PromQL can't say: Prometheus and VictoriaMetrics run an avg-of-series
  stand-in, unchecked, and the other arms' quantiles are checked against raw
  ClickHouse instead.

Separately, whether ingest kept up: during the query phase the feeder never
ran more than MAX_INGEST_LAG_SECS behind (from the feeder's
<mode>/paced_feeder_output/*_stats.json). A lagging system answers on-time
queries from incomplete windows, which the agreement check catches; one that
answers late but correctly is slow, not wrong, and stays valid.

Then each baseline's ratio to ASAPQuery (baseline / asap, > 1 means ASAPQuery
is better) over workloads where both runs are valid. Only workloads whose
batch arms all finished, one of them a reference, are collated.

Usage:
  python3 collate_batch.py <experiment_outputs>/<batch> [--csv out.csv]
"""

import argparse
import csv
import glob
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_arms import Arm, agreement, percentile  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from random_workload_batch import ARMS, OUTPUTS_DIR  # noqa: E402

REFERENCE = "prom"
# Exact pooled quantiles, for a pooled set's quantile queries.
POOLED_REFERENCE = "ch"
# The reference of a batch without Prometheus: exact, and answers every window.
FALLBACK_REFERENCE = "chmv"
# The queries' freshness delay: a feed further behind leaves on-time
# queries' windows incomplete.
MAX_INGEST_LAG_SECS = 10
EXACT_REL_ERR = 1e-3
# VictoriaMetrics may count a window-boundary sample Prometheus doesn't.
VM_REL_ERR = 5e-3
EXACT_TOPK_PRECISION = 0.99
# ASAPQuery's sketches: quantiles at the 0.95 accuracy SLA; sums are exact
# but for a window-boundary sample; top-k at its precision target.
ASAP_QUANTILE_REL_ERR = 0.05
ASAP_SUM_REL_ERR = 5e-3
ASAP_TOPK_PRECISION = 0.95

FIELDS = [
    "set",
    "workload",
    "arm",
    "valid",
    "why_invalid",
    "ingest_kept_up",
    "cpu_secs",
    "warmup_cpu_secs",
    "query_cpu_secs",
    "peak_memory_gib",
    "latency_p50",
    "latency_p95",
    "latency_sum",
    "queries",
    "failed_queries",
    "max_ingest_lag_secs",
]


def max_ingest_lag(arm, mode_dir):
    """The feeder's largest start lag (s) during the query phase, or None
    without its stats."""
    stats = glob.glob(str(mode_dir / "paced_feeder_output" / "*_stats.json"))
    if not stats:
        return None
    ticks = json.loads(Path(stats[0]).read_text())["ticks"]
    query_ticks = [t for t in ticks if t["tick"] >= arm.warmup_secs] or ticks
    return max(t["start_lag_ms"] for t in query_ticks) / 1000


def disagreements(arm_name, arm, ref, checked=lambda name: True):
    """The `checked` queries whose answers fall outside `arm_name`'s bounds
    against the reference arm `ref`."""
    bad = []
    for gq, name in arm.names.items():
        if not checked(name):
            continue
        topk = name.startswith("topk")
        quantile = "quantile" in name
        values = []
        for key, want in ref.results.items():
            if key[:2] != gq or key not in arm.results or not want:
                continue
            value, _ = agreement(arm.results[key], want, topk)
            if value is not None:
                values.append(value)
        if not values:
            bad.append(f"{name}: no comparable windows")
            continue
        mean = statistics.mean(values)
        if arm_name == "asap":
            ok = (
                mean >= ASAP_TOPK_PRECISION
                if topk
                else mean <= (ASAP_QUANTILE_REL_ERR if quantile else ASAP_SUM_REL_ERR)
            )
        else:
            rel_err = VM_REL_ERR if arm_name == "vm" else EXACT_REL_ERR
            ok = mean >= EXACT_TOPK_PRECISION if topk else mean <= rel_err
        if not ok:
            bad.append(f"{name}: {'precision' if topk else 'rel err'} {mean:.3g}")
    return bad


def rows(batch_dir):
    jobs = json.loads((Path(batch_dir) / "state.json").read_text())
    by_workload = defaultdict(dict)
    for j in jobs:
        by_workload[(j["set"], j["workload"])][j["arm"]] = j
    out = []
    for (set_name, workload), jobs_by_arm in sorted(by_workload.items()):
        if any(j["status"] != "done" for j in jobs_by_arm.values()):
            continue
        reference = REFERENCE if REFERENCE in jobs_by_arm else FALLBACK_REFERENCE
        if reference not in jobs_by_arm:
            continue
        dirs = {
            a: OUTPUTS_DIR / j["names"][-1] / ARMS[a][3] for a, j in jobs_by_arm.items()
        }
        arms = {a: Arm(d) for a, d in dirs.items()}
        for arm_name, arm in arms.items():
            r = arm.resources()
            recorded = [x for xs in arm.latencies.values() for x in xs]
            # A query that failed or timed out records no latency.
            latencies = [x for x in recorded if x is not None]
            lag = max_ingest_lag(arm, dirs[arm_name])
            why = []
            if len(recorded) != len(latencies):
                why.append(f"{len(recorded) - len(latencies)} failed queries")
            pooled = set_name.endswith("-pooled")
            if arm_name != reference:
                why += disagreements(
                    arm_name,
                    arm,
                    arms[reference],
                    lambda name: not (pooled and "quantile" in name),
                )
            if pooled and arm_name not in (REFERENCE, "vm", POOLED_REFERENCE):
                if POOLED_REFERENCE not in arms:
                    why.append("pooled quantiles: no raw ClickHouse to check against")
                else:
                    why += disagreements(
                        arm_name,
                        arm,
                        arms[POOLED_REFERENCE],
                        lambda name: "quantile" in name,
                    )
            out.append(
                {
                    "set": set_name,
                    "workload": workload,
                    "arm": arm_name,
                    "valid": not why,
                    "why_invalid": "; ".join(why),
                    "ingest_kept_up": (
                        None if lag is None else lag <= MAX_INGEST_LAG_SECS
                    ),
                    "cpu_secs": round(r["cpu_secs"]),
                    "warmup_cpu_secs": round(r["warmup_cpu_secs"]),
                    "query_cpu_secs": round(r["query_cpu_secs"]),
                    "peak_memory_gib": round(r["peak_memory_bytes"] / 2**30, 2),
                    "latency_p50": round(statistics.median(latencies), 3),
                    "latency_p95": round(percentile(latencies, 95), 3),
                    "latency_sum": round(sum(latencies), 1),
                    "queries": len(recorded),
                    "failed_queries": len(recorded) - len(latencies),
                    "max_ingest_lag_secs": None if lag is None else round(lag, 1),
                }
            )
    return out


def ratios(table):
    """Per set and baseline arm: the geometric mean, over workloads where
    both it and ASAPQuery are valid, of baseline / asap."""
    by = {(r["set"], r["workload"], r["arm"]): r for r in table}
    result = defaultdict(lambda: defaultdict(list))
    for (set_name, workload, arm), r in by.items():
        asap = by.get((set_name, workload, "asap"))
        if arm == "asap" or not (asap and r["valid"] and asap["valid"]):
            continue
        for key in ("cpu_secs", "latency_p50", "latency_p95", "latency_sum"):
            result[(set_name, arm)][key].append(r[key] / asap[key])
    return {
        k: {
            **{m: statistics.geometric_mean(v) for m, v in metrics.items()},
            "n": len(metrics["cpu_secs"]),
        }
        for k, metrics in result.items()
    }


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("batch_dir")
    parser.add_argument("--csv")
    args = parser.parse_args()
    table = rows(args.batch_dir)
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, FIELDS)
            w.writeheader()
            w.writerows(table)
    print("Invalid runs:")
    for r in table:
        if not r["valid"]:
            print(f"  {r['set']} {r['workload']} {r['arm']}: {r['why_invalid']}")
    print(
        "\nIngest fell behind (feeder lag > {}s in the query phase):".format(
            MAX_INGEST_LAG_SECS
        )
    )
    for r in table:
        if r["ingest_kept_up"] is False:
            print(
                f"  {r['set']} {r['workload']} {r['arm']}: {r['max_ingest_lag_secs']}s"
            )
    print(
        "\nASAPQuery improvement = baseline / asap, geometric mean over workloads "
        "where both runs are valid"
    )
    print(
        f"{'set':14s} {'baseline':9s} {'n':>2s} {'CPU':>7s} {'p50':>7s} {'p95':>7s} {'sum':>7s}"
    )
    for (set_name, arm), m in sorted(ratios(table).items()):
        print(
            f"{set_name:14s} {arm:9s} {m['n']:2d} {m['cpu_secs']:6.1f}x "
            f"{m['latency_p50']:6.1f}x {m['latency_p95']:6.1f}x {m['latency_sum']:6.1f}x"
        )


if __name__ == "__main__":
    main()

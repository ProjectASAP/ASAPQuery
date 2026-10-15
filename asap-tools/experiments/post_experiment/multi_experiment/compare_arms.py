#!/usr/bin/env python3
"""Compare one workload's arms, each a mode of its own experiment run.

For every query: latency (p50/p95/p99) and the share answered within its
repeat interval; each arm's agreement with a reference arm (mean relative
error, or precision@k for top-k); and per arm the total CPU-seconds and peak
memory from remote_monitor's samples. Arms must have run the same queries in
the same order on the same data (same feeder seed), so (query group, query,
repetition) names the same window in every arm.

Usage:
  python3 compare_arms.py --reference prometheus \\
      --arm asapquery=experiment_outputs/w01_asap/sketchdb \\
      --arm prometheus=experiment_outputs/w01_prom/baseline \\
      --arm clickhouse_mv=experiment_outputs/w01_ch_mv/baseline_mv
"""

import argparse
import gzip
import json
import statistics
from collections import defaultdict
from pathlib import Path

import yaml


def read_jsonl(path):
    """The complete lines of a gzipped JSONL file. A client stopped mid-run
    leaves the gzip stream unterminated: keep what precedes the cut."""
    rows = []
    try:
        with gzip.open(path, "rt") as f:
            for line in f:
                if line.endswith("\n"):
                    rows.append(json.loads(line))
    except EOFError:
        pass
    return rows


def sql_labels(sql):
    """The label columns a ClickHouse statement selects before its value,
    after any leading `-- description` line."""
    lines = sql.splitlines()
    body = "\n".join(lines[1:]) if lines and lines[0].startswith("--") else sql
    cols = []
    for token in body[len("SELECT ") :].split(", "):
        if "(" in token:
            break
        cols.append(token.strip())
    return cols


def query_name(query):
    """PromQL as is; ClickHouse SQL by its `-- <PromQL>` description line."""
    first = query.splitlines()[0]
    return first[2:].strip() if first.startswith("--") else query


class Arm:
    """One experiment mode's latencies, results, intervals and resources."""

    def __init__(self, mode_dir):
        mode_dir = Path(mode_dir)
        out = mode_dir / "prometheus_client_output"
        self.latencies = defaultdict(list)  # (group, query) -> [seconds]
        for r in read_jsonl(out / "query_latencies.jsonl.gz"):
            self.latencies[(r["query_group_idx"], r["query_idx"])].append(r["latency"])
        self.results = defaultdict(dict)  # (group, query, rep) -> {labels: value}
        self.names = {}
        for r in read_jsonl(out / "query_results.jsonl.gz"):
            key = (r["query_group_idx"], r["query_idx"], r["repetition_idx"])
            self.names[key[:2]] = query_name(r["query"])
            window = self.results[key]
            if "raw_text_result" in r:
                cols = sql_labels(r["query"])
                for line in (r["raw_text_result"] or "").strip().splitlines():
                    *labels, value = line.split("\t")
                    window[tuple(sorted(zip(cols, labels)))] = float(value)
            elif r.get("result_labels") is not None:
                labels = json.loads(r["result_labels"])
                labels.pop("__name__", None)
                window[tuple(sorted(labels.items()))] = float(r["result_value"])
        client = mode_dir.parent / "controller_client_configs" / f"{mode_dir.name}.yaml"
        groups = yaml.safe_load(client.read_text())["query_groups"]
        self.interval_secs = [g["repetition_delay_ms"] / 1000 for g in groups]
        # Queries start after this; until then the systems only ingest.
        self.warmup_secs = min(g["client_options"]["starting_delay"] for g in groups)
        self.monitor = mode_dir / "remote_monitor_output" / "monitor_output.json"
        hydra = yaml.safe_load((mode_dir.parent / "hydra_config.yaml").read_text())
        self.monitor_interval_secs = float(hydra["flow"]["monitor_interval_seconds"])

    def resources(self):
        """CPU-seconds over the whole monitored run, its warm-up and the rest
        (queries and any lag), and peak memory bytes; each sample sums the
        monitored processes. The monitor starts with the query client, so
        the warm-up is its first `warmup_secs`."""
        processes = json.loads(self.monitor.read_text()).values()
        n = max(len(p["cpu_percent"]) for p in processes)
        cpu, mem = [0.0] * n, [0.0] * n
        for p in processes:
            for t, c in enumerate(p["cpu_percent"]):
                cpu[t] += c
            for t, m in enumerate(p["memory_info"]):
                mem[t] += m
        secs = self.monitor_interval_secs
        warm = int(self.warmup_secs / secs)
        return {
            "monitored_secs": n * secs,
            "cpu_secs": sum(cpu) / 100 * secs,
            "warmup_cpu_secs": sum(cpu[:warm]) / 100 * secs,
            "query_cpu_secs": sum(cpu[warm:]) / 100 * secs,
            "peak_memory_bytes": max(mem),
        }


def percentile(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def agreement(got, want, topk):
    """`got`'s window against reference `want`: the share of `want`'s series
    `got` returns (top-k), else the mean relative error over shared series
    and the number of `want`'s series `got` lacks."""
    if topk:
        return len(set(got) & set(want)) / len(want), 0
    errors = [abs(got[k] - v) / abs(v) for k, v in want.items() if k in got and v]
    return (statistics.mean(errors) if errors else None), len(set(want) - set(got))


def report(arms, reference):
    names = arms[reference].names
    print("== Latency (s): p50 / p95 / p99, share within the repeat interval")
    for gq, name in sorted(names.items()):
        interval = arms[reference].interval_secs[gq[0]]
        print(f"\n[{gq[0]}.{gq[1]}] every {interval:g}s: {name}")
        for arm_name, arm in arms.items():
            xs = arm.latencies.get(gq)
            if not xs:
                continue
            within = sum(x <= interval for x in xs) / len(xs)
            print(
                f"  {arm_name:16s} n={len(xs):5d}  {percentile(xs, 50):8.3f} "
                f"{percentile(xs, 95):8.3f} {percentile(xs, 99):8.3f}   {within:6.1%}"
            )

    print(f"\n== Agreement with {reference}")
    ref = arms[reference].results
    for gq, name in sorted(names.items()):
        topk = name.startswith("topk")
        print(f"\n[{gq[0]}.{gq[1]}] {name}")
        for arm_name, arm in arms.items():
            if arm_name == reference:
                continue
            values, missing, empty = [], 0, 0
            for key, want in ref.items():
                if key[:2] != gq or key not in arm.results:
                    continue
                if not want:
                    empty += 1
                    continue
                value, lacking = agreement(arm.results[key], want, topk)
                missing += lacking
                if value is not None:
                    values.append(value)
            if not values:
                print(f"  {arm_name:16s} no comparable windows")
                continue
            worst = min(values) if topk else max(values)
            what = "precision@k" if topk else "mean rel err"
            print(
                f"  {arm_name:16s} {what} {statistics.mean(values):.3g} "
                f"(worst {worst:.3g}) over {len(values)} windows; "
                f"missing series {missing}, empty reference windows {empty}"
            )

    print("\n== Resources: CPU-seconds (whole run = warm-up + queries), peak memory")
    for arm_name, arm in arms.items():
        r = arm.resources()
        print(
            f"  {arm_name:16s} {r['monitored_secs']:6.0f} s monitored   CPU "
            f"{r['cpu_secs']:7.0f} = {r['warmup_cpu_secs']:6.0f} + "
            f"{r['query_cpu_secs']:6.0f}   peak memory "
            f"{r['peak_memory_bytes'] / 2**30:5.2f} GiB"
        )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--arm",
        action="append",
        required=True,
        metavar="NAME=MODE_DIR",
        help="an arm: its name and its experiment's mode directory",
    )
    parser.add_argument(
        "--reference", required=True, help="the arm others are scored against"
    )
    args = parser.parse_args()
    arms = {}
    for spec in args.arm:
        name, _, mode_dir = spec.partition("=")
        arms[name] = Arm(mode_dir)
    if args.reference not in arms:
        parser.error(f"--reference {args.reference!r} is not an --arm")
    report(arms, args.reference)


if __name__ == "__main__":
    main()

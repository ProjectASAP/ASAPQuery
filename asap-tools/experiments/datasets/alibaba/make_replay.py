#!/usr/bin/env python3
"""A replay table for paced-feeder from Alibaba v2022 MSMetrics: pod CPU
utilization in the random workloads' label shape (4 region x 5 service x 10
job x 50 label_0), so their plans apply unchanged.

Each job is one microservice and each label_0 one of its pods; region and
service only group microservices. Label values follow the feeder's table
convention: a child's value is its parent's, a `.`, and its index (`job="3.1.7"`).
Microservices are drawn by `--seed` among those with at least 50 pods that
report at every step of the input; pods are their first 50 by name.

Writes `<out>.csv` (`offset_ms,region,service,job,label_0,value`, sorted by
offset, offsets from the first step) and `<out>_mapping.csv` (labels to
msname/msinstanceid). With `--table`, also `<out>_table.json`: that
workload table with its `data` metric's values replaced by the replay, which
paced-feeder loops (one trace step per sample).

Usage: python3 make_replay.py MSMetricsUpdate_0.csv MSMetricsUpdate_1.csv \
    --out replay [--table eval_table.json]
"""

import argparse
import csv
import json
import random
from collections import defaultdict

REGIONS, SERVICES, JOBS, PODS = 4, 5, 10, 50


def rows(paths):
    for path in paths:
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                yield r


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", nargs="+")
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", type=int, default=862)
    parser.add_argument("--table", help="workload table whose `data` gets the replay")
    args = parser.parse_args()

    steps, seen = set(), defaultdict(set)  # pod -> steps
    pods = defaultdict(set)  # msname -> pods
    for r in rows(args.csv):
        steps.add(r["timestamp"])
        seen[r["msinstanceid"]].add(r["timestamp"])
        pods[r["msname"]].add(r["msinstanceid"])
    full = {p for p, s in seen.items() if len(s) == len(steps)}
    eligible = sorted(m for m, ps in pods.items() if len(ps & full) >= PODS)
    jobs = REGIONS * SERVICES * JOBS
    print(f"{len(steps)} steps; {len(eligible)} microservices eligible")
    chosen = random.Random(args.seed).sample(eligible, jobs)

    labels = {}  # pod -> (region, service, job, label_0)
    with open(f"{args.out}_mapping.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["region", "service", "job", "label_0", "msname", "msinstanceid"])
        for i, msname in enumerate(chosen):
            region = str(i // (SERVICES * JOBS))
            service = f"{region}.{i // JOBS % SERVICES}"
            job = f"{service}.{i % JOBS}"
            ordered = sorted(
                pods[msname] & full, key=lambda p: int(p.rsplit("_", 1)[1])
            )
            for k, pod in enumerate(ordered[:PODS]):
                labels[pod] = (region, service, job, f"{job}.{k}")
                w.writerow([*labels[pod], msname, pod])

    start = min(int(s) for s in steps)
    out = sorted(
        (int(r["timestamp"]) - start, *labels[r["msinstanceid"]], r["cpu_utilization"])
        for r in rows(args.csv)
        if r["msinstanceid"] in labels
    )
    with open(f"{args.out}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["offset_ms", "region", "service", "job", "label_0", "value"])
        w.writerows(out)
    print(f"{len(out)} rows, {len(labels)} series")
    if args.table:
        write_table(args.table, f"{args.out}_table.json", out, len(steps))


def write_table(table_path, out_path, rows, steps):
    """`table_path` with `data`'s values replaced by `rows`, series-major in
    paced-feeder's series order (mixed radix, label_0 fastest)."""
    by_series = {}
    for offset, *_, label_0, value in rows:  # sorted by offset
        by_series.setdefault(label_0, []).append(round(float(value), 6))
    values = []
    for r in range(REGIONS):
        for s in range(SERVICES):
            for j in range(JOBS):
                for k in range(PODS):
                    values += by_series[f"{r}.{s}.{j}.{k}"]
    assert len(values) == len(by_series) * steps
    table = json.load(open(table_path))
    data = table["workloads"][0]["e2e"]["metrics"]["data"]
    data["values"] = {"replay": {"steps": steps, "values": values}}
    with open(out_path, "w") as f:
        json.dump(table, f)
    print(out_path)


if __name__ == "__main__":
    main()

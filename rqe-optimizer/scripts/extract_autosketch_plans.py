#!/usr/bin/env python3
"""Chosen plans from an autosketch_vs_asap run kept with its plans (`--runs 1`,
without `--no-chosen`), in the `plans/<table>.plans.json.gz` format of
rqe-optimizer/results/autosketch-vs-asap-hydra/README.md: for each method and
weight setting the cheapest plan (`kind: "cheapest"`), then the plan at each
feasible SLA (`kind: "sla"`, `sla_ms`).

Usage: extract_autosketch_plans.py RESULT.json OUT.plans.json.gz
"""
import gzip
import json
import sys

FIELDS = ["objective", "cpu", "gib", "latency_ms", "active_deployments", "rolled_up_rqes",
          "chosen"]


def plans(result):
    cheapest = {}
    for r in result["results"]:
        key = (r["method"], r["weights"])
        if key not in cheapest or r["objective"] < cheapest[key]["objective"]:
            cheapest[key] = r
    out = [{"kind": "cheapest", "method": method, "weights": weights,
            **{f: r.get(f) for f in FIELDS}}
           for (method, weights), r in sorted(cheapest.items(), key=lambda kv: kv[0][::-1])]
    out += [{"kind": "sla", "sla_ms": r.get("sla_ms"), "method": r["method"],
             "weights": r["weights"], **{f: r.get(f) for f in FIELDS}}
            for r in result["sla_results"] if r.get("objective") is not None]
    return {"workload": result["workload"], "rqes": result["rqes"],
            "cost_model": result["cost_model"], "plans": out}


def main(argv):
    src, dst = argv
    with open(src) as f:
        result = json.load(f)
    with gzip.open(dst, "wt") as f:
        json.dump(plans(result), f)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Run a batch of random-workload arms on a cluster, many at once.

Each job is one arm (asap, prom, vm, ch, chmv) of one workload, an
experiment_run_e2e.py or experiment_run_clickhouse.py run on its own slot:
two nodes, node_offset = 2 * slot, num_nodes = 1. Jobs run longest first. A
failed job retries once under a new experiment name. Before each job the
slot's nodes are cleaned of anything an earlier, killed job left running.
When every arm of a workload is done, compare_arms.py writes its summary.

State is kept in <batch dir>/state.json after every change, so a restarted
driver resumes: finished jobs are skipped and jobs that were running rerun.

Configs come from random_workload_artifacts.py --pilot-dir, under
config/experiment_type/random_workload/<set>/<workload>/.

Usage (from asap-tools/experiments, in tmux):
  python3 random_workload_batch.py --batch overnight-1015 \\
      --username milindsr --hostname-suffix <suffix> --slots 21 \\
      --set seed-862:01-10 --set seed-862-long:01-10 --set seed-862-x5:01 \\
      [--dry-run] [--stop-launching-after-hours 7]
"""

import argparse
import gzip
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

import yaml

EXPERIMENTS_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = EXPERIMENTS_DIR.parent.parent / "experiment_outputs"
CONFIG_DIR = EXPERIMENTS_DIR / "config" / "experiment_type" / "random_workload"
COMPARE_ARMS = (
    EXPERIMENTS_DIR / "post_experiment" / "multi_experiment" / "compare_arms.py"
)
POLL_SECS = 30
MAX_ATTEMPTS = 2
REFERENCE_ARM = "chmv"  # exact, and the fastest exact arm: answers every window

# arm -> (runner, config, extra overrides, mode directory)
ARMS = {
    "asap": (
        "experiment_run_e2e.py",
        "e2e",
        ["experiment_params.experiment=[{mode:sketchdb,server:sketchdb}]"],
        "sketchdb",
    ),
    "prom": (
        "experiment_run_e2e.py",
        "e2e",
        ["experiment_params.experiment=[{mode:baseline,server:prometheus}]"],
        "baseline",
    ),
    "vm": ("experiment_run_e2e.py", "victoriametrics", [], "baseline"),
    "ch": ("experiment_run_clickhouse.py", "clickhouse", [], "baseline"),
    "chmv": ("experiment_run_clickhouse.py", "clickhouse_mv", [], "baseline_mv"),
}
# Arms that fall behind 1 s queries in the pilot, and so take longest.
SLOW_ARMS = {"prom", "ch"}
# Anything a job leaves running on its nodes: feeders, bare-metal
# Prometheus, monitors and query clients, and every system's container.
CLEANUP = (
    "pkill -f '[t]arget/release/paced_feeder'; pkill -x prometheus; "
    "pkill -f '[r]emote_monitor.py'; pkill -f '[q]uery_engine_rust'; "
    "docker rm -f clickhouse-server victoriametrics-single victoriametrics-agent "
    "sketchdb-queryengine-rust sketchdb-prometheusclient prometheus-container "
    ">/dev/null 2>&1; true"
)


def parse_set(spec):
    """`seed-862-long:01-10` or `seed-862-x5:01,03` -> (set, [workload ids])."""
    name, _, ranges = spec.partition(":")
    ids = []
    for part in ranges.split(","):
        lo, _, hi = part.partition("-")
        ids += [f"workload-{n:02d}" for n in range(int(lo), int(hi or lo) + 1)]
    return name, ids


def warmup_secs(set_name, workload):
    config = CONFIG_DIR / set_name / workload / "clickhouse.yaml"
    groups = yaml.safe_load(config.read_text())["query_groups"]
    return min(g["client_options"]["starting_delay"] for g in groups)


def make_jobs(sets):
    jobs = []
    for set_name, workloads in sets:
        for workload in workloads:
            for arm in ARMS:
                config = CONFIG_DIR / set_name / workload / f"{ARMS[arm][1]}.yaml"
                if not config.exists():
                    raise SystemExit(f"missing {config}; render it first")
                variant = set_name.count("-") > 1  # seed-862-long, not seed-862
                # Only for ordering: slow arms of stretched sets take longest.
                estimate = warmup_secs(set_name, workload) + (
                    (6000 if variant else 2400) if arm in SLOW_ARMS else 900
                )
                jobs.append(
                    {
                        "key": f"{set_name}/{workload}/{arm}",
                        "set": set_name,
                        "workload": workload,
                        "arm": arm,
                        "estimate_secs": estimate,
                        "attempts": 0,
                        "status": "queued",
                        "names": [],
                    }
                )
    jobs.sort(key=lambda j: -j["estimate_secs"])
    return jobs


def experiment_name(job):
    tag = job["set"].replace("seed-", "s").replace("-", "")
    name = f"{tag}_{job['workload'].replace('workload-', 'w')}_{job['arm']}"
    return name if job["attempts"] == 1 else f"{name}_r{job['attempts']}"


def command(job, name, slot, args):
    runner, config, extra, _ = ARMS[job["arm"]]
    cmd = [
        sys.executable,
        runner,
        f"experiment_type=random_workload/{job['set']}/{job['workload']}/{config}",
        f"experiment.name={name}",
        "providers.cloudlab.num_nodes=1",
        f"providers.cloudlab.node_offset={2 * slot}",
        f"providers.cloudlab.username={args.username}",
        f"providers.cloudlab.hostname_suffix={args.hostname_suffix}",
        "controller.punting=false",
        *extra,
    ]
    if runner == "experiment_run_e2e.py":
        # The Prometheus TSDB copy is large for long and x5, and unused.
        cmd.append("flow.skip_copy_prometheus_data=true")
    return cmd


def succeeded(job, name):
    """The run exited 0 and recorded latencies and results."""
    out = OUTPUTS_DIR / name / ARMS[job["arm"]][3] / "prometheus_client_output"
    for f in ("query_latencies.jsonl.gz", "query_results.jsonl.gz"):
        path = out / f
        if not path.exists() or path.stat().st_size == 0:
            return False
    try:
        with gzip.open(out / "query_latencies.jsonl.gz", "rt") as f:
            return bool(f.readline())
    except (OSError, EOFError):
        return False


def clean_slot(slot, args):
    for node in (2 * slot, 2 * slot + 1):
        host = f"{args.username}@node{node}.{args.hostname_suffix}"
        subprocess.run(
            [
                "ssh",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "ConnectTimeout=20",
                host,
                CLEANUP,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=120,
        )


def summarize(batch_dir, jobs, set_name, workload):
    done = {
        j["arm"]: j["names"][-1]
        for j in jobs
        if j["set"] == set_name and j["workload"] == workload and j["status"] == "done"
    }
    reference = REFERENCE_ARM if REFERENCE_ARM in done else "prom"
    if reference not in done:
        return
    cmd = [sys.executable, str(COMPARE_ARMS), "--reference", reference]
    for arm, name in done.items():
        cmd += ["--arm", f"{arm}={OUTPUTS_DIR / name / ARMS[arm][3]}"]
    out = batch_dir / "summaries" / f"{set_name}_{workload}.txt"
    out.parent.mkdir(exist_ok=True)
    with open(out, "w") as f:
        f.write(f"arms: {done}\n\n")
        f.flush()
        subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=EXPERIMENTS_DIR)


def save(batch_dir, jobs):
    tmp = batch_dir / "state.json.tmp"
    tmp.write_text(json.dumps(jobs, indent=1))
    os.replace(tmp, batch_dir / "state.json")


def log(batch_dir, message):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}"
    print(line, flush=True)
    with open(batch_dir / "driver.log", "a") as f:
        f.write(line + "\n")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--batch", required=True, help="name; state under experiment_outputs/"
    )
    parser.add_argument("--username", required=True)
    parser.add_argument("--hostname-suffix", required=True)
    parser.add_argument(
        "--slots", type=int, required=True, help="two nodes each, from node0"
    )
    parser.add_argument(
        "--set", action="append", required=True, metavar="SET:WORKLOADS"
    )
    parser.add_argument("--stop-launching-after-hours", type=float)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    batch_dir = OUTPUTS_DIR / args.batch
    batch_dir.mkdir(parents=True, exist_ok=True)
    state = batch_dir / "state.json"
    if state.exists():
        jobs = json.loads(state.read_text())
        for j in jobs:
            if j["status"] == "running":  # the driver died under it
                j["status"] = "queued"
    else:
        jobs = make_jobs([parse_set(s) for s in args.set])
    save(batch_dir, jobs)

    if args.dry_run:
        for j in jobs:
            j["attempts"] += 1
            print(
                f"{j['estimate_secs'] / 3600:4.1f} h  "
                + shlex.join(command(j, experiment_name(j), 0, args))
            )
            j["attempts"] -= 1
        print(
            f"{len(jobs)} jobs, ~{sum(j['estimate_secs'] for j in jobs) / 3600:.0f} "
            f"slot-hours on {args.slots} slots"
        )
        return

    started = time.time()
    running = {}  # slot -> (job, process)
    while True:
        for slot, (job, proc) in list(running.items()):
            if proc.poll() is None:
                continue
            del running[slot]
            name = job["names"][-1]
            if proc.returncode == 0 and succeeded(job, name):
                job["status"] = "done"
            elif job["attempts"] < MAX_ATTEMPTS:
                job["status"] = "queued"
            else:
                job["status"] = "failed"
            log(
                batch_dir,
                f"slot {slot}: {name} exit {proc.returncode} -> {job['status']}",
            )
            save(batch_dir, jobs)
            peers = [
                j
                for j in jobs
                if (j["set"], j["workload"]) == (job["set"], job["workload"])
            ]
            if all(j["status"] in ("done", "failed") for j in peers):
                summarize(batch_dir, jobs, job["set"], job["workload"])
                log(batch_dir, f"summary: {job['set']} {job['workload']}")

        launching = args.stop_launching_after_hours is None or (
            time.time() - started < args.stop_launching_after_hours * 3600
        )
        for slot in range(args.slots):
            if slot in running or not launching:
                continue
            # Retries first, then the longest queued job.
            queued = [j for j in jobs if j["status"] == "queued"]
            if not queued:
                break
            job = max(queued, key=lambda j: (j["attempts"], j["estimate_secs"]))
            clean_slot(slot, args)
            job["attempts"] += 1
            name = experiment_name(job)
            job["names"].append(name)
            job["status"] = "running"
            save(batch_dir, jobs)
            logs = batch_dir / "logs"
            logs.mkdir(exist_ok=True)
            out = open(logs / f"{name}.log", "w")
            proc = subprocess.Popen(
                command(job, name, slot, args),
                cwd=EXPERIMENTS_DIR,
                stdout=out,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            running[slot] = (job, proc)
            log(batch_dir, f"slot {slot} (node{2 * slot}-{2 * slot + 1}): start {name}")

        if not running and (
            not launching or not any(j["status"] == "queued" for j in jobs)
        ):
            break
        time.sleep(POLL_SECS)

    counts = {
        s: sum(j["status"] == s for j in jobs) for s in ("done", "failed", "queued")
    }
    log(batch_dir, f"batch finished: {counts}")


if __name__ == "__main__":
    main()

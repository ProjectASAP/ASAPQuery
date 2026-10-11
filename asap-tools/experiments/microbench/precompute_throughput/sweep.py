"""Precompute-engine throughput microbenchmark sweep.

Each point starts a fresh `precompute_engine` with N workers pinned to the
first N of --engine-cpus, drives it with `bench_ingest_sender` pinned to
--sender-cpus, and appends the sender's JSON result (plus the point's
parameters) to --out as one line. Give --engine-cpus one hyperthread per
physical core so workers never share a core.

Build first (in asap-query-engine):
    cargo build --release --bin precompute_engine --bin bench_ingest_sender

Usage:
    python sweep.py --bin-dir ../../../../target/release \
        --engine-cpus $(seq -s, 0 2 26) --sender-cpus 1-55:2 \
        --experiment main --out results/main.jsonl

Experiments:
    main     every pipeline x cardinality: saturation, then RATE_FRACTIONS of it
    fanin    KLL with grouping by all labels / l0,l1 / nothing
    window   KLL and DDSketch, tumbling vs sliding
    scaling  KLL and HydraKLL at 1M series over WORKER_COUNTS
"""

import argparse
import json
import os
import socket
import subprocess
import time

import yaml

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs")
PIPELINES = ["kll", "ddsketch", "cms_heap", "hydra_kll"]
# Engine with --pass-raw-samples: decode/route/store cost with no sketching.
RAW_PIPELINE = "raw"
RAW_BASE_CONFIG = "kll"
AGGREGATION_ID = 1  # every config in CONFIG_DIR uses this id

SERIES_COUNTS = [1_000, 10_000, 100_000, 1_000_000]
DEFAULT_SERIES = 100_000
# The engine is pinned to as many CPUs as it has workers (taskset), so
# decode/routing on the async runtime can't spill onto extra cores.
DEFAULT_WORKERS = 8
RATE_FRACTIONS = [0.1, 0.25, 0.5, 0.75, 0.9]
WORKER_COUNTS = [1, 2, 4, 8, 14]
SCALING_SERIES = 1_000_000
FANIN_GROUPINGS = {
    "1to1": ["l0", "l1", "l2", "l3", "l4"],
    "100groups": ["l0", "l1"],
    "1group": [],
}
SLIDING_WINDOW_MS = 60_000
SLIDING_SLIDE_MS = 1_000

STEP_MS = 1_000  # simulated scrape interval
# The engine handles each request mostly serially, so in-flight requests
# (one per sender) must exceed engine cores or low-cardinality runs measure
# request latency, not throughput.
SENDER_CONCURRENCY = 16
SAMPLES_PER_REQUEST = 2_000  # Prometheus' default max_samples_per_send
# Must exceed the sender's MAX_PASS_SKEW (2) * STEP_MS, or skewed senders'
# samples are dropped as late.
ALLOWED_LATENESS_MS = 5_000
# Disables the engine's wall-clock fallback that force-closes panes idle for
# window + grace of wall time. With per-series groups at low rates, one pass
# over all series can exceed that, so it would drop samples the sender still
# counts as acked. The sender always advances event time, so it isn't needed.
WALL_CLOCK_GRACE_PERIOD_MS = 0
# Store keeps this many x4 windows per key (per_key.rs max_epochs). Memory is
# ~ series x 4 x sketch size, so 1M per-series KLLs stay in tens of GB.
NUM_AGGREGATES_TO_RETAIN = 1
WARMUP_SECS = 15
MEASURE_SECS = 30
BASE_PORT = 19500
ENGINE_READY_TIMEOUT_SECS = 30
ENGINE_READY_POLL_SECS = 0.2
ENGINE_STOP_TIMEOUT_SECS = 10
SENDER_TIMEOUT_SECS = WARMUP_SECS + MEASURE_SECS + 120


def config_variant(base, out_dir, name, **overrides):
    """Write CONFIG_DIR/<base>.yaml with aggregation fields overridden."""
    with open(os.path.join(CONFIG_DIR, f"{base}.yaml")) as f:
        cfg = yaml.safe_load(f)
    agg = cfg["aggregations"][0]
    for key, value in overrides.items():
        if key == "grouping":
            agg["labels"]["grouping"] = value
        else:
            agg[key] = value
    path = os.path.join(out_dir, "configs", f"{name}.yaml")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(cfg, f)
    return path


def wait_for_port(port, proc):
    deadline = time.monotonic() + ENGINE_READY_TIMEOUT_SECS
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"engine exited with {proc.returncode} before ready")
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(ENGINE_READY_POLL_SECS)
    raise RuntimeError(
        f"engine not listening on {port} after {ENGINE_READY_TIMEOUT_SECS}s"
    )


class Runner:
    def __init__(self, args):
        self.args = args
        self.port = BASE_PORT
        os.makedirs(os.path.join(args.out_dir, "logs"), exist_ok=True)

    def run(self, label, config_path, series, workers, rate, raw=False):
        """One engine + sender run; returns the result dict (also appended to --out)."""
        a = self.args
        self.port += 1  # fresh port per run sidesteps TIME_WAIT on rebind
        engine_cmd = [
            "taskset", "-c", ",".join(a.engine_cpus[:workers]),
            os.path.join(a.bin_dir, "precompute_engine"),
            "--streaming-config", config_path,
            "--num-workers", str(workers),
            "--ingest-port", str(self.port),
            "--query-port", "0",
            "--allowed-lateness-ms", str(ALLOWED_LATENESS_MS),
            "--wall-clock-grace-period-ms", str(WALL_CLOCK_GRACE_PERIOD_MS),
            "--num-aggregates-to-retain", str(NUM_AGGREGATES_TO_RETAIN),
        ]  # fmt: skip
        if raw:
            engine_cmd += [
                "--pass-raw-samples",
                "--raw-mode-aggregation-id",
                str(AGGREGATION_ID),
            ]
        log_path = os.path.join(
            a.out_dir, "logs", f"{label}_s{series}_w{workers}_r{int(rate)}.log"
        )
        print(
            f"[{label}] series={series} workers={workers} rate={rate:.0f}", flush=True
        )
        with open(log_path, "w") as log:
            engine = subprocess.Popen(
                engine_cmd, stdout=log, stderr=subprocess.STDOUT,
                env={**os.environ, "RUST_LOG": "warn"},
            )  # fmt: skip
            try:
                wait_for_port(self.port, engine)
                sender = subprocess.run(
                    [
                        "taskset", "-c", a.sender_cpus,
                        os.path.join(a.bin_dir, "bench_ingest_sender"),
                        "--engine-pid", str(engine.pid),
                        "--port", str(self.port),
                        "--num-series", str(series),
                        "--target-rate", str(rate),
                        "--concurrency", str(SENDER_CONCURRENCY),
                        "--samples-per-request", str(SAMPLES_PER_REQUEST),
                        "--step-ms", str(STEP_MS),
                        "--warmup-secs", str(WARMUP_SECS),
                        "--measure-secs", str(MEASURE_SECS),
                    ],  # fmt: skip
                    capture_output=True, text=True, timeout=SENDER_TIMEOUT_SECS, check=True,
                )  # fmt: skip
            finally:
                engine.terminate()
                engine.wait(timeout=ENGINE_STOP_TIMEOUT_SECS)
        result = json.loads(sender.stdout.strip().splitlines()[-1])
        result.update(label=label, workers=workers, config=config_path)
        with open(a.out, "a") as f:
            f.write(json.dumps(result) + "\n")
        print(
            f"    achieved={result['achieved_rate']:.0f}/s "
            f"cpu={result['engine_cpu_cores']:.2f} cores "
            f"sender_cpu={result['sender_cpu_cores']:.2f} cores "
            f"rss={result['engine_peak_rss_mb']:.0f}MB",
            flush=True,
        )
        return result

    def saturation_then_fractions(self, label, config_path, series, workers, raw=False):
        sat = self.run(label, config_path, series, workers, 0, raw)
        for frac in RATE_FRACTIONS:
            self.run(
                label, config_path, series, workers, frac * sat["achieved_rate"], raw
            )


def main_experiment(r):
    for series in SERIES_COUNTS:
        for p in PIPELINES:
            r.saturation_then_fractions(
                p, os.path.join(CONFIG_DIR, f"{p}.yaml"), series, DEFAULT_WORKERS
            )
        raw_cfg = os.path.join(CONFIG_DIR, f"{RAW_BASE_CONFIG}.yaml")
        r.saturation_then_fractions(
            RAW_PIPELINE, raw_cfg, series, DEFAULT_WORKERS, raw=True
        )


def fanin_experiment(r):
    for name, grouping in FANIN_GROUPINGS.items():
        cfg = config_variant(
            "kll", r.args.out_dir, f"kll_fanin_{name}", grouping=grouping
        )
        r.run(f"kll_fanin_{name}", cfg, DEFAULT_SERIES, DEFAULT_WORKERS, 0)


def window_experiment(r):
    for p in ["kll", "ddsketch"]:
        r.run(
            f"{p}_tumbling",
            os.path.join(CONFIG_DIR, f"{p}.yaml"),
            DEFAULT_SERIES,
            DEFAULT_WORKERS,
            0,
        )
        cfg = config_variant(
            p, r.args.out_dir, f"{p}_sliding",
            windowType="sliding", windowSizeMs=SLIDING_WINDOW_MS, slideIntervalMs=SLIDING_SLIDE_MS,
        )  # fmt: skip
        r.run(f"{p}_sliding", cfg, DEFAULT_SERIES, DEFAULT_WORKERS, 0)


def scaling_experiment(r):
    for p in ["kll", "hydra_kll"]:
        for workers in WORKER_COUNTS:
            r.run(p, os.path.join(CONFIG_DIR, f"{p}.yaml"), SCALING_SERIES, workers, 0)


EXPERIMENTS = {
    "main": main_experiment,
    "fanin": fanin_experiment,
    "window": window_experiment,
    "scaling": scaling_experiment,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--bin-dir",
        required=True,
        help="dir with precompute_engine and bench_ingest_sender",
    )
    parser.add_argument(
        "--engine-cpus", required=True, type=lambda s: s.split(","),
        help="comma list, one CPU per physical core; a run with N workers uses the first N",
    )  # fmt: skip
    parser.add_argument(
        "--sender-cpus", required=True, help="taskset list, disjoint from engine cpus"
    )
    parser.add_argument("--experiment", required=True, choices=EXPERIMENTS)
    parser.add_argument("--out", required=True, help="results .jsonl (appended)")
    args = parser.parse_args()
    args.out_dir = os.path.dirname(os.path.abspath(args.out))
    needed = max(
        [DEFAULT_WORKERS] + (WORKER_COUNTS if args.experiment == "scaling" else [])
    )
    if len(args.engine_cpus) < needed:
        parser.error(
            f"--engine-cpus has {len(args.engine_cpus)} CPUs; {args.experiment} needs {needed}"
        )
    EXPERIMENTS[args.experiment](Runner(args))

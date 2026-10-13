#!/usr/bin/env python3
"""Generate fake_exporter-equivalent data for the ClickHouse generated2 workloads.

Mirrors the generated2 Prometheus setup: one fake_exporter per instance
(zipf, valuescale 10000, 3 labels x 20 values, counter), scraped every 1s.
Values match fake_exporter in distribution, not sample-for-sample:
- each scrape draws one bounded Zipf(n=VALUE_SCALE, s=ZIPF_ALPHA) sample per
  series and adds it to the instance's single shared counter, in series order
  (fake_exporter's counter_state is per process, not per series);
- every series of every instance is stamped with the same 1s-aligned tick.

Ticks are DATA_START + t for t in [0, duration). Writes gzipped JSON lines
with columns matching init.sql.
"""
import argparse
import gzip
import itertools
from typing import List

import numpy as np

DATA_START = 1767225600  # 2026-01-01 00:00:00 UTC; the SQL translator anchors T here
START_PORT = 50000
JOB = "fake_exporter"
NUM_LABELS = 3
NUM_VALUES_PER_LABEL = 20
VALUE_SCALE = 10000
ZIPF_ALPHA = 1.01
# Last query in generated2 fires at T = starting_delay 70 + 1149 reps - offset 10.
DEFAULT_DURATION_SECONDS = 1210


def zipf_cdf(n: int, alpha: float) -> np.ndarray:
    weights = np.arange(1, n + 1, dtype=np.float64) ** -alpha
    cdf = np.cumsum(weights) / weights.sum()
    cdf[-1] = 1.0  # float rounding must not let searchsorted step past n
    return cdf


def series_prefixes(instance: str) -> List[str]:
    combos = itertools.product(range(NUM_VALUES_PER_LABEL), repeat=NUM_LABELS)
    return [
        f'{{"instance":"{instance}","job":"{JOB}",'
        + ",".join(f'"label_{i}":"value_{i}_value_{v}"' for i, v in enumerate(combo))
        + ","
        for combo in combos
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="output .jsonl.gz path")
    parser.add_argument("--num-instances", type=int, default=10)
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    cdf = zipf_cdf(VALUE_SCALE, ZIPF_ALPHA)
    prefixes = [
        series_prefixes(f"localhost:{START_PORT + i}")
        for i in range(args.num_instances)
    ]
    num_series = len(prefixes[0])
    counters = np.zeros(args.num_instances)

    # ponytail: gzip level 1, the loader re-parses every line so CPU here is not the bottleneck
    with gzip.open(args.out, "wt", compresslevel=1) as f:
        for t in range(args.duration):
            ts = DATA_START + t
            samples = (
                np.searchsorted(cdf, rng.random((args.num_instances, num_series))) + 1.0
            )
            values = counters[:, None] + np.cumsum(samples, axis=1)
            counters = values[:, -1]
            f.writelines(
                f'{p}"ts":{ts},"value":{v!r}}}\n'
                for inst_prefixes, inst_values in zip(prefixes, values.tolist())
                for p, v in zip(inst_prefixes, inst_values)
            )


if __name__ == "__main__":
    main()

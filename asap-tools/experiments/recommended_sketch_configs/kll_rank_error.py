"""Rank error of ASAP's p99 estimates against the replayed Google CPU values.

During a run the exporter has exported the part-0 task_usage rows with
start_time <= --cutoff-us (default: the 5-minute window starting at 600 s)
and aggregation_type 0; F is their empirical CDF and the rank error of an
estimate is |F(estimate) - 0.99|.
"""

import argparse
import gzip
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(
    0, os.path.join(HERE, "../../../asap-common/dependencies/py/promql_utilities")
)
from post_experiment.lib.results_loader import load_results  # noqa: E402


def replayed_values(trace: str, cutoff_us: int) -> np.ndarray:
    values = []
    with gzip.open(trace, "rt") as f:
        for line in f:
            c = line.rstrip("\n").split(",")
            if int(c[0]) <= cutoff_us and c[18] in ("", "0") and c[5] != "":
                values.append(float(c[5]))
    return np.sort(values)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--trace", required=True, help="task_usage part-00000-of-00500.csv.gz"
    )
    parser.add_argument(
        "--outputs", required=True, help="experiment_outputs directory of the runs"
    )
    parser.add_argument("--cutoff-us", type=int, default=615_000_000)
    parser.add_argument(
        "--runs",
        default="recommended,default,k500",
        help="suffixes of google_2011_cpu_p99_instant_<run>",
    )
    args = parser.parse_args()

    v = replayed_values(args.trace, args.cutoff_us)
    print(f"{len(v)} values, exact p99 {np.quantile(v, 0.99):.6f}")
    for name in args.runs.split(","):
        out = os.path.join(
            args.outputs,
            f"google_2011_cpu_p99_instant_{name}",
            "sketchdb",
            "prometheus_client_output",
        )
        results = load_results(out)
        est = [
            list(q.result.values())[0]
            for q in results["sketchdb"][0].query_results
            if q.result
        ]
        err = np.abs(np.searchsorted(v, est, side="right") / len(v) - 0.99)
        print(
            f"{name}: n={len(est)} rank_err median={np.median(err):.4f} "
            f"mean={np.mean(err):.4f} max={np.max(err):.4f}"
        )


if __name__ == "__main__":
    main()

"""Generate JSON-serialized patterns from Python builder.

Writes to tests/compare_patterns/out/python_patterns.json
"""

import json
import os
import sys

root = os.path.dirname(__file__)
sys.path.append(
    os.path.abspath(
        os.path.join(root, "../../CommonDependencies/dependencies/py/promql_utilities")
    )
)

from promql_utilities.ast_matching.PromQLPatternBuilder import PromQLPatternBuilder


def build_all():
    patterns = {}

    temporal = [
        PromQLPatternBuilder.function(
            ["rate", "increase"],
            PromQLPatternBuilder.matrix_selector(
                PromQLPatternBuilder.metric(collect_as="metric"),
                collect_as="range_vector",
            ),
            collect_as="function",
        ),
        PromQLPatternBuilder.function(
            "quantile_over_time",
            PromQLPatternBuilder.number(),
            PromQLPatternBuilder.matrix_selector(
                PromQLPatternBuilder.metric(collect_as="metric"),
                collect_as="range_vector",
            ),
            collect_as="function",
            collect_args_as="function_args",
        ),
    ]

    spatial = [
        PromQLPatternBuilder.aggregation(
            ["sum", "count", "avg", "quantile", "min", "max"],
            PromQLPatternBuilder.metric(collect_as="metric"),
            collect_as="aggregation",
        ),
        PromQLPatternBuilder.metric(collect_as="metric"),
    ]

    combined = [
        PromQLPatternBuilder.aggregation(
            ["sum", "count", "avg", "quantile", "min", "max"],
            PromQLPatternBuilder.function(
                "quantile_over_time",
                PromQLPatternBuilder.number(),
                PromQLPatternBuilder.matrix_selector(
                    PromQLPatternBuilder.metric(collect_as="metric"),
                    collect_as="range_vector",
                ),
                collect_as="function",
                collect_args_as="function_args",
            ),
            collect_as="aggregation",
        ),
        PromQLPatternBuilder.aggregation(
            ["sum", "count", "avg", "quantile", "min", "max"],
            PromQLPatternBuilder.function(
                [
                    "sum_over_time",
                    "count_over_time",
                    "avg_over_time",
                    "min_over_time",
                    "max_over_time",
                    "rate",
                    "increase",
                ],
                PromQLPatternBuilder.matrix_selector(
                    PromQLPatternBuilder.metric(collect_as="metric"),
                    collect_as="range_vector",
                ),
                collect_as="function",
            ),
            collect_as="aggregation",
        ),
    ]

    patterns["ONLY_TEMPORAL"] = temporal
    patterns["ONLY_SPATIAL"] = spatial
    patterns["ONE_TEMPORAL_ONE_SPATIAL"] = combined

    return patterns


def main():
    out_dir = os.path.join(os.path.dirname(__file__), "out")
    os.makedirs(out_dir, exist_ok=True)
    patterns = build_all()
    out_path = os.path.join(out_dir, "python_patterns.json")
    with open(out_path, "w") as f:
        # sort by keys
        sorted_patterns = {k: patterns[k] for k in sorted(patterns.keys())}
        json.dump(sorted_patterns, f, indent=2)
    print("Wrote", out_path)


if __name__ == "__main__":
    main()

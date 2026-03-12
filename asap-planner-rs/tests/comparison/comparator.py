#!/usr/bin/env python3
"""Semantic comparison of Python and Rust planner outputs."""
import yaml
import sys
from typing import List, Optional


def normalize_labels(labels) -> frozenset:
    if labels is None:
        return frozenset()
    if isinstance(labels, list):
        return frozenset(labels)
    return frozenset()


def agg_signature(agg: dict) -> tuple:
    """Canonical signature for matching aggregations across Python/Rust outputs."""
    return (
        agg.get("aggregationType", ""),
        agg.get("aggregationSubType", ""),
        agg.get("metric", "") or agg.get("table_name", ""),
        agg.get("spatialFilter", ""),
        normalize_labels(agg.get("labels", {}).get("rollup")),
        normalize_labels(agg.get("labels", {}).get("grouping")),
        normalize_labels(agg.get("labels", {}).get("aggregated")),
    )


def compare_streaming_configs(py_path: str, rs_path: str) -> List[str]:
    """Compare two streaming_config.yaml files. Returns list of error messages."""
    errors = []
    with open(py_path) as f:
        py = yaml.safe_load(f)
    with open(rs_path) as f:
        rs = yaml.safe_load(f)

    py_aggs = py.get("aggregations", [])
    rs_aggs = rs.get("aggregations", [])

    if len(py_aggs) != len(rs_aggs):
        errors.append(f"Aggregation count mismatch: Python={len(py_aggs)}, Rust={len(rs_aggs)}")
        return errors

    py_by_sig = {agg_signature(a): a for a in py_aggs}
    rs_by_sig = {agg_signature(a): a for a in rs_aggs}

    for sig, py_agg in py_by_sig.items():
        if sig not in rs_by_sig:
            errors.append(f"Aggregation missing in Rust: {sig}")
            continue
        rs_agg = rs_by_sig[sig]

        for field in ["windowType", "windowSize", "slideInterval", "tumblingWindowSize"]:
            if py_agg.get(field) != rs_agg.get(field):
                errors.append(
                    f"Field '{field}' mismatch for {sig}: Python={py_agg.get(field)}, Rust={rs_agg.get(field)}"
                )

        if py_agg.get("parameters") != rs_agg.get("parameters"):
            errors.append(
                f"Parameters mismatch for {sig}: Python={py_agg.get('parameters')}, Rust={rs_agg.get('parameters')}"
            )

    return errors


def compare_inference_configs(py_path: str, rs_path: str) -> List[str]:
    """Compare two inference_config.yaml files. Returns list of error messages."""
    errors = []
    with open(py_path) as f:
        py = yaml.safe_load(f)
    with open(rs_path) as f:
        rs = yaml.safe_load(f)

    py_policy = py.get("cleanup_policy", {}).get("name", "")
    rs_policy = rs.get("cleanup_policy", {}).get("name", "")
    if py_policy != rs_policy:
        errors.append(f"Cleanup policy mismatch: Python={py_policy}, Rust={rs_policy}")

    py_queries = py.get("queries", [])
    rs_queries = rs.get("queries", [])

    if len(py_queries) != len(rs_queries):
        errors.append(f"Query count mismatch: Python={len(py_queries)}, Rust={len(rs_queries)}")
        return errors

    py_by_q = {q["query"]: q for q in py_queries}
    rs_by_q = {q["query"]: q for q in rs_queries}

    for query, py_q in py_by_q.items():
        if query not in rs_by_q:
            errors.append(f"Query missing in Rust output: {query}")
            continue
        rs_q = rs_by_q[query]

        py_agg_count = len(py_q.get("aggregations", []))
        rs_agg_count = len(rs_q.get("aggregations", []))
        if py_agg_count != rs_agg_count:
            errors.append(
                f"Aggregation count mismatch for query '{query}': Python={py_agg_count}, Rust={rs_agg_count}"
            )

    return errors


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: comparator.py <py_streaming> <py_inference> <rs_streaming> <rs_inference>")
        sys.exit(1)

    errors = compare_streaming_configs(sys.argv[1], sys.argv[3])
    errors += compare_inference_configs(sys.argv[2], sys.argv[4])

    if errors:
        for e in errors:
            print(f"  DIFF: {e}")
        sys.exit(1)
    else:
        print("  MATCH")

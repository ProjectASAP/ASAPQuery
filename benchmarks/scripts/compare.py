#!/usr/bin/env python3
"""compare.py — compares baseline (Prometheus) vs ASAP query engine results.

Applies pass/fail policy:
  - FAIL if any query returned an error in ASAP (query failure)
  - WARN if any ASAP-native query has relative error > max_error (default 5%)
  - WARN (not FAIL) on >10% p95 latency regression (GH runner noise)

Generates benchmarks/reports/eval_report.md and prints it to stdout.
Exits 1 if any PASS/FAIL check failed.

Usage:
    python benchmarks/scripts/compare.py \
        [--baseline FILE] \
        [--asap FILE] \
        [--output FILE] \
        [--max-error FLOAT]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

DEFAULT_BASELINE = os.path.join(
    os.path.dirname(__file__), "..", "reports", "baseline_results.json"
)
DEFAULT_ASAP = os.path.join(
    os.path.dirname(__file__), "..", "reports", "asap_results.json"
)
DEFAULT_OUTPUT = os.path.join(
    os.path.dirname(__file__), "..", "reports", "eval_report.md"
)
DEFAULT_MAX_ERROR = 0.05   # 5 %
LATENCY_REGRESSION_THRESHOLD = 0.10  # 10 % — warn only


# ── helpers ──────────────────────────────────────────────────────────────────

def percentile(values: list[float], pct: float) -> float:
    """Compute percentile (0-100) from a list of floats using linear interpolation."""
    if not values:
        return float("nan")
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    idx = (pct / 100.0) * (n - 1)
    lo = int(idx)
    hi = lo + 1
    if hi >= n:
        return sorted_vals[-1]
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def valid_latencies(latencies: list) -> list[float]:
    """Strip None entries (failed HTTP requests) from a latency list."""
    return [x for x in latencies if x is not None]


def label_key(metric: dict) -> str:
    """Canonical sort key for a Prometheus metric label set."""
    return json.dumps(metric, sort_keys=True)


def extract_scalar_value(result: list) -> float | None:
    """Return a single float from a scalar/single-entry vector result."""
    if not result:
        return None
    entry = result[0]
    try:
        return float(entry["value"][1])
    except (KeyError, IndexError, ValueError, TypeError):
        return None


def compute_relative_error(baseline_val: float, asap_val: float) -> float:
    """Relative error: |asap - baseline| / max(|baseline|, 1e-9)."""
    denom = max(abs(baseline_val), 1e-9)
    return abs(asap_val - baseline_val) / denom


def compare_results(
    baseline_result: list, asap_result: list
) -> tuple[float | None, str]:
    """
    Compare two Prometheus vector result arrays.

    Returns (max_relative_error, note).
    For grouped results, matches entries by label set.
    """
    if not baseline_result and not asap_result:
        return 0.0, "both empty"
    if not baseline_result:
        return None, "baseline empty"
    if not asap_result:
        return None, "asap empty"

    # Build label-key → float maps
    baseline_map: dict[str, float] = {}
    for entry in baseline_result:
        key = label_key(entry.get("metric", {}))
        try:
            baseline_map[key] = float(entry["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            pass

    asap_map: dict[str, float] = {}
    for entry in asap_result:
        key = label_key(entry.get("metric", {}))
        try:
            asap_map[key] = float(entry["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            pass

    # Single-value result (no labels / collapsed)
    if len(baseline_map) == 1 and len(asap_map) == 1:
        bv = next(iter(baseline_map.values()))
        av = next(iter(asap_map.values()))
        return compute_relative_error(bv, av), ""

    # Multi-value: match by label set
    max_err = 0.0
    missing = []
    for key, bv in baseline_map.items():
        if key not in asap_map:
            missing.append(key)
            continue
        err = compute_relative_error(bv, asap_map[key])
        max_err = max(max_err, err)

    note = f"{len(missing)} label set(s) missing from ASAP" if missing else ""
    return max_err, note


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Compare baseline vs ASAP results")
    parser.add_argument("--baseline", default=DEFAULT_BASELINE, help="Baseline JSON file")
    parser.add_argument("--asap", default=DEFAULT_ASAP, help="ASAP results JSON file")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output Markdown file")
    parser.add_argument(
        "--max-error",
        type=float,
        default=DEFAULT_MAX_ERROR,
        help="Max relative error for ASAP-native queries (default: 0.01 = 1%%)",
    )
    args = parser.parse_args()

    with open(args.baseline) as f:
        baseline_data = json.load(f)
    with open(args.asap) as f:
        asap_data = json.load(f)

    baseline_results = baseline_data["results"]
    asap_results = asap_data["results"]

    # Collect all query IDs (union)
    all_ids = sorted(set(list(baseline_results.keys()) + list(asap_results.keys())))

    failures: list[str] = []
    warnings: list[str] = []
    rows: list[dict] = []

    for qid in all_ids:
        b = baseline_results.get(qid, {})
        a = asap_results.get(qid, {})

        approximate = a.get("approximate", False)
        asap_status = a.get("status", "missing")
        asap_error = a.get("error")

        # Latency p95
        b_lats = valid_latencies(b.get("latencies_ms", []))
        a_lats = valid_latencies(a.get("latencies_ms", []))
        b_p95 = percentile(b_lats, 95) if b_lats else float("nan")
        a_p95 = percentile(a_lats, 95) if a_lats else float("nan")

        # Relative error
        rel_error: float | None = None
        note = ""
        if asap_status == "success" and b.get("status") == "success":
            rel_error, note = compare_results(
                b.get("data", []), a.get("data", [])
            )

        # Pass/fail determination
        row_status = "PASS"

        if asap_status == "error" or asap_status == "missing":
            row_status = "FAIL"
            reason = asap_error or "query not present in ASAP results"
            failures.append(f"{qid}: ASAP query failed — {reason}")

        elif approximate and rel_error is not None and rel_error > args.max_error:
            row_status = "WARN"
            warnings.append(
                f"{qid}: relative error {rel_error:.4f} > threshold {args.max_error:.4f} — informational only"
            )

        # Latency regression — warn only
        if (
            not (b_p95 != b_p95)  # not NaN
            and not (a_p95 != a_p95)
            and b_p95 > 0
        ):
            latency_regression = (a_p95 - b_p95) / b_p95
            if latency_regression > LATENCY_REGRESSION_THRESHOLD:
                warnings.append(
                    f"{qid}: p95 latency regression {latency_regression:.1%} "
                    f"(baseline={b_p95:.1f}ms, asap={a_p95:.1f}ms) — "
                    "GH runner noise expected; informational only"
                )

        rows.append(
            {
                "id": qid,
                "approximate": approximate,
                "b_p95": b_p95,
                "a_p95": a_p95,
                "rel_error": rel_error,
                "status": row_status,
                "note": note,
            }
        )

    overall = "PASS" if not failures else "FAIL"

    # ── Build Markdown report ────────────────────────────────────────────────
    now = datetime.now(timezone.utc).isoformat()
    lines: list[str] = []
    lines.append("# ASAP PR Evaluation Report")
    lines.append("")
    lines.append(f"**Generated:** {now}")
    lines.append(
        f"**Overall verdict:** {'✅ PASS' if overall == 'PASS' else '❌ FAIL'}"
    )
    lines.append(f"**Max error threshold:** {args.max_error:.2%}")
    lines.append("")

    # Summary table
    lines.append(
        "| Query | ASAP Native | Baseline p95 (ms) | ASAP p95 (ms) | Rel Error | Status |"
    )
    lines.append("|-------|:-----------:|:-----------------:|:-------------:|:---------:|:------:|")
    for row in rows:
        native_mark = "yes" if row["approximate"] else "no"
        b_p95_s = f"{row['b_p95']:.1f}" if row["b_p95"] == row["b_p95"] else "n/a"
        a_p95_s = f"{row['a_p95']:.1f}" if row["a_p95"] == row["a_p95"] else "n/a"
        if row["rel_error"] is None:
            rel_err_s = "n/a"
        else:
            rel_err_s = f"{row['rel_error']:.4f}"
        status_s = "✅ PASS" if row["status"] == "PASS" else "❌ FAIL"
        note_s = f" ({row['note']})" if row["note"] else ""
        lines.append(
            f"| {row['id']}{note_s} | {native_mark} | {b_p95_s} | {a_p95_s} "
            f"| {rel_err_s} | {status_s} |"
        )

    lines.append("")

    if failures:
        lines.append("## Failures")
        lines.append("")
        for f_msg in failures:
            lines.append(f"- {f_msg}")
        lines.append("")

    if warnings:
        lines.append("## Latency Warnings (informational)")
        lines.append("")
        lines.append(
            "> **Note:** GitHub-hosted runners exhibit significant timing noise. "
            "Latency numbers are indicative only. For precise benchmarks, register "
            "a self-hosted runner once asap-tools infra is decoupled from Cloudlab "
            "(see PDF eval guide Phase 3)."
        )
        lines.append("")
        for w in warnings:
            lines.append(f"- {w}")
        lines.append("")

    lines.append("---")
    lines.append(
        "_This report was generated automatically by `benchmarks/scripts/compare.py`._"
    )

    report = "\n".join(lines)
    print(report)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        f.write(report + "\n")
    print(f"\n[compare] Report saved to {args.output}", file=sys.stderr)

    if overall == "FAIL":
        print(
            f"\n[compare] Evaluation FAILED. {len(failures)} check(s) failed:",
            file=sys.stderr,
        )
        for msg in failures:
            print(f"  - {msg}", file=sys.stderr)
        sys.exit(1)
    else:
        print("\n[compare] Evaluation PASSED.", file=sys.stderr)


if __name__ == "__main__":
    main()

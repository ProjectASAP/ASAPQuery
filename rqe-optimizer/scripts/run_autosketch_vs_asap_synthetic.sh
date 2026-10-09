#!/usr/bin/env bash
# Run the synthetic workload grid of the AutoSketch vs. ASAP evaluation
# (ProjectASAP/ASAPQuery#777, section 6 "Workload grid") and plot it.
#
# Usage:
#   SATURATION_DIR=DIR scripts/run_autosketch_vs_asap_synthetic.sh TABLES_DIR run
#   scripts/run_autosketch_vs_asap_synthetic.sh TABLES_DIR plot
#
# TABLES_DIR holds the tables and plan.tsv of
# `export_autosketch_eval_table.py --synthetic` (the workload). SATURATION_DIR
# holds the curves and the cost table of scripts/study_saturation.py, the
# same inputs the planner reads. `run` evaluates every plan.tsv
# row, JOBS at a time, at every weight setting and SLA of #777; a run over
# JOB_TIMEOUT seconds is stopped and listed in failed.txt. RUNS timing runs
# each.

set -euo pipefail

TABLES=${1:?usage: run_autosketch_vs_asap_synthetic.sh TABLES_DIR (run|plot)}
MODE=${2:?usage: run_autosketch_vs_asap_synthetic.sh TABLES_DIR (run|plot)}
OUT=${OUT:-rqe-optimizer/results/autosketch-vs-asap-synthetic}
JOBS=${JOBS:-4}
RUNS=${RUNS:-3}
JOB_TIMEOUT=${JOB_TIMEOUT:-14400}
BIN=${CARGO_TARGET_DIR:-./target}/release/examples/autosketch_vs_asap
mkdir -p "$OUT"

case "$MODE" in
run)
    : "${SATURATION_DIR:?set SATURATION_DIR to the saturation study output directory}"
    export SATURATION_DIR JOB_TIMEOUT RUNS OUT
    cargo build --release -p rqe-optimizer --example autosketch_vs_asap
    : > "$OUT/failed.txt"
    while IFS=$'\t' read -r table target result; do
        [[ -s "$OUT/$result" ]] || echo "$TABLES/$table $target $OUT/$result"
    done < "$TABLES/plan.tsv" |
        xargs -P "$JOBS" -L 1 sh -c \
            'timeout "$JOB_TIMEOUT" "$0" synthetic --table "$1" --target "$2" --runs "$RUNS" \
                --saturation-dir "$SATURATION_DIR" --out "$3" 2>> "$3.log" \
                || echo "$3" >> "$OUT/failed.txt"' \
            "$BIN"
    ;;
plot)
    python3 scripts/plot_autosketch_vs_asap_synthetic.py "$OUT"
    ;;
*)
    echo "unknown mode $MODE" >&2
    exit 2
    ;;
esac

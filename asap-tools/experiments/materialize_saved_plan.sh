#!/usr/bin/env bash
# Materialize one already-solved sketch-bench eval plan; it never re-solves it.
set -euo pipefail

usage() {
    echo "usage: $0 WORKLOAD_TABLE PLAN METHOD WEIGHTS SCRAPE_INTERVAL_MS OUTPUT_DIR [SLA_MS]" >&2
    exit 2
}

if (( $# < 6 || $# > 7 )); then
    usage
fi

workload_table=$1
plan=$2
method=$3
weights=$4
scrape_interval_ms=$5
output_dir=$6

script_dir=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(CDPATH='' cd -- "$script_dir/../.." && pwd)

args=(
    --planner saved
    --workload-table "$workload_table"
    --plan "$plan"
    --method "$method"
    --weights "$weights"
    --data-ingestion-interval-ms "$scrape_interval_ms"
    --streaming_engine precompute
    --output_dir "$output_dir"
)
if (( $# == 7 )); then
    args+=(--sla-ms "$7")
fi

cargo run --manifest-path "$repo_root/Cargo.toml" -p asap_planner --bin asap-planner -- "${args[@]}"

#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: $0 PROFILE_DIRECTORY [FLAMEGRAPH_DIRECTORY]" >&2
    exit 2
fi

PROFILE_DIRECTORY=$(readlink -f "$1")
FLAMEGRAPH_DIRECTORY=$(readlink -f "${2:-/tmp/FlameGraph}")

[[ -d "$PROFILE_DIRECTORY" ]] || {
    echo "Profile directory does not exist: $PROFILE_DIRECTORY" >&2
    exit 1
}
[[ -x "$FLAMEGRAPH_DIRECTORY/stackcollapse-perf.pl" ]] || {
    echo "Missing stackcollapse-perf.pl in $FLAMEGRAPH_DIRECTORY" >&2
    exit 1
}
[[ -x "$FLAMEGRAPH_DIRECTORY/flamegraph.pl" ]] || {
    echo "Missing flamegraph.pl in $FLAMEGRAPH_DIRECTORY" >&2
    exit 1
}

shopt -s nullglob
data_files=("$PROFILE_DIRECTORY"/perf_*.data)
(( ${#data_files[@]} > 0 )) || {
    echo "No perf_*.data files found in $PROFILE_DIRECTORY" >&2
    exit 1
}

for data_file in "${data_files[@]}"; do
    stem=${data_file%.data}
    script_file="$stem.script"
    folded_file="$stem.folded"
    svg_file="$stem.svg"
    if [[ ! -f "$script_file" ]]; then
        perf script --header -i "$data_file" > "$script_file"
    fi

    "$FLAMEGRAPH_DIRECTORY/stackcollapse-perf.pl" \
        "$script_file" > "$folded_file"
    "$FLAMEGRAPH_DIRECTORY/flamegraph.pl" \
        --colors hot \
        --title "Query Engine profile: $(basename "$data_file")" \
        "$folded_file" > "$svg_file"

    echo "Wrote $script_file"
    echo "Wrote $folded_file"
    echo "Wrote $svg_file"
done

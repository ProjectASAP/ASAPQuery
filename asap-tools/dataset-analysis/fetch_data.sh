#!/usr/bin/env bash
# Download the trace subsets analyzed by fit_skew.py into DATA_ROOT.
# Files that already exist are skipped, so the script can be re-run to resume.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 DATA_ROOT" >&2
    exit 1
fi
DATA_ROOT=$1

GOOGLE_URL=https://storage.googleapis.com/clusterdata-2011-2
ALIBABA_URL=https://aliopentrace.oss-cn-beijing.aliyuncs.com/v2022MicroservicesTraces
BOOM_URL=https://huggingface.co/datasets/Datadog/BOOM/resolve/main

# task_usage parts cover about 1.4 hours each; 120 parts = about 7 days.
# task_events uses the same parts; job_events is read in full for job names.
GOOGLE_TASK_PARTS=120
GOOGLE_JOB_EVENT_PARTS=500
# CallGraph and MCRRTUpdate shards cover 3 minutes each: 480 shards = 1 day.
ALIBABA_RPC_SHARDS=480
# MSMetricsUpdate shards cover 30 minutes, NodeMetricsUpdate 12 hours: 1 day.
ALIBABA_MS_SHARDS=48
ALIBABA_NODE_SHARDS=2

BOOM_SERIES=(
    ds-2187-H ds-2394-D ds-1135-5T ds-1833-D ds-2806-D
    ds-2573-D ds-2222-30T ds-1914-30T ds-2577-H ds-2212-D
    ds-2782-H ds-1400-10S ds-2650-D ds-1316-10S ds-1558-5T
    ds-1972-D ds-1840-D ds-671-10S ds-1524-5T ds-899-T
)
BOOM_SERIES_FILES=(data-00000-of-00001.arrow dataset_info.json state.json)

# fetch URL DEST: download to a temp file, then rename so partial files never look complete.
fetch() {
    local url=$1 dest=$2
    if [ -s "$dest" ]; then
        return
    fi
    mkdir -p "$(dirname "$dest")"
    echo "fetch $url"
    curl -fL --retry 5 --retry-delay 5 -C - -o "$dest.part" "$url"
    mv "$dest.part" "$dest"
}

google_part() {
    printf 'part-%05d-of-00500.csv.gz' "$1"
}

G=$DATA_ROOT/google-2011
fetch "$GOOGLE_URL/schema.csv" "$G/schema.csv"
for ((i = 0; i < GOOGLE_TASK_PARTS; i++)); do
    for t in task_usage task_events; do
        fetch "$GOOGLE_URL/$t/$(google_part "$i")" "$G/$t/$(google_part "$i")"
    done
done
for ((i = 0; i < GOOGLE_JOB_EVENT_PARTS; i++)); do
    fetch "$GOOGLE_URL/job_events/$(google_part "$i")" "$G/job_events/$(google_part "$i")"
done

A=$DATA_ROOT/alibaba-v2022
for ((i = 0; i < ALIBABA_NODE_SHARDS; i++)); do
    fetch "$ALIBABA_URL/NodeMetricsUpdate/NodeMetricsUpdate_$i.tar.gz" "$A/NodeMetricsUpdate_$i.tar.gz"
done
for ((i = 0; i < ALIBABA_MS_SHARDS; i++)); do
    fetch "$ALIBABA_URL/MSMetricsUpdate/MSMetricsUpdate_$i.tar.gz" "$A/MSMetricsUpdate_$i.tar.gz"
done
for ((i = 0; i < ALIBABA_RPC_SHARDS; i++)); do
    fetch "$ALIBABA_URL/CallGraph/CallGraph_$i.tar.gz" "$A/CallGraph_$i.tar.gz"
    fetch "$ALIBABA_URL/MCRRTUpdate/MCRRTUpdate_$i.tar.gz" "$A/MCRRTUpdate_$i.tar.gz"
done

B=$DATA_ROOT/boom
fetch "$BOOM_URL/dataset_taxonomy.json" "$B/dataset_taxonomy.json"
for series in "${BOOM_SERIES[@]}"; do
    for f in "${BOOM_SERIES_FILES[@]}"; do
        fetch "$BOOM_URL/$series/$f" "$B/$series/$f"
    done
done

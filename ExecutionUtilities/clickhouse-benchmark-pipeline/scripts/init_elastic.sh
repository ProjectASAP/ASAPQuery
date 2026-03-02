#!/bin/bash
# Load H2O data directly into Elasticsearch using Rust data_exporter

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

# Load config
set -a
source "${PROJECT_DIR}/config.env"
set +a

H2O_FILE="${PROJECT_DIR}/${H2O_DATA_DIR}/${H2O_FILENAME}"

echo "=== H2O → Elasticsearch Direct Import (Rust) ==="
echo ""

# Check if H2O data file exists
if [ ! -f "${H2O_FILE}" ]; then
    echo "H2O data file not found at ${H2O_FILE}"
    echo "Downloading H2O dataset..."
    cd "${PROJECT_DIR}/benchmark_importer"
    python3 download_h2o_data.py --output-dir "${PROJECT_DIR}/${H2O_DATA_DIR}"
    cd "${SCRIPT_DIR}"
    
    if [ ! -f "${H2O_FILE}" ]; then
        echo "Error: Failed to download H2O data"
        exit 1
    fi
fi

FILE_SIZE=$(du -h "${H2O_FILE}" | cut -f1)
echo "Found H2O data file: ${H2O_FILE} (${FILE_SIZE})"
echo ""

# Check Elasticsearch connection
echo "Checking Elasticsearch connection at ${ES_HOST}:${ES_PORT}..."
if ! curl -s "http://${ES_HOST}:${ES_PORT}/" > /dev/null; then
    echo "Error: Cannot connect to Elasticsearch at ${ES_HOST}:${ES_PORT}"
    echo "Please ensure Elasticsearch is running"
    exit 1
fi

ES_VERSION=$(curl -s "http://${ES_HOST}:${ES_PORT}/" | grep -o '"number" : "[^"]*"' | head -1 | sed 's/.*: "\(.*\)"/\1/')
echo "Connected to Elasticsearch version: ${ES_VERSION}"
echo ""

# Build the data_exporter binary if needed
cd "${PROJECT_DIR}/data_exporter"
if [ ! -f target/release/data_exporter ]; then
    echo "Building data_exporter..."
    cargo build --release
fi

echo ""
echo "Importing H2O data into Elasticsearch..."
echo "This may take several minutes..."
echo ""

EXTRA_ARGS=()
if [ -n "${TOTAL_RECORDS}" ] && [ "${TOTAL_RECORDS}" -gt 0 ]; then
    EXTRA_ARGS+=(--total-records "${TOTAL_RECORDS}")
fi

./target/release/data_exporter \
    --mode h2o-elasticsearch \
    --input-file "${H2O_FILE}" \
    --elastic-host "${ES_HOST}" \
    --elastic-port "${ES_PORT}" \
    --elastic-index "${ES_INDEX_NAME}" \
    --elastic-api-key "${ES_API_KEY}" \
    --batch-size "${ES_BULK_SIZE}" \
    "${EXTRA_ARGS[@]}"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Import complete!"
    echo ""
    echo "Index: ${ES_INDEX_NAME}"
else
    echo ""
    echo "Error: Failed to import data into Elasticsearch"
    exit 1
fi

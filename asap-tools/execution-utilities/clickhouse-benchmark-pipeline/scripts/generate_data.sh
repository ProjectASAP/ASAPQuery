#!/bin/bash
# Data generation script with multiple data source modes
#
# Usage:
#   DATA_MODE=fake ./scripts/generate_data.sh
#   DATA_MODE=clickbench ./scripts/generate_data.sh
#   DATA_MODE=h2o ./scripts/generate_data.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

# Load config
set -a
source "${PROJECT_DIR}/config.env"
set +a

# Define Directories
IMPORTER_DIR="${PROJECT_DIR}/benchmark_importer"
DATA_DIR="${IMPORTER_DIR}/data"

# Hardcoded Kafka Path
KAFKA_HOME="${PROJECT_DIR}/../../installation/kafka/kafka"
KAFKA_BIN="${KAFKA_HOME}/bin"

# Check Mode
if [ -z "${DATA_MODE}" ]; then
    echo "Error: DATA_MODE is required"
    echo "Usage: DATA_MODE=fake|clickbench|h2o $0"
    exit 1
fi

echo "Data generation mode: ${DATA_MODE}"
echo "Kafka broker: ${KAFKA_BROKER}"

# 1. Build Rust Binary (Common for all modes)
echo "Building data_exporter..."
cd "${PROJECT_DIR}/data_exporter"
if [ ! -f target/release/data_exporter ]; then
    cargo build --release
fi
cd "${PROJECT_DIR}"

case "${DATA_MODE}" in
    fake)
        # Synthetic Data Generation
        # Note: We don't strictly need to create the topic here as the producer will auto-create it,
        # but if you wanted to enforce partitions, you would use ${KAFKA_BIN}/kafka-topics.sh here.
        
        EXTRA_ARGS=()
        if [ -n "${TOTAL_RECORDS}" ] && [ "${TOTAL_RECORDS}" -gt 0 ]; then
            EXTRA_ARGS+=(--total-records "${TOTAL_RECORDS}")
        fi

        ./data_exporter/target/release/data_exporter \
            --mode fake \
            --kafka-broker "${KAFKA_BROKER}" \
            --kafka-topic "${KAFKA_TOPIC}" \
            "${EXTRA_ARGS[@]}"
        ;;

    clickbench)
        # Real ClickBench Data
        echo "Ensuring ClickBench data exists..."
        
        # Call the Python downloader
        python3 "${IMPORTER_DIR}/download_data.py" \
            --output-dir "${DATA_DIR}"

        FILE_NAME="hits.json" 
        # Check for .gz if .json doesn't exist
        if [ ! -f "${DATA_DIR}/${FILE_NAME}" ] && [ -f "${DATA_DIR}/hits.json.gz" ]; then
            FILE_NAME="hits.json.gz"
        fi

        echo "Ingesting ${FILE_NAME} to Kafka..."
        ./data_exporter/target/release/data_exporter \
            --mode clickbench \
            --clickbench-file "${DATA_DIR}/${FILE_NAME}" \
            --kafka-broker "${KAFKA_BROKER}" \
            --kafka-topic "${KAFKA_TOPIC}"
        ;;

    h2o)
        # H2O Benchmark Data
        H2O_TOPIC=${H2O_KAFKA_TOPIC:-h2o_groupby}
        H2O_FILE="G1_1e7_1e2_0_0.csv"
        
        # 1. Ensure Topic Exists using hardcoded path
        echo "Ensuring topic '${H2O_TOPIC}' exists..."
        
        if [ -x "${KAFKA_BIN}/kafka-topics.sh" ]; then
            "${KAFKA_BIN}/kafka-topics.sh" --create --if-not-exists \
                --topic "${H2O_TOPIC}" \
                --bootstrap-server "${KAFKA_BROKER}" \
                --partitions 1 --replication-factor 1 2>/dev/null || true
        else
            echo "Warning: ${KAFKA_BIN}/kafka-topics.sh not found. Skipping explicit topic creation."
        fi

        # 2. Download Data
        echo "Ensuring H2O data exists..."
        
        # Check and install gdown if missing (required for H2O download)
        if ! python3 -c "import gdown" 2>/dev/null; then
            echo "Installing python dependency: gdown..."
            pip install gdown
        fi

        python3 "${IMPORTER_DIR}/download_h2o_data.py" \
            --output-dir "${DATA_DIR}"

        # 3. Ingest Data
        echo "Ingesting ${H2O_FILE} to Kafka topic ${H2O_TOPIC}..."
        ./data_exporter/target/release/data_exporter \
            --mode h2o \
            --input-file "${DATA_DIR}/${H2O_FILE}" \
            --kafka-broker "${KAFKA_BROKER}" \
            --kafka-topic "${H2O_TOPIC}"
        ;;

    *)
        echo "Error: Unknown DATA_MODE '${DATA_MODE}'"
        exit 1
        ;;
esac
#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 <path to kafka directory>"
  exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

# Load overrides if they exist
if [ -f "$THIS_DIR/override.env" ]; then
    source "$THIS_DIR/override.env"
fi

KAFKA_DIR=$1

KAFKA_CONFIG_FILE="./config/kraft/server.properties"

# Build override arguments if set
OVERRIDE_ARGS=""
[ -n "$LOG_RETENTION_MS" ] && OVERRIDE_ARGS="$OVERRIDE_ARGS --override log.retention.ms=$LOG_RETENTION_MS"
[ -n "$LOG_RETENTION_BYTES" ] && OVERRIDE_ARGS="$OVERRIDE_ARGS --override log.retention.bytes=$LOG_RETENTION_BYTES"
[ -n "$LOG_SEGMENT_BYTES" ] && OVERRIDE_ARGS="$OVERRIDE_ARGS --override log.segment.bytes=$LOG_SEGMENT_BYTES"
[ -n "$LOG_RETENTION_CHECK_INTERVAL_MS" ] && OVERRIDE_ARGS="$OVERRIDE_ARGS --override log.retention.check.interval.ms=$LOG_RETENTION_CHECK_INTERVAL_MS"

cd "$KAFKA_DIR" || exit
# shellcheck disable=SC2086
if ! ./bin/kafka-server-start.sh $KAFKA_CONFIG_FILE $OVERRIDE_ARGS; then
    echo "Error starting Kafka server"
    echo "Trying reset"
    UUID=$(./bin/kafka-storage.sh random-uuid)
    ./bin/kafka-storage.sh format -t "$UUID" --config $KAFKA_CONFIG_FILE
    # shellcheck disable=SC2086
    if ! ./bin/kafka-server-start.sh $KAFKA_CONFIG_FILE $OVERRIDE_ARGS; then
        echo "Error starting Kafka server again"
        exit 1
    fi
fi

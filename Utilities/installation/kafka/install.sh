#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <install_dir>"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$THIS_DIR/../utils.sh"

# Load overrides if they exist
if [ -f "$THIS_DIR/override.env" ]; then
    source "$THIS_DIR/override.env"
fi

INSTALL_DIR=$1

KAFKA_FILENAME="kafka_2.13-3.8.0.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/3.8.0/"$KAFKA_FILENAME
KAFKA_DIRNAME="kafka"

KAFKA_CONFIG_FILE="./config/kraft/server.properties"
KAFKA_LOG_DIR="${KAFKA_LOG_DIR:-/scratch/kraft-combined-logs}"

mkdir -p "$KAFKA_LOG_DIR"

cd "$INSTALL_DIR" || exit
wget $KAFKA_URL
untar $KAFKA_FILENAME $KAFKA_DIRNAME
cd "$KAFKA_DIRNAME" || exit

#HOST_IP=$(ip r | grep 10.10 | awk '{print $9}')
if [ -n "$KAFKA_HOST_IP" ]; then
    HOST_IP="$KAFKA_HOST_IP"
else
    HOST_IP=$(ip a | grep 10.10 | awk '{print $2}' | cut -d '/' -f1)
fi

# Set up Kafka configuration
sed -i "s|log.dirs=.*|log.dirs=$KAFKA_LOG_DIR|g" $KAFKA_CONFIG_FILE
# Increase message size limit to 20MB to accommodate large precomputes (e.g., 3x65536 CountMinSketch)
set_property "$KAFKA_CONFIG_FILE" "message.max.bytes" "20971520"
set_property "$KAFKA_CONFIG_FILE" "replica.fetch.max.bytes" "20971520"
set_property "$KAFKA_CONFIG_FILE" "log.retention.hours" "1"
set_property "$KAFKA_CONFIG_FILE" "advertised.listeners" "PLAINTEXT://${HOST_IP}:9092"

echo "Resetting Kafka storage"
UUID=$(./bin/kafka-storage.sh random-uuid)
./bin/kafka-storage.sh format -t "$UUID" --config $KAFKA_CONFIG_FILE
echo "Done resetting Kafka storage"

#!/bin/bash

# PREDEFINED_COMPONENTS=("benchmarks" "exporters" "flink" "grafana" "kafka" "prometheus" "prometheus_kafka_adapter" "asprof")
PREDEFINED_COMPONENTS=("benchmarks" "exporters" "flink" "grafana" "kafka" "prometheus" "asprof" "arroyo")

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <install_dir> <component1> [<component2> ...]"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

INSTALL_DIR=$1
shift
COMPONENTS=("$@")

if [ "${COMPONENTS[0]}" == "all" ]; then
    COMPONENTS=("${PREDEFINED_COMPONENTS[@]}")
fi
COMPONENTS=("common" "${COMPONENTS[@]}")

for COMPONENT in "${COMPONENTS[@]}"; do
    COMPONENT_DIR="$THIS_DIR/$COMPONENT"
    if [ ! -d "$COMPONENT_DIR" ]; then
        echo "Error: Component directory $COMPONENT_DIR does not exist."
        exit 1
    fi

    if [ -f "$COMPONENT_DIR/setup_dependencies.sh" ]; then
        (source "$COMPONENT_DIR/setup_dependencies.sh")
    fi
    if [ -f "$COMPONENT_DIR/install.sh" ]; then
        (source "$COMPONENT_DIR/install.sh" "$INSTALL_DIR")
    fi
done

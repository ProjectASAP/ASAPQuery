#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <experiment_dir>"
    exit 1
fi

EXPERIMENT_DIR=$1
PROMETHEUS_DATA_DIR=$EXPERIMENT_DIR/prometheus/prometheus_data

# PROMETHEUS_HOME_DIR=/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/prometheus/prometheus-2.53.2.linux-amd64
PROMETHEUS_HOME_DIR=/scratch/sketch_db_for_prometheus/prometheus

$PROMETHEUS_HOME_DIR/prometheus --storage.tsdb.path=$PROMETHEUS_DATA_DIR/data --config.file=$PROMETHEUS_HOME_DIR/prometheus.yml &
sleep 20
python3 export_prometheus_data.py --output_dir $PROMETHEUS_DATA_DIR/exported_data --metric_names fake_metric_total --formats csv
killall $PROMETHEUS_HOME_DIR/prometheus

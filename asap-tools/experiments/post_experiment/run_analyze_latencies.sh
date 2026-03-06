#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <experiment_name> <experiment_mode> [--print_per_query]"
  echo "Example: $0 my_experiment sketchdb --print_per_query"
  exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

EXP_NAME=$1
EXP_MODE=$2
PER_QUERY_FLAG=$3

python3 $THIS_DIR/analyze_latencies.py --experiment_name $EXP_NAME --experiment_mode $EXP_MODE ${PER_QUERY_FLAG}

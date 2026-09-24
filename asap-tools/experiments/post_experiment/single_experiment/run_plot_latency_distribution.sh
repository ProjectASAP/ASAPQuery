#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <experiment_name> [--print] [--show] [--save] [--output <path>]"
  echo ""
  echo "Options:"
  echo "  --print       Print percentile data to console"
  echo "  --show        Display the plot interactively"
  echo "  --save        Save the plot to a file"
  echo "  --output PATH Specify output file path (default: experiment_dir/latency_distribution_<modes>.png)"
  echo ""
  echo "Example: $0 my_experiment --print --save"
  exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

EXP_NAME=$1
shift  # Remove first argument, rest are optional flags

python3 "$THIS_DIR"/plot_latency_distribution.py \
  --experiment_name "$EXP_NAME" \
  --exact_experiment_mode baseline \
  --estimate_experiment_mode sketchdb \
  "$@"

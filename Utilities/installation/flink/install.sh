#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <install_dir>"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$THIS_DIR/../utils.sh"

INSTALL_DIR=$1

FLINK_FILENAME="flink-1.20.0-bin-scala_2.12.tgz"
FLINK_URL="https://dlcdn.apache.org/flink/flink-1.20.0/"$FLINK_FILENAME
FLINK_DIRNAME="flink"

cd $INSTALL_DIR
wget $FLINK_URL
rm -rf $FLINK_DIRNAME; mkdir $FLINK_DIRNAME
untar $FLINK_FILENAME $FLINK_DIRNAME

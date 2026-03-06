#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <install_dir>"
    exit 1
fi

INSTALL_DIR=$1

mkdir -p "$INSTALL_DIR"

cd "$INSTALL_DIR" || exit

# Download ClickHouse static binary
curl https://clickhouse.com/ | sh
chmod +x clickhouse

echo "ClickHouse installed to $INSTALL_DIR"

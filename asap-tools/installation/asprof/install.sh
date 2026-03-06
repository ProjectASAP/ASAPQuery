#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <install_dir>"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$THIS_DIR/../utils.sh"

INSTALL_DIR=$1

ASPROF_FILENAME="async-profiler-3.0-linux-x64.tar.gz"
ASPROF_URL="https://github.com/async-profiler/async-profiler/releases/download/v3.0/"$ASPROF_FILENAME
ASPROF_DIRNAME="asprof"

cd $INSTALL_DIR
wget $ASPROF_URL
rm -rf $ASPROF_DIRNAME; mkdir $ASPROF_DIRNAME
untar $ASPROF_FILENAME $ASPROF_DIRNAME

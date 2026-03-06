#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <root_dir>"
  exit 1
fi

ROOT_DIR=$1

# DeathStarBench
DEATHSTAR_BENCH_REPO="https://github.com/delimitrou/DeathStarBench.git"
DEATHSTAR_BENCH_DIRNAME="DeathStarBench"

cd $ROOT_DIR
mkdir -p benchmarks
cd benchmarks

rm -rf $DEATHSTAR_BENCH_DIRNAME
git clone --recurse-submodules $DEATHSTAR_BENCH_REPO
cd $DEATHSTAR_BENCH_DIRNAME/wrk2
make -j`nproc`

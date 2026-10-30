# Workload 01 pilot plan

The saved sketch-bench plan for workload 01's seven common-promql queries at
100 ms ingestion, and its ASAPQuery configs:

- `eval_table.json`: sketch-bench's eval table of the manifest
  (`export_autosketch_eval_table.py --random-manifest`).
- `plans.json.gz`: each method's chosen plans (`autosketch_vs_asap`, p95).
- `streaming_config.yaml`, `inference_config.yaml`: the cheapest `asap` plan
  on CPU weights, through `asap-planner --planner saved`.

The `asap` plan serves the coarse sum from the fine sum's deployment (a
roll-up) and the three quantiles from one per-series DDSketch. Top-k heaps
over 500 series read the curves at their first 1,000 keys (the table's
`assumptions`).

`../workload-NN-pilot/` hold workloads 02-10's plans, made the same way
(sketch-bench f885d7e: queries reading the same RQE share it).

Regenerate, with `SKETCH_BENCH` a sketch-bench checkout at the table's
`sketch_bench_revision` (for the exporter), from this repository's root. The
runner and plan extractor come from this repository's `rqe-optimizer`, so to
reproduce the committed plans exactly, check out the commit that last changed
them (`git log -1 -- $DIR/workload-01-pilot/plans.json.gz`) first:

```bash
DIR=asap-tools/experiments/datasets/fake_metric/random_workloads/seed-862
OUT=$DIR/workload-01-pilot; SAT=$(mktemp -d); TMP=$(mktemp -d)
cp -r rqe-optimizer/results/autosketch-vs-asap-inputs/saturation/out_* $SAT/
cp -r rqe-optimizer/results/autosketch-vs-asap-hydra/inputs/saturation/* $SAT/
cargo build --release -p rqe-optimizer --examples
python3 $SKETCH_BENCH/scripts/export_autosketch_eval_table.py \
  --random-manifest $DIR/workload-01.json --out $OUT/eval_table.json
target/release/examples/autosketch_vs_asap synthetic --table $OUT/eval_table.json \
  --target p95 --runs 1 --saturation-dir $SAT --out $TMP/tp95.json
python3 rqe-optimizer/scripts/extract_autosketch_plans.py $TMP/tp95.json $OUT/plans.json.gz
cargo run --release --manifest-path asap-planner-rs/Cargo.toml --bin asap-planner -- \
  --planner saved --workload-table $OUT/eval_table.json --plan $OUT/plans.json.gz \
  --method asap --weights cpu --output_dir $OUT \
  --streaming_engine precompute --data-ingestion-interval-ms 100
```

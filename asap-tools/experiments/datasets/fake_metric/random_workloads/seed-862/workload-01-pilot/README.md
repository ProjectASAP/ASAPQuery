# Workload 01 pilot plan

Planner input and legacy-planner output for workload 01's seven common-promql
queries at 100 ms ingestion. Regenerate from the repository root:

```bash
python3 asap-tools/experiments/datasets/fake_metric/random_workload_artifacts.py \
  --manifest asap-tools/experiments/datasets/fake_metric/random_workloads/seed-862/workload-01.json \
  --out /tmp/w01
cargo run --release --manifest-path asap-planner-rs/Cargo.toml --bin asap-planner -- \
  --input_config /tmp/w01/pilot_promql_planner.yaml --output_dir /tmp/w01/plan \
  --streaming_engine precompute --data-ingestion-interval-ms 100
```

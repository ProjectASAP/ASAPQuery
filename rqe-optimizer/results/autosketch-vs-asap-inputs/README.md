# Inputs of the AutoSketch vs. ASAP figures

Everything the three figures were computed from (`../autosketch-vs-asap-synthetic/`
and `../autosketch-vs-asap-synthetic-sla/`), as run on 2026-10-08.

- `saturation/`: the sketch-bench study on asap_sketchlib 0.3.0 (#174,
  #178–#186), the `--saturation-dir` the runner reads:
  - `optimizer_cost/rqe_atomic_costs.json`: the cost table, one row per
    config of every family the optimizer plans, sketches (HLL, KLL,
    DDSketch, CMS-heap top-k) and exact aggregations (exact-sum,
    exact-increase, exact-min, exact-max, exact-delta-set): CPU per insert,
    merge and query, memory per instance, accuracy, measured serially on one
    idle machine at the synthetic data's shape;
  - `out_grid_1e7_cost/`: accuracy vs. items to N = 1e7 over the grid,
    including the cost table's shape (Zipf 1.1 over 1e4 keys; Pareto a = 2),
    with merge curves for the sketches that lose accuracy when merged;
  - `out_1e9/`: the targeted points past 1e7 that the workloads read.
- `tables/`: the synthetic mixed-set workloads from
  `scripts/export_autosketch_eval_table.py --synthetic` (RQEs, streams,
  targets, data shapes) and `plan.tsv` (one run per table).

AutoSketch's measured benchmark times are next to each figure's results
(`autosketch-benchmark-times.json`, `autosketch_benchmark_times.json`).

# Synthetic mixed set: AutoSketch vs. ASAP under a batch latency SLA (version 2)

Version 2 of the evaluation in ProjectASAP/ASAPQuery#777, with the cost model
of sketch-bench `docs/rqe_optimizer_cost_model.md`, "Cost by use and batch
latency":

- **Cost by use:** `w_cpu · AUC(CPU) + w_mem · AUC(memory)`, CPU elastic, for
  two weight settings: CPU only (vCPU), and Fargate's prices ($/hour).
- **Batch latency:** the longest chain of a batch (the newest window's
  compaction, then the query). This is the job placement's closed form with
  elastic CPU.
- **ASAP and PerQuery:** at each SLA `L` ∈ {100, 300, 1000, 3000, 10000} ms,
  each plans the cheapest plan whose batch latency is at most `L`
  (`milp::minimize_usage_cost` with `latency_bound_ms`). The solve is exact,
  so every plan meets its SLA.
- **AutoSketch-Adapted:** keeps its one plan (accuracy only, memory
  objective), and `meets_sla` says whether that plan's latency meets each
  SLA.

Version 1 (no latency constraint, with the cost–latency frontiers) is in
`../autosketch-vs-asap-synthetic/`. Both versions come from the same runner;
the JSON files here hold version 2 in `sla_results`.

## Results

Run on clnode109 (Intel Xeon E5-2683 v3 @ 2.00 GHz, 56 cores, 2026-10-08),
`--runs 3`, without per-RQE choices in the JSON files, over the asap_sketchlib
0.3.0 study. `fig_cost_vs_sla.png` plots each method's cost at each SLA;
`summary_sla.md` has every number.

No RQE was dropped, and no sanity check failed: every ASAP and PerQuery plan
meets its SLA, and ASAP costs no more than PerQuery at each SLA. The tightest
SLA either method can meet is 92 ms. AutoSketch's latency is 1011 ms, so it
meets only the 3000 and 10000 ms SLAs.

CPU only (vCPU):

| Workload | RQEs | SLA (ms) | ASAP | PerQuery-CostAware | AutoSketch-Adapted |
|---|---|---|---|---|---|
| mixed | 50 | 100 | 34.5 | 59.2 | 3410 (misses) |
| | | 1000 | 5.12 | 12.0 | 3410 (misses) |
| | | 3000 | 3.32 | 9.74 | 3410 |
| mixed, r = 8 | 92 | 100 | 34.5 | 74.2 | 4089 (misses) |
| | | 1000 | 5.15 | 19.3 | 4089 (misses) |
| | | 3000 | 3.39 | 16.9 | 4089 |
| mixed, m = 8 | 400 | 100 | 276 | 473 | 27280 (misses) |
| | | 1000 | 41.0 | 96.2 | 27280 (misses) |
| | | 3000 | 26.6 | 77.9 | 27280 |
| mixed, m = 16 | 800 | 100 | 553 | 947 | 54560 (misses) |
| | | 1000 | 81.9 | 192 | 54560 (misses) |
| | | 3000 | 53.2 | 156 | 54560 |

## Planning time

| Workload | ASAP (candidates + MILP, unbounded) | PerQuery | AutoSketch: search + measured benchmark |
|---|---|---|---|
| mixed | 0.67 s | 0.57 s | 0.002 s + 197 s |
| mixed, r = 8 | 1.48 s | 1.02 s | 0.003 s + 197 s |
| mixed, m = 8 | 6.17 s | 5.08 s | 0.014 s + 1580 s |
| mixed, m = 16 | 13.4 s | 10.8 s | 0.029 s + 3159 s |

**How the AutoSketch benchmark is measured:**
`scripts/autosketch_benchmark_time.py` runs every probed config's
approxbench accuracy benchmark, serially, alone on clnode109
(`autosketch_benchmark_times.json`).
- Each run is 1e8 items on the probe's data shape: generate the data, run the
  sketch, compute the exact baseline, and score it. It takes 20–33 s.
- AutoSketch benchmarks each probed config once per metric, on that metric's
  data. Exact accumulators have nothing to benchmark.
- For reference, the paper's rate (60 s per probe) would give 600 s at m = 1.

## Reproduce

```sh
# The inputs these results used are committed: TABLES=../autosketch-vs-asap-inputs/tables,
# DIR=../autosketch-vs-asap-inputs/saturation (paths from this directory).
python3 scripts/export_autosketch_eval_table.py --synthetic --out TABLES
# For each row of TABLES/plan.tsv:
target/release/examples/autosketch_vs_asap synthetic --table TABLES/T --target p95 \
    --saturation-dir DIR --out OUT/R --runs 3
python3 scripts/autosketch_benchmark_time.py --binary target/release/approxbench \
    --out OUT/autosketch_benchmark_times.json OUT/synthetic-*-tp95.json
python3 scripts/plot_autosketch_vs_asap_sla.py OUT OUT/synthetic-*-tp95.json
```

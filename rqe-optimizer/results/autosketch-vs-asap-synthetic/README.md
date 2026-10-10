# Synthetic workload: AutoSketch vs. ASAP

Results of ProjectASAP/ASAPQuery#777's synthetic workload on the **mixed
template set** (the 10 templates, 50 RQEs), scaled by shared replicas r = 8
(on one metric, 92 RQEs) and by m = 8 and 16 metrics (one copy of the
template set per metric, 50 · m RQEs). One accuracy level, p95: 95% in each
family's own metric (rank, relative value and relative error at most 0.05;
top-k precision at least 0.95).

**Cost model: cost by use** (sketch-bench `docs/rqe_optimizer_cost_model.md`,
"Cost by use and batch latency", #188): `w_cpu · AUC(CPU) + w_mem ·
AUC(memory)`, CPU elastic, at two weight settings (CPU only, in vCPU; AWS
Fargate's per-vCPU and per-GB prices, in $/hour). CPU covers ingest (on
`⌈ρ⌉` workers split by sample), compaction of each closed window, and one
query job (merge, then estimate) per firing; memory covers ingest, storage,
compaction and query memory, each counted once.

**Latency is reported, not constrained (version 1, no SLA).** A plan's query
latency is its worst batch latency, which with elastic CPU is its longest
chain (the newest window's compaction, then the query). ASAP and
PerQuery-CostAware are solved without a bound (the cheapest plan) and for 12
latency bounds log-spaced from the tightest feasible bound to the unbounded
plan's latency, plus a bound at AutoSketch's latency: each method's
**cost-latency frontier**. AutoSketch-Adapted ignores latency and is one
point. AutoSketch's planning time is its search **plus its measured
benchmark**: `scripts/autosketch_benchmark_time.py` ran approxbench's
accuracy benchmark (1e8 items, exact baseline and scoring) once per distinct
probed (config, data shape), serially on idle clnode155
(`autosketch-benchmark-times.json`), and charged it once per probed
(metric, config).

Runs: one workload per CloudLab node, all Intel Xeon E5-2683 v3 @ 2.00 GHz
with 56 cores (mixed on clnode155, r = 8 on clnode178, m = 8 on clnode167,
m = 16 on clnode138), 2026-10-08, `--runs 3`,
sketch-bench at #188 + this branch. No RQE was dropped and no sanity check
failed (ASAP ≤ PerQuery, ASAP ≤ AutoSketch, each frontier monotone).

Files: `synthetic-*.json` (raw: every method's plan, unbounded and bounded,
per weight setting, without per-RQE choices; rerun with the runner for them), `summary_synthetic.md` (every point),
`fig_frontier.png` (cost vs. query latency per workload and weight setting;
ASAP and PerQuery frontiers as lines, AutoSketch as a point, every point
labeled), `fig_planning_time.png` (planning time vs. RQEs over m).

## Results

Each method's cheapest plan and its latency, and ASAP's and PerQuery's cost
at no more than AutoSketch's latency (absolute costs):

| Workload | RQEs | Weights | ASAP: cheapest (latency) | PerQuery: cheapest (latency) | AutoSketch (latency) | ASAP at AutoSketch's latency | PerQuery at AutoSketch's latency |
|---|---|---|---|---|---|---|---|
| mixed (default) | 50 | cpu | 3.32 vCPU (1.21e+04 ms) | 9.55 vCPU (3.84e+03 ms) | 3.41e+03 vCPU (1.01e+03 ms) | 5.12 vCPU | 12 vCPU |
| mixed (default) | 50 | fargate | 0.22 $/h (2.94e+03 ms) | 0.72 $/h (2.97e+03 ms) | 163 $/h (1.01e+03 ms) | 0.284 $/h | 0.813 $/h |
| mixed, m = 8 | 400 | cpu | 26.5 vCPU (1.21e+04 ms) | 76.4 vCPU (3.84e+03 ms) | 2.73e+04 vCPU (1.01e+03 ms) | 41 vCPU | 96.2 vCPU |
| mixed, m = 8 | 400 | fargate | 1.76 $/h (2.94e+03 ms) | 5.76 $/h (2.97e+03 ms) | 1.31e+03 $/h (1.01e+03 ms) | 2.27 $/h | 6.5 $/h |
| mixed, m = 16 | 800 | cpu | 53.1 vCPU (1.21e+04 ms) | 153 vCPU (3.84e+03 ms) | 5.46e+04 vCPU (1.01e+03 ms) | 81.9 vCPU | 192 vCPU |
| mixed, m = 16 | 800 | fargate | 3.52 $/h (2.94e+03 ms) | 11.5 $/h (2.97e+03 ms) | 2.61e+03 $/h (1.01e+03 ms) | 4.54 $/h | 13 $/h |
| mixed, r = 8 | 92 | cpu | 3.39 vCPU (2.94e+03 ms) | 16.7 vCPU (3.84e+03 ms) | 4.09e+03 vCPU (1.01e+03 ms) | 5.15 vCPU | 19.3 vCPU |
| mixed, r = 8 | 92 | fargate | 0.223 $/h (2.94e+03 ms) | 1.08 $/h (2.97e+03 ms) | 192 $/h (1.01e+03 ms) | 0.285 $/h | 1.18 $/h |

Planning time (CPU-only weights; AutoSketch's includes its measured
benchmark; the paper's 60 s per probe is a reference only):

| RQEs | ASAP (candidates + MILP) | PerQuery-CostAware | AutoSketch: search | AutoSketch: search + measured benchmark | (reference: + 60 s per probe) |
|---|---|---|---|---|---|
| 50 | 0.758 s | 0.65 s | 0.00197 s | 212 s | 600 s |
| 400 | 6.82 s | 5.55 s | 0.0153 s | 1.7e+03 s | 4.8e+03 s |
| 800 | 14.7 s | 11.7 s | 0.031 s | 3.39e+03 s | 9.6e+03 s |

## Inputs

Costs and accuracy come from one saturation-study directory `DIR`, the same
inputs the planner reads (#174, #178, #179). The one these results used is
committed in `../autosketch-vs-asap-inputs/saturation/`, and the tables in
`../autosketch-vs-asap-inputs/tables/`:
- `DIR/out_grid_1e7_cost/`: the accuracy grid to N = 1e7, including the
  cost table's shape (Zipf 1.1 over 1e4 keys, Pareto a = 2), with merge
  curves for the lossy sketches;
- `DIR/out_1e9/`: the targeted points past 1e7 that the workload reads;
- `DIR/optimizer_cost/rqe_atomic_costs.json`: the cost table.

The tables from `export_autosketch_eval_table.py --synthetic` hold only the
workload: RQEs, streams, targets and data shapes.

## Reproduce

```sh
cargo build -p aqpbm-cli --release
# Accuracy grid with merge curves (parallel), then the targeted 1e9 points.
python3 scripts/study_saturation.py --phase accuracy --n-max 1e7 --seeds 3 \
    --jobs 48 --merge-shards-list 1,4,16,64 --out DIR/out_grid_1e7_cost
python3 scripts/study_saturation.py --phase accuracy --n-max 1e9 --seeds 3 \
    --jobs 16 --points-from POINTS.csv --out DIR/out_1e9
# Cost table, serially on a quiet machine.
python3 scripts/study_saturation.py --phase optimizer-cost --out DIR/optimizer_cost

# Workload tables and plan.tsv, then the runs.
python3 scripts/export_autosketch_eval_table.py --synthetic --out TABLES
# Or use the committed inputs: TABLES=rqe-optimizer/results/autosketch-vs-asap-inputs/tables,
# DIR=rqe-optimizer/results/autosketch-vs-asap-inputs/saturation.
SATURATION_DIR=DIR scripts/run_autosketch_vs_asap_synthetic.sh TABLES run
# AutoSketch's measured benchmark (serially, on an idle machine; writes
# benchmark_secs_measured into the results), then the figures.
python3 scripts/autosketch_benchmark_time.py --binary ./target/release/approxbench \
    --out OUT/autosketch-benchmark-times.json OUT/synthetic-*.json
scripts/run_autosketch_vs_asap_synthetic.sh TABLES plot
```

`--merge-shards-list` and `POINTS.csv` follow the workloads' queries: the
merge counts their KLL deployments can reach (`L / x`), and the points whose
items per instance exceed 1e7. These are the synthetic workload's (Zipf 1.1
over 1e4 keys, Pareto a = 2) and alibaba's quantiles (Pareto a = 3, up to
1.6e8 items).

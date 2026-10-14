# AutoSketch vs. ASAP with Hydra candidates and roll-ups

Results of ProjectASAP/ASAPQuery#777's synthetic evaluation (§6.3), rerun with
everything added since #138:

- the planner may serve a coarse RQE from a finer deployment by merging its
  groups (**roll-ups**, #190), and an **ablation** without them;
- **Hydra** grids (hydra-hll, hydra-univmon-cardinality, hydra-kll) are
  candidates for ASAP and PerQuery, with accuracy measured on the eval's own
  data (#193 design; #195–#202 measurement and optimizer);
- **multi-grouping** templates (`http` and `flows`, templates 11–17, #196),
  among them p99 latency `GROUP BY CUBE(region, service, endpoint)`
  (template 16, all 7 groupings), evaluated alone (**multi-grouping**) and
  added to the classic mixed set (**mixed + multi-grouping**);
- the KLL footprint is asap_sketchlib's real allocation (#192), and the cost
  table is re-measured;
- classic templates 7 (`sum by (job) (rate(data_total[w]))`) and 9
  (`topk(32, sum by (label_0) (rate(data_total[w])))`) are dropped, as
  ASAPQuery's engine would need per-series counter deltas to serve them
  (2026-10-13 rerun of the classic and all workloads; the multi-grouping
  results are unchanged).

Cost is billed by use (sketch-bench `docs/rqe_optimizer_cost_model.md`):
`w_cpu · AUC(CPU) + w_mem · AUC(memory)`, at two weight settings (CPU only, in
vCPU; AWS Fargate's per-vCPU and per-GiB prices, in $/hour). Every RQE has a
p95 accuracy target (0.05 in its family's own metric). An RQE with a coverage
rule is held to the worse of its smallest and largest covered group
(sketch-bench#189). Hydra is held to its measured worst error over the covered
groups, the worst over N and seeds.

## Methods

| Method | What it may use |
|---|---|
| ASAP | the MILP over every RQE jointly: shared deployments, roll-ups, Hydra grids |
| ASAP (no roll-ups) | the same MILP, each RQE only at its own grouping (or a Hydra grid): the roll-up ablation |
| PerQuery-CostAware | the same MILP without sharing (one deployment per RQE, no roll-ups) |
| AutoSketch-Adapted | fixed configs chosen by memory (NSDI '24); no Hydra (#159) |

## Results

Cost of each method's cheapest plan (no latency bound), absolute:

| Workload | RQEs | Weights | ASAP | ASAP (no roll-ups) | PerQuery | AutoSketch | RQEs rolled up | Deployments (with / without roll-ups) |
|---|---|---|---|---|---|---|---|---|
| mixed (classic) | 42 | CPU (vCPU) | 2.38 | 2.38 | 7.77 | 2,739 | 0 | 6 / 6 |
| mixed (classic) | 42 | Fargate ($/h) | 0.170 | 0.170 | 0.623 | 136 | 0 | 6 / 6 |
| mixed, r = 8 | 76 | CPU | 2.47 | 2.47 | 13.6 | 3,284 | 0 | 6 / 6 |
| mixed, m = 8 | 336 | CPU | 19.1 | 19.1 | 62.2 | 21,916 | 0 | 48 / 48 |
| mixed, m = 16 | 672 | CPU | 38.1 | 38.1 | 124 | 43,831 | 0 | 96 / 96 |
| mixed, m = 16 | 672 | Fargate | 2.73 | 2.73 | 9.97 | 2,175 | 0 | 96 / 96 |
| multi-grouping | 62 | CPU | 0.221 | 0.551 | 1.07 | 10.2 | 52 | 5 / 26 |
| multi-grouping | 62 | Fargate | 0.0137 | 0.0339 | 0.0597 | 0.430 | 52 | 5 / 26 |
| multi-grouping, r = 8 | 124 | CPU | 0.242 | 0.573 | 2.04 | 12.3 | 104 | 5 / 26 |
| multi-grouping, m = 8 | 496 | CPU | 1.77 | 4.41 | 8.55 | 81.7 | 416 | 40 / 208 |
| multi-grouping, m = 16 | 992 | CPU | 3.53 | 8.81 | 17.1 | 163 | 832 | 80 / 416 |
| multi-grouping, m = 16 | 992 | Fargate | 0.218 | 0.542 | 0.955 | 6.87 | 832 | 80 / 416 |
| mixed + multi-grouping | 104 | CPU | 2.60 | 2.93 | 8.84 | 2,750 | 52 | 11 / 32 |
| mixed + multi-grouping | 104 | Fargate | 0.184 | 0.204 | 0.683 | 136 | 52 | 11 / 32 |
| mixed + multi-grouping, r = 8 | 200 | CPU | 2.71 | 3.04 | 15.6 | 3,296 | 104 | 11 / 32 |
| mixed + multi-grouping, m = 8 | 832 | CPU | 20.8 | 23.5 | 70.7 | 21,997 | 416 | 88 / 256 |
| mixed + multi-grouping, m = 16 | 1664 | CPU | 41.7 | 47.0 | 141 | 43,995 | 832 | 176 / 512 |
| mixed + multi-grouping, m = 16 | 1664 | Fargate | 2.95 | 3.27 | 10.9 | 2,182 | 832 | 176 / 512 |

Every workload: 0 RQEs dropped, 0 sanity violations (ASAP ≤ ASAP without
roll-ups ≤ PerQuery, and ASAP ≤ AutoSketch). `summary_synthetic.md` has
every frontier point; `summary_sla.md` every SLA.

Planning time (CPU weights): ASAP (candidates + MILP) vs. AutoSketch (search
plus its measured benchmark, approxbench accuracy runs at 1e8 items):

| Workload | RQEs | ASAP | PerQuery | AutoSketch |
|---|---|---|---|---|
| mixed (classic) | 42 | 0.44 s | 0.49 s | 0.002 s + 212 s |
| mixed, m = 16 | 672 | 8.1 s | 8.9 s | 0.027 s + 3,394 s |
| multi-grouping | 62 | 0.41 s | 0.22 s | 0.002 s + 269 s |
| multi-grouping, m = 16 | 992 | 7.0 s | 4.1 s | 0.037 s + 4,309 s |
| mixed + multi-grouping | 104 | 0.94 s | 0.64 s | 0.005 s + 481 s |
| mixed + multi-grouping, m = 16 | 1664 | 15.6 s | 13.0 s | 0.137 s + 7,702 s |

### Roll-ups

On the **multi-grouping** set, ASAP serves 52 of 62 RQEs from a finer
deployment, cutting its deployments from 26 to 5 and its cost by **60%**
(0.551 → 0.221 vCPU; 0.0339 → 0.0137 $/h), and by the same share at r = 8,
m = 8 and m = 16. The cube (template 16) is the clearest case: one KLL
deployment at (region, service, endpoint) answers all 14 p99 RQEs. Every
deployment ingests the whole stream, so cost grows with the number of
groupings kept. A KLL or DDSketch merge summarizes the pooled data of the
merged groups, so a roll-up answers a coarse group's p99, not a quantile of
quantiles (KLL reads its measured merge curve).

On **mixed + multi-grouping**, roll-ups save 11% (2.93 → 2.60 vCPU, 32 → 11
deployments): the classic templates, whose streams have one grouping each,
are most of the cost. On classic alone, roll-ups never apply and the two ASAP
lines coincide. ASAP's ~3× advantage over PerQuery on classic (7.77 vs. 2.38
vCPU; 1.8–3.3× at every SLA) comes from sharing deployments across RQEs at
the same grouping.

### Hydra

Hydra grids are candidates for every eligible RQE, and many are eligible
(e.g. hydra-hll at W ≥ 4096 for flows at ≥ 5% coverage; hydra-kll by service
at W = 16384 for every group). **Neither ASAP nor PerQuery chooses a Hydra
grid in any workload, weight setting, bound or SLA** (`hydra_rqes = 0`
throughout). A Hydra insert fans out to every label subset in every row and
costs 1.9 µs per record (flows, 3 labels) to 5.1 µs (http, 4 labels), against
3.85 ns for a per-group HLL insert; at the eval's rates that is 8–11 vCPU of
ingest for one grid, against ASAP's 0.05–1.9 vCPU for the whole stream. The
only near case is flows under Fargate weights, where a Hydra-only plan costs
1.8× ASAP's (it uses 4.4× less memory). A check on real traces (Alibaba 2022,
Google 2011; not committed) agreed: Hydra meets 0.05 only for groups holding
≥ 1–5% of records, and per-group sketches that grow with n use less memory
than the grid at the W that needs.

hydra-univmon-cardinality reads ≈ 100% error at N ≥ 1e6 at every W
(8 layers × a small heap saturate at ~1e6 distinct values per cell), so it is
ruled out by accuracy; cardinality's Hydra candidate is hydra-hll.

## Figures

- `fig_frontier.png`: version 1, cost vs. query latency per workload and
  weight setting; frontiers swept over latency bounds (ASAP, ASAP without
  roll-ups, PerQuery as lines; AutoSketch, which ignores latency, as a point).
- `fig_cost_vs_sla.png`: version 2, each method's cost at batch-latency SLAs
  {100, 300, 1000, 3000, 10000} ms (AutoSketch: ● meets the SLA, × misses).
  With the multi-grouping templates the tightest feasible bound is 490 ms
  (the per-(service, endpoint) queries), so the 100 and 300 ms SLAs have no
  plan.
- `fig_planning_time.png`: planning time vs. RQEs (multi-grouping set).
- `fig_rollup_ablation.png`: ASAP vs. ASAP without roll-ups, cheapest plan per
  workload (CPU only, log scale).

## Plans

`plans/<table>.plans.json.gz`: each workload's chosen plans, from a rerun with
the plans kept (`--runs 1`, without `--no-chosen`; costs are deterministic, so
they match the results above). `plans` lists, for each method and weight
setting, the cheapest plan (`kind: "cheapest"`) and the plan at each feasible
SLA (`kind: "sla"`, `sla_ms`), with its cost, latency and deployments.
`chosen` has one entry per RQE:

| Field | Meaning |
|---|---|
| `rqe` | the RQE's id (table, query, kind, range) |
| `sketch`, `params` | the deployment's sketch and config |
| `window_secs`, `slide_secs` | the deployment's window and slide |
| `merged_shards` | windows the query merges (lookback / window) |
| `deployment` | the deployment's index; RQEs with one index share it |
| `deployment_grouping` | present for a roll-up or Hydra grid: the deployment's (finer) grouping |
| `ingest_workers`, `latency_ms` | ingest workers and the RQE's chain latency |
| `asap_accuracy`, `accuracy_source` | estimated error and where it was read |
| `covers_share` | the RQE's coverage rule, if any |

```sh
zcat plans/synthetic-templatesmultigroup-shared1.plans.json.gz | jq '.plans[]
  | select(.kind == "cheapest" and .method == "asap" and .weights == "cpu") | .chosen'
```

## Inputs

- `inputs/saturation/optimizer_cost/rqe_atomic_costs.json`: the cost table,
  56 rows, measured serially on idle clnode138 (`study_saturation.py --phase
  optimizer-cost --seeds 3 --hydra-variants hydra-hll,hydra-kll`), including
  Hydra rows per dataset (`measured_at.dataset`) and #192's KLL footprint.
- `inputs/saturation/hydra_saturation.csv`: the Hydra study (`--phase hydra`,
  #202), 6,779 runs on 4 CloudLab nodes; hydra-kll on `hydra_http_latency`
  rerun with per-service latency scales (×1–10 log-uniform), so groups differ
  in distribution as they do in practice.
- The accuracy curves (`out_grid_1e7_cost/`, `out_1e9/`) are the committed
  #138 study's, in `../autosketch-vs-asap-inputs/saturation/` (#194).
- `inputs/tables/`: the workloads, `export_autosketch_eval_table.py
  --synthetic` (classic, multigroup and all × r ∈ {1, 8} × m ∈ {1, 8, 16}).

## Reproduce

```sh
cargo build --release -p aqpbm-cli -p rqe-optimizer --bins --examples
DIR=rqe-optimizer/results/autosketch-vs-asap-hydra
# saturation dir = the #138 curves + this cost table and Hydra study:
SAT=$(mktemp -d); cp -r rqe-optimizer/results/autosketch-vs-asap-inputs/saturation/out_* $SAT/
cp -r $DIR/inputs/saturation/* $SAT/
while IFS=$'\t' read -r table target result; do
  target/release/examples/autosketch_vs_asap synthetic --table $DIR/inputs/tables/$table \
      --target $target --runs 3 --no-chosen --saturation-dir $SAT --out $DIR/$result
done < $DIR/inputs/tables/plan.tsv
python3 scripts/autosketch_benchmark_time.py --binary target/release/approxbench \
    --out $DIR/autosketch-benchmark-times.json $DIR/synthetic-*-tp95.json
python3 scripts/plot_autosketch_vs_asap_synthetic.py $DIR
python3 scripts/plot_autosketch_vs_asap_sla.py $DIR $DIR/synthetic-*-tp95.json
```

Runs: Xeon E5-2683 v3, 56 cores, `--runs 3`, runner from sketch-bench
`hydra/eval-integration` at 44ac160. Classic: one large table per CloudLab node
(clnode178, 167; the small ones serially on clnode138), 2026-10-10.
Multigroup and all (with the cube template): serially on one idle node,
2026-10-11.

Rerun without templates 7 and 9 (2026-10-13), runner at sketch-bench
eval/drop-group-rate-templates: all serially on idle clnode109; classic
m = 16 on clnode167, m = 8 on clnode155, the small ones serially on
clnode138; plans (`--runs 1`, kept) on clnode138 and clnode155. The
multigroup tables are unchanged, so their results stand (a rerun of
multigroup-shared1 reproduces its costs and plans exactly). AutoSketch's
benchmark times are reused: the dropped RQEs used exact accumulators or a
top-k config already probed on the same metric, so every probe was already
measured and each workload's benchmark time is unchanged.

## Caveats

- The cost model charges a per-group sketch its insert only, not routing a
  sample to its group's sketch (~100 ns per record per deployment on the
  real traces). Adding it raises ASAP's flows ingest from 0.11 to ~1.3 vCPU,
  still far below a Hydra grid's 8.2; the Hydra conclusion holds.
- Per-group sketch memory is asap_sketchlib's full allocation from
  construction (#192); sketches that grow with n would favour per-group
  sketches further.
- Hydra datasets: no flows DDoS burst (datagen can't tie a source set to a
  subnet), uniform proto, smaller UnivMon cells (3×256, 8 layers, heap 64).
- Template 12 groups by (dst_subnet, proto) rather than (dst_subnet,
  dst_port): answering 1e6 groups every minute took ~49 s and set every
  plan's batch latency above the largest SLA.

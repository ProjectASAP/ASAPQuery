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
  results are unchanged);
- a **denser config grid** (2026-10-14, #213): KLL `k` ∈ {25, 50, 100, 200,
  400, 800} (was 50/200/800), DDSketch α adds 0.1, and Hydra grids add
  W = 65536 (hydra-hll, hydra-kll). Only the new configs were measured and
  appended to the curves, the cost table and the Hydra study. HLL stays at
  `lg_k` 12/14/16, the precisions asap_sketchlib builds;
- **instant RQEs read one scrape** (2026-10-14, #215): `sum by (job)` and
  `quantile by (job)` look back 5 ms, one sample per series (N = 1e3 per
  job), instead of 1 s (2e5), as a PromQL instant query does. Rerun of the
  classic and all workloads; the multi-grouping tables are unchanged, so
  their results stand.

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
| mixed (classic) | 42 | CPU (vCPU) | 2.71 | 2.71 | 7.67 | 2,701 | 0 | 7 / 7 |
| mixed (classic) | 42 | Fargate ($/h) | 0.163 | 0.163 | 0.487 | 127 | 0 | 7 / 7 |
| mixed, r = 8 | 76 | CPU | 2.8 | 2.8 | 13.5 | 3,239 | 0 | 7 / 7 |
| mixed, m = 8 | 336 | CPU | 21.7 | 21.7 | 61.3 | 21,610 | 0 | 56 / 56 |
| mixed, m = 16 | 672 | CPU | 43.4 | 43.4 | 123 | 43,221 | 0 | 112 / 112 |
| mixed, m = 16 | 672 | Fargate | 2.61 | 2.61 | 7.78 | 2,035 | 0 | 112 / 112 |
| multi-grouping | 62 | CPU | 0.22 | 0.55 | 1.07 | 9.65 | 52 | 5 / 26 |
| multi-grouping | 62 | Fargate | 0.0134 | 0.0333 | 0.0589 | 0.406 | 52 | 5 / 26 |
| multi-grouping, r = 8 | 124 | CPU | 0.241 | 0.572 | 2.04 | 11.6 | 104 | 5 / 26 |
| multi-grouping, m = 8 | 496 | CPU | 1.76 | 4.4 | 8.54 | 77.2 | 416 | 40 / 208 |
| multi-grouping, m = 16 | 992 | CPU | 3.52 | 8.8 | 17.1 | 154 | 832 | 80 / 416 |
| multi-grouping, m = 16 | 992 | Fargate | 0.215 | 0.532 | 0.942 | 6.5 | 832 | 80 / 416 |
| mixed + multi-grouping | 104 | CPU | 2.93 | 3.26 | 8.74 | 2,711 | 52 | 12 / 33 |
| mixed + multi-grouping | 104 | Fargate | 0.177 | 0.197 | 0.545 | 128 | 52 | 12 / 33 |
| mixed + multi-grouping, r = 8 | 200 | CPU | 3.04 | 3.37 | 15.5 | 3,250 | 104 | 12 / 33 |
| mixed + multi-grouping, m = 8 | 832 | CPU | 23.5 | 26.1 | 69.9 | 21,688 | 416 | 96 / 264 |
| mixed + multi-grouping, m = 16 | 1664 | CPU | 46.9 | 52.2 | 140 | 43,375 | 832 | 192 / 528 |
| mixed + multi-grouping, m = 16 | 1664 | Fargate | 2.83 | 3.15 | 8.73 | 2,042 | 832 | 192 / 528 |

Every workload: 0 RQEs dropped, 0 sanity violations (ASAP ≤ ASAP without
roll-ups ≤ PerQuery, and ASAP ≤ AutoSketch). `summary_synthetic.md` has
every frontier point; `summary_sla.md` every SLA.

Planning time (CPU weights): ASAP (candidates + MILP) vs. AutoSketch (search
plus its measured benchmark, approxbench accuracy runs at 1e8 items):

| Workload | RQEs | ASAP | PerQuery | AutoSketch |
|---|---|---|---|---|
| mixed | 42 | 0.82 s | 1.8 s | 0.0006 s + 169 s |
| mixed, m = 16 | 672 | 13.3 s | 28.4 s | 0.01 s + 2,710 s |
| multi-grouping | 62 | 0.52 s | 0.28 s | 0.0023 s + 227 s |
| multi-grouping, m = 16 | 992 | 8.5 s | 5 s | 0.037 s + 3,625 s |
| mixed + multi-grouping | 104 | 2.9 s | 5.0 s | 0.0042 s + 396 s |
| mixed + multi-grouping, m = 16 | 1664 | 17.8 s | 33 s | 0.026 s + 6,335 s |

The denser grid gives ASAP and PerQuery more candidates (6 KLL and 5 DDSketch
configs instead of 3 and 4), so their MILPs are larger than before (mixed +
multi-grouping: 0.94 → 2.1 s). The reruns also spread over 4 nodes and ran
beside each other's shares, so planning times are not comparable across reruns
to better than about 2×; within one rerun they are. AutoSketch reaches a
feasible config sooner (15 probes instead of 19 on mixed + multi-grouping), so
its measured benchmark is shorter (481 → 396 s).

**What the denser grid changed** (95%, cheapest plans): CPU cost falls 0–1.4%
(mixed 2.38 → 2.35 vCPU, mixed + multi-grouping 2.60 → 2.57), since insert
CPU per item is flat across configs (KLL ≈ 30 ns, DDSketch ≈ 19 ns at every
size). Memory-weighted cost falls more, as plans pick a smaller KLL or
DDSketch closer to the target: Fargate 0.170 → 0.149 $/h on mixed (−13%),
0.184 → 0.162 on mixed + multi-grouping (−12%), 0.0137 → 0.0134 on
multi-grouping. No conclusion changes.

**What one-scrape instant RQEs changed** (95%, cheapest plans): ASAP's CPU
rises ~15% on every mixed and mixed + multi-grouping workload (mixed
2.35 → 2.71 vCPU, mixed + multi-grouping 2.57 → 2.93; Fargate +9%), with one
more deployment per metric. The instant `sum by (job)` now reads a 5 ms
window, so `sum by (job) (sum_over_time)` no longer shares its 1 s exact-sum
deployment and gets its own, ingesting the stream again (~0.36 vCPU per
metric). PerQuery and AutoSketch, which never shared it, are unchanged, as
are AutoSketch's probes and benchmark times, and the tightest feasible
latency bounds. No conclusion changes: ASAP stays 2.8–3.0× below PerQuery on
mixed and ~1,000× below AutoSketch.

### Roll-ups

On the **multi-grouping** set, ASAP serves 52 of 62 RQEs from a finer
deployment, cutting its deployments from 26 to 5 and its cost by **60%**
(0.550 → 0.220 vCPU; 0.0333 → 0.0134 $/h), and by the same share at r = 8,
m = 8 and m = 16. The cube (template 16) is the clearest case: one KLL
deployment at (region, service, endpoint) answers all 14 p99 RQEs. Every
deployment ingests the whole stream, so cost grows with the number of
groupings kept. A KLL or DDSketch merge summarizes the pooled data of the
merged groups, so a roll-up answers a coarse group's p99, not a quantile of
quantiles (KLL reads its measured merge curve).

On **mixed + multi-grouping**, roll-ups save 10% (3.26 → 2.93 vCPU, 33 → 12
deployments): the classic templates, whose streams have one grouping each,
are most of the cost. On classic alone, roll-ups never apply and the two ASAP
lines coincide. ASAP's ~2.8× advantage over PerQuery on classic (7.67 vs. 2.71
vCPU; 1.7–3.0× at every SLA) comes from sharing deployments across RQEs at
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

W = 65536 (#213) widens what Hydra is accurate for: on groups holding ≥ 1% of
the records, the worst error at 1e7 records drops from 0.283 to 0.039
(hydra-hll, `http`), 0.074 to 0.021 (hydra-hll, `flows`) and 0.054 to 0.017
(hydra-kll, unmerged; 0.056 to 0.017 over every merge count). But it is the
most expensive grid on every axis (3.3 GB per hydra-hll grid, 6.2 µs per
insert on `http`), and it is never chosen either.

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

- `inputs/saturation/optimizer_cost/rqe_atomic_costs.json`: the cost table, 63
  rows (56, plus #213's KLL `k` 25/100/400, DDSketch α 0.1 and Hydra W = 65536
  rows), measured serially on idle clnode138 (`study_saturation.py --phase
  optimizer-cost --seeds 3 --hydra-variants hydra-hll,hydra-kll`), including
  Hydra rows per dataset (`measured_at.dataset`) and #192's KLL footprint.
- `inputs/saturation/hydra_saturation.csv`: the Hydra study (`--phase hydra`,
  #202), 7,778 runs on 4 CloudLab nodes (#213 added 999 at W = 65536);
  hydra-kll on `hydra_http_latency` rerun with per-service latency scales
  (×1–10 log-uniform), so groups differ in distribution as they do in
  practice.
- The accuracy curves (`out_grid_1e7_cost/`, `out_1e9/`) are the committed
  #138 study's, in `../autosketch-vs-asap-inputs/saturation/` (#194), with
  #213's new KLL and DDSketch configs appended.
- `inputs/tables/`: the workloads, `export_autosketch_eval_table.py --synthetic
  --keep-duplicate-queries` (classic, multigroup and all × r ∈ {1, 8} × m ∈ {1, 8, 16}).

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

Runs: Xeon E5-2683 v3, 56 cores, `--runs 3`. First run (superseded by the
reruns below): runner from sketch-bench `hydra/eval-integration` at 44ac160.
Classic: one large table per CloudLab node (clnode178, 167; the small ones
serially on clnode138), 2026-10-10. Multigroup and all (with the cube
template): serially on one idle node, 2026-10-11.

Rerun without templates 7 and 9 (2026-10-13), runner at sketch-bench
eval/drop-group-rate-templates: all serially on idle clnode109; classic
m = 16 on clnode167, m = 8 on clnode155, the small ones serially on
clnode138; plans (`--runs 1`, kept) on clnode138 and clnode155. The
multigroup tables are unchanged, so their results stand (a rerun of
multigroup-shared1 reproduces its costs and plans exactly). AutoSketch's
benchmark times are reused: the dropped RQEs used exact accumulators or a
top-k config already probed on the same metric, so every probe was already
measured and each workload's benchmark time is unchanged.

Denser grid (2026-10-14, #213, runner at sketch-bench eval/dense-grid):
multi-grouping on the 4 CloudLab nodes clnode109/155/167/178, then classic and
all on #210's tables on the same 4 nodes, each node running its share
serially. Plans: `--runs 1`, without `--no-chosen`, extracted with
`scripts/extract_autosketch_plans.py`.

One-scrape instant RQEs (2026-10-14, #215, sketch-bench main at acc2db1): the
8 classic and all tables re-exported (`--keep-duplicate-queries`; only the 6
instant RQEs per metric change) and rerun serially on a 16-core desktop, not
an idle cluster node, with their plans and the 90%/99% runs of
`../autosketch-vs-asap-targets/`. The multi-grouping JSONs and plans are the
denser-grid rerun's. AutoSketch's probes are unchanged, so every benchmark time
is reused from `autosketch-benchmark-times.json`.

The JSONs, figures and plans here are from those last two reruns. Planning
times of the mixed and mixed + multi-grouping rows come from the desktop, those
of the multi-grouping rows from the cluster, so compare them across rows only
to about 2×.

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

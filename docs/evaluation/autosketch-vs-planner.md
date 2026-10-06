# Evaluation plan: AutoSketch vs. the ASAPQuery planner (paper §6.3)

Status: revised 2026-10-06 to sketch-bench #145's cost model and capabilities (#144). The evaluation code (#138–#141) is reworked onto it before the runs; earlier results are obsolete. The design decisions are settled in §9.

## 1. Question

Given the same measured sketch costs and the same workload of repeating query
expressions (RQEs), how much cheaper is the plan from ASAPQuery's planner than
the plans from AutoSketch, and how long does each take to plan?

AutoSketch ([NSDI '24](https://www.usenix.org/system/files/nsdi24-sun.pdf),
Algorithm 4) differs from ASAPQuery's planner in four ways that matter here:

| | AutoSketch | ASAPQuery planner |
| --- | --- | --- |
| Unit of optimization | One query at a time | The whole batch of RQEs, jointly |
| Repetition over time | Not modeled | Lookback `S` and repeat interval `T` drive window/slide choice |
| Sharing | None: each query gets its own sketch | One deployment may serve several compatible RQEs |
| Constraints | Accuracy only | Accuracy and per-RQE query latency |
| Objective | Resource use (memory) | Weighted CPU + memory: `w_cpu · CPU + w_mem · memory` |

So the comparison invokes AutoSketch **once per RQE** (one QE at one repeat
interval) and sums the results; the planner is invoked once for the batch.

## 2. What already exists

| Piece | Where | Status |
| --- | --- | --- |
| AutoSketch Algorithm 4 adaptation (LHS seeds, feasibility-directed width/depth neighbor search, pruning) | ASAPQuery-backend `data_plane/examples/autosketch_comparison.rs` ([#547](https://github.com/ProjectASAP/ASAPQuery-backend/pull/547)) | Merged. CMS/Count Sketch/Bloom only; hardcoded CMS grid; executes sketches to measure accuracy. |
| Earlier protocol (E1–E3, end-to-end execution) | ASAPQuery-backend `docs/evaluation/autosketch-comparison.md` ([#545](https://github.com/ProjectASAP/ASAPQuery-backend/pull/545)) | Merged. Execution-based; this plan is planner-level and uses estimated costs instead. |
| Top-K dashboard comparison | ASAPQuery-backend [#602](https://github.com/ProjectASAP/ASAPQuery-backend/pull/602) | Closed, not merged. |
| RQE deployment MILP (HiGHS): candidates `(capability, config, labels, x, y)`, sharing, latency bounds | sketch-bench `rqe-optimizer/` ([#129](https://github.com/ProjectASAP/sketch-bench/pull/129)); per-phase cost model and weighted objective [#145](https://github.com/ProjectASAP/sketch-bench/pull/145); exact accumulators and top-k families [#144](https://github.com/ProjectASAP/sketch-bench/pull/144) | Merged. **This is the planner we evaluate.** |
| Measured per-operation costs (`AtomicCostEntry`: memory/instance, insert/merge/query CPU, accuracy) | sketch-bench `scripts/export_rqe_optimizer_costs.sh` | Merged. **The cost table this evaluation reads** (§6). Each row records its measurement conditions (`measured_at`, #155) and accuracy after merging (#154); a sweep over items per instance is #157. |
| Saturation study: error vs. `N`, `N_sat`, cost per config and shape | sketch-bench [#130](https://github.com/ProjectASAP/sketch-bench/pull/130) | Merged. Background for how error and cost depend on `N`; no longer read by the evaluation. |
| Accuracy after merging `m` shards | sketch-bench [#131](https://github.com/ProjectASAP/sketch-bench/pull/131), carried into the cost table by [#154](https://github.com/ProjectASAP/sketch-bench/pull/154) | Merged. Read when a deployment merges `m = S/x > 1` windows. |
| Moving the MILP into ASAPQuery's planner | ASAPQuery `asap-planner-rs/src/optimizer/` (Milind; related: [#776](https://github.com/ProjectASAP/ASAPQuery/pull/776), [#725](https://github.com/ProjectASAP/ASAPQuery/pull/725)) | Out of scope: the evaluation uses sketch-bench `rqe-optimizer` and is not rerun on `asap-planner-rs`. |

AutoSketch-Adapted is implemented in sketch-bench `rqe-optimizer/src/autosketch.rs` (#135).

## 3. Methods compared

All methods read the same `AtomicCostTable`, the same RQEs and the same
label-set cardinalities and arrival rates, and are scored by the same cost
function (§4).

1. **ASAP** — sketch-bench `rqe-optimizer` MILP over the whole batch, with
   accuracy and latency constraints, minimizing the §4 objective at each weight
   setting.
2. **AutoSketch-Adapted** — Algorithm 4 run independently per RQE:
   - search space: the measured configs of the RQE's capability families
     (`Capability::families()`);
   - constraint: the RQE's accuracy tolerance must hold on **every** benchmark
     input (§6, "Benchmark input"), as in the paper's §5.2;
   - objective: per-instance memory (AutoSketch's register-memory objective),
     tie-break on insert CPU;
   - window adapter: one sliding sketch per query, `x = S`, `y = T` (if
     `S % T != 0`, `y = gcd(S, T)`), so each evaluation reads one instance and
     merges nothing;
   - no sharing: every RQE gets its own deployment, even when two RQEs pick an
     identical one, so ingest and memory are paid per RQE;
   - latency is ignored during search, then checked after.
3. **PerQuery-CostAware** (strawman) — the ASAP MILP solved on each RQE alone,
   with the same accuracy and latency requirements, and the results summed. It uses the same objective
   and window choices as ASAP but no batching or sharing. ASAP vs. this ablation
   isolates the batch/sharing benefit; this ablation vs. AutoSketch-Adapted
   isolates the objective/window benefit.
Only AutoSketch-Adapted ignores latency; PerQuery-CostAware must meet the same
requirements as ASAP, so every point in the cost–latency figure except
AutoSketch's is a feasible plan.

A "fewest plans" strawman (minimize the number of deployments, then cost) was
considered and dropped (decided 2026-10-05).

AutoSketch-Adapted is a planner baseline, not a reproduction of the P4
compiler: stage/page/ALU constraints are dropped. Its accuracy probes read
sketch-bench measurements instead of running a benchmark inside the search, but
the cost of running those benchmarks is charged to its planning time (§9 Q3).

## 4. Cost model

The cost model is sketch-bench `rqe-optimizer`'s (#145), so every method is
scored by the function ASAP optimizes. Disk is excluded. Units: CPU in vCPU
(CPU-seconds per second, the mean over time), memory in GiB.

A deployment `D` groups by labels `G` with window `x` and slide `y`; RQE `r`
has lookback `S` and repeat interval `T`. Per-instance costs are measured
(§6): memory `m`, insert `c_ins`, merge `c_mrg`, query `c_qry`.
`λ = card(series) / scrape interval`.

| Phase | CPU (vCPU) | Memory (bytes) |
| --- | --- | --- |
| Ingest | `λ · (x/y) · c_ins` | `card(G) · m · x/y` (open windows) |
| Merge | `card(G) · (S/x − 1) · c_mrg / T` | `card(G) · m` (one accumulator per group), 0 when `S = x` |
| Query | `card(G) · c_qry / T` | `card(G) · 8 B`; top-k `k · 16 B` |
| Storage | 0 | `card(G) · m · ((max S − x)/y + 1)` (closed windows) |

Ingest and storage are paid once per active deployment; merge and query once
per RQE it serves. Memory sums every term, as if every query evaluates at once.

**Objective** — `w_cpu · CPU + w_mem · Memory_GiB`, summed over the plan:

1. **CPU only**, `(w_cpu, w_mem) = (1, 0)`: the first run. Memory is still
   reported.
2. **Fargate prices**, `w_cpu = 0.0405` $/vCPU-hour and `w_mem = 0.00445`
   $/GB-hour (AWS Fargate, us-east-1, Linux/x86, 2026-10-06), so the objective
   is in $/hour. CPU costs about 9× memory per unit; serverless pricing charges
   exactly these two resources, which is why it fits the model.

CPU is the mean, i.e. the area under the CPU-over-time curve: plans are sized
for average load, not bursts. Peak-provisioned pricing (buy machines for the
peak CPU) and per-instance-family EC2 pricing were considered and dropped
(2026-10-06).

**Latency** — per-RQE estimate:
`card(G) · (c_qry + (S/x − 1) · c_mrg)`, the evaluation's CPU time run
serially on one core (an upper bound; parallel execution across instances
would reduce it proportionally). `c_qry` is one query of one instance: one
value of a sum or increase accumulator, one top-k list, or one quantile, so a
template asking `n` quantiles is `n` RQEs.

**Units and per-operation costs.** CPU is CPU time (user + system), in
core-seconds, measured by sketch-bench:
- `c_ins` (`insert_cpu_secs`): the insert phase's CPU ÷ N, in CPU-seconds per item;
- `c_mrg` (`merge_cpu_secs`): merging 16 shards ÷ 15, in CPU-seconds per merge;
- `c_qry` (`query_cpu_secs`): the query phase's CPU ÷ the number of queries in it, per call
  (a top-k heap dump is repeated per pass so it is timed above the clock's
  floor, #151);
- `m` (`mem_bytes_per_instance`): the self-reported bytes per instance, not process RSS. Top-k counts
  its heap and exact accumulators their hash table (#151). Exact accumulators
  are priced per group: memory and merge are divided by the group count they
  were measured at.

Loads are reported in vCPU (core-seconds per second) and totals in CPU-hours.

## 5. Constraints

**Accuracy target**: one of three strictness levels, each with its own target
per capability. The default level matches the `traces` targets from ASAPQuery's
dataset analysis. The synthetic workload sweeps all three; `traces` uses its
fitted targets.

| Level | Quantile (rank error) | TopK (precision@k) |
| --- | --- | --- |
| loose | ≤ 0.02 | ≥ 0.90 |
| **default** | **≤ 0.01** | **≥ 0.95** |
| strict | ≤ 0.005 | ≥ 0.99 |

Sum and increase are served by exact accumulators, so their error is 0 and
every level is met. Strictness therefore matters only for quantile and top-k
RQEs. No template asks for cardinality.

A deployment that merges `m = S/x` windows must meet the target at both
measured merge counts bracketing `m` (count 1 is the single instance), as
`rqe-optimizer` checks it (#154).

An earlier version mapped one percentage `p` to error ≤ `1 − p` for every
capability. It was dropped (2026-10-05): at 95% it allowed quantile rank error
0.05, so a p99 query could return the p94 value.

**Latency** — one absolute SLA applies to every RQE, swept over
{0.01, 0.1, 1, 10} ms and no limit.
- An RQE that no method can meet at a given SLA is excluded from every method
  at that SLA and reported by ID. Costs at different SLAs therefore cover
  different RQE sets; compare methods only at one SLA.
- ASAP and PerQuery-CostAware must meet the SLA.
- AutoSketch-Adapted ignores it. Its violations are counted, and its cost is
  shown for those points but marked as infeasible.

An earlier version set `L_r = α × the fastest latency of r`. It was dropped:
on the dropped `example` workload, α = 2 forced plans with no merging at 40× the unconstrained
cost.

## 6. Workloads

Two workloads, decided 2026-10-05. The earlier `example` (8 RQEs from
`small_problem`) and `scaling` (random RQE batches) workloads were dropped.
Planning-time scaling is now the replica dimension of the synthetic workload
grid.

| ID | Description | Purpose |
| --- | --- | --- |
| `synthetic` | Synthetic PromQL workload: the 10 queries in "Synthetic workload" below, over Zipf data. Main figure. | Cost–latency trade-off across data and requirements |
| `traces` | Real-trace RQEs, one workload per dataset: Alibaba 2022, BOOM and Google 2011. Taken from `asap-tools/dataset-analysis/results/skew_summary.csv` ([#746](https://github.com/ProjectASAP/ASAPQuery/pull/746)): each row's `range_s` is `S` and its `step_s` is `T`. Data parameters and accuracy targets are fit over each whole trace. | Appendix: real-trace results |

### Synthetic workload

The synthetic workload is the paper's main experiment. It builds many
workloads from a fixed set of PromQL query templates by sweeping the workload
dimensions below, and runs every planning baseline on each workload.

#### Data model and scale

The data model is fixed: the comparison varies the queries and requirements,
not the data. There is one metric, `data`, with two labels:

| Label | Cardinality | Role |
|---|---|---|
| `label_0` | **C = 1e4** | Identifies the series (one series per value). The top-k key |
| `job` | **J = 10** | Coarse grouping: every series belongs to one job, so each job has `C/J` = 1,000 series |

| Quantity | Value |
|---|---|
| Series | C = 10,000 |
| Samples per series | 200 per second (one every 5 ms) |
| Stream rate `λ` | 2e6 samples/s; 2e5 per job |
| Values | Zipf s = 1.1 over 10,000 keys: the cost table's data (sketch-bench `export_rqe_optimizer_costs.sh` with `CARDINALITY=10000`). Quantile sketches see the Zipf ranks as values; a top-k key's total over a window follows the same Zipf law |
| Lookback windows `W` | {15m, 1h, 6h, 24h}: at least 15 minutes, so every per-series summary sees at least 1e5 items |
| Repeat interval `T` | 1 m for temporal templates; spatial templates read and repeat every 1 s |

**Summaries and their input size.** Every deployment keeps one summary per
group of its grouping labels (one per window). The number of items one summary
ingests decides whether a sketch is worth it:

| Grouping | Summaries per window | Items per summary | Kind |
|---|---|---|---|
| per series, temporal (templates 4, 5, 6, 10; D1, D5) | C = 1e4 | `200 · S`: 1.8e5 (15m) to 1.7e7 (24h) | exact accumulator or quantile sketch |
| `by (job)`, spatial, `S` = 1 s (1, 3; D2) | J = 10 | `2e5 · S` = 2e5 | exact accumulator or quantile sketch |
| `by (job)`, temporal (7, 8; D3) | J = 10 | 1.8e8 to 1.7e10 | exact accumulator |
| top-k over `label_0` (2, 9; D4) | 1 | `λ · S`: 1.8e9 to 1.7e11, over 1e4 keys | top-k sketch (CMS-heap) |

The cost table is measured at 1e5 to 1e8 items per instance (sketch-bench
#157). Exact accumulators keep constant state and a constant per-item cost, so
their size does not matter. Every quantile sketch falls inside the measured
range. Top-k sketches exceed it and read the 1e8 measurement: CMS-heap's
per-item CPU and memory do not depend on `N` (sketch-bench #130), and Zipf
s = 1.1 keys have saturated by then.

**Modeling choice for spatial templates.** A spatial template evaluates every
1 s over every sample of the last second (`S = T = 1 s`). PromQL's instant
semantics would read only each series' latest sample. Aggregating the whole
second is what a summary maintained over a 1-second window answers.

#### Query templates

These exercise every capability the templates need: sum, rate/increase, top-k
and quantile, spatial and temporal aggregation, and a binary operator. Each
template expands into one RQE per window `w ∈ W`, per quantile `q` and per
`k ∈ {100, 200, 300}`.

| # | PromQL | Capability | Grouping | RQEs |
|---|---|---|---|---|
| 1 | `sum by (job) (data)` | SumOrCount | `job` | 1 |
| 2 | `topk(k, sum by (label_0) (sum_over_time(data[w])))` | TopK | none; keys `label_0` | 3·\|W\| = 12 |
| 3 | `quantile by (job) (q, data)`, q ∈ {0.5, 0.75, 0.9, 0.95, 0.99} | Quantile | `job` | 5 |
| 4 | `sum_over_time(data[w])` | SumOrCount | per series | 4 |
| 5 | `quantile_over_time(q, data[w])`, same five q | Quantile | per series | 20 |
| 6 | `rate(data[w])` | RateOrIncrease | per series | 4 |
| 7 | `sum by (job) (rate(data[w]))` | RateOrIncrease | `job` | 4 |
| 8 | `sum by (job) (sum_over_time(data[w]))` | SumOrCount | `job` | 4 |
| 9 | `topk(k, sum by (label_0) (rate(data[w])))` | TopK over per-series increases | none; keys `label_0` | 12 |
| 10 | `quantile_over_time(0.9, data[w]) / quantile_over_time(0.5, data[w])` | Two Quantile RQEs | per series | 8 |

74 RQEs per replica. Template 10's operands are the same RQEs as template 5's
q = 0.9 and q = 0.5, so 66 are distinct.

Mapping notes:
- Capabilities are `rqe-optimizer`'s (#144). Sum and rate/increase are their
  own capabilities, served by exact per-group accumulators (`exact-sum`,
  `exact-increase`). Observability queries ask for a group's total, not for the
  frequency of arbitrary keys, so no template is a frequency query.
- An exact accumulator has one configuration, so AutoSketch has nothing to
  search for sum and increase RQEs. There, ASAP differs from it only by window
  choice and sharing.
- Top-k ranks each `label_0` key's total over the window: the heavy hitters
  among 1e4 keys. A top-k sketch with heap capacity `K` serves every RQE with
  `k ≤ K` on the same stream and window, so ASAP can serve k = 100, 200 and 300
  from one heap-300 deployment; AutoSketch configures each RQE with its own
  heap.
- The quantiles of one template, and the two operands of template 10, read the
  same stream. ASAP can serve them from one deployment; AutoSketch gets one
  deployment per RQE.
- Templates come from the planner's supported query classes: `SpatialAgg`
  (`count`/`sum`/`quantile`/`topk`), `TemporalAgg`
  (`count_over_time`/`sum_over_time`/`quantile_over_time`/`increase`/`rate`),
  `TemporalAgg SpatialAgg*`, and `AnyAgg <binaryOp> AnyAgg`. Every query below
  is checked with `promql-parser`.

<details>
<summary>Every query of the 10-template set (70 queries, 74 RQEs)</summary>

| # | Template | PromQL | Capability | Grouping | `S` | `T` | RQEs |
|---|---|---|---|---|---|---|---|
| 1 | 1 | `sum by (job) (data)` | SumOrCount | job | 1s | 1s | 1 |
| 2 | 2 | `topk(100, sum by (label_0) (sum_over_time(data[15m])))` | TopK | — (keys: label_0) | 15m | 1m | 1 |
| 3 | 2 | `topk(100, sum by (label_0) (sum_over_time(data[1h])))` | TopK | — (keys: label_0) | 1h | 1m | 1 |
| 4 | 2 | `topk(100, sum by (label_0) (sum_over_time(data[6h])))` | TopK | — (keys: label_0) | 6h | 1m | 1 |
| 5 | 2 | `topk(100, sum by (label_0) (sum_over_time(data[24h])))` | TopK | — (keys: label_0) | 24h | 1m | 1 |
| 6 | 2 | `topk(200, sum by (label_0) (sum_over_time(data[15m])))` | TopK | — (keys: label_0) | 15m | 1m | 1 |
| 7 | 2 | `topk(200, sum by (label_0) (sum_over_time(data[1h])))` | TopK | — (keys: label_0) | 1h | 1m | 1 |
| 8 | 2 | `topk(200, sum by (label_0) (sum_over_time(data[6h])))` | TopK | — (keys: label_0) | 6h | 1m | 1 |
| 9 | 2 | `topk(200, sum by (label_0) (sum_over_time(data[24h])))` | TopK | — (keys: label_0) | 24h | 1m | 1 |
| 10 | 2 | `topk(300, sum by (label_0) (sum_over_time(data[15m])))` | TopK | — (keys: label_0) | 15m | 1m | 1 |
| 11 | 2 | `topk(300, sum by (label_0) (sum_over_time(data[1h])))` | TopK | — (keys: label_0) | 1h | 1m | 1 |
| 12 | 2 | `topk(300, sum by (label_0) (sum_over_time(data[6h])))` | TopK | — (keys: label_0) | 6h | 1m | 1 |
| 13 | 2 | `topk(300, sum by (label_0) (sum_over_time(data[24h])))` | TopK | — (keys: label_0) | 24h | 1m | 1 |
| 14 | 3 | `quantile by (job) (0.5, data)` | Quantile | job | 1s | 1s | 1 |
| 15 | 3 | `quantile by (job) (0.75, data)` | Quantile | job | 1s | 1s | 1 |
| 16 | 3 | `quantile by (job) (0.9, data)` | Quantile | job | 1s | 1s | 1 |
| 17 | 3 | `quantile by (job) (0.95, data)` | Quantile | job | 1s | 1s | 1 |
| 18 | 3 | `quantile by (job) (0.99, data)` | Quantile | job | 1s | 1s | 1 |
| 19 | 4 | `sum_over_time(data[15m])` | SumOrCount | series | 15m | 1m | 1 |
| 20 | 4 | `sum_over_time(data[1h])` | SumOrCount | series | 1h | 1m | 1 |
| 21 | 4 | `sum_over_time(data[6h])` | SumOrCount | series | 6h | 1m | 1 |
| 22 | 4 | `sum_over_time(data[24h])` | SumOrCount | series | 24h | 1m | 1 |
| 23 | 5 | `quantile_over_time(0.5, data[15m])` | Quantile | series | 15m | 1m | 1 |
| 24 | 5 | `quantile_over_time(0.5, data[1h])` | Quantile | series | 1h | 1m | 1 |
| 25 | 5 | `quantile_over_time(0.5, data[6h])` | Quantile | series | 6h | 1m | 1 |
| 26 | 5 | `quantile_over_time(0.5, data[24h])` | Quantile | series | 24h | 1m | 1 |
| 27 | 5 | `quantile_over_time(0.75, data[15m])` | Quantile | series | 15m | 1m | 1 |
| 28 | 5 | `quantile_over_time(0.75, data[1h])` | Quantile | series | 1h | 1m | 1 |
| 29 | 5 | `quantile_over_time(0.75, data[6h])` | Quantile | series | 6h | 1m | 1 |
| 30 | 5 | `quantile_over_time(0.75, data[24h])` | Quantile | series | 24h | 1m | 1 |
| 31 | 5 | `quantile_over_time(0.9, data[15m])` | Quantile | series | 15m | 1m | 1 |
| 32 | 5 | `quantile_over_time(0.9, data[1h])` | Quantile | series | 1h | 1m | 1 |
| 33 | 5 | `quantile_over_time(0.9, data[6h])` | Quantile | series | 6h | 1m | 1 |
| 34 | 5 | `quantile_over_time(0.9, data[24h])` | Quantile | series | 24h | 1m | 1 |
| 35 | 5 | `quantile_over_time(0.95, data[15m])` | Quantile | series | 15m | 1m | 1 |
| 36 | 5 | `quantile_over_time(0.95, data[1h])` | Quantile | series | 1h | 1m | 1 |
| 37 | 5 | `quantile_over_time(0.95, data[6h])` | Quantile | series | 6h | 1m | 1 |
| 38 | 5 | `quantile_over_time(0.95, data[24h])` | Quantile | series | 24h | 1m | 1 |
| 39 | 5 | `quantile_over_time(0.99, data[15m])` | Quantile | series | 15m | 1m | 1 |
| 40 | 5 | `quantile_over_time(0.99, data[1h])` | Quantile | series | 1h | 1m | 1 |
| 41 | 5 | `quantile_over_time(0.99, data[6h])` | Quantile | series | 6h | 1m | 1 |
| 42 | 5 | `quantile_over_time(0.99, data[24h])` | Quantile | series | 24h | 1m | 1 |
| 43 | 6 | `rate(data[15m])` | RateOrIncrease | series | 15m | 1m | 1 |
| 44 | 6 | `rate(data[1h])` | RateOrIncrease | series | 1h | 1m | 1 |
| 45 | 6 | `rate(data[6h])` | RateOrIncrease | series | 6h | 1m | 1 |
| 46 | 6 | `rate(data[24h])` | RateOrIncrease | series | 24h | 1m | 1 |
| 47 | 7 | `sum by (job) (rate(data[15m]))` | RateOrIncrease | job | 15m | 1m | 1 |
| 48 | 7 | `sum by (job) (rate(data[1h]))` | RateOrIncrease | job | 1h | 1m | 1 |
| 49 | 7 | `sum by (job) (rate(data[6h]))` | RateOrIncrease | job | 6h | 1m | 1 |
| 50 | 7 | `sum by (job) (rate(data[24h]))` | RateOrIncrease | job | 24h | 1m | 1 |
| 51 | 8 | `sum by (job) (sum_over_time(data[15m]))` | SumOrCount | job | 15m | 1m | 1 |
| 52 | 8 | `sum by (job) (sum_over_time(data[1h]))` | SumOrCount | job | 1h | 1m | 1 |
| 53 | 8 | `sum by (job) (sum_over_time(data[6h]))` | SumOrCount | job | 6h | 1m | 1 |
| 54 | 8 | `sum by (job) (sum_over_time(data[24h]))` | SumOrCount | job | 24h | 1m | 1 |
| 55 | 9 | `topk(100, sum by (label_0) (rate(data[15m])))` | TopK over increases | — (keys: label_0) | 15m | 1m | 1 |
| 56 | 9 | `topk(100, sum by (label_0) (rate(data[1h])))` | TopK over increases | — (keys: label_0) | 1h | 1m | 1 |
| 57 | 9 | `topk(100, sum by (label_0) (rate(data[6h])))` | TopK over increases | — (keys: label_0) | 6h | 1m | 1 |
| 58 | 9 | `topk(100, sum by (label_0) (rate(data[24h])))` | TopK over increases | — (keys: label_0) | 24h | 1m | 1 |
| 59 | 9 | `topk(200, sum by (label_0) (rate(data[15m])))` | TopK over increases | — (keys: label_0) | 15m | 1m | 1 |
| 60 | 9 | `topk(200, sum by (label_0) (rate(data[1h])))` | TopK over increases | — (keys: label_0) | 1h | 1m | 1 |
| 61 | 9 | `topk(200, sum by (label_0) (rate(data[6h])))` | TopK over increases | — (keys: label_0) | 6h | 1m | 1 |
| 62 | 9 | `topk(200, sum by (label_0) (rate(data[24h])))` | TopK over increases | — (keys: label_0) | 24h | 1m | 1 |
| 63 | 9 | `topk(300, sum by (label_0) (rate(data[15m])))` | TopK over increases | — (keys: label_0) | 15m | 1m | 1 |
| 64 | 9 | `topk(300, sum by (label_0) (rate(data[1h])))` | TopK over increases | — (keys: label_0) | 1h | 1m | 1 |
| 65 | 9 | `topk(300, sum by (label_0) (rate(data[6h])))` | TopK over increases | — (keys: label_0) | 6h | 1m | 1 |
| 66 | 9 | `topk(300, sum by (label_0) (rate(data[24h])))` | TopK over increases | — (keys: label_0) | 24h | 1m | 1 |
| 67 | 10 | `quantile_over_time(0.9, data[15m]) / quantile_over_time(0.5, data[15m])` | 2 × Quantile | series | 15m | 1m | 2 |
| 68 | 10 | `quantile_over_time(0.9, data[1h]) / quantile_over_time(0.5, data[1h])` | 2 × Quantile | series | 1h | 1m | 2 |
| 69 | 10 | `quantile_over_time(0.9, data[6h]) / quantile_over_time(0.5, data[6h])` | 2 × Quantile | series | 6h | 1m | 2 |
| 70 | 10 | `quantile_over_time(0.9, data[24h]) / quantile_over_time(0.5, data[24h])` | 2 × Quantile | series | 24h | 1m | 2 |

</details>

#### Dashboard template set

A second template set models an SLO / monitoring dashboard, latency-quantile
heavy, refreshed every 1 m, over the same windows `W`.

| # | PromQL | Capability | Grouping | RQEs |
|---|---|---|---|---|
| D1 | `quantile_over_time(q, data[w])`, q ∈ {0.5, 0.9, 0.99} | Quantile | per series | 12 |
| D2 | `quantile by (job) (q, data)`, q ∈ {0.5, 0.9, 0.99} | Quantile | `job` | 3 |
| D3 | `sum by (job) (rate(data[w]))` | RateOrIncrease | `job` | 4 |
| D4 | `topk(k, sum by (label_0) (rate(data[w])))`, w ∈ {15m, 1h} | TopK over per-series increases | none; keys `label_0` | 6 |
| D5 | `quantile_over_time(0.99, data[w]) / quantile_over_time(0.5, data[w])`, w ∈ {15m, 1h} | Two Quantile RQEs | per series | 4 |

29 RQEs per replica; D5's operands repeat D1's, so 25 are distinct. D1's
quantiles share one stream across overlapping windows; D3 and D4 share the
increase stream.

<details>
<summary>Every query of the dashboard set (27 queries, 29 RQEs)</summary>

| # | Template | PromQL | Capability | Grouping | `S` | `T` | RQEs |
|---|---|---|---|---|---|---|---|
| 1 | D1 | `quantile_over_time(0.5, data[15m])` | Quantile | series | 15m | 1m | 1 |
| 2 | D1 | `quantile_over_time(0.5, data[1h])` | Quantile | series | 1h | 1m | 1 |
| 3 | D1 | `quantile_over_time(0.5, data[6h])` | Quantile | series | 6h | 1m | 1 |
| 4 | D1 | `quantile_over_time(0.5, data[24h])` | Quantile | series | 24h | 1m | 1 |
| 5 | D1 | `quantile_over_time(0.9, data[15m])` | Quantile | series | 15m | 1m | 1 |
| 6 | D1 | `quantile_over_time(0.9, data[1h])` | Quantile | series | 1h | 1m | 1 |
| 7 | D1 | `quantile_over_time(0.9, data[6h])` | Quantile | series | 6h | 1m | 1 |
| 8 | D1 | `quantile_over_time(0.9, data[24h])` | Quantile | series | 24h | 1m | 1 |
| 9 | D1 | `quantile_over_time(0.99, data[15m])` | Quantile | series | 15m | 1m | 1 |
| 10 | D1 | `quantile_over_time(0.99, data[1h])` | Quantile | series | 1h | 1m | 1 |
| 11 | D1 | `quantile_over_time(0.99, data[6h])` | Quantile | series | 6h | 1m | 1 |
| 12 | D1 | `quantile_over_time(0.99, data[24h])` | Quantile | series | 24h | 1m | 1 |
| 13 | D2 | `quantile by (job) (0.5, data)` | Quantile | job | 1s | 1s | 1 |
| 14 | D2 | `quantile by (job) (0.9, data)` | Quantile | job | 1s | 1s | 1 |
| 15 | D2 | `quantile by (job) (0.99, data)` | Quantile | job | 1s | 1s | 1 |
| 16 | D3 | `sum by (job) (rate(data[15m]))` | RateOrIncrease | job | 15m | 1m | 1 |
| 17 | D3 | `sum by (job) (rate(data[1h]))` | RateOrIncrease | job | 1h | 1m | 1 |
| 18 | D3 | `sum by (job) (rate(data[6h]))` | RateOrIncrease | job | 6h | 1m | 1 |
| 19 | D3 | `sum by (job) (rate(data[24h]))` | RateOrIncrease | job | 24h | 1m | 1 |
| 20 | D4 | `topk(100, sum by (label_0) (rate(data[15m])))` | TopK over increases | — (keys: label_0) | 15m | 1m | 1 |
| 21 | D4 | `topk(100, sum by (label_0) (rate(data[1h])))` | TopK over increases | — (keys: label_0) | 1h | 1m | 1 |
| 22 | D4 | `topk(200, sum by (label_0) (rate(data[15m])))` | TopK over increases | — (keys: label_0) | 15m | 1m | 1 |
| 23 | D4 | `topk(200, sum by (label_0) (rate(data[1h])))` | TopK over increases | — (keys: label_0) | 1h | 1m | 1 |
| 24 | D4 | `topk(300, sum by (label_0) (rate(data[15m])))` | TopK over increases | — (keys: label_0) | 15m | 1m | 1 |
| 25 | D4 | `topk(300, sum by (label_0) (rate(data[1h])))` | TopK over increases | — (keys: label_0) | 1h | 1m | 1 |
| 26 | D5 | `quantile_over_time(0.99, data[15m]) / quantile_over_time(0.5, data[15m])` | 2 × Quantile | series | 15m | 1m | 2 |
| 27 | D5 | `quantile_over_time(0.99, data[1h]) / quantile_over_time(0.5, data[1h])` | 2 × Quantile | series | 1h | 1m | 2 |

</details>

#### Workload grid

Each dimension has a default (bold). A workload fixes every dimension. The grid
keeps only what changes the comparison with AutoSketch.

| Dimension | Values | What it varies |
|---|---|---|
| Template set | **dashboard**; the 10 templates | Workload realism, and which capabilities appear |
| Replicas `r` | **1**, 8, 64 | Every replica reads the same stream with a seeded random subset of 3 windows from `W`, 3 quantiles from {0.5, 0.75, 0.9, 0.95, 0.99}, a `k` from {100, 200, 300} and `T` from {10 s, 1 m, 5 m}; identical RQEs are deduplicated. Many users or dashboards over the same metrics: how the sharing benefit and planning time grow with the number of RQEs |
| Accuracy target (strictness) | loose, **default**, strict | What AutoSketch optimizes for (§5) |
| Latency SLA | the §5 grid, **no limit** | §5 |

The data model is fixed (§6 "Data model and scale"); no grid dimension changes the data.

The sweep is the default workload, then each dimension varied alone with the
others at their defaults. Every workload runs every baseline (ASAP,
AutoSketch-Adapted, PerQuery-CostAware) at both weight settings (§4). Per
(workload, baseline, weights, SLA), report:
- the objective, mean CPU (vCPU) and memory (GiB), and memory per phase;
- max and median estimated latency, and SLA violations;
- active deployments and instances;
- planning time (AutoSketch: search plus charged benchmark time);
- RQEs excluded by the SLA or unservable.

Replicas with disjoint series (each replica filtering `{label_1="v_i"}`) were
dropped: the planner rejects spatial filters, and the shared mode is the case
that shows sharing.

#### Planner sensitivity (not part of this comparison)

These dimensions describe the planner, not its gap to AutoSketch. They belong
in the planner's micro-benchmarks:
- query mix (spatial only, temporal only, one capability at a time);
- lookback window set `W`, including windows under 15 minutes;
- repeat interval `T` (10 s, 1 m, 5 m);
- data scale: `C`, `J` and the sample rate;
- data distribution (key skew, value tail);
- interactions such as `r × C` and `W × C`.

### Benchmark input

Every method reads the cost table from sketch-bench
`scripts/export_rqe_optimizer_costs.sh`, the same table the planner reads.
Deployable rows: exact sum, min, max and increase; KLL k ∈ {200, 500}; HLL
`lg_k` ∈ {12, 14}; CMS-heap top-k with rows ∈ {3, 5}, cols = 2048 and heap
∈ {100, 200, 300}, each scored at every `k` up to its heap. DDSketch,
CountSketch-heap and UnivMon are measured but not deployable by default.

The table is exported with `CARDINALITY=10000`, the workload's key count, and
measured at 1e5 to 1e8 items per instance (#157).

**Exact accumulators** have no saturation point: their answer is exact at any
size. They are still benchmarked for cost, on the grouped column specs
(200,000 rows, about 9,900 groups), and priced per group. The sweep over items
per instance (#157) includes them, so their per-group cost is read at the
workload's size like every other row's.

#### Data parameters, shared by both methods

The benchmark inputs are generated from data parameters fit over the **whole
measured dataset**, not from a short sample or the average. These are key skew
`θ`, distinct keys `K` per window, value tail index `a`, and per-label-set
`λ` and `card`. Use the worst case across the dataset; for example, size top-k
from the lower `θ` bound. Longer samples expose worse cases (sketch-bench
`docs/saturation_conclusions.md`, conclusions 1–5). Fitting follows ASAPQuery
#746 and sketch-bench `scripts/recommend_config.py`.

- **`traces` gives the appendix results.** Its parameters are fit on the
  full trace.
- **`synthetic`** uses the cost table's own data, Zipf s = 1.1 over 10,000
  keys (§6 "Data model and scale").

Each config is benchmarked at these worst-case parameters. AutoSketch §5.2
injects random traffic bursts into synthetic workloads to cover variation over
time. We don't need them: worst-case fits over every window of the whole dataset
already cover that variation. AutoSketch's benchmark uses the same inputs,
which matches the paper: it lets users "use their own trace".

A window of length `S` on grouping labels `G` holds about

```text
n(S, G) = λ · S / card(G)   items per instance
```

#### Reading the table at the workload's size

Each row records the size it was measured at (`measured_at`, #155). Until the
size sweep (#157) lands, every row is read at its measured size and the
points whose instance size differs are flagged. With the sweep:

- **ASAP** reads each deployment's row at its own instance size,
  `λ · x / card(G)` items per window. When it merges `m = S/x` windows, the
  accuracy check uses the measured merge counts bracketing `m` (#154); merge
  error is not monotone in `m`, so both must pass.
- **AutoSketch** never merges (`x = S`). It reads the row at its window's size,
  `λ · S / card(G)`, and accepts a config if the target holds there, as the
  paper's benchmark-then-accept loop does. It configures once, before
  deployment (§9 Q2).
- **Exact accumulators** meet every target at every size; only their cost is
  read at the size.

## 7. Metrics and figures

Reported per (workload, method, weight setting, latency SLA),
median of repeated runs for timings:

- **Planning time.** Reported in two parts, because the two planners spend
  their time differently:
  - *Search time:* AutoSketch is the sum over RQEs of Algorithm 4 wall time,
    using table lookups. ASAP is candidate generation, dominance pruning and
    MILP solve.
  - *Benchmark time:* AutoSketch benchmarks every probed configuration, as in
    the paper (§5.2, Exp#9: 1–2 minutes per config, about 6.5 minutes per
    application). Reported two ways, per distinct probed (config, input size):
    - a **lower bound**, `N_bench · insert_cpu_per_item + one query phase`
      with `N_bench = 1e8`, the size sketch-bench benchmarks at. The paper
      also benchmarks a fixed-size representative workload, not the query
      window's full data;
    - a **paper-rate estimate**, 60 s per distinct probe.
  - *ASAP's one-time profiling:* the wall time of the sketch-bench cost export
    that produced its table. It is shared by all RQEs
    and reusable across workloads, so it is reported once, plus amortized per
    RQE served.
  - The paper's figure shows search + benchmark per method, stacked.
- **Objective** at each weight setting (§4), with its inputs: mean CPU (vCPU)
  and memory (GiB), each split by phase. Baselines are normalized to ASAP at
  the same weights.
- **Estimated query latency and latency SLA violations** per method.
  - Estimated latency per RQE: `card(G) · (c_qry + (S/x − 1) · c_mrg)` (§4). Report its maximum and median over the RQEs, plus the per-RQE values in the raw output.
  - SLA violations: the number of RQEs whose estimated latency exceeds the SLA. Only AutoSketch-Adapted can have any, since the other methods are constrained.
- **Estimated accuracy** per RQE: every method meets its target under its own
  reading rule (§6, "Benchmark input").
- Active deployments and total sketch instances.

Figures:

1. Synthetic workload, objective vs. achieved max estimated latency, one panel
   per weight setting (main paper figure).
2. Planning time vs. number of RQEs (synthetic, replica dimension), log–log.
3. Objective vs. each workload-grid dimension (synthetic, one dimension at a
   time).
4. Objective vs. absolute latency SLA (synthetic default workload, `traces`).

## 8. Who implements what, in which PR

| # | Repo / PR | Scope | Status (2026-10-06) |
| --- | --- | --- | --- |
| this | ASAPQuery #777 | This plan | Draft, updated as decisions change |
| — | sketch-bench #144, #145 | Exact accumulators and top-k families; per-phase cost model and weighted objective | Merged |
| — | sketch-bench #151, #152, #154, #155 | Cost-table fixes, value range, accuracy after merging, `measured_at` (#147) | Merged |
| — | sketch-bench #157 | Sweep items per instance; re-export the cost table | Open |
| — | sketch-bench (to open) | Top-k for this workload: heap size as a parameter, precision scored at k ∈ {100, 200, 300}; `rqe-optimizer` RQEs carry `k`, and a heap-`K` deployment serves `k ≤ K`; export at `CARDINALITY=10000`, 1e5–1e8 items | Not started |
| — | sketch-bench #130, #131 | Saturation curves; accuracy after merging `m` shards | Merged; background |
| 1 | sketch-bench #137 | Retained memory, EC2 pricing, `milp::minimize_cost` | Merged; superseded by #145 |
| 3 | sketch-bench #135 | AutoSketch-Adapted (Algorithm 4), aligned with the paper's EXAMINE rule and seeding | Merged |
| 2 | sketch-bench #136 | Evaluation table for the trace workloads | Merged |
| 4 | sketch-bench #138, #139, #140, #141 | Runner, synthetic workload, saturation at K = 1e1–1e6, two cost models and grid driver | Open; to be reworked onto #145's objective, the cost table and the reduced grid. #140 is no longer needed for the comparison |

## 9. Decisions

**Q1. AutoSketch's objective.** Per-instance memory, as in the paper
(`SC(c) = α·n_ALU + β·n_mem` with no ALUs in software). PerQuery-CostAware is
the strong ablation.

**Q2. One AutoSketch call per RQE, reused for every evaluation.** The paper
configures statically: "AutoSketch adopts static configuration instead of
dynamic adjusting" (§3.2), and "the searching is performed once before an
application is deployed" (§7, Exp#9). Data varies between windows of a repeating
query, but AutoSketch handles that through its benchmark inputs, not by
re-planning: a config must meet the target on every benchmark workload (§5.2).
Here that is the worst case over the whole dataset (§6). So the benchmark input changes per RQE
in one way only: its size follows the RQE's window, `n(S, ℓ)` (§6). It does not
change per evaluation.

**Q3. Probes read sketch-bench measurements, and their benchmark cost is
charged.** In the paper every probe is a benchmark run (§5.2, Algorithm 4
line 5: "Evaluate T by c"), and benchmarking dominates search time (Exp#9).
Running sketch-bench inside the search would give the same accuracy answers as
reading the same measurements, so the plan is unchanged. Planning time adds the
measured benchmark time of each distinct probed (config, size) point (§7). A
lookup-only time would understate AutoSketch's planning cost.

**Q4. AutoSketch window adapter.** `x = S`, `y = T` (or `gcd(S, T)`): one
sliding sketch per query.

**Q5. Memory model.** Per phase, as in #145 (§4): open windows, one merge
accumulator per group, query output, and closed windows.

**Q6. Cost.** `w_cpu · CPU + w_mem · memory`: CPU only first, then Fargate's
per-vCPU and per-GB prices (§4). Machine-family and peak-provisioned pricing
were dropped (2026-10-06).

## 10. Known limitations

- **Merged accuracy comes from shard-merge measurements, not from replaying
  the plans.** The cost table measures it at `m` ∈ {4, 16, 64, 256, 1024} over
  one benchmark stream (#154). A deployment needing more (e.g. a 1-day
  lookback over 1-minute windows, `m = 1440`) reads the 1024 measurement.
  Replay the synthetic default workload's chosen plans in sketch-bench once to
  confirm.
- Costs and latencies are estimates from per-operation measurements, not
  end-to-end executions. The execution-based comparison is ASAPQuery-backend
  #545/#547.
- AutoSketch-Adapted's deployments (`x = S`, `y = gcd(S, T)`) are in the ASAP
  candidate set (`candidates.rs` generates every divisor of `S` as a window and
  `gcd(x, T)` as a slide). So when its plan meets the latency bounds and its
  configurations also pass ASAP's lookup rule (the two rules differ on
  size, §6), the ASAP MILP can choose the same deployments and pay for
  shared ones once; ASAP's cost is never higher. The sanity check reports any
  case where this does not hold. The result to report is the size of the gap and where it comes
  from, not that a gap exists.

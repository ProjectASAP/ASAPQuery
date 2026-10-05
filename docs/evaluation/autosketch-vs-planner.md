# Evaluation plan: AutoSketch vs. the ASAPQuery planner (paper §6.3)

Status: plan, no results yet. The design decisions are settled in §9.

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
| Objective | Resource use (memory) | Weighted CPU + memory cost from EC2 prices |

So the comparison invokes AutoSketch **once per RQE** (one QE at one repeat
interval) and sums the results; the planner is invoked once for the batch.

## 2. What already exists

| Piece | Where | Status |
| --- | --- | --- |
| AutoSketch Algorithm 4 adaptation (LHS seeds, feasibility-directed width/depth neighbor search, pruning) | ASAPQuery-backend `data_plane/examples/autosketch_comparison.rs` ([#547](https://github.com/ProjectASAP/ASAPQuery-backend/pull/547)) | Merged. CMS/Count Sketch/Bloom only; hardcoded CMS grid; executes sketches to measure accuracy. |
| Earlier protocol (E1–E3, end-to-end execution) | ASAPQuery-backend `docs/evaluation/autosketch-comparison.md` ([#545](https://github.com/ProjectASAP/ASAPQuery-backend/pull/545)) | Merged. Execution-based; this plan is planner-level and uses estimated costs instead. |
| Top-K dashboard comparison | ASAPQuery-backend [#602](https://github.com/ProjectASAP/ASAPQuery-backend/pull/602) | Closed, not merged. |
| RQE deployment MILP (HiGHS): candidates `(capability, config, labels, x, y)`, sharing, latency bounds, minimum-CPU objective | sketch-bench `rqe-optimizer/` ([#129](https://github.com/ProjectASAP/sketch-bench/pull/129)) | Merged 2026-10-04. **This is the planner we evaluate for now.** |
| Measured per-operation costs (`AtomicCostEntry`: memory/instance, insert/merge/query CPU, accuracy) | sketch-bench `scripts/export_rqe_optimizer_costs.sh` | Merged; 18 rows, 2 configs per sketch variant. |
| Saturation study: error vs. `N`, `N_sat`, cost per config and shape | sketch-bench [#130](https://github.com/ProjectASAP/sketch-bench/pull/130) | Merged. Source of the saturated lookup (§6). |
| Accuracy after merging `m` shards (KLL, top-k) | sketch-bench [#131](https://github.com/ProjectASAP/sketch-bench/pull/131) | Merged 2026-10-05 (`bd644fe`). Source of KLL/top-k lookups when `m > 1`. |
| Moving the MILP into ASAPQuery's planner | ASAPQuery `asap-planner-rs/src/optimizer/` (Milind; related: [#776](https://github.com/ProjectASAP/ASAPQuery/pull/776), [#725](https://github.com/ProjectASAP/ASAPQuery/pull/725)) | In progress. The evaluation does not wait for it (§8, PR 5). |

No AutoSketch implementation exists in sketch-bench or in this repository.

## 3. Methods compared

All methods read the same `AtomicCostTable`, the same RQEs and the same
label-set cardinalities and arrival rates, and are scored by the same cost
function (§4).

1. **ASAP** — sketch-bench `rqe-optimizer` MILP over the whole batch, with
   accuracy and latency constraints, minimizing the §4 cost for one machine
   family.
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
4. **FewestPlans** (strawman) — the ASAP MILP minimizing the number of active
   deployments first, then cost among plans with that minimum count, under
   the same accuracy and latency requirements.

Only AutoSketch-Adapted ignores latency; the two strawmen must meet the same
requirements as ASAP, so every point in the cost–latency figure except
AutoSketch's is a feasible plan.

AutoSketch-Adapted is a planner baseline, not a reproduction of the P4
compiler: stage/page/ALU constraints are dropped. Its accuracy probes read
sketch-bench measurements instead of running a benchmark inside the search, but
the cost of running those benchmarks is charged to its planning time (§9 Q3).

## 4. Cost model

Disk is excluded (agreed with Milind). Units: CPU in vCPU (CPU-seconds per
second), memory in GiB.

**CPU** — already in `rqe_optimizer::objectives::score`:

```text
CPU = Σ_active D  λ(ℓ_D) · (x_D / y_D) · insert_cpu_D                      (ingest)
    + Σ_r  card(ℓ_r) · (query_cpu_D(r) + (S_r / x_D(r) − 1) · merge_cpu_D(r)) / T_r   (query + merge)
```

**Memory** — new. Today's objective tracks peak per-query memory, not retained
state. Retained state of an active deployment `D` holds `x/y` open instances
plus the closed instances needed by the longest lookback it serves:

```text
Mem_D = card(ℓ_D) · mem_bytes_per_instance_D · (x_D + max_{r→D} S_r) / y_D
Mem   = Σ_active D  Mem_D
```

In the MILP the `max` is linear: `Mem_D ≥ coef(r, D) · z_{r,D}` for each
eligible `r`.

**Price per machine family (steady state, implemented in sketch-bench #137)** — for family `f` with `vCPU_f`, `GiB_f` and
on-demand `price_f` ($/hour), the plan needs a fractional instance count
`n_f ≥ CPU / vCPU_f` and `n_f ≥ Mem / GiB_f`; cost is `price_f · n_f`. Here
`CPU` is the steady-state average. The two cost models below replace it in the
reported results. This
adds one continuous variable and two constraints, and needs no arbitrary split
of an instance's price between CPU and memory. Families:

| Family | Example instance | Role |
| --- | --- | --- |
| Compute-optimized | c7i.xlarge | cheap CPU, scarce memory |
| General purpose | m7i.xlarge | balanced |
| Memory-optimized | r7i.xlarge | cheap memory, scarce CPU |

Storage-optimized families are dropped with disk. Prices are fetched once from
the AWS Pricing API (us-east-1, Linux, on-demand), committed as
`rqe-optimizer/data/ec2-pricing-<date>.json` with the query used, and never
edited by hand. Since `n_f` is fractional, instance size within a family does
not change the result.

ASAP is solved once per family. AutoSketch-Adapted's plan does not depend on
the family; it is scored under each family's cost.

### Two cost models per experiment run

The average CPU hides that query load is bursty: ingest is continuous, but
query and merge work arrives at each evaluation. Every run is therefore priced
two ways, from one simulated CPU timeline.

**CPU timeline.**
- Simulate 24 hours in 1-second bins.
- Every RQE first evaluates at `t = 0`, then every `T_r`. This aligned start
  is the worst case.
- Each evaluation occupies one core for its estimated latency, starting when
  it fires. Work longer than 1 s spills into later bins.
- `CPU(bin)` = ingest rate + busy-core time overlapping the bin.

From the timeline:
- **total CPU-seconds** = the area under the curve, `Σ_bins CPU(bin) · 1 s`;
- **peak CPU** = `max_bin CPU(bin)`, in vCPU.

**Model A — usage-based (pay for what is used).**

```text
$/hour = a · (total CPU-seconds / 24 h) + b · Mem_GiB
```

`a` ($/vCPU-hour) and `b` ($/GiB-hour) are a least-squares fit of
`vCPU · a + GiB · b = price` over c7i.xlarge, m7i.xlarge and r7i.xlarge: about
a = 0.0368 and b = 0.00364 with the 2026-10-04 prices. Model A does not depend
on the machine family.

**Model B — peak-provisioned (buy machines for the peak).** Per family `f`:

```text
n_f    = max(peak CPU / vCPU_f, Mem_GiB / GiB_f)
$/hour = n_f · price_f
```

**Optimizing each model.** ASAP is solved separately for model A and for each
family's model B. PerQuery-CostAware and FewestPlans (its cost stage) also use
the model being compared. AutoSketch-Adapted's plan does not depend on cost
and is scored under both.
- Model A is linear: the average-CPU and memory terms weighted by `a` and `b`.
- Model B is linear through the aligned start: the peak is in bin 0, so
  `peak ≈ ingest + Σ_r min(latency_{r,D}, 1 s) · z_{r,D}`, where each
  (RQE, deployment) latency is a constant.
- After solving, the exact peak is recomputed from the timeline. Runs where it
  exceeds the bin-0 value, e.g. an evaluation longer than its interval
  overlapping itself, are reported.

Memory is kept in both models: without it, memory-bound workloads would look
almost free under model A.

**Latency** — per-RQE estimate already in sketch-bench:
`card(ℓ) · (query_cpu + (S/x − 1) · merge_cpu)`.

## 5. Constraints

**Accuracy target**, swept over {90%, 95%, 99%} in the synthetic workload
(95% elsewhere). A target `p` maps to error ≤ `1 − p` and to precision ≥ `p`.
The 95% case, per capability, using the metrics the cost table already records:

| Capability | Metric | Constraint |
| --- | --- | --- |
| Freq | relative error | ≤ 0.05 |
| Quantile | rank error | ≤ 0.05 |
| Cardinality | relative error | ≤ 0.05 |
| TopK | precision@k | ≥ 0.95 |

**Latency** — one absolute SLA applies to every RQE, swept over
{0.01, 0.1, 1, 10, 100, 1000} ms and no limit. The synthetic workload extends
the grid as needed.
- An RQE that no method can meet at a given SLA is excluded from every method
  at that SLA and reported by ID. Costs at different SLAs therefore cover
  different RQE sets; compare methods only at one SLA.
- ASAP, PerQuery-CostAware and FewestPlans must meet the SLA.
- AutoSketch-Adapted ignores it. Its violations are counted, and its cost is
  shown for those points but marked as infeasible.

An earlier version set `L_r = α × the fastest latency of r`. It was dropped:
on `example`, α = 2 forced plans with no merging at 40× the unconstrained
cost.

## 6. Workloads

| ID | Description | Purpose |
| --- | --- | --- |
| `example` | `small_problem`'s 8 RQEs (freq, quantile, cardinality, top-k; 1h–1d lookbacks; 60s/300s intervals), tolerances moved to §5 | Readable worked example; one table in the paper |
| `synthetic` | Synthetic PromQL workload: the 10 queries in "Synthetic workload" below, over Zipf/Pareto data. Main figure. | Cost–latency trade-off across data and requirements |
| `scaling` | Seeded random batches, `N ∈ {8, 32, 128, 512, 2048}` RQEs. Lookbacks {5m, 15m, 1h, 6h, 1d}, intervals {10s, 60s, 300s}, 4 capabilities, 3 label sets with fixed cardinality/rate. Knob: fraction of RQEs drawn from shared (capability, labels) cohorts, {0, 0.5, 1}. | Planning-time scaling; cost vs. shareability |
| `traces` | Real-trace RQEs, one workload per dataset: Alibaba 2022, BOOM and Google 2011. Taken from `asap-tools/dataset-analysis/results/skew_summary.csv` ([#746](https://github.com/ProjectASAP/ASAPQuery/pull/746)): each row's `range_s` is `S` and its `step_s` is `T`. Data parameters and accuracy targets are fit over each whole trace. | Appendix: real-trace results |

### Synthetic workload

**Data.**
- Series carry `label_0`, with cardinality in {10^1, …, 10^6}, and an
  `instance` label with 100 values per `label_0` value.
- Each series is scraped every 10 ms, i.e. 100 samples per second, so
  `λ = 100 · 100 · card(label_0)` samples/s.
- The data volume is chosen so that even the smallest windows hold enough
  samples for a sketch (1e4 per group for 1 s spatial queries, 6e3 per series
  for 1 m temporal ones). Points that still fall below `N_sat` are flagged.
- Key weights for frequency and top-k follow Zipf θ ∈ {0, 0.5, 1.0, 1.5, 2.0}.
  Values for quantiles follow Pareto a ∈ {1.1, 2, 3}.
- The saturation curves are extended to K ∈ {1e1, 1e2, 1e4, 1e6} by
  measuring, not by interpolation.

**Queries.** Each spatial query repeats every 1 s, with `S = T = 1 s`, so each
evaluation reads the last second. Each temporal query repeats every 1 m, with
`S = T_range ∈ {1m, 10m, 1h, 6h, 24h}`.

| # | Query | Capability | Grouping |
|---|---|---|---|
| 1 | `sum by (label_0) (data)` | Freq | `label_0` |
| 2 | `topk by (3, label_0) (data)` | TopK | `label_0` |
| 3 | `quantile by (q, label_0) (data)`, q ∈ {.5, .75, .9, .95, .99} | Quantile | per `label_0` group |
| 4 | `sum_over_time(data[T])` | Freq | per series |
| 5 | `quantile_over_time(q, data[T])`, same five q | Quantile | per series |
| 6 | `rate(data[T])` | Freq over per-series increments | per series |
| 7 | `sum by (label_0) (rate(data[T]))` | Freq over increments | `label_0` |
| 8 | `sum by (label_0) (sum_over_time(data[T]))` | Freq | `label_0` |
| 9 | `topk by (3, label_0) (rate(data[T]))` | TopK over increments | `label_0` |
| 10 | `quantile_over_time(0.9, data[T]) / quantile_over_time(0.5, data[T])` | Two Quantile RQEs | per series |

Notes on the mapping:
- `rate`/`increase` are modeled as a frequency sum of per-series increments,
  equivalent to `sum_over_time` over deltas.
- The quantiles of one query, and the two operands of query 10, read the same
  stream. ASAP can serve them from one deployment; AutoSketch gets one per RQE.
- One workload instance is the 67 RQEs above (2 spatial + 5 spatial quantiles + 5×5 temporal for queries 4, 6–9 + 5×5 for query 5 + 2×5 for query 10), for one (cardinality, θ or a,
  accuracy target, SLA) combination. The figure sweeps the accuracy target over
  {90%, 95%, 99%} and an absolute latency SLA grid. For the scalability study,
  the RQE set is replicated with distinct `label_0` filters.

### Benchmark input

The cost table used for the evaluation needs a wider grid than today's two
configs per variant, otherwise Algorithm 4's neighbor search has nothing to
search: CMS/Count Sketch depth {2..8} × width {256..8192}, KLL k
{50..800}, DD α {0.005..0.05}, HLL precision {10..16}.

#### Data parameters, shared by both methods

The benchmark inputs are generated from data parameters fit over the **whole
measured dataset**, not from a short sample or the average. These are key skew
`θ`, distinct keys `K` per window, value tail index `a`, and per-label-set
`λ` and `card`. Use the worst case across the dataset; for example, size CMS
and top-k from the lower `θ` bound. Longer samples expose worse cases (sketch-bench
`docs/saturation_conclusions.md`, conclusions 1–5). Fitting follows ASAPQuery
#746 and sketch-bench `scripts/recommend_config.py`.

- **`traces` gives the appendix results.** Its parameters are fit on the
  full trace.
- **`example`, `scaling` and `synthetic`** use their generators' parameters.

Each config is benchmarked at these worst-case parameters. AutoSketch §5.2
injects random traffic bursts into synthetic workloads to cover variation over
time. We don't need them: worst-case fits over every window of the whole dataset
already cover that variation. AutoSketch's benchmark uses the same inputs,
which matches the paper: it lets users "use their own trace".

A query with lookback `S` on label set `ℓ` reads about

```text
n(S, ℓ) = λ(ℓ) · S / card(ℓ)   events per group
```

#### ASAPQuery: saturated values, because it merges

ASAP may answer a query by merging `m = S/x` smaller-window sketches into one.
Each of them holds only `n(x, ℓ)` events, which may be below the length at
which its error has settled. So ASAP reads accuracy and cost at saturation, as
measured in sketch-bench's saturation study (#130):

- Past `N_sat`, error depends on the config alone.
- Per-item insert, merge and query CPU are flat in `N`.
- Memory is fixed by the config; DDSketch and KLL grow only with `ln N`.

Each (config, dataset parameters) point therefore contributes its error
plateau and its costs at `N_sat`. `N_sat` is taken over the whole measured
dataset: it is the saturation length at the dataset's worst-case parameters
above. A lookup is valid when `n(S, ℓ) ≥ N_sat`.

- **CMS, Count Sketch, HLL and DDSketch merge exactly.** The merged sketch
  equals one sketch over all `n(S, ℓ)` events, so the saturated single-sketch
  value applies however small each pane is.
- **KLL and top-k do not.** Merging raises KLL's error, by 1.0–1.1× at
  k = 50/200 and 1.12–1.32× at k = 800, and up to 3–4× at small `N`. Top-k
  loses up to 40% precision at large `K`. For these sketches, a deployment
  with `m > 1` uses the saturated value from the merged curves at `m` shards
  (sketch-bench #131).
- If `n(S, ℓ) < N_sat`, the query's window never saturates. ASAP uses the curve's
  value at the smallest measured checkpoint `≥ n(S, ℓ)`.
- With uniform keys and large `K`, CMS, Count Sketch and top-k do not saturate
  by 1e9 events. Their `N_sat` is a lower bound; flag these points in the
  results.

#### AutoSketch: no saturation requirement

AutoSketch never merges: it keeps one sketch per query window. In the paper it
benchmarks a config on its workloads and accepts it if the accuracy intent
holds, with no notion of `N_sat`. AutoSketch-Adapted therefore reads the
measured value at its own input size `n(S, ℓ)`, using the dataset-derived
inputs above.

The windows of a repeating query see different data each time. AutoSketch does
not re-tune for this: it configures once, before deployment, against these
inputs (§9 Q2).

## 7. Metrics and figures

Reported per (workload, method, cost model and machine family, latency SLA),
median of repeated runs for timings:

- **Planning time.** Reported in two parts, because the two planners spend
  their time differently:
  - *Search time:* AutoSketch is the sum over RQEs of Algorithm 4 wall time,
    using table lookups. ASAP is candidate generation, dominance pruning and
    MILP solve.
  - *Benchmark time:* AutoSketch benchmarks every probed (config, input size)
    per RQE, as in the paper (§5.2, Exp#9: 1–2 minutes per config, about
    6.5 minutes per application). We charge `Σ_r Σ_probes t_bench(config,
    n(S_r, ℓ_r))`, where `t_bench` is sketch-bench's measured wall time for
    that point over all benchmark inputs; probes already charged for the same
    (config, size) are not charged again. ASAP's benchmark time is one profiling
    pass over the grid, shared by all RQEs and reusable across workloads. It is
    reported once, next to how many RQEs it served.
  - The paper's figure shows search + benchmark per method, stacked.
- **Total cost** ($/hour) under **model A** and under **model B** for each
  family, with its inputs: total CPU-seconds, peak CPU, retained GiB, and
  which resource binds `n_f` in model B. Baselines are compared under each
  model separately, each normalized to ASAP under the same model.
- **Estimated query latency and latency SLA violations** per method.
  - Estimated latency per RQE: `card(ℓ) · (query_cpu + (S/x − 1) · merge_cpu)` (§4). Report its maximum and median over the RQEs, plus the per-RQE values in the raw output.
  - SLA violations: the number of RQEs whose estimated latency exceeds the SLA. Only AutoSketch-Adapted can have any, since the other methods are constrained.
- **Estimated accuracy** per RQE (all methods meet it on single-instance
  measurements by construction).
- Active deployments and total sketch instances.

Figures:

1. Total cost by method, grouped by machine family (`example` and `scaling` at N = 128).
2. Planning time vs. N, log–log (`scaling`).
3. Cost vs. shareability (`scaling`).
4. Cost vs. absolute latency SLA (`example`, `scaling`).
5. Baselines under the two cost models: paired bars per workload, model A
   next to model B, each normalized to ASAP.
6. Synthetic workload: cost vs. achieved max estimated latency, one panel per
   cost model (main paper figure).

## 8. Who implements what, in which PR

| # | Repo / PR | Scope | Status (2026-10-05) |
| --- | --- | --- | --- |
| this | ASAPQuery #777 | This plan | Draft, updated as decisions change |
| — | sketch-bench #130, #131 | Saturation curves at K ∈ {1e3, 1e5, 1e7}; accuracy after merging `m` shards | Merged |
| 1 | sketch-bench #137 | Retained memory, EC2 pricing, `milp::minimize_cost` (steady-state model), solver scaling | Merged |
| 3 | sketch-bench #135 | AutoSketch-Adapted (Algorithm 4), aligned with the paper's EXAMINE rule and seeding | Merged |
| 2 | sketch-bench #136 | Evaluation table for the trace workloads: per (RQE, config) accuracy for AutoSketch and for ASAP at each `m`, saturation, costs | In review |
| 4 | sketch-bench #138 | Runner, absolute SLA, results for `example`, `scaling`, `traces` | Open; needs a rebase on main and an AutoSketch rerun with #135's final search |
| — | sketch-bench #140 | Saturation curves at K ∈ {1e1, 1e2, 1e4, 1e6} for the synthetic workload | Draft; accuracy done, cost 197 of 240 points |
| — | sketch-bench #139 | Synthetic workload (67 RQEs), FewestPlans, strawmen bound by the SLA | Open; final sweep pending #140 |
| — | sketch-bench, not yet opened | The two cost models (§4): CPU timeline, model A, model B, rerun of every experiment | Not started as a PR |
| 5 | ASAPQuery | After the MILP lands in `asap-planner-rs`: port the objective there and rerun, so the paper reports the planner that ships | Not started; waits for Milind's port |

Merge order: #136 → rebase and merge #138 → #140 → #139 → two-cost-model PR.

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

**Q5. Memory model.** Retained state, as in §4.

**Q6. Machine-family cost.** The fractional-instance `max` model in §4.

## 10. Known limitations

- **Merged accuracy comes from shard-merge measurements, not from replaying
  the plans.** It is exact by construction for CMS, Count Sketch, HLL and
  DDSketch, and taken from #131 for KLL and top-k. #131 covers `N ≤ 1e7` and
  `m ≤ 64`. A deployment needing `m > 64` (e.g. a 1-day lookback over 1-minute
  windows, `m = 1440`) is outside the measured range. Mark it as extrapolated,
  or exclude it for KLL/top-k. Replay `example`'s chosen plans in sketch-bench once to
  confirm the lookups.
- Costs and latencies are estimates from per-operation measurements, not
  end-to-end executions. The execution-based comparison is ASAPQuery-backend
  #545/#547.
- AutoSketch-Adapted's deployments (`x = S`, `y = gcd(S, T)`) are in the ASAP
  candidate set (`candidates.rs` generates every divisor of `S` as a window and
  `gcd(x, T)` as a slide). So when its plan meets the latency bounds, the ASAP
  MILP can choose the same deployments and pay for shared ones once; ASAP's
  cost is never higher. The result to report is the size of the gap and where it comes
  from, not that a gap exists.

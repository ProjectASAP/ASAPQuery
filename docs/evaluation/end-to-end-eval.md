# Evaluation plan: end-to-end cost and latency (paper §6.2)

Status: draft 2026-10-13. Reuses the §6.3 workloads ([#777](https://github.com/ProjectASAP/ASAPQuery/issues/777)) at 1/20 of their
data rate, on the plumbing from [#840](https://github.com/ProjectASAP/ASAPQuery/issues/840)–[#845](https://github.com/ProjectASAP/ASAPQuery/issues/845), [#841](https://github.com/ProjectASAP/ASAPQuery/issues/841) and [#828](https://github.com/ProjectASAP/ASAPQuery/issues/828). The plans re-solved
at 10 samples/s are in §6. Open decisions are in §12.

## 1. Question

On the same data and the same repeating queries, how much CPU and memory does
ASAPQuery use, and how fast does it answer, compared with Prometheus,
VictoriaMetrics, ClickHouse, and ClickHouse with materialized views? The result
is a cost–latency scatter, one point per (system, workload), with cost defined
the way the planner defines it (§9).

## 2. Approach

- **The set of RQEs is the workload.** The workloads are the §6.3 ones: their
  sketch-bench tables already hold the RQEs, the PromQL (classic) or the
  structured query fields (multi-grouping), and the data description
  (`workloads[0].e2e`, sketch-bench [#208](https://github.com/ProjectASAP/sketch-bench/issues/208), [#209](https://github.com/ProjectASAP/sketch-bench/issues/209), [#210](https://github.com/ProjectASAP/sketch-bench/issues/210)).
- **ASAPQuery runs the §6.3 planner's plan**, converted with
  `asap-planner --planner saved` ([#828](https://github.com/ProjectASAP/ASAPQuery/issues/828)). The planner is not re-run inside the
  experiment.
- **Every system gets identical data**: the same feeder, seed, event-time
  interval, and per-series timestamp order. It bulk-loads the 24 h history,
  then sends the measurement tail at the paced rate
  ([#840](https://github.com/ProjectASAP/ASAPQuery/issues/840)–[#843](https://github.com/ProjectASAP/ASAPQuery/issues/843)).
- **One system per CloudLab node**, so each system's CPU and memory are measured
  alone.
- **At 1/20 of the §6.3 rate** (§5), so that every system can ingest it on one
  node and the long-window baseline queries are not hopeless. The plan is
  re-solved at that rate (§6).

## 3. Systems

| System | Version | Queries | Data path | Runner |
|---|---|---|---|---|
| ASAPQuery | `main` + this plan's PRs | PromQL (classic); SQL frontend (multi-grouping) | remote write from the feeder | `experiment_run_e2e.py` with `controller.prebuilt_output_dir` ([#828](https://github.com/ProjectASAP/ASAPQuery/issues/828), [#841](https://github.com/ProjectASAP/ASAPQuery/issues/841)) |
| Prometheus | 2.53.2 (bare metal) | PromQL | remote-write receiver | `experiment_run_e2e.py` ([#841](https://github.com/ProjectASAP/ASAPQuery/issues/841)) |
| VictoriaMetrics | vmsingle | PromQL (MetricsQL-compatible subset) | remote write into vmsingle | `experiment_run_e2e.py` (feeder sink: to do, §11) |
| ClickHouse | 26.10 | SQL with Prometheus 2.53 semantics ([#844](https://github.com/ProjectASAP/ASAPQuery/issues/844)) | `INSERT … RowBinary` from the feeder | `experiment_run_clickhouse.py` (live feed: to do, §11) |
| ClickHouse + MV | 26.10 | SQL over exact materialized views, bucket = T ([#845](https://github.com/ProjectASAP/ASAPQuery/issues/845)) | same, views created before data | same, `baseline_mv` arm |

PilotDB (the AQP comparison in the paper) is out of scope here; it needs its
own plan (PromQL→SQL for PilotDB, its sampling modes).

## 4. Workloads

| Workload | sketch-bench table | RQEs | Queries | Windows `W` | Repeat `T` |
|---|---|---|---|---|---|
| **mixed** | `synthetic-templatesclassic-shared1` | 42 (50 incl. template 10's shared operands) | 46 PromQL | 15m, 1h, 6h, 24h | 60 s; 1 s for templates 1 and 3 |
| **all** | `synthetic-templatesall-shared1` | 104 (102 served at 10/s, §6) | 46 PromQL + 52 structured (SQL); 50 run | classic as above; multi-grouping 5m, 15m | 60 s; 1 s for templates 1 and 3 |

- **mixed** is the paper's \WorkloadAggregations: one gauge `data` and one
  counter `data_total`, labels `job` (10) → `label_0` (1,000 per job), 1e4
  series. Templates 1–6, 8, 10 (7 and 9 dropped in sketch-bench [#210](https://github.com/ProjectASAP/sketch-bench/issues/210); template
  2's PromQL is `topk(32, sum_over_time(data[w]))`, same RQE).
- **all** adds the multi-grouping templates (11–17) on two records tables,
  `http` (region × service × endpoint × status; `user_id`, `latency`) and
  `flows` (dst_subnet × dst_port × proto; `src_ip`): distinct counts and a
  pooled p99 `GROUP BY CUBE(region, service, endpoint)`. They are SQL only:
  PromQL can express neither a distinct count of a value field nor a p99
  pooled over a group's series and a window.
- **Who runs what.** Prometheus and VictoriaMetrics run the classic queries
  only, in both workloads; in **all** they have no answer for the
  multi-grouping queries, which the results state. ASAPQuery, ClickHouse and
  ClickHouse + MV run everything.
- Random workloads (8 RAQEs + 2 binary queries per workload, to answer
  cherry-picking) come after this plan works on mixed and all; they would be a
  new template set in the sketch-bench export, with the same e2e block.

## 5. Data and rate

| | §6.3 | This plan |
|---|---|---|
| Samples/s per series (classic) | 200 (5 ms apart) | **10** (100 ms apart) |
| Classic rows/s (`data` + `data_total`) | 4e6 | 2e5 |
| `http` records/s | 2e6 | 1e5 |
| `flows` records/s | 4e6 | 2e5 |

- **Why 1/20.** At the §6.3 rate one node cannot ingest it: on clnode109 a
  single ClickHouse fell behind at 6e6 rows/s and a single ASAPQuery engine at
  4e6 samples/s. Prometheus at 10 samples/s already took up to 235 s per query
  on 25 minutes of data (A6, `/mydata/a6-results/`).
- **Generator.** The paced feeder reads `e2e.metrics`: `child_of` fan-out,
  Zipf weights folded into Pareto(2) values for the gauge, a closed-form
  counter, Zipf label shares with the parent's condition for records, the
  `scaled_by`/anomaly/burst features. `--verify-facts` checks every label
  subset's cardinality before a run.
- **Order.** Ticks start on whole seconds and hold samples in `(t, t+1s]`;
  each series' samples of a tick go in one request, in timestamp order, and
  requests run concurrently ([#840](https://github.com/ProjectASAP/ASAPQuery/issues/840)). ASAPQuery's allowed lateness stays at its
  5 s default. Prometheus needs no out-of-order window.
- **History and logical time.** Before each measurement, the feeder sends the
  largest lookback as historical event-time data at the system's maximum
  sustainable write rate. The history's last timestamp, rather than wall-clock
  `now`, anchors the query clients' evaluation time. The following measurement
  hour advances that logical time at the stated paced rate. Thus every query
  still sees the intended 15 m–24 h of samples without waiting 24 wall-clock
  hours to fill it.
- **Bulk-sender policy.** The accelerated feeder keeps one documented request
  shape, timestamp ordering, batch size, and bounded in-flight request/byte
  limit; it removes pacing only. The accelerated-backfill pilot selects one
  conservative configuration that drains without receiver errors or runaway
  backlog, then freezes it for every workload and repetition. Each system's
  backpressure determines its achieved bulk rate. There is no separate
  bulk-rate benchmark or per-system concurrency tuning.

**Windows are not scaled.** The windows (15m–24h) define the RQEs; the rate
only sets how much data a window holds. Keeping them makes the e2e workload the
§6.3 one. Scaling them up 20× to keep the items per window would mean 5 h to 20
day windows; scaling them down 20× would put most per-series windows below the
accuracy curves (a series' 15m window would hold 450 items) and change the
workload. At 1/20 of the rate each baseline query scans 1/20 of the data, so
the measured speedups understate ASAPQuery's advantage at the §6.3 rate.

## 6. Planner at 10 samples/s

The §6.3 plans were solved at 200 samples/s. Ingest CPU scales with the rate,
but each sketch's accuracy and memory depend on its items per window `n`, so
the plan can change at 1/20 of the rate, and the tables and plans are
regenerated at 10 samples/s (`SYNTHETIC_SAMPLES_PER_SEC`). These plans are for
the e2e run only; §6.3's results stay at 200 samples/s. Constraints:

- **Accuracy curves start at N = 1e3.** Below a curve's first measured N the
  optimizer has no accuracy for the pair and the deployment is ineligible
  (`rqe-optimizer/src/saturation.rs`, `first_measured_n`). Classic is above it
  everywhere (a series' 15m window holds 9,000 items). Multi-grouping
  templates that must cover every group (16's finest grouping, 17) can fall
  below it in 5m windows.
- **Settings.** ASAP method, cheapest plan (no latency SLA), p95 accuracy as in
  §6.3, cpu weights and Fargate weights.

**Feasibility run** (sketch-bench runner on the [#210](https://github.com/ProjectASAP/sketch-bench/issues/210) tables with
`SYNTHETIC_SAMPLES_PER_SEC = 10`; the 200/s tables reproduce the committed
plans exactly with the same binary):

- **Served RQEs.** Every classic RQE is served. In **all**, 102 of 104: template
  16's 5m p99 by (region, endpoint) and by (region, service, endpoint) have
  2,500 groups whose smallest covered group holds 403 items, below the curves'
  first N (1e3), so no method can serve them (the runner drops them; no theory
  fallback). Their 15m windows pass (1,210). The next smallest RQE is at 2,245.
  These two queries are left out of the e2e run (§12, Q2).
- **Cost lookups.** The cost table has one row per config, measured at one size
  (1e6 items for sketches, 2e5 for exact accumulators). Per-insert, merge and
  query CPU and per-instance memory are constants in the cost model
  (`analytical_cost_model.rs`), except DDSketch memory, which is capped by the
  values a window holds. So nothing is extrapolated at the lower rate; only
  accuracy depends on N.
- **Plans.** Classic keeps its 6 deployments, families and parameters, with
  longer windows, since ingest is cheaper (CPU weights): quantile_over_time
  DDSketch 300/60 s → 900/60 s, exact-increase 60/60 s → 180/60 s, top-k CMS
  3×1024 60/60 s → 300/60 s (heap 46,080 → 9,216). Multi-grouping uses 9
  deployments instead of 5 and rolls up 41 RQEs instead of 52; the p99 cube
  moves from DDSketch α = 0.02 to α = 0.05. Every chosen accuracy is measured;
  the worst is 0.025 against 0.05.
- **Predicted cost** (cheapest plan, p95):

  | Workload | Weights | ASAP | ASAP, no roll-ups | PerQuery | AutoSketch |
  |---|---|---|---|---|---|
  | mixed | CPU (vCPU) | 2.38 → **0.309** | same | 7.77 → 0.725 | 2,739 → 137 |
  | mixed | Fargate ($/h) | 0.170 → **0.0787** | same | 0.623 → 0.324 | 136 → 7.02 |
  | all | CPU (vCPU) | 2.60 → **0.404** | 2.93 → 0.424 | 8.84 → 0.865 | 2,750 → 137.5 |
  | all | Fargate ($/h) | 0.184 → **0.0874** | 0.204 → 0.0945 | 0.683 → 0.345 | 136 → 7.06 |

  ASAP's ingest CPU falls about 10× (mixed 1.84 → 0.18 vCPU), not 20×,
  because of the longer windows. Its merge and query CPU barely moves (0.54 →
  0.13) and is now half or more of its cost. Memory hardly moves (ASAP 21.4 →
  15.3 GiB), so Fargate costs fall only about 2×. AutoSketch, almost all
  ingest, falls the full 20×.
- **Files.** The 10/s tables, plan files and runner output are in sketch-bench
  [#212](https://github.com/ProjectASAP/sketch-bench/issues/212), `rqe-optimizer/results/e2e-rate10/` (export with `--samples-per-sec 10`;
  the two unservable RQEs are left out of the tables and their e2e queries).

## 7. Semantics and ground truth

| | Window | Instant query (`sum by (job) (data)`, T = 1 s) |
|---|---|---|
| Prometheus 2.53 | closed `[E−W, E]` | latest sample per series within the lookback |
| ClickHouse ([#844](https://github.com/ProjectASAP/ASAPQuery/issues/844)) | closed (classic), `toDateTime64` bounds | same as Prometheus |
| ClickHouse + MV ([#845](https://github.com/ProjectASAP/ASAPQuery/issues/845)) | same as raw, exactly | same |
| ASAPQuery | left-open `(E−W, E]` | every sample of the last second (≈10× Prometheus's sum at 10 samples/s) |
| Multi-grouping (all systems) | left-open `(E−W, E]` | — |

- **Exact baselines** are checked against each other (Prometheus = ClickHouse =
  ClickHouse + MV up to float rounding, as in A6).
- **ASAPQuery's accuracy** is measured against ClickHouse SQL in ASAPQuery's own
  semantics (left-open windows; all samples of the last second for instant
  queries). The instant-query difference is a semantic difference between the
  systems, not an error; the paper should say ASAPQuery answers instant
  queries over the whole last interval.
- **Evaluation times** are aligned to multiples of T on every system
  (`align_to_interval`, [#841](https://github.com/ProjectASAP/ASAPQuery/issues/841); floored in the ClickHouse SQL, [#844](https://github.com/ProjectASAP/ASAPQuery/issues/844)), with a 10 s
  offset for ingestion to settle. They are explicit logical timestamps during
  the historical backfill and paced tail, not implicit wall-clock `now`.

### ASAPQuery's retention: memory, then disk

The baselines keep their whole history on disk and can answer any range.
ASAPQuery keeps sketch windows instead, in two tiers ([#800](https://github.com/ProjectASAP/ASAPQuery/issues/800)):

- **Memory: CircularBuffer.** Each aggregation keeps its last
  `retained_instance_count` windows (`num_aggregates_to_retain`), which is
  exactly what the MILP charges memory for, sized for its longest-lookback
  reader. Today the MILP output uses `NoCleanup` and keeps every window, so
  memory grows for the whole run.
- **Disk: spill, not drop.** A window that leaves the buffer is written to disk
  (its serialized accumulator, indexed by window start) and kept for a
  configured retention. A query reaching past the in-memory windows reads the
  older ones from disk. The scheduled RQEs never need it, since the buffer
  holds their whole lookback; ad-hoc and longer-range queries do.
- **Accounting.** The cost model is unchanged: memory is the retained windows,
  and disk is not charged, as in §6.3. In the runs, spill writes count in
  ASAPQuery's measured CPU, and its disk bytes are reported next to the
  baselines' (§9).

## 8. Procedure

1. **Plan.** Regenerate the tables and plans at 10 samples/s (§6), convert the
   ASAPQuery plan with `--planner saved`, run `--verify-facts` on the table.
2. **Accelerated-backfill pilot (≈2 h).** On all systems, bulk-load a
   representative 1 h event-time history, then run a short paced tail. Compare
   15 m and 1 h answers, ASAPQuery accuracy, and retained data with an
   equivalent paced pilot at identical explicit evaluation timestamps. Confirm
   that every receiver accepts the historical timestamps, no write or late-data
   rejections occur, monitors split the phases, and the systems reach a stable
   post-load flush/compaction state. Use this pilot to choose and freeze one
   conservative bulk-sender configuration (§5); do not create a separate
   bulk-rate benchmark. ASAPQuery uses
   `wall_clock_grace_period_ms: 0` for the pilot and runs: its normal
   wall-clock fallback would treat historical panes as idle; strict event-time
   closure is required instead.
3. **Run.** Per workload, one node per system, all started together: bulk-load
   the largest lookback (24 h) with the frozen bulk-sender configuration (§5),
   allowing each system's backpressure to determine its achieved rate; wait for
   each system's feeder backlog to drain and every scheduled baseline query to
   return an answer, then feed and query for one paced measurement hour. A
   final event-time tail advances the watermark so ASAPQuery closes the last
   panes. Phase boundaries and logical evaluation times are recorded with the
   results.
4. **No timeouts.** Baseline queries run to completion: [#861](https://github.com/ProjectASAP/ASAPQuery/issues/861) lifts
   Prometheus' 2-minute `--query.timeout`, VictoriaMetrics' 30 s query limit
   and the ClickHouse client's 30 s timeout. A query still running when its
   next firing is due is reported as late, with its full latency (§12, Q3).

### Rapid randomized E2E fallback (draft)

This is a smaller deadline-oriented result, not a replacement for the full
mixed/all runs. Its claim is: across randomly generated supported query mixes,
ASAPQuery has lower resource cost and/or latency at bounded fidelity error than
exact baselines, after the same logical history is loaded.

- **Systems.** ASAPQuery, Prometheus, and raw ClickHouse are required.
  VictoriaMetrics and ClickHouse + exact MVs are optional and do not gate this
  result.
- **Workloads.** Generate ten workloads from the classic PromQL query-string
  pool, using seeds 0–9. Each workload samples ten distinct query strings
  uniformly without replacement; different parameterizations, such as two
  quantiles, are distinct strings and may co-occur. Windowed queries use only
  15 m and 1 h windows. Quantile-over-time may occur more often because it has
  more eligible parameterizations.
- **Instant queries.** Templates 1 and 3 are included only if a planner-only
  sensitivity run with 100 ms instant windows, using the existing saturation
  curves at N = 1,000 values/job, meets their fidelity targets. The saved 10/s
  plan currently models them as 1 s windows, so it cannot establish
  Prometheus-equivalent latest-sample semantics by itself.
- **Protocol.** For each workload, bulk-load one hour of logical history and
  run a 15-minute paced tail. Query instances keep their source cadence: one
  second for instant templates and one minute for windowed templates. ASAPQuery
  receives a workload-specific saved plan and a fresh run. Each baseline may
  bulk-load the common history once, then run the ten workloads as
  phase-separated measurements in independently randomized, recorded order.
- **Measures.** During each 15-minute phase, collect mean vCPU, RSS, raw
  latencies and maxima, and fidelity. Report median latency rather than a
  per-query p99 for minute-cadence queries. The common fidelity headline is
  the share of ASAPQuery evaluations meeting their template target; retain
  rank/relative error for quantiles, precision for top-k, and relative error
  or exact match for sums, rates, and instant aggregates.
- **Pilot.** Run one sampled workload through all three systems before the
  ten-workload sweep, validating data acceptance, results, monitor boundaries,
  and ClickHouse ground truth in ASAPQuery's configured window semantics.

## 9. Metrics and figures

Per (system, workload), over the measurement hour:

- **CPU**: core-seconds of the system's own processes for both the bulk-history
  phase and paced measurement phase (ASAPQuery split into ingest/precompute and
  query threads), plus mean vCPU during the paced measurement hour. Bulk CPU
  seconds capture the total work to construct the 24 h history; they are not
  assumed equal to paced-ingestion CPU seconds, since batching, concurrency,
  and storage compaction can change work. Preserve phase boundaries and raw
  samples so the analysis can later choose whether and how to amortize them.
- **Memory**: RSS, mean and peak, in GiB.
- **Disk**: bytes on disk at the end of the run (TSDB, ClickHouse parts,
  ASAPQuery's spilled windows), reported, not charged.
- **Latency**: per query, median and p99; misses counted per §8.
- **Results**: every answer saved; accuracy for ASAPQuery per §7
  (quantile rank error / relative error, top-k precision, relative error for
  exact accumulators).
- **Cost**, as the planner defines it (cost by use, sketch-bench [#188](https://github.com/ProjectASAP/sketch-bench/issues/188)):
  `w_cpu · mean vCPU + w_mem · mean GiB`, reported with cpu weights (vCPU) and
  with Fargate prices (0.0405 $/vCPU-h, 0.00445 $/GB-h). Retain both phase
  measurements before choosing whether the final figures use paced steady-state
  cost, total CPU work, or an amortized combination. Absolute costs per system,
  not ratios.

Figures and tables:

1. Cost vs. median (and p99) latency scatter, one point per (system,
   workload).
2. The paper's table (CPU reduction, median and p99 latency speedup vs. each
   baseline), at this scale.
3. ASAPQuery's measured cost against the plan's predicted cost (a check of the
   cost model).

## 10. Time and resources (estimates)

| Step | Wall time | Nodes |
|---|---|---|
| Re-plan at 10/s, build, verify | < 1 h | 1 |
| Accelerated-backfill pilot | ≈ 2 h | 5 |
| mixed: bulk 24 h history + settle + 1 h measure + ≈1 h setup | bulk duration + ≈2 h | 5 (ASAPQuery, Prometheus, VictoriaMetrics, ClickHouse, ClickHouse + MV) |
| all: same | bulk duration + ≈2 h | 3 (ASAPQuery, ClickHouse, ClickHouse + MV); Prometheus and VictoriaMetrics reuse mixed's run, since their data and queries are identical |
| **Total** | determined by measured bulk throughput; expected below 2.3 days | the five c6320 nodes (clnode109, 155, 178, 138, 167) |

On one node, one system at a time, the same runs take ≈ 8.5 days.

Storage per node for a 25 h run (estimates): ClickHouse ≈ 150–250 GB for mixed
(1.8e10 rows) and ≈ 300–400 GB more for all's records; Prometheus ≈ 25–30 GB;
VictoriaMetrics ≈ 10–20 GB. Each node has ≈ 800 GB on `/mydata`; clean up
between rounds.

## 11. What exists and what is left

| Piece | Where | Status |
|---|---|---|
| Workload tables with e2e blocks; templates 7/9 dropped; §6.3 rerun | sketch-bench [#209](https://github.com/ProjectASAP/sketch-bench/pull/209), [#210](https://github.com/ProjectASAP/sketch-bench/pull/210), [#212](https://github.com/ProjectASAP/sketch-bench/pull/212) | merged |
| Paced feeder: facts-driven classic data, records, per-series order | [#840](https://github.com/ProjectASAP/ASAPQuery/pull/840) → [#842](https://github.com/ProjectASAP/ASAPQuery/pull/842) → [#843](https://github.com/ProjectASAP/ASAPQuery/pull/843) | merged |
| ClickHouse SQL and exact MV arms from the tables | [#844](https://github.com/ProjectASAP/ASAPQuery/pull/844) → [#845](https://github.com/ProjectASAP/ASAPQuery/pull/845) | merged |
| Feeder + aligned query client in `experiment_run_e2e.py` | [#841](https://github.com/ProjectASAP/ASAPQuery/pull/841) | merged for paced classic workloads; bulk-history mode with a bounded, frozen sender configuration and phase-boundary metadata remains to do |
| `--planner saved`; prebuilt configs in both runners | [#828](https://github.com/ProjectASAP/ASAPQuery/pull/828), [#860](https://github.com/ProjectASAP/ASAPQuery/pull/860) | merged |
| Tables and plans at 10 samples/s | sketch-bench [#212](https://github.com/ProjectASAP/sketch-bench/pull/212) | merged |
| Feeder → VictoriaMetrics (remote write into vmsingle) | `experiment_run_e2e.py` | to do |
| Feeder → ClickHouse live ingest, tables/views before data, server CPU/memory monitoring | [#875](https://github.com/ProjectASAP/ASAPQuery/pull/875), `experiment_run_clickhouse.py` | live feed and pre-feed tables/views merged; server CPU/memory monitoring remains to do |
| No query timeouts on the baselines (Prometheus `--query.timeout`, VictoriaMetrics search limits, ClickHouse client) | [#861](https://github.com/ProjectASAP/ASAPQuery/pull/861) | merged |
| Analysis: per-system CPU/memory/latency/accuracy, planner cost, scatter, table | `asap-tools/experiments/post_experiment/` | to do |
| ASAPQuery multi-grouping (all): `--planner saved` for SQL | sketch-bench [#209](https://github.com/ProjectASAP/sketch-bench/pull/209), [#856](https://github.com/ProjectASAP/ASAPQuery/pull/856), [#844](https://github.com/ProjectASAP/ASAPQuery/pull/844) | merged: structured table entries carry the ASAPQuery SQL; the planner reads it; the ClickHouse arm substitutes only `NOW()`. |
| Sending SQL to ASAPQuery from a runner | [#874](https://github.com/ProjectASAP/ASAPQuery/issues/874) | open for a live-fed sketchdb arm: wire remote-write ingest and shared ASAPQuery/ClickHouse feed into `experiment_run_clickhouse.py` |
| 100 ms exact ClickHouse MV arm | [#876](https://github.com/ProjectASAP/ASAPQuery/issues/876) | open; not required by the current rapid plan (instant queries repeat every second), but needed if it later adds a 100 ms query cadence |
| Rapid randomized workload manifests and offline planner inputs | [#877](https://github.com/ProjectASAP/ASAPQuery/pull/877) | merged; executing them depends on the remaining runner work above |

## 12. Open questions (proposed answers)

1. **Q1, rate.** 10 samples/s per series, all other §6.3 parameters unchanged.
   *Proposed: yes.*
2. **Q2, the two template-16 5m RQEs below the accuracy curves at 10/s**
   (§6). *Proposed: report them as unservable in the plan and leave their two
   queries out of the e2e run (all: 96 queries), rather than change the
   template.*
3. **Q3, baseline queries slower than T.** No timeouts (decided). *Proposed:
   keep firing on schedule and report each query's full latency and how many
   firings finished after their next firing was due.* The alternative (fire
   once per hour) understates the baselines' CPU.
4. **Q4, all on Prometheus/VictoriaMetrics.** *Proposed: classic part only,
   reused from mixed; multi-grouping shown as "not expressible in PromQL".*
5. **Q5, ASAPQuery's instant queries.** *Proposed: keep ASAPQuery's semantics
   (all samples of the last second) and state it; accuracy is measured in its
   semantics.*
6. **Q6, top-k accuracy.** The plan's CMS reaches precision 0.9375 on two of
   three data seeds (sketch-bench [#211](https://github.com/ProjectASAP/sketch-bench/issues/211)). *Proposed: run with the plan as
   solved, report precision per query; revisit if [#211](https://github.com/ProjectASAP/sketch-bench/issues/211) changes the accuracy
   check.*
7. **Q7, the disk tier is not ready when the runs start.** *Proposed: run
   with CircularBuffer alone and say in the paper that ASAPQuery keeps only the
   windows its RQEs read; add the disk tier's write cost from a short separate
   measurement.*
8. **Q8, accelerated history.** *Proposed: bulk-load the real 24 h event-time
   history through each system's normal write path and retain phase-tagged CPU
   measurements for both history construction and the paced tail.* The analysis
   chooses their final cost presentation after inspecting the measurements. The
   accelerated-backfill pilot in §8 must pass before this replaces the original
   paced 24 h fill.

## 13. Blocked on others

| Item | Owner | Blocks |
|---|---|---|
| [#800](https://github.com/ProjectASAP/ASAPQuery/issues/800): planner retention bound | merged: MILP output now bounds retention with CircularBuffer and spills evicted windows to disk | resolved |
| [#856](https://github.com/ProjectASAP/ASAPQuery/issues/856): `--planner saved` for SQL, reading the tables' `sql` strings | merged | resolved |
| Engine roll-ups for keyed aggregations ([#846](https://github.com/ProjectASAP/ASAPQuery/issues/846)) | Milind | the all plan's 52 rolled-up multi-grouping queries, if [#851](https://github.com/ProjectASAP/ASAPQuery/issues/851)/[#852](https://github.com/ProjectASAP/ASAPQuery/issues/852) don't cover them |

Not blocking, reported on [#828](https://github.com/ProjectASAP/ASAPQuery/issues/828): `IncreaseAccumulator` treats an out-of-order
sample as a counter reset (the feeder now keeps per-series order), and sliding
windows don't answer on partial data (the 24 h fill avoids it).

## 14. Known limitations

- One data seed per run; the top-k result depends on the seed ([#211](https://github.com/ProjectASAP/sketch-bench/issues/211)).
- 1/20 of the §6.3 rate; costs are not the §6.3 numbers scaled.
- History is bulk-loaded, not paced for 24 wall-clock hours. Bulk CPU seconds
  are retained with a separate phase label; batching and compaction can produce
  a different physical layout and CPU cost from paced ingestion.
- The feeder shares the node with the system it feeds; only the system's own
  processes are measured.
- Prometheus and VictoriaMetrics answer only the classic queries.
- VictoriaMetrics evaluates PromQL with MetricsQL semantics (e.g. `rate` and
  range windows use the sample before the window), so its answers are compared
  with Prometheus's, not required to match them.

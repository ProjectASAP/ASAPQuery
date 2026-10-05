# Evaluation plan: ASAPQuery data plane vs ClickHouse with MVs and summaries

Status: **draft for discussion**, no results yet. Backs the paper subsection
"ASAP's data plane". Companion to the planner-level plan in
`docs/evaluation/autosketch-vs-planner.md` (branch
`docs/autosketch-vs-planner-eval`), whose synthetic data model this plan reuses.

The data plane under test is **ASAPQuery's** precompute engine and query
engine (`asap-query-engine`, run as `query_engine_rust`), driven by
`asap-tools/experiments/experiment_run_clickhouse.py`.

## 1. Question

Fix a dataset and a query workload. Run the planner once and get its set of
summary-based plans. Deploy that **same** plan set two ways:

1. **ASAPQuery**: the precompute engine builds the planned summaries, and the
   query engine answers SQL from them.
2. **ClickHouse**: the closest equivalent ClickHouse can express, which is
   materialized views (MVs) over the raw table that store ClickHouse's built-in
   aggregate states.

How much cheaper and faster is (1) than (2), at what accuracy?

The planner is held fixed, so this isolates the data plane. ClickHouse cannot
deploy MVs or summaries automatically, so the harness translates each plan.
Where ClickHouse cannot express a planned summary, the translation records the
reason. That list becomes the paper's "ClickHouse may not support it" point:

- (a) the sketch is not implemented: Count-Min, KLL;
- (b) the sketch's parameters are not configurable: `quantilesState`'s
  reservoir is fixed at 8192, `uniqHLL12` is fixed at 12 bits, and `topK`'s
  load factor is fixed.

## 2. What exists today

Checked on `main` @ `52821e0`.

### 2.1 `experiment_run_clickhouse.py`

This is a single-node runner (`num_nodes=1`; `node_offset` picks the node).

1. rsync the JSONL dataset to the node and start ClickHouse in Docker
   (`ClickHouseService`).
2. **Load the data once** (`ClickHouseDataLoaderService`), under an ingest
   monitor that samples the `clickhouse` process. With
   `dataset.init_sql_file` set, that SQL owns all DDL, *including MVs*. It runs
   before the load, so the MVs fill on insert.
3. Loop over the modes in `experiment_params.experiment` (`[{mode, server}]`).

   **`sketchdb`:**
   - `generate_sql_planner_input` builds the planner input.
   - `asap-planner` runs in SQL mode and writes `streaming_config.yaml` +
     `inference_config.yaml`.
   - `query_engine_rust` starts. It ingests the **same JSONL file**
     (`ingest_json_config`), builds summaries, and forwards unsupported
     queries to ClickHouse.
   - After `flow.steady_state_wait`, the query client runs against `:8088`
     under one continuous monitor that covers ingest + query, with per-thread
     attribution (`pc-worker` threads = precompute).

   **Any other mode** (`baseline`, `baseline_mv`, `baseline_sketch` or
   `baseline_mv_sketch`, defined in `constants.py`): the query client runs
   against ClickHouse `:8123` with that mode's SQL file.
4. rsync the results back.

`query_groups[i].sql_file` is a dict keyed by mode. `sketchdb` always reuses
`baseline`'s SQL text so the engine can pattern-match it.

The mode names, per-mode SQL and MV-capable init SQL already exist. Nothing
generates the MV DDL or the MV-targeted SQL.

### 2.2 SQL shapes ASAPQuery serves

From `sql_utilities` (`sqlpattern_matcher.rs`, `sqlhelper.rs`):
- `SUM`, `COUNT`, `AVG`, `MIN`, `MAX` and `QUANTILE`, with `GROUP BY` labels
  over a `WHERE ts BETWEEN …` window;
- `COUNT(DISTINCT col)`, normalised to `CARDINALITY` and served by HLL;
- top-k as `ORDER BY SUM|COUNT(..) DESC LIMIT k`, served by
  `CountMinSketchWithHeap`;
- nested subqueries (spatial-of-temporal).

There is no `rate`/`increase` and no binary operator between aggregates.

Example of the shape the engine matches (from the deleted
`clickhouse_quantile_queries.sql`):

```sql
SELECT quantile(0.95)(v1) FROM h2o_groupby
WHERE timestamp BETWEEN '2024-01-01 00:00:00' AND '2024-01-01 00:00:10'
GROUP BY id1, id2;
```

### 2.3 Planner: sketch-bench `rqe-optimizer`

The plan comes from **sketch-bench `rqe-optimizer`**, the MILP used in the
AutoSketch plan (checked at sketch-bench `964ccb5`). The `sketchdb` mode
currently runs `asap-planner`'s hardcoded SQL generator instead, so the runner
needs to bypass it (§5).

What `rqe-optimizer` provides today:
- **A library, not a CLI.** RQEs and label sets are built in Rust
  (`examples/small_problem.rs`), and `milp::minimize_cost` solves them.
- **Input:**
  - `Rqe {capability, lookback S, interval T, labels, accuracy metric/tolerance}`;
  - `LabelSetInfo {cardinality, arrival_rate}`;
  - the measured `AtomicCostTable` from `scripts/export_rqe_optimizer_costs.sh`.
- **Output:** a `Mapping` from each RQE to a
  `Deployment {capability, labels, config: AtomicCostEntry, window x, slide y}`.
  `config.sketch` is a sketch-bench variant, and `config.sketch_config` holds
  its parameters. It is not written to a file.
- **Capabilities:** `Freq`, `Quantile`, `Cardinality` and `TopK`. Exact
  Sum/MinMax are not capabilities.

Mapping its variants to ASAPQuery `aggregationType`s:

| `rqe-optimizer` variant | ASAPQuery `aggregationType` | Status |
|---|---|---|
| `cms-fastpath-vector2d` `{rows, cols}` | `CountMinSketch` `{depth, width}` | ok |
| `cms-heap-topk-fastpath-vector2d` `{rows, cols}` | `CountMinSketchWithHeap` `{depth, width, heap}` | ok |
| `kll-percall` `{k}` | `DatasketchesKLL` `{K}` (per group via `labels.grouping`) | ok |
| `hll` `{precision}` | `HLL` | ok |
| `countsketch-fastpath-vector2d`, `dd`, `univmon-cardinality` | none | **Exclude from candidates.** ASAPQuery can't deploy them |

The optimizer's costs are measured on sketch-bench's implementations, not
ASAPQuery's. The plan's *estimated* cost is reported next to the *measured* one,
and the gap between them is a result in its own right.

### 2.4 Gaps

| Gap | Impact |
|---|---|
| `config/experiment_type/clickhouse.yaml` is used in the runner docstring but was never committed | Can't run from a clean checkout |
| `execution-utilities/benchmark/generate_queries.py` was deleted in #404 | No SQL workload generator |
| The MV init SQL examples (`netflow_init.sql`, `quantile_demo/init.sql`) are referenced in comments but absent | No template for the MV arms |
| No Zipf/Pareto **JSONL** generator. The fake exporter's Zipf is Prometheus-only, with a fixed α=1.01 | Can't sweep skew or cardinality |
| Data loads once per run, then the modes loop | MV maintenance would be charged to *every* ClickHouse mode, and `baseline` inserts would slow down. Run each arm separately (§4) |
| `sketchdb` requires the ClickHouse raw load for fallback | Report ASAP's cost without it, and require 0 fallbacks |
| No way to feed an externally chosen plan to `sketchdb`: it always runs `asap-planner` | Needed to deploy the `rqe-optimizer` plan |
| `rqe-optimizer` has no workload-file input or plan-file output | Needed to drive it from the sweep |
| `post_experiment/single_experiment/compare_costs.py` keys on the Prometheus process | Needs ClickHouse and precompute paths |
| `cloudlab_setup/single_node/constants.sh` assumes `/scratch` | These nodes use `/mydata` (§9) |

## 3. Workload

### 3.1 Data

The data model is the one in `autosketch-vs-planner.md` §6, written as JSONL
for both systems. ASAPQuery's JSON ingest and ClickHouse's loader read the
**same file**.

- Rows are `{ts, label_0, instance, value}`.
- `label_0` takes `C` values (the groups). `instance` takes `s` values per
  group.
- Each series emits 100 samples/s of event time.
- Frequency and top-k: the weight of each key follows Zipf θ.
- Quantiles: values follow Pareto a.
- Generation is seeded, so every arm and trial reads identical bytes.

Event time is replayed, not real time: generate a fixed span (default 2 h of
event time) and let each system ingest as fast as it can. Ingest throughput is
reported separately.

### 3.2 Query templates (SQL)

These are the AutoSketch plan's templates that ASAPQuery's SQL path can serve
(§2.2), plus count-distinct, which SQL adds. `W` is the window, and queries
repeat every `T` over the span.

| # | SQL (per window) | `rqe-optimizer` RQE (capability, labels) | Mirrors AutoSketch template |
|---|---|---|---|
| 1 | `SUM(value) … GROUP BY label_0` | Freq, `{label_0}` → CMS | 1, 8 |
| 2 | `label_0, SUM(value) … GROUP BY label_0 ORDER BY 2 DESC LIMIT 3` | TopK, `{label_0}` → CMS with heap | 2 |
| 3 | `quantile(q)(value) … GROUP BY label_0`, q ∈ {0.5, 0.75, 0.9, 0.95, 0.99} | Quantile, `{label_0}` → KLL per group | 3 |
| 4 | `SUM(value) … GROUP BY label_0, instance` | Freq, `{label_0, instance}` → CMS | 4 |
| 5 | `quantile(q)(value) … GROUP BY label_0, instance`, same q | Quantile, `{label_0, instance}` → KLL per series | 5 |
| 6 | `COUNT(DISTINCT instance) … GROUP BY label_0` | Cardinality, `{label_0}` → HLL | none (SQL-only) |

Each template is one RQE per window in `W`, repeating every `T`. Quantiles of
one template share a stream, so the optimizer can serve all five from one
deployment.

Dropped, because ASAPQuery SQL cannot express them: AutoSketch templates 6, 7
and 9 (`rate`), and 10 (a ratio of two quantiles). The earlier `MAX` control is
also dropped: `rqe-optimizer` has no MinMax capability, so it would not come
from the plan.

### 3.3 Grid

One dimension at a time around the bold defaults:

| Dimension | Values |
|---|---|
| Query mix | **all 6**, frequency only {1, 4}, quantile only {3, 5}, top-k only {2} |
| `C` | 1e2, **1e3**, 1e4, 1e5 |
| `s` | 1, 10, **100** |
| θ / a | θ ∈ {0, 0.5, **1.0**, 1.5}; a ∈ {1.1, **2**, 3} |
| Window `W` / repeat `T` | {10 s}, **{1 m}**, {10 m}; `T = W` (tumbling) |
| Accuracy target | 90%, **95%**, 99% |

## 4. Arms

All arms see the same rows and the same plan set. **Each arm is a separate
runner invocation** (`experiment=[<one mode>]`), with its own `init_sql_file`,
so ingest cost isn't cross-charged.

| Arm (runner mode) | Init SQL | Role |
|---|---|---|
| `baseline` | raw `MergeTree` only | Exact reference and accuracy ground truth |
| `baseline_mv` | raw + MVs with exact states (`sumState`, `countState`, `maxState`, `quantilesExactState`, `uniqExactState`, per-key `sumState` for top-k) | "MVs without summaries" |
| `baseline_mv_sketch` | raw + MVs with the closest summary state (table below) | **Main ClickHouse point** |
| `sketchdb` | raw only (for fallback) | **ASAPQuery**. Must report 0 fallbacks |

Each MV is keyed by `(toStartOfInterval(ts, W), group-by labels)`. The
`baseline_mv*` SQL files query the MV target tables with the `-Merge`
combinators. Every mode's `sql_file` entry in `query_groups[i].sql_file`
covers the same logical query.

Plan → ClickHouse mapping for `baseline_mv_sketch`:

| Planned summary | ClickHouse state | Gap reported |
|---|---|---|
| KLL (`DatasketchesKLL`) | `quantilesState(levels)(v)` (reservoir 8192) or `quantilesTDigestState` | No KLL. Size/accuracy cannot be set |
| CountMin (`CountMinSketch`) | none: exact `sumState` per key | Not implemented. State is one row per key, not `d×w` |
| CountMin with heap | `topKWeightedState(k)` (Filtered Space-Saving) | No CMS. The load factor cannot be set |
| HLL | `uniqHLL12State` or `uniqCombinedState(p)` | `uniqHLL12` is fixed at 12 bits. `uniqCombined` lets you set p |

The translator emits every row of this table. A "none" row becomes an exact
state, and it is counted and listed under the figure.

## 5. Plan source and deployment

The pipeline for one (dataset, workload) point:

1. **Workload → RQEs.** From the template set, `W`, `T`, `C`, `s` and the
   accuracy target, emit `workload.json`:
   - one RQE per (template, window);
   - one label set per grouping, with `cardinality` (`C`, or `C·s`) and
     `arrival_rate` (`100·C·s`/s at the replay rate).

   Accuracy metrics follow `small_problem.rs`:
   - `are_top100` for Freq;
   - `mean_rank_err` for Quantile;
   - `relative_error` for Cardinality;
   - `precision_at_k` (a floor) for TopK.
2. **Solve.** A new `rqe-optimizer` binary (in sketch-bench) reads
   `workload.json` and the `AtomicCostTable`, with the candidate families
   restricted to the ASAPQuery-deployable variants (§2.3). It runs
   the objective chosen in D1a and writes `plan.json`:
   - per deployment: capability, labels, sketch variant + `sketch_config`,
     `x`, `y`, and the RQEs it serves;
   - the MILP's estimated CPU, memory and $/h.
3. **Translate** (new, in this repo). `plan.json` is the single source for
   every arm:
   - ASAPQuery `streaming_config.yaml`: one aggregation per deployment, with
     `aggregationType`/`parameters` from §2.3, `labels.grouping`,
     `windowSizeMs = x` and `windowType` tumbling if `x = y`, else sliding with
     slide `y`;
   - `inference_config.yaml`: each RQE's SQL → its aggregation ID;
   - the ClickHouse `init.sql` for `baseline_mv` and `baseline_mv_sketch`
     (§4), plus the per-mode SQL files.

   The two systems therefore cannot drift apart.
4. **Deploy.** Add a runner option (e.g. `controller.plan_dir=…`) that skips
   `asap-planner` in `sketchdb` mode and rsyncs the translated configs into
   `controller_output/`. Everything after that stays as it is.

**Check before building:** whether `query_engine_rust` accepts sliding windows
(`x ≠ y`) for every summary type from SQL. If not, constrain the optimizer to
`y = x` and note it.

`sketchdb` ingests by itself from the JSONL. Its cost excludes the ClickHouse
raw load, which exists only for fallback. Every RQE counted as served must show
0 forwards to ClickHouse, taken from the query-engine logs.

## 6. Metrics

| Metric | ClickHouse arms | `sketchdb` |
|---|---|---|
| Ingest CPU (core-s), including MV/summary maintenance | `monitor_output_ingest.json` (`clickhouse` process) | `pc-worker` threads in `monitor_output.json` |
| Retained state bytes | `system.parts.bytes_on_disk` + `primary_key_bytes_in_memory` for the MV target tables (raw table reported separately) | query-engine store size (to add if not already exported) |
| RSS (steady and peak) | process | process |
| Query latency p50/p95/p99 | prometheus-client per query (`analyze_latencies.py`) | same |
| Query CPU | monitor during the query phase | same |
| Accuracy | vs `baseline` (`calculate_fidelity.py --exact_experiment_mode baseline`): rank error for quantiles (as in #784), relative error for sums, recall@k for top-k, relative error for distinct counts | same |

**Cost.** Use the AutoSketch plan's **model A**
(`$/h = a·avg vCPU + b·GiB`, with `a ≈ 0.0368` and `b ≈ 0.00364` from the
2026-10-04 EC2 fit) over measured ingest + query CPU and retained state memory.
This keeps the planner and data-plane sections in the same units. Report raw
CPU-seconds and bytes as well.

**Fairness**
- Same node type: `c6320` (2× E5-2683 v3, 56 threads, 251 GB).
- ClickHouse and `query_engine_rust` get the same cores and memory cap
  (`docker --cpuset-cpus/--memory` and `taskset`/cgroup).
- ClickHouse image pinned by tag or digest (`dataset.clickhouse_image_tag`),
  not `latest`.
- Query cache off (`use_query_cache=0`). The first query pass is a discarded
  warm-up.
- 3 trials per point. Report the median, with min/max error bars.

## 7. Open decisions

- **D0: data plane.** Decided: ASAPQuery `query_engine_rust`
  (`experiment_run_clickhouse.py` `sketchdb`).
- **D1: plan source.** Decided: sketch-bench `rqe-optimizer` (§2.3, §5).
- **D1a: the optimizer objective.** At sketch-bench `964ccb5`, `rqe-optimizer`
  offers `minimize_tco` (minimum steady-state CPU) and `minimize_cost(family)`
  (fractional EC2 instances of one family, from #137). The model A and model B
  objectives in the AutoSketch plan are not in the code yet. Proposal: use
  `minimize_cost` with `general_purpose` now, and switch to model A when it
  lands. Every arm is still measured and priced under model A.
- **D2:** keep `baseline_mv` as an extra bar, or show only `baseline_mv_sketch`.
- **D3:** the ClickHouse quantile state, `quantilesState` or
  `quantilesTDigestState`. Report the one closest to ASAP in accuracy, and put
  the other in the appendix.
- **D4:** whether to add `rate` or a quantile ratio to the ASAPQuery SQL path,
  to cover all 10 AutoSketch templates.

## 8. Work items (each a small PR)

1. Data generator: seeded JSONL with Zipf θ weights and Pareto a values over
   `(C, s)`, plus a test on the empirical rank-frequency slope.
2. Workload generator for templates 1–6 over the grid. It emits both the SQL
   files (replacing the deleted `generate_queries.py`) and `workload.json` for
   `rqe-optimizer`, and includes a differential test against ClickHouse on a
   small `C, s`.
3. **sketch-bench PR:** an `rqe-optimizer` binary with
   `workload.json` + cost table → `plan.json`, plus a flag restricting the
   candidate families.
4. Plan translator: `plan.json` in; the ASAPQuery configs and the ClickHouse
   `init.sql` + per-mode SQL out. It logs every unsupported mapping.
5. Runner:
   - commit `config/experiment_type/clickhouse.yaml`;
   - add `controller.plan_dir` to skip `asap-planner` and deploy the
     translated configs;
   - collect MV bytes from `system.parts` after the load.
6. Post-processing: ClickHouse and precompute paths in `compare_costs.py`,
   model-A pricing, and the figure script.
7. Sweep driver that fans out (workload, arm, trial) across node1–3.

## 9. Cluster and run procedure

CloudLab experiment `scratch2.cloudmigration-pg0.clemson.cloudlab.us`.
`node1`–`node3` run 3 configs in parallel (`node_offset` 1/2/3,
`num_nodes=1`). `node1` is also the orchestrator.

Setup:
1. `/mydata` (845 GB ext4) is already mounted, world-writable, on node1–3. The
   setup scripts expect `/scratch`, so symlink it or make the root volume
   configurable.
2. `deploy_from_scratch.sh` without its storage step: Docker, the Rust
   toolchain, and `cargo build --release` for `asap-query-engine` and
   `asap-planner-rs`, plus sketch-bench (`rqe-optimizer`, and the cost table
   from `scripts/export_rqe_optimizer_costs.sh`).
3. The orchestrator needs SSH to `node{1,2,3}.<suffix>`, including itself.
4. Generate datasets once into `/mydata/datasets/`. The runner rsyncs them.

Order:
1. **Pilot**: the default workload with `C=1e3`, `s=10`, all 4 arms, 1 trial.
   Check:
   - 0 fallbacks in `sketchdb`;
   - exact MV results equal `baseline`;
   - accuracy is within target.
2. **Sweep**: the §3.3 dimensions one at a time
   (3+3+2+5+2+2 = 17 non-default points + default = 18 workloads),
   × 4 arms × 3 trials = **216 runs**. Allowing about 15 min per run, that is
   about 18 h on 3 nodes.
3. **Figure**: grouped bars of model-A $/h and p95 latency per workload, with
   `sketchdb` ÷ `baseline_mv_sketch` annotated (the "up to X×" numbers). Each
   bar carries its accuracy, and the plans ClickHouse can't express are listed
   under the figure.

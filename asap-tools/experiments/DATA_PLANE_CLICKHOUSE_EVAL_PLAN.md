# Evaluation plan: ASAPQuery data plane vs ClickHouse with MVs and summaries

Status: **design settled, implementation not started**. Backs the paper
subsection "ASAP's data plane". The data model follows the synthetic workload in
`docs/evaluation/autosketch-vs-planner.md` (branch
`docs/autosketch-vs-planner-eval`), narrowed as described in §3.

## 1. Question

Fix a dataset and a query workload, and let the planner choose its
summary-based plan. Deploy that **same** plan two ways:

1. **ASAPQuery**: the precompute engine and query engine (`query_engine_rust`).
2. **ClickHouse**: materialized views (MVs) over the raw table, with exact
   states or ClickHouse's own sketch states.

How much cheaper and faster is (1) than (2), at the same accuracy target?

The first pass covers **quantile queries only**, with **DDSketch on both
sides**: the same algorithm and the same relative-accuracy parameter α. Only
the data plane differs between them. The frequency and top-k templates are
deferred (§10); for those, ClickHouse has no equivalent sketch.

## 2. Decisions

| # | Decision |
|---|---|
| Data plane | ASAPQuery `query_engine_rust`, via `experiment_run_clickhouse.py`. Not ASAPQuery-backend |
| Plan source | The sketch-bench MILP via `asap-planner --planner milp` ([#753](https://github.com/ProjectASAP/ASAPQuery/issues/753), Milind). The planner's `streaming_config.yaml` **is** the plan. No separate plan file |
| Optimizer objective | `minimize_cost` for the `general_purpose` EC2 family, until model A is in code |
| Cost reporting | One run per arm, priced two ways from the same CPU/memory timeline: usage-based (model A) and peak-provisioned EC2 (model B) (§6) |
| Quantile sketch | DDSketch on both sides: ClickHouse `quantilesDD(α, …)`; ASAPQuery `DDSketch` ([#792](https://github.com/ProjectASAP/ASAPQuery/pull/792), part of [#787](https://github.com/ProjectASAP/ASAPQuery/issues/787)). The optimizer's quantile candidates are limited to `dd` |
| KLL | Appendix only: the optimizer may choose KLL, and ClickHouse uses its closest substitute |
| ClickHouse bars | Exact MV, and MV + sketch state. Plain ClickHouse is the accuracy reference (in a table, not a bar) |
| Key tracker (`DeltaSetAggregator`) | Not needed for quantile plans. For the later frequency/top-k pass: if it is still required, it is deployed, measured and reported as ASAP's cost. Milind is checking whether it can be removed, and a cost-table row is being added separately |
| Ingest | Paced at the workload's arrival rate, with queries running during ingest |
| Rate | **1 sample/s per series**, so the total rate is `C·s` samples/s |
| Fixed data parameters | θ = 1.1, Pareto a = 1.5, accuracy target 95%. Not swept |
| Templates | Quantile only (§3.2). Per group in the main figure; per series in the appendix |
| Run length | 15 min of event time per run (largest W = 10 min), plus one 1 h run of the default workload |
| Resources | 16 cores / 64 GiB cap per system under test. The feeder and fallback ClickHouse get separate cores |
| Pilot | Run now with ASAPQuery on KLL from today's `asap-planner`, to debug the plumbing; not reported. Rerun once DD and `--planner milp` land |

## 3. Workload

### 3.1 Data

- One metric with labels `label_0` (`C` groups) and `instance` (`s` series per
  group), so there are `C·s` time series.
- Every series emits **one sample per second** of event time.
- Values are drawn from **Pareto a = 1.5**.
- θ = 1.1 (Zipf key weights) is fixed for the later frequency/top-k pass. It
  has no effect on the quantile-only pass, because every series emits at the
  same rate.
- The data is generated on the fly by the paced feeder
  ([#793](https://github.com/ProjectASAP/ASAPQuery/pull/793)), with no dataset
  file. Each value is a hash of `(seed, series, sample index)`, so the ClickHouse
  and ASAPQuery feeders send identical data. A file would be about 9e8 rows
  (~50 GB) per workload at 1e6 series × 900 s.

### 3.2 Queries

Each query repeats every `T = 1 min` (a refreshing dashboard panel) over the
last `W`, with q ∈ {0.5, 0.75, 0.9, 0.95, 0.99}:

```sql
-- per group (main figure)
SELECT label_0, quantiles(0.5, 0.75, 0.9, 0.95, 0.99)(value) FROM data
WHERE ts BETWEEN now() - W AND now() GROUP BY label_0;

-- per series (appendix)
... GROUP BY label_0, instance;
```

The ASAPQuery SQL path matches these shapes (`QUANTILE` with `GROUP BY` labels
over a `WHERE ts BETWEEN …` window). The five quantiles read one stream, so a
single deployment serves all of them.

Values per sketch per window are `s·W` per group and `W` per series. With the
defaults (s = 100, W = 1 min), that is 6,000 per group and 60 per series. This
is why per series is in the appendix: sketches that small mostly measure
per-sketch overhead, not summarization.

### 3.3 Workloads

One dimension is varied at a time, around the default:

| # | Varied | Value | C | s | W | Series | Rate (samples/s) |
|---|---|---|---|---|---|---|---|
| 1 | default | — | 1e3 | 100 | 1 m | 1e5 | 1e5 |
| 2 | C | 1e2 | 1e2 | 100 | 1 m | 1e4 | 1e4 |
| 3 | C | 1e4 | 1e4 | 100 | 1 m | 1e6 | 1e6 |
| 4 | C | 1e5 | 1e5 | **10** | 1 m | 1e6 | 1e6 |
| 5 | s | 1 | 1e3 | 1 | 1 m | 1e3 | 1e3 |
| 6 | s | 10 | 1e3 | 10 | 1 m | 1e4 | 1e4 |
| 7 | W | 10 s | 1e3 | 100 | 10 s | 1e5 | 1e5 |
| 8 | W | 10 m | 1e3 | 100 | 10 m | 1e5 | 1e5 |

Workload 4 uses s = 10, which caps every workload at 1e6 series and 1e6
samples/s.

## 4. Arms

Each arm is a **separate run**, with its own data feed and DDL, so no arm pays
for another's MV maintenance.

| Arm | Deployed | Role |
|---|---|---|
| Reference | ClickHouse raw `MergeTree`, no MVs | Accuracy ground truth (table) |
| Exact MV | MV keyed by `(toStartOfInterval(ts, x), label_0)`, storing `quantilesExactState(...)(value)` | Bar 1: what MVs alone buy |
| Sketch MV | Same MV layout, storing `quantilesDDState(α, ...)(value)` with the plan's α | Bar 2: ClickHouse's best summary option |
| ASAPQuery | `streaming_config.yaml` from `asap-planner --planner milp`, with `DDSketch(α)` | Bar 3 |

- **MV layout.** The MV's interval `x` and slide come from the plan's window
  and slide. ClickHouse queries merge the panes inside `W` with
  `quantilesExactMerge` / `quantilesDDMerge`.
- **ASAPQuery's fallback ClickHouse is empty.** Any query that falls back fails
  loudly instead of being answered by ClickHouse, so "0 fallbacks" is checked
  by construction. That ClickHouse runs on its own 2 cores / 4 GiB and is not
  counted in ASAP's cost.
- **ClickHouse version.** The image is pinned to ≥ 24.1, for `quantileDD`. The
  pilot checks that `quantilesDD` works with `-State`/`-Merge` in an
  `AggregatingMergeTree` MV.

## 5. Pipeline per workload

```
workload generator ──► SQL files + planner input + feeder flags (C, s, seed)
                       │
asap-planner --planner milp ──► streaming_config.yaml + inference_config.yaml   (the plan)
                       │
translator ──► ClickHouse init.sql (raw + MVs) and per-arm SQL
                       │
runner: one run per arm × trial, paced feeders, resource caps, monitors
```

- **Translator.** It reads ASAPQuery's `streaming_config.yaml`, which gives
  each aggregation's type, `parameters`, `labels.grouping`, `windowSizeMs` and
  `windowType`/slide. From it, the translator writes the MV DDL and the queries
  that read the MVs. Both systems therefore deploy one plan.
- **Paced feeders** (`asap-tools/data-sources/paced-feeder`,
  [#793](https://github.com/ProjectASAP/ASAPQuery/pull/793)). There is one
  feeder process per system under test, both run with the same workload flags,
  and each runs on its own 2 cores outside the caps. Every second it sends that
  second's rows:
  - ASAPQuery gets them over Prometheus Remote Write (50k rows per request,
    under axum's 2 MB body limit);
  - ClickHouse gets them via `INSERT … FORMAT RowBinary` (100k rows per
    request), its efficient native path, just as Remote Write is ASAPQuery's.
    JSONEachRow would have charged ClickHouse for JSON parsing.

  Measured pinned to 2 cores, both sinks hold 1e6 rows/s with no tick over
  1 s; the feeder uses 0.15 cores (ClickHouse) and 0.34 cores (Remote Write).
  The existing bulk paths (JSON ingest, `_load_json_batched`) do not pace.
- **Runner.** It extends `experiment_run_clickhouse.py`:
  - one mode per arm;
  - a feeder-driven ingest phase in place of the bulk load;
  - CPU/memory caps (`docker --cpuset-cpus/--memory` for ClickHouse,
    `taskset` + a memory cgroup for `query_engine_rust`);
  - collection of MV bytes from `system.parts`.

## 6. Metrics

| Metric | ClickHouse arms | ASAPQuery |
|---|---|---|
| Ingest CPU (core-s), including MV/summary maintenance | `clickhouse` process | `pc-worker` threads |
| Retained state | `system.parts.bytes_on_disk` + `primary_key_bytes_in_memory` for MV tables (raw table reported separately) | Store size |
| RSS (steady state and peak) | process | process |
| Query latency p50/p95/p99 | per query, client-side | same |
| Query CPU | process CPU over the query phase | same |
| Accuracy | Rank error vs the reference, per quantile (as in #784). Target: 95% | same |

**Cost.** Each arm runs once. The monitor samples the system's CPU and
memory every second, and that one timeline is priced two ways, using the cost
models of the AutoSketch plan:

- **Usage-based (model A).** Total CPU = the area under the CPU curve
  (CPU-seconds), divided by the run length to give average vCPU. Then
  `$/h = a·avg vCPU + b·GiB`, with `a ≈ 0.0368` and `b ≈ 0.00364` (2026-10-04
  EC2 fit) and the average retained-state memory.
- **Peak-provisioned (model B).** Take the peak of the CPU curve and peak
  memory, and buy enough instances of one EC2 family to cover both:
  `n_f = max(peak vCPU / vCPU_f, peak GiB / GiB_f)`, `$/h = n_f · price_f`.
  This is reported for c7i, m7i and r7i.

Model A shows the work each system does. Model B shows what it costs to
provision for the bursts when queries fire on top of ingest. Raw CPU-seconds,
peaks and bytes are reported too, as is the planner's estimated cost next to
the measured one.

**Fairness:**
- Same node type: `c6320`, 2× E5-2683 v3, 56 threads, 251 GB.
- Same cap for each system under test.
- Query cache off. The first query pass is discarded as warm-up.
- 3 trials per point. Report the median, with min/max error bars.

## 7. Run count and time

| Set | Runs | Time each | Wall time on 3 nodes |
|---|---|---|---|
| Main: 8 workloads × 4 arms × 3 trials, 15 min span | 96 | ~25 min | ~13 h |
| 1 h span, default workload, 4 arms × 3 trials | 12 | ~70 min | ~5 h |
| Appendix: per-series queries, default workload, 4 arms × 3 trials | 12 | ~25 min | ~2 h |
| Appendix: KLL plan, default workload, 4 arms × 3 trials | 12 | ~25 min | ~2 h |
| **Total** | **132** | | **~22 h** |

The pilot (the default workload, 4 arms, 1 trial) comes before all of these.
It checks:
- 0 fallbacks;
- exact-MV results equal the reference exactly;
- DD accuracy is within target on both sides;
- the systems absorb 1e6 samples/s under their caps (workloads 3 and 4). The
  feeders themselves are already shown to keep up at that rate.

## 8. Work items

| # | Item | Owner | Depends on |
|---|---|---|---|
| 1 | Workload generator: SQL files, planner input and feeder flags per workload | Zeying | — |
| 2 | Paced feeder with on-the-fly data generation (Remote Write and ClickHouse RowBinary) | Zeying | Draft PR #793 |
| 3 | Translator: `streaming_config.yaml` → ClickHouse `init.sql` + per-arm SQL | Zeying | — |
| 4 | Runner: per-arm modes, feeder ingest, caps, `system.parts` collection, `config/experiment_type/clickhouse.yaml` | Zeying | 2 |
| 5 | Post-processing: ClickHouse and precompute paths in `compare_costs.py`, model A and model B pricing from the monitor timeline, figure | Zeying | 4 |
| 6 | DDSketch in ASAPQuery (the DD slice of #762 + #787) | Zeying | Draft PR #792 |
| 7 | `asap-planner --planner milp` with quantiles limited to `dd` | Milind | #753 |
| 8 | Key-tracker removal check, and its cost-table row (for the later frequency/top-k pass) | Milind | — |

Items 1–5 can start now, and the pilot runs on today's `asap-planner` (KLL).
The main runs need items 6 and 7.

## 9. Cluster

CloudLab experiment `scratch2.cloudmigration-pg0.clemson.cloudlab.us`.
`node1`–`node3` run in parallel (`node_offset` 1/2/3, `num_nodes=1`), and
`node1` is also the orchestrator.

- `/mydata` (845 GB ext4) is mounted and world-writable on all three nodes.
  The setup scripts expect `/scratch`, so add a symlink or make the root volume
  configurable.
- Install Docker and the Rust toolchain, and build `asap-query-engine` and
  `asap-planner-rs` in release mode (`deploy_from_scratch.sh`, minus its
  storage step).
- The orchestrator needs SSH to `node{1,2,3}.<suffix>`, including itself.

## 10. Later passes (out of scope here)

- **Frequency and top-k templates.** Matching sketches where ClickHouse has
  them (HLL ↔ `uniqCombined(p)`). Where it has none (Count-Min, CMS-heap vs
  `topKWeighted`), the bars are labeled "no ClickHouse equivalent", and a top-k
  sketch MV that misses precision@k is shown with that marked.
- **`rate` and quantile-ratio templates.** These need ASAPQuery SQL support.
- **The connection between ASAPQuery's planner and the ILP** belongs to #753,
  not to this evaluation.

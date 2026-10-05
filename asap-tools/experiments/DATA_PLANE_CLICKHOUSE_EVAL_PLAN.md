# Evaluation plan: ASAP data plane vs ClickHouse with MVs and summaries

Status: **draft for discussion**, no results yet. Backs the paper subsection
"ASAP's data plane". Companion to the planner-level plan in
`docs/evaluation/autosketch-vs-planner.md` (branch
`docs/autosketch-vs-planner-eval`), whose synthetic workload this plan reuses.

## 1. Question

Fix a dataset and a query workload. Run the planner once and get its set of
summary-based plans. Deploy that **same** plan set two ways:

1. **ASAP data plane**: the backend precompute runtime, SummaryStore and query
   adapters serve the plan natively.
2. **ClickHouse**: the closest equivalent ClickHouse can express, which is
   materialized views (MVs) over the raw table that store ClickHouse's built-in
   aggregate states.

How much cheaper and faster is (1) than (2), at what accuracy?

The planner is held fixed, so this isolates the data plane. ClickHouse cannot
deploy MVs or summaries automatically, so the harness translates each plan by
hand. Where ClickHouse cannot express a planned summary, the translation records
the reason. That list becomes the paper's "ClickHouse may not support it" point:

- (a) the sketch is not implemented: Count-Min, KLL, keyed KLL;
- (b) the sketch's parameters are not configurable: `quantilesState`'s
  reservoir is fixed at 8192, `uniqHLL12` is fixed at 12 bits, and `topK`'s
  load factor is fixed.

## 2. What exists today

### 2.1 ASAPQuery (this repo): `experiment_run_clickhouse.py`

Checked on `main` @ `52821e0`. This is a single-node runner (`num_nodes=1`;
`node_offset` picks the node).

1. rsync the JSONL dataset to the node and start ClickHouse in Docker.
2. **Load the data once**, under an ingest monitor that samples the
   `clickhouse` process. With `dataset.init_sql_file` set, that SQL owns all
   DDL, *including MVs*. It runs before the load, so the MVs fill on insert.
3. Loop over the modes in `experiment_params.experiment`:
   - **`sketchdb`**:
     - Run `asap-planner` in SQL mode, using the **hardcoded generator, not
       the cost optimizer**.
     - Start the legacy `query_engine_rust`. It ingests the same JSONL itself
       and forwards unsupported queries to ClickHouse.
     - Wait `steady_state_wait`, then query `:8088`.
   - **Any other mode** (`baseline`, `baseline_mv`, `baseline_sketch` or
     `baseline_mv_sketch`, defined in `constants.py`): query ClickHouse
     `:8123` with that mode's SQL file. `query_groups[i].sql_file` is a dict
     keyed by mode.

The mode names, per-mode SQL and MV-capable init SQL already exist. Nothing
generates the MV DDL or the MV-targeted SQL.

### 2.2 ASAPQuery-backend: the current data plane

The backend has the precompute runtime, SummaryStore and PromQL, MetricsQL and
ClickHouse SQL adapters, each with exact fallback. Its families include KLL,
DDSketch, HLL, CountMin, CountMin with heap and TopK.

The relevant prior ClickHouse work is in two places.

**`tools/o11y-sql-main-eval/`**
- A **27-query o11y corpus** (`corpus.json`): each query appears as PromQL,
  MetricsQL and a ClickHouse SQL translation over
  `raw_samples(metric, labels Map, ts_ms, value)`.
- Matched-budget runners: 2 pinned cores, 4 GiB, a pinned ClickHouse image,
  restart per trial, query cache off, `max_threads=2`.
- Reported results:
  - Only q05, q06 and q23 run warm, all three on exact MinMax.
  - 18 queries fall back to exact ClickHouse.
  - 6 queries OOM on both sides.
  - The SQL frontend rejects ClickHouse-specific syntax: lambdas, Map access,
    `mapConcat`.

**`tools/clickhouse-benefit/`**
- A single-query warm-path probe against ClickHouse.
- A matched-trial runner.

**Limit that shapes this plan.** The backend's *automatic* ClickHouse-SQL
binder supports only bounded scalar reductions (sum, count, min, max) today.
Quantile and top-k plans are **not** warm through the SQL adapter. They are
through the PromQL adapter (`quantile_over_time`, `sum by`, `rate`).

### 2.3 Planner

The cost-optimal planner used in the paper is sketch-bench `rqe-optimizer`, a
MILP. It reads the measured `AtomicCostTable` and implements the Model A and
Model B cost models from the AutoSketch plan. `asap-planner-rs/src/optimizer`
is still offline and PromQL-only, and the SQL-mode `asap-planner` uses the
hardcoded generator.

### 2.4 Gaps

| Gap | Where |
|---|---|
| `config/experiment_type/clickhouse.yaml` is used in the runner docstring but was never committed | ASAPQuery |
| `generate_queries.py` (SQL workload generator) was deleted in #404 | ASAPQuery |
| The MV init SQL examples (`netflow_init.sql`, `quantile_demo/init.sql`) are referenced but absent | ASAPQuery |
| No Zipf/Pareto **JSONL** generator. The fake exporter's Zipf is Prometheus-only, with a fixed α=1.01 | ASAPQuery |
| Data loads once per run, so MV maintenance cost would be charged to every ClickHouse mode | ASAPQuery runner |
| `compare_costs.py` keys on the Prometheus process | ASAPQuery post-processing |
| No plan → ClickHouse MV translator | both |
| SQL adapter is not warm for quantile/top-k plans | backend |
| The setup scripts assume `/scratch`; these nodes use `/mydata` | ASAPQuery `cloudlab_setup` |

## 3. Workload

Reuse the **synthetic workload from `autosketch-vs-planner.md` §6** so both
evaluation sections share data, queries and plans. Add the real o11y corpus as
a secondary point.

### 3.1 Synthetic (main figure)

**Data**
- One metric, `data`, with labels `label_0` (`C` groups) and `instance`
  (`s` series per group).
- Each series emits 100 samples/s.
- Frequency and top-k weights follow Zipf θ. Quantile values follow Pareto a.

**Templates**
- The 10 PromQL templates from the AutoSketch plan: `sum by`, `topk by`,
  `quantile by`, `sum_over_time`, `quantile_over_time`, `rate`, their spatial
  aggregations, and the `quantile_over_time` ratio.
- Each one is paired here with a hand-checked **ClickHouse SQL** translation
  over a flat table `data(ts_ms, label_0, instance, value)`.
- A flat table is used because Map columns are what break the backend SQL
  frontend in the corpus results.

**Grid** (one dimension at a time around the bold defaults, as in that plan)

| Dimension | Values |
|---|---|
| Query mix | **all 10**, frequency only, quantile only, top-k only |
| `C` | 1e2, **1e3**, 1e4, 1e5 |
| `s` | 1, 10, **100** |
| θ / a | θ ∈ {0, 0.5, **1.0**, 1.5}; a ∈ {1.1, **2**, 3} |
| Window set `W` | {1m}, {1m, 1h}, **{1m, 10m, 1h}** |
| Accuracy target | 90%, **95%**, 99% |

The defaults are capped at `W` ≤ 1h and `C·s` = 1e5 series × 100 samples/s
(1e7 samples/s). That is too much for one ClickHouse node at real time, so the
data plane runs on **replayed time**:

- generate a fixed span (default: 2 h of event time);
- ingest it as fast as each system accepts;
- run each RQE at every `T` over that span.

The rate is a knob, not a fixed point: 1e5 samples/s by default, with ingest
throughput reported separately.

### 3.2 Real o11y corpus (secondary)

The backend's 27-query corpus already has a correctness matrix. Use only the
queries whose plans actually install. Re-run the corpus audit first, since
today that is q05, q06 and q23. Report them as "real workload" bars.

## 4. Arms

All arms see the same rows and the same plan set.

| Arm | What is deployed | Role |
|---|---|---|
| `ch_raw` | ClickHouse raw `MergeTree`, no MVs | Exact reference and accuracy ground truth |
| `ch_mv_exact` | ClickHouse with MVs per plan, exact states (`sumState`, `countState`, `min/maxState`, `quantilesExactState`, per-key `count` for top-k) | "MVs without summaries" |
| `ch_mv_sketch` | ClickHouse with MVs per plan, closest summary state (table below) | **Main ClickHouse point** |
| `asap` | Backend data plane with the same plan set | **Main ASAP point** |

Each MV is keyed by `(toStartOfInterval(ts, x), group-by labels)`, where `x`
is the plan's window/pane. Queries merge `S / x` panes with the `-Merge`
combinators, mirroring the plan's `(x, y)` and merge pattern.

Plan → ClickHouse mapping for `ch_mv_sketch`:

| Planned summary | ClickHouse state | Gap reported |
|---|---|---|
| Sum / count | `sumState` / `countState` | none: an exact parity control |
| MinMax | `minState` / `maxState` | none: a control (matches corpus q05/q06/q23) |
| KLL (per group or per series) | `quantilesState(levels)(v)` (reservoir 8192) or `quantilesTDigestState` | No KLL. Size/accuracy cannot be set: a reservoir is either over-sized or misses the target |
| DDSketch | `quantilesDDState(relative_accuracy, …)` | Closest match. Parameterized, so expected near-parity. Report it honestly |
| CountMin (frequency) | none: exact `sumState` per key | Not implemented. Cost scales with key cardinality, so state is `C·s` rows, not `d×w` |
| CountMin with heap / TopK | `topKWeightedState(k)` (Filtered Space-Saving) | No CMS. The load factor cannot be set |
| HLL | `uniqHLL12State` / `uniqCombinedState(p)` | `uniqHLL12` is fixed at 12 bits; `uniqCombined` lets you set the precision |

The translator emits every row of this table. A plan that falls into a "none"
row becomes an exact fallback and is counted.

## 5. Running ASAP on the same plan

1. Run `rqe-optimizer` on the workload, with the model A cost and the default
   accuracy target. Its output is the plan set.
2. Translate it into a backend `PhysicalPlanInstallRequest` / `PlanEnvelope`
   publication. The compiler path already exists for Planner DAGs. **Check
   with the backend owners whether an `rqe-optimizer` plan can be published
   directly, or only through ASAPPlanner.**
3. Ingest:
   - **ASAP**: Prometheus Remote Write (or the backfill reader from the
     ClickHouse raw table, `storage_engines/sketch_db/backfill/clickhouse_reader.rs`).
   - **ClickHouse**: JSONL bulk insert.

   Report the ingest cost of each path separately. Do not charge ASAP for the
   ClickHouse raw load unless fallback is needed.
4. Queries:
   - **ASAP**: PromQL against the backend. Quantile and top-k are not warm
     through the SQL adapter yet (§2.2).
   - **ClickHouse**: the paired SQL.

   Correctness is checked per query, as the corpus runner does it: compare the
   decoded results against `ch_raw` within the accuracy target.
5. Require **0 fallbacks** on the ASAP side for every RQE counted as served.
   Count and report any fallback separately, as the corpus report does.

If the team prefers the legacy ASAPQuery `query_engine_rust` path in
`experiment_run_clickhouse.py`, which is SQL end to end, steps 2 and 4 change.
That path cannot take an `rqe-optimizer` plan either (decision D1).

## 6. Metrics

| Metric | ClickHouse | ASAP |
|---|---|---|
| Ingest CPU (core-s), including MV/summary maintenance | `clickhouse` process during load (existing ingest monitor) | backend precompute threads |
| Retained state bytes | `system.parts.bytes_on_disk` + `primary_key_bytes_in_memory` for the MV target tables (raw table reported separately) | SummaryStore state directory + in-memory instances |
| RSS (steady and peak) | process | process |
| Query latency p50/p95/p99 | client-side per request, warm (10 warm-ups discarded) | same |
| Query CPU | process CPU delta over the query phase | same |
| Accuracy | vs `ch_raw`: rank error for quantiles (as in #784), relative error for sums, recall@k for top-k | same |

**Cost.** Use the AutoSketch plan's **model A**
(`$/h = a·avg vCPU + b·GiB`, with `a ≈ 0.0368` and `b ≈ 0.00364` from the
2026-10-04 EC2 fit) over measured ingest + query CPU and retained state memory.
Report model B (peak-provisioned, staggered) in the appendix. This keeps the
data-plane and planner sections in the same units.

**Fairness**
- Same node type: `c6320` (2× E5-2683 v3, 56 threads, 251 GB).
- Both systems are pinned to the same 2 cores and 4 GiB, matching the
  backend's matched-budget runners.
- ClickHouse image pinned by digest. Query cache off, `max_threads=2`.
- ClickHouse restarted per trial.
- 3 trials per point, alternating route order. Report the median, with
  min/max error bars.

## 7. Open decisions

- **D0: which data plane is measured.** The ASAPQuery-backend (recommended: its
  README describes it as the summary storage, precompute runtime and query
  adapters, and its ClickHouse harness already exists), or the legacy `query_engine_rust` in this repo.
- **D1: the source of the plan.** `rqe-optimizer` (recommended, and the paper
  calls the plan optimal) or the `asap-planner` SQL-mode generator, which works
  with today's runner but is not cost-optimal.
- **D2: the ASAP query interface.** PromQL now (recommended: quantile and top-k
  are warm there), or wait until the SQL adapter can bind quantile and top-k.
- **D3:** keep `ch_mv_exact` as an extra bar, or show only `ch_mv_sketch`.
- **D4:** the ClickHouse quantile state: `quantilesState`, `quantilesTDigest`
  or `quantilesDD`, per workload. Proposal: report the one closest to ASAP in
  accuracy and list the others in the appendix.
- **D5:** where this harness lives. The backend already has the matched runners.
  This repo has the Hydra/CloudLab orchestration.

## 8. Work items (each a small PR)

1. Data generator: seeded Zipf (θ) weights and Pareto (a) values over
   `(C, s, r)`, written as JSONL for ClickHouse plus Remote Write or the
   OpenMetrics replay for ASAP from the same seed. Include a test on the
   empirical rank-frequency slope.
2. Template set: the 10 synthetic templates as PromQL + ClickHouse SQL pairs,
   with a differential test against `ch_raw` on a small `C, s`.
3. Plan → ClickHouse translator. It reads an `rqe-optimizer` plan and emits
   `init.sql` (raw table + MVs per §4) and the MV-targeted query SQL. It logs
   every unsupported mapping.
4. Plan → backend publication (or confirm the existing path), then the install
   and backfill driver.
5. Runner. Either extend `experiment_run_clickhouse.py` with `ch_mv_exact`,
   `ch_mv_sketch` and an `asap_backend` mode, or adapt the backend's
   `run_matched.sh` (D5). In both cases, **one ClickHouse load per arm**, each
   with its own `init.sql`.
6. Post-processing: ClickHouse and backend paths in the cost script, the model A
   and model B pricing, and the figure script.

## 9. Cluster and run procedure

CloudLab experiment `scratch2.cloudmigration-pg0.clemson.cloudlab.us`.
`node1`–`node3` run 3 configs in parallel (`node_offset` 1/2/3). `node1` is
also the orchestrator.

Setup:
1. `/mydata` (845 GB ext4) is already mounted, world-writable, on node1–3. The
   setup scripts expect `/scratch`, so symlink it or make the root volume
   configurable.
2. Docker, the Rust toolchain, release builds of the backend (and of
   `asap-query-engine` / `asap-planner-rs` if D0 picks the legacy path), and
   `rqe-optimizer`.
3. Generate datasets once into `/mydata/datasets/`, and rsync them to the nodes.

Order:
1. **Pilot**: the default workload with `C=1e3`, `s=10`, all 4 arms, 1 trial.
   Check:
   - 0 ASAP fallbacks;
   - MV aggregates equal `ch_raw` exactly for exact states;
   - accuracy within target;
   - corpus q05/q06/q23 reproduce the backend report's speedups (sanity).
2. **Sweep**: the §3.1 dimensions one at a time
   (≈ 3+3+2+5+2+2 = 17 non-default points + default = 18 workloads),
   × 4 arms × 3 trials = **216 runs**. Allowing about 15 min per run, that is
   about 18 h on 3 nodes.
3. **Figure**: grouped bars of model-A $/h and p95 latency per workload, with
   ASAP ÷ `ch_mv_sketch` annotated (the "up to X×" numbers). Each bar carries
   its accuracy, and the "not supported in ClickHouse" plans are listed under
   the figure.

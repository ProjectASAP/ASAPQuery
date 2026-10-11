# End-to-end planning flow: from traces to an rqe-optimizer plan

How to plan sketch deployments for a query workload over a real trace:

1. describe the workload as RAQEs;
2. fit the trace's parameters;
3. measure the sketches;
4. run the optimizer;
5. read the plan.

This page follows what exists on `main` (through #183). It names the
steps that are still manual. The model itself (candidates, cost model,
eligibility) is in [`rqe_sketch_deployment_v1.md`](rqe_sketch_deployment_v1.md),
and the saturation study is in [`saturation_study.md`](saturation_study.md).

```text
query workload ──► RAQEs (capability, S, T, G, accuracy SLA, latency SLA)
                          │
trace ── fit_skew.py ─► skew_summary.csv ── (by hand) ─► WorkloadFacts
        (ASAPQuery)       θ, K, N, a per query             per metric: labels, scrape,
                                                           card(X), value range,
                                                           data_shape per grouping
                                                                  │
study_saturation.py ─► SATURATION_DIR/                            │
  accuracy grid (+ merge curves)   out_grid_1e7_cost/             │
  1e9 subset                       out_1e9/                       │
  --phase optimizer-cost           optimizer_cost/rqe_atomic_costs.json
                                          │                       │
                                          ▼                       ▼
                        rqe-optimizer: SaturationCurves::load + cost table
                        candidates ─► eligibility (accuracy, latency) ─► MILP ─► plan
```

## 1. The workload as RAQEs

The optimizer plans repeating atomic query expressions (`rqe_optimizer::Raqe`).
Each one is a single statistic that is evaluated every `T` over the last `S`:

| Field | Meaning |
|---|---|
| `id` | Name used in the plan output |
| `capability` | `Sum`, `Count`, `Min`, `Max`, `RateOrIncrease`, `Quantile`, `Cardinality`, `TopKByValue`, `TopKByCount` |
| `lookback_ms` | `S`, the range of the query (`[5m]` is 300,000) |
| `interval_ms` | `T`, how often it is evaluated |
| `metric` | The metric it reads; must have an entry in `WorkloadFacts` |
| `spatial_filter` | Must be empty for now. `validate_facts` rejects filters until facts carry per-filter cardinality |
| `grouping_labels` | `G`, the `by (...)` labels; empty for a whole-metric aggregate |
| `accuracy_sla` | Bound on the family's own accuracy metric. It is a ceiling for errors and a floor for top-k precision (#172) |
| `latency_sla_ms` | Optional ceiling on the modeled query latency. Only the MILP enforces it |

A PromQL query expands into one RAQE per statistic. For example, a query
asking for 3 quantiles is 3 RAQEs, and `a / b` is the RAQEs of `a` plus those of `b`.
This expansion is done by hand today.

Where workloads are written down:

- **sketch-bench:** `raqes()` in
  [`rqe-optimizer/examples/small_problem.rs`](../rqe-optimizer/examples/small_problem.rs)
  is Rust code; edit it and rebuild.
- **ASAPQuery:** `asap-optimizer-cli --milp` reads the planner's workload YAML
  (`--input_config`, the same format as `asap-planner --input_config`). Its
  `metrics:` hints supply each metric's labels. ASAPQuery#804 (open) adapts
  this path to the curve-based accuracy below.

## 2. Parameters from the trace

The optimizer needs, per metric, a `rqe_optimizer::MetricFacts`:

| Field | What it is | Where it comes from |
|---|---|---|
| `labels` | Every label the metric's series carry | The trace's schema |
| `scrape_interval_ms` | One sample per series per scrape | The trace's sampling period (`step_s` in `skew_summary.csv`) |
| `cardinality` | `card(X)` for each label set `X` in use, including all labels (the series count) | Counted on the trace |
| `value_range` | `(lo, hi)` of positive values; sizes DDSketch | The trace's value column |
| `data_shape[G]` | Per grouping `G`: Zipf skew `zipf_s`, distinct keys per group per window `distinct_keys`, Pareto tail index `tail_index` | `skew_summary.csv` (below) |

`data_shape` is what keys the saturation curves. A grouping without one gets
no sketch accuracy, so only exact accumulators can serve it.

### Fitting the shape: ASAPQuery `asap-tools/dataset-analysis`

`fit_skew.py` evaluates each query of `queries/<dataset>.yaml` at every step
over the whole trace, as Prometheus would. It fits the key skew θ and the value
tail α per evaluation (see that directory's README):

```bash
cd ASAPQuery/asap-tools/dataset-analysis
pip install -r requirements.txt
./fetch_data.sh /path/to/trace-data
python fit_skew.py --data-root /path/to/trace-data   # writes results/skew_summary.csv
```

`results/skew_summary.csv` has one row per (dataset, query_id, kind, weight,
range). Those three key columns are:
- **kind**: `keys` or `values`. A key query (group-by keys: sums, top-k,
  distinct counts) gets the Zipf fit, which gives `zipf_s` (θ) and
  `distinct_keys` (K). A value query (quantiles) gets the power-law tail fit,
  which gives `tail_index` (a).
- **weight** (key queries only): what each key's rank-frequency counts. With
  `count` it is the row count per key; with `value`, the value sum per key,
  negatives clipped. Use the one that matches how the sketch is fed: once per
  sample (`count`, e.g. `topk` over `count_over_time`) or weighted by the
  sample value (`value`, e.g. `topk` over `sum` or `rate`).
- **range**: the query's lookback (`instant`, `5m`, `1h`, ...), i.e. the
  RAQE's `S`. Each row is that query evaluated at every step with that window.

`data_shape` is keyed by (metric, grouping), not by range. When RAQEs on one
grouping have different ranges, take the worst case over them: the smallest θ,
the largest K and the smallest a. Use the worst case over the whole trace too,
not an average, since longer samples expose worse cases
(`saturation_conclusions.md`). The columns map onto `data_shape` as follows:

| `skew_summary.csv` | `DataShape` / facts | Note |
|---|---|---|
| `worst_theta_cms` | `zipf_s` | Key queries. Flatter (smaller) is harder |
| `worst_K` | `distinct_keys` | Distinct keys per evaluation window |
| `worst_alpha_rank` | `tail_index` | Value queries. Steepest tail, hardest for rank error |
| `worst_alpha_memory` | — | Heaviest tail; used for DDSketch cost by `recommend_config.py`, not by the optimizer |
| `min_N`, `max_N` | — | Items per evaluation; the optimizer computes its own from `cardinality`, `scrape_interval_ms` and `S` |
| `range_s`, `step_s` | `lookback_ms`, `interval_ms` | `range_s` is empty for instant queries |
| `target_*` | `accuracy_sla` | Per-family targets from the analysis |

Copying these numbers into `MetricFacts` is **manual**. sketch-bench's
[`scripts/recommend_config.py`](../scripts/recommend_config.py) reads the same
file, but it recommends one config per query from the curves; it doesn't write
facts for the optimizer.

## 3. Measuring the sketches: `scripts/study_saturation.py`

The optimizer reads two things from one directory, `SATURATION_DIR`: the
saturation curves, for accuracy, and the cost table, for CPU and memory. Both
come from `scripts/study_saturation.py`, and its configs are one grid (#174).
Run it on a quiet machine. We use CloudLab `clnode109`. Cost and timing runs
must be the only job on the machine.

```bash
cargo build -p aqpbm-cli --release
D=SATURATION_DIR

# Accuracy grid to N = 1e7, with merge curves (KLL needs them, #158; top-k
# merges with a heap of m·k and reads its plain curve, #182).
python3 scripts/study_saturation.py --phase accuracy --n-max 1e7 --seeds 3 --jobs 48 \
    --merge-shards-list 1,4,16,64 --out $D/out_grid_1e7_cost

# Larger N for the points that need it: list them in a CSV with
# sketch,config,dist,param,cardinality columns.
python3 scripts/study_saturation.py --phase accuracy --n-max 1e9 --seeds 3 --jobs 16 \
    --points-from points.csv --out $D/out_1e9

# The cost table (serial).
python3 scripts/study_saturation.py --phase optimizer-cost --out $D/optimizer_cost
```

- **Grid:** `--thetas`, `--cardinalities` and `--alphas` set the data shapes,
  and `SKETCHES` sets the configs. `--cost-shape` (on by default) adds the cost
  table's shape (`COST_THETA` = 1.1, `COST_KEYS` = 1e4, `COST_PARETO_ALPHA` = 2)
  to every config, so each cost-table row is a point on a curve.
- **Merge curves:** `--merge-shards-list` also scores the sketch merged from
  `m` shards, into `saturation_merge_curve.csv`. List every `m` the workload's
  merges need (`m = S / x` for the windows `x` the optimizer may pick).
  Past the largest measured `m`, a merged KLL answer falls back to its
  guarantee (#180). Restrict `--merge-shards-list` to the quantile family with
  `--families quantile` and `--resume`.
- **Cost table:** `--phase optimizer-cost` measures each config of the families
  the optimizer plans (`OPTIMIZER_FAMILIES`: top-k, cardinality, quantile) and
  the exact accumulators, once each at the `COST_*` shape with `COST_N` = 1e6
  items, 5 runs after 3 warm-ups. `atomic-costs` reduces them to
  `rqe_atomic_costs.json`, and every row names its `accuracy_metric`. The
  phase fails if any row is skipped.
- **Resuming:** `--resume` keeps complete curves after an interruption, and
  with a narrower grid it keeps the other points' rows (#182).

### Fanning out over machines

Accuracy runs split cleanly across machines; only the cost table needs a
machine to itself.
1. Build `approxbench` once and copy it, with `scripts/` and `configs/`, to
   each machine. The machines must share the OS and libc.
2. Give each machine a disjoint set of points: whole families with
   `--families`, or a CSV of points with `--points-from`. Each machine writes
   its own run directory.
3. Concatenate the parts into one run directory:

   ```bash
   python3 scripts/merge_study_parts.py $D/out_grid_1e7_cost part_a/grid part_b/grid
   ```

   It refuses a point that appears in two parts and mismatched headers.
4. Run `--phase optimizer-cost` alone on one machine.

On asap_sketchlib 0.3.0, five 56-core nodes run the default grid and the
targeted 1e9 points in about 10 minutes.

`SaturationCurves::load` expects exactly this layout:

```text
SATURATION_DIR/
  out_grid_1e7_cost/  saturation.csv  saturation_curve.csv  saturation_merge_curve.csv
  out_1e9/            saturation.csv  saturation_curve.csv
  optimizer_cost/     rqe_atomic_costs.json
```

A point present in both runs is taken from `out_1e9` and keeps the merge
curves of `out_grid_1e7_cost`.

## 4. Running the optimizer

### sketch-bench: `small_problem`

```bash
cargo run --release -p rqe-optimizer --example small_problem -- \
    --saturation-dir $D --milp [--w-cpu 1 --w-mem 0] [--latency-sla RAQE_ID=MS ...]
```

Other modes:
- `--candidates-only` prints candidate counts before and after pruning;
- `--sample-mappings N` prints N feasible mappings;
- `--streaming` and `--progress-every`/`--print-first` enumerate mappings;
- `--allow-undeployable-families` also plans with families ASAPQuery can't
  deploy.

Do not run it with no mode flag on a large workload: eager enumeration keeps
every mapping in memory.

What is checked before planning:
- `SaturationCurves::load` refuses a candidate family's curve whose
  `error_metric` isn't the family's accuracy metric. That is a stale study, so
  rerun it.
- It also refuses a study with no merge curves for a candidate KLL sketch.
- `check_cost_table` refuses to plan when a cost row's `accuracy_metric` isn't
  its family's or its curve's, or when the row disagrees with the curve at its
  `measured_at`. Rows with no grid point at their shape are printed as
  unchecked.

### ASAPQuery: `asap-optimizer-cli --milp`

With ASAPQuery#804 (open, pinned to sketch-bench main once it merges):

```bash
asap-optimizer-cli --milp --input_config workload.yaml \
    --data-ingestion-interval-ms 15000 \
    --workload-facts facts.yaml \
    --atomic-costs $D/optimizer_cost/rqe_atomic_costs.json \
    --saturation-dir $D --output-dir out/
```

`facts.yaml` gives, per metric, the cardinality of each label set and the
`shape` of each grouping sketches may serve:

```yaml
metrics:
  - metric: http_request_duration_seconds
    groups:
      - labels: [service, endpoint, pod]   # all labels: the series count
        cardinality: 30000
      - labels: [service]
        cardinality: 5
        shape: {zipf_s: 1.1, distinct_keys: 10000, tail_index: 1.5}
```

This path doesn't run `check_cost_table`.

### How accuracy is read

For each candidate deployment, at `n` = items one group receives over the
lookback (series per group × scrapes per lookback):

- **Exact accumulators:** the cost table's value (`relative_error` 0).
- **Sketches:** the curve of the config at the grouping's `data_shape`, the
  worse of the bracketing grid points. Between checkpoints it takes the worse
  neighbour. Below the first checkpoint there is no accuracy. Past the last,
  it uses the plateau if the point saturated by then, else there is no accuracy.
- **Merging `m = S / x` windows:** CMS, CountSketch, HLL and DDSketch merge
  exactly, so they read the plain curve. KLL reads the merge curve at `m`,
  taking the worse of the measured counts either side. Top-k deployments keep
  a heap of `m·k` (#182), so a merged top-k answer reads the plain curve.
- **Unmeasured:** past an unsaturated curve, past the measured `m` or N, or
  with no merge curve, the accuracy falls back to the sketch's published
  guarantee at 95% (#180), never better than the last measurement it extends.
- **No accuracy** (below the first checkpoint, or a shape outside the grid)
  means the candidate is not eligible.

## 5. Reading the plan

`small_problem --milp` prints:
- the objective and its totals: CPU in CPU-seconds per second, memory in MB;
- CPU and memory per phase (ingest, merge, query, storage);
- each active deployment: family, config, metric, grouping, window `x` and
  slide `y`, and retained instances;
- each RAQE's deployment, merged instances and modeled query latency.

`asap-optimizer-cli --milp --output-dir` also writes `streaming_config.yaml`
and `inference_config.yaml` for the engine (ASAPQuery#803).

## What isn't automated yet

- **PromQL → RAQEs:** expanding queries into RAQEs is manual (Rust in
  `small_problem`, YAML in ASAPQuery).
- **`skew_summary.csv` → `WorkloadFacts`/`facts.yaml`:** copying θ, K and α into
  `data_shape`, and counting `cardinality` and `value_range`, is manual. No
  script reads `skew_summary.csv` into facts.
- **Choosing the study's points:** the 1e9 subset's `points.csv` and the
  `--merge-shards-list` values are picked by hand from the workload's N and
  `m = S / x`.
- **Spatial filters:** rejected until facts carry per-filter cardinality.
- **The ASAPQuery path:** depends on ASAPQuery#804, which is not merged yet.

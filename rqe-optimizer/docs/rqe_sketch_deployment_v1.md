# RAQE-to-sketch deployment mapping: v1

## Purpose

Given a workload of repeating atomic query expressions (RAQEs), find sketch deployments
that can serve it and report the trade-offs among query working memory, CPU,
and per-RAQE latency.

## Glossary

- **RAQE**: repeating atomic query expression, evaluated every `T` over the last `S`.
- **Capability**: the statistic an RAQE needs: sum/count, min, max,
  rate/increase, quantile, cardinality or top-k.
- **Metric**: a named stream of samples whose series are told apart by labels.
- **Group**: one combination of values of an RAQE's group-by labels `G`. There
  are `card(G)` groups.
- **Family**: one sketch or accumulator implementation, such as `kll-percall`,
  `cms-heap-topk-fastpath-vector2d` or `exact-sum`. Each capability lists the
  families that serve it.
- **Configuration**: a family plus its parameters, such as KLL with `k = 200`.
  One row of the cost table.
- **Cost table**: per configuration, measured memory per instance, CPU per
  insert, merge and query, and accuracy in its named `accuracy_metric`.
  Measured by
  [`scripts/study_saturation.py --phase optimizer-cost`](../scripts/study_saturation.py).
- **Window**: the interval `[k × y, k × y + x)` an instance covers; `x` is the
  window size and `y` the slide.
- **Instance**: one sketch or accumulator object holding one group's samples
  (or all groups', for a shared family) over one window. Open or closed; see
  [Window model](#window-model).
- **Deployment**: a configuration run over one metric, grouped by `G`, with
  window `x` and slide `y`. A **candidate** is a deployment the optimizer may
  choose.
- **Phase**: one of the kinds of work a deployment does, each costed in CPU
  and memory: **ingest** (inserting samples into open instances),
  **compaction** (merging the ingest workers' copies of a closed window),
  the **query job** (merging a query's windows, and on a roll-up its fine
  groups, then reading the result) and **storage** (keeping closed
  instances, no CPU). The snapshot objective and `PlanCost` report the query
  job as two phases, **merge** and **query**, and have no compaction. See
  [Cost models](rqe_optimizer_cost_model.md).
- **Roll-up**: a deployment grouped by `G_d` serving a RAQE grouped by a
  subset `G_r`, by merging each coarse group's fine instances at query time.

## Inputs

Let `R = {r_1, ..., r_n}` be the RAQE workload. Each RAQE `r_i` provides:

| Field | Meaning |
|---|---|
| `cap_i` | Required capability. |
| `S_i` | Query-window size. This is the current `lookback` field. |
| `T_i` | Query slide: how often the RAQE runs. This is the current `interval` field. |
| `metric_i` | Metric the RAQE reads. |
| `G_i` | Group-by label set (`grouping_labels`). |
| `tol_i` | Accuracy threshold, checked against each candidate family's own metric. |

For each metric, the caller provides `MetricFacts`:

- `labels`: every label the metric's series carry.
- `scrape_interval`: every series yields one sample per scrape.
- `card(X)`: distinct value combinations of each label set `X` in use,
  including `labels` itself (the raw series count).
- `value_range` (optional): `(lo, hi)`, the smallest and largest positive
  sample value, with `0 < lo <= hi`. It sizes DDSketch (see below).

The arrival rate is derived as follows (not provided by the user):

```text
lambda(metric) = card(metric.labels) / scrape_interval      samples/second
```

`validate_facts` reports every missing or inconsistent fact up front.

The cost table, measured by
[`scripts/study_saturation.py --phase optimizer-cost`](../scripts/study_saturation.py)
(#174), gives for each configuration:

- memory per instance;
- CPU per insert;
- CPU per query of one instance;
- CPU per pairwise merge;
- the comparator's accuracy scores, and `accuracy_metric`, the one that is
  the row's accuracy (only exact accumulators read it; see
  [eligibility](rqe_optimizer_candidates.md#candidate-deployments)); and
- `measured_at`, the data it was measured on.

The study measures every configuration once, serially, at one data shape:
Zipf θ = 1.1 over 1e4 keys (Pareto `a` = 2 for quantiles), 1e6 items, 5 runs
after 3 warm-ups. That is the dataset of the AutoSketch vs. ASAP evaluation
(ASAPQuery#777), so the evaluation's costs and the cost table are one
measurement. Cost is taken as independent of the data shape. The same study
measures the saturation curves, so the configurations in both are one grid.

Each metric's facts also carry a fitted `data_shape` per grouping, which keys
the saturation curves.

Families are named by sketch-bench variant, not algorithm, because variants
of the same algorithm can have different costs.

## Window model

A deployment materializes sliding sketch instances with:

- `x`: sketch-window size;
- `y`: sketch slide.

For every label-value group, it creates an instance for every interval:

```text
[k × y, k × y + x)
```

At time `t`, an instance is **open** while its interval has not ended
(`t < k × y + x`): it still receives samples. After that it is **closed**: it
never changes again and is kept only while some RAQE's lookback still needs it.
A deployment has `x / y` open instances per group at any time.

Instances overlap when `x > y`. There is no subtract operation. To answer an
RAQE, the system selects and merges only non-overlapping instances, so items
are never counted twice.

An RAQE with query window `S` and query slide `T` can use a deployment when:

```text
x % y == 0
S % x == 0
T % y == 0
```

For a query ending at time `t`, use the instances:

```text
[t-S, t-S+x), [t-S+x, t-S+2x), ..., [t-x, t)
```

They exactly cover the query window and do not overlap. The deployment may
have produced other, overlapping instances between these starts. That is okay. Those overlapping instances are not used for this query.

`S % T == 0` is not required. For example, a 10-minute window running every
3 minutes can use `x = 2 minutes` and `y = 1 minute`; each query merges five
non-overlapping two-minute instances.

We assume all RAQEs share a common time origin. Query phase offsets are not
modeled.

## Planning design

The optimizer has two companion design documents:

- [Candidate generation and eligibility](rqe_optimizer_candidates.md):
  candidate construction, eligibility (accuracy included), finer-to-coarser
  roll-ups and dominance pruning.
- [Cost models](rqe_optimizer_cost_model.md): the one cost table, the
  canonical cost-by-use and batch-latency model with its MILP, and the
  supported snapshot-AUC objective.


## Procedure

1. Generate candidate deployments for each input RAQE, then deduplicate them.
2. Build the eligible candidate–RAQE pairs. Reject pairs that fail capability,
   metric, grouping (equal, or a subset for a roll-up), window alignment, or
   accuracy.
3. Enumerate feasible workload mappings. Every RAQE must select one eligible
   candidate; a single selected candidate may serve multiple RAQEs. Small
   instances may retain every mapping eagerly; larger ones stream mappings
   through incremental Pareto filtering, retaining only the current frontier.
4. Score every complete mapping with the analytical cost model.
5. Compute and report the Pareto frontier.

The Pareto vector is:

```text
(CPU, memory, {latency_i})
```

CPU and memory are the sums over the snapshot objective's phases (ingest, merge, query, storage). The report also includes
each phase's CPU and memory, together with the selected deployment mapping.

## v1 scope and TODOs

- **Accuracy after merging:** KLL and univmon-cardinality read their merge
  curves up to the largest shard count and N the study measured; past them,
  KLL's guarantee, no better than the last measurement (univmon-cardinality
  has none, so no accuracy). Roll-ups multiply the merge count by the
  fan-out, so they reach the guarantee sooner. Heap top-k reads its plain
  curve (#158).
- **Query-result sharing:** v1 charges every RAQE its own query and merge CPU.
  Revisit when RAQE semantics and execution timing identify safe reuse cases.
- **Latency SLAs:** `minimize` takes optional per-RAQE latency bounds;
  `minimize_usage_cost` takes an optional batch-latency bound and reports the
  batch latency. The enumerator reports latency but does not reject a
  mapping for it.
- **Memory model:** cost by use holds query memory only while a query runs;
  the snapshot objective sums every RAQE's merge and output memory as if all
  queries ran at once. Query output size is an estimate.
- **Bursts:** CPU is a mean over time, so plans are sized for average load.
- **Static planning:** no RAQE churn, replanning, or migration cost.
- **Temporal pre-merging:** v1 does not precompute merged windows. Queries
  merge their selected base instances when they run.
- **Selection policy:** the enumerator reports the Pareto frontier; the MILP
  selects one point by the `w_cpu`/`w_mem` weights.

## Implementation status

`rqe-optimizer/` implements the `(x, y)` sliding-instance model, candidate
generation and pruning, eligibility checks, analytical objectives, and
streaming Pareto filtering. The separation between candidate generation,
enumeration, objectives, and Pareto filtering remains the intended boundary
for future solver work. Hydra, a shared-subpopulation deployment rather than
a fine-state roll-up, is future work.

Exporter bugs, benchmark-dataset choices, and historic run counts belong in
`rqe_optimizer_TODO.md` or issue tracking, not in this problem statement.

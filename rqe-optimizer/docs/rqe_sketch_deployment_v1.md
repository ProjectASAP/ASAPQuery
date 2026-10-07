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
- **Phase**: one of the four kinds of work a deployment does, each costed in
  CPU and memory: **ingest** (inserting samples into open instances),
  **merge** (folding a query's windows into an accumulator), **query**
  (reading the merged result) and **storage** (keeping closed instances, no
  CPU). See [Analytical cost model](#analytical-cost-model).

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
  [eligibility](#candidate-deployments)); and
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

## Candidate deployments

A candidate deployment is:

```text
D = (capability, configuration, metric, G, x, y)
```

Candidates may be generated from individual RAQEs, then deduplicated. This is
only a way to construct the candidate set: when solving a workload, one
selected deployment may serve multiple compatible RAQEs.

Intuition: candidates are built in two steps, and each drops only candidates
that another candidate beats, so the best plan is never lost. Generation uses
no measurements. It keeps every legal window and configuration, and drops only
slides that a coarser slide beats for the same RAQEs. Pruning then attaches the
measured costs and drops a candidate when another serves all of its RAQEs and
is no worse on every cost.

For each (capability, metric, G) group, generate candidates as follows:

```text
for each window x that divides some S_i and is a multiple of the scrape interval:
    g = { g_i = gcd(x, T_i) : RAQE i with S_i divisible by x }
    slides = { gcd(A) : A is a non-empty subset of g }
    for each y in slides that is a multiple of the scrape interval:
        for each configuration that serves the capability:
            add (capability, configuration, metric, G, x, y)
```

`g_i = gcd(x, T_i)` is the largest slide that serves RAQE `i`.

**Windows.** An RAQE merges `S_i / x` whole windows, so `x` must divide `S_i`.
Windows and slides finer than the scrape interval would only split one
scrape's samples.

**Slides.** A slide `y` serves RAQE `i` when it divides both `x` and `T_i`,
that is, when it divides `g_i`. A finer slide serves no more RAQEs but holds
more open windows (`x / y`) and more closed ones, so it costs more ingest and
storage at the same latency. Hence:

- for one RAQE, `y = g_i` is strictly better than every finer slide that serves
  it;
- for a set `A` of RAQEs sharing a deployment, `y = gcd(A)`, the largest slide
  dividing every `g_i` in `A`, is strictly better than every finer slide that
  serves them all.

Which RAQEs share is the solver's choice, so every subset's gcd is a candidate.
Every other divisor of `x` is strictly worse than one of these.

Example: `x = 60`, RAQE `a` every 20 s, RAQE `b` every 30 s.

| Served RAQEs | `y` | Open windows `x / y` |
|---|---|---|
| `a` | gcd(60, 20) = 20 | 3 |
| `b` | gcd(60, 30) = 30 | 2 |
| `a` and `b` | gcd(20, 30) = 10 | 6 |

The slides are `{10, 20, 30}`. `y = 5` would also serve both, but holds 12 open
windows instead of 6. With a 15 s scrape interval only `y = 30` remains: no
multiple of 15 divides both 20 and 30, so `a` and `b` cannot share at
`x = 60`.

The subset gcds take one pass, without listing subsets: keep the gcds found so
far, and for each new `g` add `g` and `gcd(found, g)` for every `found`. For
`{20, 30}`: `{20}`, then `{20, 30, 10}`. One pass suffices because
`gcd(A ∪ {g}) = gcd(gcd(A), g)`.

An RAQE `r_i` is eligible for a candidate `D` when:

1. `cap_i = D.capability`, `metric_i = D.metric` and `G_i = D.G`.
2. `D.x % D.y = 0`, `S_i % D.x = 0`, and `T_i % D.y = 0`.
3. The accuracy of `D` for `r_i`, in the accuracy metric of `D`'s family
   (`family_properties`, which also gives the direction), clears `tol_i`.

Exact accumulators take that accuracy from the cost table; they merge without
loss, so the window size doesn't matter. Sketches take it
from the saturation study's error-vs-N curves (`SaturationCurves`, #156),
read at the number of items one group receives over the RAQE's whole
lookback: series per group times scrapes per lookback.

- The curve is the configuration's at the metric's fitted `data_shape` for
  `G` (zipf θ and keys `K`, or tail index `a` for quantiles). Between grid
  points, the worst bracketing point; outside the grid, or with no fit, no
  accuracy.
- Between checkpoints, the worse neighbour. Below the first checkpoint, no
  accuracy. Past the last, the plateau if the point saturated, else none.
- The curve's `error_metric` must be the family's metric; `SaturationCurves::load`
  refuses a curve for another metric as a stale study.
- For sketches that merge exactly, the window size doesn't matter: a merged
  answer reads the curve at the lookback's item count, like a single sketch.
  KLL and top-k merge lossily (#131): an answer merged from `m = S_i / D.x`
  windows reads the study's merge curve (`saturation_merge_curve.csv`, the
  sketch merged from `m` shards of the same items) at the lookback's item
  count. Between measured shard counts, the worse. Past the largest
  measured count, or without a merge curve, a merged KLL or top-k answer has
  no accuracy: the study must measure the merge counts a workload needs
  (`--merge-shards-list`) up to the N it reads at: past the merge curve's
  last N, the merged answer has no accuracy either (#158).
  `SaturationCurves::load` refuses a study with no merge curves for a
  candidate KLL sketch.
- A top-k RAQE asks for its own `k` (`Raqe::topk_k`, default 32). Heap
  top-k (CMS-heap, CountSketch-heap) keeps a heap of `m · k` in a deployment
  answering from `m = S_i / D.x` merged windows (`m = 1` gives `k`).
  Candidates carry one heap per distinct `m · k` among the RAQEs on a window,
  and a RAQE is eligible only where the heap holds its `m · k`. Such a merged answer reads as one sketch: the
  plain curve, then the guarantee fallback. It assumes the merged heaps still
  hold the true top `k`; it is not a guarantee. So heap top-k needs no merge
  curves. Costs come from the cost table's rows at heaps 32, 128, 512 and
  2048 (`heap=`), linear in the heap between them and past them. Curves are
  measured per `k` (`topk_k=` in the config; 10, 32 and 100 by default) at a
  heap of `k`, and larger heaps read the same curve. A RAQE reads the curve
  at its `k`, else at the next larger measured `k` (harder); past every
  measured `k`, only the guarantee at its `k`. Its query outputs `k`
  entries per group.
- Where the study measured nothing for a bracketing point at or above its
  first N (past an unsaturated curve, past the measured merge counts or N,
  or with no merge curve), the accuracy falls back to the algorithm's
  guarantee at 95% confidence (`rqe_optimizer::theory`; two-sided for
  rank and relative errors, one-sided for CMS overestimates). It is never
  better than the worst last measurement of the curves it extends. The fallbacks are:
  - KLL: rank error `2.296 / k^0.9723` (DataSketches' 99% value), scaled
    to 95% two-sided;
  - DDSketch: relative value error `α` (unbounded store);
  - HLL: `1.96 · 1.04 / sqrt(2^lg_k)`;
  - CMS-heap top-k, unmerged: the share of the top `k` Zipf(θ, K) keys kept
    ranked for every key at once. With Markov per row, independent rows,
    and a union bound over the `K − k` other keys, key `i` fails with
    probability at most `(cols · (p_i − p_{k+1}))^−rows`. Keys are counted
    from the most frequent while the summed failure stays within 5%. It
    assumes the heap ranks keys by their final CMS estimates.

  A sketch with no published bound, a merged top-k answer, a point below
  the first N, or a shape outside the grid stays unknown.
  `accuracy_with_source` reports which points used a bound.

The cost table's sketch accuracies are not read, but they are one point on
each curve: a row's `measured_at` (items, Zipf θ and population, or Pareto
`a`) is a grid point's key. `SaturationCurves::check_cost_table` reports every
row whose `accuracy_metric` is not its family's or not its curve's
`error_metric`, whose configuration is not in the grid, or whose accuracy
disagrees with the curve at `measured_at` (beyond 3 seed standard errors plus
5%). The study adds the cost table's shape to the grid as a full row and
column (θ = 1.1 at every K, K = 1e4 at every θ), so every sketch row has a
grid point and the grid stays a full cross. `SaturationCurves::load`
refuses curves whose Zipf grid isn't a full cross of its θ and K values and
names the missing points: a hole would leave every shape around it without
accuracy (for example, curves from `--points-from` or a narrowed `--resume`). A row without a grid point (a study run
with `--no-cost-shape`) is reported as unchecked. `small_problem` refuses to plan
when any row disagrees.

For lower-is-better metrics, passing means `measured <= tol_i`. For
higher-is-better metrics, passing means `measured >= tol_i`. Direction is
explicit on the RAQE; it is never inferred from a metric name. A missing metric
does not pass.

Before mapping, prune a candidate only if another candidate can serve every
RAQE it can and is no worse in ingest CPU and memory, and in latency, merge
memory and stored memory for each such RAQE. This is safe because any mapping using
the removed candidate can substitute the remaining one without weakening a
modeled objective.

Closed instances must be stored long enough to answer the RAQEs assigned to a
deployment. Retention is not a candidate parameter: a deployment stores the
history needed by the largest assigned query window, costed as the storage
phase (below).

## Workload mapping

Let `z_{i,D}` be 1 when RAQE `i` uses candidate deployment `D`; let `u_D` be 1
when deployment `D` is active.

```text
Σ_D z_{i,D} = 1       for every RAQE i
z_{i,D} <= u_D        for every eligible pair (i, D)
```

Every RAQE selects exactly one eligible deployment. Multiple RAQEs may select
the same deployment and therefore share its ingest work. A query may merge
multiple instances from its selected deployment, but v1 does not combine
results from several deployments to satisfy one RAQE.

### MILP formulation

Let `E` be the eligible `(i, D)` pairs. The solver has one binary `z_{i,D}`
for each pair in `E`, one binary `u_D` for each candidate, and a continuous
`stored_D` for each candidate's closed-window storage.

```text
z_{i,D}  ∈ {0, 1}    for (i, D) ∈ E
u_D      ∈ {0, 1}    for D ∈ candidates
stored_D ≥ 0

Σ_{D: (i,D) ∈ E} z_{i,D} = 1                    for every RAQE i
z_{i,D} ≤ u_D                                    for (i, D) ∈ E
u_D ≤ Σ_{i: (i,D) ∈ E} z_{i,D}                   for every candidate D
stored_D ≥ w(storage_{i,D}) × z_{i,D}            for (i, D) ∈ E
```

The `u_D ≤ Σ z` constraint prevents a deployment from becoming active when no
RAQE selected it. It is not required for feasibility, but makes ingest cost
unambiguous. `stored_D` is a linearized max: a deployment stores enough for
the longest lookback it serves.

The only objective, `Objective::AUCCost { w_cpu, w_mem }`, weighs each phase
cost (below) as `w(c) = w_cpu × c.cpu + w_mem × c.memory_GiB`:

```text
minimize  Σ_D w(ingest_D) × u_D
        + Σ_(i,D)∈E (w(merge_{i,D}) + w(query_{i,D})) × z_{i,D}
        + Σ_D stored_D
```

CPU is the area under the CPU curve (mean CPU-sec/sec), so plans are sized
for the mean load, not for bursts. Memory sums every phase, as if every query
evaluates at once. The default weights are `(1, 0)`: CPU only.

Per-RAQE latency bounds forbid the pairs over them (`z_{i,D} = 0`). The solver
never enumerates full mappings.

## Analytical cost model

The analytical model combines the empirical per-operation Sketch Bench
measurements with workload properties such as group cardinality, arrival rate,
window size, and query frequency.

For deployment `D` serving RAQE `i`:

- `lambda`: samples/sec arriving for `D`'s metric, `card(metric.labels) / scrape_interval`.
- `card(G)`: cardinality of `G`, so the number of parallel accumulator instances per window (one for a shared fixed-size sketch; see below).
- `x`, `y`: `D`'s window and slide.
- `a_D = x / y`: open windows; each sample is inserted into `a_D` instances of its group.
- `S_i`, `T_i`: RAQE `i`'s lookback and interval.
- `n_i = S_i / x`: windows merged per query.
- `m`: memory per instance, measured. For DDSketch on a metric with a
  `value_range`, `m = (floor(ln(hi / lo) / ln((1 + alpha) / (1 - alpha))) + 1) × 8`
  bytes instead: one bucket count per `gamma`-power in the range, capped by
  the values one instance sees (`λ · x / card(G)` for a window, `λ · L / card(G)`
  for a query's merge accumulator), as sketch-bench's `dd_footprint` counts them. The range is the metric's, so `m` is an upper
  bound for a group whose own values span less.
- `c_ins`: CPU per insert, measured.
- `c_mrg`: CPU per pairwise merge, measured.
- `c_qry`: CPU per query of one instance, measured.

Costs split into four phases, each with CPU (mean CPU-sec/sec) and memory
(bytes):

| Phase | CPU | Memory |
|---|---|---|
| Ingest, per active `D` | `lambda × a_D × c_ins` | `card(G) × m × a_D` (open windows) |
| Merge, per RAQE | `card(G) × (n_i − 1) × c_mrg / T_i` | `card(G) × m` (one accumulator per group); 0 when `n_i = 1` |
| Query, per RAQE | `card(G) × c_qry / T_i` | `card(G) ×` output bytes |
| Storage, per active `D` | 0 | `card(G) × m × ((max_i S_i − x) / y + 1)` (closed windows) |

Closed windows wholly inside a lookback start in `[t − S, t − x]`, hence
`(S − x) / y + 1` of them; storage takes the longest lookback `D` serves.
Query output is estimated at 8 bytes per group, or 32 × 16 bytes per group for
top-k (sketch-bench's heap size, with 64-bit key hashes); it is not measured.

### Instance shape and size law

Each row of the cost table (from
[`scripts/study_saturation.py --phase optimizer-cost`](../scripts/study_saturation.py))
is measured on one instance, at whatever key count the benchmark fed it
(`measured_keys`). A key is one distinct entry an instance stores exactly,
such as one group's running sum in an exact accumulator. Turning a row into a
deployment's cost needs two properties of the family:

- **Shape**: instances per window. `PerGroup` keeps one per group, so
  `card(G)`; `Shared` keeps one for all groups.
- **Law**: how one instance's size grows. `Fixed` is set by the configuration
  (e.g. CMS rows × columns); `PerKey` stores one entry per key, so it grows
  linearly with keys.

Memory per window is instances × instance size:

| | Fixed | PerKey |
|---|---|---|
| **PerGroup** | `card(G) × m`: CMS, CountSketch, KLL, DDSketch, HLL, UnivMon, top-k (heap `m · k`) | `card(G) × m × keys_per_group / measured_keys`: none yet (e.g. an exact top-k map) |
| **Shared** | `m`: HydraKLL and other Hydra sketches | `m × card(G) / measured_keys`: exact sum, min, max, increase (one value per group, so keys = groups) |

The same factor scales merge and query CPU. Insert CPU is per sample and does
not scale.

Quantile sketches have no keys, but are not strictly fixed either: KLL grows
slowly with the number of values inserted (about `k × log(n / k)`), and
DDSketch with the range of values. The model treats KLL as Fixed at the size
the export measured (1,000,000 values per instance), so it misstates instances
that see far fewer or far more values. DDSketch is sized from the metric's
`value_range` when given (capped by values per instance), else from the
measured size.

The model uses `I × m`, where `I` is instances per window:

- PerGroup + Fixed: `I = card(G)`.
- Shared + PerKey reduces to `I = card(G)` because the export divides the
  exact accumulators' memory and merge cost by `measured_keys`
  (`groups_per_instance`), making `m` a per-group cost. Their query cost is
  already per group.
- Shared + Fixed: `I = 1`. In code this is the family property
  `one_fixed_size_sketch_for_all_groups`, true only for HydraKLL. It applies
  to memory and merge; query stays `card(G) × c_qry`, one probe per group.
  A fixed-size sketch degrades as groups grow, so it is eligible only when
  `card(G)` is at most the `subpopulations` its row was measured at. This
  models a sketch keyed by the joined `G` value alone; the current rows fan
  out to every label subset (sketch-bench#165), and merging several windows
  is not yet measured (sketch-bench#166).

PerGroup + PerKey is deferred until a family needs it: it needs the key labels
`K`, the metric's labels minus `G`. So for every family in use, nothing scales
with keys below the group: a per-service top-k CMS costs the same however
many endpoints it counts.

Side by side, with `G = card(G)` and `C` closed windows:

| | KLL (per group) | exact-sum (per-group normalized) | HydraKLL (shared, fixed) |
|---|---|---|---|
| `m`, `c_mrg` in the row | one group's | whole ÷ groups | whole sketch |
| Ingest CPU | `λ·a_D·c_ins` | `λ·a_D·c_ins` | `λ·a_D·c_ins` |
| Ingest memory | `G·m·a_D` | `G·m·a_D` | `m·a_D` |
| Merge CPU | `G·(n_i−1)·c_mrg/T_i` | same | `(n_i−1)·c_mrg/T_i` |
| Merge memory | `G·m` | same | `m` |
| Query CPU | `G·c_qry/T_i` | same | `G·c_qry/T_i` |
| Query memory | `G ×` output bytes | same | `G ×` output bytes |
| Storage memory | `G·m·C` | same | `m·C` |
| Latency | `G·(c_qry + (n_i−1)·c_mrg)` | same | `(n_i−1)·c_mrg + G·c_qry` |

### Family properties and key tracker

Each family has properties, hardcoded by variant in `family_properties`
(an unknown variant panics rather than getting a default):

- `mergeable_across_windows`: a family without it only serves `S_i = x`.
  Every family in use merges.
- `one_fixed_size_sketch_for_all_groups`: the Shared + Fixed cell above.
- `needs_delta_set_key_tracker`: the sketch can't list its groups, so a
  DeltaSet (`exact-delta-set`) records each window's keys for the query to
  probe. True for HydraKLL. A candidate needing one carries the DeltaSet row
  and is dropped if the cost table has none; a deployment without its tracker
  is never eligible. The cost table may hold at most one DeltaSet row.

The tracker is priced as an exact accumulator on the same `x`, `y` and `G`,
in every phase: ingest, merge (union of the `n_i` windows' key sets), query
(enumerate keys) and storage. Its `m` is per key, so it adds `card(G) × m_ds`
per window. The AutoSketch baseline skips shared fixed-size families for now
(sketch-bench#159).

Per-RAQE latency is the serial CPU time of one query:

```text
latency_i = card(G) × c_qry + I × (n_i − 1) × c_mrg
```

summed over the sketch and its key tracker, if any.

It is not a wall-clock SLA: it assumes no parallel execution across groups and
no cheaper k-way merge.

## Procedure

1. Generate candidate deployments for each input RAQE, then deduplicate them.
2. Build the eligible candidate–RAQE pairs. Reject pairs that fail capability,
   metric, grouping, window alignment, or empirical accuracy.
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

CPU and memory are the sums over the four phases. The report also includes
each phase's CPU and memory, together with the selected deployment mapping.

## v1 scope and TODOs

- **Accuracy after merging:** KLL and top-k are planned only up to the
  largest shard count and N the study measured merge curves at (#158).
- **Query-result sharing:** v1 charges every RAQE its own query and merge CPU.
  Revisit when RAQE semantics and execution timing identify safe reuse cases.
- **Latency SLAs:** the MILP takes optional per-RAQE latency bounds; the
  enumerator reports latency but does not reject a mapping for it.
- **Memory model:** query memory sums every RAQE's merge and output memory, as
  if all queries run at once; real concurrency is not modeled. Query output
  size is an estimate.
- **Bursts:** CPU is a mean over time, so plans are sized for average load.
- **Static planning:** no RAQE churn, replanning, or migration cost.
- **Rollups:** v1 does not precompute merged rollups. Queries merge their
  selected base instances when they run.
- **Selection policy:** the enumerator reports the Pareto frontier; the MILP
  selects one point by the `w_cpu`/`w_mem` weights.

## Implementation status

`rqe-optimizer/` implements the `(x, y)` sliding-instance model, candidate
generation and pruning, eligibility checks, analytical objectives, and
streaming Pareto filtering. The separation between candidate generation,
enumeration, objectives, and Pareto filtering remains the intended boundary
for future solver work.

Exporter bugs, benchmark-dataset choices, and historic run counts belong in
`rqe_optimizer_TODO.md` or issue tracking, not in this problem statement.

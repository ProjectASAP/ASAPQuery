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

### Cost by use and batch latency

Status: agreed design (ProjectASAP/ASAPQuery#777), implemented in
`rqe-optimizer/src/usage.rs` (resource use, cost, latency) and
`milp::minimize_usage_cost`. The MILP minimizes the cost of a plan billed by
use, in two versions:

- **Version 1, no latency constraint:** the cheapest plan; its latency is
  reported. Sweeping an optional latency bound traces each method's
  cost–latency Pareto frontier.
- **Version 2, a batch latency SLA:** the cheapest plan whose batch latency,
  from the job placement (§6), is at most the SLA `L`, for a grid of SLAs.
Evaluated first on the synthetic mixed template set; methods: ASAP (this
MILP, with sharing), PerQuery (the same MILP without sharing) and
AutoSketch-Adapted (fixed configs chosen by memory).

#### 1. Definitions

| Symbol | Meaning |
|---|---|
| `D`, `i` | a candidate deployment; a RAQE |
| `x_D`, `y_D` | `D`'s window and slide; a window closes every `y_D` |
| `S_i`, `T_i` | RAQE `i`'s lookback and interval; it fires at `t = k · T_i` |
| `card(G)` | groups of `D`'s grouping |
| `inst_D` | instances per window: `card(G)`, or 1 for a sketch shared by all groups, plus `card(G)` for a key tracker if the family needs one |
| `λ_D` | samples/s arriving for `D`'s metric: `card(series) / scrape interval`, already a rate |
| `c_ins`, `c_mrg`, `c_qry` | measured CPU-seconds per insert, per pairwise merge, per query of one instance |
| `m` | measured bytes per instance; `w_D = Σ_parts inst · m`, one window of all instances |
| `n_{i,D} = S_i / x_D` | windows a query merges |
| batch | all query jobs issued at one instant |

**Measurement assumption (sketch-bench as is).** Every sketch operation is
single-threaded and compute-bound: sketch-bench's CPU time equals its wall
time (e.g. an insert phase of 62.65 ms CPU and 62.65 ms wall). So a job uses
at most one core and, on a full core, takes its CPU time. The cost table's
`*_cpu_secs` are all the model needs.

#### 2. Cost model

`cost = w1 · AUC(CPU) + w2 · AUC(memory)`: the mean vCPUs and GiB over time,
priced per vCPU and per GiB (CPU only: `(1, 0)`; Fargate's prices). CPU is
elastic: a job gets a core whenever it is ready.

This is billing by use, as on fine-grained autoscaling platforms (Cloud Run
with request-based billing, Dataflow streaming, Flink on Kubernetes with an
autoscaler), idealized: capacity follows the load at once. Billing for
provisioned capacity (VMs, or containers billed by allocation such as
Fargate) is not modeled. Since a job holds its memory only while it runs, its
memory × time is the same whenever it runs, and queueing a job would save
nothing.

#### 3. CPU: parts and how each is computed

Every "mean vCPUs" below is a long-run average rate, CPU-seconds per second,
over the same period: a whole number of hyperperiods `H = lcm(T_i, y_D)`,
over which every job pattern repeats exactly. A part with `w` CPU-seconds per
event every `P` seconds runs `H / P` times per hyperperiod, so its mean is
`(H / P) · w / H = w / P`; ingest is already a rate. So the three columns add.

| Part | When | CPU-seconds | Mean vCPUs |
|---|---|---|---|
| Ingest (the precompute) | continuously, as samples arrive | — | `ρ_D = λ_D · (x_D / y_D) · c_ins` |
| Compaction | at each window close | `c_D = (k_D − 1) · Σ_parts inst · c_mrg` | `c_D / y_D` |
| Query job of RAQE `i` | at each firing | `ℓ_{i,D} = card(G) · c_qry + Σ_parts inst · (n_{i,D} − 1) · c_mrg` | `ℓ_{i,D} / T_i` |

- Ingest runs on `k_D = ⌈ρ_D⌉` parallel workers (at least one; each uses at
  most one core), **split by sample**: each worker reads a share of the input
  (e.g. some Kafka partitions or scrape targets) with no shuffle by group, so
  it keeps its own open window instance of every group it sees.
- When a window closes (the watermark passes its end), a **compaction** job
  merges the `k_D` partial instances of each group into one stored instance.
  With `k_D = 1` there is none.
- Each firing of RAQE `i` issues one **query job**: merge the `n_{i,D}`
  stored instances of each group for the query window, then estimate. Merging
  is part of the query. It reads the newest window, which closes at the firing
  time, so it starts after that window's compaction.

`AUC(CPU) = Σ_D ρ_D + Σ_D c_D / y_D + Σ_i ℓ_{i,D} / T_i`: ingest is already
a rate; only compaction (per window close, every `y_D`) and queries (per
firing, every `T_i`) are divided by their period. It is fixed by the plan,
whatever the schedule.

In words:

- `ρ_D`: every second, `λ_D` samples arrive (the scrape interval is already
  inside `λ_D`), and each is inserted into the `x_D / y_D` windows still
  open, at `c_ins` CPU-seconds each: CPU-seconds per second.
- `c_D`: closing a window merges, for every instance, the `k_D` workers'
  partial copies into one, which takes `k_D − 1` merges. This happens once
  per slide.
- `ℓ_{i,D}`: a query merges each instance's `n_{i,D}` stored windows
  (`n − 1` merges per instance), then answers once per group. This happens
  once per interval. (A sketch shared by all groups merges one instance but
  still answers every group.)
- `AUC(CPU)` adds the three rates: work per second, in vCPUs.

#### 4. Memory: parts and how each is computed

Each part is counted once: a window counts as ingest memory while open and as
storage once compacted, and a query reads stored instances in place.

| Part | What | Bytes | Held |
|---|---|---|---|
| Ingest | open windows, one copy per worker | `I_D = w_D · (x_D / y_D) · k_D` | always |
| Storage | closed windows for the longest lookback served (one compacted copy) | `stored_D = max_i w_D · ((S_i − x_D) / y_D + 1)` | always |
| Compaction | the closed window's `k_D` partial copies, until merged | `k_D · w_D` (0 when `k_D = 1`) | while the compaction job runs |
| Query | the accumulators the merge creates (none when `n = 1`) and the output | `q_{i,D} = w_D · [n_{i,D} > 1] + card(G) · output bytes` | while the query job runs |

`AUC(memory) = Σ_D (I_D + stored_D + k_D · w_D · (compaction run time) / y_D)
+ Σ_i q_{i,D} · (query run time) / T_i`. CPU is elastic, so every job runs on its
own core: a compaction runs for `c_D` and a query for `ℓ_{i,D}`.

In words:

- `I_D`: every worker keeps one instance per group for each of the
  `x_D / y_D` windows still open, and there are `k_D` workers.
- `stored_D`: closed windows are kept until the longest lookback that `D`
  serves no longer needs them. That is `(S − x)/y + 1` windows per group.
- Compaction: when a window closes, a new one opens, so ingest still holds
  `x_D / y_D` open windows. The closed window's `k_D` partial copies stay in
  memory until the compaction has merged them. The merged copy takes the
  storage slot that the oldest window frees at the same moment, so it is not
  counted twice.
- `q_{i,D}`: a merge builds one accumulator per instance (a direct query,
  with `n = 1`, reads the stored window and needs none), plus the answer for
  every group.
- `AUC(memory)`: ingest and storage are held all the time. A compaction's
  and a query's memory are held only while they run, a fraction
  `run time / y_D` or `run time / T_i` of the time.

#### 5. Batch latency and the SLA

A batch's **latency** is the time from its issue until its last query job
finishes, compaction included. A plan's **query latency** is its worst batch
latency, given by the job placement (§6). Version 1 reports it; version 2
requires it to be at most the SLA `L`. Per-RAQE latency bounds
(`Raqe::latency_sla_ms`) are not used.

CPU is elastic, so no job waits for a core, and a batch's latency is its
longest **chain**: the newest window's compaction, then the query, each on a
full core, `c_D + ℓ_{i,D}` (`analytical_cost_model::chain_ms`). A plan's query
latency is the longest chain over its RAQEs.

In words: the trade-off between cost and latency comes from the plan. A cheap
plan keeps short windows and merges many of them at query time (long
chains); a faster one precomputes more at ingest (more open windows, more CPU
and memory). With no bound the MILP picks the cheapest plan, whatever its
latency; a bound `L` asks for the cheapest plan at least that fast.

#### 6. Job placement algorithm

The job placement decides when each job runs on the available CPU, and so
gives every batch's latency. It is defined for any CPU capacity; billing by
use makes CPU elastic, so it runs with unlimited capacity.

```text
capacity for jobs  K = C − Σ_D ρ_D      (C = ∞ when CPU is elastic)
event-driven, over the hyperperiod H = lcm(T_i, y_D), twice
(events: query issues, window closes, completions):
  ready = compaction jobs (triggered at window closes)
        + query jobs whose newest window is compacted
  order: issue / trigger time (older first), then compaction before query
         (it unblocks queries), then longest remaining work first
  give rates in that order: job j gets a_j = min(1, K − rates already given)
  a job at rate a_j finishes when its remaining work / a_j elapses
  recompute the rates at every event
outputs: per-RAQE and per-batch latency, worst batch latency
```

In words:

1. **Reserve ingest.** Ingest runs all the time and takes `Σ ρ_D` vCPUs; the
   rest, `K`, is shared by compaction and query jobs.
2. **Issue jobs.** Each window close triggers a compaction job for its
   deployment. Each firing issues a query job, which becomes ready only once
   its deployment's newest window is compacted.
3. **Prioritize.** Ready jobs run oldest batch first, so no batch starves.
   Within a batch, compaction runs before queries, because queries wait for
   it. Then the longest remaining work runs first, so a long job doesn't end
   up running alone at the end of the batch.
4. **Share capacity as rates.** In that order, each job gets up to one core
   (a single-threaded job can't use more) from the capacity still free. The
   last one served may get only a fraction and runs slower in proportion.
5. **Advance to the next event.** Time jumps to the next query issue, window
   close or completion; remaining work drops by rate × elapsed time, and the
   rates are recomputed.
6. **Read off the results.** A query's latency is its finish minus its
   issue, and a batch's latency is that of its last query.

With elastic CPU (`K = ∞`) every job starts as soon as it is ready on its own
core, so the placement has a closed form: a compaction runs for `c_D`, a
query for `ℓ_{i,D}`, and a batch's latency is its longest chain
`c_D + ℓ_{i,D}`. The code uses this closed form (`usage::usage_cost`), and
it is what makes both MILP versions exact.

#### 7. MILP formulation

Variables: `z_{i,D} ∈ {0,1}` (RAQE `i` uses `D`, for eligible pairs `E`),
`u_D ∈ {0,1}` (`D` is active), `stored_D ≥ 0`. Constants per candidate and
pair are §3 and §4's.

```text
minimize  w1 · [ Σ_D (ρ_D + c_D / y_D) · u_D + Σ_(i,D) (ℓ_{i,D} / T_i) · z_{i,D} ]
        + w2 · [ Σ_D ((I_D + k_D · w_D · c_D / y_D) · u_D + stored_D)
                 + Σ_(i,D) (q_{i,D} · ℓ_{i,D} / T_i) · z_{i,D} ]
s.t.      Σ_D z_{i,D} = 1                        for every RAQE i
          z_{i,D} ≤ u_D ≤ Σ_i z_{i,D}             for (i, D) ∈ E
          stored_D ≥ w_D · ((S_i − x_D) / y_D + 1) · z_{i,D}
version 2, a batch latency SLA L (or version 1's frontier bound L):
          z_{i,D} = 0  if c_D + ℓ_{i,D} > L
```

In words:

- The objective is the price of the mean vCPUs (ingest and compaction per
  active deployment, plus each RAQE's query work per second), plus the price
  of the mean memory (ingest and storage per active deployment, each
  compaction's partial copies for the fraction of time it runs, and each
  query's memory for the fraction of time it runs).
- Every RAQE is served by exactly one eligible deployment.
- A deployment is active if and only if some RAQE uses it, so its ingest is
  paid once, however many RAQEs share it.
- A deployment stores enough closed windows for the longest lookback it
  serves (a linearized max).
- Version 2 (and version 1's frontier bound): a pair whose chain (the newest
  window's compaction, then the query, each on a full core) is longer than
  `L` is ruled out. By the placement's closed form, the plan's batch latency
  is then at most `L`, exactly: the SLA is met. Chains are
  compared with `L` with a relative slack of `1e-9`, so float error can't rule
  out a chain equal to `L`.

#### 8. Solution method

Every term is linear in `u` and `z`, with or without a bound, so the MILP is
solved exactly
(`milp::minimize_usage_cost`; costs are scaled by each RAQE's cheapest pair
so the solver sees magnitudes near 1). The solved plan's cost and latency are
then computed from its resource use (`usage::usage_cost`).

- **PerQuery** is the same MILP with sharing ruled out. `minimize_usage_cost`
  takes `allowed`, the candidate indices each RAQE may use. PerQuery gives
  each RAQE its own candidates, with a separate copy of any deployment that
  two RAQEs could both use. Two RAQEs share a deployment only by choosing the
  same candidate index (`u_D` is per index), so their copies stay apart and
  each pays its own ingest.
- **AutoSketch** keeps its memory-chosen configs and is priced by the same
  function: its cost by use and its latency, the longest chain. It ignores
  latency, so it is one point, not a frontier.
- **Version 1:** ASAP and PerQuery are solved with no bound (the cheapest
  plan), and for a sweep of bounds `L` from the smallest feasible one (the
  largest, over RAQEs, of each RAQE's fastest chain) up to the unbounded
  plan's latency. Each solve gives a (latency, cost) point; together they are
  the method's frontier. `L` = AutoSketch's latency gives each method's cost
  at no more than AutoSketch's latency.
- **Version 2:** ASAP and PerQuery are solved at each SLA of a fixed grid;
  an SLA below the smallest feasible bound has no plan. AutoSketch ignores
  the SLA; its latency either meets it or not.

#### 9. Outputs

Per (method, weight setting, bound or SLA): cost; mean CPU and memory, by
part; query latency and per-RAQE latency; planning time. Figures: version 1,
each method's cost–latency frontier for each workload (ASAP and PerQuery as
lines, AutoSketch as a point); version 2, each method's cost at each SLA.

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

On its own it is not a wall-clock SLA: it assumes no parallel execution across
groups and no cheaper k-way merge. In "Cost by use and batch latency" above it
is one firing's query job `ℓ_{i,D}` (merge, then estimate) on one core.

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
- **Latency SLAs:** `minimize` takes optional per-RAQE latency bounds;
  `minimize_usage_cost` has none and reports the batch latency (above). The
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

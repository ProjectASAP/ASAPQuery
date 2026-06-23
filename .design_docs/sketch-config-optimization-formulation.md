# Sketch/Streaming Config Selection as an Optimization Problem

## Motivation

`asap-planner-rs` currently hardcodes the decisions that should be the output
of an optimization:

- **Sketch choice** — `planner/sketch.rs`: a fixed `Statistic → AggregationType`
  map (`map_statistic_to_precompute_operator`).
- **Sketch parameters** — `planner/sketch.rs:6-14`: fixed constants
  (e.g. CMS depth=3/width=1024), overridable via `SketchParameterOverrides`
  but not chosen algorithmically.
- **Window size** — `planner/window.rs`: always tumbling
  (`should_use_sliding_window()` hardcoded to `false`, "sliding windows
  crash Arroyo"), size = effective repeat interval or scrape interval.
- **Query support** — `planner/promql.rs:163-169`: `is_supported()` is a
  boolean AST-pattern match, with no notion of cost/accuracy tradeoff.

There is no mechanism for one streaming config to serve multiple queries
(`CAPABILITY_MATCHING_DESIGN.md` is exploring this separately), and no
connection between `sketch-bench`'s empirical cost/accuracy measurements and
the planner's decisions — `sketch-bench` is a standalone offline benchmark
today.

This doc formalizes sketch/config/window selection as a single optimization
problem (v1 scope: static, one-shot — see "Out of scope for v1" below).

## Inputs

- **RQE** (repeating query expression): `r = (QE_r, T_r)` — a query
  expression repeating every `T_r` seconds (e.g. a dashboard panel's refresh
  interval).
- **QE** (query expression): a tree of AQEs joined by binary arithmetic
  operators. E.g. `quantile_over_time(0.5, data[1m]) / quantile_over_time(0.9, data[5m])`
  is one QE with two AQEs.
- **AQE** (atomic query expression): a single aggregation
  (metric + label set + statistic + spatial op + range vector). AQEs are
  deduplicated by structural identity across all RQEs. The same AQE can
  appear in multiple RQEs, each with its own `T_r`.
- `range_a` — the lookback duration baked into AQE `a`'s range vector
  selector (e.g. the `5m` in `data[5m]`). This is a static property of the
  AQE itself, not something the optimizer estimates.
- `θ_a` — profiled cardinality/skew characteristics of `a`. Computed by an
  external profiling/estimation step (sampling) **before** the optimization
  runs; consumed as opaque input data. (How to build this profiling step is
  punted — see below.)
- `ρ_g` — real-world arrival rate of data feeding streaming config `g`
  (e.g. Prometheus scrape rate × number of active series matching `g`'s
  metric/label set). Defined on `g`'s own metric/label set, not on any
  particular assigned AQE, for the same reason retention is a field of `g`
  rather than derived from the assignment. Needed because sketch-bench's
  throughput numbers measure a sketch's *capacity* (max items/sec it can
  absorb), not the cost at the actual production rate. `IngestCost(g)`
  should be computed as `per_item_cost(g) × ρ_g` where `per_item_cost(g) =
  total_CPU_time / item_count` from sketch-bench's benchmark run, not read
  directly off the benchmark's reported throughput.
- `ε_a` — required accuracy tolerance for `a`, derived from the query's
  semantics.
- sketch-bench data: empirical (and where available, analytic) cost/accuracy
  numbers as a function of `(sketch_type, params, dataset characteristics)`.

## Sets

- `A = {a_1, ..., a_m}` — all AQEs (deduplicated).
- `R = {r_1, ..., r_n}` — all RQEs, each with `A_r ⊆ A`.
- `R_a = {r ∈ R : a ∈ A_r}` — RQEs that reference AQE `a`.
- `G = {g_1, ..., g_k}` — candidate streaming configs (see "Generating G").
  Each `g = (sketch_type, params, ingest_type, W, S, retention_windows,
  metric, label_set, spatial_filter)` where `ingest_type ∈ {tumbling,
  sliding}`, `W` = window size, `S` = slide interval (`S = W` for tumbling,
  `S < W` for sliding). For every AQE `a`, `G` includes a dedicated
  `EXACT_a` config: no streaming dataflow, answers `a` directly from
  already-retained raw data at query time (`IngestCost = 0`, `Error = 0`).

## Derived quantities

- `f_a = Σ_{r ∈ R_a} 1/T_r` — query frequency for AQE `a` (queries/sec),
  combining all RQEs that reference it.
- `IngestCost(g) = w₁·CPU_ingest(g) + w₂·Mem_ingest(g)` — a **rate** (cost
  per second), purely a function of `g`. Crucially does **not** depend on
  which AQEs are assigned to `g` (see "Why retention is part of g").
  Concrete formulas given in "Analytical Cost Model" below.
- `QueryCost(a,g) = w₃·CPU_query(a,g) + w₄·Mem_query(a,g)` — cost of **one**
  query of `a` against `g`, a function of `(a, g, θ_a, range_a)`. Per-event
  cost, not a rate. (Latency is *not* folded in here — see optional latency
  constraint below.) Concrete formulas given in "Analytical Cost Model" below.
- `Error(a,g,θ_a)` — accuracy of `g` answering `a` given `a`'s real profile.
  Use closed-form bounds where the sketch has one (e.g. CMS's ε-δ
  guarantee); empirical sketch-bench lookup at the nearest profiled point
  otherwise. `Error(a, EXACT_a, θ_a) = 0` always.
- `Feasible(a,g) ∈ {0,1}`: `g` can answer `a` if and only if —
  1. `g`'s label set/metric/spatial-filter capability covers what `a` needs
     (capability matching — generalizes `CAPABILITY_MATCHING_DESIGN.md`).
     Directional: `g` computed at a finer label granularity can serve an
     `a` that wants a coarser rollup (merge at query time, costed in
     `QueryCost`); the reverse is never feasible — once `g` aggregates
     away a label, that information is gone and no `a` needing that finer
     granularity can be served from it.
  2. `retention_windows(g) · window_size(g) ≥ range_a` (g retains enough
     history to answer a's lookback range),
  3. `Error(a,g,θ_a) ≤ ε_a`,
  4. A valid (ingest type, query method) combination exists for `(g, a)` —
     see the compatibility table in "Analytical Cost Model". Concretely:
     sliding ingestion with `W < range_a` is always infeasible; tumbling
     with `W < range_a` requires `mergeable(s)` or `subtractable(s)`.

  Folding all four checks into `Feasible` means none needs to appear as a
  separate constraint later — they just determine which `(a,g)` pairs are
  legal.

## Analytical Cost Model

This section gives concrete formulas for `IngestCost(g)` and `QueryCost(a,g)`
in terms of atomic per-sketch costs from sketch-bench and structural
multipliers derived from window configuration and sketch algebraic properties.

### Sketch algebraic properties

Three boolean properties per sketch type `s`. Exact values per sketch family
are to be catalogued separately (deferred — see "Out of scope for v1"):

- **`mergeable(s)`** — two instances can be combined into one representing the
  union of their input streams. Required for multi-window range queries via
  merge at query time.
- **`subtractable(s)`** — one instance can be approximately subtracted from
  another to give the difference of their input streams. Enables O(1)-cost
  range queries via prefix-sum checkpoints regardless of `range_a/W`. Only
  valid with tumbling ingestion: sliding ingestion's overlapping retained
  windows are non-prefix-summable, ruling out subtraction.
- **`subpopulation_aware(s)`** — one sketch instance handles all label-group
  keys internally (e.g. CMS maps arbitrary string keys to counts in a single
  structure). When `false`, a separate instance is maintained per distinct
  label-group combination and costs scale with `N_g`.

### Atomic costs from sketch-bench

For sketch type `s`, params `p`:

| Symbol | Meaning |
|--------|---------|
| `mem(s,p)` | memory of one sketch instance |
| `insert_cpu(s,p)` | CPU per item inserted |
| `merge_cpu(s,p)` | CPU to merge two instances (`mergeable(s)` only) |
| `subtract_cpu(s,p)` | CPU to subtract two instances (`subtractable(s)` only) |
| `query_cpu(s,p)` | CPU to answer one query from one instance |

These are extracted from sketch-bench as: `per_item_cost = total_CPU_time /
item_count` for insert; direct measurements for merge/subtract/query. See
"Data gap" section — cardinality sweeps needed before these are available
across the full parameter grid.

### Label-group scaling multiplier

```
N(s,g) = 1     if subpopulation_aware(s)
         N_g    otherwise
```

where `N_g` = number of distinct label-group combinations for `g`'s
metric/label set (from `θ_a`). Applies uniformly to memory, query CPU, and
merge/subtract CPU — both subpopulation-aware and non-subpopulation-aware
sketches use the same formulas below with this multiplier.

### Window constraints (on `g`'s parameters)

```
W ≤ range_a    — a sketch must not over-cover the query range; data from
                  before range_a seconds ago is permanently absorbed and
                  cannot be extracted after the fact
W ≤ T_r        — freshness: a completed window must be available for each
                  query repetition cycle
```

For sliding ingestion: "neither" (the only valid sliding query method)
requires `range_a ≤ W`, combined with `W ≤ range_a` this forces `W =
range_a`. Sliding ingestion is therefore only valid when `range_a ≤ T_r`.

### Valid (ingest type, query method) combinations

The query method is fully determined by ingest type, the relationship of `W`
to `range_a`, and the sketch's algebraic properties. It is not an independent
decision variable:

| Ingest type | Condition | Query method |
|-------------|-----------|--------------|
| Tumbling | `W = range_a` | **Neither** — direct read from one window |
| Tumbling | `W < range_a`, `subtractable(s)` | **Subtract** |
| Tumbling | `W < range_a`, `mergeable(s)`, not subtractable | **Merge** |
| Tumbling | `W < range_a`, neither property | **Infeasible** |
| Sliding | `W = range_a` (forced) | **Neither** |
| Sliding | `W < range_a` | **Infeasible** — overlapping retained windows cannot be merged or subtracted |

### Cost formulas

Let `n = ⌈range_a / W⌉` (number of retained windows needed to cover `range_a`).

#### Ingestion memory (steady-state, two persistent components)

**Active windows** — in-flight, currently being written to:

| Ingest type | `Mem_active(g)` |
|-------------|----------------|
| Tumbling | `N(s,g) × mem(s,p)` |
| Sliding | `⌈W/S⌉ × N(s,g) × mem(s,p)` |

Sliding has `⌈W/S⌉` concurrent overlapping windows open at any moment;
each arriving item is inserted into all of them.

**Retained windows** — completed windows kept for query lookback:

| Ingest type | `Mem_retain(g)` |
|-------------|----------------|
| Tumbling | `n × N(s,g) × mem(s,p)` |
| Sliding | `N(s,g) × mem(s,p)` (W = range_a forced, so n = 1) |

Note: for subtractable sketches (tumbling only), one additional running
cumulative prefix instance is maintained in active memory:
`+N(s,g) × mem(s,p)` added to `Mem_active`.

**Total ingestion memory:**
```
Mem_ingest(g) = Mem_active(g) + Mem_retain(g)
```

#### Ingestion CPU (rate — cost per second)

| Ingest type | `CPU_ingest(g)` |
|-------------|----------------|
| Tumbling | `ρ_g × insert_cpu(s,p)` |
| Sliding | `ρ_g × ⌈W/S⌉ × insert_cpu(s,p)` |

Does **not** scale with `N(s,g)`: each arriving item triggers exactly one
insert regardless of subpopulation structure (routing overhead ignored per
modelling assumption).

#### Query CPU (cost per query invocation)

| Query method | `CPU_query(a,g)` |
|--------------|----------------|
| Neither | `N(s,g) × query_cpu(s,p)` |
| Merge | `N(s,g) × ((n - 1) × merge_cpu(s,p) + query_cpu(s,p))` |
| Subtract | `N(s,g) × (subtract_cpu(s,p) + query_cpu(s,p))` |

For merge: `n - 1` pairwise merge operations reduce `n` retained windows to
one, then one query on the result. Subtract is O(1) with respect to `n`.

#### Query memory (transient — peak working memory per query invocation)

| Query method | `Mem_query(a,g)` |
|--------------|----------------|
| Neither | `N(s,g) × mem(s,p)` |
| Merge | `n × N(s,g) × mem(s,p)` (reducible to `2 × N(s,g) × mem(s,p)` with incremental merge) |
| Subtract | `2 × N(s,g) × mem(s,p)` (two prefix checkpoints loaded simultaneously) |

Query memory is transient (not steady-state) and is not amortised across
concurrent queries (concurrent queries are out of scope for v1).

### Connection to the MIP objective

```
IngestCost(g)   = w₁ × CPU_ingest(g) + w₂ × Mem_ingest(g)
QueryCost(a,g)  = w₃ × CPU_query(a,g) + w₄ × Mem_query(a,g)
```

Both are now fully concrete functions of `g` and the profiled inputs
`(θ_a, ρ_g, range_a)`. The query method (neither / merge / subtract) is
read off the compatibility table above given `g`'s ingest type, `W`, and
`s`'s algebraic properties — so `QueryCost(a,g)` remains assignment-independent.

## Decision variables

- `y_g ∈ {0,1}` — is config `g` deployed.
- `x_{a,g} ∈ {0,1}`, defined only where `Feasible(a,g)=1` — is AQE `a`
  served by config `g`.

## Objective

```
minimize  Σ_g y_g · IngestCost(g)  +  Σ_{a,g} x_{a,g} · f_a · QueryCost(a,g)
```

Both terms are cost-per-second; `f_a` converts the per-query `QueryCost`
into a rate so it's commensurate with the continuously-accruing
`IngestCost`.

## Constraints

```
Σ_{g: Feasible(a,g)} x_{a,g} = 1     for all a ∈ A   (each AQE assigned to exactly one config)
x_{a,g} ≤ y_g                        for all a, g     (can't use an undeployed config)
x_{a,g}, y_g ∈ {0,1}
```

**Optional latency constraint** (v1: off by default). For AQEs that
specify an `SLA_a` (otherwise `SLA_a = ∞`, i.e. unconstrained):

```
x_{a,g} = 1  ⟹  Latency_query(a,g) ≤ SLA_a
```

equivalently, fold this into `Feasible(a,g)` as a fifth condition
(`Feasible(a,g) := ... AND Latency_query(a,g) ≤ SLA_a`), keeping the same
"infeasible pairs are filtered out before the MIP runs" pattern used for
accuracy, retention, and ingest/query compatibility.

Because `EXACT_a` is always feasible for `a`, the assignment constraint is
always satisfiable — there's no need for a separate "is this RQE supported"
boolean or a hard-coverage constraint; "unsupported" simply means the
optimizer picked `EXACT_a` because no sketch config met `ε_a` more cheaply.

## Problem class

This is an **uncapacitated facility-location MIP**: `y_g` = "open facility
g", `x_{a,g}` = "assign demand point a to facility g", `IngestCost(g)` =
facility's fixed cost, `f_a·QueryCost(a,g)` = assignment cost. Standard
structure — solvable via off-the-shelf MIP solvers (CBC/OR-tools/Gurobi) or
LP-relaxation + rounding heuristics. Expected problem size (AQEs × candidate
configs in the hundreds) should be comfortably within reach of exact
solvers. No wall-clock solve-time budget imposed on v1 — the optimizer runs
as an offline planning step before deployment, so any solve time is
acceptable.

## Generating `G`

`G` is not given — it must be enumerated:

1. Per AQE `a`, propose candidates from sketch types compatible with `a`'s
   statistic type (today's `map_statistic_to_precompute_operator`
   compatibility list) × sketch-bench's parameter grid × a window-size grid
   (multiples of scrape interval) × a retention-depth grid (multiples of
   `range_a / window_size`, since deeper retention only matters if some AQE
   needs that much range).
2. For sharing across AQEs, check pairs/groups with the same metric and a
   label-superset relationship: if one AQE's labels are a superset of
   another's and windows align, a single rolled-up config can serve both —
   this is the `CAPABILITY_MATCHING_DESIGN.md` predicate, reused as
   `Feasible`.
3. `θ_a` profiling must run before `Error(a,g,θ_a)` can be computed for any
   `g` — it's a precursor pass, not part of the MIP.

For v1, assume this enumeration is combinatorially manageable (no pruning
heuristic designed yet — revisit if `|G|` turns out too large for the
solver in practice).

**Candidate generation is reactive, not proactive.** For each metric, only
propose configs at label granularities that existing AQEs in the current
optimization run actually need — do not enumerate finer label combinations
than any AQE requires. A finer-grained config that no current AQE benefits
from only adds ingest cost with no benefit in a static one-shot problem.
(Cross-AQE sharing via step 2 still happens: if AQE `a1` needs `{region}`
and AQE `a2` needs `{region, instance}`, `a2`'s generated config at
`{region, instance}` is also checked as a candidate for `a1`, since
`Feasible(a1, g_{region,instance}) = true` via merge.)

### Data gap: sketch-bench doesn't yet have the data this formulation needs

Investigated `sketch-bench`'s actual schema and sweep behavior; two findings
that block using it as-is for `Error(a,g,θ_a)` lookups:

- **Cardinality is not a swept parameter.** `sketch-cli/src/main.rs:85-87`
  takes a single `--cardinality` value per invocation (default 100,000);
  `docs/BENCH_SWEEP.md`'s default grids only vary sketch params (e.g. CMS
  `rows×cols`, HLL `lg_k`), never cardinality. A real output record looks
  like:
  ```json
  {"sketch_config": {"family": "cms", "params": {"cols": 1024, "rows": 3}},
   "workload": {"shape": "zipf", "size": 1000000, "cardinality": 100000, ...},
   "bench": {"accuracy": {"relative_error_mean": 13.438, ...}}}
  ```
  `cardinality` is fixed per run, not a list. To get accuracy at multiple
  cardinalities (needed to look up `Error` at whatever effective cardinality
  a given `window_size` implies), someone has to add cardinality to the
  sweep grid and re-run the benchmarks — this data doesn't exist yet.
- **No windowing concept at all.** The only "window" in `sketch-core`
  (`report.rs:17`) means measurement-repetition window (N runs averaged),
  not a stream/tumbling window. Workloads are loaded as one static
  in-memory batch, then queried (README.md:120). The formulation's
  assumption — "window size determines the effective cardinality the
  sketch must absorb before flush, so look that up against sketch-bench's
  cardinality axis" — translates streaming semantics onto sketch-bench's
  batch semantics. That translation is plausible but unverified: it assumes
  a tumbling window's insert pattern is well-approximated by sketch-bench's
  batch-load-then-query pattern, which sketch-bench was never designed to
  validate.

Until sketch-bench's sweep grid includes cardinality and someone confirms
the batch-vs-window translation is reasonable, analytic error bounds (where
they exist) are the safer source for `Error(a,g,θ_a)` rather than empirical
lookup.

Extending sketch-bench (adding cardinality to the sweep grids, validating
the batch-vs-window translation) is in scope for this project, not punted
to a separate workstream.

### Data gap: heap-based sketches have cardinality-dependent memory

`CountMinSketchWithHeap` (`asap-internal/sketch-core/src/count_min_with_heap.rs:46-67`)
holds its top-k entries in a `Vec<HeapItem>` that starts empty and grows on
insert until it reaches `heap_size`; each `HeapItem` stores a `String` key,
so memory depends on the number of distinct keys actually seen (up to the
cap) and on key length — not purely on configured params. (`HydraKLL` is
fine: a fixed-size grid of fixed-capacity KLL buffers, memory determined
entirely by `params`.) This is the same structural risk retention posed:
`Mem_ingest(g)` would depend on the assignment (how many distinct keys the
assigned AQEs' data actually has), not just on `g`.

`CountMinSketchWithHeap` is also not currently benchmarked in sketch-bench
at all (`sketch-cli/src/wrappers/cms.rs` only wraps non-heap CMS variants:
`oxide`, `datasketches`, `lib_vector2d_*`) — another item to add when
extending sketch-bench.

Fix, analogous to retention: use the **worst-case (saturated) memory
bound** — `heap_size · avg_key_size` — as `Mem_ingest(g)` for any config
using this sketch, rather than measuring actual usage. This keeps
`IngestCost(g)` assignment-independent (conservative when real cardinality
is below `heap_size`, exact once the heap saturates, which it will for any
AQE with cardinality ≥ `heap_size`).

## Out of scope for v1 (explicitly punted)

- **Hard resource budget constraints** (e.g. `Σ_g y_g·Mem_ingest(g) ≤
  M_total`). v1 only weights memory/CPU into the objective via `w₂`/`w₄`;
  nothing stops a solution from exceeding real cluster capacity. Add as a
  hard constraint later if this becomes a problem in practice.
- **Engine limitations** (e.g. Arroyo can't do sliding windows today) are
  treated purely as a filter on `G`'s generation (step 1 above), not as part
  of the math itself, so the formulation stays engine-agnostic as Arroyo's
  capabilities change.
- **Decomposing `OneTemporalOneSpatial` AQEs into separate temporal/spatial
  pipeline stages.** Today (and in this v1 formulation) a combined
  temporal+spatial AQE like `sum(quantile_over_time(0.9, metric[5m]))`
  (`planner/patterns.rs:75-103`) is one atomic unit mapped to one `g`. This
  hides sharing: two RQEs differing only in their spatial reducer (e.g.
  `sum(...)` vs `avg(...)` over the same temporal sub-computation) can't
  share the temporal stage if `g` stays atomic. Decomposing into a
  multi-stage pipeline (`g` as a DAG, not a single config) would expose
  this reuse but adds real modeling complexity (`Feasible`/`IngestCost`
  need to handle chained stages). Keep AQEs atomic for v1; revisit if this
  sharing turns out to matter in practice.
- **Workload drift / re-planning.** v1 assumes a static, one-shot
  optimization over a fixed, known set of RQEs. No switching-cost term for
  redeploying configs when the workload changes, and no online/adaptive
  profiling of `θ_a` (it's assumed measured once, upfront). Revisit once the
  system needs to handle RQE sets that change over time.
- **Weight calibration** (`w₁..w₄`). No method is specified yet for how
  these get chosen (e.g. fixed $/CPU-sec, $/GB-sec conversions, or some
  other tuning process). The math is agnostic to their values; calibrating
  them is a separate problem.
- **Global weights, not per-AQE/per-RQE.** v1 assumes one global
  `(w₁,w₂,w₃,w₄)` vector for the whole system, not per-AQE priorities (e.g.
  an on-call dashboard weighted more latency-sensitive than a weekly
  report). This is a deliberate v1 simplification, not a discovered
  constraint — revisit if different RQEs need genuinely different
  cost/accuracy tradeoffs.
- **`θ_a` profiling/estimation step itself.** This doc assumes `θ_a` is
  available as input; how to actually estimate real cardinality/skew per
  AQE (sampling strategy, accuracy of the estimate, cost of running it) is
  not designed here.
- **Binary-op evaluation cost.** Combining two AQE results at query time
  (e.g. the division in a quantile-ratio QE) is assumed negligible compared
  to sketch query cost and isn't given its own cost term. Revisit if this
  assumption turns out false for some operator/sketch combination.
- **Pruning `G`'s enumeration.** Assumed combinatorially manageable for v1
  with no pruning strategy designed. If sketch type × param grid × window
  size × retention depth × sharing candidates turns out too large for the
  solver, will need a pruning heuristic (e.g. only propose Pareto-optimal
  `(cost, accuracy)` configs per AQE) — not designed yet.
- **Extending sketch-bench to cover the cardinality/window axis** this
  formulation needs (see "Data gap" above) — adding cardinality to the
  default sweep grids, and validating that batch-load benchmarks transfer
  to tumbling-window insert patterns.
- **Sketch property catalogue** — which specific sketch families are
  `mergeable`, `subtractable`, `subpopulation_aware`. The analytical cost
  model uses these properties to determine valid query methods and cost
  scaling, but the actual per-family values are not yet catalogued. To be
  defined once all sketch implementations are reviewed (includes confirming
  whether `CountMinSketchWithHeap` is subtractable via its CMS matrix alone,
  whether `HydraKLL` is mergeable via element-wise KLL cell merging, etc.).

## Where this would slot into the code (not yet implemented)

- `planner/sketch.rs`'s hardcoded `Statistic → AggregationType` map and
  fixed parameter constants would be replaced by step 1 of `G`'s generation
  plus the MIP's `x_{a,g}` assignment.
- `planner/window.rs`'s hardcoded window sizing would become part of `G`'s
  window-size/retention-depth grid instead of a fixed formula.
- `CAPABILITY_MATCHING_DESIGN.md`'s capability-matching logic becomes the
  `Feasible(a,g)` predicate (extended with the accuracy and retention
  checks above).
- A new component is needed to: (a) run the `θ_a` profiling pass, (b) query
  sketch-bench data (or an analytic formula) for `IngestCost`, `QueryCost`,
  `Error`, (c) enumerate `G`, (d) invoke a MIP solver, (e) translate the
  `x_{a,g}`/`y_g` solution into `StreamingConfig`/`InferenceConfig` structs.

# RQE optimizer: cost models

This document is the one home for what a candidate costs (§3's table), the
canonical cost-by-use model with its MILP, and the still-supported
snapshot-AUC objective. Candidates and eligibility are in
[Candidate generation and eligibility](rqe_optimizer_candidates.md).

## Model status and relationship

- **Cost by use and batch latency** is the canonical model for the current
  ASAP-versus-AutoSketch evaluation. It accounts for ingest workers,
  window-close compaction, transient memory over its holding time, elastic CPU,
  and a batch-latency bound.
- **Resource primitives and the snapshot-AUC objective** remain supported for
  the original generic optimizer. They define the measured per-operation and
  per-instance inputs used everywhere, then use the older snapshot accounting:
  query memory is summed as if concurrent, there is no worker fan-out or
  compaction, and latency is serial query work. It is not the evaluation's
  billing model.
- **Hydra** is future work, not a candidate today.

The two implemented objectives share the same resource primitives and §3's
table; they differ in how they account for them over time, and in latency
(a batch's longest chain vs. each RAQE's serial query time).

## Cost by use and batch latency

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
| `a_D = x_D / y_D` | open windows; each sample is inserted into `a_D` instances |
| `S_i`, `T_i` | RAQE `i`'s lookback and interval; it fires at `t = k · T_i` |
| `G_d`, `G_r` | `D`'s grouping; RAQE `i`'s grouping, equal to `G_d` or, for a family that merges across groups, a subset of it (a **roll-up**) |
| `card(G)` | groups of grouping `G` |
| part | the sketch, plus a key tracker if the family needs one; every cost below is summed over parts |
| `I_d` | a part's instances per window: `card(G_d)`, or 1 for a sketch shared by all groups |
| `I_r` | the states a query of `i` ends with: `card(G_r)`, or 1 for a sketch shared by all groups |
| `λ_D` | samples/s arriving for `D`'s metric: `card(series) / scrape interval`, already a rate |
| `c_ins`, `c_mrg`, `c_qry` | measured CPU-seconds per insert, per pairwise merge, per query of one instance |
| `m` | measured bytes per instance; `w_D = Σ_parts I_d · m`, one window of all instances |
| `n_{i,D} = S_i / x_D` | windows a query merges |
| `k_D = ⌈ρ_D⌉` | ingest workers (at least one), from ingest CPU `ρ_D` (§3) |
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

#### 3. Cost of each phase: CPU and memory

This is the one cost table: every phase, with its CPU and its memory. Each
cell is per part; a phase's cost is the sum over parts.

| Phase | When | CPU-seconds per event | Mean vCPUs | Memory (bytes) | Held |
|---|---|---|---|---|---|
| Ingest (the precompute) | continuously, as samples arrive | — | `ρ_D = λ_D · a_D · c_ins` | `ingest_D = I_d · m · a_D · k_D` (open windows, one copy per worker) | always |
| Compaction | at each window close | `c_D = (k_D − 1) · I_d · c_mrg` | `c_D / y_D` | `k_D · I_d · m` (the closed window's partial copies; 0 when `k_D = 1`) | while it runs: `c_D` |
| Query job of RAQE `i` (merge, then estimate) | at each firing | `ℓ_{i,D} = card(G_r) · c_qry + (I_d · n_{i,D} − I_r) · c_mrg` | `ℓ_{i,D} / T_i` | `q_{i,D} = I_r · m(S_i) · [merges > 0] + card(G_r) · output bytes` | while it runs: `ℓ_{i,D}` |
| Storage | always | — | 0 | `stored_D = max_i I_d · m · ((S_i − x_D) / y_D + 1)` (closed windows, one compacted copy) | always |

- **No roll-up** (`G_r = G_d`, so `I_r = I_d`): the query merges
  `I_d · (n − 1)` times, and a direct query (`n = 1`) merges nothing and
  holds no accumulator.
- **Roll-up** (`G_r ⊂ G_d`): a query reads `I_d · n` states and ends with
  `I_r`, and each merge folds two into one, so it merges `I_d · n − I_r`
  times, even when direct. Ingest, compaction and storage stay at `G_d`;
  the query's accumulators, answers and output are at `G_r`.
- `m(S_i)` is an accumulator holding the lookback of one `G_r` group (it
  differs from `m` only for DDSketch on a metric with a value range, which is
  sized from the values it holds).
- Ingest runs on `k_D` parallel workers (each uses at most one core),
  **split by sample**: each worker reads a share of the input (e.g. some
  Kafka partitions or scrape targets) with no shuffle by group, so it keeps
  its own open window instance of every group it sees.
- When a window closes (the watermark passes its end), a **compaction** job
  merges the `k_D` partial instances of each group into one stored instance.
  With `k_D = 1` there is none. The merged copy takes the storage slot that
  the oldest window frees at the same moment, so it is not counted twice.
- Each firing of RAQE `i` issues one **query job**: merge the `n_{i,D}`
  stored instances of each group for the query window, then estimate.
  Merging is part of the query. It reads the newest window, which closes at
  the firing time, so it starts after that window's compaction. It reads
  stored instances in place.
- Closed windows wholly inside a lookback start in `[t − S, t − x]`, hence
  `(S − x) / y + 1` of them; storage takes the longest lookback `D` serves.
- Query output is estimated at 8 bytes per group, or `k` × 16 bytes per group
  for top-k (`k` the deployment's answered top-k, 32 by default, with 64-bit
  key hashes); it is not measured. A top-k sketch's `c_qry` is measured at
  `k = 32` and scaled by `k / 32`.
- `k_D = ⌈ρ_D⌉` takes the deployment's whole ingest CPU, summed over its
  parts (sketch and key tracker).

In words:

- `ρ_D`: every second, `λ_D` samples arrive (the scrape interval is already
  inside `λ_D`), and each is inserted into the `a_D` windows still open, at
  `c_ins` CPU-seconds each: CPU-seconds per second.
- `c_D`: closing a window merges, for every instance, the `k_D` workers'
  partial copies into one, which takes `k_D − 1` merges. This happens once
  per slide.
- `ℓ_{i,D}`: a query merges the stored windows it reads down to one state per
  answered group, then answers once per `G_r` group. This happens once per
  interval. (A sketch shared by all groups merges one instance but still
  answers every group.)
- `ingest_D`: every worker keeps one instance per group for each of the `a_D`
  windows still open, and there are `k_D` workers.
- `stored_D`: closed windows are kept until the longest lookback that `D`
  serves no longer needs them.
- `q_{i,D}`: a merge builds one accumulator per answered group, plus the
  answer for every group.

#### 4. AUC(CPU) and AUC(memory)

Every "mean vCPUs" is a long-run average rate, CPU-seconds per second, over
the same period: a whole number of hyperperiods `H = lcm(T_i, y_D)`, over
which every job pattern repeats exactly. A part with `w` CPU-seconds per
event every `P` seconds runs `H / P` times per hyperperiod, so its mean is
`(H / P) · w / H = w / P`; ingest is already a rate. So the columns add.

```text
AUC(CPU)    = Σ_D ρ_D + Σ_D c_D / y_D + Σ_i ℓ_{i,D} / T_i
AUC(memory) = Σ_D (ingest_D + stored_D + k_D · w_D · c_D / y_D)
            + Σ_i q_{i,D} · ℓ_{i,D} / T_i
```

CPU is elastic, so every job runs on its own core: a compaction runs for
`c_D` and a query for `ℓ_{i,D}`. Both AUCs are fixed by the plan, whatever
the schedule.

In words: `AUC(CPU)` adds the three rates, work per second in vCPUs; ingest
is already a rate, and only compaction (per window close, every `y_D`) and
queries (per firing, every `T_i`) are divided by their period.
`AUC(memory)` holds ingest and storage all the time, and a compaction's and
a query's memory only while they run, a fraction `run time / y_D` or
`run time / T_i` of the time.

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
        + w2 · [ Σ_D ((ingest_D + k_D · w_D · c_D / y_D) · u_D + stored_D)
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
(`milp::minimize_usage_cost`; costs are divided by the sum, over RAQEs, of
each RAQE's cheapest pair on its own, so the solver sees magnitudes near 1). The solved plan's cost and latency are
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

## Resource primitives and the snapshot-AUC objective

The measured per-operation primitives (`m`, `c_ins`, `c_mrg`, `c_qry`, from
the cost table) and how they scale with a deployment (instance shape and size
law, below) are shared by both objectives; the one cost table is §3's.

`m` is measured per instance, except for DDSketch on a metric with a
`value_range`: `m = (floor(ln(hi / lo) / ln((1 + alpha) / (1 - alpha))) + 1) × 8`
bytes, one bucket count per `gamma`-power in the range, capped by the values
one instance holds (`λ · x / card(G_d)` for a window, `λ · S / card(G_r)` for
a query's merge accumulator), as sketch-bench's `dd_footprint` counts them.
The range is the metric's, so `m` is an upper bound for a group whose own
values span less.

**Snapshot-AUC objective** (`milp::minimize`, `analytical_cost_model::score`;
still supported). It prices §3's table with older accounting:

- one ingest worker (`k_D = 1`), so no compaction;
- each RAQE's merge and query memory (`q_{i,D}`) held all the time, as if
  every query ran at once, instead of for `ℓ_{i,D} / T_i` of the time;
- a RAQE's latency is its query job's serial CPU time `ℓ_{i,D}`, with no
  compaction before it, checked against the RAQE's own
  `latency_sla_ms` (no batch latency).

Its CPU (`ρ_D`, `ℓ_{i,D} / T_i`) and its ingest and storage memory are §3's
with `k_D = 1`, roll-ups included.

### Instance shape and size law

Each row of the cost table (from
[`scripts/study_saturation.py --phase optimizer-cost`](../scripts/study_saturation.py))
is measured on one instance, at whatever key count the benchmark fed it
(`measured_keys`). A key is one distinct entry an instance stores exactly,
such as one group's running sum in an exact accumulator. Turning a row into a
deployment's cost needs two properties of the family:

- **Shape**: instances per window. `PerGroup` keeps one per group, so
  `card(G_d)`; `Shared` keeps one for all groups.
- **Law**: how one instance's size grows. `Fixed` is set by the configuration
  (e.g. CMS rows × columns); `PerKey` stores one entry per key, so it grows
  linearly with keys.

Memory per window is instances × instance size:

| | Fixed | PerKey |
|---|---|---|
| **PerGroup** | `card(G_d) × m`: CMS, CountSketch, KLL, DDSketch, HLL, UnivMon, top-k (heap `m · k`) | `card(G_d) × m × keys_per_group / measured_keys`: none yet (e.g. an exact top-k map) |
| **Shared** | `m`: HydraKLL and other Hydra sketches | `m × card(G_d) / measured_keys`: exact sum, min, max, increase (one value per group, so keys = groups) |

The same factor scales merge and query CPU. Insert CPU is per sample and does
not scale.

Quantile sketches have no keys. KLL's `m` is Fixed, and the same at every
item count, because `asap_sketchlib::KLL` allocates once at construction.
Since #192 the cost table's KLL rows report that allocation: item slots,
level index and merge buffer, 5536 B at `k = 50`. Cost tables measured
before it, including the committed `autosketch-vs-asap-inputs`, carry a
nominal `4 · k · sizeof(T)` instead (1600 B at `k = 50`; the allocation is
1.58× that at `k = 200`, 1.14× at `k = 800`), so they understate KLL
memory (#191).
DDSketch is sized from the metric's `value_range` when given (capped by
values per instance), else from the measured size.

The model uses `I_d × m`, where `I_d` is instances per window:

- PerGroup + Fixed: `I_d = card(G_d)`.
- Shared + PerKey reduces to `I_d = card(G_d)` because the export divides the
  exact accumulators' memory and merge cost by `measured_keys`
  (`groups_per_instance`), making `m` a per-group cost. Their query cost is
  already per group.
- Shared + Fixed: `I_d = 1`. In code this is the family property
  `one_fixed_size_sketch_for_all_groups`, true only for HydraKLL. It applies
  to memory and merge; query stays `card(G_r) × c_qry`, one probe per answered group.
  A fixed-size sketch degrades as groups grow, so it is eligible only when
  `card(G_d)` is at most the `subpopulations` its row was measured at. This
  models a sketch keyed by the joined `G` value alone; the current rows fan
  out to every label subset (sketch-bench#165), and merging several windows
  is not yet measured (sketch-bench#166).

PerGroup + PerKey is deferred until a family needs it: it needs the key labels
`K`, the metric's labels minus `G`. So for every family in use, nothing scales
with keys below the group: a per-service top-k CMS costs the same however
many endpoints it counts.

Side by side, what each family puts into §3's table (every phase then
follows from it):

| | KLL (per group) | exact-sum (per-group normalized) | HydraKLL (shared, fixed) |
|---|---|---|---|
| `m`, `c_mrg` in the row | one group's | whole ÷ groups | whole sketch |
| `I_d` (instances per window) | `card(G_d)` | `card(G_d)` | 1 |
| `I_r` (states a query ends with) | `card(G_r)` | `card(G_r)` | 1 |
| probes per query (`c_qry`) | `card(G_r)` | `card(G_r)` | `card(G_r)` |
| key tracker part | none | none | DeltaSet, `I_d = card(G_d)` |
| rolls up (`G_r ⊂ G_d`) | yes | yes | no |

HydraKLL has `FamilyProperties` but is in no capability's family list, so it
is never a candidate.

### Family properties and key tracker

Each family has properties, hardcoded by variant in `family_properties`
(an unknown variant panics rather than getting a default):

- `mergeable_across_windows`: a family without it only serves `S_i = x`.
  Every family in use merges.
- `mergeable_across_groups`: it can serve a RAQE at a subset grouping (a
  roll-up, [candidates](rqe_optimizer_candidates.md#fine-to-coarse-group-roll-ups)).
  True for exact sum/min/max, HLL, univmon-cardinality, KLL and DDSketch.
- `one_fixed_size_sketch_for_all_groups`: the Shared + Fixed cell above.
- `needs_delta_set_key_tracker`: the sketch can't list its groups, so a
  DeltaSet (`exact-delta-set`) records each window's keys for the query to
  probe. True for HydraKLL. A candidate needing one carries the DeltaSet row
  and is dropped if the cost table has none; a deployment without its tracker
  is never eligible. The cost table may hold at most one DeltaSet row.

The tracker is priced as an exact accumulator on the same `x`, `y` and `G`,
in every phase: ingest, merge (union of the `n_i` windows' key sets), query
(enumerate keys) and storage. Its `m` is per key, so it adds `card(G_d) × m_ds`
per window. The AutoSketch baseline skips shared fixed-size families for now
(sketch-bench#159).

A query job's serial CPU time, `ℓ_{i,D}` (§3), summed over the sketch and
its key tracker, is a RAQE's latency in the snapshot-AUC objective and the
query half of its chain in cost by use. On its own it is not a wall-clock
SLA: it assumes no parallel execution across groups and no cheaper k-way
merge.

# RQE optimizer: candidate generation and mapping

This document is the canonical design for candidate construction, eligibility
(accuracy included), finer-to-coarser roll-ups and dominance pruning. What a
candidate costs, and the MILP that maps RAQEs to candidates, are in
[Cost models](rqe_optimizer_cost_model.md).

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

Deployments exist only at groupings some RAQE asks for. For each (capability,
metric, spatial filter, grouping `G`), let `R(G)` be the RAQEs a deployment at
`G` may serve: those grouped exactly by `G` for a family that does not merge
across groups, and those grouped by any subset of `G` (roll-ups included) for
a family that does (`mergeable_across_groups`). A coarse RAQE so contributes
its lookback and interval to the fine deployment's windows and slides. Top-k
doesn't roll up, so its heaps come from exact-grouping RAQEs only. Then:

```text
for each RAQE set R(G), and each window x that divides some S_i in it and is a multiple of the scrape interval:
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

1. `cap_i = D.capability`, `metric_i = D.metric`, the same spatial filter,
   and `G_i = D.G`, or `G_i ⊂ D.G` when `D`'s family has
   `mergeable_across_groups` (a roll-up; see below). A coarse deployment never
   serves a finer RAQE.
2. `D.x % D.y = 0`, `S_i % D.x = 0`, and `T_i % D.y = 0`.
3. The accuracy of `D` for `r_i`, in the accuracy metric of `D`'s family
   (`family_properties`, which also gives the direction), clears `tol_i`.

Exact accumulators take that accuracy from the cost table; they merge without
loss, so the window size doesn't matter. Sketches take it
from the saturation study's error-vs-N curves (`SaturationCurves`, #156),
read at the number of items one answered group receives over the RAQE's
whole lookback: series per `G_i` group times scrapes per lookback. On a
roll-up the answered group is the RAQE's coarse one.

- The curve is the configuration's at the metric's fitted `data_shape` for
  the RAQE's grouping `G_i` (zipf θ and keys `K`, or tail index `a` for
  quantiles). Between grid
  points, the worst bracketing point; outside the grid, or with no fit, no
  accuracy.
- Between checkpoints, the worse neighbour. Below the first checkpoint, no
  accuracy. Past the last, the plateau if the point saturated, else none.
- The curve's `error_metric` must be the family's metric; `SaturationCurves::load`
  refuses a curve for another metric as a stale study.
- For sketches that merge exactly, the window size doesn't matter: a merged
  answer reads the curve at the lookback's item count, like a single sketch.
  KLL and univmon-cardinality merge lossily (#131; UnivMon rebuilds its
  heavy-hitter heaps from the union of the merged heaps): an answer merged from `n = S_i / D.x` windows,
  times the average fan-out `⌈card(D.G) / card(G_i)⌉` on a roll-up, reads
  the study's merge curve (`saturation_merge_curve.csv`, the sketch merged
  from `n` shards of the same items) at the lookback's item count. Between
  measured shard counts, the worse. Past the largest measured count, or past
  a merge curve's last N, the curves say nothing, so the answer takes the
  guarantee below, no better than the largest count's last measurement; the
  study should measure the merge counts a workload needs
  (`--merge-shards-list`) so the guarantee isn't what decides (#158);
  univmon-cardinality has no guarantee, so past its curves it has no
  accuracy. `SaturationCurves::load` refuses a study with no merge curves
  for a lossy non-heap candidate sketch (KLL, univmon-cardinality,
  univmon-topk). The fan-out is the average; the largest group's is
  #189.
- A top-k RAQE asks for its own `k` (`Raqe::topk_k`, default 32). Heap
  top-k (CMS-heap, CountSketch-heap) keeps a heap of `n · k` in a deployment
  answering from `n = S_i / D.x` merged windows (`n = 1` gives `k`).
  Candidates carry one heap per distinct `n · k` among the RAQEs on a window,
  and a RAQE is eligible only where the heap holds its `n · k`. Such a merged answer reads as one sketch: the
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

Before mapping, prune a candidate only if another candidate with the same
capability, metric, spatial filter and grouping can serve every RAQE it can,
and is no worse in ingest CPU and memory, in compaction (CPU-seconds per
window, and CPU and byte-seconds per second), and, for each RAQE it serves,
in query-job CPU (`ℓ`), query memory (merge accumulators and output) and
stored memory. Every cost in both objectives, and every chain (compaction, then
query), is monotone in these, so any mapping using the removed candidate can
substitute the remaining one without raising cost or latency.

Closed instances must be stored long enough to answer the RAQEs assigned to a
deployment. Retention is not a candidate parameter: a deployment stores the
history needed by the largest assigned query window, costed as the storage
phase ([Cost models](rqe_optimizer_cost_model.md), §3).

## Workload mapping

Every RAQE selects exactly one eligible candidate; several RAQEs may select
the same one and so share its ingest. A query may merge several instances
of its candidate, but v1 never combines results from several deployments to
answer one RAQE. The MILP over the eligible pairs, its objective and its
latency bound are in [Cost models](rqe_optimizer_cost_model.md) (§7 for cost
by use; the snapshot objective changes only its coefficients). The solver
never enumerates full mappings.

## Fine-to-coarse group roll-ups

A deployment grouped by `G_d` can answer a RAQE grouped by `G_r ⊂ G_d` when
its family merges across groups (`mergeable_across_groups`): at query time,
the fine instances of each coarse group are merged into one, then answered.
This is a **roll-up**. Families that do: exact sum/min/max, HLL,
univmon-cardinality, KLL and DDSketch. These don't, and serve only
`G_r = G_d`:

- **Rate/increase** (`exact-increase`). A coarse increase is the sum of its
  series' increases, `Σ_s increase_s`, so it is not the increase of the
  summed counter: a reset in one series is hidden by the others' growth.
  The accumulator tracks one counter, and its merge joins two pieces of it
  in time (a drop at the seam is a reset), so merging two groups' counters
  is wrong: A going 100 → 110 and B going 5 → 8 over the same minute merge
  to 10, not 13. A roll-up would need per-series state and a sum of values,
  not this merge. (ASAPQuery keys `MultipleIncrease` per series: it plans
  only bare `rate`/`increase`, which keep every label.)
- **Top-k** (CMS-heap, CountSketch-heap, univmon-topk). A coarse group's top
  `k` can hold keys that are in none of its fine groups' heaps.
- **Hydra**. It inserts every label subset of its schema, so a coarse
  grouping inside the schema is answered directly from the grid, never by
  merging fine groups; it is not a candidate today.

- **Cost**: ingest, compaction and storage stay at `G_d`; the query job merges
  `I_d · n − I_r` times and answers `card(G_r)` groups
  ([Cost models](rqe_optimizer_cost_model.md), §3). A direct roll-up
  (`n = 1`) still merges `card(G_d) − card(G_r)` instances.
- **Accuracy**: read at `G_r`: its `data_shape`, and its items per group over
  the lookback. KLL's and univmon-cardinality's merge curves are read at
  `⌈card(G_d)/card(G_r)⌉ · n` merged instances, the average fan-out, an estimate rather than a
  worst-group bound (uneven fan-out is #189).
- **Facts**: `validate_facts` requires `card(X) ≤ card(Y)` for every pair
  `X ⊂ Y` among the label sets a plan can use (every RAQE grouping on the
  metric, and its full label set), so a merge count is never negative.
  Label sets no RAQE groups by are not checked.
- **Plan output**: a RAQE rolls up exactly when its grouping differs from its
  deployment's.

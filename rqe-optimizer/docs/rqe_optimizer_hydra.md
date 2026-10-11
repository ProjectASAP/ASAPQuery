# RQE optimizer: admitting Hydra

Status: design, not implemented. Hydra is never a candidate today:
`hydra-kll` has `FamilyProperties` (`rqe-optimizer/src/lib.rs`) but is in no
capability's family list, and the other four Hydra variants have none. This
document says, for each Hydra combination sketch-bench benchmarks, what
accuracy guarantee it has, how it is costed, how it would enter the
candidates and the MILP, and what sketch-bench must measure first. Symbols
follow [Cost models](rqe_optimizer_cost_model.md) §1; `Λ` is a Hydra
schema (the hyperperiod there is `H`).

## 1. What Hydra is in sketch-bench

All five combinations wrap `asap_sketchlib::Hydra` (0.3.0; Manousis et al.,
VLDB 2022, arXiv:2208.04927). The `polars` rows of the same names are exact
baselines.

- **Grid.** `R` rows × `W` columns of cells, each an inner sketch, over a
  schema `Λ` of label columns (`hydra_shared.rs`).
- **Insert fans out.** A record with `|Λ| = d` labels is inserted into the
  cell of every non-empty label subset (`2^d − 1` subkeys, each tagged with
  its column names), in every row: `R · (2^d − 1)` inner inserts per record
  (`hydra.rs` `update`, masks `1..2^d`). The empty grouping (the whole
  stream) is not inserted.
- **Query.** A subpopulation (values for a subset `G_r ⊆ Λ`, the rest
  unconstrained) hashes to one cell per row; the answer is the **median**
  over the `R` rows of each cell's inner answer (`query_key`). The library
  answers any non-empty subset of `Λ`; sketch-bench's wrapper constrains
  only leading columns (`hydra_shared.rs` `query`), so it can ask only
  prefix groupings.
- **No keys.** Hydra stores no subpopulation keys. To answer every group of
  `G_r`, something else must list them (in the optimizer, a DeltaSet key
  tracker).
- **Merge** is cell by cell, needing the same dimensions, cell type and
  schema; every row's column comes from one seeded hash (`HYDRA_SEED`), so
  two grids built apart line up.

| Variant | Inner cell (canonical) | Counter bytes |
|---|---|---|
| `hydra-cms` | Count-Min, cell 3 × 512 i32; grid 3 × 128 | `R·W·cell_rows·cell_cols·4` |
| `hydra-cs` | Count Sketch, same shape | same |
| `hydra-hll` | HLL, `p = 14` fixed | `R·W·2^14` |
| `hydra-kll` | KLL, `cell_k = 200` | `R·W·(slots(k)·8 + 62·8)` (no merge buffer; #191) |
| `hydra-univmon` | UnivMon: heap 1000, 5 × 2048, 8 layers; grid 2 × 8 | `R·W·layers·((sr·sc + sr)·8 + heap·entry)` |

`hydra-univmon` is registered as four statistics: cardinality, L1, L2 and
entropy. Each footprint adds `(R·W + 1)` cell headers.

## 2. Accuracy guarantee per combination

### 2.1 The source of error

For a subpopulation `q` (one group of `G_r`), in row `j` its cell also holds
every other subkey hashed there, the colliders `C_j`. Let:

- `N_q`: items of `q`; `N_C`: items of the colliders in one row's cell;
- `M`: items inserted into one row, after fan-out: `N · (2^d − 1)` for `N`
  records. A coarser label subset is inserted too, so every label added to
  `Λ` roughly doubles `M`;
- `β`: a Markov factor. Modeling the column hash as random,
  `E[N_C] = Σ_{s≠q} N_s / W ≤ M / W`, so `N_C ≤ β·M/W` in a row with
  probability at least `1 − 1/β`;
- `δ_in`: the inner sketch's failure probability.

The answer is the median over rows (the mean when `R = 2`). If more than
half the rows give answers inside one common interval, so does the median,
so a bound below holds when more than half the rows are **good**: their
contamination is within `β·M/W` and their inner sketch is within its own
bound.

**Hydra's Theorem 2** (§4.5, checked against the paper): if each cell
estimates a **monotone G-sum** within `(1 ± ε_US)` with probability
`1 − δ_US`, then with `W = c(1 + ε_US)/ε` columns for a `c` with
`1 − δ_US − 1/c > 1/2`, and `R = O(log 1/δ)` rows, with probability
`1 − δ`, `G_q(1 − ε_US) ≤ Ĝ_q ≤ G_q(1 + ε_US) + ε·G_S`, where `G_S` is the
G-sum of the whole (fanned-out) stream. The proof takes the rows as
i.i.d. Three things about this library weaken it:

- **Inner errors are correlated across rows.** Every cell is a clone of one
  template, so the inner sketches hash with the same seeds (CMS and Count
  Sketch matrix seed, HLL's seed, KLL's RNG state): `q`'s own items collide
  the same way in every row. Only the contamination is independent across
  rows. A sound confidence is
  `P[Binomial(R, 1 − 1/β) > R/2] − R·δ_in`.
- **One fixed hash.** Columns come from one seeded hash (`HYDRA_SEED`), not
  a random family, so the probabilities are a random-oracle heuristic, and
  a group that collides badly collides in every window, merge and
  deployment. For `W` not a power of two the column is not uniform (at
  `W = 100` some columns get 1.56/W of the keys), so use the worst column's
  share in place of `1/W`.
- **Few rows.** It is asymptotic in `R`. The canonical `hydra-cms`,
  `hydra-cs`, `hydra-hll` and `hydra-kll` grids have `R = 3`: with
  `β = 4` and `δ_in = 0.05` the bounds hold with probability about
  `0.84 − 0.15 = 0.69`. The canonical `hydra-univmon` grid has `R = 2`,
  `W = 8`: both rows must be good, and `β·M/W = 1.5·N`, so even its form is
  vacuous.

The error is **additive in the global mass** (`ε·G_S`, here `β·M/W`), not
relative to `q`. So a bound is useful only for groups large against
`β·M/W`. For the canonical `W = 128`, `d = 2` (`M = 3N`) and `β = 4`,
`β·M/W ≈ 0.094·N`: a group holding 1% of the records gets a contamination
term about 9× its own size.

### 2.2 Bounds

Derived from the setup above; only Theorem 2 is the paper's. `F_v` is the
fanned-out frequency of value `v` over all subkeys, `ΣD` the sum over all
subkeys of their distinct counts, and `D_q` the distinct count of `q`.

| Combination | Statistic (sketch-bench metric) | Bound in a good row, so for the median | Closed form usable for planning? |
|---|---|---|---|
| `hydra-cms` | frequency of `v` in `q` (top-k ARE/AAE) | `f_q(v) ≤ f̂ ≤ f_q(v) + ε_c·N_q + β·(F_v + ε_c·M)/W`, `ε_c = e/cell_cols`, `δ_in = e^−cell_rows`. One-sided: never under. | Yes, for heavy (`q`, `v`) pairs; vacuous for small `f_q(v)`. |
| `hydra-cs` | same | `f̂ − f_q(v) ∈ [−ε_s·(L2_q + X), ε_s·L2_q + X']` with `ε_s ≈ 1/√cell_cols`, where the colliders' terms `X = L2_C` and `X' = f_C(v) + ε_s·L2_C` are bounded together by one Markov event (`E[F2_C] ≤ F2/W`, `E[f_C(v)] ≤ F_v/W`). Two-sided, biased upward by `f_C(v) ≥ 0`. | Yes, same caveat; not unbiased for `q`. |
| `hydra-hll` | distinct count of `q` (relative error) | `(1 − ε_h)·D_q ≤ D̂ ≤ (1 + ε_h)·(D_q + β·ΣD/W)`, `ε_h ≈ z·1.04/√2^14` (a normal approximation, not a hard bound). Cardinality is a monotone G-sum, so Theorem 2 applies. | Yes. The median removes contamination, not HLL noise (§2.1). |
| `hydra-univmon` cardinality, L1, L2 | `Σ 1[f>0]`, `Σ f`, `√Σ f²` | Theorem 2 directly (L2 via `F2 = Σ f²`: `√` of the bound). | Form only: `ε_US` for concrete heap, layers and widths is not known in closed form (UnivMon's Theorem 1 is asymptotic). Empirical. |
| `hydra-univmon` entropy | `log L1 − (Σ f log f)/L1` | None: a nonlinear combination of two G-sums, and contamination moves both. | No. Empirical only. |
| `hydra-kll` | rank error of `q`'s quantiles (`mean_rank_err`) | `|rank_q(x̂)/N_q − φ| ≤ ε_k + (1 + ε_k)·N_C/N_q`, `N_C ≤ β·M/W`: the cell answers the quantile of a mixture. | No: vacuous unless `N_q ≫ β·M/W`, and the real error depends on how the colliders' values overlap `q`'s. The paper states quantiles cannot be directly estimated by Hydra. Empirical only. |

The KLL bound: the cell's KLL returns `x̂` with
`|rank_cell(x̂) − φ·N_cell| ≤ ε_k·N_cell`, `N_cell = N_q + N_C`, and
`rank_q(x̂) = rank_cell(x̂) − rank_C(x̂)` with `0 ≤ rank_C ≤ N_C`.

So **no Hydra combination has a closed-form guarantee the planner can use
for every group**: CMS, CountSketch and HLL have bounds that are vacuous for
small groups; UnivMon and KLL have none. Admission must be empirical (§2.4),
and these bounds serve as sanity checks on the measurements.

### 2.3 Merges and coarse groupings

- **Across windows and ingest workers.** CMS and CountSketch add counters,
  and HLL takes register maxima; with the shared seeds, a merged grid equals
  one built from all the records, so merged accuracy is the single-grid
  accuracy at the merged mass. KLL cells merge lossily (as `kll-percall`,
  #131) and UnivMon rebuilds heaps from the union of candidates, so both
  need measured merge curves.
- **Coarse groupings.** A grouping inside `Λ` is answered directly, because
  its subkey was inserted; Hydra never rolls up, which is why
  `mergeable_across_groups` is false. The cost is mass: every coarser subset
  inserted adds to `M`, and so to every group's contamination. A grouping
  not inside `Λ` cannot be answered.

### 2.4 The accuracy predicate

Because only empirical accuracy is usable, a Hydra edge is eligible only if
a measurement covers it:

```text
hydra_error(variant, config (R, W, inner), |Λ|, G_r, shape(G_r),
            N_q distribution, M, merged windows) <= the RAQE's SLA
```

in the family's metric and direction, with no borrowing from a
non-Hydra curve or another grouping. One `subpopulations` ceiling (today's
`hydra-kll` eligibility) is not enough: two groupings of equal cardinality
differ in skew and fan-out. Since error is driven by `N_q / (M / W)`, the
measurement should report error by group size, and the SLA must say which
groups it covers (§6).

## 3. Cost model

Hydra is priced by [Cost models](rqe_optimizer_cost_model.md) §3's table,
with no separate formula. One grid is one instance per window, so
`I_d = I_r = 1`, and a query probes it once per `card(G_r)` group. Let
`m_h`, `c_ins,h`, `c_mrg,h` and `c_probe,h(G_r)` be measured for one
variant, configuration and schema width `|Λ|`; `c_ins,h` is per record, fan-out
included (`R · (2^|Λ| − 1)` inner inserts).

| Phase | CPU-seconds per event | Mean vCPUs | Memory (bytes) |
|---|---|---|---|
| Ingest | — | `λ · a_D · c_ins,h` | `m_h · a_D · k_D` |
| Compaction | `(k_D − 1) · c_mrg,h` | `/ y_D` | `k_D · m_h`, while it runs |
| Query job | `card(G_r) · c_probe,h(G_r) + (n_{i,D} − 1) · c_mrg,h` | `/ T_i` | `m_h · [n_{i,D} > 1] + card(G_r) · output bytes`, while it runs |
| Storage | — | 0 | `m_h · ((max_i S_i − x_D) / y_D + 1)` |

plus the key tracker's part. A Hydra merge combines whole grids, not
`card(G_d)` addressable states, which is what `I_d = 1` says. This assumes
the merge cost does not depend on the number of probes; sketch-bench must
confirm it or report a probe-count term.

**Key tracker.** If groups are listed by a DeltaSet (`exact-delta-set`), its
ingest, compaction, merge, query and storage are added as a part. A tracker
at `Λ` that derives coarse keys costs `card(Λ)` per window, not
`card(G_r)`; a tracker per requested grouping costs `card(G_r)` each but
multiplies trackers. This is a decision (§6), not a free operation.

## 4. Candidates and the MILP

The MILP does not change: one binary `z_{i,D}` per eligible edge, one
`u_D` per candidate, `stored_D`, the same constraints and objective
([Cost models](rqe_optimizer_cost_model.md) §7). Hydra adds candidates and
edges with their own eligibility and coefficients.

- **Candidate identity.** `(capability, variant + config, metric, spatial
  filter, Λ, x, y, key tracker)`: the schema `Λ` takes the place of the
  grouping. `Λ` is an ordered list: the library encodes subkeys in its
  declaration order, merges only grids of the same order, and the wrapper's
  prefix queries depend on it. So `Λ` needs its own ordered field (a
  `LabelSet` loses the order), plus a new family property (e.g.
  `answers_any_subgrouping`) that marks Hydra.
- **Generation.** For each (capability, metric, filter), `Λ` ranges over
  every distinct RAQE grouping and their union (more schemas cost more,
  since `M` and `c_ins,h` grow with `|Λ|`). Windows and slides come, as for
  roll-ups, from the RAQEs whose grouping is a non-empty subset of `Λ`.
- **Edge `(i, D)`.** Same capability, metric and filter; `∅ ≠ G_i ⊆ Λ`
  (a prefix of `Λ` until the wrapper queries any subset); window alignment
  as today; and the §2.4 predicate measured and passing.
- **Linearity.** An edge's accuracy depends only on the candidate (`Λ`,
  config, window) and the metric's mass, never on which other RAQEs share
  it: everything on the metric and filter is inserted whatever is selected.
  So eligibility stays a per-edge filter and the MILP stays linear.
- **Coefficients.** Per candidate: ingest, compaction, storage (§3, plus the
  tracker). Per edge: the query job, with `c_probe,h(G_i)`. A Hydra edge is
  never priced or validated as a roll-up.
- **Pruning.** Unchanged: candidates compare only at the same capability,
  metric, filter and grouping (here `Λ`), on the costs listed in
  [candidates](rqe_optimizer_candidates.md).
- **Competition.** A quantile RAQE by `(service)` can then be served by KLL
  at `(service)`, by KLL at `(service, endpoint)` through a roll-up, or by
  Hydra with `Λ ⊇ {service}`; the MILP picks the cheapest that passes.

Code this needs, in order: `FamilyProperties` for the four other variants;
the new property and the Hydra branch in `is_eligible` and
`build_all_candidates_unpruned`; Hydra in `Capability::families` and
`DEPLOYABLE_FAMILIES`; the saturation lookup keyed as in §2.4; cost-table
rows. AutoSketch keeps skipping Hydra (#159).

## 5. What sketch-bench must add

Each item names what is missing today. Every Hydra measurement sketch-bench
has today predates these requirements, so admitting Hydra means rerunning
them all; `hydra-kll` is measured in that same run, alongside `hydra-cms`,
`hydra-cs`, `hydra-hll` and `hydra-univmon`, under every item below.

1. **Target grouping.** Comparators score only column 0
   (`aqpbm-cli/src/rows/mod.rs`); they must score any named subset of the
   schema, and the wrapper must query any subset, not only prefixes
   (`hydra_shared.rs` `query`).
2. **Exact baseline.** The `polars` baselines build untagged subkeys
   (`polars_shared.rs`), so overlapping label values alias there but not in
   the 0.3.0 sketch (#74); tag them the same way.
3. **Data.** Hierarchical labels with set cardinality per label, per-parent
   fan-out and skew; the result records `|Λ|`, the target grouping's
   cardinality, every group's `N_q`, and the fanned-out mass `M`.
4. **Sweep.** Grid (`R`, `W`), inner parameters, schema width, target
   grouping, cardinality and fan-out, distribution. Report error per group
   against `N_q / (M / W)`, and quantiles over groups, not only the mean.
   The column hash has a fixed seed, so vary the label values (not only the
   data seed) to see the spread over collision patterns.
5. **Bounds as checks.** For CMS, CountSketch and HLL, assert the measured
   per-group error is within §2.2's bound at its confidence; a violation
   means a bug.
6. **Merges.** Accuracy and cost of merged grids at the shard counts
   workloads need, with records split by sample (interleaved), not only in
   contiguous chunks (`sketch-bench/src/wrappers/mod.rs`); exactness checks
   for CMS, CountSketch and HLL; merge curves for KLL and UnivMon cells.
7. **Cost table.** Hydra rows in `study_saturation.py` (`SKETCHES`,
   `OPTIMIZER_FAMILIES`) and `approxbench atomic-costs`: `m_h` (including
   each KLL cell's merge buffer, #191), `c_ins,h` per record at the schema
   width, `c_mrg,h`, and `c_probe,h` per probe at the target grouping.
8. **Key enumeration.** Benchmark the chosen tracker (§6) at its grouping,
   with its input and output cardinality.
9. **Saturation key.** Emit §2.4's fields machine-readably. The optimizer
   rejects a Hydra edge with no matching point unless an interpolation rule
   is approved (§6).
10. **Fixtures.** Equal grouping, a direct coarse subset, and a multi-window
    merge, each against exact answers, asserting the exported accuracy and
    cost fields.

## 6. Decisions needed

1. Group enumeration: one DeltaSet at `Λ` that derives coarse keys, or one
   per requested grouping?
2. Groupings: prefixes of `Λ` only, or any non-empty subset once the wrapper
   and comparators support it?
3. Which groups an SLA covers: every group (then Hydra almost never passes
   for small groups), groups above a size, or a quantile over groups?
4. Interpolation between measured Hydra points: none, or the worse
   bracketing point as for the other sketches?
5. Schemas generated per workload: each RAQE grouping and their union, or
   more?
6. Whether `hydra-univmon` entropy, which has no bound, is admitted at all.

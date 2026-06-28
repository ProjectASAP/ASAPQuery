# ASAP Optimizer: V1 Implementation Plan

**Branch**: `405-feat-optimization-based-sketch-config-selection`
**GitHub Issue**: #405
**Design doc** (read first): `.design_docs/sketch-config-optimization-formulation.md`
**Module**: `asap-planner-rs/src/optimizer/`

---

## Background

`asap-planner-rs` currently hardcodes sketch/config decisions. This module replaces that
with an optimization that selects, for each atomic query expression (AQE), the best streaming
config to deploy (or falls back to exact/raw computation if no sketch is cost-effective).

The optimizer output type is `OptimizerSolution`, which is then translated into the existing
`(StreamingConfig, InferenceConfig)` deployment artifacts.

---

## Key Vocabulary

- **RQE** (`Rqe`): a repeating query — one PromQL string + its repetition interval T_r.
- **AQE** (`Aqe`): a deduplicated atomic leaf aggregation extracted from RQEs. Has:
  - `requirements: QueryRequirements` — metric, statistic, range, labels, filter
  - `query_frequency_hz: f64` — Σ 1/T_r (MIP objective weight)
  - `min_t_repeat_secs: u64` — min(T_r), bounds W ≤ min_t (freshness)
  - `t_repeat_gcd_secs: u64` — GCD(T_r), natural slide interval S
- **g** (`CandidateConfig`): a candidate streaming config — agg type, params, window W, slide S, retention depth.
- **EXACT_a fallback**: always feasible; means raw Prometheus query at query time; zero ingest cost.

### Three algebraic sketch properties (per `AggregationType`)

| Property | Meaning |
|---|---|
| `mergeable` | Two instances can be combined → supports Merge query method |
| `subtractable` | Element-wise difference defined → supports Subtract (tumbling only) |
| `subpopulation_aware` | One instance handles multiple label-group keys → N(s,g)=1 |

### Query method (not a free variable — derived from ingest_type × W vs range_a × algebra)

| Ingest | W vs range_a | Algebra | Method |
|---|---|---|---|
| Any | W = range_a | any | Neither |
| Tumbling | W < range_a | mergeable | Merge{n=range_a/W} |
| Tumbling | W < range_a | subtractable | Subtract |
| Tumbling | W < range_a | neither | infeasible |
| Sliding | W < range_a | any | infeasible |

### Window constraints

- `W ≤ range_a` (no over-coverage)
- `W ≤ min_t_repeat_secs` (freshness — fastest dashboard is binding)
- Sliding forces W = range_a exactly

### Cost model (objective)

```
minimize  Σ_g y_g · IngestCost(g)  +  Σ_{a,g} x_{a,g} · f_a · QueryCost(a,g)
s.t.      Σ_g x_{a,g} = 1   for all a        (every AQE assigned)
          x_{a,g} ≤ y_g      for all a, g     (only deploy what is used)
          x_{a,g}, y_g ∈ {0,1}
```

`IngestCost(g)` must be independent of which AQEs use g (facility-location property).
`f_a = Σ 1/T_r` (sum, not GCD) — each dashboard independently queries the sketch.

---

## File Map

```
asap-planner-rs/src/optimizer/
├── mod.rs                 re-exports
├── solution.rs            Aqe, QueryMethod, AqeAssignment, OptimizerSolution
├── translator.rs          translate(&OptimizerSolution) → (StreamingConfig, InferenceConfig)
├── aqe_extractor.rs       Rqe, extract_aqes()
├── pipeline.rs            run_all_exact_pipeline(), run_greedy_pipeline()
├── sketch_properties.rs   algebraic properties per AggregationType    [Phase 2a, done]
├── candidate_gen.rs       enumerate candidate configs per AQE         [Phase 2b, done]
├── cost_model.rs          ingest/query cost formulas                  [Phase 2c, done]
├── greedy.rs              per-AQE greedy assignment                   [Phase 2e, done]
│
│  ── TO BE ADDED ──
├── feasibility.rs          Feasible(a,g) predicate — only meaningful once configs
│                            are shared across AQEs; skipped in Phase 2 since every
│                            candidate is already self-consistent by construction [Phase 3]
└── mip_solver.rs           facility-location MIP (good_lp + coin_cbc)            [Phase 3]
```

Existing infrastructure to reuse (do not re-implement):
- `asap_types::capability_matching::{compatible_agg_types, window_compatible, spatial_filter_compatible, topk_weighting_compatible, labels_compatible}` (PR #259)
- `asap_types::streaming_config::StreamingConfig`, `::inference_config::InferenceConfig`, `::aggregation_config::AggregationConfig`
- `asap_planner::planner::patterns::build_patterns()` — 5 PromQL pattern types
- `promql_utilities::query_logics::parsing::{get_metric_and_spatial_filter, get_statistics_to_compute, get_spatial_aggregation_output_labels}`

---

## Phase Status

### ✅ Phase 1 — Scaffolding (3 commits, all tests pass)

**Commits**: `89d8bd7`, `4965ebf`, `9269ce7`

What exists:
- `solution.rs`: `Aqe`, `QueryMethod`, `AqeAssignment`, `OptimizerSolution` (with `all_exact()` constructor)
- `translator.rs`: `translate()` — Phase 1 stub, emits empty `InferenceConfig` (TODO Phase 2)
- `aqe_extractor.rs`: `Rqe`, `extract_aqes()` with GCD/min/sum frequency tracking; `decompose_to_leaves()` splits binary arithmetic; `AqeKey` for deduplication
- `pipeline.rs`: `run_all_exact_pipeline(config, schema) → (StreamingConfig, InferenceConfig)`
  - Flow: `ControllerConfig → config_to_rqes() → extract_aqes() → OptimizerSolution::all_exact() → translate()`

**Result**: 98 existing + 7 new optimizer tests pass. All AQEs fall back to EXACT; no configs deployed.

---

### ✅ Phase 2 — Per-AQE Greedy Sketch Selection (4 commits, all tests pass)

Each AQE gets its own best config independently (no cross-AQE sharing). MIP sharing is Phase 3.

**Commits**: `6899787` (2a+2b), `0d59c18` (2c), `cb2b9dc` (2e + pipeline/translator wiring).
2d (`feasibility.rs`) was deliberately skipped — see note below.

#### 2a — `sketch_properties.rs` (done)

```rust
pub struct SketchProperties { pub mergeable: bool, pub subtractable: bool, pub subpopulation_aware: bool }
pub fn sketch_properties(t: AggregationType) -> SketchProperties
```

Implemented values — `CountMinSketchWithHeap` ended up `mergeable=false` (not `true`/unclear as
originally guessed): the heap top-k list doesn't compose across merged/subtracted windows even
though the underlying CMS cells would. Everything else matches the original guess:
- `CountMinSketch`: mergeable=T, subtractable=T, subpopulation_aware=T
- `CountMinSketchWithHeap`: mergeable=F, subtractable=F, subpopulation_aware=T
- `DatasketchesKLL`, `HydraKLL`: mergeable=T, subtractable=F, subpopulation_aware=F (true for HydraKLL too)
- `Sum`/`MultipleSum`: mergeable=T, subtractable=T, subpopulation_aware=F/T respectively
- `HLL`, `SetAggregator`, `DeltaSetAggregator`: mergeable=T, subtractable=F, subpopulation_aware=F
- `MinMax`/`MultipleMinMax`, `Increase`/`MultipleIncrease`: mergeable=T, subtractable=F, subpopulation_aware=F/T
- `SingleSubpopulation`/`MultipleSubpopulation` (legacy wrapper types): all false (unknown, treated conservatively)

When a type is both `mergeable` and `subtractable` (e.g. `Sum`, `CountMinSketch`),
`candidate_gen.rs` prefers `Subtract` — it's O(1) regardless of `n`, strictly cheaper than `Merge`'s O(n).

#### 2b — `candidate_gen.rs` (done)

```rust
pub struct CandidateConfig {
    pub config: Option<AggregationConfig>,  // None = EXACT fallback
    pub query_method: QueryMethod,
    pub n_windows: u64,
}
pub fn enumerate_candidates(aqe: &Aqe, scrape_interval_secs: u64) -> Vec<CandidateConfig>
```

Enumeration: `compatible_agg_types(stat)` × param grid (hardcoded small grids per sketch type,
see `CMS_DEPTHS`/`CMS_WIDTHS`/etc. constants — replace with sketch-bench sweep results in Phase 3)
× window candidates × ingest type:
- **Tumbling**: all `W` that divide `range_a`, are multiples of `scrape_interval_secs`, and
  `≤ min(range_a, min_t_repeat_secs)`. Slide interval = `W` (tumbles by its own width).
- **Sliding**: `W = range_a` exactly is forced (overlapping sliding windows can't merge/subtract
  to cover a larger range — only mutually-exclusive tumbling windows compose). But the slide
  interval `S` is still a free choice ≤ `W`: smaller `S` means more concurrent overlapping
  sub-windows maintained internally (`⌈W/S⌉`), costing more ingest CPU/mem for fresher results.
  `S` doubles from `scrape_interval_secs` up to `W` to cover that tradeoff cheaply.
- Multi-statistic AQEs (e.g. `avg` = `[Sum, Count]`) return only the EXACT candidate — a single
  sketch family can't serve two incompatible statistics in v1.
- Always appends an EXACT candidate (no config, `query_method = Exact`) — always feasible.

Label granularity: only proposes configs at the label granularity the AQE itself needs
(reactive, no superset matching — that's Phase 3b).

#### 2c — `cost_model.rs` (done)

```rust
pub struct AtomicCosts { mem_bytes_per_instance, insert_cpu_secs, merge_cpu_secs, subtract_cpu_secs,
                          query_cpu_secs, exact_query_cpu_secs: f64 }
pub struct CostWeights { ingest_mem, ingest_cpu, query_mem, query_cpu: f64 }
pub fn ingest_cost(candidate: &CandidateConfig, rho_g: f64, costs: &AtomicCosts, weights: &CostWeights) -> f64
pub fn query_cost(a: &Aqe, candidate: &CandidateConfig, costs: &AtomicCosts, weights: &CostWeights) -> f64
pub fn total_cost_rate(a: &Aqe, candidate: &CandidateConfig, rho_g: f64, costs: &AtomicCosts, weights: &CostWeights) -> f64
```

Implements the design doc's formulas with `N(s,g) = 1` hardcoded everywhere (real `N_g` —
distinct label-group count — needs Prometheus series-count profiling, not wired up; Phase 3).
`AtomicCosts` added `exact_query_cpu_secs` (not in the original plan) — without a non-zero cost
for the EXACT fallback's query, `IngestCost=0, QueryCost=0` would make EXACT always win trivially.

**Important finding**: `CostWeights::default()` originally weighted memory (bytes) and CPU
(seconds) equally (`1.0` each) — but those aren't comparable units, and combining them at parity
made EXACT win regardless of workload (a sketch's flat memory-deployment cost always dwarfed any
per-query savings). Fixed by scaling memory weights down to `1e-9` relative to CPU weights, loosely
reflecting that RAM-held-over-time costs ~1e6x less per unit than CPU-time in real cloud pricing
(~$5/GB-month vs ~$0.04/vCPU-hour). This is still a stub, not real calibration — but it's at least
self-consistent enough that sketches can plausibly win when the workload calls for it.

#### 2d — `feasibility.rs` — **skipped, deliberately**

Originally planned as a `Feasible(a,g)` predicate wrapping `capability_matching.rs`. Not built:
every candidate `candidate_gen.rs` produces is already constructed *from* that exact AQE's own
labels/spatial-filter/metric/window constraints, so a feasibility check would always return `true`
at this phase — unexercised scaffolding. It becomes meaningful in **Phase 3**, once configs are
shared across AQEs (a config built for AQE X needs checking against AQE Y's requirements). Build
it then, reusing `window_compatible`, `spatial_filter_compatible`, `topk_weighting_compatible`,
`labels_compatible` from `asap_types::capability_matching` as originally planned.

#### 2e — `greedy.rs` + `pipeline.rs` + `translator.rs` (done)

```rust
pub fn greedy_assign(aqes: Vec<Aqe>, scrape_interval_secs: u64, rho_g: f64,
                      costs: &AtomicCosts, weights: &CostWeights) -> OptimizerSolution
```

For each AQE independently: `argmin` over `enumerate_candidates(aqe, ...)` by `total_cost_rate`.
No feasibility filter needed (see 2d note) — every candidate from `enumerate_candidates` is valid
for that AQE by construction. Assigns sequential `aggregation_id`s to deployed configs.

`run_greedy_pipeline(config, schema, scrape_interval_secs, rho_g) -> (StreamingConfig, InferenceConfig)`
added to `pipeline.rs` alongside `run_all_exact_pipeline()`. `rho_g` is currently a single
placeholder value applied uniformly to every candidate — see open TODO below.

`translator.rs::build_inference_config()` now populates `query_configs`: for each assignment with
a real `aggregation_id`, emits one `QueryConfig` per original query string, with
`num_aggregates_to_retain` from `retention_count_for_assignment()`.

---

### 🔲 Phase 3 — Full MIP with Cross-AQE Sharing

#### 3a — `mip_solver.rs`

Add `good_lp` + `coin_cbc` to `Cargo.toml`. Implement facility-location MIP above.
Add `run_mip_pipeline()` to `pipeline.rs`.

#### 3b — Label superset matching

Relax `labels_compatible()` in `asap_types::capability_matching` at line 86 (TODO comment already there).
Allow a config with labels ⊇ query labels to serve that AQE. This is what enables cross-AQE sharing.

#### 3c — sketch-bench cardinality sweep

- Add cardinality to sweep grids in `sketch-bench`
- Add `CountMinSketchWithHeap` wrapper in `sketch-cli/src/wrappers/`
- Plug real `AtomicCosts` values into cost model

#### 3d — Accuracy constraint

Implement `Error(a,g,θ_a) ≤ ε_a` in `is_feasible()`:
- CMS: use analytic ε-δ guarantee
- Others: empirical lookup from sketch-bench
- Add θ_a profiling step (Prometheus series count API) before optimizer runs

---

## Offline Testing — Not Wired Into the Real Planner

**The optimizer is not used by `Controller::generate()`.** That method calls
`generator::generate_plan()` (the existing hardcoded path in `promql/generator.rs`)
unconditionally — confirmed by grepping for callers of `run_greedy_pipeline`/
`run_all_exact_pipeline` outside the `optimizer` module: there are none. Running
`asap-planner` (the real CLI) today is completely unaffected by anything in this module.

To test the optimizer against real workload YAMLs before wiring it in, use the
standalone `asap-optimizer-cli` binary (`asap-planner-rs/src/bin/optimizer_cli.rs`):

```
cargo run -p asap_planner --bin asap-optimizer-cli -- \
  --input_config <path/to/workload.yaml> \
  --prometheus_scrape_interval 60 \
  [--rho 1.0]
```

Takes the same `ControllerConfig` YAML format as `asap-planner --input_config`
(with a `metrics:` hints block for label schema — no live Prometheus needed).
Prints deployed streaming configs and query configs to stdout. `--rho` is the
placeholder arrival rate (see TODOs below — not real yet).

Wire-in decision (deferred): once Phase 3 (MIP + feasibility + label superset
matching) lands, swap `Controller::generate()` to call `run_mip_pipeline()`
instead of `generator::generate_plan()`, likely behind an opt-in flag first.

---

## Open TODOs in Code

| File | Location | TODO |
|---|---|---|
| `aqe_extractor.rs` | `extract_requirements()` | Duplicates `build_query_requirements_promql` in `asap-query-engine/src/engines/simple_engine/promql.rs:614` — extract to shared free fn in `asap_types::query_requirements` |
| `capability_matching.rs` | `labels_compatible()`, line 86 | Relax to superset matching — do in Phase 3b |
| `cost_model.rs` | `ingest_cost()`, `query_cost()` | `N(s,g)` hardcoded to `1` everywhere; real `N_g` (distinct label-group count) needs Prometheus series-count profiling |
| `cost_model.rs` | `CostWeights::default()` | Self-consistent stub ratio (mem:cpu ≈ 1e-9:1), not real $/byte-sec vs $/cpu-sec calibration |
| `greedy.rs`, `pipeline.rs` | `rho_g` parameter | Single placeholder value applied uniformly; real per-config rates need Prometheus scrape-rate × active-series-count, not wired up |
| `translator.rs` | `retention_count_for_assignment(Subtract)` | Returns hardcoded `1`; should be the actual checkpoint count needed to cover the full lookback |
| `promql/generator.rs` | `generate_plan()` doc comment | Flags that `Controller::generate()` still uses the hardcoded path, not the optimizer — see "Offline Testing" section above |
| — | Accuracy constraint | No `Error(a,g) ≤ ε_a` check exists anywhere; nothing stops picking an under-provisioned sketch (Phase 3d) |
| — | sketch-bench | No cardinality sweep, no `CountMinSketchWithHeap` wrapper; `AtomicCosts` are still stub numbers (Phase 3c) |

---

## Out of Scope for V1

- Hard cluster budget constraints (Σ Mem ≤ M_total)
- Workload drift / re-planning (static one-shot only)
- Per-AQE weight vectors (global w₁..w₄ only)
- Binary-op evaluation cost
- OneTemporalOneSpatial pipeline decomposition
- `ρ_g` as a new profiling mechanism (assume available or use placeholder)

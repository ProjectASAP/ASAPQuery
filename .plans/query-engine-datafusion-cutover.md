# Query Engine DataFusion Cutover Plan

## Background

`simple_engine.rs` was originally a bespoke implementation for a fixed set of pre-registered
PromQL queries. The goal is to expand query coverage (more PromQL patterns + SQL) by cutting
over to the existing DataFusion execution path, and fixing the root cause that blocks coverage:
exact query-string matching in `find_query_config()`.

### What already exists and is NOT changing
- DataFusion physical operators: `PrecomputedSummaryReadExec`, `SummaryMergeMultipleExec`,
  `SummaryInferExec` — fully implemented
- `execute_plan()` on `SimpleEngine` — implemented but currently `#[allow(dead_code)]`
- `QueryExecutionContext::to_logical_plan()` — implemented

### Root cause of limited coverage
```rust
fn find_query_config(&self, query: &str) -> Option<&QueryConfig> {
    self.inference_config.query_configs.iter()
        .find(|config| config.query == query)  // exact string match
}
```
Every query must be pre-registered verbatim. Any whitespace difference, any parameterization,
any new query shape → miss. This blocks everything.

### Key design decisions
- **`QueryIntent`** is the unified intermediate between query parsing and config lookup.
  It captures what the query is asking (metric, operation, window, grouping) before any
  config resolution. It replaces `PromQLMatchResult` (PromQL path) and `SQLQuery` (SQL path).
- **Config lookup** changes from string matching to intent matching:
  `(metric, temporal_op ∈ supports_temporal, window_ms % tumbling_window_ms == 0)`
- **`QueryExecutionContext`** is retained — it is the post-config-lookup structure holding
  resolved aggregation IDs, computed timestamps, and the store plan.
- **Convergence point** for PromQL and SQL is the `LogicalPlan`, not the string.
  Both parsers produce `QueryIntent`; one plan builder turns `QueryIntent` → `LogicalPlan`.
- Range queries are out of scope for this plan (handled separately).

---

## PR 1 — Define `QueryIntent` and AST extraction

**Objective:** Introduce the unified query intermediate representation with extraction
functions for both PromQL and SQL; no behavior changes.

### Engine — new file `src/engines/query_intent.rs`

- `QueryIntent` struct:
  ```rust
  pub struct QueryIntent {
      pub metric: String,
      pub label_filters: HashMap<String, String>,
      pub temporal_op: Option<TemporalOp>,
      pub window_ms: Option<u64>,
      pub spatial_op: Option<SpatialOp>,
      pub output_labels: Option<Vec<String>>,
      pub params: QueryParams,
      pub at_modifier: Option<u64>,
  }
  ```
- `TemporalOp` enum: `Sum, Count, Avg, Min, Max, Rate, Increase, Quantile, Entropy,
  Distinct, L1, L2, StdDev, StdVar, Sum2`
- `SpatialOp` enum: `Sum, Count, Avg, Min, Max, Quantile, Topk`
- `QueryParams` struct: `quantile: Option<f64>`, `k: Option<usize>`
- `extract_intent_from_promql(query: &str) -> Result<QueryIntent, IntentError>`
  — direct walk of `promql_parser` AST; no pattern objects
- `extract_intent_from_sql(query: &str) -> Result<QueryIntent, IntentError>`
  — walk of `sqlparser` AST
- Unit tests covering each query shape (OnlyTemporal, OnlySpatial, OneTemporalOneSpatial)
  for both languages

### Engine — `src/engines/mod.rs`
- Add `pub mod query_intent`

Nothing in `simple_engine.rs` calls these yet. No behavior change.

---

## PR 2 — New config format (planner output + engine parsing + new lookup)

**Objective:** Replace the query-string-keyed inference config with a metric-keyed
aggregation capability format; engine can parse and look up configs by intent.

### New inference_config YAML contract

```yaml
metrics:
  - name: http_requests_total
    labels: [service, endpoint, method]
    aggregation_groups:
      - values_aggregation_id: 2
        keys_aggregation_id: 1      # optional, only for CMS/HydraKLL
        tumbling_window_seconds: 300
        window_type: tumbling       # or: sliding
        supports_temporal: [sum, count, avg]
        supports_spatial: [sum, count, avg, min, max]
        grouping_labels: [service, endpoint]
        aggregated_labels: []
```

`keys_aggregation_id` is present only when a `DeltaSetAggregator` companion exists
(i.e., for CountMinSketch and HydraKLL queries). This replaces the current two-element
`aggregations` list that `get_aggregation_id_info()` unpacks by inspecting streaming
config types.

### Planner — `main_controller.py`

**File:** `asap-planner/main_controller.py`
**Change:** Replace the `inference_config["queries"]` output block (lines 112–137).
Currently iterates `query_aggregation_config_keys_map` (query string → aggregation IDs)
and emits query strings. Replace with iteration over
`streaming_aggregation_configs_map` grouped by metric, emitting capability entries.

**New file:** `asap-planner/utils/accumulator_capabilities.py`

```python
ACCUMULATOR_SUPPORTS = {
    "SumAccumulator":      {"temporal": ["sum", "count", "avg"],
                            "spatial":  ["sum", "count", "avg", "min", "max"]},
    "IncreaseAccumulator": {"temporal": ["increase", "rate"],
                            "spatial":  ["sum"]},
    "MinMaxAccumulator":   {"temporal": ["min", "max"],
                            "spatial":  ["min", "max"]},
    "DatasketchesKLL":     {"temporal": ["quantile"],
                            "spatial":  ["quantile"]},
    "HydraKLL":            {"temporal": ["quantile"],
                            "spatial":  ["quantile"]},
    "CountMinSketch":      {"temporal": ["distinct", "cardinality"],
                            "spatial":  ["topk"]},
    "DeltaSetAggregator":  {},  # keys-only, never stands alone
}
```

> **Rust port note:**
> - The block being replaced in `main_controller.py` is lines 112–137.
> - The source of `aggregationType` (needed to key into `ACCUMULATOR_SUPPORTS`) is
>   `SingleQueryConfig.get_streaming_aggregation_configs()` line 494:
>   `config.aggregationType = aggregation_type`.
> - The `keys_aggregation_id` pairing comes from lines 505–524 in
>   `SingleQueryConfig.get_streaming_aggregation_configs()` — the block that adds
>   a `DeltaSetAggregator` companion config for CMS/HydraKLL.
> - `accumulator_capabilities.py` should be ported as a standalone static table
>   in the Rust planner.

### Engine — `sketch_db_common` crate

Update `InferenceConfig` and related structs + YAML deserialization:

- Add `AggregationGroupConfig`:
  ```rust
  pub struct AggregationGroupConfig {
      pub values_aggregation_id: u64,
      pub keys_aggregation_id: Option<u64>,
      pub tumbling_window_seconds: u64,
      pub window_type: String,
      pub supports_temporal: Vec<String>,
      pub supports_spatial: Vec<String>,
      pub grouping_labels: KeyByLabelNames,
      pub aggregated_labels: KeyByLabelNames,
  }
  ```
- Update `InferenceConfig` to contain `metrics: Vec<MetricInferenceConfig>` where each
  entry has `name`, `labels`, and `aggregation_groups: Vec<AggregationGroupConfig>`

### Engine — `src/engines/simple_engine.rs`

- Add `find_config_by_intent(&self, intent: &QueryIntent) -> Option<&AggregationGroupConfig>`
  with matching rule:
  ```
  config.metric == intent.metric
  AND (intent.temporal_op is None OR intent.temporal_op.as_str() ∈ config.supports_temporal)
  AND (intent.spatial_op is None  OR intent.spatial_op.as_str() ∈ config.supports_spatial)
  AND intent.window_ms.map_or(true, |w| w % (config.tumbling_window_seconds * 1000) == 0)
  ```
- Keep `find_query_config()` and old `build_query_execution_context_*` methods alive —
  nothing switched over yet

---

## PR 3 — Unified context builder

**Objective:** Replace the two language-specific `build_query_execution_context_*` methods
with a single builder fed by `QueryIntent`; old execution path still active.

### Engine — `src/engines/simple_engine.rs`

- Add `build_execution_context(&self, intent: QueryIntent, query_time: u64) -> Option<QueryExecutionContext>`:
  - Calls `find_config_by_intent(&intent)` (from PR 2)
  - `get_aggregation_id_info()` now reads directly from
    `AggregationGroupConfig.values/keys_aggregation_id` instead of inspecting
    streaming config types
  - Computes timestamps from `intent.window_ms` instead of re-extracting from match result
  - Assembles `QueryExecutionContext` identically to today

- Replace bodies of `handle_query_promql()` and `handle_query_sql()`:
  ```
  extract_intent_from_*(query) → build_execution_context() → execute_query_pipeline()
  ```

- Delete `build_query_execution_context_promql()` and `build_query_execution_context_sql()`

At this point `find_query_config()`, `PromQLPattern`, `SQLPatternMatcher` etc. are dead
code but not yet deleted.

---

## PR 4 — DataFusion execution path

**Objective:** Make `execute_plan()` the primary path and delete the bespoke execution
pipeline.

### Engine — `src/engines/simple_engine.rs`

- Remove `#[allow(dead_code)]` from `execute_plan()`
- In `handle_query_promql()` and `handle_query_sql()`: replace `execute_query_pipeline()`
  with `execute_plan()`. Requires either:
  - Making handle methods `async` and propagating up to HTTP handlers, or
  - Using `tokio::task::block_in_place` (commented example already exists at line 1714)
- Delete all bespoke execution methods:
  - `execute_query_pipeline()`
  - `execute_and_merge_store_queries()`
  - `merge_precomputed_outputs()`
  - `merge_accumulators()`
  - `collect_all_results()`
  - `collect_results_separate_keys()`
  - `collect_results_same_aggregation()`
  - `query_precompute_for_statistic()`
  - `limit_keys_for_topk()`
  - `format_final_results()`

`handle_range_query_promql()` and `execute_range_query_pipeline()` are left unchanged
(range queries are out of scope).

---

## PR 5 — Remove dead infrastructure

**Objective:** Delete the old pattern-matching and string-based config lookup code that
is now unreachable.

### Engine — `src/engines/simple_engine.rs`

- Remove `controller_patterns: HashMap<QueryPatternType, Vec<PromQLPattern>>` field and
  its entire initialization block in `new()` (currently ~lines 159–278)
- Remove `find_query_config()`
- Remove all imports of `PromQLPattern`, `PromQLPatternBuilder`, `PromQLMatchResult`,
  `QueryPatternType`
- Remove all imports of `SQLPatternMatcher`, `SQLPatternParser`, `SQLQuery`, `QueryType`

### Crates `promql_utilities` and `sql_utilities`
(confirmed consumed only by `asap-query-engine`)

- Delete `ast_matching/` modules: `PromQLPattern`, `PromQLPatternBuilder`,
  `PromQLMatchResult`, `TokenData` and all sub-token structs (`MetricToken`,
  `FunctionToken`, `AggregationToken`, `RangeToken`, `NumberToken`, etc.)
- Delete `sql_utilities::ast_matching`: `SQLPatternMatcher`, `SQLPatternParser`,
  `SQLQuery`, `QueryType`
- Review and remove `get_metric_and_spatial_filter`, `get_statistics_to_compute`,
  `get_spatial_aggregation_output_labels`, `get_is_collapsable` if no longer referenced

> **Rust port note:**
> The Python equivalents of what is deleted here are:
> - `promql_utilities/ast_matching/PromQLPattern.py`
> - `promql_utilities/ast_matching/PromQLPatternBuilder.py`
> - The `patterns` dict in `SingleQueryConfig.__init__()` (lines 58–176)
>
> When the planner is ported to Rust, these should be replaced by the same
> `extract_intent_from_promql()` function introduced in PR 1 — do not port
> the pattern matching machinery.

---

## Summary table

| PR | Scope | Behavior change |
|----|-------|-----------------|
| 1 | Engine: `QueryIntent` + extraction functions | None |
| 2 | Planner: new inference_config format; Engine: new config structs + intent-based lookup | Config format (breaking); new lookup added alongside old |
| 3 | Engine: unified `build_execution_context()` | Old execution path, new builder |
| 4 | Engine: DataFusion as primary execution path | **Switchover** |
| 5 | Engine + crates: delete dead pattern matching + bespoke pipeline code | None (cleanup) |

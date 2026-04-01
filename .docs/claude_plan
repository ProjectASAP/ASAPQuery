# Capability-Based Aggregation Matching

## Context

Queries currently route to an aggregation ID by exact/structural string match against a pre-configured
`query_configs` table in `InferenceConfig`. This means every distinct query string (including
`quantile(0.5, ...)` vs `quantile(0.9, ...)`) requires its own config entry, even when the same
sketch can serve both. The goal is to add a capability-matching fallback: extract what a query
*needs* (metric, statistics, data range, labels, spatial filter) and find an `AggregationConfig`
in `StreamingConfig` that can satisfy it. The old `query_configs` path remains as the primary
route; capability matching fires only when no pre-configured entry is found.

---

## Key Design Decisions (already settled)

- **Fallback order**: `query_configs` lookup first; capability matching only when no entry found
- **Multiple statistics** (e.g. `avg` = `[Sum, Count]`): `Vec<Statistic>`; all must be satisfied by
  configs with identical `window_size`, `grouping_labels`, and `spatial_filter`
- **Window**: store `data_range_ms: Option<u64>` (the range the query needs); both tumbling and
  sliding configs can satisfy it — see compatibility rules below
- **Priority**: largest `window_size` wins among compatible candidates (encapsulated in its own
  function so it is swappable)
- **Label compatibility**: strict exact match for now (add TODO for superset support)
- **Spatial filter**: if a config has a non-empty filter and the query's normalized filter differs,
  reject
- **Sub-type**: must match (e.g. `Min` needs `aggregation_sub_type == "min"`)
- **Multi-population**: after finding the value aggregation, if its type is multi-population, also
  find a key aggregation (`SetAggregator` or `DeltaSetAggregator`) on the same metric
- **`QueryRequirements` + capability logic**: lives in `sketch_db_common`
- **Fallback warning**: log `warn!` when capability matching is used (query not in `query_configs`)

---

## Codebase Facts

- `Statistic` enum: `promql_utilities::query_logics::enums` — already imported by `simple_engine.rs`
  as `use promql_utilities::query_logics::enums::{QueryPatternType, Statistic};`
- `AggregationIdInfo`: defined in `asap-query-engine/src/engines/simple_engine.rs:43` — needs to
  move to `sketch_db_common`
- `sketch_db_common` already depends on `promql_utilities` (uses `KeyByLabelNames`)
- `StreamingConfig`: in `asap-query-engine/src/data_model/streaming_config.rs` — stays there, gets
  a new `find_compatible_aggregation` wrapper method
- `normalize_spatial_filter`: already in `sketch_db_common::utils`
- `AggregationConfig.aggregation_type` string values: `"Sum"`, `"MultipleSumAccumulator"`,
  `"MinMax"`, `"MultipleMinMaxAccumulator"`, `"Increase"`, `"MultipleIncreaseAccumulator"`,
  `"CountMinSketch"`, `"CountMinSketchWithHeap"` / `"CountMinSketchWithHeapAccumulator"`,
  `"DatasketchesKLL"`, `"HydraKLL"`, `"SetAggregator"`, `"DeltaSetAggregator"`
- Multi-population value types (require a key aggregation):
  `MultipleSumAccumulator`, `MultipleMinMaxAccumulator`, `MultipleIncreaseAccumulator`,
  `CountMinSketchWithHeap`, `CountMinSketchWithHeapAccumulator`
- Key aggregation types: `SetAggregator`, `DeltaSetAggregator`
- Test utilities: `asap-query-engine/src/tests/test_utilities/config_builders.rs` has
  `TestConfigBuilder` — reuse for integration tests

---

## Implementation Steps (TDD order)

### Step 1 — Move `AggregationIdInfo` to `sketch_db_common`

**File**: `asap-common/dependencies/rs/sketch_db_common/src/aggregation_config.rs`

Move the struct definition here (it has no engine-specific deps):

```rust
#[derive(Debug, Clone)]
pub struct AggregationIdInfo {
    pub aggregation_id_for_key: u64,
    pub aggregation_id_for_value: u64,
    pub aggregation_type_for_key: String,
    pub aggregation_type_for_value: String,
}
```

In `sketch_db_common/src/lib.rs`, ensure it is re-exported via `pub use aggregation_config::*`.

In `asap-query-engine/src/engines/simple_engine.rs`, remove the local definition and add:
```rust
use sketch_db_common::AggregationIdInfo;
```
Verify nothing else breaks (`cargo check`).

---

### Step 2 — Add `QueryRequirements` to `sketch_db_common`

**New file**: `asap-common/dependencies/rs/sketch_db_common/src/query_requirements.rs`

```rust
use promql_utilities::query_logics::enums::Statistic;
use promql_utilities::data_model::KeyByLabelNames;

/// What a query needs in order to be answered by a stored aggregation.
#[derive(Debug, Clone)]
pub struct QueryRequirements {
    /// Metric name (PromQL) or "table_name.value_column" (SQL)
    pub metric: String,
    /// One or more statistics needed. For avg this is [Sum, Count].
    /// All must be satisfied by aggregations sharing the same window / labels.
    pub statistics: Vec<Statistic>,
    /// The span of historical data the query reads, in milliseconds.
    /// None for spatial-only queries (no time range).
    pub data_range_ms: Option<u64>,
    /// GROUP BY labels expected in the result.
    pub grouping_labels: KeyByLabelNames,
    /// Normalized label filter string (use normalize_spatial_filter).
    pub spatial_filter_normalized: String,
}
```

Register in `lib.rs`:
```rust
pub mod query_requirements;
pub use query_requirements::*;
```

---

### Step 3 — Write tests for capability matching (TDD — write before implementation)

**New file**: `asap-common/dependencies/rs/sketch_db_common/src/capability_matching.rs`

Put the `#[cfg(test)]` block at the bottom of this file **before** writing any implementation.
Each test constructs a minimal `HashMap<u64, AggregationConfig>` and calls
`find_compatible_aggregation`.

Helper for tests — build a minimal `AggregationConfig`:
```rust
fn make_config(
    id: u64, metric: &str, agg_type: &str, sub_type: &str,
    window_size_s: u64, window_type: &str,
    grouping: &[&str], spatial_filter: &str,
) -> AggregationConfig { ... }
```

Tests to write (all should compile and **fail** before implementation):

| Test name | What it asserts |
|-----------|----------------|
| `basic_sum_match` | Sum query finds Sum config on correct metric |
| `quantile_any_value_finds_kll` | `Statistic::Quantile` matches `DatasketchesKLL` regardless of quantile value |
| `quantile_matches_hydrarkll` | `Statistic::Quantile` also matches `HydraKLL` |
| `no_match_wrong_metric` | Returns None when metric differs |
| `no_match_wrong_type` | Sum query does not match `DatasketchesKLL` |
| `window_tumbling_exact` | 5 min query (300_000 ms) matches 300 s tumbling config |
| `window_tumbling_divisible` | 900_000 ms query matches 300 s tumbling config (3 buckets) |
| `window_tumbling_not_divisible` | 600_000 ms query does NOT match 900 s tumbling config |
| `window_sliding_exact` | 5 min query matches 300 s sliding config |
| `window_sliding_too_large` | Query range > sliding window_size → no match |
| `window_priority_largest_wins` | Two tumbling configs (300 s and 900 s) for same metric/stat — 900 s chosen for 900_000 ms query |
| `spatial_only_no_range` | `data_range_ms = None` matches any window_size |
| `label_strict_exact` | Exact label set matches |
| `label_strict_superset_rejected` | Config with `{job, instance}` does NOT match query requiring only `{job}` (strict for now) |
| `label_mismatch_rejected` | Completely different labels → None |
| `spatial_filter_empty_both` | Both empty → match |
| `spatial_filter_query_empty_config_has_filter` | Config has filter, query has none → no match |
| `spatial_filter_same` | Same normalized filter → match |
| `spatial_filter_different` | Different filters → no match |
| `sub_type_min_matches_min` | `Statistic::Min` finds `MinMax` config with `sub_type = "min"` |
| `sub_type_max_rejects_min` | `Statistic::Max` does NOT find `MinMax` config with `sub_type = "min"` |
| `multi_pop_finds_key_agg` | `Statistic::Topk` finds `CountMinSketchWithHeap` (value) + `DeltaSetAggregator` (key) on same metric; result has distinct key/value ids |
| `avg_finds_sum_and_count` | `statistics = [Sum, Count]`, two configs present → both found, same window/labels |
| `avg_different_windows_rejected` | Sum config has 300 s window, Count config has 900 s window → None (must agree) |

---

### Step 4 — Implement capability matching in `sketch_db_common`

**File**: `asap-common/dependencies/rs/sketch_db_common/src/capability_matching.rs`

#### 4a. Pure compatibility helpers

```rust
/// Returns the aggregation_type strings that can serve this statistic.
pub fn compatible_agg_types(stat: Statistic) -> &'static [&'static str]

/// Returns the required aggregation_sub_type for this statistic, if any.
/// e.g. Min → Some("min"), Max → Some("max"), Quantile → None
pub fn required_sub_type(stat: Statistic) -> Option<&'static str>

/// Whether this value aggregation type requires a paired key aggregation
/// (SetAggregator / DeltaSetAggregator).
pub fn is_multi_population_value_type(agg_type: &str) -> bool

/// Window compatibility: can `config` serve a query needing `data_range_ms`?
/// - None (spatial-only): always true
/// - Tumbling: data_range_ms must be a positive multiple of window_size_ms
/// - Sliding: data_range_ms must equal window_size_ms exactly
pub fn window_compatible(config: &AggregationConfig, data_range_ms: Option<u64>) -> bool

/// Label compatibility: strict exact match.
/// TODO: relax to superset (config.grouping_labels ⊇ req.grouping_labels)
pub fn labels_compatible(config_labels: &KeyByLabelNames, req_labels: &KeyByLabelNames) -> bool

/// Spatial filter compatibility: both empty → ok; config non-empty and differs → reject.
pub fn spatial_filter_compatible(config_filter: &str, req_filter: &str) -> bool

/// Aggregation priority comparator: prefer larger window_size (descending).
/// Separate function so callers can swap it out.
pub fn aggregation_priority(a: &AggregationConfig, b: &AggregationConfig) -> std::cmp::Ordering
```

#### 4b. Core matching function

```rust
/// Find a compatible aggregation (or set of aggregations for avg / multi-pop queries)
/// given a map of all available aggregations and a set of query requirements.
///
/// Returns None if no fully compatible match exists.
/// Logs a debug message for each candidate that was considered but rejected.
pub fn find_compatible_aggregation(
    configs: &HashMap<u64, AggregationConfig>,
    requirements: &QueryRequirements,
) -> Option<AggregationIdInfo>
```

**Algorithm** (single-statistic path first, then generalize):

1. For each statistic `s` in `requirements.statistics`:
   - Collect all configs where:
     - `config.metric == requirements.metric`
     - `compatible_agg_types(s)` contains `config.aggregation_type`
     - `required_sub_type(s)` matches `config.aggregation_sub_type` (if Some)
     - `window_compatible(config, requirements.data_range_ms)`
     - `labels_compatible(&config.grouping_labels, &requirements.grouping_labels)`
     - `spatial_filter_compatible(&config.spatial_filter_normalized, &requirements.spatial_filter_normalized)`
   - Sort by `aggregation_priority` (largest window first)
   - Keep sorted list as `candidates[s]`

2. If any statistic has zero candidates → return None

3. For `Vec<Statistic>` with multiple entries (e.g. avg = [Sum, Count]):
   - Take the best candidate for `statistics[0]` (first after sort)
   - For each remaining statistic, find the best candidate that also shares the same
     `window_size` and `grouping_labels` as the first
   - If all are found → proceed; else return None

4. The value aggregation is the candidate for `statistics[0]`.

5. If `is_multi_population_value_type(value_agg.aggregation_type)`:
   - Search configs for one where `aggregation_type` is `"SetAggregator"` or
     `"DeltaSetAggregator"` AND `config.metric == requirements.metric`
   - If not found → return None
   - This becomes `aggregation_id_for_key`

6. Otherwise `aggregation_id_for_key = aggregation_id_for_value`.

7. Return:
   ```rust
   Some(AggregationIdInfo {
       aggregation_id_for_value: value_agg.aggregation_id,
       aggregation_type_for_value: value_agg.aggregation_type.clone(),
       aggregation_id_for_key: key_agg.aggregation_id,
       aggregation_type_for_key: key_agg.aggregation_type.clone(),
   })
   ```

Register in `lib.rs`:
```rust
pub mod capability_matching;
pub use capability_matching::find_compatible_aggregation;
```

All tests from Step 3 should now pass.

---

### Step 5 — Add wrapper method on `StreamingConfig`

**File**: `asap-query-engine/src/data_model/streaming_config.rs`

```rust
use sketch_db_common::{find_compatible_aggregation, QueryRequirements, AggregationIdInfo};

impl StreamingConfig {
    pub fn find_compatible_aggregation(
        &self,
        requirements: &QueryRequirements,
    ) -> Option<AggregationIdInfo> {
        find_compatible_aggregation(&self.aggregation_configs, requirements)
    }
}
```

---

### Step 6 — Write tests for `QueryRequirements` extraction (TDD — write before implementation)

**File**: add a new test module in `asap-query-engine/src/tests/` (e.g. `capability_matching_tests.rs`)

Tests for `build_query_requirements_promql`:

| Test | Input PromQL | Expected `QueryRequirements` field(s) |
|------|-------------|--------------------------------------|
| `promql_temporal_sum` | `sum_over_time(cpu[5m])` | `statistics=[Sum]`, `data_range_ms=Some(300_000)` |
| `promql_spatial_sum` | `sum(cpu)` | `statistics=[Sum]`, `data_range_ms=None` |
| `promql_temporal_quantile` | `quantile_over_time(0.9, latency[5m])` | `statistics=[Quantile]`, `data_range_ms=Some(300_000)` |
| `promql_temporal_rate` | `rate(requests[1m])` | `statistics=[Rate]`, `data_range_ms=Some(60_000)` |
| `promql_avg_expands` | `avg_over_time(cpu[5m])` | `statistics=[Sum, Count]` |
| `promql_label_extraction` | `sum(cpu{job="foo"})` | `grouping_labels` empty, `spatial_filter_normalized` encodes `job=foo` |

Tests for `build_query_requirements_sql` (same spirit, different syntax).

Integration tests for fallback wiring:

| Test | Setup | Expected behaviour |
|------|-------|--------------------|
| `capability_fallback_fires_when_no_config` | No `query_configs` entry; streaming_config has compatible KLL | Returns valid result |
| `config_path_takes_priority` | Both config entry and compatible KLL exist | Uses config entry (no capability matching) |
| `capability_fallback_warns` | No config entry; capability match found | `warn!` is emitted (check tracing subscriber) |
| `no_match_returns_none` | No config entry; no compatible aggregation | Returns None |
| `quantile_different_values_same_agg` | query_configs empty; KLL agg for `latency`; two queries `q(0.5)` and `q(0.9)` | Both resolve to same `aggregation_id` |

---

### Step 7 — Implement `build_query_requirements_promql` and `build_query_requirements_sql`

**File**: `asap-query-engine/src/engines/simple_engine.rs`

Add two private methods. Both reuse data already extracted during parsing (do not re-parse):

```rust
fn build_query_requirements_promql(
    &self,
    match_result: &PromQLMatchResult,
    query_pattern_type: QueryPatternType,
) -> QueryRequirements

fn build_query_requirements_sql(
    &self,
    match_result: &SQLQuery,
    query_pattern_type: QueryPatternType,
) -> QueryRequirements
```

**Extraction rules** (both methods):

| `QueryRequirements` field | Source |
|--------------------------|--------|
| `metric` | Already extracted from the parsed AST in the build-context methods |
| `statistics` | Map from `Statistic` (already computed); `Avg` → `[Sum, Count]`, else `[stat]` |
| `data_range_ms` | `OnlySpatial` → `None`; temporal → `range_seconds * 1000` (from match_result range) |
| `grouping_labels` | GROUP BY / `by(...)` labels from parsed AST |
| `spatial_filter_normalized` | Call `normalize_spatial_filter(&spatial_filter)` (already in `sketch_db_common::utils`) |

---

### Step 8 — Wire fallback into build context methods

**File**: `asap-query-engine/src/engines/simple_engine.rs`

In `build_query_execution_context_promql`, `build_query_execution_context_sql`,
`build_spatiotemporal_context`, and the Elastic DSL context builder — all four call
`find_query_config` / `find_query_config_sql` and then `get_aggregation_id_info`. Replace that
pair with:

```rust
let agg_info: AggregationIdInfo = if let Some(config) = self.find_query_config(&query) {
    self.get_aggregation_id_info(config)
} else {
    warn!(
        "No query_config entry for query '{}'. Attempting capability-based matching.",
        query
    );
    let requirements = self.build_query_requirements_promql(&match_result, query_pattern_type);
    self.streaming_config.find_compatible_aggregation(&requirements)?
};
```

For SQL, swap `find_query_config_sql` and `build_query_requirements_sql` accordingly.

No other logic in the build-context methods changes.

---

## Files Changed

| File | Change |
|------|--------|
| `asap-common/dependencies/rs/sketch_db_common/src/aggregation_config.rs` | Add `AggregationIdInfo` struct here (move from engine) |
| `asap-common/dependencies/rs/sketch_db_common/src/query_requirements.rs` | **New** — `QueryRequirements` struct |
| `asap-common/dependencies/rs/sketch_db_common/src/capability_matching.rs` | **New** — all compatibility helpers + `find_compatible_aggregation` |
| `asap-common/dependencies/rs/sketch_db_common/src/lib.rs` | Register two new modules, re-export |
| `asap-query-engine/src/data_model/streaming_config.rs` | Add `find_compatible_aggregation` wrapper |
| `asap-query-engine/src/engines/simple_engine.rs` | Remove local `AggregationIdInfo`, add build-requirements helpers, wire fallback in 4 build-context methods |
| `asap-query-engine/src/tests/capability_matching_tests.rs` | **New** — integration tests for fallback path |

---

## Verification

```bash
# 1. All existing tests still pass (no regressions)
cargo test -p asap-query-engine

# 2. New unit tests in sketch_db_common pass
cargo test -p sketch_db_common

# 3. Manually verify quantile routing: configure a streaming_config with one
#    DatasketchesKLL aggregation for a metric, no query_configs entry.
#    Send quantile(0.5) and quantile(0.9) range queries — both should succeed
#    and return results from the same aggregation_id (visible via debug logging).

# 4. Verify fallback warning: set RUST_LOG=warn and send an unconfigured query —
#    the warn! line should appear in stderr.

# 5. Verify priority: configure two tumbling Sum aggregations for the same metric
#    (window_size 300 s and 900 s). Send a sum_over_time(metric[900s]) query.
#    Confirm the 900 s aggregation is selected (check via debug log or test assertion).
```

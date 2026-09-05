# How to Add a New Sketch Algorithm

Adding a new sketch requires changes to 2 components: asap-common (sketch selection logic) and asap-query-engine (accumulator and query logic). The precompute engine builds sketches in-process from Prometheus remote write, so there's no separate UDF/pipeline-configuration step.

## Step 1: asap-common - Define Sketch Mapping

**File**: `asap-common/dependencies/py/promql_utilities/promql_utilities/query_logics/logics.py`

**What to modify**:
- `map_statistic_to_precompute_operator()` - Add mapping from statistic to your sketch name
- `does_precompute_operator_support_subpopulations()` - Add whether your sketch supports subpopulations

**Optional**: Add new statistic type to `enums.py::Statistic` if needed.

---

## Step 2: asap-query-engine - Implement Accumulator

### 2.1 Create Accumulator File

**File to create**: `asap-query-engine/src/precompute_operators/your_sketch_accumulator.rs`

**What to implement**:
- `YourSketchAccumulator` struct with sketch state
- `deserialize_from_json()` - Deserialize from the JSON wire format
- Query methods (e.g., `get_quantile()`, `get_sum()`)
- `merge_multiple()` - Merge multiple accumulators efficiently
- Implement traits:
  - `AggregateCore` (required) - `as_any()`, `get_accumulator_type()`, `clone_box()`, `merge_into()`
  - `MergeableAccumulator` (marker trait)
  - `SingleSubpopulationAggregate` (required) - `get_statistics()`, `get_statistic_values()`, `merge_with()`
  - `SerializableToSink` (if needed) - `serialize_to_sink()`

**Key requirement**: `get_accumulator_type()` must return the sketch name from CommonDependencies (PascalCase).

### 2.2 Register Accumulator

**File to modify**: `asap-query-engine/src/precompute_operators/mod.rs`

**What to add**:
```rust
pub mod your_sketch_accumulator;
pub use your_sketch_accumulator::*;
```

### 2.3 Wire Up the Precompute Engine

**Files to search**: Look for the "DatasketchesKLL" pattern in `asap-query-engine/src/precompute_engine/accumulator_factory.rs`.

**What to add**: A match arm for your sketch's `AggregationType` that constructs `YourSketchAccumulator`.

**Reference examples**:
- `asap-query-engine/src/precompute_operators/datasketches_kll_accumulator.rs`
- `asap-query-engine/src/precompute_operators/count_min_sketch_accumulator.rs`

---

## Step 3: asap-planner-rs - Sketch Parameters (Optional)

**Usually**: asap-planner-rs picks up sketch automatically from asap-common mapping. Custom sketch parameters (size, epsilon, etc.) can be added in the Rust source under `asap-planner-rs/src/`.

---

## Testing Checklist

- [ ] `cargo build --release` succeeds (asap-query-engine)
- [ ] `cargo test` passes (asap-query-engine)
- [ ] End-to-end: asap-planner-rs → QueryEngine (precompute engine) → Query result

---

## Naming Conventions

| Component | Format | Example |
|-----------|--------|---------|
| asap-common mapping | PascalCase | `DatasketchesKLL` |
| QueryEngine accumulator | PascalCase + Accumulator | `DatasketchesKLLAccumulator` |
| `get_accumulator_type()` return | Must match mapping | `"DatasketchesKLL"` |

---

## Common Issues

- **Query returns no results**: Check `get_statistic_values()` handles correct `Statistic` enum
- **Sketch not found**: Verify name matches across all components (case-sensitive)

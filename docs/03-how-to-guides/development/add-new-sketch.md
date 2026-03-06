# How to Add a New Sketch Algorithm

Adding a new sketch requires changes to 3 components: asap-common (sketch selection logic), asap-sketch-ingest (UDF for building sketches), and asap-query-engine (deserialization and query logic).

## Step 1: asap-common - Define Sketch Mapping

**File**: `asap-common/dependencies/py/promql_utilities/promql_utilities/query_logics/logics.py`

**What to modify**:
- `map_statistic_to_precompute_operator()` - Add mapping from statistic to your sketch name
- `does_precompute_operator_support_subpopulations()` - Add whether your sketch supports subpopulations

**Optional**: Add new statistic type to `enums.py::Statistic` if needed.

---

## Step 2: asap-sketch-ingest - Create Sketch UDF

**File to create**: `asap-sketch-ingest/templates/udfs/yoursketchname_[subop].rs.j2` (or `.rs` if no template vars)

**What to implement**:
- Rust UDF function using `#[udf]` macro
- Input: `Vec<f64>` (values to aggregate)
- Output: `Option<Vec<u8>>` (serialized sketch using MessagePack)
- Serialization format: Wrap sketch in struct with parameters, serialize with `rmp_serde`

**Naming convention**: Lowercase sketch name with optional sub-operator suffix (e.g., `datasketcheskll_.rs.j2`, `countminsketch_sum.rs.j2`)

**Validate**: Run `python validate_udfs.py` to check UDF compiles.

**Reference examples**:
- `asap-sketch-ingest/templates/udfs/datasketcheskll_.rs.j2`
- `asap-sketch-ingest/templates/udfs/countminsketch_sum.rs.j2`

---

## Step 3: asap-query-engine - Implement Accumulator

### 3.1 Create Accumulator File

**File to create**: `asap-query-engine/src/precompute_operators/your_sketch_accumulator.rs`

**What to implement**:
- `YourSketchAccumulator` struct with sketch state
- `deserialize_from_bytes_arroyo()` - Deserialize from MessagePack (must match UDF format)
- Query methods (e.g., `get_quantile()`, `get_sum()`)
- `merge_multiple()` - Merge multiple accumulators efficiently
- Implement traits:
  - `AggregateCore` (required) - `as_any()`, `get_accumulator_type()`, `clone_box()`, `merge_into()`
  - `MergeableAccumulator` (marker trait)
  - `SingleSubpopulationAggregate` (required) - `get_statistics()`, `get_statistic_values()`, `merge_with()`
  - `SerializableToSink` (if needed) - `serialize_to_sink()`

**Key requirement**: `get_accumulator_type()` must return the sketch name from CommonDependencies (PascalCase).

### 3.2 Register Accumulator

**File to modify**: `asap-query-engine/src/precompute_operators/mod.rs`

**What to add**:
```rust
pub mod your_sketch_accumulator;
pub use your_sketch_accumulator::*;
```

### 3.3 Add Deserialization Dispatcher

**Files to search**: Look for "DatasketchesKLL" pattern in `asap-query-engine/src/stores/` or `asap-query-engine/src/drivers/ingest/`

**What to add**: Match case for your sketch name calling `YourSketchAccumulator::deserialize_from_bytes_arroyo(buffer)`.

**Reference examples**:
- `asap-query-engine/src/precompute_operators/datasketches_kll_accumulator.rs`
- `asap-query-engine/src/precompute_operators/count_min_sketch_accumulator.rs`

---

## Step 4: asap-planner - Sketch Parameters (Optional)

**File to modify**: `asap-planner/classes/StreamingAggregationConfig.py` or `asap-planner/utils/logics.py`

**What to add**:
- Custom sketch parameters (size, epsilon, etc.) in `get_sketch_parameters()` or similar
- SLA-based parameter computation in `compute_sketch_parameters()` if needed

**Usually**: asap-planner picks up sketch automatically from asap-common mapping.

---

## Testing Checklist

- [ ] `validate_udfs.py` passes (ArroyoSketch)
- [ ] `cargo build --release` succeeds (asap-query-engine)
- [ ] `cargo test` passes (asap-query-engine)
- [ ] End-to-end: asap-planner → asap-sketch-ingest → Arroyo → Kafka → QueryEngine → Query result

---

## Naming Conventions

| Component | Format | Example |
|-----------|--------|---------|
| asap-common mapping | PascalCase | `DatasketchesKLL` |
| asap-sketch-ingest UDF filename | lowercase_subop | `datasketcheskll_.rs.j2` |
| QueryEngine accumulator | PascalCase + Accumulator | `DatasketchesKLLAccumulator` |
| `get_accumulator_type()` return | Must match mapping | `"DatasketchesKLL"` |

---

## Common Issues

- **UDF won't compile**: Check Rust syntax, dependencies in `[dependencies]` comment block
- **Deserialization fails**: MessagePack format must match exactly between UDF and accumulator
- **Query returns no results**: Check `get_statistic_values()` handles correct `Statistic` enum
- **Sketch not found**: Verify name matches across all components (case-sensitive)

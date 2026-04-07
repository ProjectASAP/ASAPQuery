# Capability-Based Aggregation Matching — Design Decision Record

## Problem Statement

Currntly, when query engine gets a query, it does an exact string match of that query against the inference config to find what aggregation ID to use. This results in 2 problems:
1. It also requires the inference config to exactly specify each and every query that can be supported.
2. This does NOT handle ad-hoc queries for which we may still have a sketch that is computed.

For instance, even when the same sketch can answer both `quantile(0.5, metric[5m])` and `quantile(0.9, metric[5m])`, we need 2 entries in inference config`.

Moreover, if we now get `quantile(0.6, metric[5m])` or `quantile(0.5, metric[10m])`, those queries get punted even if we can support them.

The goal: let the engine understand what a query *needs* and find an existing aggregation that can
*provide* it, without requiring a one-to-one mapping in config.

---

## Architecture Investigation

Before designing anything, the existing query routing path was traced:

1. An incoming query (PromQL or SQL) is parsed and reduced to a `QueryExecutionContext`.
2. Inside that process, `find_query_config` (exact string match) or `find_query_config_sql`
   (structural AST match) look up a `QueryConfig` in `InferenceConfig.query_configs`.
3. `QueryConfig` is nothing more than a join record: query string → list of `aggregation_id`s.
4. `get_aggregation_id_info` then looks up those IDs in `StreamingConfig` to get the actual
   `AggregationConfig` (sketch type, window, labels, etc.).

The key insight: **all capability information lives in `AggregationConfig` inside `StreamingConfig`**.
The `QueryConfig` table is just indirection that requires manual pre-population. The fix is to
skip it and match against `AggregationConfig` directly when no pre-configured entry exists.

So the logic now is (a) define what the query needs (QueryRequirements), (b) match QueryRequirements against all the available aggregation_ids to see if there is a match. There is no explicit CapabilityProvidedBySketch data structure

---

## Design Questions and Answers

The following questions were worked through before writing a single line of implementation.

### Q1: When multiple aggregations are compatible, which one wins?
**Decision**: Prefer the largest `window_size`. Encapsulated in a separate `aggregation_priority`
comparator function so this policy is swappable later without touching the matching logic.

### Q2: Label compatibility — how strict?
**Decision**: Strict exact match for now. A sketch grouped by `{job, instance}` does **not** serve
a query that groups by `{job}` only, even though collapsing labels is mathematically valid for
simple accumulators (Sum, Min, Max). The reason: for sketch types (KLL, CountMin), label collapsing
is not well-defined. Adding a TODO to relax this to "superset ok" for simple accumulators in a
future iteration.

### Q3: Spatial filter compatibility?
**Decision**: If the stored aggregation has a non-empty `spatial_filter` and the query's normalized
filter differs (or is absent), reject. Never silently serve data filtered to `{env="prod"}` to a
query that expects unfiltered data.

### Q4: Multi-population sketches (SetAggregator / DeltaSetAggregator)?
These require two aggregation IDs: one "value" aggregation and one "key" aggregation. The existing
`get_aggregation_id_info` already distinguishes them by type: `SetAggregator` and
`DeltaSetAggregator` are key aggregations; everything else is a value aggregation.

**Decision**: Capability matching finds the value aggregation first (based on the statistic). If
the matched value type is a "multi-population" type (`MultipleSumAccumulator`,
`MultipleMinMaxAccumulator`, `MultipleIncreaseAccumulator`, `CountMinSketchWithHeap`), the matcher
then separately searches for a key aggregation (`SetAggregator` or `DeltaSetAggregator`) on the
same metric. Both IDs are required; if either is missing, the match fails.

### Q5: Backward compatibility — keep old `query_configs` path?
**Decision**: Yes, as a primary route. The `query_configs` lookup runs first; capability matching
fires only when no pre-configured entry is found. This means existing deployments change behavior
only for queries that had no config entry. A `warn!` log is emitted whenever capability matching
is used, so operators can detect fallback usage.

### Q6: Rich error messages on no-match?
**Decision**: Deferred. Collecting per-candidate rejection reasons adds significant complexity.
The matcher returns `None` on failure for now.

### Q7: How to model `avg` (needs both Sum and Count)?
**Decision**: `QueryRequirements` holds `Vec<Statistic>`. For `avg`, this is `[Sum, Count]`.
All statistics in the vec must be satisfied by aggregations that share the **same** `window_size`
and `grouping_labels`. This ensures temporal consistency.

### Q8: How is window type (sliding vs tumbling) expressed in requirements?
Framing the requirement as a specific `window_type` was considered but rejected. Instead,
`QueryRequirements` stores only `data_range_ms: Option<u64>` — the span of historical data the
query reads. Both tumbling and sliding aggregations can satisfy this, subject to different
compatibility rules:

- **Tumbling**: `data_range_ms` must be a positive integer multiple of `window_size_ms` (so
  multiple buckets can be merged to cover the range).
- **Sliding**: `data_range_ms` must equal `window_size_ms` exactly (a sliding window precomputes
  exactly one range per timestamp; you can't merge overlapping windows).
- **Spatial-only** (`data_range_ms = None`): any window is compatible.

`QueryRequirements` intentionally does **not** carry start/end timestamps. Capability matching is
about whether a config can serve a query's *shape* (how much historical data it needs), not *when*
that data was recorded. The actual timestamps are computed separately in
`calculate_query_timestamps_promql` / `calculate_query_timestamps_sql` and placed into
`StoreQueryParams` after the aggregation has been selected. This keeps the two concerns — "can
this config serve this query type?" and "fetch this time window from the store" — cleanly
separated.

### Q9: Where does the capability matching logic live?
**Decision**: `sketch_db_common` (the shared crate). Rationale: this logic is pure — it takes a
map of `AggregationConfig` values and a `QueryRequirements` and produces an `AggregationIdInfo`.
It has no dependency on query engine internals. Putting it in common means the planner and other
components can eventually reuse it.

`AggregationIdInfo` (previously defined in `simple_engine.rs`) was moved to `sketch_db_common` as
a prerequisite, since the common function needs to return it.

`StreamingConfig` (in `asap-query-engine`) gets a thin wrapper method that delegates to the
common function, so call sites inside the engine don't need to reach into common directly.

### Q10: `aggregation_sub_type` — does it matter for matching?
**Decision**: Yes. `Min` requires `aggregation_sub_type == "min"`, `Max` requires `"max"`. The
`required_sub_type(statistic)` helper encodes this. Other statistics have no sub-type constraint.

### Q11: For `Vec<Statistic>` — must all statistics agree on window and labels?
**Decision**: Yes. For `avg = [Sum, Count]`, the matched Sum aggregation and the matched Count
aggregation must have the same `window_size` and `grouping_labels`. This is the simpler, safer
choice — mixing aggregations with different windows or label granularities would produce
semantically incorrect results.

---

## Known Limitations

### Cleanup policy is not considered
`CleanupPolicy` (`CircularBuffer`, `ReadBased`, `NoCleanup`) and `num_aggregates_to_retain` live
on `InferenceConfig` / `AggregationReference` — not on `AggregationConfig`. Capability matching
only inspects `AggregationConfig`, so it has no visibility into how many historical windows a given
aggregation is actually retaining.

**Practical consequence**: if a `CircularBuffer` aggregation retains only N windows but a query
needs more, capability matching will still route to it. The failure surfaces at query execution
time (the store returns insufficient data), not at routing time.

The `query_configs` path handles this correctly because `num_aggregates_to_retain` is set
explicitly per query via `AggregationReference`, giving operators direct control. Capability
matching has no equivalent mechanism today.

**Future mitigation**: add `data_range_ms` coverage check — verify that the store actually holds
at least `ceil(data_range_ms / window_size_ms)` recent windows for the selected aggregation before
committing to it.

### Label compatibility is strictly exact
A config grouped by `{job, instance}` does **not** match a query grouping by `{job}` only, even
though label collapsing is mathematically valid for simple accumulators. This is conservative: for
sketch types (KLL, CountMin) label collapsing is not well-defined. See the TODO comment in
`labels_compatible` in `capability_matching.rs` for the planned relaxation.

### No structured rejection reasons
When no match is found, `find_compatible_aggregation` returns `None` without explaining which
candidates were considered and why each was rejected. Debug-level logs record per-candidate
rejections, but there is no structured error type. This makes diagnosing misconfigurations harder
— see "Rich rejection errors" in the Rejected section below.

---

## What Was Rejected

### "Translate PromQL to SQL and execute via DataFusion SQL engine"
Considered as a broader architectural direction. Rejected for this feature because:
- Data is stored as binary sketches (KLL, CountMin, etc.), not raw values. SQL aggregation
  functions cannot merge sketches natively.
- Every sketch operation would need a DataFusion UDF, recreating the existing operator logic
  with more indirection.
- The existing `execute_plan()` path already uses DataFusion as an execution *framework* with
  custom physical operators — that is the right abstraction boundary, not SQL strings.

### Merging PromQL and SQL `build_query_execution_context` paths
The two build paths (PromQL and SQL) were kept separate. They parse different syntaxes into
different AST types. Merging them would require a common intermediate representation before the
current `QueryExecutionContext` and would not reduce complexity. The shared logic is the capability
matching layer, not the parsing layer.

### Rich rejection errors
Collecting per-candidate rejection reasons (e.g. "found KLL for metric X but window 15 m doesn't
match 5 m query") was considered. Deferred: the matching logic touches every candidate and
collecting structured reasons multiplies the implementation surface significantly. Simple `None`
return with `debug!` logging is sufficient for now.

---

## Final Architecture

```
Incoming query (PromQL or SQL)
        │
        ▼
   Parse query AST
        │
        ▼
Try find_query_config / find_query_config_sql    ← existing path (unchanged)
        │
        ├── found ──► get_aggregation_id_info(config) ──► AggregationIdInfo
        │
        └── not found ──► warn!("falling back to capability matching")
                              │
                              ▼
                     build_query_requirements_{promql|sql}
                     → QueryRequirements {
                           metric, statistics: Vec<Statistic>,
                           data_range_ms, grouping_labels,
                           spatial_filter_normalized
                       }
                              │
                              ▼
                     StreamingConfig::find_compatible_aggregation(&requirements)
                     → sketch_db_common::find_compatible_aggregation(
                           &self.aggregation_configs, requirements
                       )
                     → Option<AggregationIdInfo>
```

The `find_compatible_aggregation` function in `sketch_db_common`:
1. For each statistic, collects candidates from `StreamingConfig` passing all filters
   (metric, type, sub-type, window, labels, spatial filter).
2. Sorts candidates by `aggregation_priority` (largest window first).
3. For `Vec<Statistic>`, ensures all statistics are satisfied by configs agreeing on
   window and labels.
4. If the value aggregation type is multi-population, also finds the paired key aggregation.
5. Returns `AggregationIdInfo` or `None`.

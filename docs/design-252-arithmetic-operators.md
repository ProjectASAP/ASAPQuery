# Design: PromQL Arithmetic Operator Acceleration (Issue #252)

## Problem

ASAPQuery accelerates PromQL queries by pre-computing sketches over streaming data and serving answers from those sketches at query time, bypassing the underlying TSDB for supported query patterns.

The supported patterns today are all single-expression forms:
- `rate(metric[range])`, `increase(metric[range])`, etc. (OnlyTemporal)
- `sum(metric) by (label)`, `quantile(...)`, etc. (OnlySpatial)
- Combinations like `sum by (host) (rate(metric[range]))` (OneTemporalOneSpatial)

Binary arithmetic expressions like `rate(errors[5m]) / rate(requests[5m])` fall through entirely — `handle_query_promql` finds no matching pattern and returns `None`, causing a full fallback to Prometheus. This is a significant coverage gap: error rate, saturation, and ratio queries are extremely common in practice.

---

## Approaches Considered

### Option A: Extend the PromQL-specific execution path

Detect binary arithmetic at the top of `handle_query_promql`, execute each arm through the existing `execute_query_pipeline`, collect two `HashMap<Labels, f64>` result sets, and combine them with a Rust-level label-matching join.

**Pros:** Self-contained, surgical change. No DataFusion involvement in the combination step.

**Cons:** The combination logic (label-matching join + f64 arithmetic) would be written twice — once here, and again later when the PromQL execution is migrated to DataFusion. `execute_query_pipeline` is already on a path to being replaced.

### Option B: DataFusion JOIN + Projection

Use the existing DataFusion execution path (`execute_plan`, already tested but not wired in for PromQL) to build a plan that looks like:

```
Projection (value = lhs.value OP rhs.value, label columns...)
  └── Join (inner, on = label columns)
        ├── SubqueryAlias("lhs") → SummaryInfer → SummaryMergeMultiple → PrecomputedSummaryRead
        └── SubqueryAlias("rhs") → SummaryInfer → SummaryMergeMultiple → PrecomputedSummaryRead
```

For scalar-vector arithmetic (`rate(errors[5m]) * 100`), the plan is simpler — just a `Projection` on top of the single arm's plan, no join needed.

**Pros:** `execute_plan` is tested and ready to wire in. DataFusion's `Join + Projection` replaces hand-written label-matching join logic. Wiring this in for binary arithmetic also migrates all PromQL execution to `execute_plan`, eliminating `execute_query_pipeline` as an active path.

**Cons:** Slightly more complex plan construction (SubqueryAlias for column disambiguation). The migration of all PromQL to `execute_plan` is a broader scope, but it's the right time to do it.

---

## Decision: Option B (DataFusion)

The decisive factor: `execute_plan` is tested and just not wired in. Building the arithmetic combination in Rust (Option A) would be work done twice — the exact same join logic would need to be re-implemented in DataFusion when the migration happens. Option B does it once, correctly.

The additional complexity of the DataFusion `Join + Projection` plan is manageable and follows patterns already established in the codebase (`SubqueryAlias`, `LogicalPlanBuilder`).

---

## Design

### Planner (`asap-planner-rs`)

For a query like `rate(errors[5m]) / rate(requests[5m])`, the planner:

1. Detects the top-level `BinaryExpr` in the PromQL AST
2. Checks whether each arm is individually acceleratable (matches an existing pattern)
3. If both arms are acceleratable: emits a separate `QueryConfig` entry for each arm, as if they were independent queries
4. If either arm is not acceleratable: skips both — the engine will fall back to Prometheus
5. Deduplicates: if an arm's config already exists (e.g., `rate(errors[5m])` was also configured as a standalone query), it reuses the existing entry

For scalar-vector arithmetic (`rate(errors[5m]) * 100`): the scalar literal is not a metric expression and needs no aggregation config. The planner emits a config only for the vector arm.

**The planner does not emit a combined config for the binary expression itself.** The engine detects the arithmetic operator at query time from the PromQL AST and handles the combination.

### Engine (`asap-query-engine`)

#### Config lookup: structural PromQL matching

To find a config for each arm at query time, the engine uses structural AST comparison (`find_query_config_promql_structural`) rather than exact string matching. This mirrors the existing `find_query_config_sql` pattern and is robust to formatting differences.

Structural equality for PromQL compares: function name, metric name, label selectors, range duration. Evaluation timestamps are ignored.

#### Plan construction

For **vector op vector**:
1. Find config for each arm via structural matching
2. Build a `QueryExecutionContext` for each arm (reusing existing logic)
3. Call `to_logical_plan()` on each context to get the sketch sub-plans
4. Wrap each sub-plan in a `SubqueryAlias` (`"lhs"` / `"rhs"`) to disambiguate the `value` column name
5. Build a DataFusion inner `Join` on the shared label columns
6. Add a `Projection` computing `lhs.value OP rhs.value AS value` and projecting label columns through

For **scalar op vector** (e.g., `rate(errors[5m]) * 100`):
1. Build a `QueryExecutionContext` for the vector arm only
2. Call `to_logical_plan()` on the context
3. Add a `Projection` computing `value OP lit(scalar) AS value` (or `lit(scalar) OP value` if scalar is on the left)

#### Execution

Both cases execute through `execute_logical_plan` — the refactored inner method extracted from `execute_plan`. This method takes a pre-built `LogicalPlan` and runs it through the DataFusion session.

#### Dispatch

In `handle_query_promql`, after parsing the PromQL AST, the engine checks whether the top node is a `BinaryExpr`. If yes, it routes to `handle_binary_expr_promql`. If no, it follows the existing single-expression path — but now calling `execute_plan` instead of `execute_query_pipeline`.

Recursion is natural: `(A + B) / C` is a `BinaryExpr` at the top level. The LHS is itself a `BinaryExpr`, which `handle_binary_expr_promql` handles recursively when building the LHS sub-plan.

If either arm cannot be accelerated (no matching config, or unsupported pattern), `handle_binary_expr_promql` returns `None` and the caller falls back to Prometheus for the whole query.

### Operators supported

All six PromQL binary arithmetic operators: `+`, `-`, `*`, `/`, `^`, `%`.

### Out of scope

- PromQL vector matching modifiers (`on()`, `ignoring()`, `group_left()`, `group_right()`): the join is always on all label columns. Adding vector matching is a follow-on.
- Range queries (only instant queries addressed here).
- Comparison operators (`==`, `!=`, `>`, etc.) and set operators (`and`, `or`, `unless`).

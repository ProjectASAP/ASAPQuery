# Engine boundaries for ASAPPlanner plans

Status: in discussion. This document is the source of truth; update it as decisions change.

## Context

ASAPQuery (`asap-query-engine`) has two core engines. The precompute engine
builds sketches at ingest. `SimpleEngine` answers queries from them. Both are
driven by asap-planner-rs output (`StreamingConfig`, `InferenceConfig`). We are
replacing both engines with ones that execute plans from ASAPPlanner
(`../ASAPPlanner`). Those plans arrive as an `InstallablePlan`, which wraps a
`PhysicalASAPDAG` plus the query texts, one per DAG root. This lets us retire
asap-planner-rs. The protocol adapters, fallback clients and ingest decoders
stay. This doc defines the traits and boundaries around the new engines. It
does not cover how the engines are implemented.

Terms:
- **Summary**: a stored, materialized node of the plan (ASAPPlanner calls it a
  `MaterializedView`), identified by its `PhysicalASAPNodeId`.
- **Planned query**: a query whose text is in `InstallablePlan.query_texts`.
- **Capability-matched query**: a query that is not in the plan but can be
  answered from the installed summaries.

## Interfaces: before → after

Kept unchanged:
- `QueryRequestAdapter`, `QueryResponseAdapter`: still parse requests and format
  `QueryResult`. The request struct they return changes (see `QueryRequest`).
- `FallbackClient`: unchanged, except that it takes the merged `QueryRequest`.
- Ingest decoders (Prometheus remote write, VictoriaMetrics, OTLP, CSV, JSON).

Changed:
- `ParsedQueryRequest {query, time}` and
  `ParsedRangeQueryRequest {query, start, end, step}` → one
  `QueryRequest {language, query, eval: Instant{time} | Range{start, end, step}}`.
  Times are `i64` ms; the adapters convert from protocol seconds.
- `QueryResult {Vector, Matrix}` → adds `Scalar` and `Table {columns, rows}`.
  The SQL response adapters (ClickHouse, Elastic) format `Table`. Today they
  flatten `Vector` into rows of label values plus one value, and return 501 for
  `Matrix`. Each response adapter returns an error for result variants it
  doesn't support.
- `HttpProtocolAdapter::handle_runtime_info(Arc<dyn Store>)` → takes a
  `RuntimeInfoSource` instead of the store. Contents are tabled (T4).
- `IngestSource`: its `IngestContext` holds old-engine routing state
  (`SeriesRouter`, `AggregationConfig`-based routing configs). It is replaced by
  a handle to `IngestEngine`. `OtlpReceiver` becomes an ordinary
  `IngestSource`. Today it runs outside `IngestSource`, and the precompute
  engine is skipped for OTLP: it decodes points and only logs them, so the
  data is dropped.
- `QueryTracker` → `QueryWorkloadTracker`. It emits ASAPPlanner's
  `PlanningWorkload`.
- `HttpServer` holds `Arc<dyn QueryEngine>` instead of `Arc<SimpleEngine>`.

Replaced:
- `PlannerClient` (calls asap-planner-rs) → `PlanProvider` (returns an
  ASAPPlanner `InstallablePlan`).
- `StreamingConfig` / `InferenceConfig` → `InstalledPlan`.
- `Store`, keyed by `(metric, aggregation_id, window)` with `AggregateCore`
  values → `SummaryStore` + `RawDataStore`.
- `SimpleEngine` (a concrete struct) → `QueryEngine` trait.
- `PrecomputeEngine` (a concrete struct) → `IngestEngine` trait.

Removed from the boundary (internal to the old engines): `AccumulatorUpdater`,
`AggregateCore` and the related aggregate traits, `OutputSink`, `WindowMerger`,
`QueryPlanRuntime`.

## New interfaces

Signatures are indicative. Method-level details are still open.

```rust
struct InstalledPlan {                 // ASAPQuery's validated view of an InstallablePlan
    id: PlanId,
    plan: InstallablePlan,             // + derived summaries, refresh groups, raw retention
}

trait PlanProvider {                   // input: PlanningWorkload + DeploymentCapabilities
    // → InstalledPlan, pushed to PlanInstaller. Implementations: from a plan file,
    //   or in-process ASAPPlanner.
}

trait IngestEngine {
    install(plan); ingest(SampleBatch); flush(); shutdown();
}

trait QueryEngine {                   // steps through range queries itself
    install(plan);
    execute(plan, &AnswerSource, &QueryRequest) -> Result<EngineQueryResult, QueryEngineError>;
}

struct PlanInstaller { /* installs each new InstalledPlan into both engines */ }

trait QueryResolver {                  // chained: planned resolver, then capability resolver
    resolve(plan, &QueryRequest) -> Option<AnswerSource>;   // None: can't answer it; never fails
}

struct QueryDispatcher { resolvers, engine }   // called by HttpServer for every query
// dispatch(&QueryRequest) -> QueryOutcome:
//   no resolver matches        → NotAnswerable(Unmatched)
//   engine → NotReady           → NotAnswerable(NotReady)
//   engine → Err(e)             → NotAnswerable(EngineError(e)) or Failed(e), per error policy
//   engine → Ready              → Ready

enum AnswerSource {
    Planned { root: PhysicalASAPNodeId },               // matched InstallablePlan.query_texts
    CapabilityMatched { matched: CapabilityMatch },     // contents tabled (T2)
}

enum EngineQueryResult {
    Ready { result: QueryResult },     // label/column names live inside QueryResult
    NotReady,                          // summaries don't yet cover the query → fallback
}

enum QueryOutcome {
    Ready { result: QueryResult, source: AnswerSource },
    NotAnswerable(NotAnswerableReason),                 // → FallbackClient
    Failed(QueryEngineError),                           // → error response
}
enum NotAnswerableReason { Unmatched, NotReady, EngineError(QueryEngineError) }

trait SummaryStore {
    put(SummaryKey, SummaryState); get(plan, summary, range); evict_before(plan, summary, ts);
    earliest_timestamp_per_summary() -> Map<summary, ts>;   // was get_earliest_timestamp_per_aggregation_id
}
struct SummaryKey   { plan: PlanId, summary: PhysicalASAPNodeId, window: (i64, i64) }
struct SummaryState { encoding: EncodingId, bytes: Bytes }  // opaque to the store

trait RawDataStore { append(Source, SampleBatch); read(Source, range); evict(retention, now); }

enum SampleBatch {
    TimeSeries { /* (ts, value, structured label set) rows */ },   // Source::TimeSeries
    Table { schema, rows },                                         // Source::Table
}
```

How these fit together:
- **`QueryResolver`** is one matching strategy. `None` means "this resolver
  can't answer this query".
- **`QueryDispatcher`** orchestrates every query in the same three steps:
  resolve it to an `AnswerSource`, execute it, and map the result to an answer,
  a fallback or an error. Putting this in `HttpServer` would repeat it in every
  handler (instant, range). Putting it in `QueryEngine` would repeat plan
  matching and fallback policy in every engine implementation. The dispatcher
  does it once, so engines only execute. `QueryResolver` is a trait only so
  capability matching (T2) can be replaced without touching the dispatcher.
- **`AnswerSource`** is passed to `QueryEngine::execute` to tell the engine what
  to run: a DAG root, or a capability match. The engine doesn't redo matching.
- **`EngineQueryResult`** is what the engine reports. **`QueryOutcome`** is the
  dispatcher's final verdict to `HttpServer`, combining the resolver result, the
  engine result and the error policy.

## Query path (pseudocode)

```rust
// HttpServer: one handler shape for instant and range queries.
async fn handle(http_req) -> Response {
    let req: QueryRequest = match adapter.parse(http_req) {
        Ok(r) => r,
        Err(e) => return adapter.format_error(e),
    };
    match dispatcher.dispatch(&req).await {
        QueryOutcome::Ready { result, .. } =>
            adapter.format_success(result),           // errors on unsupported result variants
        QueryOutcome::NotAnswerable(_) => match fallback {
            Some(f) => f.execute(&req).await,
            None => adapter.format_unsupported(),
        },
        QueryOutcome::Failed(e) => adapter.format_error(e),
    }
}

impl QueryDispatcher {
    async fn dispatch(&self, req: &QueryRequest) -> QueryOutcome {
        let plan = self.current_plan();   // set by PlanInstaller; one snapshot for the whole query
        let Some(source) = self.resolvers.iter().find_map(|r| r.resolve(&plan, req)) else {
            return QueryOutcome::NotAnswerable(Unmatched);
        };
        match self.engine.execute(&plan, &source, req).await {
            Ok(EngineQueryResult::Ready { result }) => QueryOutcome::Ready { result, source },
            Ok(EngineQueryResult::NotReady) => QueryOutcome::NotAnswerable(NotReady),
            Err(e) => self.error_policy(e),   // NotAnswerable(EngineError(e)) or Failed(e); see Q3
        }
    }
}

// Planned resolver: exact match against the plan's query texts.
fn resolve(plan: &InstalledPlan, req: &QueryRequest) -> Option<AnswerSource> {
    let i = plan.query_texts().position(|q| q == req.query)?;
    Some(AnswerSource::Planned { root: plan.dag().roots[i] })
}
```

## Decisions

1. **The contract is ASAPPlanner's plan types, not its executor.** Only
   ASAPPlanner's `asap-types` plan types (`InstallablePlan`, `PhysicalASAPDAG`,
   `MaterializedView`, `Source`, …) cross ASAPQuery boundaries. The engine
   traits must not depend on `asap_executor`, which may change or be retired.
2. **Two engine traits.** `IngestEngine` and `QueryEngine` are the only seams for
   the engines. Adapters, fallback and decoders don't know which engine
   implementation is behind them.
3. **Fallback.** A query falls back to the real backend when it is not in the
   plan and not capability-matched, or when the engine returns `NotReady`.
4. **Answers record their source.** A query result records whether it came from
   a planned root or from capability matching (`AnswerSource`).
5. **One place for engine-error policy.** How engine errors map to responses
   (fallback or error) is decided in a single place: `QueryDispatcher`.
6. **Query language is on the request.** `QueryRequest` carries the language so
   that one deployment can later serve several languages.
7. **Times.** Engines and stores see `i64` ms only. The adapters convert from
   protocol times.
8. **Stores.** The deployment owns the stores and injects them into both
   engines. They are in-memory only, with no durability: after a restart they
   are empty and queries return `NotReady` (→ fallback). Summary keys include
   `PlanId`.
9. **Capabilities.** The deployment declares one `DeploymentCapabilities`, which
   `PlanProvider` passes to ASAPPlanner.
10. **Workload.** `QueryWorkloadTracker` emits ASAPPlanner's `PlanningWorkload`.
11. **Resolution is whole-query.** An `AnswerSource` covers a whole query; there
    is no partial or sub-expression resolution for now.
12. **Raw data enters through `IngestSource`** (via `IngestEngine`; see 23).
13. **Ingest batches.** `SampleBatch` carries structured label sets instead of
    today's `labels: String`. It is one enum with a `TimeSeries` variant and a
    `Table` variant, matching the plan's two `Source` kinds.
14. **`QueryResult` gains `Scalar` and `Table`.**
15. **RQE endpoint.** It is kept for testing only and removed at the end of the
    migration.
16. **`QueryDispatcher`.** It resolves each query, runs the engine and applies
    the fallback and error policy. `HttpServer` calls it.
17. **One resolver trait.** `QueryResolver` implementations are chained: the
    planned resolver first, then the capability resolver. Split it into
    separate traits later if their inputs diverge.
18. **`PlanInstaller`.** One component installs each new plan into both engines.
19. **Range queries.** `QueryEngine` receives the whole range request and
    decides how to step through it.
20. **Watermarks, lateness and backpressure.** `IngestEngine` owns them,
    configured by deployment config (today's `LateDataPolicy` model). This is
    the simplest option, chosen for now. The alternatives were per-source
    watermarks, lateness stated in the plan, or a combination.
21. **Plan delivery is push, for now.** `PlanProvider` decides when a new plan
    exists (for example, the workload changed or the plan file changed) and
    hands it to `PlanInstaller`.
22. **`QueryLanguage` comes from ASAPPlanner.** We use
    `asap_types::workload::QueryLanguage` (`PromQL`, `MetricsQL`,
    `SQL(DataFusionSQL | ClickhouseSQL | ElasticSQL)`, `DataFusion`,
    `ElasticDSL`), which covers ASAPQuery's current enum.
23. **`IngestSource` dependencies.** The deployment owns the engine and the
    stores. An `IngestSource` gets only a shared `IngestEngine` handle,
    `run(self, engine: Arc<dyn IngestEngine>)`, and calls `ingest(batch)`. The
    engine decides what goes into `RawDataStore`. Sources never write to stores:
    otherwise every source would need the plan's raw-data retention, and raw data
    the engine rejects as late would still be stored. A cleaner alternative is
    for sources to be pure producers (`run(self) -> Stream<SampleBatch>`, or
    sending into a channel) that the deployment forwards to the engine, so sources
    know nothing about engines. It isn't chosen for now because it adds stream
    or channel wiring.
24. **Resolvers can't fail.** `resolve` returns `None` when it can't answer a
    query; it has no error case.
25. **A planned root is a `PhysicalASAPNodeId`.**
26. **`PlanInstaller` validates incoming plans.** If it rejects a plan, the
    current plan stays installed.
27. **Install failure.** If installing a plan into either engine fails, the
    current plan stays installed.
28. **`DeploymentCapabilities` is a constant** in the code.
29. **Response adapters return an error for result variants they don't
    support** (for example, PromQL given `Table`, or SQL given `Matrix`).
30. **Engine implementations are selected by a command-line flag**, so old and
    new engines can coexist during the migration.
31. **Label names live inside `QueryResult`.** `Vector` and `Matrix` carry their
    output label names, as `Table` carries its columns. This replaces today's
    separate `KeyByLabelNames` next to the result
    (`QueryExecutionResult {query_output_labels, query_result}`), so every
    variant is self-describing.
32. **The RQE endpoint bypasses the dispatcher.** RQE requests are a JSON
    expression tree, not a query text, so they don't fit `QueryRequest` or the
    resolvers. The test-only endpoint keeps calling the old `SimpleEngine`
    directly until it is removed (decision 15).

## Tabled

- **T1. Plan swap semantics.** When a new plan is installed, do we drain and
  rebuild, or run the old and new plans side by side until the new summaries
  cover the needed lookback? This affects `install` versus building engines per
  plan.
- **T2. How a capability match is computed.** Ideally this is an ASAPPlanner API
  (plan a query against the installed summaries, with no new materialization).
  It is not designed yet, so `CapabilityMatch` is a placeholder.
- **T3. Per-request query requirements.** The same query text with different
  accuracy requirements collides today (issue #778). Should `QueryRequest` carry
  requirements?
- **T4. `RuntimeInfoSource` contents.**
- **T5. Store method signatures.** What `SummaryStore::get` returns, who
  allocates `EncodingId`, who triggers eviction (at install time or
  periodically), and whether `RawDataStore::read` takes predicates.
- **T6. Ingesting pre-built sketches.** OTLP can carry sketch payloads, which
  today are decoded and only logged. Should `SampleBatch` accept pre-built
  summaries? Out of scope for the MVP; to be done later.

## Open

- **Q3. Error policy and `QueryEngineError` variants.** Does an engine error fall
  back (`NotAnswerable(EngineError)`) or fail (`Failed`)? Today it fails with a
  500. Does the policy need to tell "plan not executable by this engine" apart
  from "execution failed"? If not, a single error type is enough.
- **Q9. Plan components.** Are `PlanProvider` and `PlanInstaller` separate
  components, and which one decides when to replan? This may revise decision 21.
- **Q10. Engine method errors and backpressure.** What errors do `install`,
  `ingest` and `flush` return? When the engine is overloaded, does `ingest` wait
  until it can accept the batch, or return an error immediately (so a
  remote-write source can return 503)? Today it waits: samples go to workers
  over bounded channels (`channel_buffer_size`, default 10,000) and
  `send().await` blocks when a channel is full, so the remote-write HTTP response
  is delayed. An error happens only if a worker's channel is closed, and the
  source then returns 500. A decode error returns 400.

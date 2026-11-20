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
- **View**: a stored, materialized node of the plan (ASAPPlanner's
  `MaterializedView`). Three concepts describe what is stored:
  - **`ViewDefinitionId`**: the computation, physically: ASAPPlanner's logical
    `definition_id` (what the view computes, incl. summary type) plus its
    physical properties, sketch parameters θ and pane width (decision 44).
  - **`ViewInputDataScope`**: the concrete data that went in: the selection's
    constraints (e.g. label matchers) and the exact pane start and end.
  - **`ViewInstance`**: the stored state for one definition over one input data
    scope, plus metadata.
- **Planned query**: a query whose text is in `InstallablePlan.query_texts`.
- **Capability-matched query**: a query that is not in the plan but can be
  answered from the stored views.

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
- `HttpServer` holds a `QueryDispatcher` (which holds `Arc<dyn QueryEngine>`)
  instead of `Arc<SimpleEngine>`, except with the old engines (decision 34).

Replaced:
- `PlannerClient` (calls asap-planner-rs) → no trait. `QueryWorkloadTracker`
  calls ASAPPlanner (`plan_installable`) directly and hands the result to
  `PlanInstaller`.
- `StreamingConfig` / `InferenceConfig` → `InstalledPlan`.
- `Store`, keyed by `(metric, aggregation_id, window)` with `AggregateCore`
  values → `ViewStore` + `RawDataStore`.
- `SimpleEngine` (a concrete struct) → `QueryEngine` trait.
- `PrecomputeEngine` (a concrete struct) → `IngestEngine` trait.

Removed from the boundary (internal to the old engines): `AccumulatorUpdater`,
`AggregateCore` and the related aggregate traits, `OutputSink`, `WindowMerger`,
`QueryPlanRuntime`.

## New interfaces

Signatures are indicative. Method-level details are still open.

```rust
struct InstalledPlan {                 // ASAPQuery's validated view of an InstallablePlan
    id: PlanId,                        // the plan's version for the workload: a counter, incremented per install
    plan: InstallablePlan,             // nothing else; derived data is computed from it
}

struct PlanInstaller {                 // validates and installs into both engines + dispatcher
    install(InstallablePlan) -> Result<()>;   // on any failure, the current plan stays
}
// Callers: startup (plan file, if given) and QueryWorkloadTracker
// (plan_installable(workload, DeploymentCapabilities) when it decides to replan).

trait IngestEngine {
    install(plan) -> Result<(), InstallError>; ingest(RawDataBatch); shutdown();   // closes windows on its own timer
}

trait QueryEngine {                   // holds its installed plan; steps through range queries itself
    install(plan) -> Result<(), InstallError>;
    execute(&AnswerSource, &QueryRequest) -> Result<EngineQueryResult, QueryEngineError>;
}

struct QueryDispatcher { engine }     // called by HttpServer for every query
// dispatch(&QueryRequest) -> QueryOutcome:
//   no match (planned, then capability) → NotAnswerable(Unmatched)
//   engine → NotReady           → NotAnswerable(NotReady)
//   engine → Err(e)             → NotAnswerable(EngineError(e)), logged and counted (I12)
//   engine → Ready              → Ready

enum AnswerSource {
    Planned { plan: PlanId, root: PhysicalASAPNodeId }, // matched InstallablePlan.query_texts
    CapabilityMatched { plan: PlanId, matched: CapabilityMatch },  // contents tabled (T2)
}

enum EngineQueryResult {
    Ready { result: QueryResult },     // label/column names live inside QueryResult
    NotReady,                          // stored views don't cover the whole query → fallback
}

enum QueryOutcome {
    Ready { result: QueryResult, source: AnswerSource },   // source: for logging
    NotAnswerable(NotAnswerableReason),                 // → FallbackClient
}
enum NotAnswerableReason { Unmatched, NotReady, EngineError(QueryEngineError) }

struct ViewDefinitionId {             // decision 44
    definition_id: DefinitionId,       // ASAPPlanner's logical id
    physical: PhysicalProperties,      // from ASAPPlanner: θ, pane width
}
struct ViewInputDataScope {           // the concrete data that went in
    constraints: SelectionConstraints, // from the plan's selection, e.g. label matchers
    interval: (i64, i64),              // the exact pane (start, end]
}
struct ViewInstanceKey { definition: ViewDefinitionId, scope: ViewInputDataScope }
struct ViewInstance { state: ViewState, metadata: ViewInstanceMetadata }   // metadata: open, e.g. size
struct ViewState(Arc<dyn Any + Send + Sync>);   // the engine's native state, opaque to the store (I6)

trait ViewStore {                     // in-memory or on disk; indexing is the implementation's (decision 8)
    put(ViewInstanceKey, ViewInstance);                        // once; never rewritten (I16)
    get(&ViewInstanceKey) -> Option<ViewInstance>;
    instances(&ViewDefinitionId, &SelectionConstraints, interval) -> Vec<(ViewInputDataScope, ViewInstance)>;
    evict_before(&ViewDefinitionId, ts);
    timestamp_range_per_view() -> Map<ViewDefinitionId, (earliest, latest)>;  // runtime info; was get_earliest_timestamp_per_aggregation_id
}

trait RawDataStore { append(Source, RawDataBatch); read(Source, range); evict(retention, now); }

enum RawDataBatch {                    // raw input only; never data between operators
    TimeSeries { series: Vec<(LabelSet, Vec<(i64, f64)>)> },   // Source::TimeSeries; Table later
}
```

How these fit together:
- **`QueryDispatcher`** orchestrates every query in the same three steps:
  resolve it to an `AnswerSource`, execute it, and map the result to an answer,
  a fallback or an error. Putting this in `HttpServer` would repeat it in every
  handler (instant, range). Putting it in `QueryEngine` would repeat plan
  matching and fallback policy in every engine implementation. The dispatcher
  does it once, so engines only execute.
- **Resolving** is matching only: which plan root, or which capability match,
  answers this query. It is two private functions of the dispatcher,
  `resolve_planned` then `resolve_capability`; `None` means no match. It is not
  a trait because there are only two fixed matchers and capability matching is
  undesigned (T2).
- **`AnswerSource`** is passed to `QueryEngine::execute` to tell the engine what
  to run: a DAG root, or a capability match. The engine doesn't redo matching.
- **`EngineQueryResult`** is what the engine reports. **`QueryOutcome`** is the
  dispatcher's final verdict to `HttpServer`, combining the match result and
  the engine result. Engine errors fall back (I12), so there is no failure
  variant.

## Query path (pseudocode)

New engines only. With the old engines (decision 34), `HttpServer` calls
`SimpleEngine` directly, as today.

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
    }
}

impl QueryDispatcher {
    async fn dispatch(&self, req: &QueryRequest) -> QueryOutcome {
        // Set by PlanInstaller; None before the first install.
        let Some(plan) = self.current_plan() else { return QueryOutcome::NotAnswerable(Unmatched) };
        let Some(source) = resolve_planned(&plan, req).or_else(|| resolve_capability(&plan, req)) else {
            return QueryOutcome::NotAnswerable(Unmatched);
        };
        // The engine compares source's PlanId with its installed plan; a mismatch
        // (a plan swap between resolve and execute) returns NotReady.
        match self.engine.execute(&source, req).await {
            Ok(EngineQueryResult::Ready { result }) => QueryOutcome::Ready { result, source },
            Ok(EngineQueryResult::NotReady) => QueryOutcome::NotAnswerable(NotReady),
            Err(e) => { self.count_engine_error(&e); QueryOutcome::NotAnswerable(EngineError(e)) }  // I12
        }
    }
}

// Exact match against the plan's query texts.
fn resolve_planned(plan: &InstalledPlan, req: &QueryRequest) -> Option<AnswerSource> {
    let i = plan.query_texts().position(|q| q.trim() == req.query.trim())?;   // I18
    Some(AnswerSource::Planned { plan: plan.id, root: plan.dag().roots[i] })
}

fn resolve_capability(plan: &InstalledPlan, req: &QueryRequest) -> Option<AnswerSource> {
    None   // until capability matching is designed (T2)
}
```

## Startup, plan install and ingest path (pseudocode)

New engines only. With the old engines (decision 34), wiring is as today.

```rust
// main: the deployment owns the stores and injects them (decision 8).
let views = Arc::new(InMemoryViewStore::new());
let raw = Arc::new(InMemoryRawDataStore::new());
let (ingest_engine, query_engine) = engines_for(cli.engine, views.clone(), raw.clone());  // decision 30
let dispatcher = Arc::new(QueryDispatcher::new(query_engine.clone()));
let installer = Arc::new(PlanInstaller::new(ingest_engine.clone(), query_engine.clone(), dispatcher.clone()));
if let Some(path) = cli.plan_file {
    installer.install(InstallablePlan::from_json(&read(path))?);   // failure → no plan; queries fall back
}
for source in ingest_sources { spawn(source.run(ingest_engine.clone())); }  // decision 23
spawn(QueryWorkloadTracker::new(installer.clone()).run());
HttpServer::new(adapter, dispatcher, fallback).run().await;

impl PlanInstaller {
    fn install(&self, plan: InstallablePlan) -> Result<()> {
        let plan = InstalledPlan::validate(plan, self.next_id())?;  // reject → current plan stays (26)
        // All-or-nothing across the three: if any step fails, the current plan stays (27).
        self.ingest_engine.install(plan.clone())?;
        self.query_engine.install(plan.clone())?;
        self.dispatcher.set_plan(plan);
        Ok(())
    }
}

// QueryWorkloadTracker: the data plane decides when to replan (decision 21).
loop {
    // records queries the HttpServer reports
    if self.should_replan() {
        let plan = asap_planner::plan_installable(self.workload(), DEPLOYMENT_CAPABILITIES)?;
        let _ = self.installer.install(plan);
    }
}

// An IngestSource's HTTP handler; the protocol (e.g. Prometheus remote write) picks the decoder.
async fn handle_ingestion(protocol: IngestProtocol, body: Bytes) -> StatusCode {
    let Ok(batch) = decoder_for(protocol).decode(body) else { return StatusCode::BAD_REQUEST };
    match engine.ingest(batch).await {   // waits under backpressure (decision 33)
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,   // engine gone
    }
}

// IngestEngine::ingest, at the boundary: uses its installed plan to decide what
// to keep; writes RawDataStore, and ViewStore keyed by (ViewDefinitionId, ViewInputDataScope).
```

## Decisions

1. **The contract is ASAPPlanner's plan types, not its executor.** ASAPPlanner's
   `asap-types` plan types (`InstallablePlan`, `PhysicalASAPDAG`,
   `MaterializedView`, `Source`, …) cross ASAPQuery boundaries. Executor value
   types may appear where they fit, but the traits must not depend on the
   executor's runtime (`compile`, `RunContext`, `DataStore`), which may change or
   be retired.
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
   engines. `ViewStore` and `RawDataStore` are the boundary; an implementation
   may keep data in memory, on disk or both, and chooses its own indexing. The
   store never looks inside a view's state, so the state's structure can change
   without changing the store. The first implementations are in-memory; after a
   restart they are empty and queries return `NotReady` (→ fallback). Recovery
   from a durable store is deferred; `ViewDefinitionId` is the same across
   restarts, so it can key recovered instances.
9. **Capabilities.** The deployment declares one `DeploymentCapabilities`, which
   `QueryWorkloadTracker` passes to ASAPPlanner when it replans.
10. **Workload.** `QueryWorkloadTracker` emits ASAPPlanner's `PlanningWorkload`.
11. **Resolution is whole-query.** An `AnswerSource` covers a whole query; there
    is no partial or sub-expression resolution for now.
12. **Raw data enters through `IngestSource`** (via `IngestEngine`; see 23).
13. **Raw data batches.** `RawDataBatch` (named after ASAPPlanner's
    `read_raw_data`) is raw input only: what `IngestSource` passes to
    `IngestEngine::ingest` and what `RawDataStore` appends and returns. Data
    between operators is internal to the engine. `TimeSeries` holds per-series
    structured label sets (instead of today's `labels: String`), since series in
    one batch have different labels. v1 has only `TimeSeries` (I8); a `Table`
    variant comes with table sources. ASAPPlanner has no such type: the planner
    handles no data, and the executor's `Batch` has one fixed schema per batch
    (a time-series `Scan` gives one column per label), so the ingest engine
    converts into it using the plan's `Scan` schema.
14. **`QueryResult` gains `Scalar` and `Table`.**
15. **RQE endpoint.** It is kept for testing only and removed at the end of the
    migration.
16. **`QueryDispatcher`.** It resolves each query, runs the engine and applies
    the fallback and error policy. `HttpServer` calls it.
17. **No resolver trait.** Resolving is two private dispatcher functions, tried
    in order: `resolve_planned`, then `resolve_capability`. Add a trait only if
    capability matching later needs to be swappable.
18. **`PlanInstaller`.** One component installs each new plan into both engines.
19. **Range queries.** `QueryEngine` receives the whole range request and
    decides how to step through it.
20. **Watermarks, lateness and backpressure.** `IngestEngine` owns them,
    configured by deployment config (today's `LateDataPolicy` model). This is
    the simplest option, chosen for now. The alternatives were per-source
    watermarks, lateness stated in the plan, or a combination.
21. **Plan delivery: the data plane decides when to replan, with no plan
    trait.** `QueryWorkloadTracker` decides when to replan, calls ASAPPlanner
    directly and passes the plan to `PlanInstaller::install`. At startup, a plan
    file (if given) is loaded and installed the same way. There is no
    `PlanProvider` or plan-source trait: the plan's origin is simply whoever
    calls `install`.
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
    for sources to be pure producers (`run(self) -> Stream<RawDataBatch>`, or
    sending into a channel) that the deployment forwards to the engine, so sources
    know nothing about engines. It isn't chosen for now because it adds stream
    or channel wiring.
24. **Resolving can't fail.** The resolve functions return `None` when they
    can't answer a query; they have no error case.
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
    expression tree, not a query text, so they don't fit `QueryRequest` or
    query-text matching. The test-only endpoint keeps calling the old `SimpleEngine`
    directly until it is removed (decision 15).
33. **Ingest backpressure: `ingest` waits** until the engine can accept the
    batch, as today. Today samples go to workers over bounded channels
    (`channel_buffer_size`, default 10,000) and `send().await` blocks when a
    channel is full, delaying the remote-write HTTP response. `ingest` returns an
    error only if the engine is gone (today a closed worker channel, which leads
    to a 500).
34. **The old engines bypass the new pipeline.** When the flag (decision 30)
    selects the old engines, `HttpServer` calls `SimpleEngine` directly and
    ingest uses `PrecomputeEngine`, both driven by asap-planner-rs as today. The
    dispatcher and `PlanInstaller` are used only with the new
    engines.
35. **Engines hold their installed plan.** `QueryEngine::execute` takes only the
    `AnswerSource` and the request, not the plan. `AnswerSource` carries the
    `PlanId` it was resolved against. If the engine's installed plan differs (a
    plan swap between resolve and execute), the engine returns `NotReady`, so the
    query falls back.
36. **`NotReady` covers the whole query.** If the stored views don't cover the
    whole query, the result is `NotReady`: for a range query, if any step isn't
    covered; for an instant query, if its time is past what the views cover.
    There are no partial answers.
37. **`AnswerSource` in `QueryOutcome::Ready` is for logging only.**
38. **`PlanId` is the plan's version for the workload**: a counter,
    incremented per install. It is not part of view store keys.
39. **Before the first plan is installed, queries fall back.** The dispatcher has
    no plan, so every query is `Unmatched`.
40. **One capabilities constant.** There is one old engine and one new engine,
    and only the new engine uses ASAPPlanner, so a single
    `DeploymentCapabilities` constant is enough (decision 28).
41. **Ingest before the first plan is dropped.** `IngestEngine::ingest` discards
    batches until a plan is installed.
42. **`InstalledPlan` is just `InstallablePlan` + `PlanId`.**
43. **View store model: definition, input data scope, instance.** A
    `ViewInstance` (state + metadata) is keyed by
    `(ViewDefinitionId, ViewInputDataScope)`: what was computed, and over which
    data. The store is opaque to the state's structure (decision 8). This
    matches the ASAPQuery-backend's store (`SummaryInstanceKey {definition,
    selection}` over opaque states).
44. **`ViewDefinitionId` is ASAPPlanner's logical id plus its physical
    properties; we wait for them.** ASAPPlanner today has only a logical id
    (`ViewCoverage::definition_id`: a hash of the view's sub-DAG down to its
    `Scan`s, including the summary type), which omits θ (`node.parameters`) and
    window geometry, so two plans can share it while storing states that are
    not interchangeable. ASAPPlanner (ProjectASAP/ASAPPlanner#1429) keeps the
    logical id and exposes the physical properties as separate fields on each
    materialized view; together they change exactly when stored states are not
    interchangeable:
    - in: the logical definition (incl. summary type), θ, pane width;
    - out: refresh mode (ingestion or first read) and retention, which change
      how a state is built or kept, not what it is; the pane's exact start and
      end, which are in `ViewInputDataScope`.

    We take both from the plan and don't derive either ourselves. The logical
    id alone is not a store key; it lets capability matching (T2) find a view
    that answers a query at a different θ.
45. **`timestamp_range_per_view` stays.** It feeds runtime info (today's
    `earliest_timestamp_per_aggregation_id`), used to send queries aligned with
    when views are computed.

## Implementation plan

A stack of PRs on top of the design-doc PR (#935), each on the previous one
and each one logical step, so a human can review them one at a time. The repo
may be broken between PRs. The interfaces live in a module of
`asap-query-engine` (`src/engine_api/`), not a separate crate: nothing outside
`asap-query-engine` uses them yet.

0. **Blocked on ASAPPlanner exposing physical view properties** (decision 44; ProjectASAP/ASAPPlanner#1429). The
   stack starts after that lands upstream.
1. **Dependency and interfaces only.** ASAPPlanner as a pinned git dependency
   (`asap-types`, renamed to avoid clashing with ASAPQuery's `asap_types`, and
   `asap-planner`). A new module with only the traits, structs and enums of
   this doc, no implementations and no call-site changes.
2. **`QueryRequest` replaces `ParsedQueryRequest`/`ParsedRangeQueryRequest`**
   in adapters, `HttpServer`, `FallbackClient` and `SimpleEngine`. Mechanical.
3. **`QueryResult` gains `Scalar` and `Table`**; adapters error on variants
   they don't support.
4. **In-memory `ViewStore` and `RawDataStore`**, with unit tests.
5. **`QueryDispatcher` and `PlanInstaller`**, tested with fake engines
   (fallback rules, `NotReady`, no plan yet, install-once, error policy).
6. **Split `PrecomputeEngine` into pipeline + build specification (I9).** No
   behavior change; old tests pass unchanged.
7. **Push `IngestEngine`**: plan → build specification (I8), executor kernels,
   installer validation of supported operators.
8. **Executor-backed `QueryEngine`**: `DataStore` adapter, thread pool (I15),
   missing view instance → `NotReady` (I10), eviction (I17).
9. **Wiring**: `--engine`, `--plan-file`, `IngestSource` → `IngestEngine`,
   `OtlpReceiver` behind `IngestSource`, `handle_ingestion`; first end-to-end
   test (one PromQL instant query).
10. **Range queries and the remaining query types.**
11. **`QueryWorkloadTracker`** emits `PlanningWorkload` with the default
    `AccuracyTarget` (I5).
12. **I4 gate harness.**
13. **Deletions, one PR each:** `SimpleEngine`, `PrecomputeEngine`,
    asap-planner-rs, `PlannerClient`, `StreamingConfig`/`InferenceConfig`, the
    RQE endpoint.

`RuntimeInfoSource` joins the stack once its contents are decided (T4).

Implementation decisions:

- **I1. The first `QueryEngine` wraps `asap-executor`'s query path.** Its
  `RunContext` is `!Send`, so it runs on a dedicated engine thread. Its
  `read_materialized_view` serves states from `ViewStore` by downcast (I6).
  The traits let us replace it later (decision 1).
- **I7. The first `IngestEngine` is our own push-based streaming engine,**
  modeled on today's precompute engine (route series, per-window updaters in
  workers, seal into `ViewStore` when the watermark passes a window's end
  plus lateness). The executor's ingestion is pull-based: it refreshes each
  tumbling window once, after it closes, by reading the window's raw rows,
  so every sample would be buffered for a window even when the plan keeps no
  raw data. What to build comes from the plan's ingestion subgraphs instead of
  `AggregationConfig`. States are built with the executor's kernel factory
  (`AccumulatorUpdater`), so they are the executor's `AggregateCore` and the
  query engine reads them without conversion. It writes to `RawDataStore` only
  what the plan's raw-data retention requires.
- **I8. Ingest-time operators the push engine runs (v1):** `Scan` of one
  `TimeSeries` source, `Filter`, `Project`, `Aggregate` (exact), and the
  sketch builds `SummaryAgg` and `SummaryTumblingWindowChain`. `Table`
  sources come later. The capabilities constant (decision 28) steers the
  planner to this subset: `excluded_candidates` excludes every other
  ingestion-time operator, and `supported_summaries` lists only the executor
  kernel factory's sketch types. `PlanInstaller` validation (decision 26)
  rejects a plan that still uses another ingestion-time operator; the current
  plan stays.
- **I9. The push engine reuses today's precompute pipeline.** A
  no-behavior-change PR first splits `PrecomputeEngine` into a generic pipeline
  (series routing, workers, windows, watermark and late-data policy,
  backpressure) and a build specification it is given (what to build per series
  and where to write it). The old engine supplies an `AggregationConfig`-based
  one and its tests must pass unchanged; the new `IngestEngine` supplies a
  plan-based one (I8). Windowing and lateness logic then exists once.
- **I10. `NotReady` is detected by running, not by a pre-check.** The
  executor reads only stored windows that exactly cover the requested range,
  and requires an error for a missing one. Our `DataStore` adapter returns a
  distinct missing-instance error, which the engine maps to `NotReady`; other
  executor errors are `Err` (I12). A pre-check would duplicate the executor's
  window arithmetic. In a range query, the first missing step makes the whole
  query `NotReady` (decision 36).
- **I11. Strict evaluation time.** A query is answered at exactly its `T`, or
  is `NotReady`. The engine never rounds `T` down to the newest sealed window
  and never reads the ingest engine's open windows. Queries at "now" therefore
  fall back until the newest window is sealed (T7).
- **I12. Engine errors fall back** (`NotAnswerable(EngineError)`), and are
  logged and counted so engine bugs stay visible (the counter is exposed via
  `RuntimeInfoSource`, T4, or logs). Today they return a 500; falling back
  still gives the user a correct answer. `QueryOutcome` has no failure variant;
  only a failing fallback is an error response. The policy doesn't tell error
  kinds apart, so `QueryEngineError` is a single type.
- **I13. No `flush` on `IngestEngine`.** Closing idle windows stays inside the
  engine on its own timer, as today's precompute engine does; nothing outside
  calls it, and with no durability there is nothing to flush at shutdown. Tests
  close windows by ingesting samples that advance the watermark. `install`
  returns `Result<(), InstallError>`, a single type with a message:
  `PlanInstaller` only needs failure (keep the current plan, decision 27) and a
  reason to log. `shutdown` is best-effort and returns nothing.
- **I14. v1 installs once.** `PlanInstaller` accepts only the first plan;
  `QueryWorkloadTracker` keeps replanning and logs later plans without
  installing them, as today's `QueryTracker` does. Plan swap (T1) stays tabled;
  `install` and `NotReady` on a `PlanId` mismatch already leave room for it.
- **I15. The executor-backed `QueryEngine` runs a pool of N engine threads**
  (deployment config, default the CPU count). Each compiles the installed plan
  itself and instantiates it against one shared `DataStore` adapter; queries are
  dispatched round-robin over per-thread channels. One thread would serialize
  every query, where today's `SimpleEngine` serves them concurrently.
  Compiling per thread avoids relying on the compiled plan being `Send`.
- **I16. Late data: only `Drop`.** Samples later than the allowed lateness are
  dropped; a sealed window never changes. Every query run then reads a
  consistent store state, and each window has exactly one state, as the
  executor's `DataStore` contract requires, with no snapshots or locking.
  `ForwardToStore` is tabled (T8).
- **I17. Only the query engine evicts, by time.** The ingest engine never
  evicts. The query engine tracks the latest evaluation time it has served
  (a range query's end) and, after each query, calls
  `evict_before(ViewDefinitionId, latest_T − retention_ms)` per view and
  evicts raw data past the plan's raw-data
  retention. This uses the planner's retentions as given; today's read-count
  policy (`ReadBased`) would need a read count the plan doesn't have. With no
  queries nothing is evicted, as with `ReadBased` today.
- **I18. Planned queries match by exact text**, after trimming surrounding
  whitespace. The tracker plans the texts it saw in requests, so repeated
  queries match; cosmetic variants fall back. Parsing and comparing queries
  would need a parser per language. Capability matching (T2) is the general fix.
- **I2. Phase 1 before the new engine path**, so the dispatcher is built on
  `QueryRequest` from the start and Phase 1 PRs are useful on their own.
- **I3. ASAPPlanner is a pinned git dependency.**
- **I4. Gate for deleting the old engines.** Old and new run different plans,
  so their answers are not compared with each other. On a fixed set of
  benchmark workloads: (1) coverage: every query the old engine answers without
  fallback, the new engine answers too once its views cover it; (2)
  accuracy: every new answer meets the `AccuracyTarget` its query was planned
  with (ε, allowing δ failures), against the exact
  answer from the backend (Prometheus or ClickHouse via the fallback client),
  and exactly-answered queries match exactly. Performance and memory are not
  part of the gate.
- **I5. One deployment-wide default `AccuracyTarget`** (config or CLI), which
  `QueryWorkloadTracker` puts on every query it records. ASAPPlanner treats a
  query without one as exact, so the planner would never choose sketches. A
  plan file is planned with its own workload's targets; keep that workload with
  it so I4 can check against them.
- **I6. A `ViewInstance` holds the engine's native state**, as an opaque
  `ViewState(Arc<dyn Any + Send + Sync>)` the engine downcasts. Bytes would
  cost a decode on every query read (the executor reads views as live
  `AggregateCore` objects; today's `Store` holds them too). Executor types in
  the trait would tie the store to the executor. A durable store would need an
  engine encode method, added when durability is.

## Tabled

- **T1. Plan swap semantics.** When a new plan is installed, do we drain and
  rebuild, or run the old and new plans side by side until the new views
  cover the needed lookback? This affects `install` versus building engines per
  plan. v1 sidesteps this (I14). A `ViewDefinitionId` that includes physical properties (decision 44)
  lets a new plan reuse instances an old plan stored.
- **T2. How a capability match is computed.** Ideally this is an ASAPPlanner API (ProjectASAP/ASAPPlanner#1430)
  (plan a query against the stored views and raw data, with no new
  materialization). Not in v1.
  It is not designed yet, so `CapabilityMatch` is a placeholder.
- **T3. Per-request query requirements.** The same query text with different
  accuracy requirements collides today (issue #778). Should `QueryRequest` carry
  requirements?
- **T4. `RuntimeInfoSource` contents.**
- **T5. `RawDataStore::read` predicates.** Whether it takes predicates (the
  executor re-applies them, so a store may ignore them).
- **T6. Ingesting pre-built sketches.** OTLP can carry sketch payloads, which
  today are decoded and only logged. Should `RawDataBatch` accept pre-built
  summaries? Out of scope for the MVP; to be done later.

- **T7. Freshness at "now".** If fallback for queries at "now" dominates in
  benchmarks, allow a configured staleness tolerance (answer from the newest
  sealed window) or read the open window's state from the ingest engine.
- **T8. `ForwardToStore` late data.** Merging late samples into sealed windows
  needs merge-on-read (one state per window) and snapshot reads per query run.

## Open

None currently.

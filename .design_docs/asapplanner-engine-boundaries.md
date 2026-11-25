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
stay, in their own crate, `asap-adapters` (decision 46). This doc defines the traits and boundaries around the new engines. It
does not cover how the engines are implemented.

Terms:
- **View**: a stored, materialized node of the plan (ASAPPlanner's
  `MaterializedView`). Two concepts describe what is stored:
  - **`ViewId`**: `(PlanId, PhysicalASAPNodeId)`, the view's node in the
    installed plan. The node fixes what the view computes: its sub-DAG,
    selection, sketch parameters θ and window widths (decision 44).
  - **`ViewInstance`**: the stored state of one view for one window, the
    absolute `(start, end]`, plus metadata.
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
    execute(&AnswerSource, &QueryRequest) -> Result<EngineQueryResult, QueryEngineError>;   // a batch of one
    execute_batch(&[(AnswerSource, QueryRequest)]) -> Vec<Result<EngineQueryResult, QueryEngineError>>;   // decision 47
}

struct QueryDispatcher { engine }     // called by HttpServer for every query
// dispatch(&QueryRequest) -> QueryOutcome:
//   no match (planned, then capability) → NotAnswerable(Unmatched)
//   engine → NotReady           → NotAnswerable(NotReady)
//   engine → Err(e)             → NotAnswerable(EngineError(e)), logged and counted (I12)
//   engine → Ready              → Ready
// dispatch_batch(&[QueryRequest]) -> Vec<QueryOutcome>: the same, per query, with one
//   engine.execute_batch call for every matched query (decision 47)

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

struct ViewId { plan: PlanId, node: PhysicalASAPNodeId }   // decision 44
struct ViewInstanceKey { view: ViewId, window: (i64, i64) }  // the absolute window (start, end]
struct ViewInstance { state: ViewState, metadata: ViewInstanceMetadata }   // metadata: open, e.g. size
struct ViewState(Arc<dyn Any + Send + Sync>);   // the engine's native state, opaque to the store (I6)

trait ViewStore {                     // in-memory or on disk; indexing is the implementation's (decision 8)
    put(ViewInstanceKey, ViewInstance);                        // once; never rewritten (I16)
    get(&ViewInstanceKey) -> Option<ViewInstance>;
    instances(&ViewId, interval) -> Vec<((i64, i64), ViewInstance)>;   // the stored windows within interval
    evict_before(&ViewId, ts);
    timestamp_range_per_view() -> Map<ViewId, (earliest, latest)>;  // runtime info; was get_earliest_timestamp_per_aggregation_id
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
// to keep; writes RawDataStore, and ViewStore keyed by (ViewId, window).
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
   from a durable store is deferred. It needs a view id that does not depend
   on the plan's `PlanId` (ProjectASAP/ASAPPlanner#1429).
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
    handles no data, and the query engine reads Arrow record batches with one
    fixed schema each (a time-series `Scan` gives one column per label), so the
    `QueryEngine` wrapper converts raw data into them using the plan's `Scan`
    schema.
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
    incremented per install. It is part of view store keys (decision 44).
39. **Before the first plan is installed, queries fall back.** The dispatcher has
    no plan, so every query is `Unmatched`.
40. **One capabilities constant.** There is one old engine and one new engine,
    and only the new engine uses ASAPPlanner, so a single
    `DeploymentCapabilities` constant is enough (decision 28).
41. **Ingest before the first plan is dropped.** `IngestEngine::ingest` discards
    batches until a plan is installed.
42. **`InstalledPlan` is just `InstallablePlan` + `PlanId`.**
43. **View store model: view, window, instance.** A `ViewInstance` (state +
    metadata) is keyed by `(ViewId, window)`: which view, and which absolute
    window `(start, end]`. One instance holds the states of all of the
    window's group keys. The store is opaque to the state's structure
    (decision 8).
44. **A view is identified by `(PlanId, PhysicalASAPNodeId)`.** The node fixes
    the view's sub-DAG, selection, θ (`node.parameters`) and window widths, so
    no hashing and no new ASAPPlanner field are needed. ASAPPlanner's
    `ViewCoverage::definition_id` is not a store key: it is a logical id that
    leaves out θ, the window width and the selection, so two different views in
    one plan can share it. It stays useful for capability matching (T2).
    Instances cannot be reused across plans or recovered after a restart; that
    needs a plan-independent id (ProjectASAP/ASAPPlanner#1429). v1 installs once
    (I14), so this does not matter yet. This replaces an earlier design keyed by
    `definition_id` plus physical properties and an input data scope.
45. **`timestamp_range_per_view` stays.** It feeds runtime info (today's
    `earliest_timestamp_per_aggregation_id`), used to send queries aligned with
    when views are computed.
46. **Adapters are a separate crate, `asap-adapters`, that depends on the
    engine crate.**
    - `asap-adapters` holds everything that talks to an outside system:
      protocol adapters, `HttpServer`, fallback clients, ingest decoders and
      `IngestSource` implementations, `QueryWorkloadTracker`, and every
      binary.
    - `asap-query-engine` becomes a library: the engines, stores,
      `QueryDispatcher`, `PlanInstaller` and `engine_api`. `QueryDispatcher`
      and `PlanInstaller` use only the engine traits; fallback is called from
      `HttpServer` (decision 5).
    - Dependencies point one way, adapters → engine, so the engine can't reach
      HTTP, protocol code or a planner.
    - The engine crate never depends on a planning algorithm. Today's
      asap-planner-rs (deleted in the last step) and ASAPPlanner's
      `asap-planner` (added with `QueryWorkloadTracker`) are used only from
      `asap-adapters`. The engine crate depends only on ASAPPlanner's
      `asap-types` plan types (decision 1). A new or remote planner changes
      only `asap-adapters`.
    - ASAPPlanner stays a pinned git dependency (I3), not copied in: planning
      is control-plane logic, not protocol translation.

    Not solved: the engine still depends on ASAPPlanner's plan types (decision
    1). `asap-adapters` can see everything public in the engine crate;
    visibility narrows this, and it is fully closed once the old engines are
    deleted. `handle_runtime_info` still takes the old engine's
    `Arc<dyn Store>` until T4. There is no separate `engine_api` crate:
    `HttpServer` calls `SimpleEngine` directly while the old engines are
    selectable (decision 34), so `asap-adapters` depends on the engine crate
    anyway.

47. **Queries can be executed as a batch.** `QueryEngine::execute_batch` and
    `QueryDispatcher::dispatch_batch` take several queries at once, so the
    engine computes results that the plan shares between queries evaluated at
    the same time only once (ASAPPlanner's `Lifetime::OneEvaluation`).
    `execute` is a batch of one, and the instant and range endpoints use it
    unchanged. A batch endpoint uses `dispatch_batch`; each query in a batch
    still gets its own result, `NotReady` and fallback.
48. **Sketch kernels live in a shared crate.** The sketch kernels (the
    executor's `summary_kernels/`, including its kernel factory) move out of
    `asap-executor` into their own ASAPPlanner crate. The ingest engine builds
    states with it and the query engine reads them, so states need no
    conversion and deleting the executor does not affect ingestion.

## Implementation plan

A stack of PRs on top of the design-doc PR (#935), each on the previous one
and each one logical step, so a human can review them one at a time. The repo
may be broken between PRs. The interfaces live in a module of
`asap-query-engine` (`src/engine_api/`), not a separate crate: their only
outside user is `asap-adapters`, which depends on the engine crate anyway.
Split it out only if a second engine crate appears.

1. **Extract `asap-adapters` (decision 46) as a pure move.** `drivers/`
   (query adapters, fallback, ingest), `query_tracker/`, `planner_client.rs`
   with its asap-planner-rs dependency, `main.rs` and every binary in
   `src/bin/` move to the new crate; all of them use `drivers`. The engine
   crate becomes a library. `tests/sql_query_support.rs` and
   `tests/e2e_precompute_equivalence.rs` move too, or keep asap-planner-rs as a
   dev-dependency. The Dockerfiles (`asap-query-engine/Dockerfile`,
   `asap-quickstart/Dockerfile.queryengine-local`) point at the new crate.
   `precompute_engine/{csv_ingest,json_ingest,ingest_source}.rs` move too,
   unless they pull in precompute internals; then they move with the
   `IngestSource` rewiring (wiring step). No ASAPPlanner dependency is added
   and there is no behavior change; every test passes unchanged. Imports and
   `Cargo.toml` are the only changes, so later diffs to adapter signatures,
   wiring and the tracker each land in one crate. Optional follow-up:
   `pub(crate)` on engine internals, re-exporting only `engine_api` plus the
   old engines, stores and precompute types the adapters need until the
   deletions.
2. **`QueryRequest` replaces `ParsedQueryRequest`/`ParsedRangeQueryRequest`**
   in adapters, `HttpServer`, `FallbackClient` and `SimpleEngine`. Mechanical.
3. **`QueryResult` gains `Scalar` and `Table`**; adapters error on variants
   they don't support.
4. **Split `PrecomputeEngine` into pipeline + build specification (I9).** No
   behavior change; old tests pass unchanged.

Steps 1–4 don't use ASAPPlanner.

5. **Dependency and interfaces only.** ASAPPlanner as a git dependency pinned
   to a commit (`asap-types`, renamed to avoid clashing
   with ASAPQuery's `asap_types`, and `asap-planner`). A new module with only
   the traits, structs and enums of this doc, no implementations and no
   call-site changes.
6. **In-memory `ViewStore` and `RawDataStore`**, with unit tests.
7. **`QueryDispatcher` and `PlanInstaller`**, tested with fake engines
   (fallback rules, `NotReady`, no plan yet, install-once, error policy).
8. **Push `IngestEngine`**: plan → build specification (I8), shared kernels (decision 48),
   installer validation of supported operators.
9. **`QueryEngine` backed by ASAPPlanner's DataFusion engine** (I1): an adapter
   from `ViewStore`/`RawDataStore` to the engine's read interface, missing view
   instance → `NotReady` (I10), eviction (I17).
10. **Wiring**: `--engine`, `--plan-file`, `IngestSource` → `IngestEngine`,
   `OtlpReceiver` behind `IngestSource`, `handle_ingestion`; first end-to-end
   test (one PromQL instant query).
11. **Range queries and the remaining query types.**
12. **`QueryWorkloadTracker`** emits `PlanningWorkload` with the default
    `AccuracyTarget` (I5).
13. **I4 gate harness.**
14. **Deletions, one PR each:** `SimpleEngine`, `PrecomputeEngine`,
    asap-planner-rs, `PlannerClient`, `StreamingConfig`/`InferenceConfig`, the
    RQE endpoint.

`RuntimeInfoSource` joins the stack once its contents are decided (T4).

Implementation decisions:

- **I1. The `QueryEngine` wraps ASAPPlanner's DataFusion query-time engine**
  (`asap-query-time-batch-engine`, designed in ProjectASAP/ASAPPlanner#1468),
  not `asap-executor`, which is being retired. Its reads of stored views are
  served from `ViewStore` by downcast (I6). The traits let us replace it later
  (decision 1).
- **I7. The first `IngestEngine` is our own push-based streaming engine,**
  modeled on today's precompute engine (route series, per-window updaters in
  workers, seal into `ViewStore` when the watermark passes a window's end
  plus lateness). The executor's ingestion is pull-based: it refreshes each
  tumbling window once, after it closes, by reading the window's raw rows,
  so every sample would be buffered for a window even when the plan keeps no
  raw data. What to build comes from the plan's ingestion subgraphs instead of
  `AggregationConfig`. States are built with the shared kernel crate's factory
  (decision 48), so the query engine reads them without conversion. It writes to `RawDataStore` only
  what the plan's raw-data retention requires.
- **I8. Ingest-time operators the push engine runs (v1):** `Scan` of one
  `TimeSeries` source, `Filter`, `Project`, `Aggregate` (exact), and the
  sketch builds `SummaryAgg` and `SummaryTumblingWindowChain`. `Table`
  sources come later. The capabilities constant (decision 28) steers the
  planner to this subset: `excluded_candidates` excludes every other
  ingestion-time operator, and `supported_summaries` lists only the shared
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
- **I10. `NotReady` is detected by running, not by a pre-check.** The query
  engine reads only stored windows that exactly cover the requested range, and
  a missing one makes the read return `NotReady`; other engine errors are `Err`
  (I12). A pre-check would duplicate the engine's window arithmetic. In a range
  query, the first missing step makes the whole query `NotReady` (decision 36).
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
- **I15. Concurrency comes from the query engine's async runtime.** An earlier
  design ran a pool of engine threads because the executor's `RunContext`
  cannot move between threads. The DataFusion engine runs on tokio, so the
  dispatcher can run queries and batches concurrently.
- **I16. Late data: only `Drop`.** Samples later than the allowed lateness are
  dropped; a sealed window never changes. Every query run then reads a
  consistent store state, and each window has exactly one state, as the
  query engine's exact view reads require, with no snapshots or locking.
  `ForwardToStore` is tabled (T8).
- **I17. Only the query engine evicts, by time.** The ingest engine never
  evicts. The query engine tracks the latest evaluation time it has served
  (a range query's end) and, after each query, calls
  `evict_before(ViewId, latest_T − retention_ms)` per view and
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
  cost a decode on every query read (the query engine reads views as live
  sketch objects from the shared kernel crate; today's `Store` holds live
  objects too). Kernel types in the trait would tie the store to one engine. A durable store would need an
  engine encode method, added when durability is.

## Tabled

- **T1. Plan swap semantics.** When a new plan is installed, do we drain and
  rebuild, or run the old and new plans side by side until the new views
  cover the needed lookback? This affects `install` versus building engines per
  plan. v1 sidesteps this (I14). Reusing instances an old plan stored needs a
  plan-independent view id (decision 44; ProjectASAP/ASAPPlanner#1429).
- **T2. How a capability match is computed.** Ideally this is an ASAPPlanner API (ProjectASAP/ASAPPlanner#1430)
  (plan a query against the stored views and raw data, with no new
  materialization). Not in v1.
  It is not designed yet, so `CapabilityMatch` is a placeholder.
- **T3. Per-request query requirements.** The same query text with different
  accuracy requirements collides today (issue #778). Should `QueryRequest` carry
  requirements?
- **T4. `RuntimeInfoSource` contents.**
- **T5. `RawDataStore::read` predicates.** Whether it takes predicates (the
  query engine re-applies them, so a store may ignore them).
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

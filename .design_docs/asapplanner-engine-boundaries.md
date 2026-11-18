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
- `FallbackClient`: unchanged.
- Ingest decoders (Prometheus remote write, VictoriaMetrics, OTLP, CSV, JSON).

Changed:
- `ParsedQueryRequest {query, time}` and
  `ParsedRangeQueryRequest {query, start, end, step}` → one
  `QueryRequest {language, query, eval: Instant{time} | Range{start, end, step}}`.
  Times are `i64` ms; the adapters convert from protocol seconds.
- `QueryResult {Vector, Matrix}` → adds `Scalar` and `Table {columns, rows}`.
  The SQL response adapters (ClickHouse, Elastic) format `Table`. Today they
  flatten `Vector` into rows of label values plus one value, and return 501 for
  `Matrix`.
- `HttpProtocolAdapter::handle_runtime_info(Arc<dyn Store>)` → takes a
  `RuntimeInfoSource` instead of the store. Contents are tabled (T4).
- `IngestSource`: its `IngestContext` holds old-engine routing state
  (`SeriesRouter`, `AggregationConfig`-based routing configs). It is replaced by
  a handle to `IngestEngine`. `OtlpReceiver`, which bypasses `IngestSource`
  today, moves behind it.
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
    resolve(plan, &QueryRequest) -> Option<AnswerSource>;
}

struct QueryDispatcher { resolvers, engine }   // called by HttpServer for every query
// dispatch(&QueryRequest) -> QueryOutcome:
//   no resolver matches        → NotAnswerable(Unmatched)
//   engine → NotReady           → NotAnswerable(NotReady)
//   engine → Err(e)             → NotAnswerable(EngineError(e)) or Failed(e), per error policy
//   engine → Ready              → Ready

enum AnswerSource {
    Planned { root: RootRef },                          // matched InstallablePlan.query_texts
    CapabilityMatched { matched: CapabilityMatch },     // contents tabled (T2)
}

enum EngineQueryResult {
    Ready { labels: KeyByLabelNames, result: QueryResult },
    NotReady,                          // summaries don't yet cover the query → fallback
}

enum QueryOutcome {
    Ready { labels, result, source: AnswerSource },
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

Why `QueryDispatcher`: every query goes through the same three steps: resolve
it to an `AnswerSource`, execute it, and map the result to an answer, a fallback
or an error. Putting this in `HttpServer` would repeat it in every handler
(instant, range, RQE). Putting it in `QueryEngine` would repeat plan matching
and fallback policy in every engine implementation. The dispatcher does it once,
so engines only execute.

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

## Open

- **Q3. Error policy and `QueryEngineError` variants.** Does an engine error fall
  back (`NotAnswerable(EngineError)`) or fail (`Failed`)? Today it fails with a
  500. Does the policy need to tell "plan not executable by this engine" apart
  from "execution failed"? If not, a single error type is enough.

# ASAPQuery-backend audit

Audited the local repositories at their current revisions on 2026-08-28. The repositories share history through approximately 2026-04-10; afterward, ASAPQuery-backend developed a separate backend/platform track. ASAPQuery has since continued independently with query-engine and planner refactors.

The fork is not merely a modified query engine. It evolves ASAPQuery into a deployable, plan-driven observability backend with warm approximate summaries, exact archive fallback, durable storage, control-plane integration, and a Gorilla/Thanos ingestion path.

## Executive summary

The major additions are:

1. A new two-plane backend architecture:
   - `control_plane`: planning, optimization, sketch selection, configuration emission, monitoring.
   - `data_plane`: ingestion, summary storage, query execution, routing, persistence.

2. A warm summary backend:
   - DDSketch, KLL, HLL, CMS, CountSketch, heap-bearing variants.
   - Exact accumulator materializations for sum, count, increase, rate, and min/max.
   - Per-series and per-policy indexing.
   - Approximation/error metadata propagated to query responses.

3. A second, exact archive path:
   - Gorilla XOR fragments.
   - A new Go `gorilla-merger`.
   - Prometheus TSDB blocks.
   - Thanos StoreAPI and optional `thanos-query` forwarding.
   - Warm/archive routing and fallback.

4. Plan-driven runtime behavior:
   - A typed `BackendPlan` protobuf.
   - Controller-to-backend plan publication.
   - Atomic plan/config hot reload.
   - Backend-side materialization and routing decisions.

5. Durable and operational storage:
   - SID-based identity and lifecycle management.
   - Disk-backed SketchStore parts and manifests.
   - WAL/recovery.
   - Memory-bounded eviction.
   - Backfill APIs and lifecycle reconciliation.

6. Distributed monitoring and autonomous allocation:
   - Coordinator-based sampling.
   - F2/L2 monitoring work.
   - Runtime telemetry.
   - Epsilon-to-sketch-parameter allocation.
   - Automatic plan generation/replanning.

## 1. New architecture and deployment model

The fork adds a full `control_plane` crate and reorganizes the runtime into separate control/data responsibilities. The root workspace now contains `control_plane`, `data_plane`, `asap_types`, and `asap_otel_proto`.

The documented architecture is in [`README.md`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/README.md) and the implementation is centered in:

- [`control_plane/src/`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/control_plane/src)
- [`data_plane/src/`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src)

### Impact

This changes ASAPQuery from primarily a query/precompute component into a system component that can:

- receive plans from a controller;
- maintain summary materializations;
- route queries across storage tiers;
- manage runtime configuration;
- expose operational state;
- support multi-stage deployments.

The control plane is still transitional: the code contains substantial planner logic locally, while the current documentation describes eventual ownership by ASAPPlanner. The fork also pins multiple crates directly to specific ASAPPlanner commits, creating upgrade coupling.

## 2. BackendPlan and plan-driven execution

The fork adds a typed control-plane-to-backend contract:

- [`backend_plan.proto`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/control_plane/proto/backend_plan.proto)
- [`backend_plan/mod.rs`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/control_plane/src/backend_plan/mod.rs)

A `BackendPlan` contains:

- materializations;
- summary family and parameters;
- grouping and rollup;
- windows;
- storage backend;
- routing capabilities;
- retention information;
- monitor specifications.

The backend exposes:

- `GET/POST /api/v1/backend-plan`
- `GET/POST /api/v1/storage_routing`
- `GET/POST /api/v1/streaming-config`

See [`http.rs:451-508`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/query/servers/http.rs:451).

### Impact

Planning decisions can now be made once and consumed consistently by the backend. This reduces the need for the backend to infer sketch families or routing behavior from query text and local metadata.

It also creates new operational requirements:

- control plane and backend must agree on protobuf/schema versions;
- plan publication becomes part of deployment correctness;
- plan identity, materialization identity, and stored state must remain synchronized.

### Important limitation

The current `POST /api/v1/backend-plan` path decodes and atomically swaps the plan, but does not perform the full validation/lifecycle protocol described in the developer documentation. It does not, for example, visibly validate that every routing entry points to a compatible materialization before installation.

Also, BackendPlan installation and StreamingConfig installation are separate operations. The source explicitly states that the BackendPlan swap does not update SID lifecycle or SketchStore reconciliation; those remain on the StreamingConfig path ([`http.rs:5559-5568`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/query/servers/http.rs:5559)).

This creates a possible temporary mismatch between:

- the plan used for serving;
- the config used for ingestion;
- the materializations actually present in storage.

## 3. Warm summary tier

The fork adds a substantially expanded summary-backed engine and storage model.

Supported summary families include:

- DDSketch;
- KLL;
- HLL;
- Count-Min Sketch;
- CountSketch;
- heap-bearing CMS/CountSketch;
- exact accumulator variants.

The data-plane implementation is spread across:

- [`precompute_engine/`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/precompute_engine)
- [`query_engines/asap_query_engine/`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/query_engines/asap_query_engine)
- [`storage_engines/sketch_db/`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/storage_engines/sketch_db)

The fork also adds serving-time `SummaryExecutor` behavior, including:

- quantile/cardinality readout;
- exact aggregation readout;
- frequency estimates;
- top-k readout;
- range/per-window evaluation;
- coverage tracking;
- hybrid warm/archive stitching.

### Impact

The backend can answer planned query shapes with lower latency and bounded accuracy while preserving exact fallback for unsupported queries.

The main semantic change is that approximate and exact answers become first-class, rather than every query being treated as a conventional raw Prometheus query.

The implementation also distinguishes capabilities carefully. For example:

- bare selectors are not necessarily warm-tier answerable;
- exact quantile requests decline approximate sketches;
- rate/increase/sum can use particular exact accumulator materializations;
- top-k requires heap-bearing state for some paths.

## 4. New SID and policy identity model

The fork replaces the older aggregation-centric identity model with SID and policy-fingerprint concepts.

Key implementation:

- [`SketchInstanceMetadata`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/storage_engines/sketch_db/index/mod.rs:170)
- [`SketchStore`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/storage_engines/sketch_db/index/mod.rs:276)
- [`policy_fingerprint.rs`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/crates/asap_types/src/policy_fingerprint.rs)
- [`series_resolver.rs`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/ingest/series_resolver.rs)

The storage index now maintains:

- SID → metadata;
- SID → state;
- policy fingerprint → SIDs;
- metric name → SIDs;
- item-label metadata;
- lifecycle status.

It also introduces `Hit`, `Ghost`, and `Unknown` SID lookup states.

### Impact

This enables:

- stable identity across ingestion and query paths;
- multiple materializations per metric;
- query lookup without scanning every aggregation;
- stale-SID recovery;
- lifecycle-aware retirement and expiry;
- distinguishing registered-but-empty “ghost” series from unknown series.

This is a significant internal interface change. Code should no longer assume that `aggregation_id` alone identifies queryable state.

## 5. Durable SketchStore

The fork adds a disk-backed persistence layer:

- manifests;
- append/recovery records;
- sealed epochs;
- memory-mapped parts;
- part cache;
- background flushing;
- retention-based deletion;
- crash recovery.

See [`persistence/config.rs`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/storage_engines/sketch_db/persistence/config.rs:4).

The persistence design supports:

- memory high/low watermarks;
- hard caps;
- time-based flushing;
- disk TTL;
- part-cache budgets;
- recovery after restart.

### Impact

The warm tier no longer has to be purely memory-resident. This improves survivability and allows the backend to retain more history than RAM alone permits.

It also introduces substantial operational complexity:

- manifest and part compatibility;
- recovery correctness;
- eviction versus query readiness;
- disk-retention behavior;
- consistency during concurrent flush and query operations.

## 6. Gorilla archive and Thanos integration

The fork adds an independent Go component, [`gorilla-merger/`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/gorilla-merger), with:

- decode-free Gorilla fragment ingestion;
- block-level WAL;
- pending blocks;
- compacted/shipped blocks;
- Prometheus TSDB block creation;
- Thanos StoreAPI;
- object-store shipping;
- background compaction and re-chunking.

The design is summarized in [`gorilla-merger/README.md`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/gorilla-merger/README.md).

The Rust backend also adds a `ThanosQueryEngine` forwarder and storage-tier routing. `StorageBackend` now includes:

- `SketchStore`;
- `GorillaObjectStore`;
- `DoubleWrite`;
- `PrometheusRemote`.

See [`storage_backend.rs:19-70`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/storage_engines/types/storage_backend.rs:19).

### Impact

The backend now supports a two-tier serving model:

- warm summaries for planned low-latency approximate queries;
- archive data for exact or unsupported queries.

This gives the system a correctness escape hatch and a more complete PromQL surface.

The Gorilla merger also decouples:

- ingest visibility window;
- compaction span;
- object-store shipping.

That improves recent-data availability while retaining storage efficiency.

## 7. Query routing and external query behavior

The fork adds an `EngineRouter` abstraction and query-engine capabilities. HTTP queries can be influenced by:

- per-metric routing;
- query shape;
- storage backend;
- requested accuracy;
- engine override;
- tenant header.

New request controls include:

- `X-ASAP-Engine`;
- `?engine=...`;
- `X-ASAP-Tenant`.

These are defined in [`http.rs:93-140`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/query/servers/http.rs:93).

The normal Prometheus HTTP surface remains, but responses now include source/accuracy/latency annotations, according to the backend README.

### Important security implication

`X-ASAP-Tenant` is currently unauthenticated. The source explicitly acknowledges that any caller can select a tenant by setting the header ([`http.rs:112-125`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/query/servers/http.rs:112)).

This is not production-safe for multi-tenant deployment without an authentication/authorization layer that binds the tenant identity to the caller.

## 8. New ingestion interfaces

The fork adds modified OTLP ingestion over:

- OTLP gRPC;
- OTLP HTTP;
- gzip-compressed bodies.

It supports typed sketch payloads through the vendored [`asap_otel_proto`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/crates/asap_otel_proto) crate.

The receiver routes:

- raw points;
- pre-built sketches;
- sketch envelopes;
- delta payloads;

into the precompute worker pipeline.

See [`otel.rs:1-22`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/ingest/otel.rs:1).

### Impact

The backend is no longer dependent only on its original ingest paths. It can consume precomputed sketch representations emitted by upstream agents and preserve their metadata through to query readout.

This creates a cross-repository compatibility dependency among:

- ASAPCollector;
- asap_sketchlib;
- asap-precompute-rs;
- ASAPQuery-backend;
- control-plane protobufs.

## 9. Monitoring and autonomous allocation

The fork adds monitoring functionality including:

- distributed F2/L2 threshold monitoring;
- coordinator-to-edge sampling coupling;
- monitor hot reload;
- geometric safe zones;
- runtime telemetry;
- epsilon allocation;
- automatic plan generation;
- replanning based on violations or runtime observations.

Relevant code includes:

- [`control_plane/src/monitor/`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/control_plane/src/monitor)
- [`data_plane/src/monitor/`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/monitor)
- [`control_plane/src/epsilon_alloc.rs`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/control_plane/src/epsilon_alloc.rs)
- [`control_plane/src/replan.rs`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/control_plane/src/replan.rs)

### Impact

This moves the system toward adaptive operation: the backend/controller can react to observed workload and accuracy conditions rather than relying solely on static configuration.

The tradeoff is more distributed state and more complicated feedback-loop failure modes.

## 10. Backfill and operational APIs

The fork adds APIs for:

- schema inspection;
- SID retirement/expiry;
- timelines;
- backfill job creation;
- backfill job inspection/cancellation;
- precompute job registration/cancellation.

The routes are visible at [`http.rs:493-508`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/query/servers/http.rs:493).

### Important limitation

Precompute job registration is currently an acknowledgment/registry stub. The implementation says it stores job specifications in memory but does not schedule precompute work ([`http.rs:26-41`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/query/servers/http.rs:26)).

Similarly, the backfill documentation/source indicates that some jobs remain queued until the worker pool is fully wired.

These endpoints should therefore not be interpreted as proof that the requested work has completed.

## 11. Removed or retired functionality

The fork removes or retires several areas that remain present in, or have since been refactored differently in, current ASAPQuery.

### Query frontends

The fork removes:

- SQL frontend support;
- Elastic DSL frontend support;
- ClickHouse fallback paths;
- related SQL/Elastic utility crates.

The fork is explicitly PromQL-only.

### Legacy archive/fallback paths

The fork removes:

- JSONL cold fallback;
- `ColdJsonlFallback`;
- local filesystem JSONL query logic;
- the older in-process Gorilla/GORILLA1 read path;
- `GorillaS3Store`;
- the older `GorillaQueryEngine`.

The surviving archive path is Thanos-oriented.

### Legacy planner/query paths

The fork retires:

- the old `handle_query` family;
- legacy AST matching paths;
- the old standalone `asap-planner-rs` workspace member;
- duplicated planner IR layers;
- `promql_utilities` planner shims;
- old `StagedPlan`/L5 pipelines;
- old schema/aggregation-ID-centric lookup paths.

### Monitoring

The fork retires:

- Discipline B alerting;
- whole-sketch F2/geometric monitoring in its earlier form.

It retains or replaces parts of the monitoring system with the newer coordinator/GOS approach.

### Internal implementation cleanup

The fork removes:

- `shadow_compare.rs`;
- `sketch_reducer.rs`;
- several dead capability-matching and legacy IR modules.

## 12. External interface changes

The most important new or changed external interfaces are:

| Interface | Change |
|---|---|
| PromQL HTTP | Retained, but now routed across engines and enriched with source/accuracy metadata |
| `X-ASAP-Engine` / `engine` | Force a specific query engine |
| `X-ASAP-Tenant` | Select tenant-scoped routing; currently unauthenticated |
| OTLP gRPC/HTTP | Modified OTLP sketch variants and gzip support |
| `/api/v1/backend-plan` | Typed protobuf plan installation and inspection |
| `/api/v1/storage_routing` | Runtime routing-table installation |
| `/api/v1/streaming-config` | Hot-reloadable streaming configuration |
| `/api/v1/db/backfill*` | Backfill lifecycle APIs |
| `/api/v1/precompute/jobs*` | Precompute job registration/cancellation |
| Gorilla `/ingest/gorilla` | Gorilla fragment ingestion |
| Thanos StoreAPI | Queryable pending/shipped archive blocks |
| Control-plane backend client | Plan/config push, retry, and endpoint derivation |

## 13. Internal interface changes

The largest internal changes are:

- `QueryEngine` abstraction and `EngineRouter`;
- `AggregateCore` and accumulator factory traits;
- SID-based SketchStore APIs;
- `PolicyFingerprint` and `PolicyRegistry`;
- `RoutingIndex`;
- hot-reload handles based on `ArcSwap`;
- typed BackendPlan domain model;
- storage-engine and query-engine separation;
- durable `EpochSource`/part persistence interfaces;
- backfill reader/processor interfaces;
- SummaryExecutor and summary readout interfaces;
- control-plane emitter/backend-client interfaces.

These changes make the system more modular, but they also make the backend tightly coupled to specific versions of ASAPPlanner, ASAPCollector, and sketch libraries.

## 14. Audit findings and risks

### High priority

1. Default archive stub can return empty success

When no archive backend is configured, the binary installs `NoDataArchiveEngine` by default unless `ASAP_REQUIRE_ARCHIVE_ENGINE=1` is set ([`main.rs:821-841`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/main.rs:821)).

An unavailable archive can therefore appear as a successful empty result rather than an explicit failure. This is dangerous for dashboards and automation because “no data” becomes indistinguishable from “backend not configured.”

2. BackendPlan installation is not fully validated

The POST handler decodes and swaps a plan but does not enforce the full semantic validation/lifecycle contract before installation ([`http.rs:5591-5625`](/home/milind/Desktop/cmu/research/sketch_db_for_prometheus/code/ASAPQuery-backend/data_plane/src/drivers/query/servers/http.rs:5591)).

3. Plan and ingestion configuration are independently mutable

BackendPlan and StreamingConfig are installed through separate endpoints and affect different subsystems. A partial rollout can produce plan/config/materialization skew.

4. Multi-tenancy is not access-controlled

The tenant header is caller-controlled and unauthenticated. This must not be treated as tenant isolation.

### Medium priority

5. Precompute API is currently largely a control-plane compatibility stub

Registration succeeds, but the source says no precompute work is scheduled.

6. Current unit suite has a failing test

`cargo test --workspace --lib` compiled successfully but reported:

- 706 control-plane tests passed;
- 42 `asap_types` tests passed;
- one control-plane test failed:

`optimizer::rules::tests::invalid_sketch_type_override_falls_back_to_default`

The test expected an invalid `(HLL, Quantile)` override to fall back to DDSketch, but the implementation returned HLL. This indicates that invalid sketch-family overrides are not being rejected as intended.

7. Build warnings remain

The successful compilation emitted unused-import, unused-variable, and dead-code warnings in both control and data plane code. These are not necessarily functional defects, but they suggest the fork is still undergoing migration.

8. Go test could not be evaluated in this environment

`go test ./...` failed before compilation because the local `snap-confine` environment lacks required permissions. This is an environment failure, not evidence of a Go test failure.

## Bottom line

ASAPQuery-backend adds a backend platform around ASAPQuery:

- plan-aware summary execution;
- exact archive fallback;
- durable SID-indexed storage;
- Gorilla/Thanos integration;
- control-plane publishing;
- adaptive monitoring and allocation;
- richer operational APIs.

Its main architectural implication is that ASAPQuery-backend becomes a stateful, multi-tier, controller-managed service rather than a standalone query engine.

The largest concerns are not missing functionality but transitional correctness boundaries: plan/config atomicity, incomplete plan validation, stubbed operational APIs, unauthenticated tenant selection, and the default empty archive fallback.

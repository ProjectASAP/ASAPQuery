# Issue #242: Design Decisions Log

This document records the design questions and decisions that shaped the implementation plan for issue #242 (programmatic control of precompute engine pipelines). It is intended to explain *why* the plan looks the way it does, not just *what* it does.

---

## Problem Statement

`asap-query-engine` acts as a proxy between Grafana and Prometheus. Currently, the `precompute_engine` inside it must be given a `streaming_config` (which metrics to precompute and how) at **startup time** via a static config file.

The goal: start `asap-query-engine` with no precompute config, intercept queries from Grafana, call `asap-planner` on them to generate a `streaming_config` and `inference_config`, and then configure the running precompute engine with the result — no manual intervention required.

---

## What Already Exists

After exploring the codebase:

- **Query interception** already exists — the HTTP proxy in `drivers/query/servers/http.rs` already intercepts and can record every query.
- **Planner integration** already exists — `LocalPlannerClient` and `QueryTracker` already collect queries and call the planner on a periodic loop.
- **The planner output is discarded** — `tracker.rs` logs the result and throws it away. This is the core gap.
- **`precompute_engine` has no runtime reconfiguration** — `agg_configs` are built from `streaming_config` at startup inside `run()` and never touched again. There is no API to add, remove, or update pipelines.

---

## Decision 1: When to trigger planning

**Question:** "Once on startup" — but at startup there are no intercepted queries yet. What does the planner plan?

**Options considered:**
- (a) Call planner at startup with no queries, letting it do pure metric discovery from Prometheus.
- (b) Wait for the first observation window to elapse, collect real queries during that window, then plan once and configure.

**Decision: (b)** — wait for the first real observation window, plan with actual queries. More meaningful input to the planner. During the observation window, all queries fall through to Prometheus via the existing fallback mechanism, so users see no gap in Grafana.

**Future:** Subsequent observation windows will eventually trigger replanning (repeated reconfiguration). The design accommodates this without structural changes.

---

## Decision 2: One-shot vs. repeated loop

**Question:** After the first plan is applied, should the `QueryTracker` loop stop or keep running?

**Decision:** Keep the loop running, but only *apply* the config on the first successful plan (via an `AtomicBool applied` flag). This keeps the tracker alive for observability and makes it trivial to extend to repeated reconfiguration later — just remove the flag check.

---

## Decision 3: Runtime reconfiguration strategy (Option A vs. B)

**Question:** How should the engine be reconfigured at runtime?

**Option A — Lazy initialization (two-phase startup):**
- Start HTTP server with no engine; all queries fall back to Prometheus.
- After first window + plan, construct all components fresh from planner output.
- Install the new engine into the HTTP server via `Arc<RwLock<Option<QueryEngine>>>`.
- For *repeated* reconfiguration: tear down old engine and swap in a new one each time — store's accumulated sketch data is lost on every reconfiguration.

**Option B — Start with empty configs, hot-swap internals:**
- Start all components immediately with empty `streaming_config` / `inference_config`.
- After planning, update shared state in-place: push new `agg_configs` to workers via message, update `IngestState` via `RwLock`, update `SimpleEngine` and `SimpleMapStore` via `RwLock`.
- Workers are **not restarted** — they lazily pick up new aggregation configs as samples arrive.
- For *repeated* reconfiguration: same mechanism — update configs, workers adapt. Store's precomputed history is preserved across reconfigurations.

**Decision: Option B** — chosen because it is more amenable to future repeated reconfiguration. Option A loses all precomputed sketch data on every config update, which is wasteful once the engine has been running for multiple windows. Option B preserves historical precomputed data and allows incremental updates.

---

## Decision 4: Config replacement vs. merging

**Question:** When new planner output arrives, does it replace the existing config entirely, or merge with it?

**Decision: Replace** — the new planner output becomes the complete truth. However, the **input** to the planner includes the current `streaming_config` and `inference_config` (as `ControllerConfig.existing_*` fields), so the planner has the context it needs to make coherent decisions across windows.

This means: if a metric was being precomputed and the planner decides it still should be, it will appear in the new config. If it doesn't appear, it is dropped — data for that aggregation expires naturally via the existing cleanup policy (see Decision 8).

---

## Decision 5: Passing existing configs to the planner

**Question:** `ControllerConfig` (in `asap-planner-rs`) currently has no fields for existing configs. The planner only knows about new query observations. For repeated reconfiguration, the planner needs context about what is already running to make coherent decisions.

**Decision:** Add `existing_streaming_config: Option<StreamingConfig>` and `existing_inference_config: Option<InferenceConfig>` to `ControllerConfig` **now**, even though the planner does not yet use them. This wires the information through so the planner can use it in the future without a second round of type-signature changes. The fields are clearly marked with a `NOTE` comment.

---

## Decision 6: Who applies the planner result

**Question:** `QueryTracker` calls the planner but has no reference to the engine components. Who applies the result?

**Options considered:**
- (a) **Channel/callback** — tracker sends `PlannerResult` via a `tokio::sync::watch` channel; a separate task in `main.rs` owns the receiver and applies it to all engine components.
- (b) **Tracker owns engine handles** — pass `Arc` references of engine components into `QueryTracker`; it applies the result directly.

**Decision: (a)** — keeps `QueryTracker` decoupled. It only produces results; it does not know about engine internals. The applier task in `main.rs` is the single place that knows about all components and applies updates to them.

---

## Decision 7: Atomic vs. non-atomic config update

**Question:** `SimpleEngine` (inference) and `PrecomputeEngine` (streaming) are updated by separate calls. There is a brief window where one has the new config and the other does not.

**Decision:** Accept the brief inconsistency. During the transition window, a query might be matched by the old inference config against data computed by the new streaming config (or vice versa), which could produce a miss and fall back to Prometheus. This is acceptable — it is transient and self-correcting within one query cycle.

**Implementation note:** The inconsistency window is marked with a `NOTE` comment at the application site in `main.rs` and in `SimpleEngine`.

---

## Decision 8: Stale precomputed data after config replace

**Question:** When `streaming_config` is replaced, the `SimpleMapStore` may contain precomputed sketch data for aggregations that are no longer in the new config. Should this data be purged?

**Decision:** Let it expire naturally via the existing cleanup policy. No active purge.

**Rationale:** The cleanup policy already handles TTL-based eviction. Implementing an active purge would require iterating over potentially large store state and coordinating with in-progress queries. The natural expiry path is safe and requires no new code.

**Implementation note:** A `NOTE` comment in `update_streaming_config` on the store marks the alternative (active purge) for future reference.

---

## Decision 9: Worker update mechanism

**Question:** Workers each hold their own `HashMap<u64, Arc<AggregationConfig>>`. To give them new configs without restarting, the options are:
- Send `UpdateAggConfigs` messages via the existing `WorkerMessage` channel.
- Use `Arc<RwLock<HashMap<...>>>` shared state that workers read on every sample.

**Decision:** Message-passing (`WorkerMessage::UpdateAggConfigs`). This fits the existing actor-like architecture (workers already process typed messages) and avoids adding a lock acquisition on every sample's hot path.

Workers lazily create `WindowManager` instances the first time they see a new `agg_id`, so new aggregations are picked up automatically as samples arrive after the update — no special initialization needed.

---

## Decision 10: Kafka / OTLP consumers during observation window

**Question:** Kafka consumers and OTLP receivers also feed data into the precompute engine. With Option B (empty configs at startup), they will be active during the observation window but producing no useful work (no aggregation configs to match against).

**Decision:** No special handling needed. With empty `agg_configs`, the ingest handler simply drops all incoming samples (no matching aggregation found). Once the first plan is applied, subsequent samples are processed correctly. This is the natural behavior and requires no additional code.

---

## Summary of Key Structural Changes

| Component | Before | After |
|---|---|---|
| `streaming_config` in store/engine | `Arc<StreamingConfig>` | `Arc<RwLock<StreamingConfig>>` |
| `inference_config` in `SimpleEngine` | `InferenceConfig` (owned) | `Arc<RwLock<InferenceConfig>>` |
| `agg_configs` in `IngestState` | `Vec<Arc<AggregationConfig>>` | `Arc<RwLock<Vec<Arc<AggregationConfig>>>>` |
| Worker config updates | impossible | `WorkerMessage::UpdateAggConfigs` |
| Planner output | logged and discarded | sent via `watch` channel, applied by `main.rs` task |
| `PrecomputeEngine::run()` | creates channels internally, consumes self | channels created in `new()`, `handle()` extracted before `run()` |
| `ControllerConfig` | queries only | queries + `existing_streaming_config` + `existing_inference_config` |

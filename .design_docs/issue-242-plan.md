# Issue #242: Programmatic Control of Precompute Engine Pipelines

## Goal

Enable `asap-query-engine` to configure its precompute engine pipelines at runtime without manual intervention. Specifically: intercept queries from Grafana, call `asap-planner` on them after one observation window, and apply the resulting `streaming_config` + `inference_config` to the running engine — no static config files required.

**Scope for this PR:** trigger once, after the first observation window. Repeated reconfiguration is future work, but the design is intentionally amenable to it.

---

## Data Flow

```
Grafana → HTTP proxy → record query in QueryTracker
                    ↓ (all queries fall back to Prometheus during observation window)

[after observation_window_secs]

QueryTracker::evaluate()
  → reads current streaming_config + inference_config (empty on first run)
  → builds ControllerConfig (queries + existing configs)
  → PlannerClient::plan()
  → sends PlannerResult via watch::Sender<Option<PlannerResult>>
  → sets applied = true (won't send again until repeated-reconfig is implemented)

main.rs applier task (watches the receiver)
  → PrecomputeEngineHandle::update_streaming_config()
  → SimpleEngine::update_inference_config()
  → SimpleMapStore::update_streaming_config()
```

---

## Changes Required

### 1. `asap-planner-rs/src/config/input.rs` — `ControllerConfig`

Add two optional fields for existing configs. The planner ignores them for now; they are wired through so that future repeated-reconfig can pass the current state as context to the planner.

```rust
// NOTE: reserved for future repeated-reconfig — planner does not yet use these
pub existing_streaming_config: Option<StreamingConfig>,
pub existing_inference_config: Option<InferenceConfig>,
```

---

### 2. `query_tracker/tracker.rs` — `QueryTracker`

- Add `Arc<RwLock<StreamingConfig>>` and `Arc<RwLock<InferenceConfig>>` fields — read-only, used to populate `ControllerConfig.existing_*` before calling the planner.
- Add `applied: AtomicBool` — only sends the result the first time. The background loop keeps running (for observability / future extension) but subsequent results are dropped until repeated-reconfig is wired up.
- `start_background_loop` gains a `tokio::sync::watch::Sender<Option<PlannerResult>>` parameter.

```rust
pub struct QueryTracker {
    entries: Mutex<Vec<LogEntry>>,
    config: QueryTrackerConfig,
    streaming_config: Arc<RwLock<StreamingConfig>>,   // read-only reference
    inference_config: Arc<RwLock<InferenceConfig>>,   // read-only reference
    applied: AtomicBool,
}
```

On first successful plan: send `Some(result)` over the watch channel and set `applied = true`.

---

### 3. `precompute_engine/series_router.rs` — `WorkerMessage`

Add a new variant so workers can receive config updates without restarting:

```rust
pub enum WorkerMessage {
    // ... existing variants ...
    UpdateAggConfigs(HashMap<u64, Arc<AggregationConfig>>),
}
```

---

### 4. `precompute_engine/engine.rs` — `PrecomputeEngine` + new `PrecomputeEngineHandle`

Currently `run()` creates channels and workers internally and consumes `self`, making post-start updates impossible. Restructure:

- **Move channel creation to `new()`** — senders and receivers created at construction, stored on the struct.
- Add `PrecomputeEngineHandle`:

```rust
pub struct PrecomputeEngineHandle {
    worker_senders: Vec<mpsc::Sender<WorkerMessage>>,
    ingest_agg_configs: Arc<ArcSwap<Vec<Arc<AggregationConfig>>>>,
}

impl PrecomputeEngineHandle {
    /// Update the ingest handler's agg_configs and broadcast new configs to all workers.
    pub async fn update_streaming_config(&self, config: &StreamingConfig) {
        let agg_configs_map: HashMap<u64, Arc<AggregationConfig>> = config
            .get_all_aggregation_configs()
            .iter()
            .map(|(&id, cfg)| (id, Arc::new(cfg.clone())))
            .collect();
        let agg_configs_vec: Vec<Arc<AggregationConfig>> =
            agg_configs_map.values().cloned().collect();

        // Lock-free atomic swap — ingest handler readers see new configs immediately
        self.ingest_agg_configs.store(Arc::new(agg_configs_vec));

        for sender in &self.worker_senders {
            let _ = sender
                .send(WorkerMessage::UpdateAggConfigs(agg_configs_map.clone()))
                .await;
        }
    }
}
```

- `PrecomputeEngine::handle() -> Arc<PrecomputeEngineHandle>` — callable before `run()`.
- `run()` uses the handle's senders and `ingest_agg_configs` rather than creating its own.

---

### 5. `precompute_engine/ingest_handler.rs` — `IngestState`

```rust
// Before
agg_configs: Vec<Arc<AggregationConfig>>,

// After
agg_configs: ArcSwap<Vec<Arc<AggregationConfig>>>,
```

`IngestState` is behind `Arc<IngestState>` (immutable), so `agg_configs` needs interior mutability. `ArcSwap` is used because the ingest handler reads `agg_configs` on the hot path (once per request, iterates over all configs per sample). `ArcSwap::load()` is lock-free for readers; the applier does a single atomic pointer swap via `ArcSwap::store()`.

```rust
// Reading (hot path, per request):
let configs = state.agg_configs.load();
for config in configs.iter() { ... }

// Writing (once, from PrecomputeEngineHandle::update_streaming_config):
self.ingest_agg_configs.store(Arc::new(new_vec));
```

---

### 6. `precompute_engine/worker.rs` — handle `UpdateAggConfigs`

Workers process `WorkerMessage::UpdateAggConfigs` in their run loop:

```rust
WorkerMessage::UpdateAggConfigs(new_configs) => {
    self.agg_configs = new_configs;
}
```

Workers lazily create `WindowManager` instances the first time they see a new `agg_id`, so adding aggregations at runtime works without additional changes.

---

### 7. `engines/simple_engine/mod.rs` — `SimpleEngine`

`SimpleEngine` is already behind `Arc` at the call site, so no extra `Arc` wrapper is needed on the field — just a `RwLock` for interior mutability.

```rust
// Before
inference_config: InferenceConfig,

// After
inference_config: RwLock<InferenceConfig>,
```

Add update method:

```rust
pub fn update_inference_config(&self, new_config: InferenceConfig) {
    *self.inference_config.write().unwrap() = new_config;
}
```

> **NOTE:** `streaming_config` and `inference_config` are applied to their respective components independently, not atomically. There is a brief window where the precompute engine has a new `streaming_config` but `SimpleEngine` is still using the old `inference_config`. This is acceptable for the current use case.

---

### 8. `stores/simple_map_store/*.rs` — `SimpleMapStore` (all variants)

The store variants are behind `Arc` at the call site. The `RwLock` goes around the `Arc<StreamingConfig>` (not around the config itself) so that readers briefly lock to clone the Arc pointer, then use it without holding the lock during lookups.

```rust
// Before
streaming_config: Arc<StreamingConfig>,

// After
streaming_config: RwLock<Arc<StreamingConfig>>,
```

Add update method on the store trait and each implementation:

```rust
pub fn update_streaming_config(&self, new_config: StreamingConfig) {
    *self.streaming_config.write().unwrap() = Arc::new(new_config);
}
```

Readers clone the Arc cheaply under a brief shared lock, then look up aggregation configs from it without holding the lock:

```rust
let config = self.streaming_config.read().unwrap().clone();
config.get_aggregation_config(aggregation_id)
```

> **NOTE:** Precomputed data for aggregations removed by the new config is not purged immediately. It expires naturally via the existing cleanup policy. If immediate purge is ever needed, `update_streaming_config` is the right place to add it.

---

### 9. `main.rs` — wiring

```rust
// Start with empty configs
let streaming_config = Arc::new(RwLock::new(StreamingConfig::empty()));
let inference_config = Arc::new(RwLock::new(InferenceConfig::empty()));

// All components share Arc clones
let store    = SimpleMapStore::new(streaming_config.clone(), cleanup_policy);
let engine   = SimpleEngine::new(inference_config.clone(), streaming_config.clone(), ...);
let precompute = PrecomputeEngine::new(config, streaming_config.clone(), sink);
let pe_handle  = precompute.handle();   // extract before run() consumes self
tokio::spawn(precompute.run());

// Tracker reads current configs, sends first result via watch channel
let (plan_tx, mut plan_rx) = tokio::sync::watch::channel(None::<PlannerResult>);
let tracker = Arc::new(QueryTracker::new(
    tracker_config,
    streaming_config.clone(),
    inference_config.clone(),
));
tracker.start_background_loop(planner_client, plan_tx);

// Applier task: watches for first plan result and applies it
// NOTE: streaming_config and inference_config are not applied atomically — see SimpleEngine note
tokio::spawn(async move {
    loop {
        if plan_rx.changed().await.is_err() { break; }
        if let Some(result) = plan_rx.borrow().clone() {
            pe_handle.update_streaming_config(&result.streaming_config).await;
            engine.update_inference_config(result.inference_config);
            store.update_streaming_config(result.streaming_config);
        }
    }
});
```

---

## Files Touched

| File | Change |
|---|---|
| `asap-planner-rs/src/config/input.rs` | Add `existing_streaming_config`, `existing_inference_config` to `ControllerConfig` |
| `query_tracker/tracker.rs` | Add config refs, `AtomicBool`, accept watch sender in `start_background_loop` |
| `precompute_engine/series_router.rs` | Add `UpdateAggConfigs` variant to `WorkerMessage` |
| `precompute_engine/engine.rs` | Move channel creation to `new()`, add `PrecomputeEngineHandle`, expose `handle()` |
| `precompute_engine/ingest_handler.rs` | `agg_configs: ArcSwap<Vec<...>>` (lock-free reads on hot path) |
| `precompute_engine/worker.rs` | Handle `UpdateAggConfigs` message |
| `engines/simple_engine/mod.rs` | `RwLock<InferenceConfig>`, add `update_inference_config` |
| `stores/simple_map_store/*.rs` | `RwLock<Arc<StreamingConfig>>`, add `update_streaming_config` |
| `main.rs` | Wire watch channel, spawn applier task |

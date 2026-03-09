# Precompute Engine Design Document

## 1. Overview

The Precompute Engine is a real-time streaming aggregation system that sits between
metric producers and ASAP storage and query engine. It accepts raw
time-series samples via multiple ingestion connectors (Prometheus remote write
and VictoriaMetrics remote write), buffers them, computes windowed aggregations
(sketches), and writes the results to a store for fast query-time retrieval.

**Key properties:**
- Single-machine, multi-threaded architecture (all workers run as async tasks within one process)
- Watermark-based windowed aggregation (tumbling and sliding windows)
- Shared-nothing worker design: series are hash-partitioned across threads with no cross-worker coordination
- Pluggable accumulator types (Sum, Min/Max, Increase, KLL, CMS, HydraKLL)
- Configurable late-data handling (Drop or ForwardToStore)
- Optional raw passthrough mode for bypassing aggregation

## 2. Architecture

```
         Prometheus Remote Write       VictoriaMetrics Remote Write
          (Snappy + Protobuf)             (Zstd + Protobuf)
                  |                              |
                  v                              v
         POST /api/v1/write           POST /api/v1/import
                  \                            /
                   \                          /
                    Axum HTTP Server (:9090)
                            |
                  route_decoded_samples()
                   (group by series key)
                            |
                     SeriesRouter (hash)
                   /        |        \
              Worker 0   Worker 1   Worker 2  ...  Worker N-1
              (shard 0)  (shard 1)  (shard 2)      (shard N-1)
                 |           |           |              |
                 +---------- + --------- + ----------- +
                             |
                      OutputSink.emit_batch()
                             |
                          Store
                   (SimpleMapStore / PerKey)
                             |
                      Query Engine
                   (PromQL / SQL / etc.)
```

A periodic **flush timer** broadcasts `Flush` messages to all workers so that
windows that would otherwise remain open (no new samples arriving) are closed
and emitted.

## 3. Components

### 3.1 PrecomputeEngine (`mod.rs`)

Top-level orchestrator. On `run()`:

1. Creates one `mpsc::channel<WorkerMessage>` per worker.
2. Constructs a `SeriesRouter` with the sender halves.
3. Spawns `Worker` tasks, each owning its receiver.
4. Spawns a flush timer that calls `router.broadcast_flush()` every
   `flush_interval_ms`.
5. Starts the Axum HTTP server with routes for each ingest connector and blocks until shutdown.

```rust
pub struct PrecomputeEngine {
    config: PrecomputeEngineConfig,
    streaming_config: Arc<StreamingConfig>,
    output_sink: Arc<dyn OutputSink>,
}
```

### 3.2 Configuration (`config.rs`)

```rust
pub struct PrecomputeEngineConfig {
    pub num_workers: usize,              // default: 4
    pub ingest_port: u16,                // default: 9090
    pub allowed_lateness_ms: i64,        // default: 5,000
    pub max_buffer_per_series: usize,    // default: 10,000
    pub flush_interval_ms: u64,          // default: 1,000
    pub channel_buffer_size: usize,      // default: 10,000
    pub pass_raw_samples: bool,          // default: false
    pub raw_mode_aggregation_id: u64,    // default: 0
    pub late_data_policy: LateDataPolicy, // default: Drop
}

pub enum LateDataPolicy {
    Drop,            // Silently discard late samples for closed windows
    ForwardToStore,  // Emit a mini-accumulator for query-time merge
}
```

### 3.3 SeriesRouter (`series_router.rs`)

Deterministic hash-based routing using XXHash64:

```
worker_idx = xxhash64(series_key) % num_workers
```

All samples for a given series always land on the same worker, so per-series
state (buffer, watermark, active windows) needs no synchronization.

**Message types:**
```rust
enum WorkerMessage {
    Samples { series_key: String, samples: Vec<(i64, f64)>, ingest_received_at: Instant },
    Flush,
    Shutdown,
}
```

`route_batch()` groups messages by target worker and sends them in parallel for
throughput while preserving per-worker ordering.

### 3.4 Worker (`worker.rs`)

Each worker owns an isolated shard of the series space.

```rust
struct Worker {
    id: usize,
    receiver: mpsc::Receiver<WorkerMessage>,
    output_sink: Arc<dyn OutputSink>,
    series_map: HashMap<String, SeriesState>,
    agg_configs: HashMap<u64, AggregationConfig>,
    max_buffer_per_series: usize,
    allowed_lateness_ms: i64,
    pass_raw_samples: bool,
    raw_mode_aggregation_id: u64,
    late_data_policy: LateDataPolicy,
}
```

**Per-series state:**
```rust
struct SeriesState {
    buffer: SeriesBuffer,                      // sorted sample buffer
    previous_watermark_ms: i64,                // last-seen watermark
    aggregations: Vec<AggregationState>,       // one per matching config
}

struct AggregationState {
    config: AggregationConfig,
    window_manager: WindowManager,
    active_panes: BTreeMap<i64, Box<dyn AccumulatorUpdater>>,
}
```

#### Accumulator lifecycle and ownership

Accumulators are not pre-assigned — they are created **lazily** at three nested levels:

**1. At engine startup** (`engine.rs`): every worker receives a full copy of all `AggregationConfig`s. All workers are symmetric; none is pre-assigned to any series or config.

```rust
let agg_configs = streaming_config.get_all_aggregation_configs().clone();
for (id, rx) in receivers {
    Worker::new(id, rx, sink.clone(), agg_configs.clone(), ...)
}
```

**2. On first sample for a series** (`get_or_create_series_state`): the worker calls `matching_agg_configs(series_key)` to filter the config map by metric name, then creates one `AggregationState` per match (a `WindowManager` + empty pane map). No accumulators exist yet.

```rust
let aggregations = matching_agg_configs(series_key).map(|(_, config)| AggregationState {
    window_manager: WindowManager::new(config.window_size, config.slide_interval),
    config: config.clone(),
    active_panes: BTreeMap::new(),   // ← empty; no memory allocated for sketches yet
}).collect();
```

**3. On first sample in a pane** (`process_samples`): the accumulator is created the moment a sample falls into a pane that does not yet exist in `active_panes`.

```rust
let updater = agg_state.active_panes
    .entry(pane_start)
    .or_insert_with(|| create_accumulator_updater(&agg_state.config));
```

**Ownership hierarchy:**

```
Worker
└── series_map[series_key]           one entry per series this worker owns
    └── aggregations[i]              one AggregationState per matching config
        └── active_panes[pane_start] one AccumulatorUpdater per open pane
            └── Box<dyn AggregateCore>   the actual sketch / sum / minmax / etc.
```

Because `xxhash64(series_key) % N` is deterministic, a series always lands on the same worker. Its accumulators live in exactly one worker with no sharing and no locking. Workers that never receive a series never allocate any state for it.

#### Pane-Based Sliding Window Optimization

The worker uses **pane-based incremental computation** to reduce per-sample
work for sliding windows. The timeline is divided into non-overlapping **panes**
of size `slide_interval`. Each window is composed of `W = window_size /
slide_interval` consecutive panes. Consecutive windows share W-1 panes.

```
Panes:     [0,10)  [10,20)  [20,30)  [30,40)  [40,50)
Window A:  [───────── 0,30 ─────────)
Window B:          [───────── 10,40 ─────────)
Window C:                  [───────── 20,50 ─────────)
```

**Why panes instead of subtraction?** Only Sum is invertible. MinMax, Increase,
KLL, CMS, HydraKLL are all non-invertible. Pane+merge works universally because
all accumulator types implement `AggregateCore::merge_with()`.

**Performance comparison** (N samples per window, W = window_size / slide_interval):

| | Per-window approach | Pane-based |
|--|---------|------------|
| Per-sample accumulator updates | N × W | N × 1 |
| Per-window-close merges | 0 | W - 1 |
| Per-window-close clones | 0 | W - 2 (shared panes) |

Net win when N >> W (typical: thousands of samples per window, W = 3-5).
For tumbling windows (W=1), panes degenerate to 1 pane = 1 window with
zero merges — identical behavior to the non-pane approach.

**Key methods:**

| Method | Description |
|--------|-------------|
| `pane_start_for(ts)` | Align timestamp to slide grid (same as `window_start_for`) |
| `panes_for_window(ws)` | All pane starts composing window `[ws, ws+size)` |
| `snapshot_accumulator()` | Non-destructive read of a pane's accumulator (for shared panes) |

**Pane eviction:** When window `[S, S+W)` closes, pane `[S, S+slide)` is the
oldest pane and is not needed by any later window (next window starts at
`S+slide`). It is destructively consumed via `take_accumulator()` and removed
from `active_panes`. Remaining panes are read non-destructively via
`snapshot_accumulator()`.

#### Processing pipeline (`process_samples`)

```
1. Match series to AggregationConfigs (by metric name / spatial_filter)
2. Insert samples into SeriesBuffer, update watermark
3. Drop samples beyond allowed_lateness_ms behind watermark
4. For each sample × each aggregation:
   a. Compute pane_start = pane_start_for(ts)
   b. If pane was evicted (late data for closed window):
      → late_data_policy == Drop:           skip
      → late_data_policy == ForwardToStore:  create mini-accumulator, emit
   c. Else: get-or-create pane in active_panes, feed value (1 update per sample)
5. Detect newly closed windows via closed_windows(prev_wm, current_wm)
6. For each closed window:
   a. Get pane starts via panes_for_window(window_start)
   b. Oldest pane: take_accumulator() + remove from active_panes (destructive)
   c. Remaining panes: snapshot_accumulator() (non-destructive, shared)
   d. Merge all pane accumulators via AggregateCore::merge_with()
   e. Emit merged result as PrecomputedOutput + AggregateCore
7. Emit batch to OutputSink
8. Update previous_watermark_ms
```

#### Raw mode

When `pass_raw_samples = true`, the entire aggregation pipeline is bypassed.
Each sample is emitted as a `SumAccumulator::with_sum(value)` with point-window
bounds `[ts, ts]` and the configured `raw_mode_aggregation_id`.

### 3.5 SeriesBuffer (`series_buffer.rs`)

Per-series in-memory buffer backed by `BTreeMap<i64, f64>`.

```rust
struct SeriesBuffer {
    samples: BTreeMap<i64, f64>,   // timestamp_ms → value
    watermark_ms: i64,              // max timestamp ever seen (monotonic)
    max_buffer_size: usize,
}
```

- Samples are automatically sorted by timestamp.
- Watermark only advances forward (monotonic).
- When the buffer exceeds `max_buffer_size`, the oldest samples are evicted.
- Supports range reads (`read_range`) and destructive drains (`drain_up_to`).

### 3.6 WindowManager (`window_manager.rs`)

Handles both tumbling and sliding window semantics.

```rust
struct WindowManager {
    window_size_ms: i64,       // e.g. 60_000
    slide_interval_ms: i64,    // == window_size for tumbling; < window_size for sliding
}
```

**Key methods:**

| Method | Description |
|--------|-------------|
| `window_start_for(ts)` | Align timestamp down to nearest slide boundary |
| `window_starts_containing(ts)` | All windows whose `[start, start+size)` includes `ts`. Tumbling → 1 window; sliding → `ceil(size/slide)` windows |
| `closed_windows(prev_wm, curr_wm)` | Windows that transitioned open→closed as the watermark advanced |
| `window_bounds(start)` | Returns `(start, start + window_size_ms)` |
| `pane_start_for(ts)` | Pane start for a timestamp (same slide-aligned grid as `window_start_for`) |
| `panes_for_window(ws)` | All pane starts composing window `[ws, ws+size)`, in ascending order |
| `slide_interval_ms()` | Slide interval accessor |

**Window closure rule:** a window `[S, S + size)` closes when `watermark >= S + size`.
Once closed, a window never reopens.

#### Sliding window mechanics

Tumbling windows are a special case of sliding windows where
`slide_interval == window_size`. The same code handles both — no separate paths.

**`window_start_for(ts)`** aligns a timestamp to the slide grid:
```rust
let n = timestamp_ms.div_euclid(slide_interval_ms);
n * slide_interval_ms
```

**`window_starts_containing(ts)`** returns all windows whose `[start, start+size)`
contains the timestamp, by walking backwards from the aligned start:
```rust
let mut start = window_start_for(timestamp_ms);
while start + window_size_ms > timestamp_ms {
    starts.push(start);
    start -= slide_interval_ms;
}
```

For tumbling windows this always yields exactly 1 result. For sliding windows,
each sample belongs to `ceil(window_size / slide_interval)` overlapping windows.

**Example** (30s window, 10s slide):
```
t=15s → belongs to windows [0, 30s), [10s, 40s), [-10s, 20s)   (3 windows)
t=35s → belongs to windows [30s, 60s), [20s, 50s), [10s, 40s)  (3 windows)
```

**`closed_windows(prev_wm, curr_wm)`** finds windows that transitioned open→closed
as the watermark advanced. It scans forward from the earliest possibly-open window
start, collecting those where `start + size <= curr_wm` (now closed) AND
`start + size > prev_wm` (was still open before).

The worker calls `window_starts_containing(ts)` for each incoming sample and feeds
the value into the accumulator for every matching window. When
`closed_windows()` fires, each closed window's accumulator is extracted and
emitted independently.

### 3.7 AccumulatorUpdater (`accumulator_factory.rs`)

Trait-based interface for feeding samples into sketch accumulators:

```rust
trait AccumulatorUpdater: Send {
    fn update_single(&mut self, value: f64, timestamp_ms: i64);
    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp_ms: i64);
    fn take_accumulator(&mut self) -> Box<dyn AggregateCore>;
    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore>;  // non-destructive clone
    fn reset(&mut self);
    fn is_keyed(&self) -> bool;
    fn memory_usage_bytes(&self) -> usize;
}
```

`snapshot_accumulator()` returns a clone of the current state without resetting.
Used by pane-based sliding windows to read shared panes that are still needed
by future windows.

The factory function `create_accumulator_updater(config)` dispatches on
`(aggregation_type, aggregation_sub_type)`:

| Type | Sub-type | Updater |
|------|----------|---------|
| SingleSubpopulation | Sum | SumAccumulatorUpdater |
| SingleSubpopulation | Min/Max | MinMaxAccumulatorUpdater |
| SingleSubpopulation | Increase | IncreaseAccumulatorUpdater |
| SingleSubpopulation | KLL | KllAccumulatorUpdater |
| MultipleSubpopulation | Sum | MultipleSumUpdater |
| MultipleSubpopulation | Min/Max | MultipleMinMaxUpdater |
| MultipleSubpopulation | Increase | MultipleIncreaseUpdater |
| MultipleSubpopulation | CMS | CmsAccumulatorUpdater |
| MultipleSubpopulation | HydraKLL | HydraKllAccumulatorUpdater |

### 3.8 OutputSink (`output_sink.rs`)

```rust
trait OutputSink: Send + Sync {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
```

**Implementations:**
- `StoreOutputSink` — calls `store.insert_precomputed_output_batch()`
- `RawPassthroughSink` — same interface, used for raw mode
- `NoopOutputSink` — testing helper that counts emitted items via `AtomicU64`
- `CapturingOutputSink` — testing helper that stores all emitted `(PrecomputedOutput, Box<dyn AggregateCore>)` pairs in a `Mutex<Vec<...>>`, with `drain()` and `len()` for assertions

## 4. Cross-Series (GROUP BY) Aggregation

### Label dimension roles

`AggregationConfig` has three label dimension fields that control the spatial aggregation shape:

| Field | Role |
|---|---|
| `grouping_labels` | Labels preserved in `PrecomputedOutput.key`; form the GROUP BY key visible at query time |
| `aggregated_labels` | Internal sub-keys for MultipleSubpopulation sketches (e.g. CMS, HydraKLL) |
| `rollup_labels` | Dropped entirely at ingest; not recoverable at query time |

A config with `grouping_labels: [job]` and `rollup_labels: [instance]` means: "aggregate across all instances, keep one output series per job value." Multiple input series (`metric{job=j1,instance=h1}`, `metric{job=j1,instance=h2}`, ...) all contribute to the same logical output key `(job=j1)`.

### Cross-worker fan-in

Because routing is by full series key (`xxhash64(series_key) % N`), two series that share a `grouping_labels` value but differ in rolled-up labels typically land on different workers:

```
metric{job=j1, instance=h1} → Worker 0 → pane accumulator with key (job=j1)
metric{job=j1, instance=h2} → Worker 3 → pane accumulator with key (job=j1)
```

Each worker independently closes its window and emits a separate `PrecomputedOutput` with key `(job=j1)` for the same window `[0, 60s)`. The store **appends** rather than overwrites on the same `(aggregation_id, key, window)` tuple:

```
store[(agg_id, key=(j1), [0,60s))] → [acc_worker0, acc_worker3]
```

Query-time `SummaryMergeMultipleExec` merges all entries for the same key and window via `AggregateCore::merge_with()`. No ingest-time cross-worker coordination is needed.

### Eventual consistency

Workers have independent watermarks. For a standard Prometheus scrape (all instances delivered in one HTTP batch via `route_batch()`), all workers receive their samples in the same round-trip and close the window on the same flush cycle. The incompleteness window — time between the first and last worker emitting for the same cross-series window — is typically milliseconds (bounded by Tokio task scheduling jitter).

For staggered multi-source producers arriving at different times, the incompleteness window is bounded by the spread of producer arrival times. In both cases the result is **eventually consistent**: once all contributing workers have emitted, the store holds a complete set of accumulators and queries return the correct merged value.

This deferred-merge design is intentional — it preserves the shared-nothing worker architecture with zero ingest-time cross-worker coordination. The store's append-multiple-per-window design and the query-time merge handle the fan-in correctly for both cross-series aggregation and `ForwardToStore` late data.

### Sliding windows with cross-worker GROUP BY

The pane-sharing optimization is an **intra-worker** implementation detail. From the store and query engine's perspective, each worker always emits a complete, self-consistent accumulator for each closed window — tumbling or sliding makes no difference to the cross-worker fan-in.

**Within a single worker** (e.g. Worker 0, series `{job=j1, instance=h1}`, 30s/10s sliding):

```
Window [0, 30s)  — panes [0, 10s, 20s]
  pane 0:   take (evict — no future window needs it)
  pane 10s: snapshot (shared with [10s, 40s))
  pane 20s: snapshot (shared with [10s, 40s) and [20s, 50s))
  → emit: acc_w0, key=(j1), window=[0,30s), sum = v_0 + v_10 + v_20

Window [10s, 40s)  — panes [10s, 20s, 30s]
  pane 10s: take (evict — snapshot for [0,30s) already completed)
  pane 20s: snapshot
  pane 30s: snapshot (or take, depending on future windows)
  → emit: acc_w0, key=(j1), window=[10s,40s), sum = v_10 + v_20 + v_30
```

Worker 3 (series `{job=j1, instance=h2}`) performs the same steps independently — its own pane `BTreeMap`, its own snapshots, its own emits.

**What the store sees:**

```
store[(agg_id, key=(j1), [0,  30s))] → [acc_w0, acc_w3]
store[(agg_id, key=(j1), [10s,40s))] → [acc_w0, acc_w3]
store[(agg_id, key=(j1), [20s,50s))] → [acc_w0, acc_w3]
```

Each entry is a complete accumulator from one worker for one window. Query-time merge combines them identically to the tumbling case.

The pane snapshot/take logic reduces memory and CPU inside each worker (avoiding re-accumulation of shared panes), but what exits the worker is always one standalone `Box<dyn AggregateCore>` per window. Consecutive sliding windows `[0,30s)` and `[10s,40s)` share panes *inside* the worker but have independent store entries — their cross-worker merges at query time are completely unrelated.

## 5. Data Model

### PrecomputedOutput

```rust
pub struct PrecomputedOutput {
    pub start_timestamp: u64,              // window start (ms)
    pub end_timestamp: u64,                // window end (ms)
    pub key: Option<KeyByLabelValues>,     // grouping key (e.g. method="GET")
    pub aggregation_id: u64,
}
```

### KeyByLabelValues

Ordered vector of label values matching the `grouping_labels` in the aggregation
config. Serialized as semicolon-delimited strings for hashing/storage.

### AggregationConfig

Loaded from `streaming_config.yaml`:

```rust
pub struct AggregationConfig {
    pub aggregation_id: u64,
    pub aggregation_type: String,        // "SingleSubpopulation" | "MultipleSubpopulation"
    pub aggregation_sub_type: String,    // "Sum" | "Min" | "Max" | "Increase" | "KLL" | ...
    pub parameters: HashMap<String, Value>,
    pub grouping_labels: KeyByLabelNames,
    pub window_size: u64,                // seconds
    pub slide_interval: u64,             // seconds (0 = tumbling)
    pub metric: String,
    pub spatial_filter: String,
    pub num_aggregates_to_retain: Option<u64>,
    // ...
}
```

## 6. Store Integration

### Write path

`OutputSink.emit_batch()` → `Store.insert_precomputed_output_batch()`

Because the precompute engine runs in the same process as the store, the write
path involves **zero serialization and zero network hops**. Closed window
accumulators flow from worker to store entirely as in-memory trait objects:

```
Worker: updater.take_accumulator()     → Box<dyn AggregateCore>  (in-memory)
   ↓  (direct function call, no IPC)
OutputSink: store.insert_precomputed_output_batch(outputs)  (pass-through)
   ↓  (direct function call, same process)
SimpleMapStore: HashMap entry insert   → Box<dyn AggregateCore>  (stored as-is)
```

No serialization, deserialization, compression, or network transfer occurs
between the worker extracting an accumulator and the store persisting it.
The only network hops in the system are at the edges: HTTP ingest (in) and
HTTP query (out). Serialization of accumulators only happens on the read path
when query results are returned to clients.

This is in contrast to the external Kafka ingest path, where precomputes from
Arroyo/Flink arrive hex-encoded + gzip-compressed + MessagePack-serialized and
require multiple deserialization steps.

The `SimpleMapStore` (PerKey variant) uses:
```
DashMap<aggregation_id, Arc<RwLock<StoreKeyData>>>
```
where:
```rust
struct StoreKeyData {
    time_map: HashMap<(u64, u64), Vec<(Option<KeyByLabelValues>, Box<dyn AggregateCore>)>>,
    read_counts: HashMap<(u64, u64), u64>,
}
```

Multiple entries per `(start_ts, end_ts)` are allowed — they are appended, not
overwritten. This is what makes `ForwardToStore` late-data policy work: the late
mini-accumulator is stored alongside the original window accumulator.

### Read path / query-time merge

At query time, `PrecomputedSummaryReadExec` reads sparse buckets from the store.
`SummaryMergeMultipleExec` groups by label key and merges via
`accumulator.merge_with()`.

The `NaiveMerger` re-merges all accumulators in the window on each slide.
The store's existing multi-entry-per-window design means late data is
automatically combined with original window data at query time.

### Cleanup policies

| Policy | Behavior |
|--------|----------|
| CircularBuffer | Keep N most recent windows (4x `num_aggregates_to_retain`) |
| ReadBased | Remove after `read_count >= threshold` |
| NoCleanup | Retain forever |

## 7. Late Data Handling

Two checks determine whether a sample is "late":

1. **Watermark check** (sample-level): `ts < watermark - allowed_lateness_ms` →
   sample is dropped entirely before reaching any aggregation logic.

2. **Window closure check** (window-level): the sample passes the watermark check
   but targets a window that is already closed
   (`window not in active_windows && watermark >= window_end`).

For case 2, the `LateDataPolicy` controls behavior:

- **Drop**: log at debug level and skip. No ghost accumulator is created
  (fixing the original bug where `or_insert_with` would create orphaned entries).

- **ForwardToStore**: create a fresh `AccumulatorUpdater`, feed the single
  late sample, wrap as `PrecomputedOutput`, and push into the same `emit_batch`
  as normal closed-window outputs. The store appends it alongside the original
  window data, and query-time merge combines them.

## 8. Concurrency Model

The current implementation is **single-machine, multi-threaded**. All components
(HTTP server, workers, store) run within a single OS process as Tokio async
tasks on a shared thread pool. There is no distributed coordination, no
cross-machine communication, and no external dependency beyond the store.

- **Ingest HTTP handlers**: Per-connector Axum async handlers (Prometheus, VictoriaMetrics) with shared format-agnostic routing logic.
- **SeriesRouter**: Lock-free hash routing. No shared mutable state.
- **Workers**: Each worker is a single Tokio task that owns its `series_map`
  exclusively. No locks needed within a worker — thread safety comes from
  the hash-partitioning guarantee that each series is assigned to exactly one
  worker.
- **OutputSink / Store**: Thread-safe (`Arc<dyn OutputSink>`, DashMap-backed store).
  Workers emit concurrently; the PerKey store uses per-aggregation_id RwLocks
  to minimize contention.
- **Flush timer**: Separate Tokio task, communicates via the same MPSC channels.

Scaling beyond a single machine would require partitioning the series space
across multiple engine instances (e.g. via consistent hashing at the load
balancer level), each running this same single-process architecture
independently.

## 9. Performance Characteristics

**Ingest path (per batch):**
- Sample insert: O(log B) per sample (BTreeMap, B = buffer size)
- Pane routing: O(A) per sample (A = matching aggregations; each sample
  touches exactly 1 pane per aggregation, regardless of window overlap)
- Accumulator update: O(1) for Sum/MinMax, O(log k) for KLL
- Window close: O(W-1) merges per closed window (W = window_size / slide_interval)

**Memory:**
- O(S × N) buffered samples (S = max per series, N = active series)
- O(A × W_open) active pane accumulators (fewer than window accumulators
  since panes are shared across overlapping windows)

**Throughput (measured, 2× Xeon E5-2630 v3, 32 logical CPUs, 125 GiB RAM):**
- Raw mode, `NoopOutputSink`, 16 workers: **~8.9M samples/sec** flush throughput; near-linear scaling (19× at 16 workers vs 16× ideal).
- Windowed aggregation (Sum, W=1–6), 4 workers: **~660K samples/sec** E2E; throughput is nearly identical across W=1 and W=6, confirming the pane-based optimization.
- Workers process in parallel with no cross-shard coordination.

**Benchmark caveat — `workers = senders` coupling:** The raw-mode scalability benchmark uses one concurrent HTTP sender per worker. The 1-worker baseline is bottlenecked by a single sender (one CPU for Snappy compression, one in-flight HTTP connection); at 16 workers, 16 senders parallelize compression across cores and pipeline connections. The apparent super-linear speedup (9.37× at 8 workers, 19.13× at 16 workers) reflects sender-side parallelism as much as engine-side scaling. A clean engine-scaling measurement would fix sender count and vary only worker count.

## 10. CLI Usage

### Standalone binary

```bash
cargo run --bin precompute_engine -- \
  --streaming-config streaming_config.yaml \
  --ingest-port 9090 \
  --num-workers 4 \
  --allowed-lateness-ms 5000 \
  --max-buffer-per-series 10000 \
  --flush-interval-ms 1000 \
  --channel-buffer-size 10000 \
  --query-port 8080 \
  --lock-strategy per-key \
  --late-data-policy drop
```

### Embedded in main binary

The precompute engine is also embedded in the main `query_engine_rust` binary,
enabled via `--enable-prometheus-remote-write`. In this mode it shares the
store with the Kafka consumer path.

## 11. Testing

- **Unit tests — `worker.rs` (correctness, via `CapturingOutputSink`):**

  | Test | What it verifies |
  |---|---|
  | `test_raw_mode_forwarding` | 3 samples → 3 emits; `start == end == ts`, `SumAccumulator.sum == value` |
  | `test_tumbling_window_correctness` | Samples at t=1s/5s/9s; window [0,10s) closes on t=10s; `sum=6` |
  | `test_sliding_window_pane_sharing` | Sample at t=15s in 30s/10s window → 2 emits for [0,30s) and [10s,40s), both `sum=42` via shared pane snapshot/take |
  | `test_groupby_separate_emits_per_series` | Two series (`host=A`, `host=B`) on same worker → 2 independent `MultipleSumAccumulator` emits (no ingest-time cross-series merge) |
  | `test_late_data_drop` | Sample behind `watermark - allowed_lateness_ms` with `Drop` policy → 0 emits |
  | `test_late_data_forward_to_store` | Late sample for evicted pane with `ForwardToStore` → 1 emit as mini-accumulator with correct window bounds and sum |

- **Unit tests — other modules**: `window_manager.rs` (tumbling/sliding arithmetic, pane enumeration, closure detection), `series_buffer.rs` (ordering, watermark), `accumulator_factory.rs` (updater creation and reset), `series_router.rs` (consistent hash routing), `config.rs` (defaults).

- **E2E test** (`bin/test_e2e_precompute.rs`): Starts engine + store + query
  server in-process, sends remote-write samples over HTTP, queries via PromQL HTTP,
  validates aggregated results. Includes batch latency benchmark, windowed throughput
  benchmark (W=1/3/6), and worker scalability benchmark (1–16 workers).

## 12. File Map

| File | Purpose |
|------|---------|
| `precompute_engine/mod.rs` | Orchestrator, HTTP ingest handler |
| `precompute_engine/config.rs` | `PrecomputeEngineConfig`, `LateDataPolicy` |
| `precompute_engine/worker.rs` | Per-shard processing, aggregation, window management |
| `precompute_engine/series_router.rs` | Hash-based series → worker routing |
| `precompute_engine/series_buffer.rs` | Per-series BTreeMap sample buffer |
| `precompute_engine/window_manager.rs` | Tumbling/sliding window logic |
| `precompute_engine/accumulator_factory.rs` | `AccumulatorUpdater` trait + factory |
| `precompute_engine/output_sink.rs` | `OutputSink` trait + `StoreOutputSink`, `NoopOutputSink`, `CapturingOutputSink` (testing) |
| `bin/precompute_engine.rs` | Standalone CLI binary |
| `bin/test_e2e_precompute.rs` | End-to-end integration test |

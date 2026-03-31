# Precompute Engine Design Document

## 1. Overview

### Why this PR is needed

ASAPQuery already has a query path over precomputed summaries, but before this PR
there was no standalone runtime inside `asap-query-engine` that could continuously:

- accept raw metric samples,
- turn them into windowed precomputed outputs, and
- write those outputs into the same store that the query engine reads.

PR #228 fills that gap by introducing a first working version of a **precompute
engine**. The engine runs as a separate binary, accepts Prometheus remote write
traffic, partitions incoming series across workers, computes windowed
accumulators, and stores the results for later query-time retrieval.

### Why not ArroyoSketch?

The existing precompute path — **ArroyoSketch** (`asap-summary-ingest/run_arroyosketch.py`)
— already performs windowed sketch aggregation, but it does so through an
entirely separate operational stack:

| Dimension | ArroyoSketch | Precompute Engine (this PR) |
|---|---|---|
| **Runtime** | External Arroyo cluster (separate process, separate binary) | In-process Rust binary alongside the query engine |
| **Orchestration language** | Python (Jinja2 SQL templates deployed via REST API to Arroyo) | Native Rust, driven directly by `StreamingConfig` |
| **Ingest transport** | Kafka topic or Prometheus remote write → Arroyo pipeline | Prometheus remote write directly to the engine |
| **Output transport** | Kafka topic → consumed by a separate pipeline stage | Direct write to the store already read by the query engine |
| **Operational dependencies** | Arroyo cluster + Kafka brokers must be running and healthy | None beyond the query engine process itself |
| **Configuration coupling** | Arroyo pipeline SQL is rendered from `streaming_config.yaml` by a Python script; any config change requires re-deploying pipelines via the Arroyo REST API | Engine reads `StreamingConfig` directly at startup; same structs used throughout `asap-query-engine` |
| **Failure boundary** | Arroyo crash or Kafka lag is invisible to the query engine until queries begin returning stale results | Precompute workers and query engine share the same process and store; failures surface immediately |

In short, ArroyoSketch trades simplicity for power: it is a general-purpose
streaming SQL engine that can express complex multi-stage pipelines, but it
requires standing up and operating Arroyo and Kafka as separate infrastructure.
That operational overhead is the main barrier to running the precompute path in
development, in CI, or in environments where Kafka is not already present.

This PR replaces the ingest-and-aggregate role of ArroyoSketch with a
self-contained Rust implementation that has no external service dependencies,
shares the same store and configuration types as the rest of `asap-query-engine`,
and can be validated end to end in a single process. ArroyoSketch remains useful
as a production deployment option when Arroyo and Kafka are already available,
but the precompute engine is the path forward for native integration within the
Rust codebase.

This PR is primarily about establishing the end-to-end execution path and the
core abstractions:

- ingest endpoint,
- worker sharding model,
- window management,
- accumulator construction and update,
- output sink abstraction, and
- integration with the existing store and query engine.

### Requirements

The implementation in this PR is driven by the following requirements:

1. ASAPQuery needs a native precompute path inside the Rust query engine codebase.
2. The system must ingest a high volume of time-series samples without forcing
   cross-worker coordination on every sample.
3. Samples for the same series must be processed consistently by the same worker
   so per-series state can stay local.
4. The engine must support windowed precomputation for the aggregation
   configurations already defined in `StreamingConfig`.
5. The output must be written in the same `PrecomputedOutput` form already
   consumed by the store and query engine.
6. The design must stay simple enough to validate correctness end to end before
   adding more advanced features such as richer late-data policies or multi-stage
   aggregation.

### Scope of this PR

This PR delivers a pragmatic v1:

- single-process, multi-worker execution,
- Prometheus remote write ingest,
- store-backed output,
- watermark-based window closing,
- bounded per-series buffering,
- best-effort handling of out-of-order data via a lateness threshold,
- optional raw passthrough mode.

It does **not** try to solve every future concern yet. In particular, it does
not add multi-stage aggregation, explicit late-data re-emission policies,
cross-worker merge coordination, or pane-based sliding-window optimization.

## 2. Architecture

### High-level data flow

```text
Prometheus Remote Write
        |
        v
POST /api/v1/write (Axum)
        |
        v
decode_prometheus_remote_write()
        |
        v
group samples by series key
        |
        v
SeriesRouter (xxhash(series_key) % num_workers)
        |
        +-------------------+-------------------+-------------------+
        |                   |                   |                   |
        v                   v                   v                   v
    Worker 0            Worker 1            Worker 2          Worker N-1
        |                   |                   |                   |
        |  per-series buffer + per-aggregation active windows       |
        +-------------------+-------------------+-------------------+
                                |
                                v
                        OutputSink::emit_batch()
                                |
                                v
                               Store
                                |
                                v
                           Query Engine
```

### Main components

#### `PrecomputeEngine` (`mod.rs`)

`PrecomputeEngine` is the top-level orchestrator. It:

- loads aggregation configs from `StreamingConfig`,
- creates one bounded MPSC channel per worker,
- builds a `SeriesRouter`,
- spawns worker tasks,
- starts the ingest HTTP server, and
- starts a periodic flush loop.

The engine keeps the worker model intentionally simple: workers are symmetric,
and routing is deterministic.

#### `SeriesRouter` (`series_router.rs`)

The router computes:

```text
worker_idx = xxhash64(series_key) % num_workers
```

This guarantees that all samples for one exact series key go to the same worker.
That is the main design decision that keeps worker-local state lock-free.

#### `Worker` (`worker.rs`)

Each worker owns a shard of the series space. For each series it stores:

- a `SeriesBuffer`,
- the previous watermark seen for that series,
- one `AggregationState` per matching aggregation config.

Each `AggregationState` contains:

- the copied `AggregationConfig`,
- a `WindowManager`,
- a map of active window accumulators.

Workers receive `Samples`, `Flush`, and `Shutdown` messages. On samples, the
worker inserts data into the series buffer, applies lateness filtering, updates
active window accumulators, detects newly closed windows, and emits completed
accumulators to the sink.

#### `SeriesBuffer` (`series_buffer.rs`)

The buffer stores timestamped samples per series in timestamp order and tracks a
monotonic watermark. It is bounded by `max_buffer_per_series`, which prevents a
single hot or stalled series from growing unbounded in memory.

#### `WindowManager` (`window_manager.rs`)

`WindowManager` encapsulates window boundary logic:

- map a timestamp to an aligned window start,
- decide which windows became closed after watermark advancement,
- return `[window_start, window_end)` bounds.

The current implementation supports both tumbling and slide-aligned window
closure logic. Window close is driven by event-time watermark progression, not
wall-clock time.

#### `AccumulatorUpdater` factory (`accumulator_factory.rs`)

Workers do not hardcode sketch logic. Instead, they construct accumulator
updaters from the aggregation config. This keeps the precompute engine generic
across supported aggregation types and lets it emit the same accumulator objects
already used elsewhere in ASAPQuery.

#### `OutputSink` (`output_sink.rs`)

`OutputSink` separates computation from persistence. This PR ships three useful
implementations:

- `StoreOutputSink` for normal precompute writes,
- `RawPassthroughSink` for writing raw samples as `SumAccumulator`s,
- `NoopOutputSink` for tests.

### Execution model

The execution model is:

1. Decode one remote-write request.
2. Group samples by exact series key.
3. Route each grouped batch to one worker.
4. Process series state only on that worker.
5. Emit completed windows in batches to the sink.

This design avoids per-sample cross-worker synchronization and keeps the first
version operationally understandable.

## 3. Key Features Derived From the Requirements

### Deterministic per-series routing

Requirement: samples for one series must share local state.

Derived feature: the hash-based router always sends the same series key to the
same worker. This means:

- no shared mutable state across workers for a given series,
- no locking around per-series accumulators,
- predictable ownership of series-local watermarks and buffers.

### Config-driven aggregation matching

Requirement: reuse aggregation definitions already present in the system.

Derived feature: each worker matches a series against the loaded
`AggregationConfig`s and creates aggregation state only for the configs relevant
to that series. The engine therefore stays driven by `StreamingConfig` instead
of inventing a separate configuration model.

### Windowed precomputation with watermark closure

Requirement: emit queryable precomputed windows rather than raw streams only.

Derived feature: each aggregation uses a `WindowManager` to:

- align samples to windows,
- detect when watermark movement closes a window,
- emit `PrecomputedOutput` records with exact window bounds.

This gives the query engine stable window ranges to read later.

### Bounded memory for series-local state

Requirement: the engine must remain safe under continuous ingestion.

Derived feature: each series uses a bounded `SeriesBuffer`, and each worker uses
bounded channels from the router. This does not solve every overload scenario,
but it prevents the obvious unbounded growth cases in the v1 design.

### Optional raw passthrough mode

Requirement: support bring-up, debugging, and staged rollout.

Derived feature: when `pass_raw_samples=true`, the worker bypasses windowed
aggregation and emits one `SumAccumulator` per sample. This is useful for
testing the ingest-to-store plumbing independently from sketch behavior.

### Direct integration with the existing store and query engine

Requirement: the precompute path must fit ASAPQuery's existing runtime.

Derived feature: the standalone `precompute_engine` binary can be launched with:

- a `StreamingConfig`,
- a store implementation,
- an optional query HTTP server in the same process.

That makes the PR immediately testable end to end.

## 4. System Implementation Corner Cases

### Late and out-of-order samples

The current policy is intentionally simple:

- if `timestamp < watermark - allowed_lateness_ms`, the sample is dropped;
- otherwise it is accepted.

This means the PR chooses predictability over replay complexity. There is no
secondary path yet for re-opening or patching already emitted windows.

### Idle series and flush behavior

The engine has a periodic flush loop, but the current implementation does **not**
advance watermarks on its own. As a result, a flush only emits windows that have
become closable due to prior event-time progress. If a series stops receiving
samples before a later sample advances the watermark, the worker does not invent
time progress just because wall-clock time passed.

This is an important behavior boundary for this PR.

### Sliding-window semantics in v1

`WindowManager` understands slide intervals, and tests cover slide-aligned
window closing. However, this PR keeps the worker update path simple: samples
are placed into the accumulator keyed by `window_start_for(ts)`, and the design
does not yet implement the more advanced pane-sharing or multi-window fan-out
approach described in earlier discussion branches.

So the current PR establishes the reusable windowing abstraction first, while
leaving richer sliding-window execution strategies for follow-up work.

### Cross-series aggregation across workers

Routing is based on the full series key, not on the final grouping key. That
keeps ingestion simple, but it also means different source series that
contribute to the same logical grouped result may be processed on different
workers. This PR does not introduce a second-tier reduce stage; it relies on the
existing downstream model of storing precomputed outputs and reading them later.

### Series-key parsing assumptions

Grouping-label extraction currently parses series keys in the expected Prometheus
text form:

```text
metric_name{label1="value1",label2="value2"}
```

Missing grouping labels are converted to empty strings. This keeps the worker
path robust, but it is worth documenting because output keys depend on this
parsing behavior.

### Raw mode loses label-group semantics

In raw passthrough mode, the engine emits one point output per sample with
`key=None`. That is acceptable for the intended debugging and plumbing use case,
but it is deliberately not equivalent to fully configured grouped aggregation.

## 5. Examples

### Example 1: basic tumbling-window flow

Assume:

- metric: `fake_metric`
- window size: 60 seconds
- slide interval: 0 (tumbling)
- one sample arrives at `t=12_000 ms`

The worker computes:

- `window_start = 0`
- `window_end = 60_000`

The sample updates the active accumulator for window `[0, 60_000)`. Once the
watermark later reaches at least `60_000`, the worker emits:

- `PrecomputedOutput(start=0, end=60_000, aggregation_id=...)`
- the finished accumulator for that window

### Example 2: out-of-order sample handling

Assume:

- current series watermark is `100_000 ms`
- `allowed_lateness_ms = 5_000`

Then:

- sample at `97_000 ms` is accepted,
- sample at `94_999 ms` is dropped.

This keeps the lateness rule easy to reason about.

### Example 3: deterministic sharding

Assume two incoming series:

- `cpu_usage{host="a",job="node"}`
- `cpu_usage{host="b",job="node"}`

The router hashes each full series key independently. Each series is assigned to
one worker, and every later batch for that same series goes back to that same
worker. The benefit is that each worker can maintain series-local state without
coordination.

### Example 4: raw passthrough mode

If `pass_raw_samples=true` and a sample arrives:

```text
series_key = fake_metric{instance="i1"}
timestamp  = 25_000
value      = 42.0
```

The worker emits one point output immediately:

- `PrecomputedOutput(start=25_000, end=25_000, key=None, aggregation_id=raw_mode_aggregation_id)`
- `SumAccumulator::with_sum(42.0)`

This mode is useful when validating the ingest path independently from
windowed aggregation correctness.

## 6. Summary

PR #228 introduces the first integrated precompute engine inside
`asap-query-engine`. The design deliberately favors a clear and testable v1:

- one process,
- deterministic worker sharding,
- config-driven accumulator creation,
- watermark-based window emission,
- direct store integration.

That foundation is the reason this PR is needed. It creates the runtime path
that later PRs can extend with more sophisticated window execution, richer late
data handling, and more advanced cross-worker aggregation strategies.

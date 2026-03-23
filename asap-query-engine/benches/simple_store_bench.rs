//! Benchmarks for `SimpleMapStore` — insert, range query, exact query,
//! store-analyze, and concurrent reads.
//!
//! These benchmarks profile the existing (pre-PR-175) store implementation and
//! provide concrete measurements of algorithm complexity for:
//!
//! | Operation                          | Expected complexity      |
//! |------------------------------------|--------------------------|
//! | `insert_precomputed_output_batch`  | O(B)                     |
//! | `query_precomputed_output` (range) | O(W·log W + k)           |
//! | `query_precomputed_output_exact`   | O(1) HashMap lookup      |
//! | `get_earliest_timestamp` (analyze) | O(A) — scan agg-id map   |
//! | concurrent reads (n threads)       | serialised by write lock |
//!
//! where B = batch size, W = stored windows, k = result entries, A = agg IDs.
//!
//! Two accumulator types are benchmarked:
//! - `sum`  — `SumAccumulator` (trivial f64, ~0 clone cost, baseline)
//! - `kll`  — `DatasketchesKLLAccumulator` k=200 (~1 KB sketch, realistic clone cost)
//!
//! Run with:
//!   cargo bench -p query_engine_rust --bench simple_store_bench
//!
//! Results land in `target/criterion/`.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use promql_utilities::data_model::KeyByLabelNames;
use query_engine_rust::{
    data_model::{CleanupPolicy, LockStrategy, StreamingConfig},
    precompute_operators::{DatasketchesKLLAccumulator, SumAccumulator},
    AggregateCore, AggregationConfig, PrecomputedOutput, SimpleMapStore, Store,
};
use std::collections::HashMap;
use std::sync::Arc;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a minimal `AggregationConfig` suitable for benchmarking.
fn make_agg_config(agg_id: u64) -> AggregationConfig {
    AggregationConfig::new(
        agg_id,
        "Sum".to_string(),
        "".to_string(),
        HashMap::new(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        "".to_string(),
        60,                     // window_size (seconds)
        60,                     // slide_interval (seconds)
        "tumbling".to_string(), // window_type
        "".to_string(),         // spatial_filter
        "cpu_usage".to_string(),
        None, // num_aggregates_to_retain
        None, // read_count_threshold
        None, // table_name
        None, // value_column
    )
}

/// Build a `StreamingConfig` with `num_agg_ids` distinct aggregation IDs.
fn make_streaming_config(num_agg_ids: u64) -> Arc<StreamingConfig> {
    let configs = (1..=num_agg_ids)
        .map(|id| (id, make_agg_config(id)))
        .collect();
    Arc::new(StreamingConfig::new(configs))
}

/// Build a `SimpleMapStore` with no cleanup policy.
fn make_store(config: Arc<StreamingConfig>, strategy: LockStrategy) -> SimpleMapStore {
    SimpleMapStore::new_with_strategy(config, CleanupPolicy::NoCleanup, strategy)
}

/// Build a batch of `n` `(PrecomputedOutput, SumAccumulator)` pairs for
/// `agg_id`, starting at `base_ts` with windows of `window_ms` milliseconds.
fn make_batch_sum(
    n: usize,
    agg_id: u64,
    base_ts: u64,
    window_ms: u64,
) -> Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> {
    (0..n as u64)
        .map(|i| {
            let start = base_ts + i * window_ms;
            let end = start + window_ms;
            let output = PrecomputedOutput::new(start, end, None, agg_id);
            let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(i as f64));
            (output, acc)
        })
        .collect()
}

/// Build a batch of `n` `(PrecomputedOutput, DatasketchesKLLAccumulator)` pairs.
/// Each sketch is updated with 20 values to give it a realistic memory footprint.
fn make_batch_kll(
    n: usize,
    agg_id: u64,
    base_ts: u64,
    window_ms: u64,
) -> Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> {
    (0..n as u64)
        .map(|i| {
            let start = base_ts + i * window_ms;
            let end = start + window_ms;
            let output = PrecomputedOutput::new(start, end, None, agg_id);
            let mut acc = DatasketchesKLLAccumulator::new(200);
            for v in 0..20 {
                acc._update(v as f64 * (i as f64 + 1.0));
            }
            let acc: Box<dyn AggregateCore> = Box::new(acc);
            (output, acc)
        })
        .collect()
}

/// Pre-populate a store with `num_windows` SumAccumulator entries for `agg_id = 1`.
fn populate_store_sum(store: &SimpleMapStore, num_windows: usize) {
    let batch = make_batch_sum(num_windows, 1, 1_000_000, 60_000);
    store.insert_precomputed_output_batch(batch).unwrap();
}

/// Pre-populate a store with `num_windows` KLL entries for `agg_id = 1`.
fn populate_store_kll(store: &SimpleMapStore, num_windows: usize) {
    let batch = make_batch_kll(num_windows, 1, 1_000_000, 60_000);
    store.insert_precomputed_output_batch(batch).unwrap();
}

// ── benchmark 1: insert throughput vs batch size ──────────────────────────

/// Measures how insert latency scales with the number of items in a batch.
///
/// Both lock strategies and both accumulator types are profiled. The expected
/// complexity is O(B) in batch size B, so throughput (items/s) should remain
/// roughly constant. KLL variant reveals sketch-construction overhead.
fn bench_insert_batch_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/batch_size");
    let streaming_config = make_streaming_config(1);

    for &batch_size in &[100usize, 1_000, 5_000, 10_000, 50_000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        for (acc_label, make_batch) in [
            (
                "sum",
                make_batch_sum
                    as fn(usize, u64, u64, u64) -> Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
            ),
            ("kll", make_batch_kll),
        ] {
            for (lock_label, strategy) in [
                ("per_key", LockStrategy::PerKey),
                ("global", LockStrategy::Global),
            ] {
                let label = format!("{acc_label}/{lock_label}");
                group.bench_with_input(
                    BenchmarkId::new(label, batch_size),
                    &batch_size,
                    |b, &n| {
                        b.iter_batched(
                            || {
                                (
                                    make_store(streaming_config.clone(), strategy),
                                    make_batch(n, 1, 1_000_000, 60_000),
                                )
                            },
                            |(store, batch)| {
                                store.insert_precomputed_output_batch(batch).unwrap();
                            },
                            criterion::BatchSize::SmallInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

// ── benchmark 2: insert throughput vs number of aggregation IDs ──────────

/// Measures how insert latency scales with the number of distinct aggregation
/// IDs in a batch (the outer DashMap dimension).
///
/// The batch always has 1 000 items total; we vary how they are spread across
/// aggregation IDs (1, 10, 50, 200, 1 000). Expected: O(A·lock_overhead).
fn bench_insert_num_agg_ids(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/num_agg_ids");
    const TOTAL_ITEMS: usize = 1_000;

    for &num_ids in &[1usize, 10, 50, 200, 1_000] {
        group.throughput(Throughput::Elements(TOTAL_ITEMS as u64));

        for (label, strategy) in [
            ("per_key", LockStrategy::PerKey),
            ("global", LockStrategy::Global),
        ] {
            let streaming_config = make_streaming_config(num_ids as u64);

            group.bench_with_input(BenchmarkId::new(label, num_ids), &num_ids, |b, &n| {
                b.iter_batched(
                    || {
                        let store = make_store(streaming_config.clone(), strategy);
                        // Spread TOTAL_ITEMS evenly across n aggregation IDs.
                        let per_id = TOTAL_ITEMS / n;
                        let mut batch = Vec::with_capacity(TOTAL_ITEMS);
                        for agg_id in 1..=n as u64 {
                            let mut sub = make_batch_sum(per_id, agg_id, 1_000_000, 60_000);
                            batch.append(&mut sub);
                        }
                        (store, batch)
                    },
                    |(store, batch)| {
                        store.insert_precomputed_output_batch(batch).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

// ── benchmark 3: range query latency vs store size ───────────────────────

/// Measures how range-query latency scales with the number of stored windows W.
///
/// The query always covers the full time span, so all W windows are matched and
/// sorted. Expected: O(W·log W + k) — sorting dominates for large W.
/// The KLL variant reveals the additional cost of cloning large sketch objects
/// during result collection.
fn bench_query_range_store_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("query/range_store_size");

    for &num_windows in &[500usize, 1_000, 5_000, 10_000, 50_000] {
        for (acc_label, populate) in [
            ("sum", populate_store_sum as fn(&SimpleMapStore, usize)),
            ("kll", populate_store_kll),
        ] {
            for (lock_label, strategy) in [
                ("per_key", LockStrategy::PerKey),
                ("global", LockStrategy::Global),
            ] {
                let streaming_config = make_streaming_config(1);
                let store = make_store(streaming_config, strategy);
                populate(&store, num_windows);

                let query_start = 1_000_000u64;
                let query_end = query_start + num_windows as u64 * 60_000;
                let label = format!("{acc_label}/{lock_label}");

                group.bench_with_input(
                    BenchmarkId::new(label, num_windows),
                    &num_windows,
                    |b, _| {
                        b.iter(|| {
                            store
                                .query_precomputed_output("cpu_usage", 1, query_start, query_end)
                                .unwrap()
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

// ── benchmark 4: exact query latency vs store size ───────────────────────

/// Measures how exact-query latency scales with the number of stored windows.
///
/// The exact query is a direct HashMap lookup: O(1) regardless of W. This
/// benchmark verifies that claim and quantifies the constant factor.
fn bench_query_exact_store_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("query/exact_store_size");

    for &num_windows in &[500usize, 1_000, 5_000, 10_000, 50_000] {
        for (label, strategy) in [
            ("per_key", LockStrategy::PerKey),
            ("global", LockStrategy::Global),
        ] {
            let streaming_config = make_streaming_config(1);
            let store = make_store(streaming_config, strategy);
            populate_store_sum(&store, num_windows);

            // Target: the last inserted window (no warm-cache advantage).
            let exact_start = 1_000_000u64 + (num_windows as u64 - 1) * 60_000;
            let exact_end = exact_start + 60_000;

            group.bench_with_input(
                BenchmarkId::new(label, num_windows),
                &num_windows,
                |b, _| {
                    b.iter(|| {
                        store
                            .query_precomputed_output_exact("cpu_usage", 1, exact_start, exact_end)
                            .unwrap()
                    });
                },
            );
        }
    }
    group.finish();
}

// ── benchmark 5: store analyze vs number of aggregation IDs ─────────────

/// Measures how `get_earliest_timestamp_per_aggregation_id` (store analyze)
/// scales with the number of aggregation IDs A.
///
/// Per-key: iterates DashMap shards (lock-free atomics) — O(A).
/// Global:  acquires the Mutex and clones a HashMap — O(A) + lock overhead.
fn bench_store_analyze(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_analyze/num_agg_ids");

    for &num_ids in &[10usize, 100, 500, 1_000, 5_000] {
        for (label, strategy) in [
            ("per_key", LockStrategy::PerKey),
            ("global", LockStrategy::Global),
        ] {
            let streaming_config = make_streaming_config(num_ids as u64);
            let store = make_store(streaming_config, strategy);

            // Insert one entry per aggregation ID so each has an earliest timestamp.
            for agg_id in 1..=num_ids as u64 {
                let output = PrecomputedOutput::new(1_000_000, 1_060_000, None, agg_id);
                let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(1.0));
                store.insert_precomputed_output(output, acc).unwrap();
            }

            group.bench_with_input(BenchmarkId::new(label, num_ids), &num_ids, |b, _| {
                b.iter(|| store.get_earliest_timestamp_per_aggregation_id().unwrap());
            });
        }
    }
    group.finish();
}

// ── benchmark 6: concurrent reads vs thread count ────────────────────────

/// Measures how throughput degrades as more threads simultaneously call
/// `query_precomputed_output` on the same aggregation ID.
///
/// Per-key store: the per-aggregation-id RwLock is taken as a *write* lock
/// even for queries (to update read_counts), so concurrent reads serialize.
/// Global store:  the single Mutex blocks all operations globally.
///
/// Both strategies should show nearly linear degradation with thread count,
/// but the absolute latency baseline differs.
fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads/thread_count");
    let num_windows = 5_000usize;
    let query_start = 1_000_000u64;
    let query_end = query_start + num_windows as u64 * 60_000;

    for (label, strategy) in [
        ("per_key", LockStrategy::PerKey),
        ("global", LockStrategy::Global),
    ] {
        let streaming_config = make_streaming_config(1);
        let store = Arc::new(make_store(streaming_config, strategy));
        populate_store_sum(&store, num_windows);

        for &num_threads in &[1usize, 2, 4, 8, 16] {
            group.bench_with_input(
                BenchmarkId::new(label, num_threads),
                &num_threads,
                |b, &n| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..n)
                            .map(|_| {
                                let store = store.clone();
                                std::thread::spawn(move || {
                                    store
                                        .query_precomputed_output(
                                            "cpu_usage",
                                            1,
                                            query_start,
                                            query_end,
                                        )
                                        .unwrap()
                                })
                            })
                            .collect();
                        handles.into_iter().for_each(|h| {
                            h.join().unwrap();
                        });
                    });
                },
            );
        }
    }
    group.finish();
}

// ── criterion entry point ─────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_insert_batch_size,
    bench_insert_num_agg_ids,
    bench_query_range_store_size,
    bench_query_exact_store_size,
    bench_store_analyze,
    bench_concurrent_reads,
);
criterion_main!(benches);

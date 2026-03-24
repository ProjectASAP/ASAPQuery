//! Benchmarks for `LegacySimpleMapStore` — insert, range query, exact query,
//! store-analyze, and concurrent reads.
//!
//! These benchmarks profile the legacy store implementation
//! (`LegacySimpleMapStoreGlobal` / `LegacySimpleMapStorePerKey`) and provide
//! concrete measurements of algorithm complexity for:
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

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use promql_utilities::data_model::KeyByLabelNames;
use query_engine_rust::data_model::{
    AggregateCore, CleanupPolicy, KeyByLabelValues, LockStrategy, StreamingConfig,
};
use query_engine_rust::precompute_operators::{DatasketchesKLLAccumulator, SumAccumulator};
use query_engine_rust::stores::simple_map_store::legacy::{
    LegacySimpleMapStoreGlobal, LegacySimpleMapStorePerKey,
};
use query_engine_rust::stores::Store;
use query_engine_rust::{AggregationConfig, PrecomputedOutput, SimpleMapStore};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};

#[derive(Clone, Copy)]
enum StoreKind {
    LegacyPerKey,
    LegacyGlobal,
    NewPerKey,
    NewGlobal,
}

impl StoreKind {
    const ALL: [Self; 4] = [
        Self::LegacyPerKey,
        Self::LegacyGlobal,
        Self::NewPerKey,
        Self::NewGlobal,
    ];

    fn slug(self) -> &'static str {
        match self {
            Self::LegacyPerKey => "legacy/per_key",
            Self::LegacyGlobal => "legacy/global",
            Self::NewPerKey => "new/per_key",
            Self::NewGlobal => "new/global",
        }
    }

    fn build(self, config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Arc<dyn Store> {
        match self {
            Self::LegacyPerKey => Arc::new(LegacySimpleMapStorePerKey::new(config, cleanup_policy)),
            Self::LegacyGlobal => Arc::new(LegacySimpleMapStoreGlobal::new(config, cleanup_policy)),
            Self::NewPerKey => Arc::new(SimpleMapStore::new_with_strategy(
                config,
                cleanup_policy,
                LockStrategy::PerKey,
            )),
            Self::NewGlobal => Arc::new(SimpleMapStore::new_with_strategy(
                config,
                cleanup_policy,
                LockStrategy::Global,
            )),
        }
    }
}

#[derive(Clone, Copy)]
enum AccumulatorKind {
    Sum,
    Kll,
}

impl AccumulatorKind {
    const ALL: [Self; 2] = [Self::Sum, Self::Kll];

    fn slug(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Kll => "kll",
        }
    }

    fn aggregation_type(self) -> &'static str {
        match self {
            Self::Sum => "Sum",
            Self::Kll => "DatasketchesKLL",
        }
    }

    fn build(self, value: f64) -> Box<dyn AggregateCore> {
        match self {
            Self::Sum => Box::new(SumAccumulator::with_sum(value)),
            Self::Kll => {
                let mut acc = DatasketchesKLLAccumulator::new(200);
                for v in 0..20 {
                    acc._update(v as f64 * (value + 1.0));
                }
                Box::new(acc)
            }
        }
    }
}

fn make_agg_config(
    agg_id: u64,
    aggregation_type: &str,
    metric: &str,
    num_aggregates_to_retain: Option<u64>,
    read_count_threshold: Option<u64>,
) -> AggregationConfig {
    AggregationConfig::new(
        agg_id,
        aggregation_type.to_string(),
        "".to_string(),
        HashMap::new(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        "".to_string(),
        60,
        "".to_string(),
        metric.to_string(),
        num_aggregates_to_retain,
        read_count_threshold,
        None,
        None,
        None,
        None,
        None,
    )
}

fn make_streaming_config(
    agg_ids: &[u64],
    accumulator_kind: AccumulatorKind,
    metric: &str,
    num_aggregates_to_retain: Option<u64>,
    read_count_threshold: Option<u64>,
) -> Arc<StreamingConfig> {
    let configs = agg_ids
        .iter()
        .copied()
        .map(|agg_id| {
            (
                agg_id,
                make_agg_config(
                    agg_id,
                    accumulator_kind.aggregation_type(),
                    metric,
                    num_aggregates_to_retain,
                    read_count_threshold,
                ),
            )
        })
        .collect();
    Arc::new(StreamingConfig::new(configs))
}

fn make_batch(
    count: usize,
    agg_id: u64,
    base_ts: u64,
    window_ms: u64,
    accumulator_kind: AccumulatorKind,
) -> Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> {
    (0..count as u64)
        .map(|i| {
            let start = base_ts + i * window_ms;
            let end = start + window_ms;
            (
                PrecomputedOutput::new(start, end, None, agg_id),
                accumulator_kind.build(i as f64),
            )
        })
        .collect()
}

fn insert_labelled_entry(
    store: &dyn Store,
    start: u64,
    end: u64,
    label: &str,
    agg_id: u64,
    accumulator_kind: AccumulatorKind,
    value: f64,
) {
    let output = PrecomputedOutput::new(
        start,
        end,
        Some(KeyByLabelValues::new_with_labels(vec![label.to_string()])),
        agg_id,
    );
    store
        .insert_precomputed_output(output, accumulator_kind.build(value))
        .unwrap();
}

fn populate_store_labelled(
    store: &dyn Store,
    time_ranges: usize,
    labels: usize,
    agg_id: u64,
    accumulator_kind: AccumulatorKind,
) {
    for i in 0..time_ranges {
        let start = i as u64 * 1_000;
        let end = start + 1_000;
        for j in 0..labels {
            insert_labelled_entry(
                store,
                start,
                end,
                &format!("host-{j}"),
                agg_id,
                accumulator_kind,
                1.0,
            );
        }
    }
}

fn populate_store_batch(
    store: &dyn Store,
    num_windows: usize,
    agg_id: u64,
    accumulator_kind: AccumulatorKind,
) {
    let batch = make_batch(num_windows, agg_id, 1_000_000, 60_000, accumulator_kind);
    store.insert_precomputed_output_batch(batch).unwrap();
}

fn build_populated_store(
    kind: StoreKind,
    accumulator_kind: AccumulatorKind,
    time_ranges: usize,
    labels: usize,
) -> Arc<dyn Store> {
    let config = make_streaming_config(&[1], accumulator_kind, "test_metric", None, None);
    let store = kind.build(config, CleanupPolicy::NoCleanup);
    populate_store_labelled(store.as_ref(), time_ranges, labels, 1, accumulator_kind);
    store
}

fn bench_insert_batch_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/batch_size");

    for &batch_size in &[100usize, 1_000, 5_000, 10_000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        for kind in StoreKind::ALL {
            for accumulator_kind in AccumulatorKind::ALL {
                let id = format!("{}/{}", kind.slug(), accumulator_kind.slug());
                group.bench_with_input(BenchmarkId::new(id, batch_size), &batch_size, |b, &n| {
                    b.iter_batched(
                        || {
                            let config = make_streaming_config(
                                &[1],
                                accumulator_kind,
                                "cpu_usage",
                                None,
                                None,
                            );
                            (
                                kind.build(config, CleanupPolicy::NoCleanup),
                                make_batch(n, 1, 1_000_000, 60_000, accumulator_kind),
                            )
                        },
                        |(store, batch)| {
                            store.insert_precomputed_output_batch(batch).unwrap();
                        },
                        criterion::BatchSize::SmallInput,
                    );
                });
            }
        }
    }

    group.finish();
}

fn bench_insert_num_agg_ids(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert/num_agg_ids");
    const TOTAL_ITEMS: usize = 1_000;

    for &num_ids in &[1usize, 10, 50, 200] {
        group.throughput(Throughput::Elements(TOTAL_ITEMS as u64));

        for kind in StoreKind::ALL {
            group.bench_with_input(BenchmarkId::new(kind.slug(), num_ids), &num_ids, |b, &n| {
                b.iter_batched(
                    || {
                        let agg_ids: Vec<u64> = (1..=n as u64).collect();
                        let config = make_streaming_config(
                            &agg_ids,
                            AccumulatorKind::Sum,
                            "cpu_usage",
                            None,
                            None,
                        );
                        let store = kind.build(config, CleanupPolicy::NoCleanup);
                        let per_id = TOTAL_ITEMS / n;
                        let mut batch = Vec::with_capacity(per_id * n);
                        for agg_id in agg_ids {
                            batch.extend(make_batch(
                                per_id,
                                agg_id,
                                1_000_000,
                                60_000,
                                AccumulatorKind::Sum,
                            ));
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

fn bench_query_range_store_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("query/range_store_size");

    for &num_windows in &[500usize, 1_000, 5_000, 10_000] {
        for kind in StoreKind::ALL {
            for accumulator_kind in AccumulatorKind::ALL {
                let store = {
                    let config =
                        make_streaming_config(&[1], accumulator_kind, "cpu_usage", None, None);
                    let store = kind.build(config, CleanupPolicy::NoCleanup);
                    populate_store_batch(store.as_ref(), num_windows, 1, accumulator_kind);
                    store
                };
                let id = format!("{}/{}", kind.slug(), accumulator_kind.slug());
                let query_start = 1_000_000u64;
                let query_end = query_start + num_windows as u64 * 60_000;

                group.bench_with_input(BenchmarkId::new(id, num_windows), &num_windows, |b, _| {
                    b.iter(|| {
                        black_box(
                            store
                                .query_precomputed_output("cpu_usage", 1, query_start, query_end)
                                .unwrap(),
                        )
                    });
                });
            }
        }
    }

    group.finish();
}

fn bench_query_exact_store_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("query/exact_store_size");

    for &num_windows in &[500usize, 1_000, 5_000, 10_000] {
        for kind in StoreKind::ALL {
            let store = {
                let config =
                    make_streaming_config(&[1], AccumulatorKind::Sum, "cpu_usage", None, None);
                let store = kind.build(config, CleanupPolicy::NoCleanup);
                populate_store_batch(store.as_ref(), num_windows, 1, AccumulatorKind::Sum);
                store
            };
            let exact_start = 1_000_000u64 + (num_windows as u64 - 1) * 60_000;
            let exact_end = exact_start + 60_000;

            group.bench_with_input(
                BenchmarkId::new(kind.slug(), num_windows),
                &num_windows,
                |b, _| {
                    b.iter(|| {
                        black_box(
                            store
                                .query_precomputed_output_exact(
                                    "cpu_usage",
                                    1,
                                    exact_start,
                                    exact_end,
                                )
                                .unwrap(),
                        )
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_store_analyze(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_analyze/num_agg_ids");

    for &num_ids in &[10usize, 100, 500, 1_000] {
        let agg_ids: Vec<u64> = (1..=num_ids as u64).collect();
        for kind in StoreKind::ALL {
            let config =
                make_streaming_config(&agg_ids, AccumulatorKind::Sum, "cpu_usage", None, None);
            let store = kind.build(config, CleanupPolicy::NoCleanup);
            for agg_id in 1..=num_ids as u64 {
                let output = PrecomputedOutput::new(1_000_000, 1_060_000, None, agg_id);
                store
                    .insert_precomputed_output(output, AccumulatorKind::Sum.build(1.0))
                    .unwrap();
            }

            group.bench_with_input(BenchmarkId::new(kind.slug(), num_ids), &num_ids, |b, _| {
                b.iter(|| black_box(store.get_earliest_timestamp_per_aggregation_id().unwrap()));
            });
        }
    }

    group.finish();
}

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads/thread_count");
    let num_windows = 5_000usize;
    let query_start = 1_000_000u64;
    let query_end = query_start + num_windows as u64 * 60_000;

    for kind in StoreKind::ALL {
        let config = make_streaming_config(&[1], AccumulatorKind::Sum, "cpu_usage", None, None);
        let store = kind.build(config, CleanupPolicy::NoCleanup);
        populate_store_batch(store.as_ref(), num_windows, 1, AccumulatorKind::Sum);

        for &num_threads in &[1usize, 2, 4, 8] {
            group.bench_with_input(
                BenchmarkId::new(kind.slug(), num_threads),
                &num_threads,
                |b, &n| {
                    let store = store.clone();
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
                        for handle in handles {
                            black_box(handle.join().unwrap());
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes/thread_count");
    let labels = 10usize;
    let entries_per_thread = 500usize;
    let time_ranges_per_thread = entries_per_thread / labels;

    for kind in StoreKind::ALL {
        for &num_threads in &[1usize, 2, 4, 8] {
            group.bench_with_input(
                BenchmarkId::new(kind.slug(), num_threads),
                &num_threads,
                |b, &n| {
                    b.iter(|| {
                        let config = make_streaming_config(
                            &[1],
                            AccumulatorKind::Sum,
                            "test_metric",
                            None,
                            None,
                        );
                        let store = kind.build(config, CleanupPolicy::NoCleanup);
                        let barrier = Arc::new(Barrier::new(n));
                        std::thread::scope(|scope| {
                            for t in 0..n {
                                let store = store.clone();
                                let barrier = barrier.clone();
                                scope.spawn(move || {
                                    barrier.wait();
                                    for i in 0..time_ranges_per_thread {
                                        let start = i as u64 * 1_000;
                                        let end = start + 1_000;
                                        for j in 0..labels {
                                            insert_labelled_entry(
                                                store.as_ref(),
                                                start,
                                                end,
                                                &format!("thread-{t}-host-{j}"),
                                                1,
                                                AccumulatorKind::Sum,
                                                1.0,
                                            );
                                        }
                                    }
                                });
                            }
                        });
                        black_box(store);
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_concurrent_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_mixed_rw/config");
    let writers = 2usize;
    let readers = 2usize;
    let labels = 10usize;
    let time_ranges = 1_000usize;
    let total_threads = writers + readers;

    for kind in StoreKind::ALL {
        let store = build_populated_store(kind, AccumulatorKind::Sum, time_ranges, labels);
        let query_end = time_ranges as u64 * 1_000 / 10;

        group.bench_function(kind.slug(), |b| {
            let store = store.clone();
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(total_threads));
                std::thread::scope(|scope| {
                    for writer_id in 0..writers {
                        let store = store.clone();
                        let barrier = barrier.clone();
                        scope.spawn(move || {
                            barrier.wait();
                            for offset in 0..50usize {
                                let start = (time_ranges + writer_id * 50 + offset) as u64 * 1_000;
                                let end = start + 1_000;
                                for label_id in 0..labels {
                                    insert_labelled_entry(
                                        store.as_ref(),
                                        start,
                                        end,
                                        &format!("mixed-{writer_id}-host-{label_id}"),
                                        1,
                                        AccumulatorKind::Sum,
                                        1.0,
                                    );
                                }
                            }
                        });
                    }

                    for _ in 0..readers {
                        let store = store.clone();
                        let barrier = barrier.clone();
                        scope.spawn(move || {
                            barrier.wait();
                            for _ in 0..20 {
                                black_box(
                                    store
                                        .query_precomputed_output("test_metric", 1, 0, query_end)
                                        .unwrap(),
                                );
                            }
                        });
                    }
                });
            });
        });
    }

    group.finish();
}

fn bench_cleanup_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("cleanup_overhead");
    let labels = 5usize;

    for kind in StoreKind::ALL {
        group.bench_function(format!("{}/no_cleanup", kind.slug()), |b| {
            b.iter(|| {
                let config =
                    make_streaming_config(&[1], AccumulatorKind::Sum, "test_metric", None, None);
                let store = kind.build(config, CleanupPolicy::NoCleanup);
                populate_store_labelled(store.as_ref(), 200, labels, 1, AccumulatorKind::Sum);
                black_box(store);
            });
        });

        group.bench_function(format!("{}/circular_buffer", kind.slug()), |b| {
            b.iter(|| {
                let config = make_streaming_config(
                    &[1],
                    AccumulatorKind::Sum,
                    "test_metric",
                    Some(50),
                    None,
                );
                let store = kind.build(config, CleanupPolicy::CircularBuffer);
                populate_store_labelled(store.as_ref(), 200, labels, 1, AccumulatorKind::Sum);
                black_box(store);
            });
        });

        group.bench_function(format!("{}/read_based", kind.slug()), |b| {
            b.iter(|| {
                let config =
                    make_streaming_config(&[1], AccumulatorKind::Sum, "test_metric", None, Some(2));
                let store = kind.build(config, CleanupPolicy::ReadBased);
                populate_store_labelled(store.as_ref(), 100, labels, 1, AccumulatorKind::Sum);

                for _ in 0..2 {
                    black_box(
                        store
                            .query_precomputed_output("test_metric", 1, 0, 100_000)
                            .unwrap(),
                    );
                }

                for i in 100..200usize {
                    let start = i as u64 * 1_000;
                    let end = start + 1_000;
                    for j in 0..labels {
                        insert_labelled_entry(
                            store.as_ref(),
                            start,
                            end,
                            &format!("host-{j}"),
                            1,
                            AccumulatorKind::Sum,
                            1.0,
                        );
                    }
                }

                black_box(store);
            });
        });
    }

    group.finish();
}

fn bench_query_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_patterns");
    let time_ranges = 1_000usize;
    let labels = 10usize;
    let total_time = time_ranges as u64 * 1_000;

    for kind in StoreKind::ALL {
        let store = build_populated_store(kind, AccumulatorKind::Sum, time_ranges, labels);

        for (name, start, end) in [
            ("full_scan", 0, total_time),
            ("wide_50pct", 0, total_time / 2),
            ("narrow_1pct", 0, total_time / 100),
            ("miss", total_time + 1_000_000, total_time + 1_001_000),
        ] {
            group.bench_function(format!("{}/{}", kind.slug(), name), |b| {
                let store = store.clone();
                b.iter(|| {
                    black_box(
                        store
                            .query_precomputed_output("test_metric", 1, start, end)
                            .unwrap(),
                    );
                });
            });
        }
    }

    group.finish();
}

fn bench_high_label_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_label_cardinality");
    let time_ranges = 20usize;

    for &label_count in &[10usize, 100, 500, 1_000] {
        for kind in StoreKind::ALL {
            group.bench_with_input(
                BenchmarkId::new(format!("{}/insert", kind.slug()), label_count),
                &label_count,
                |b, &lc| {
                    b.iter(|| {
                        let store =
                            build_populated_store(kind, AccumulatorKind::Sum, time_ranges, lc);
                        black_box(store);
                    });
                },
            );

            let store = build_populated_store(kind, AccumulatorKind::Sum, time_ranges, label_count);
            let query_end = time_ranges as u64 * 1_000;
            group.bench_with_input(
                BenchmarkId::new(format!("{}/query", kind.slug()), label_count),
                &label_count,
                |b, _| {
                    let store = store.clone();
                    b.iter(|| {
                        black_box(
                            store
                                .query_precomputed_output("test_metric", 1, 0, query_end)
                                .unwrap(),
                        );
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_multi_agg_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_agg_id");
    let agg_ids: Vec<u64> = (1..=10).collect();
    let time_ranges = 100usize;
    let labels = 5usize;

    for kind in StoreKind::ALL {
        group.bench_function(format!("{}/insert_10_agg_ids", kind.slug()), |b| {
            b.iter(|| {
                let config = make_streaming_config(
                    &agg_ids,
                    AccumulatorKind::Sum,
                    "test_metric",
                    None,
                    None,
                );
                let store = kind.build(config, CleanupPolicy::NoCleanup);
                for &agg_id in &agg_ids {
                    populate_store_labelled(
                        store.as_ref(),
                        time_ranges,
                        labels,
                        agg_id,
                        AccumulatorKind::Sum,
                    );
                }
                black_box(store);
            });
        });

        let config =
            make_streaming_config(&agg_ids, AccumulatorKind::Sum, "test_metric", None, None);
        let store = kind.build(config, CleanupPolicy::NoCleanup);
        for &agg_id in &agg_ids {
            populate_store_labelled(
                store.as_ref(),
                time_ranges,
                labels,
                agg_id,
                AccumulatorKind::Sum,
            );
        }
        let query_end = time_ranges as u64 * 1_000;

        group.bench_function(format!("{}/query_hot_cold", kind.slug()), |b| {
            let store = store.clone();
            let mut query_idx = 0u64;
            b.iter(|| {
                let agg_id = if query_idx % 5 < 4 {
                    (query_idx % 2) + 1
                } else {
                    (query_idx % 8) + 3
                };
                query_idx += 1;
                black_box(
                    store
                        .query_precomputed_output("test_metric", agg_id, 0, query_end)
                        .unwrap(),
                );
            });
        });

        group.bench_function(format!("{}/concurrent_hot_cold", kind.slug()), |b| {
            let store = store.clone();
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(4));
                std::thread::scope(|scope| {
                    for t in 0..4usize {
                        let store = store.clone();
                        let barrier = barrier.clone();
                        scope.spawn(move || {
                            barrier.wait();
                            for q in 0..50usize {
                                let idx = (t * 50 + q) as u64;
                                let agg_id = if idx % 5 < 4 {
                                    (idx % 2) + 1
                                } else {
                                    (idx % 8) + 3
                                };
                                black_box(
                                    store
                                        .query_precomputed_output(
                                            "test_metric",
                                            agg_id,
                                            0,
                                            query_end,
                                        )
                                        .unwrap(),
                                );
                            }
                        });
                    }
                });
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_batch_size,
    bench_insert_num_agg_ids,
    bench_query_range_store_size,
    bench_query_exact_store_size,
    bench_store_analyze,
    bench_concurrent_reads,
    bench_concurrent_writes,
    bench_concurrent_mixed_read_write,
    bench_cleanup_overhead,
    bench_query_patterns,
    bench_high_label_cardinality,
    bench_multi_agg_id,
);
criterion_main!(benches);

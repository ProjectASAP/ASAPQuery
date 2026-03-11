#![allow(deprecated)]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};

use promql_utilities::data_model::KeyByLabelNames;
use query_engine_rust::data_model::{
    AggregateCore, CleanupPolicy, KeyByLabelValues, LockStrategy, PrecomputedOutput,
    StreamingConfig,
};
use query_engine_rust::precompute_operators::sum_accumulator::SumAccumulator;
use query_engine_rust::stores::simple_map_store::per_key_legacy::LegacySimpleMapStorePerKey;
use query_engine_rust::stores::simple_map_store::SimpleMapStore;
use query_engine_rust::stores::Store;
use sketch_db_common::aggregation_config::AggregationConfig;

/// Create a StreamingConfig with a single SumAccumulator aggregation.
fn make_streaming_config() -> Arc<StreamingConfig> {
    let mut configs = HashMap::new();
    configs.insert(
        1,
        AggregationConfig {
            aggregation_id: 1,
            aggregation_type: "SumAccumulator".to_string(),
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size: 1000,
            slide_interval: 1000,
            window_type: "tumbling".to_string(),
            tumbling_window_size: 1000,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: "test_metric".to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        },
    );
    Arc::new(StreamingConfig::new(configs))
}

/// Build a fresh SimpleMapStore and populate it with `time_ranges` × `labels` entries.
fn build_populated_store(time_ranges: usize, labels: usize) -> SimpleMapStore {
    let config = make_streaming_config();
    let store = SimpleMapStore::new(config, CleanupPolicy::NoCleanup);
    populate_store(&store, time_ranges, labels);
    store
}

/// Insert `time_ranges` × `labels` entries into an existing store.
fn populate_store(store: &SimpleMapStore, time_ranges: usize, labels: usize) {
    for i in 0..time_ranges {
        let start = (i as u64) * 1000;
        let end = start + 1000;
        for j in 0..labels {
            let key = KeyByLabelValues::new_with_labels(vec![format!("host-{j}")]);
            let output = PrecomputedOutput::new(start, end, Some(key), 1);
            let accumulator: Box<dyn query_engine_rust::data_model::AggregateCore> =
                Box::new(SumAccumulator::with_sum(1.0));
            store
                .insert_precomputed_output(output, accumulator)
                .unwrap();
        }
    }
}

/// Create a StreamingConfig with multiple agg IDs and configurable cleanup fields.
fn make_streaming_config_with_cleanup(
    agg_ids: &[u64],
    metric: &str,
    num_aggregates_to_retain: Option<u64>,
    read_count_threshold: Option<u64>,
) -> Arc<StreamingConfig> {
    let mut configs = HashMap::new();
    for &id in agg_ids {
        configs.insert(
            id,
            AggregationConfig {
                aggregation_id: id,
                aggregation_type: "SumAccumulator".to_string(),
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::empty(),
                aggregated_labels: KeyByLabelNames::empty(),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size: 1000,
                slide_interval: 1000,
                window_type: "tumbling".to_string(),
                tumbling_window_size: 1000,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: metric.to_string(),
                num_aggregates_to_retain,
                read_count_threshold,
                table_name: None,
                value_column: None,
            },
        );
    }
    Arc::new(StreamingConfig::new(configs))
}

/// Shorthand for creating a (PrecomputedOutput, Box<dyn AggregateCore>) tuple.
fn make_output(
    start: u64,
    end: u64,
    label: &str,
    agg_id: u64,
) -> (PrecomputedOutput, Box<dyn AggregateCore>) {
    let key = KeyByLabelValues::new_with_labels(vec![label.to_string()]);
    let output = PrecomputedOutput::new(start, end, Some(key), agg_id);
    let accumulator: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(1.0));
    (output, accumulator)
}

/// Insert entries into a store with a time offset, for a given set of labels and agg_id.
fn populate_store_with_offset(
    store: &SimpleMapStore,
    start_idx: usize,
    end_idx: usize,
    labels: &[String],
) {
    for i in start_idx..end_idx {
        let start = (i as u64) * 1000;
        let end = start + 1000;
        for label in labels {
            let (output, acc) = make_output(start, end, label, 1);
            store.insert_precomputed_output(output, acc).unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Insert benchmarks
// ---------------------------------------------------------------------------

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    // (time_ranges, labels) combinations that total roughly 100, 1K, 10K inserts
    let configs: Vec<(usize, usize)> = vec![(10, 10), (100, 10), (1000, 10)];

    for &(time_ranges, labels) in &configs {
        let total = time_ranges * labels;
        group.bench_with_input(
            BenchmarkId::new("inserts", total),
            &(time_ranges, labels),
            |b, &(tr, l)| {
                b.iter(|| {
                    let store = build_populated_store(black_box(tr), black_box(l));
                    black_box(&store);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Range query benchmarks
// ---------------------------------------------------------------------------

fn bench_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");
    let time_ranges = 1_000;

    for labels in [1, 10, 100] {
        let store = build_populated_store(time_ranges, labels);

        // Query ~10% of the time range
        let query_start = 0u64;
        let query_end = (time_ranges as u64) * 1000 / 10; // first 10%

        group.bench_with_input(BenchmarkId::new("labels", labels), &labels, |b, _labels| {
            b.iter(|| {
                let result = store
                    .query_precomputed_output(
                        black_box("test_metric"),
                        black_box(1),
                        black_box(query_start),
                        black_box(query_end),
                    )
                    .unwrap();
                black_box(result);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Exact query benchmarks
// ---------------------------------------------------------------------------

fn bench_exact_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("exact_query");
    let time_ranges = 1_000;

    for labels in [1, 10, 100] {
        let store = build_populated_store(time_ranges, labels);

        // Pick a timestamp in the middle of the store
        let mid = (time_ranges / 2) as u64;
        let exact_start = mid * 1000;
        let exact_end = exact_start + 1000;

        group.bench_with_input(BenchmarkId::new("labels", labels), &labels, |b, _labels| {
            b.iter(|| {
                let result = store
                    .query_precomputed_output_exact(
                        black_box("test_metric"),
                        black_box(1),
                        black_box(exact_start),
                        black_box(exact_end),
                    )
                    .unwrap();
                black_box(result);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scaling benchmarks
// ---------------------------------------------------------------------------

fn bench_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling");
    let labels = 10;

    for time_ranges in [10, 100, 1_000, 10_000] {
        let store = build_populated_store(time_ranges, labels);

        // Query ~10% of the time range
        let query_start = 0u64;
        let query_end = (time_ranges as u64) * 1000 / 10;

        group.bench_with_input(
            BenchmarkId::new("time_ranges", time_ranges),
            &time_ranges,
            |b, _tr| {
                b.iter(|| {
                    let result = store
                        .query_precomputed_output(
                            black_box("test_metric"),
                            black_box(1),
                            black_box(query_start),
                            black_box(query_end),
                        )
                        .unwrap();
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 1. Batch insert benchmarks — vary batch size with fixed 10K total inserts
// ---------------------------------------------------------------------------

fn bench_batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_insert");
    let total_inserts = 1_000usize;
    let labels = 10usize;
    let time_ranges = total_inserts / labels; // 100 time ranges

    for batch_size in [1, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            &batch_size,
            |b, &bs| {
                b.iter(|| {
                    let config = make_streaming_config();
                    let store = SimpleMapStore::new(config, CleanupPolicy::NoCleanup);

                    // Build all entries, then insert in batches
                    let mut batch = Vec::with_capacity(bs);
                    for i in 0..time_ranges {
                        let start = (i as u64) * 1000;
                        let end = start + 1000;
                        for j in 0..labels {
                            batch.push(make_output(start, end, &format!("host-{j}"), 1));
                            if batch.len() == bs {
                                store
                                    .insert_precomputed_output_batch(std::mem::replace(
                                        &mut batch,
                                        Vec::with_capacity(bs),
                                    ))
                                    .unwrap();
                            }
                        }
                    }
                    // Flush remainder
                    if !batch.is_empty() {
                        store.insert_precomputed_output_batch(batch).unwrap();
                    }
                    black_box(&store);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Concurrent writes — N threads each inserting 2,500 entries
// ---------------------------------------------------------------------------

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes");
    let entries_per_thread = 500usize;
    let labels = 10usize;
    let time_ranges_per_thread = entries_per_thread / labels; // 50

    for num_threads in [1, 2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &nt| {
                b.iter(|| {
                    let config = make_streaming_config();
                    let store = SimpleMapStore::new(config, CleanupPolicy::NoCleanup);
                    let barrier = Arc::new(Barrier::new(nt));

                    std::thread::scope(|s| {
                        for t in 0..nt {
                            let store_ref = &store;
                            let barrier_ref = barrier.clone();
                            s.spawn(move || {
                                barrier_ref.wait();
                                for i in 0..time_ranges_per_thread {
                                    let start = (i as u64) * 1000;
                                    let end = start + 1000;
                                    for j in 0..labels {
                                        // Disjoint labels per thread
                                        let label = format!("thread-{t}-host-{j}");
                                        let (output, acc) = make_output(start, end, &label, 1);
                                        store_ref.insert_precomputed_output(output, acc).unwrap();
                                    }
                                }
                            });
                        }
                    });

                    black_box(&store);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Concurrent mixed read/write — readers + writers simultaneously
// ---------------------------------------------------------------------------

fn bench_concurrent_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_mixed_rw");
    let pre_pop_time_ranges = 500usize;
    let labels = 10usize;
    let write_entries_per_thread = 100usize;
    let read_queries_per_thread = 100usize;

    let configs: Vec<(usize, usize)> = vec![(1, 1), (2, 2), (4, 4), (1, 4), (4, 1)];

    for &(num_writers, num_readers) in &configs {
        let id = format!("{num_writers}w_{num_readers}r");
        group.bench_with_input(
            BenchmarkId::new("config", &id),
            &(num_writers, num_readers),
            |b, &(nw, nr)| {
                b.iter(|| {
                    let config = make_streaming_config();
                    let store = SimpleMapStore::new(config, CleanupPolicy::NoCleanup);

                    // Pre-populate
                    populate_store(&store, pre_pop_time_ranges, labels);

                    let total_threads = nw + nr;
                    let barrier = Arc::new(Barrier::new(total_threads));
                    let query_end = (pre_pop_time_ranges as u64) * 1000;

                    std::thread::scope(|s| {
                        // Writer threads — insert beyond pre-populated range
                        for t in 0..nw {
                            let store_ref = &store;
                            let barrier_ref = barrier.clone();
                            s.spawn(move || {
                                barrier_ref.wait();
                                let base = pre_pop_time_ranges + t * write_entries_per_thread;
                                for i in 0..write_entries_per_thread {
                                    let start = ((base + i) as u64) * 1000;
                                    let end = start + 1000;
                                    let label = format!("writer-{t}-host-0");
                                    let (output, acc) = make_output(start, end, &label, 1);
                                    store_ref.insert_precomputed_output(output, acc).unwrap();
                                }
                            });
                        }

                        // Reader threads — query existing range
                        for _r in 0..nr {
                            let store_ref = &store;
                            let barrier_ref = barrier.clone();
                            s.spawn(move || {
                                barrier_ref.wait();
                                for _ in 0..read_queries_per_thread {
                                    let result = store_ref
                                        .query_precomputed_output("test_metric", 1, 0, query_end)
                                        .unwrap();
                                    black_box(result);
                                }
                            });
                        }
                    });

                    black_box(&store);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Lock strategy comparison — PerKey vs Global
// ---------------------------------------------------------------------------

fn bench_lock_strategy_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_strategy");
    let num_threads = 4usize;
    let entries_per_thread = 500usize;
    let labels = 10usize;
    let time_ranges_per_thread = entries_per_thread / labels;
    let query_time_ranges = 1_000usize;

    for strategy in [LockStrategy::PerKey, LockStrategy::Global] {
        let strategy_name = match strategy {
            LockStrategy::PerKey => "per_key",
            LockStrategy::Global => "global",
        };

        // Sub-benchmark: concurrent inserts
        group.bench_with_input(
            BenchmarkId::new("insert", strategy_name),
            &strategy,
            |b, &strat| {
                b.iter(|| {
                    let config = make_streaming_config();
                    let store =
                        SimpleMapStore::new_with_strategy(config, CleanupPolicy::NoCleanup, strat);
                    let barrier = Arc::new(Barrier::new(num_threads));

                    std::thread::scope(|s| {
                        for t in 0..num_threads {
                            let store_ref = &store;
                            let barrier_ref = barrier.clone();
                            s.spawn(move || {
                                barrier_ref.wait();
                                for i in 0..time_ranges_per_thread {
                                    let start = (i as u64) * 1000;
                                    let end = start + 1000;
                                    for j in 0..labels {
                                        let label = format!("thread-{t}-host-{j}");
                                        let (output, acc) = make_output(start, end, &label, 1);
                                        store_ref.insert_precomputed_output(output, acc).unwrap();
                                    }
                                }
                            });
                        }
                    });

                    black_box(&store);
                });
            },
        );

        // Sub-benchmark: concurrent queries
        group.bench_with_input(
            BenchmarkId::new("query", strategy_name),
            &strategy,
            |b, &strat| {
                let config = make_streaming_config();
                let store =
                    SimpleMapStore::new_with_strategy(config, CleanupPolicy::NoCleanup, strat);
                populate_store(&store, query_time_ranges, labels);
                let query_end = (query_time_ranges as u64) * 1000 / 10;

                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(num_threads));

                    std::thread::scope(|s| {
                        for _ in 0..num_threads {
                            let store_ref = &store;
                            let barrier_ref = barrier.clone();
                            s.spawn(move || {
                                barrier_ref.wait();
                                for _ in 0..20 {
                                    let result = store_ref
                                        .query_precomputed_output("test_metric", 1, 0, query_end)
                                        .unwrap();
                                    black_box(result);
                                }
                            });
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Cleanup overhead — NoCleanup vs CircularBuffer vs ReadBased
// ---------------------------------------------------------------------------

fn bench_cleanup_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("cleanup_overhead");
    let time_ranges = 200usize;
    let labels = 5usize;

    // NoCleanup
    group.bench_function("no_cleanup", |b| {
        b.iter(|| {
            let config = make_streaming_config_with_cleanup(&[1], "test_metric", None, None);
            let store = SimpleMapStore::new(config, CleanupPolicy::NoCleanup);
            populate_store(&store, time_ranges, labels);
            black_box(&store);
        });
    });

    // CircularBuffer — retain=50 means keep 50 time ranges per (agg_id, label)
    group.bench_function("circular_buffer", |b| {
        b.iter(|| {
            let config = make_streaming_config_with_cleanup(&[1], "test_metric", Some(50), None);
            let store = SimpleMapStore::new(config, CleanupPolicy::CircularBuffer);
            populate_store(&store, time_ranges, labels);
            black_box(&store);
        });
    });

    // ReadBased — threshold=2: populate 500, read twice, then insert 500 more
    group.bench_function("read_based", |b| {
        b.iter(|| {
            let config = make_streaming_config_with_cleanup(&[1], "test_metric", None, Some(2));
            let store = SimpleMapStore::new(config, CleanupPolicy::ReadBased);

            // Phase 1: populate first 100 time ranges
            populate_store(&store, 100, labels);

            // Phase 2: read twice to hit threshold
            let query_end = 100u64 * 1000;
            for _ in 0..2 {
                let _ = store
                    .query_precomputed_output("test_metric", 1, 0, query_end)
                    .unwrap();
            }

            // Phase 3: insert 100 more
            let label_strs: Vec<String> = (0..labels).map(|j| format!("host-{j}")).collect();
            populate_store_with_offset(&store, 100, 200, &label_strs);

            black_box(&store);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Query patterns — varied selectivity
// ---------------------------------------------------------------------------

fn bench_query_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_patterns");
    let time_ranges = 1_000usize;
    let labels = 10usize;
    let total_time = (time_ranges as u64) * 1000;

    let store = build_populated_store(time_ranges, labels);

    // Full scan — 100%
    group.bench_function("full_scan", |b| {
        b.iter(|| {
            let result = store
                .query_precomputed_output(
                    black_box("test_metric"),
                    black_box(1),
                    black_box(0),
                    black_box(total_time),
                )
                .unwrap();
            black_box(result);
        });
    });

    // Wide — 50%
    group.bench_function("wide_50pct", |b| {
        b.iter(|| {
            let result = store
                .query_precomputed_output(
                    black_box("test_metric"),
                    black_box(1),
                    black_box(0),
                    black_box(total_time / 2),
                )
                .unwrap();
            black_box(result);
        });
    });

    // Narrow — 1%
    group.bench_function("narrow_1pct", |b| {
        let narrow_end = total_time / 100;
        b.iter(|| {
            let result = store
                .query_precomputed_output(
                    black_box("test_metric"),
                    black_box(1),
                    black_box(0),
                    black_box(narrow_end),
                )
                .unwrap();
            black_box(result);
        });
    });

    // Miss — query range that doesn't overlap any data
    group.bench_function("miss", |b| {
        let miss_start = total_time + 1_000_000;
        let miss_end = miss_start + 1000;
        b.iter(|| {
            let result = store
                .query_precomputed_output(
                    black_box("test_metric"),
                    black_box(1),
                    black_box(miss_start),
                    black_box(miss_end),
                )
                .unwrap();
            black_box(result);
        });
    });

    // Empty store
    group.bench_function("empty_store", |b| {
        let empty_store = build_populated_store(0, 0);
        b.iter(|| {
            let result = empty_store
                .query_precomputed_output(
                    black_box("test_metric"),
                    black_box(1),
                    black_box(0),
                    black_box(1000),
                )
                .unwrap();
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. High label cardinality — 10 to 5000 labels
// ---------------------------------------------------------------------------

fn bench_high_label_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_label_cardinality");
    let time_ranges = 20usize;

    for label_count in [10, 100, 500, 1000] {
        // Insert sub-benchmark
        group.bench_with_input(
            BenchmarkId::new("insert", label_count),
            &label_count,
            |b, &lc| {
                b.iter(|| {
                    let store = build_populated_store(time_ranges, lc);
                    black_box(&store);
                });
            },
        );

        // Query sub-benchmark
        {
            let store = build_populated_store(time_ranges, label_count);
            let query_end = (time_ranges as u64) * 1000;

            group.bench_with_input(
                BenchmarkId::new("query", label_count),
                &label_count,
                |b, _lc| {
                    b.iter(|| {
                        let result = store
                            .query_precomputed_output(
                                black_box("test_metric"),
                                black_box(1),
                                black_box(0),
                                black_box(query_end),
                            )
                            .unwrap();
                        black_box(result);
                    });
                },
            );
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 8. Multiple aggregation IDs — hot/cold access patterns
// ---------------------------------------------------------------------------

fn bench_multi_agg_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_agg_id");
    let num_agg_ids = 10u64;
    let time_ranges = 100usize;
    let labels = 5usize;
    let agg_ids: Vec<u64> = (1..=num_agg_ids).collect();

    // Insert benchmark — populate all 10 agg IDs
    group.bench_function("insert_10_agg_ids", |b| {
        b.iter(|| {
            let config = make_streaming_config_with_cleanup(&agg_ids, "test_metric", None, None);
            let store = SimpleMapStore::new(config, CleanupPolicy::NoCleanup);

            for &agg_id in &agg_ids {
                for i in 0..time_ranges {
                    let start = (i as u64) * 1000;
                    let end = start + 1000;
                    for j in 0..labels {
                        let (output, acc) = make_output(start, end, &format!("host-{j}"), agg_id);
                        store.insert_precomputed_output(output, acc).unwrap();
                    }
                }
            }

            black_box(&store);
        });
    });

    // Query benchmark — 80% hot (agg_ids 1-2), 20% cold (agg_ids 3-10)
    {
        let config = make_streaming_config_with_cleanup(&agg_ids, "test_metric", None, None);
        let store = SimpleMapStore::new(config, CleanupPolicy::NoCleanup);

        for &agg_id in &agg_ids {
            for i in 0..time_ranges {
                let start = (i as u64) * 1000;
                let end = start + 1000;
                for j in 0..labels {
                    let (output, acc) = make_output(start, end, &format!("host-{j}"), agg_id);
                    store.insert_precomputed_output(output, acc).unwrap();
                }
            }
        }

        let query_end = (time_ranges as u64) * 1000;

        group.bench_function("query_hot_cold", |b| {
            let mut query_idx = 0u64;
            b.iter(|| {
                // 80% hot (agg_ids 1-2), 20% cold (agg_ids 3-10)
                let agg_id = if query_idx % 5 < 4 {
                    (query_idx % 2) + 1 // agg_id 1 or 2
                } else {
                    (query_idx % 8) + 3 // agg_id 3..10
                };
                query_idx += 1;
                let result = store
                    .query_precomputed_output(
                        black_box("test_metric"),
                        black_box(agg_id),
                        black_box(0),
                        black_box(query_end),
                    )
                    .unwrap();
                black_box(result);
            });
        });

        // Concurrent variant — 4 threads with hot/cold pattern
        group.bench_function("concurrent_hot_cold", |b| {
            let num_threads = 4usize;
            let queries_per_thread = 50usize;

            b.iter(|| {
                let barrier = Arc::new(Barrier::new(num_threads));

                std::thread::scope(|s| {
                    for t in 0..num_threads {
                        let store_ref = &store;
                        let barrier_ref = barrier.clone();
                        s.spawn(move || {
                            barrier_ref.wait();
                            for q in 0..queries_per_thread {
                                let idx = (t * queries_per_thread + q) as u64;
                                let agg_id = if idx % 5 < 4 {
                                    (idx % 2) + 1
                                } else {
                                    (idx % 8) + 3
                                };
                                let result = store_ref
                                    .query_precomputed_output("test_metric", agg_id, 0, query_end)
                                    .unwrap();
                                black_box(result);
                            }
                        });
                    }
                });
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Legacy store helpers — use the real deprecated LegacySimpleMapStorePerKey
// ---------------------------------------------------------------------------

#[allow(deprecated)]
fn build_legacy_store(time_ranges: usize, labels: usize) -> LegacySimpleMapStorePerKey {
    let config = make_streaming_config();
    let store = LegacySimpleMapStorePerKey::new(config, CleanupPolicy::NoCleanup);
    for i in 0..time_ranges {
        let start = (i as u64) * 1000;
        let end = start + 1000;
        for j in 0..labels {
            let key = KeyByLabelValues::new_with_labels(vec![format!("host-{j}")]);
            let output = PrecomputedOutput::new(start, end, Some(key), 1);
            let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(1.0));
            store.insert_precomputed_output(output, acc).unwrap();
        }
    }
    store
}

// ---------------------------------------------------------------------------
// Old vs New comparison benchmarks
// ---------------------------------------------------------------------------

#[allow(deprecated)]
fn bench_old_vs_new_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("old_vs_new/insert");

    for &(time_ranges, labels) in &[(10usize, 10usize), (100, 10), (1000, 10)] {
        let total = time_ranges * labels;

        group.bench_with_input(
            BenchmarkId::new("legacy", total),
            &(time_ranges, labels),
            |b, &(tr, l)| {
                b.iter(|| {
                    black_box(build_legacy_store(black_box(tr), black_box(l)));
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("new", total),
            &(time_ranges, labels),
            |b, &(tr, l)| {
                b.iter(|| {
                    black_box(build_populated_store(black_box(tr), black_box(l)));
                });
            },
        );
    }

    group.finish();
}

#[allow(deprecated)]
fn bench_old_vs_new_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("old_vs_new/range_query");
    let time_ranges = 1_000;
    let query_start = 0u64;
    let query_end = (time_ranges as u64) * 1000 / 10;

    for labels in [1, 10, 100] {
        {
            let store = build_legacy_store(time_ranges, labels);
            group.bench_with_input(BenchmarkId::new("legacy", labels), &labels, |b, _| {
                b.iter(|| {
                    black_box(
                        store
                            .query_precomputed_output(
                                black_box("test_metric"),
                                black_box(1),
                                black_box(query_start),
                                black_box(query_end),
                            )
                            .unwrap(),
                    )
                });
            });
        }
        {
            let store = build_populated_store(time_ranges, labels);
            group.bench_with_input(BenchmarkId::new("new", labels), &labels, |b, _| {
                b.iter(|| {
                    black_box(
                        store
                            .query_precomputed_output(
                                black_box("test_metric"),
                                black_box(1),
                                black_box(query_start),
                                black_box(query_end),
                            )
                            .unwrap(),
                    )
                });
            });
        }
    }

    group.finish();
}

#[allow(deprecated)]
fn bench_old_vs_new_exact_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("old_vs_new/exact_query");
    let time_ranges = 1_000;
    let mid = (time_ranges / 2) as u64;
    let exact_start = mid * 1000;
    let exact_end = exact_start + 1000;

    for labels in [1, 10, 100] {
        {
            let store = build_legacy_store(time_ranges, labels);
            group.bench_with_input(BenchmarkId::new("legacy", labels), &labels, |b, _| {
                b.iter(|| {
                    black_box(
                        store
                            .query_precomputed_output_exact(
                                black_box("test_metric"),
                                black_box(1),
                                black_box(exact_start),
                                black_box(exact_end),
                            )
                            .unwrap(),
                    )
                });
            });
        }
        {
            let store = build_populated_store(time_ranges, labels);
            group.bench_with_input(BenchmarkId::new("new", labels), &labels, |b, _| {
                b.iter(|| {
                    black_box(
                        store
                            .query_precomputed_output_exact(
                                black_box("test_metric"),
                                black_box(1),
                                black_box(exact_start),
                                black_box(exact_end),
                            )
                            .unwrap(),
                    )
                });
            });
        }
    }

    group.finish();
}

#[allow(deprecated)]
fn bench_old_vs_new_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("old_vs_new/concurrent_reads");
    let time_ranges = 1_000;
    let labels = 10;
    let query_end = (time_ranges as u64) * 1000 / 10;
    let num_threads = 4;
    let queries_per_thread = 20;

    // Legacy — write lock on every query serialises all concurrent reads
    {
        let store = Arc::new(build_legacy_store(time_ranges, labels));
        group.bench_function("legacy", |b| {
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(num_threads));
                std::thread::scope(|s| {
                    for _ in 0..num_threads {
                        let store_ref = Arc::clone(&store);
                        let barrier_ref = barrier.clone();
                        s.spawn(move || {
                            barrier_ref.wait();
                            for _ in 0..queries_per_thread {
                                black_box(
                                    store_ref
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

    // New — shared read lock per agg_id allows true concurrency
    {
        let store = Arc::new(build_populated_store(time_ranges, labels));
        group.bench_function("new", |b| {
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(num_threads));
                std::thread::scope(|s| {
                    for _ in 0..num_threads {
                        let store_ref = Arc::clone(&store);
                        let barrier_ref = barrier.clone();
                        s.spawn(move || {
                            barrier_ref.wait();
                            for _ in 0..queries_per_thread {
                                black_box(
                                    store_ref
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

#[allow(deprecated)]
fn bench_old_vs_new_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("old_vs_new/scaling");
    let labels = 10;

    for time_ranges in [100usize, 1_000, 10_000] {
        let query_end = (time_ranges as u64) * 1000 / 10;

        {
            let store = build_legacy_store(time_ranges, labels);
            group.bench_with_input(
                BenchmarkId::new("legacy", time_ranges),
                &time_ranges,
                |b, _| {
                    b.iter(|| {
                        black_box(
                            store
                                .query_precomputed_output("test_metric", 1, 0, query_end)
                                .unwrap(),
                        )
                    });
                },
            );
        }
        {
            let store = build_populated_store(time_ranges, labels);
            group.bench_with_input(
                BenchmarkId::new("new", time_ranges),
                &time_ranges,
                |b, _| {
                    b.iter(|| {
                        black_box(
                            store
                                .query_precomputed_output("test_metric", 1, 0, query_end)
                                .unwrap(),
                        )
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_range_query,
    bench_exact_query,
    bench_scaling,
    bench_batch_insert,
    bench_concurrent_writes,
    bench_concurrent_mixed_read_write,
    bench_lock_strategy_comparison,
    bench_cleanup_overhead,
    bench_query_patterns,
    bench_high_label_cardinality,
    bench_multi_agg_id,
    bench_old_vs_new_insert,
    bench_old_vs_new_range_query,
    bench_old_vs_new_exact_query,
    bench_old_vs_new_concurrent_reads,
    bench_old_vs_new_scaling,
);
criterion_main!(benches);

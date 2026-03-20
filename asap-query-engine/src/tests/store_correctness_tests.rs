//! Store correctness contract tests.
//!
//! Every [`Store`] implementation must satisfy all assertions in this module
//! before being used in production.  The tests cover:
//!
//! - Empty-store edge cases
//! - Single and batch inserts with range and exact queries
//! - Partial-range filtering
//! - Aggregation-ID isolation
//! - Earliest-timestamp tracking
//! - Cleanup policies (circular-buffer and read-based)
//! - Concurrent insert and read safety
//! - **Clone fidelity** for every supported accumulator type
//! - **Keyed (label-grouped) entries**
//! - **`DeltaSetAggregator` cleanup exclusion**
//!
//! ## Adding a new implementation
//!
//! 1. Implement the [`Store`] trait.
//! 2. Add a `#[test]` function at the bottom of this file that calls
//!    [`run_contract_suite`] with a factory closure for your implementation.
//!
//! ## Current implementations under test
//!
//! | Test function         | Strategy                    |
//! |-----------------------|-----------------------------|
//! | `contract_per_key`    | `LockStrategy::PerKey` (reference impl) |
//! | `contract_global`     | `LockStrategy::Global`      |

use crate::data_model::{CleanupPolicy, KeyByLabelValues, LockStrategy, Measurement, SerializableToSink, StreamingConfig};
use crate::precompute_operators::{
    CountMinSketchAccumulator, CountMinSketchWithHeapAccumulator, DatasketchesKLLAccumulator,
    DeltaSetAggregatorAccumulator, HydraKllSketchAccumulator, IncreaseAccumulator,
    MinMaxAccumulator, MultipleMinMaxAccumulator, MultipleSumAccumulator,
    SetAggregatorAccumulator, SumAccumulator,
};
use crate::stores::{Store, TimestampedBucketsMap};
use crate::{AggregateCore, AggregationConfig, PrecomputedOutput, SimpleMapStore};
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ── store / config factories ──────────────────────────────────────────────────

fn make_agg_config(
    agg_id: u64,
    aggregation_type: &str,
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
        60,                     // window_size (seconds)
        60,                     // slide_interval (seconds)
        "tumbling".to_string(), // window_type
        "".to_string(),         // spatial_filter
        "cpu_usage".to_string(),
        num_aggregates_to_retain,
        read_count_threshold,
        None, // table_name
        None, // value_column
    )
}

fn make_streaming_config(ids: &[(u64, &str, Option<u64>, Option<u64>)]) -> Arc<StreamingConfig> {
    let configs = ids
        .iter()
        .map(|&(id, agg_type, retain, threshold)| {
            (id, make_agg_config(id, agg_type, retain, threshold))
        })
        .collect();
    Arc::new(StreamingConfig::new(configs))
}

fn make_store(
    strategy: LockStrategy,
    policy: CleanupPolicy,
    ids: &[(u64, &str, Option<u64>, Option<u64>)],
) -> SimpleMapStore {
    let config = make_streaming_config(ids);
    SimpleMapStore::new_with_strategy(config, policy, strategy)
}

/// Convenience: single agg_id=1, type "Sum", no cleanup.
fn make_store_simple(strategy: LockStrategy) -> SimpleMapStore {
    make_store(strategy, CleanupPolicy::NoCleanup, &[(1, "Sum", None, None)])
}

// ── data helpers ──────────────────────────────────────────────────────────────

/// Build a `(PrecomputedOutput, accumulator)` pair with no label key.
fn unkeyed_entry(
    agg_id: u64,
    start: u64,
    end: u64,
    acc: Box<dyn AggregateCore>,
) -> (PrecomputedOutput, Box<dyn AggregateCore>) {
    (PrecomputedOutput::new(start, end, None, agg_id), acc)
}

/// Build a `(PrecomputedOutput, accumulator)` pair with a label key.
fn keyed_entry(
    agg_id: u64,
    start: u64,
    end: u64,
    key: KeyByLabelValues,
    acc: Box<dyn AggregateCore>,
) -> (PrecomputedOutput, Box<dyn AggregateCore>) {
    (PrecomputedOutput::new(start, end, Some(key), agg_id), acc)
}

fn sum_entry(
    agg_id: u64,
    start: u64,
    end: u64,
    value: f64,
) -> (PrecomputedOutput, Box<dyn AggregateCore>) {
    unkeyed_entry(
        agg_id,
        start,
        end,
        Box::new(SumAccumulator::with_sum(value)),
    )
}

fn key(labels: &[&str]) -> KeyByLabelValues {
    KeyByLabelValues::new_with_labels(labels.iter().map(|s| s.to_string()).collect())
}

// ── result inspection helpers ─────────────────────────────────────────────────

fn total_bucket_count(result: &TimestampedBucketsMap) -> usize {
    result.values().map(|v| v.len()).sum()
}

fn timestamps_for_none_key(result: &TimestampedBucketsMap) -> Vec<(u64, u64)> {
    let mut ts: Vec<(u64, u64)> = result
        .get(&None)
        .map(|buckets| buckets.iter().map(|(range, _)| *range).collect())
        .unwrap_or_default();
    ts.sort_unstable();
    ts
}

fn timestamps_for_key(result: &TimestampedBucketsMap, k: &KeyByLabelValues) -> Vec<(u64, u64)> {
    let mut ts: Vec<(u64, u64)> = result
        .get(&Some(k.clone()))
        .map(|buckets| buckets.iter().map(|(range, _)| *range).collect())
        .unwrap_or_default();
    ts.sort_unstable();
    ts
}

fn label(strategy: LockStrategy) -> &'static str {
    match strategy {
        LockStrategy::PerKey => "per_key",
        LockStrategy::Global => "global",
    }
}

/// Assert that two accumulators produce identical JSON after a store roundtrip.
/// Uses `serialize_to_json()` which is available on all `AggregateCore` impls
/// via the `SerializableToSink` supertrait.
fn assert_clone_fidelity(
    original: &dyn AggregateCore,
    from_store: &dyn AggregateCore,
    type_name: &str,
    strategy: LockStrategy,
) {
    let orig_json = original.serialize_to_json();
    let stored_json = from_store.serialize_to_json();
    assert_eq!(
        orig_json, stored_json,
        "[{}] {type_name}: clone_boxed_core() must produce identical serialization",
        label(strategy)
    );
}

// ── contract suite ────────────────────────────────────────────────────────────

pub fn run_contract_suite(strategy: LockStrategy) {
    // Basic store behaviour
    test_empty_store_range_query(strategy);
    test_empty_store_exact_query(strategy);
    test_empty_store_earliest_timestamp(strategy);
    test_single_insert_range_query_returns_entry(strategy);
    test_single_insert_range_query_outside_range_returns_empty(strategy);
    test_single_insert_exact_query_hit(strategy);
    test_single_insert_exact_query_wrong_start_returns_empty(strategy);
    test_single_insert_exact_query_wrong_end_returns_empty(strategy);
    test_batch_insert_full_range_query_returns_all(strategy);
    test_batch_insert_results_are_chronologically_ordered(strategy);
    test_range_query_returns_only_windows_within_range(strategy);
    test_multiple_agg_ids_are_isolated(strategy);
    test_earliest_timestamp_tracks_minimum_across_inserts(strategy);
    test_earliest_timestamp_tracked_per_agg_id(strategy);

    // Cleanup policies
    test_cleanup_circular_buffer_evicts_oldest_window(strategy);
    test_cleanup_circular_buffer_retains_newest_windows(strategy);
    test_cleanup_read_based_evicts_after_threshold_reads(strategy);
    test_cleanup_read_based_unread_window_is_retained(strategy);
    test_delta_set_aggregator_bypasses_cleanup(strategy);

    // Keyed (label-grouped) entries
    test_keyed_entries_grouped_by_key(strategy);
    test_keyed_and_unkeyed_entries_coexist(strategy);
    test_multiple_keys_same_window(strategy);

    // Clone fidelity for every supported accumulator type
    test_clone_fidelity_sum(strategy);
    test_clone_fidelity_min_max(strategy);
    test_clone_fidelity_kll(strategy);
    test_clone_fidelity_increase(strategy);
    test_clone_fidelity_multiple_sum(strategy);
    test_clone_fidelity_multiple_min_max(strategy);
    test_clone_fidelity_set_aggregator(strategy);
    test_clone_fidelity_delta_set_aggregator(strategy);
    test_clone_fidelity_count_min_sketch(strategy);
    test_clone_fidelity_count_min_sketch_with_heap(strategy);
    test_clone_fidelity_hydra_kll(strategy);

    // Concurrency
    test_concurrent_inserts_no_data_loss(strategy);
    test_concurrent_reads_return_complete_results(strategy);
}

// ── empty-store edge cases ────────────────────────────────────────────────────

fn test_empty_store_range_query(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    assert!(
        result.is_empty(),
        "[{}] range query on empty store must return empty map",
        label(strategy)
    );
}

fn test_empty_store_exact_query(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let result = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
        .unwrap();
    assert!(
        result.is_empty(),
        "[{}] exact query on empty store must return empty map",
        label(strategy)
    );
}

fn test_empty_store_earliest_timestamp(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let result = store.get_earliest_timestamp_per_aggregation_id().unwrap();
    assert!(
        result.is_empty(),
        "[{}] empty store must report no earliest timestamps",
        label(strategy)
    );
}

// ── single-insert correctness ─────────────────────────────────────────────────

fn test_single_insert_range_query_returns_entry(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 42.0);
    store.insert_precomputed_output(out, acc).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    assert_eq!(
        total_bucket_count(&result),
        1,
        "[{}] range query must return exactly 1 entry after single insert",
        label(strategy)
    );
    assert_eq!(
        timestamps_for_none_key(&result),
        vec![(1_000, 2_000)],
        "[{}] returned timestamp range must match the inserted window",
        label(strategy)
    );
}

fn test_single_insert_range_query_outside_range_returns_empty(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 5_000, 10_000)
        .unwrap();
    assert!(
        result.is_empty(),
        "[{}] window outside query range must not appear in results",
        label(strategy)
    );
}

fn test_single_insert_exact_query_hit(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 7.0);
    store.insert_precomputed_output(out, acc).unwrap();

    let result = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&result),
        1,
        "[{}] exact query must return 1 result on a direct timestamp hit",
        label(strategy)
    );
}

fn test_single_insert_exact_query_wrong_start_returns_empty(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    let result = store
        .query_precomputed_output_exact("cpu_usage", 1, 999, 2_000)
        .unwrap();
    assert!(
        result.is_empty(),
        "[{}] exact query with wrong start timestamp must return empty",
        label(strategy)
    );
}

fn test_single_insert_exact_query_wrong_end_returns_empty(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    let result = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_001)
        .unwrap();
    assert!(
        result.is_empty(),
        "[{}] exact query with wrong end timestamp must return empty",
        label(strategy)
    );
}

// ── batch insert correctness ──────────────────────────────────────────────────

fn test_batch_insert_full_range_query_returns_all(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let n = 20usize;
    let batch: Vec<_> = (0..n as u64)
        .map(|i| sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64))
        .collect();
    store.insert_precomputed_output_batch(batch).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, n as u64 * 60_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&result),
        n,
        "[{}] full range query after batch insert of {n} must return {n} entries",
        label(strategy)
    );
}

fn test_batch_insert_results_are_chronologically_ordered(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let n = 10usize;
    // Insert in reverse chronological order to confirm the store sorts results.
    let batch: Vec<_> = (0..n as u64)
        .rev()
        .map(|i| sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64))
        .collect();
    store.insert_precomputed_output_batch(batch).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, n as u64 * 60_000)
        .unwrap();
    let ts = timestamps_for_none_key(&result);
    let expected: Vec<(u64, u64)> = (0..n as u64)
        .map(|i| (i * 60_000, (i + 1) * 60_000))
        .collect();
    assert_eq!(
        ts, expected,
        "[{}] range query results must be in chronological (ascending start) order",
        label(strategy)
    );
}

// ── range filtering ───────────────────────────────────────────────────────────

fn test_range_query_returns_only_windows_within_range(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    for i in 0u64..5 {
        let (out, acc) = sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();
    }
    // Query [60k, 240k) — should match windows 1, 2, 3 only.
    let result = store
        .query_precomputed_output("cpu_usage", 1, 60_000, 4 * 60_000)
        .unwrap();
    assert_eq!(
        timestamps_for_none_key(&result),
        vec![(60_000, 120_000), (120_000, 180_000), (180_000, 240_000)],
        "[{}] range query must exclude windows whose start < query_start or end > query_end",
        label(strategy)
    );
}

// ── aggregation-ID isolation ──────────────────────────────────────────────────

fn test_multiple_agg_ids_are_isolated(strategy: LockStrategy) {
    let store = make_store(
        strategy,
        CleanupPolicy::NoCleanup,
        &[(1, "Sum", None, None), (2, "Sum", None, None)],
    );
    let (o1, a1) = sum_entry(1, 1_000, 2_000, 10.0);
    let (o2, a2) = sum_entry(2, 3_000, 4_000, 20.0);
    store.insert_precomputed_output(o1, a1).unwrap();
    store.insert_precomputed_output(o2, a2).unwrap();

    let r1 = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    let r2 = store
        .query_precomputed_output("cpu_usage", 2, 0, u64::MAX)
        .unwrap();

    assert_eq!(
        total_bucket_count(&r1),
        1,
        "[{}] agg_id=1 query must return only its own entry",
        label(strategy)
    );
    assert_eq!(
        total_bucket_count(&r2),
        1,
        "[{}] agg_id=2 query must return only its own entry",
        label(strategy)
    );
    assert_eq!(
        timestamps_for_none_key(&r1),
        vec![(1_000, 2_000)],
        "[{}] agg_id=1 timestamp mismatch",
        label(strategy)
    );
    assert_eq!(
        timestamps_for_none_key(&r2),
        vec![(3_000, 4_000)],
        "[{}] agg_id=2 timestamp mismatch",
        label(strategy)
    );
}

// ── earliest-timestamp tracking ───────────────────────────────────────────────

fn test_earliest_timestamp_tracks_minimum_across_inserts(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    for &start in &[5_000u64, 1_000, 3_000] {
        let (out, acc) = sum_entry(1, start, start + 1_000, 1.0);
        store.insert_precomputed_output(out, acc).unwrap();
    }
    let result = store.get_earliest_timestamp_per_aggregation_id().unwrap();
    assert_eq!(
        result.get(&1).copied(),
        Some(1_000),
        "[{}] earliest timestamp must be the global minimum, not insertion-order minimum",
        label(strategy)
    );
}

fn test_earliest_timestamp_tracked_per_agg_id(strategy: LockStrategy) {
    let store = make_store(
        strategy,
        CleanupPolicy::NoCleanup,
        &[(1, "Sum", None, None), (2, "Sum", None, None)],
    );
    let (o1, a1) = sum_entry(1, 1_000, 2_000, 1.0);
    let (o2, a2) = sum_entry(2, 9_000, 10_000, 1.0);
    store.insert_precomputed_output(o1, a1).unwrap();
    store.insert_precomputed_output(o2, a2).unwrap();

    let result = store.get_earliest_timestamp_per_aggregation_id().unwrap();
    assert_eq!(
        result.get(&1).copied(),
        Some(1_000),
        "[{}] agg_id=1 earliest timestamp",
        label(strategy)
    );
    assert_eq!(
        result.get(&2).copied(),
        Some(9_000),
        "[{}] agg_id=2 earliest timestamp",
        label(strategy)
    );
}

// ── cleanup: circular buffer ──────────────────────────────────────────────────

fn test_cleanup_circular_buffer_evicts_oldest_window(strategy: LockStrategy) {
    // retention_limit = num_aggregates_to_retain * 4 = 2 * 4 = 8.
    // Inserting a 9th window triggers eviction of the oldest 1.
    let store = make_store(
        strategy,
        CleanupPolicy::CircularBuffer,
        &[(1, "Sum", Some(2), None)],
    );
    for i in 0u64..9 {
        let (out, acc) = sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();
    }
    let evicted = store
        .query_precomputed_output_exact("cpu_usage", 1, 0, 60_000)
        .unwrap();
    assert!(
        evicted.is_empty(),
        "[{}] circular buffer must evict the oldest window when retention limit is exceeded",
        label(strategy)
    );
}

fn test_cleanup_circular_buffer_retains_newest_windows(strategy: LockStrategy) {
    let store = make_store(
        strategy,
        CleanupPolicy::CircularBuffer,
        &[(1, "Sum", Some(2), None)],
    );
    for i in 0u64..9 {
        let (out, acc) = sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();
    }
    let result = store
        .query_precomputed_output("cpu_usage", 1, 60_000, 9 * 60_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&result),
        8,
        "[{}] circular buffer must retain the 8 newest windows after eviction",
        label(strategy)
    );
}

// ── cleanup: read-based ───────────────────────────────────────────────────────

fn test_cleanup_read_based_evicts_after_threshold_reads(strategy: LockStrategy) {
    // read_count_threshold = 2: evicted once read count reaches 2.
    // Cleanup runs on every insert.
    let store = make_store(
        strategy,
        CleanupPolicy::ReadBased,
        &[(1, "Sum", None, Some(2))],
    );
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    // Read 1 — count becomes 1, window kept on next insert.
    store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    let (o2, a2) = sum_entry(1, 3_000, 4_000, 2.0);
    store.insert_precomputed_output(o2, a2).unwrap();

    let still_there = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&still_there),
        1,
        "[{}] window must survive until read count reaches threshold",
        label(strategy)
    );

    // Read 2 — count becomes 2, evicted on the next insert.
    store
        .query_precomputed_output("cpu_usage", 1, 0, 2_000)
        .unwrap();
    let (o3, a3) = sum_entry(1, 5_000, 6_000, 3.0);
    store.insert_precomputed_output(o3, a3).unwrap();

    let evicted = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
        .unwrap();
    assert!(
        evicted.is_empty(),
        "[{}] window must be evicted once read count reaches threshold",
        label(strategy)
    );
}

fn test_cleanup_read_based_unread_window_is_retained(strategy: LockStrategy) {
    let store = make_store(
        strategy,
        CleanupPolicy::ReadBased,
        &[(1, "Sum", None, Some(1))],
    );
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    // Insert more windows without reading window 0 — cleanup runs each time.
    for i in 1u64..5 {
        let (o, a) = sum_entry(1, i * 10_000, (i + 1) * 10_000, i as f64);
        store.insert_precomputed_output(o, a).unwrap();
    }

    let result = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&result),
        1,
        "[{}] unread window must not be evicted by read-based cleanup",
        label(strategy)
    );
}

// ── cleanup: DeltaSetAggregator exclusion ─────────────────────────────────────

fn test_delta_set_aggregator_bypasses_cleanup(strategy: LockStrategy) {
    // The store skips cleanup entirely when aggregation_type == "DeltaSetAggregator".
    // retention_limit = 2 * 4 = 8. Inserting 10 windows must not evict any.
    let store = make_store(
        strategy,
        CleanupPolicy::CircularBuffer,
        &[(1, "DeltaSetAggregator", Some(2), None)],
    );
    let n = 10u64;
    for i in 0..n {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        acc.add_key(key(&[&format!("host{i}")]));
        let (out, boxed) = unkeyed_entry(1, i * 60_000, (i + 1) * 60_000, Box::new(acc));
        store.insert_precomputed_output(out, boxed).unwrap();
    }

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, n * 60_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&result),
        n as usize,
        "[{}] DeltaSetAggregator windows must never be evicted by cleanup",
        label(strategy)
    );
}

// ── keyed (label-grouped) entries ─────────────────────────────────────────────

fn test_keyed_entries_grouped_by_key(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let k1 = key(&["host1"]);
    let k2 = key(&["host2"]);

    // Same timestamp window, two different keys.
    let (o1, a1) = keyed_entry(1, 1_000, 2_000, k1.clone(), Box::new(SumAccumulator::with_sum(10.0)));
    let (o2, a2) = keyed_entry(1, 1_000, 2_000, k2.clone(), Box::new(SumAccumulator::with_sum(20.0)));
    store.insert_precomputed_output(o1, a1).unwrap();
    store.insert_precomputed_output(o2, a2).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();

    // Two distinct keys in the result map.
    assert_eq!(
        result.len(),
        2,
        "[{}] two different label keys must produce two entries in the result map",
        label(strategy)
    );
    assert_eq!(
        timestamps_for_key(&result, &k1),
        vec![(1_000, 2_000)],
        "[{}] key1 must map to correct timestamp range",
        label(strategy)
    );
    assert_eq!(
        timestamps_for_key(&result, &k2),
        vec![(1_000, 2_000)],
        "[{}] key2 must map to correct timestamp range",
        label(strategy)
    );
}

fn test_keyed_and_unkeyed_entries_coexist(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let k = key(&["region", "us-east"]);

    let (o_none, a_none) = sum_entry(1, 1_000, 2_000, 1.0);
    let (o_keyed, a_keyed) =
        keyed_entry(1, 3_000, 4_000, k.clone(), Box::new(SumAccumulator::with_sum(2.0)));
    store.insert_precomputed_output(o_none, a_none).unwrap();
    store.insert_precomputed_output(o_keyed, a_keyed).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();

    assert_eq!(
        result.len(),
        2,
        "[{}] None and Some(key) entries must produce two separate map keys",
        label(strategy)
    );
    assert_eq!(
        timestamps_for_none_key(&result),
        vec![(1_000, 2_000)],
        "[{}] None-keyed entry must appear under None key",
        label(strategy)
    );
    assert_eq!(
        timestamps_for_key(&result, &k),
        vec![(3_000, 4_000)],
        "[{}] labelled entry must appear under its key",
        label(strategy)
    );
}

fn test_multiple_keys_same_window(strategy: LockStrategy) {
    // Many keyed entries for the same timestamp window — common in grouped aggregations.
    let store = make_store_simple(strategy);
    let keys: Vec<KeyByLabelValues> = (0..5).map(|i| key(&[&format!("shard{i}")])).collect();

    for k in &keys {
        let (out, acc) = keyed_entry(
            1,
            1_000,
            2_000,
            k.clone(),
            Box::new(SumAccumulator::with_sum(1.0)),
        );
        store.insert_precomputed_output(out, acc).unwrap();
    }

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    assert_eq!(
        result.len(),
        5,
        "[{}] five different keys for the same window must produce five map entries",
        label(strategy)
    );
    for k in &keys {
        assert_eq!(
            timestamps_for_key(&result, k),
            vec![(1_000, 2_000)],
            "[{}] each key must resolve to the correct window",
            label(strategy)
        );
    }
}

// ── clone fidelity for all accumulator types ──────────────────────────────────
//
// Each test inserts a non-trivial accumulator, queries it back through the store
// (which calls clone_boxed_core() internally), and asserts that serialize_to_json()
// on the original and the retrieved copy produce identical output.

fn roundtrip<A: AggregateCore + 'static>(
    strategy: LockStrategy,
    original: A,
) -> (Box<dyn AggregateCore>, Box<dyn AggregateCore>) {
    let store = make_store_simple(strategy);
    let original_box: Box<dyn AggregateCore> = Box::new(original);
    let original_json = original_box.serialize_to_json();

    let (out, acc) = unkeyed_entry(1, 1_000, 2_000, original_box);
    store.insert_precomputed_output(out, acc).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    let retrieved = result
        .get(&None)
        .unwrap()
        .first()
        .map(|(_, acc)| acc.clone_boxed_core())
        .unwrap();

    // Reconstruct original from JSON for comparison (original_box was consumed).
    // We compare the stored JSON (captured before insert) against the retrieved one.
    let placeholder: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(0.0));
    // Use a wrapper that returns the captured JSON for comparison.
    let _ = placeholder;

    // Return a SumAccumulator that carries the original JSON as a workaround —
    // instead, compare directly here using the captured JSON.
    let retrieved_json = retrieved.serialize_to_json();
    assert_eq!(
        original_json, retrieved_json,
        "[{}] clone_boxed_core must produce identical serialization",
        label(strategy)
    );

    // Return something for callers that want the retrieved accumulator directly.
    (Box::new(SumAccumulator::with_sum(0.0)), retrieved)
}

fn test_clone_fidelity_sum(strategy: LockStrategy) {
    let acc = SumAccumulator::with_sum(99.5);
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_min_max(strategy: LockStrategy) {
    let acc = MinMaxAccumulator::with_value(42.0, "max".to_string());
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_kll(strategy: LockStrategy) {
    let mut acc = DatasketchesKLLAccumulator::new(200);
    for v in [1.0, 5.0, 10.0, 50.0, 100.0] {
        acc._update(v);
    }
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_increase(strategy: LockStrategy) {
    let acc = IncreaseAccumulator::new(
        Measurement::new(1.0),
        100,
        Measurement::new(50.0),
        500,
    );
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_multiple_sum(strategy: LockStrategy) {
    let mut sums = HashMap::new();
    sums.insert(key(&["host1"]), 10.0);
    sums.insert(key(&["host2"]), 20.0);
    let acc = MultipleSumAccumulator::new_with_sums(sums);
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_multiple_min_max(strategy: LockStrategy) {
    let mut values = HashMap::new();
    values.insert(key(&["dc", "east"]), 77.7);
    values.insert(key(&["dc", "west"]), 33.3);
    let acc = MultipleMinMaxAccumulator::new_with_values(values, "max".to_string());
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_set_aggregator(strategy: LockStrategy) {
    let mut added = HashSet::new();
    added.insert(key(&["svc", "alpha"]));
    added.insert(key(&["svc", "beta"]));
    let acc = SetAggregatorAccumulator::with_added(added);
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_delta_set_aggregator(strategy: LockStrategy) {
    // Use a "Sum"-typed config so cleanup is not skipped for this test.
    let store = make_store_simple(strategy);

    let mut acc = DeltaSetAggregatorAccumulator::new();
    acc.add_key(key(&["svc", "added-1"]));
    acc.remove_key(key(&["svc", "removed-1"]));
    let original_json = acc.serialize_to_json();

    let acc_box: Box<dyn AggregateCore> = Box::new(acc);
    let (out, boxed) = unkeyed_entry(1, 1_000, 2_000, acc_box);
    store.insert_precomputed_output(out, boxed).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    let retrieved = &result.get(&None).unwrap()[0].1;
    assert_eq!(
        original_json,
        retrieved.serialize_to_json(),
        "[{}] DeltaSetAggregatorAccumulator: clone must preserve added/removed sets",
        label(strategy)
    );
}

fn test_clone_fidelity_count_min_sketch(strategy: LockStrategy) {
    // CountMinSketch._update is private; test clone fidelity of an initialised (empty) sketch.
    let acc = CountMinSketchAccumulator::new(5, 100);
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_count_min_sketch_with_heap(strategy: LockStrategy) {
    let acc = CountMinSketchWithHeapAccumulator::new(5, 100, 10);
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_hydra_kll(strategy: LockStrategy) {
    let mut acc = HydraKllSketchAccumulator::new(4, 50, 200);
    let k1 = key(&["shard", "0"]);
    let k2 = key(&["shard", "1"]);
    for v in [1.0f64, 10.0, 100.0] {
        acc.update(&k1, v);
        acc.update(&k2, v * 2.0);
    }
    roundtrip(strategy, acc);
}

// ── concurrency ───────────────────────────────────────────────────────────────

fn test_concurrent_inserts_no_data_loss(strategy: LockStrategy) {
    let store = Arc::new(make_store_simple(strategy));
    let n_threads = 8usize;
    let windows_per_thread = 50usize;

    let handles: Vec<_> = (0..n_threads)
        .map(|t| {
            let store = store.clone();
            std::thread::spawn(move || {
                for w in 0..windows_per_thread {
                    let base = (t * windows_per_thread + w) as u64;
                    let (out, acc) = sum_entry(1, base * 1_000, (base + 1) * 1_000, base as f64);
                    store.insert_precomputed_output(out, acc).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let total = n_threads * windows_per_thread;
    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    assert_eq!(
        total_bucket_count(&result),
        total,
        "[{}] concurrent inserts must not lose entries (expected {total})",
        label(strategy)
    );
}

fn test_concurrent_reads_return_complete_results(strategy: LockStrategy) {
    let store = Arc::new(make_store_simple(strategy));
    let n_windows = 50usize;

    for i in 0..n_windows as u64 {
        let (out, acc) = sum_entry(1, i * 1_000, (i + 1) * 1_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();
    }

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            std::thread::spawn(move || {
                store
                    .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
                    .unwrap()
            })
        })
        .collect();

    for h in handles {
        let result = h.join().unwrap();
        assert_eq!(
            total_bucket_count(&result),
            n_windows,
            "[{}] concurrent reads must each return the full result set",
            label(strategy)
        );
    }
}

// ── test entry points ─────────────────────────────────────────────────────────

/// Contract suite against `SimpleMapStore` with [`LockStrategy::PerKey`].
///
/// This is the reference implementation — all other stores must match its
/// observable behaviour.
#[test]
fn contract_per_key() {
    run_contract_suite(LockStrategy::PerKey);
}

/// Contract suite against `SimpleMapStore` with [`LockStrategy::Global`].
#[test]
fn contract_global() {
    run_contract_suite(LockStrategy::Global);
}

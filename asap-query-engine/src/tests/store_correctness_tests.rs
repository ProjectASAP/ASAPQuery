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

use crate::data_model::{
    AggregationType, CleanupPolicy, KeyByLabelValues, LockStrategy, Measurement,
    SerializableToSink, StreamingConfig, WindowType,
};
use crate::precompute_operators::{
    CountMinSketchAccumulator, CountMinSketchWithHeapAccumulator, DatasketchesKLLAccumulator,
    DeltaSetAggregatorAccumulator, HydraKllSketchAccumulator, IncreaseAccumulator,
    MinMaxAccumulator, MultipleMinMaxAccumulator, MultipleSumAccumulator, SetAggregatorAccumulator,
    SumAccumulator,
};
use crate::stores::{Store, TimestampedBucketsMap};
use crate::{AggregateCore, AggregationConfig, PrecomputedOutput, SimpleMapStore};
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ── store / config factories ──────────────────────────────────────────────────

fn make_agg_config(
    agg_id: u64,
    aggregation_type: AggregationType,
    num_aggregates_to_retain: Option<u64>,
    read_count_threshold: Option<u64>,
) -> AggregationConfig {
    AggregationConfig::new(
        agg_id,
        aggregation_type,
        "".to_string(),
        HashMap::new(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        "".to_string(),
        60_000,               // window_size_ms
        60_000,               // slide_interval_ms
        WindowType::Tumbling, // window_type
        "".to_string(),       // spatial_filter
        "cpu_usage".to_string(),
        num_aggregates_to_retain,
        read_count_threshold,
        None, // table_name
        None, // value_column
    )
}

fn make_streaming_config(
    ids: &[(u64, AggregationType, Option<u64>, Option<u64>)],
) -> Arc<StreamingConfig> {
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
    ids: &[(u64, AggregationType, Option<u64>, Option<u64>)],
) -> SimpleMapStore {
    let config = make_streaming_config(ids);
    SimpleMapStore::new_with_strategy(config, policy, strategy)
}

/// Convenience: single agg_id=1, type Sum, no cleanup.
fn make_store_simple(strategy: LockStrategy) -> SimpleMapStore {
    make_store(
        strategy,
        CleanupPolicy::NoCleanup,
        &[(1, AggregationType::Sum, None, None)],
    )
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

    // Exact-query cache correctness (Issue: query_precomputed_output_exact over-locking)
    test_exact_query_is_stable_across_repeated_calls(strategy);
    test_exact_query_sees_window_inserted_after_a_prior_exact_query(strategy);
    test_exact_query_miss_then_hit_after_insert(strategy);
    test_exact_query_correct_across_interleaved_inserts_and_queries(strategy);
    test_exact_query_correct_after_epoch_rotation(strategy);

    // Batched exact-query correctness (#609)
    test_batch_exact_query_empty_windows_returns_empty(strategy);
    test_batch_exact_query_unknown_metric_returns_empty(strategy);
    test_batch_exact_query_invalid_window_is_skipped_not_erroring(strategy);
    test_batch_exact_query_equivalent_to_sequential_calls(strategy);
    test_batch_exact_query_updates_read_counts_for_cleanup(strategy);

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
    test_buckets_returned_in_chronological_order_after_epoch_rotation(strategy);

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

// ── exact-query cache correctness ─────────────────────────────────────────────
//
// `query_precomputed_output_exact` is used by sliding-window instant queries to
// fetch a precompute with an exactly-matching timestamp range, no merging. Some
// implementations (PerKey's `MutableEpoch`) maintain a lazy internal lookup
// index for this path, built on first use and invalidated on the next insert.
// These tests pin the observable contract that must hold regardless of how
// (or whether) that caching is implemented: exact queries must always reflect
// the store's true current contents, never a stale snapshot from before the
// most recent write, and must remain correct across repeated calls.

fn test_exact_query_is_stable_across_repeated_calls(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 42.0);
    store.insert_precomputed_output(out, acc).unwrap();

    let mut jsons = Vec::new();
    for call in 0..5 {
        let result = store
            .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
            .unwrap();
        assert_eq!(
            total_bucket_count(&result),
            1,
            "[{}] call #{call}: repeated exact query must keep finding the inserted window",
            label(strategy)
        );
        jsons.push(result.get(&None).unwrap()[0].1.serialize_to_json().unwrap());
    }
    assert!(
        jsons.iter().all(|j| j == &jsons[0]),
        "[{}] repeated exact queries for the same window must return identical values \
         across calls (any internal lookup cache must not corrupt results)",
        label(strategy)
    );
}

fn test_exact_query_sees_window_inserted_after_a_prior_exact_query(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out1, acc1) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out1, acc1).unwrap();

    // First exact query — on implementations with a lazy lookup index (e.g. PerKey's
    // `MutableEpoch::exact_query`), this call is what builds/populates that index.
    let first = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&first),
        1,
        "[{}] sanity: first exact query must find the inserted window",
        label(strategy)
    );

    // A brand-new window inserted after that must be visible to exact queries —
    // whatever cache the query above populated must not shadow it.
    let (out2, acc2) = sum_entry(1, 3_000, 4_000, 2.0);
    store.insert_precomputed_output(out2, acc2).unwrap();

    let second = store
        .query_precomputed_output_exact("cpu_usage", 1, 3_000, 4_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&second),
        1,
        "[{}] exact query must find a window inserted after a previous exact query \
         populated any internal cache — must not return stale/empty results",
        label(strategy)
    );

    // The original window must still be correctly retrievable too.
    let first_again = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&first_again),
        1,
        "[{}] original window must remain correctly retrievable after a later insert",
        label(strategy)
    );
}

fn test_exact_query_miss_then_hit_after_insert(strategy: LockStrategy) {
    let store = make_store_simple(strategy);

    // Query for a window that doesn't exist yet. On PerKey this still builds
    // (an empty-for-this-range) lazy index as a side effect.
    let miss = store
        .query_precomputed_output_exact("cpu_usage", 1, 5_000, 6_000)
        .unwrap();
    assert!(
        miss.is_empty(),
        "[{}] query for a nonexistent window must be empty",
        label(strategy)
    );

    // Now insert exactly that window.
    let (out, acc) = sum_entry(1, 5_000, 6_000, 9.0);
    store.insert_precomputed_output(out, acc).unwrap();

    let hit = store
        .query_precomputed_output_exact("cpu_usage", 1, 5_000, 6_000)
        .unwrap();
    assert_eq!(
        total_bucket_count(&hit),
        1,
        "[{}] a window inserted after a prior miss must be found on the next exact query \
         — any cache built by the miss must be invalidated by the insert",
        label(strategy)
    );
}

fn test_exact_query_correct_across_interleaved_inserts_and_queries(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let n = 30u64;
    for i in 0..n {
        let (out, acc) = sum_entry(1, i * 1_000, (i + 1) * 1_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();

        // After each insert, re-verify every window inserted so far — including this
        // one — is retrievable with its own correct value. This interleaves cache
        // invalidation (insert) with cache use (exact query) on every iteration, and
        // checks that a rebuilt lookup index never mixes up offsets across windows.
        for j in 0..=i {
            let result = store
                .query_precomputed_output_exact("cpu_usage", 1, j * 1_000, (j + 1) * 1_000)
                .unwrap();
            assert_eq!(
                total_bucket_count(&result),
                1,
                "[{}] window {j} must be retrievable after inserting window {i}",
                label(strategy)
            );
            let expected = SumAccumulator::with_sum(j as f64)
                .serialize_to_json()
                .unwrap();
            let actual = result.get(&None).unwrap()[0].1.serialize_to_json().unwrap();
            assert_eq!(
                actual,
                expected,
                "[{}] window {j} must return its own value, not another window's, \
                 after inserting window {i}",
                label(strategy)
            );
        }
    }
}

fn test_exact_query_correct_after_epoch_rotation(strategy: LockStrategy) {
    // capacity=2, max_epochs=4 (hardcoded in StoreKeyData/PerKeyState) =>
    // retention_limit = 8. Inserting 10 windows evicts exactly the oldest 2
    // (windows 0 and 1, both in the oldest sealed epoch), leaving windows
    // 2..10 spread across sealed epochs and the current (still-open) epoch.
    let store = make_store(
        strategy,
        CleanupPolicy::CircularBuffer,
        &[(1, AggregationType::Sum, Some(2), None)],
    );
    let n = 10u64;
    for i in 0..n {
        let (out, acc) = sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();
    }

    for i in 0u64..2 {
        let evicted = store
            .query_precomputed_output_exact("cpu_usage", 1, i * 60_000, (i + 1) * 60_000)
            .unwrap();
        assert!(
            evicted.is_empty(),
            "[{}] window {i} must have been evicted by circular-buffer rotation",
            label(strategy)
        );
    }

    for i in 2u64..n {
        let result = store
            .query_precomputed_output_exact("cpu_usage", 1, i * 60_000, (i + 1) * 60_000)
            .unwrap();
        assert_eq!(
            total_bucket_count(&result),
            1,
            "[{}] window {i} must be retrievable via exact query after epoch rotation",
            label(strategy)
        );
        let expected = SumAccumulator::with_sum(i as f64)
            .serialize_to_json()
            .unwrap();
        let actual = result.get(&None).unwrap()[0].1.serialize_to_json().unwrap();
        assert_eq!(
            actual,
            expected,
            "[{}] window {i} must return its own value after epoch rotation, \
             not a value from a sealed epoch's stale offset",
            label(strategy)
        );
    }
}

// ── batched exact-query correctness (#609) ───────────────────────────────────
//
// `query_precomputed_output_exact_batch` must be observationally identical to
// calling `query_precomputed_output_exact` once per window and merging the
// results — the only difference is that it takes the shard lock once for the
// whole slice instead of once per window.

fn test_batch_exact_query_empty_windows_returns_empty(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    let result = store
        .query_precomputed_output_exact_batch("cpu_usage", 1, &[])
        .unwrap();
    assert!(
        result.is_empty(),
        "[{}] an empty windows slice must return an empty map, even with data present",
        label(strategy)
    );
}

fn test_batch_exact_query_unknown_metric_returns_empty(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let result = store
        .query_precomputed_output_exact_batch("cpu_usage", 1, &[(1_000, 2_000), (3_000, 4_000)])
        .unwrap();
    assert!(
        result.is_empty(),
        "[{}] batched exact query against an aggregation_id with no data must return empty",
        label(strategy)
    );
}

fn test_batch_exact_query_invalid_window_is_skipped_not_erroring(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    // First window is invalid (start > end); must be skipped, not fail the whole batch.
    let result = store
        .query_precomputed_output_exact_batch("cpu_usage", 1, &[(2_000, 1_000), (1_000, 2_000)])
        .unwrap();
    assert_eq!(
        total_bucket_count(&result),
        1,
        "[{}] an invalid window in the batch must be skipped, not fail or drop valid windows",
        label(strategy)
    );
}

fn test_batch_exact_query_equivalent_to_sequential_calls(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let a = key(&["a"]);
    let b = key(&["b"]);
    store
        .insert_precomputed_output(
            PrecomputedOutput::new(1_000, 2_000, Some(a.clone()), 1),
            Box::new(SumAccumulator::with_sum(10.0)),
        )
        .unwrap();
    store
        .insert_precomputed_output(
            PrecomputedOutput::new(2_000, 3_000, Some(b.clone()), 1),
            Box::new(SumAccumulator::with_sum(20.0)),
        )
        .unwrap();
    store
        .insert_precomputed_output(
            PrecomputedOutput::new(3_000, 4_000, Some(a.clone()), 1),
            Box::new(SumAccumulator::with_sum(30.0)),
        )
        .unwrap();

    // Windows include a miss (4_000, 5_000) between two hits, mirroring a real
    // fetch_window_grid_via_exact_lookups grid walk over a range with a gap.
    let windows = [
        (1_000, 2_000),
        (2_000, 3_000),
        (4_000, 5_000),
        (3_000, 4_000),
    ];

    let batched = store
        .query_precomputed_output_exact_batch("cpu_usage", 1, &windows)
        .unwrap();

    let mut sequential: TimestampedBucketsMap = HashMap::new();
    for &(start, end) in &windows {
        let partial = store
            .query_precomputed_output_exact("cpu_usage", 1, start, end)
            .unwrap();
        for (k, buckets) in partial {
            sequential.entry(k).or_default().extend(buckets);
        }
    }

    assert_eq!(
        timestamps_for_key(&batched, &a),
        timestamps_for_key(&sequential, &a),
        "[{}] batched result for key 'a' must match sequential per-window calls merged together",
        label(strategy)
    );
    assert_eq!(
        timestamps_for_key(&batched, &b),
        timestamps_for_key(&sequential, &b),
        "[{}] batched result for key 'b' must match sequential per-window calls merged together",
        label(strategy)
    );
    assert_eq!(
        total_bucket_count(&batched),
        3,
        "[{}] batched call must find exactly the 3 hits among the 4 requested windows",
        label(strategy)
    );
}

fn test_batch_exact_query_updates_read_counts_for_cleanup(strategy: LockStrategy) {
    // read_count_threshold = 2: mirrors test_cleanup_read_based_evicts_after_threshold_reads,
    // but drives the reads through the batch call instead of one-at-a-time, to pin that
    // batching still updates read_counts per matched window (not e.g. once per batch call).
    let store = make_store(
        strategy,
        CleanupPolicy::ReadBased,
        &[(1, AggregationType::Sum, None, Some(2))],
    );
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    // Read 1 via the batch call — count becomes 1.
    store
        .query_precomputed_output_exact_batch("cpu_usage", 1, &[(1_000, 2_000)])
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

    // Read 2 via the batch call — count becomes 2, evicted on the next insert.
    store
        .query_precomputed_output_exact_batch("cpu_usage", 1, &[(1_000, 2_000)])
        .unwrap();
    let (o3, a3) = sum_entry(1, 5_000, 6_000, 3.0);
    store.insert_precomputed_output(o3, a3).unwrap();

    let evicted = store
        .query_precomputed_output_exact("cpu_usage", 1, 1_000, 2_000)
        .unwrap();
    assert!(
        evicted.is_empty(),
        "[{}] window must be evicted once batched reads bring its count to threshold",
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
        ts,
        expected,
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
        &[
            (1, AggregationType::Sum, None, None),
            (2, AggregationType::Sum, None, None),
        ],
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
        &[
            (1, AggregationType::Sum, None, None),
            (2, AggregationType::Sum, None, None),
        ],
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
        &[(1, AggregationType::Sum, Some(2), None)],
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
        &[(1, AggregationType::Sum, Some(2), None)],
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
        &[(1, AggregationType::Sum, None, Some(2))],
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
        &[(1, AggregationType::Sum, None, Some(1))],
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
        &[(1, AggregationType::DeltaSetAggregator, Some(2), None)],
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

/// Bug #586 (#2): once epoch rotation has occurred, `query_precomputed_output`
/// checks the current (newest, still-open) epoch first, then sealed epochs
/// oldest-to-newest — so the concatenated result is
/// `[newest][oldest sealed]..[newest sealed]`, not chronological.
///
/// Uses a plain `Sum` aggregation (not `DeltaSetAggregator`) because
/// `DeltaSetAggregator` is unconditionally exempted from epoch rotation in
/// `insert_for_store_key` (it must retain its full history, so it never
/// seals) — meaning this ordering defect can't currently be reached through
/// the public `Store` API for that type. It's still a live bug in the
/// general `query_precomputed_output` contract for any type that *does*
/// rotate, and it's exactly what will start silently corrupting results the
/// moment an order-sensitive accumulator (`DeltaSetAggregator` included, if
/// its rotation exemption is ever relaxed) hits this path.
///
/// capacity=2 with 7 inserts forces 3 epoch seals, leaving exactly 1 window
/// in the current epoch (the newest) alongside 3 sealed epochs (the 6
/// oldest) — the exact shape under which "current checked first" prepends a
/// newer window ahead of older ones.
///
/// Deliberately does NOT use `timestamps_for_none_key` — that helper sorts
/// before returning, which would mask exactly the bug this test exists to
/// catch.
fn test_buckets_returned_in_chronological_order_after_epoch_rotation(strategy: LockStrategy) {
    let store = make_store(
        strategy,
        CleanupPolicy::CircularBuffer,
        &[(1, AggregationType::Sum, Some(2), None)],
    );
    let n = 7u64;
    for i in 0..n {
        let (out, acc) = sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();
    }

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, n * 60_000)
        .unwrap();
    let returned_order: Vec<(u64, u64)> = result
        .get(&None)
        .expect("windows must be present under the None key")
        .iter()
        .map(|(range, _)| *range)
        .collect();
    assert_eq!(
        returned_order.len(),
        n as usize,
        "[{}] no windows should have been evicted yet (7 <= retention_limit 8)",
        label(strategy)
    );

    let mut chronological = returned_order.clone();
    chronological.sort_unstable();
    assert_eq!(
        returned_order,
        chronological,
        "[{}] buckets must be returned in chronological (ascending start) order \
         even after epoch rotation has occurred",
        label(strategy)
    );
}

// ── keyed (label-grouped) entries ─────────────────────────────────────────────

fn test_keyed_entries_grouped_by_key(strategy: LockStrategy) {
    let store = make_store_simple(strategy);
    let k1 = key(&["host1"]);
    let k2 = key(&["host2"]);

    // Same timestamp window, two different keys.
    let (o1, a1) = keyed_entry(
        1,
        1_000,
        2_000,
        k1.clone(),
        Box::new(SumAccumulator::with_sum(10.0)),
    );
    let (o2, a2) = keyed_entry(
        1,
        1_000,
        2_000,
        k2.clone(),
        Box::new(SumAccumulator::with_sum(20.0)),
    );
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
    let (o_keyed, a_keyed) = keyed_entry(
        1,
        3_000,
        4_000,
        k.clone(),
        Box::new(SumAccumulator::with_sum(2.0)),
    );
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
    let original_json = original_box.serialize_to_json().unwrap();

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
    let retrieved_json = retrieved.serialize_to_json().unwrap();
    assert_eq!(
        original_json,
        retrieved_json,
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
    let acc = MinMaxAccumulator::with_value(42.0, "max".to_string()).unwrap();
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_kll(strategy: LockStrategy) {
    let mut acc = DatasketchesKLLAccumulator::new(200);
    for v in [1.0, 5.0, 10.0, 50.0, 100.0] {
        acc.update(v);
    }
    roundtrip(strategy, acc);
}

fn test_clone_fidelity_increase(strategy: LockStrategy) {
    let acc = IncreaseAccumulator::new(Measurement::new(1.0), 100, Measurement::new(50.0), 500);
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
    let acc = MultipleMinMaxAccumulator::new_with_values(values, "max".to_string()).unwrap();
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
    let original_json = acc.serialize_to_json().unwrap();

    let acc_box: Box<dyn AggregateCore> = Box::new(acc);
    let (out, boxed) = unkeyed_entry(1, 1_000, 2_000, acc_box);
    store.insert_precomputed_output(out, boxed).unwrap();

    let result = store
        .query_precomputed_output("cpu_usage", 1, 0, u64::MAX)
        .unwrap();
    let retrieved = &result.get(&None).unwrap()[0].1;
    assert_eq!(
        original_json,
        retrieved.serialize_to_json().unwrap(),
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

// ── lock-contention characterization (PerKey only) ─────────────────────────────
//
// `SimpleMapStoreGlobal` has no lock granularity to have a bug in (one giant
// `Mutex` for everything), so this section targets `LockStrategy::PerKey` only.

/// Characterizes a known over-locking bug in `SimpleMapStorePerKey`:
/// `query_precomputed_output_exact` takes the shard's `RwLock` as a *write*
/// lock (see `per_key.rs`, driven by `MutableEpoch::exact_query` taking
/// `&mut self` to lazily build/cache an internal lookup index) even though it
/// never mutates any queryable data. `query_precomputed_output` correctly
/// takes only a `.read()` lock on the same shard.
///
/// Consequently, a long-running exact query currently blocks — rather than
/// runs concurrently with — a cheap, unrelated range query on the same
/// aggregation shard.
///
/// # Method
///
/// 1. Insert a large number of distinct windows into aggregation_id=1's
///    current (still-open) epoch, then insert one more to guarantee the
///    lazy `window_to_ids` lookup index is invalidated. This makes the next
///    exact query pay a full O(current-epoch-size) index rebuild under
///    whatever lock it takes — a large, unambiguous, easily measured
///    critical section (tens of milliseconds), instead of a cheap cache hit.
/// 2. Spawn reader threads that continuously issue range queries for a time
///    range with **no overlap with any inserted data**. Per `per_key.rs`,
///    this still requires acquiring the shard's lock (the `DashMap` entry
///    exists), but the scan itself is skipped via an O(1) time-bounds check
///    — so each call's *uncontended* cost is on the order of microseconds,
///    regardless of how much data the shard holds.
/// 3. While readers are looping, issue the single expensive exact query and
///    record its duration.
/// 4. Track the maximum single-call latency observed by any reader across
///    the whole run.
///
/// A cheap, non-blocked reader call should never take anywhere near as long
/// as the exact query's own multi-millisecond lock hold — regardless of that
/// duration — because a read lock only excludes writers, not other readers.
/// If exact queries instead exclude readers (the bug), at least one reader
/// call will be observed stalled for a duration comparable to the exact
/// query's, since it has to wait out the writer's turn before proceeding.
///
/// # Expected result on the current (pre-fix) implementation
///
/// FAILS: `max_reader_latency` comes out a large fraction of `exact_duration`
/// (in practice, close to 100% — some reader gets stuck waiting the entire
/// time) instead of staying near the reader's own uncontended cost. This
/// assertion encodes the *desired* post-fix behavior, so it is expected to
/// start passing once `query_precomputed_output_exact` no longer requires a
/// write lock for this path.
///
/// # Flakiness note
///
/// This is a timing-based test. `n_windows` is chosen large enough that the
/// forced index rebuild takes tens of milliseconds — comfortably above
/// normal OS scheduling jitter (typically sub-millisecond to a few ms) — so
/// the 20% threshold has a wide margin in both directions. Extreme host
/// contention (e.g. a heavily oversubscribed CI runner) could in principle
/// still perturb timing; if this test flakes, prefer raising `n_windows` or
/// loosening the threshold over deleting it.
#[test]
fn test_exact_query_does_not_block_concurrent_range_queries_per_key() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    let n_windows = 500_000u64;
    let store = Arc::new(make_store(
        LockStrategy::PerKey,
        CleanupPolicy::NoCleanup,
        &[(1, AggregationType::Sum, None, None)],
    ));
    let batch: Vec<_> = (0..n_windows)
        .map(|i| sum_entry(1, i * 10, i * 10 + 1, i as f64))
        .collect();
    store.insert_precomputed_output_batch(batch).unwrap();

    // One more insert to guarantee the next exact query's lazy index is
    // invalidated and must be rebuilt from scratch under its lock.
    let extra_ts = n_windows * 10;
    let (out, acc) = sum_entry(1, extra_ts, extra_ts + 1, 0.0);
    store.insert_precomputed_output(out, acc).unwrap();

    // Sanity: the exact query must still find its window (also serves as a
    // warm-up call, though it is not the one that gets timed below).
    let sanity = store
        .query_precomputed_output_exact("cpu_usage", 1, extra_ts, extra_ts + 1)
        .unwrap();
    assert_eq!(total_bucket_count(&sanity), 1);

    // A second insert to invalidate the index again for the timed call.
    let extra_ts2 = extra_ts + 1;
    let (out2, acc2) = sum_entry(1, extra_ts2, extra_ts2 + 1, 0.0);
    store.insert_precomputed_output(out2, acc2).unwrap();

    // Readers query a range far outside all inserted data (max inserted
    // timestamp is extra_ts2 + 1), so the per-epoch time-bounds check skips
    // any real scan — this call is cheap purely from lock acquisition +
    // bookkeeping, independent of shard size.
    let far_start = u64::MAX - 1_000;
    let far_end = u64::MAX;

    let stop = Arc::new(AtomicBool::new(false));
    let max_reader_latency_ns = Arc::new(AtomicU64::new(0));
    let n_readers = 4;

    let reader_handles: Vec<_> = (0..n_readers)
        .map(|_| {
            let store = store.clone();
            let stop = stop.clone();
            let max_latency = max_reader_latency_ns.clone();
            std::thread::spawn(move || {
                let mut iters = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let call_start = Instant::now();
                    let result = store
                        .query_precomputed_output("cpu_usage", 1, far_start, far_end)
                        .unwrap();
                    debug_assert!(result.is_empty());
                    let elapsed_ns = call_start.elapsed().as_nanos() as u64;
                    max_latency.fetch_max(elapsed_ns, Ordering::Relaxed);
                    iters += 1;
                }
                iters
            })
        })
        .collect();

    // Let readers start looping before the exact query fires.
    std::thread::sleep(Duration::from_millis(20));

    let exact_start = Instant::now();
    let result = store
        .query_precomputed_output_exact("cpu_usage", 1, extra_ts2, extra_ts2 + 1)
        .unwrap();
    let exact_duration = exact_start.elapsed();
    assert_eq!(
        total_bucket_count(&result),
        1,
        "sanity: the timed exact query must still find its window"
    );

    // Let readers keep looping briefly after, then stop them.
    std::thread::sleep(Duration::from_millis(20));
    stop.store(true, Ordering::Relaxed);
    let total_reader_iters: u64 = reader_handles.into_iter().map(|h| h.join().unwrap()).sum();

    let max_reader_latency = Duration::from_nanos(max_reader_latency_ns.load(Ordering::Relaxed));

    eprintln!(
        "[PerKey exact-query lock contention] exact_duration={:?}, \
         max_reader_latency={:?}, total_reader_iters={total_reader_iters} \
         (ratio max_reader_latency/exact_duration = {:.3})",
        exact_duration,
        max_reader_latency,
        max_reader_latency.as_secs_f64() / exact_duration.as_secs_f64().max(1e-12)
    );

    // Sanity floor: make sure reader threads actually ran and recorded a sample.
    // Without this, a run where every reader got starved of scheduler time during
    // the timed window would leave max_reader_latency_ns at its initial 0, and the
    // ratio assertion below would pass vacuously (0 <= anything) without having
    // measured contention at all.
    assert!(
        total_reader_iters >= n_readers as u64 && max_reader_latency_ns.load(Ordering::Relaxed) > 0,
        "no reader thread recorded a latency sample (total_reader_iters={total_reader_iters}) \
         — readers may have been starved of scheduler time during this run, so it \
         doesn't validate anything; rerun, or investigate scheduler contention on this host"
    );

    // Sanity floor: make sure the forced rebuild actually took long enough
    // for this to be a meaningful signal (not swallowed by noise).
    assert!(
        exact_duration >= Duration::from_millis(1),
        "exact query completed in {:?}, too fast to reliably characterize lock \
         contention — increase n_windows",
        exact_duration
    );

    assert!(
        max_reader_latency.as_nanos() * 5 <= exact_duration.as_nanos(),
        "a concurrent range query was stalled for {:?} while a single exact query \
         ran for {:?} (ratio {:.3}, allowed <= 0.20) — this indicates \
         query_precomputed_output_exact is blocking concurrent range queries on \
         the same PerKey shard for the duration of its lock hold. This failure is \
         expected on the current implementation (which takes a write lock for \
         exact queries even though it doesn't mutate queryable data); it should \
         start passing once that lock is narrowed to a read lock.",
        max_reader_latency,
        exact_duration,
        max_reader_latency.as_secs_f64() / exact_duration.as_secs_f64().max(1e-12)
    );
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

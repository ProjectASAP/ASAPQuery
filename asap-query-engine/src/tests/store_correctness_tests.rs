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

use crate::data_model::{CleanupPolicy, LockStrategy, StreamingConfig};
use crate::precompute_operators::SumAccumulator;
use crate::stores::{Store, TimestampedBucketsMap};
use crate::{AggregateCore, AggregationConfig, PrecomputedOutput, SimpleMapStore};
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::HashMap;
use std::sync::Arc;

// ── store / config factories ──────────────────────────────────────────────────

/// Build an `AggregationConfig` for a single aggregation ID with optional
/// retention / read-threshold limits.
fn make_agg_config(
    agg_id: u64,
    num_aggregates_to_retain: Option<u64>,
    read_count_threshold: Option<u64>,
) -> AggregationConfig {
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
        num_aggregates_to_retain,
        read_count_threshold,
        None, // table_name
        None, // value_column
    )
}

/// Build a `StreamingConfig` from a slice of `(agg_id, retain, read_threshold)`.
fn make_streaming_config(ids: &[(u64, Option<u64>, Option<u64>)]) -> Arc<StreamingConfig> {
    let configs = ids
        .iter()
        .map(|&(id, retain, threshold)| (id, make_agg_config(id, retain, threshold)))
        .collect();
    Arc::new(StreamingConfig::new(configs))
}

/// Build a `SimpleMapStore` with explicit cleanup policy and aggregation IDs.
fn make_store(
    strategy: LockStrategy,
    policy: CleanupPolicy,
    ids: &[(u64, Option<u64>, Option<u64>)],
) -> SimpleMapStore {
    let config = make_streaming_config(ids);
    SimpleMapStore::new_with_strategy(config, policy, strategy)
}

/// Convenience: single agg_id=1, no cleanup.
fn make_store_simple(strategy: LockStrategy) -> SimpleMapStore {
    make_store(strategy, CleanupPolicy::NoCleanup, &[(1, None, None)])
}

// ── data helpers ──────────────────────────────────────────────────────────────

/// Build a single `(PrecomputedOutput, SumAccumulator)` pair with no label key.
fn sum_entry(
    agg_id: u64,
    start: u64,
    end: u64,
    value: f64,
) -> (PrecomputedOutput, Box<dyn AggregateCore>) {
    let output = PrecomputedOutput::new(start, end, None, agg_id);
    let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(value));
    (output, acc)
}

// ── result inspection helpers ─────────────────────────────────────────────────

/// Total number of accumulator entries across all label keys.
fn total_bucket_count(result: &TimestampedBucketsMap) -> usize {
    result.values().map(|v| v.len()).sum()
}

/// Sorted `(start, end)` timestamp ranges for the `None`-keyed (unkeyed) bucket list.
fn timestamps_for_none_key(result: &TimestampedBucketsMap) -> Vec<(u64, u64)> {
    let mut ts: Vec<(u64, u64)> = result
        .get(&None)
        .map(|buckets| buckets.iter().map(|(range, _)| *range).collect())
        .unwrap_or_default();
    ts.sort_unstable();
    ts
}

/// Human-readable label for a lock strategy (used in assertion messages).
fn label(strategy: LockStrategy) -> &'static str {
    match strategy {
        LockStrategy::PerKey => "per_key",
        LockStrategy::Global => "global",
    }
}

// ── contract suite ────────────────────────────────────────────────────────────

/// Run every contract test against a store built with `strategy`.
///
/// Call this from a `#[test]` function to register a new implementation.
pub fn run_contract_suite(strategy: LockStrategy) {
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
    test_cleanup_circular_buffer_evicts_oldest_window(strategy);
    test_cleanup_circular_buffer_retains_newest_windows(strategy);
    test_cleanup_read_based_evicts_after_threshold_reads(strategy);
    test_cleanup_read_based_unread_window_is_retained(strategy);
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

    // Query a range that does not cover [1_000, 2_000].
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
    // Insert in reverse chronological order to confirm sorting.
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
    // Insert 5 windows: [0,60k), [60k,120k), [120k,180k), [180k,240k), [240k,300k)
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
        &[(1, None, None), (2, None, None)],
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
    // Insert in a non-monotone order so the minimum is not simply the last write.
    for &start in &[5_000u64, 1_000, 3_000] {
        let (out, acc) = sum_entry(1, start, start + 1_000, 1.0);
        store.insert_precomputed_output(out, acc).unwrap();
    }
    let result = store.get_earliest_timestamp_per_aggregation_id().unwrap();
    assert_eq!(
        result.get(&1).copied(),
        Some(1_000),
        "[{}] earliest timestamp must be the global minimum, not the insertion order minimum",
        label(strategy)
    );
}

fn test_earliest_timestamp_tracked_per_agg_id(strategy: LockStrategy) {
    let store = make_store(
        strategy,
        CleanupPolicy::NoCleanup,
        &[(1, None, None), (2, None, None)],
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
        &[(1, Some(2), None)],
    );
    for i in 0u64..9 {
        let (out, acc) = sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();
    }

    // Window 0: [0, 60_000) must have been evicted.
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
    // Same setup as above: retention_limit = 8, insert 9.
    let store = make_store(
        strategy,
        CleanupPolicy::CircularBuffer,
        &[(1, Some(2), None)],
    );
    for i in 0u64..9 {
        let (out, acc) = sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64);
        store.insert_precomputed_output(out, acc).unwrap();
    }

    // Windows 1–8 must still be present.
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
    // read_count_threshold = 2: a window is evicted once its read count reaches 2.
    // Cleanup runs on every insert, so we need an insert after the threshold is met.
    let store = make_store(strategy, CleanupPolicy::ReadBased, &[(1, None, Some(2))]);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    // Read 1 — count becomes 1 (< threshold 2), window kept on next insert.
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

    // Read 2 — count becomes 2 (== threshold), evicted on the next insert.
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
    // A window that has never been read must not be evicted by read-based cleanup.
    let store = make_store(strategy, CleanupPolicy::ReadBased, &[(1, None, Some(1))]);
    let (out, acc) = sum_entry(1, 1_000, 2_000, 1.0);
    store.insert_precomputed_output(out, acc).unwrap();

    // Insert more windows without ever reading window 0 — cleanup runs each time.
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
                    // Each thread writes to a unique timestamp range — no conflicts.
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

//! Native pipeline merge tests (issue #567, Stage 1).
//!
//! `execute_and_merge_store_queries`'s Sliding-window branch
//! (`simple_engine/mod.rs`) must merge every precomputed bucket returned for
//! a key, not just the first one. The store's `query_precomputed_output_exact`
//! can legitimately return more than one bucket for the same key under one
//! exact window (see `per_key.rs::query_precomputed_output_exact`), and
//! DataFusion's `SummaryMergeMultipleExec` already merges all of them
//! correctly — native must match.

use crate::data_model::{AggregationType, WindowType};
use crate::engines::query_result::InstantVectorElement;
use crate::engines::simple_engine::SimpleEngine;
use crate::precompute_operators::sum_accumulator::SumAccumulator;
use crate::tests::test_utilities::engine_factories::create_engine_multi_timestamp_with_window;

const QUERY_TIME: f64 = 1000.0; // -> data time 1_000_000ms, see convert_query_time_to_data_time
const DATA_TIME: u64 = 1_000_000;
const SLIDING_WINDOW_MS: u64 = 1_000; // matches create_engine_multi_timestamp_with_window's fixed bucket width

/// Runs a query through the native pipeline (`execute_query_pipeline`), the
/// same path `execute_and_merge_store_queries` is reached from.
fn execute_native(
    engine: &SimpleEngine,
    query: &str,
    query_time_sec: f64,
) -> Vec<InstantVectorElement> {
    let context = engine
        .build_query_execution_context_promql(query.to_string(), query_time_sec)
        .expect("Failed to build context");
    engine
        .execute_query_pipeline(&context, false, false)
        .expect("execute_query_pipeline failed")
}

#[tokio::test]
async fn sliding_single_bucket_returns_its_value() {
    let data = vec![(
        DATA_TIME,
        Some(vec!["host-a".to_string()]),
        Box::new(SumAccumulator::with_sum(42.0)) as Box<dyn crate::AggregateCore>,
    )];
    let query = "sum_over_time(http_requests[1s])";
    let engine = create_engine_multi_timestamp_with_window(
        "http_requests",
        AggregationType::Sum,
        vec!["host"],
        data,
        query,
        SLIDING_WINDOW_MS,
        WindowType::Sliding,
    );

    let results = execute_native(&engine, query, QUERY_TIME);
    assert_eq!(results.len(), 1);
    assert!((results[0].value - 42.0).abs() < 1e-10);
}

#[tokio::test]
async fn sliding_two_buckets_for_same_key_are_merged_not_dropped() {
    // Two precomputed buckets land under the same key and the same exact
    // window (both at DATA_TIME). Today's code takes the first and warns;
    // it must merge both.
    let data = vec![
        (
            DATA_TIME,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
        ),
        (
            DATA_TIME,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn crate::AggregateCore>,
        ),
    ];
    let query = "sum_over_time(http_requests[1s])";
    let engine = create_engine_multi_timestamp_with_window(
        "http_requests",
        AggregationType::Sum,
        vec!["host"],
        data,
        query,
        SLIDING_WINDOW_MS,
        WindowType::Sliding,
    );

    let results = execute_native(&engine, query, QUERY_TIME);
    assert_eq!(results.len(), 1, "expected one merged result for host-a");
    assert!(
        (results[0].value - 15.0).abs() < 1e-10,
        "expected both buckets merged into 15.0, got {}",
        results[0].value
    );
}

#[tokio::test]
async fn sliding_bucket_count_mismatch_still_returns_merged_result() {
    // 3 buckets (not just 2) for one key: generalizes #2 beyond the
    // exactly-one-extra case, and confirms a mismatch never errors/drops —
    // it merges everything and only warns.
    let data = vec![
        (
            DATA_TIME,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
        ),
        (
            DATA_TIME,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn crate::AggregateCore>,
        ),
        (
            DATA_TIME,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(3.0)) as Box<dyn crate::AggregateCore>,
        ),
    ];
    let query = "sum_over_time(http_requests[1s])";
    let engine = create_engine_multi_timestamp_with_window(
        "http_requests",
        AggregationType::Sum,
        vec!["host"],
        data,
        query,
        SLIDING_WINDOW_MS,
        WindowType::Sliding,
    );

    let results = execute_native(&engine, query, QUERY_TIME);
    assert_eq!(results.len(), 1);
    assert!(
        (results[0].value - 18.0).abs() < 1e-10,
        "expected all 3 buckets merged into 18.0, got {}",
        results[0].value
    );
}

#[tokio::test]
async fn tumbling_multi_bucket_merge_unaffected_by_sliding_fix() {
    // Regression guard: the Sliding-branch edit lives in the same `if` as
    // the Tumbling branch below it — prove Tumbling's (already-correct)
    // multi-timestamp merge is untouched, through the native pipeline.
    let timestamps = [996_000u64, 997_000, 998_000, 999_000, 1_000_000];
    let data = timestamps
        .iter()
        .map(|&ts| {
            (
                ts,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
            )
        })
        .collect();
    let query = "sum_over_time(http_requests[5s])";
    let engine = create_engine_multi_timestamp_with_window(
        "http_requests",
        AggregationType::Sum,
        vec!["host"],
        data,
        query,
        // window_size_ms < query range so do_merge=true. Equal (5s window,
        // 5s range) hits a separate, pre-existing panic — see #569, not this stage.
        1_000,
        WindowType::Tumbling,
    );

    let results = execute_native(&engine, query, QUERY_TIME);
    assert_eq!(results.len(), 1);
    assert!(
        (results[0].value - 50.0).abs() < 1e-10,
        "expected 5 timestamps merged into 50.0, got {}",
        results[0].value
    );
}

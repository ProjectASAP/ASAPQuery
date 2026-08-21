//! End-to-end correctness tests for sliding-window instant-query execution
//! (issue #557). `should_use_sliding_window()` is hardcoded `false`, so the
//! live planner can't emit a `Sliding` config yet — these hand-construct one
//! directly and register it via `query_configs` (which `promql.rs` checks
//! before falling back to capability matching), the same bypass
//! `capability_matching_tests.rs` uses, so `window_compatible`'s still-strict
//! Sliding rule (unrelaxed until #557's PR D) never gets in the way.
//!
//! Every bucket carries the same single grouping-label key (`pod="a"`)
//! rather than no key at all: `format_final_results` (pre-existing,
//! unrelated to #557) drops `None`-keyed entries, and every other test in
//! this codebase that asserts on actual result *values* uses a real key for
//! the same reason — a `None` key is for genuinely unresolvable results,
//! not "no grouping requested".

use crate::data_model::{
    AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
    KeyByLabelValues, PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage, SchemaConfig,
    StreamingConfig, WindowType,
};
use crate::engines::query_result::QueryResult;
use crate::engines::simple_engine::SimpleEngine;
use crate::precompute_operators::sum_accumulator::SumAccumulator;
use crate::stores::simple_map_store::SimpleMapStore;
use crate::stores::traits::Store;
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::HashMap;
use std::sync::Arc;

const METRIC: &str = "reqs";
const INGESTION_INTERVAL_MS: u64 = 1000;

fn key() -> KeyByLabelValues {
    KeyByLabelValues::new_with_labels(vec!["a".to_string()])
}

/// Builds a `SimpleEngine` with one `Sliding` aggregation (`window_size_ms`,
/// `slide_interval_ms`) and one `Sum` bucket inserted at every
/// `slide_interval_ms` from 0 up to (not including) `bucket_count *
/// slide_interval_ms`. Bucket `i`'s value is `(i + 1) as f64`, distinct per
/// bucket so a test can tell exactly which ones got merged.
fn engine_with_sliding_buckets(
    window_size_ms: u64,
    slide_interval_ms: u64,
    bucket_count: u64,
    promql_query: &str,
) -> SimpleEngine {
    engine_with_sliding_buckets_missing(
        window_size_ms,
        slide_interval_ms,
        bucket_count,
        &[],
        promql_query,
    )
}

/// Same as `engine_with_sliding_buckets`, but skips inserting the buckets
/// at the given indices - for testing "partial data is okay" tolerance.
fn engine_with_sliding_buckets_missing(
    window_size_ms: u64,
    slide_interval_ms: u64,
    bucket_count: u64,
    missing: &[u64],
    promql_query: &str,
) -> SimpleEngine {
    let agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type: AggregationType::Sum,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(vec!["pod".to_string()]),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size_ms,
        slide_interval_ms,
        window_type: WindowType::Sliding,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: METRIC.to_string(),
        num_aggregates_to_retain: None,
        read_count_threshold: None,
        table_name: None,
        value_column: None,
    };

    let mut aggregation_configs = HashMap::new();
    aggregation_configs.insert(1u64, agg_config);
    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
    });
    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));

    for i in 0..bucket_count {
        if missing.contains(&i) {
            continue;
        }
        let start = i * slide_interval_ms;
        let output = PrecomputedOutput::new(start, start + window_size_ms, Some(key()), 1);
        let value = (i + 1) as f64;
        store
            .insert_precomputed_output(output, Box::new(SumAccumulator::with_sum(value)))
            .unwrap();
    }

    let promql_schema = PromQLSchema::new().add_metric(
        METRIC.to_string(),
        KeyByLabelNames::new(vec!["pod".to_string()]),
    );
    let query_config = QueryConfig::new(promql_query.to_string())
        .add_aggregation(AggregationReference::new(1, None));
    let inference_config = InferenceConfig {
        schema: SchemaConfig::PromQL(promql_schema),
        query_configs: vec![query_config],
        cleanup_policy: CleanupPolicy::NoCleanup,
    };

    SimpleEngine::new(
        store,
        inference_config,
        streaming_config,
        INGESTION_INTERVAL_MS,
        QueryLanguage::promql,
    )
}

/// Runs `sum_over_time(reqs[range_seconds])` at `query_time_seconds` and
/// returns the single `pod="a"` result value.
fn query_sum(engine: &SimpleEngine, range_seconds: u64, query_time_seconds: f64) -> f64 {
    let query = format!("sum_over_time({METRIC}[{range_seconds}s])");
    let (_labels, result) = engine
        .handle_query_promql(query, query_time_seconds)
        .expect("query should resolve via the registered query_config");
    match result {
        QueryResult::Vector(vector) => {
            assert_eq!(vector.values.len(), 1, "expected exactly one series");
            vector.values[0].value
        }
        other => panic!("expected an instant vector, got {other:?}"),
    }
}

/// For the case where query dispatch/resolution itself fails (unmatched
/// pattern, no compatible aggregation, or the store has literally nothing),
/// distinct from a resolved query that matched zero series (see
/// `query_returns_empty_vector`).
fn query_returns_none(engine: &SimpleEngine, range_seconds: u64, query_time_seconds: f64) -> bool {
    let query = format!("sum_over_time({METRIC}[{range_seconds}s])");
    engine
        .handle_query_promql(query, query_time_seconds)
        .is_none()
}

/// For the case where the query resolves and executes, but no data was
/// usable for it (e.g. the store has buckets, just not at any position the
/// merge walk needed): `handle_query_promql` returns `Some` with an empty
/// vector here, not `None`.
fn query_returns_empty_vector(engine: &SimpleEngine, range_seconds: u64, query_time_seconds: f64) {
    let query = format!("sum_over_time({METRIC}[{range_seconds}s])");
    let (_labels, result) = engine
        .handle_query_promql(query, query_time_seconds)
        .expect("query should resolve (Some) even though no series matched");
    match result {
        QueryResult::Vector(vector) => {
            assert!(
                vector.values.is_empty(),
                "expected zero series, got {:?}",
                vector.values
            );
        }
        other => panic!("expected an instant vector, got {other:?}"),
    }
}

// W=300_000ms (5m), S=60_000ms (1m). Buckets 0..15 cover [0, 900_000).
// Bucket i's start_timestamp is i*60_000 and its value is i+1.
const W: u64 = 300_000;
const S: u64 = 60_000;

#[test]
fn k1_exact_match_uses_single_bucket() {
    // range = W: query at t=300_000 (on-grid) should merge exactly bucket
    // i=0 (start_timestamp=0), value 1 - the pre-#557 exact-match case,
    // now served through the unified walk instead of query_precomputed_output_exact.
    let engine = engine_with_sliding_buckets(W, S, 15, "sum_over_time(reqs[300s])");
    assert_eq!(query_sum(&engine, 300, 300.0), 1.0);
}

#[test]
fn k2_merges_two_strided_buckets_not_all_intermediate_ones() {
    // range = 2*W = 600_000, query at t=600_000. Strided buckets are at
    // start_timestamp 0 (i=0, value 1) and 300_000 (i=5, value 6) = 7.
    // Summing all 10 S-spaced buckets in [0, 600_000) would wrongly give 55.
    let engine = engine_with_sliding_buckets(W, S, 15, "sum_over_time(reqs[600s])");
    assert_eq!(query_sum(&engine, 600, 600.0), 7.0);
}

#[test]
fn k3_merges_three_strided_buckets_not_all_fifteen() {
    // range = 3*W = 900_000, query at t=900_000. Strided buckets: i=0 (1),
    // i=5 (6), i=10 (11) = 18. Summing all 15 would wrongly give 120.
    let engine = engine_with_sliding_buckets(W, S, 15, "sum_over_time(reqs[900s])");
    assert_eq!(query_sum(&engine, 900, 900.0), 18.0);
}

#[test]
fn k6_merges_six_strided_buckets() {
    // range = 6*W = 1_800_000, query at t=1_800_000. Strided buckets:
    // i=0,5,10,15,20,25 -> values 1,6,11,16,21,26 = 81.
    let engine = engine_with_sliding_buckets(W, S, 30, "sum_over_time(reqs[1800s])");
    assert_eq!(query_sum(&engine, 1800, 1800.0), 81.0);
}

#[test]
fn misaligned_query_timestamp_floor_aligns_instead_of_missing_data() {
    // Query at t=305_000 (5s past the 300_000 slide-interval boundary) for
    // range=W=300_000: aligned_end floor-aligns 305_000 -> 300_000, so
    // window_start = 300_000 - 300_000 = 0, and the walk finds bucket i=0
    // (start_timestamp=0, value 1) - the latest complete W-wide window as
    // of the aligned query time. Without the fix, aligned_end would stay
    // 305_000, window_start would be 5_000, and the walk would look up
    // bucket_map[5_000] (nothing there, since buckets only exist on the
    // 60_000 grid) - silently resolving to zero series despite bucket i=0
    // existing.
    let engine = engine_with_sliding_buckets(W, S, 15, "sum_over_time(reqs[300s])");
    assert_eq!(query_sum(&engine, 300, 305.0), 1.0);
}

// --- Negative cases ---

#[test]
fn no_data_anywhere_near_the_window_returns_none() {
    // Buckets only exist in [0, 900_000). Querying far outside that range
    // hits the store's own empty-fetch guard - the query never resolves.
    let engine = engine_with_sliding_buckets(W, S, 15, "sum_over_time(reqs[300s])");
    assert!(query_returns_none(&engine, 300, 90_000.0));
}

#[test]
fn empty_store_returns_none() {
    let engine = engine_with_sliding_buckets(W, S, 0, "sum_over_time(reqs[300s])");
    assert!(query_returns_none(&engine, 300, 300.0));
}

#[test]
fn partial_data_merges_only_the_buckets_that_exist() {
    // k=3 query (strided positions i=0, 5, 10), but i=5 is missing from the
    // store. "Partial data is okay" (design doc §2/§4): the walk should
    // merge just i=0 and i=10 (1 + 11 = 12) instead of erroring outright.
    let engine = engine_with_sliding_buckets_missing(W, S, 15, &[5], "sum_over_time(reqs[900s])");
    assert_eq!(query_sum(&engine, 900, 900.0), 12.0);
}

#[test]
fn all_strided_positions_missing_resolves_to_zero_series() {
    // Only intermediate S-spaced buckets exist (i=1..4), none of the
    // strided positions (i=0, 5, 10) a k=3 query actually needs. The store
    // has data (so this resolves, unlike the empty-store case above), but
    // none of it is usable for this query - must match zero series, not a
    // wrong answer built from buckets outside the stride.
    let engine =
        engine_with_sliding_buckets_missing(W, S, 15, &[0, 5, 10], "sum_over_time(reqs[900s])");
    query_returns_empty_vector(&engine, 900, 900.0);
}

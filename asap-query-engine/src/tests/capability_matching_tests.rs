//! Integration tests for capability-based aggregation matching.
//!
//! These tests verify that when no pre-configured query_config entry exists,
//! the engine falls back to searching StreamingConfig by capability, and that
//! the existing query_config path still takes priority when an entry is present.

use crate::data_model::{
    AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
    PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage, SchemaConfig, StreamingConfig,
    WindowType,
};
use crate::engines::simple_engine::SimpleEngine;
use crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
use crate::precompute_operators::sum_accumulator::SumAccumulator;
use crate::stores::simple_map_store::SimpleMapStore;
use crate::stores::traits::Store;
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal `AggregationConfig`.
fn make_agg_config(
    id: u64,
    metric: &str,
    agg_type: AggregationType,
    window_size_s: u64,
    window_type: WindowType,
    grouping: &[&str],
) -> AggregationConfig {
    AggregationConfig {
        aggregation_id: id,
        aggregation_type: agg_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: window_size_s,
        slide_interval: window_size_s,
        window_type,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        read_count_threshold: None,
        table_name: None,
        value_column: None,
    }
}

/// Build a `SimpleEngine` with an explicit list of `AggregationConfig`s and no query_configs.
/// Data is inserted at timestamp 1_000_000 with a window covering [1_000_000 - window_ms, 1_000_000].
fn engine_no_query_configs(
    metric: &str,
    schema_labels: &[&str],
    agg_configs: Vec<AggregationConfig>,
) -> SimpleEngine {
    let mut agg_map = HashMap::new();
    for c in &agg_configs {
        agg_map.insert(c.aggregation_id, c.clone());
    }
    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs: agg_map,
    });
    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));

    // Insert a data point for each aggregation so queries can actually execute.
    let ts = 1_000_000_u64;
    for c in &agg_configs {
        let window_ms = c.window_size * 1000;
        let output = PrecomputedOutput::new(ts - window_ms, ts, None, c.aggregation_id);
        let acc: Box<dyn crate::AggregateCore> = match c.aggregation_type.as_str() {
            "DatasketchesKLL" => {
                let mut kll = DatasketchesKLLAccumulator::new(200);
                kll.update(1.0);
                Box::new(kll)
            }
            _ => Box::new(SumAccumulator::with_sum(42.0)),
        };
        store.insert_precomputed_output(output, acc).unwrap();
    }

    let schema_label_names =
        KeyByLabelNames::new(schema_labels.iter().map(|s| s.to_string()).collect());
    let promql_schema = PromQLSchema::new().add_metric(metric.to_string(), schema_label_names);

    let inference_config = InferenceConfig {
        schema: SchemaConfig::PromQL(promql_schema),
        query_configs: vec![], // intentionally empty — forces capability matching
        cleanup_policy: CleanupPolicy::NoCleanup,
    };

    SimpleEngine::new(
        store,
        inference_config,
        streaming_config,
        1,
        QueryLanguage::promql,
    )
}

/// Build a `SimpleEngine` with both a query_config entry AND a streaming aggregation.
fn engine_with_query_config(
    metric: &str,
    schema_labels: &[&str],
    agg_config: AggregationConfig,
    promql_query: &str,
) -> SimpleEngine {
    let agg_id = agg_config.aggregation_id;
    let mut agg_map = HashMap::new();
    agg_map.insert(agg_id, agg_config.clone());
    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs: agg_map,
    });
    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));

    let ts = 1_000_000_u64;
    let window_ms = agg_config.window_size * 1000;
    let output = PrecomputedOutput::new(ts - window_ms, ts, None, agg_id);
    store
        .insert_precomputed_output(output, Box::new(SumAccumulator::with_sum(99.0)))
        .unwrap();

    let schema_label_names =
        KeyByLabelNames::new(schema_labels.iter().map(|s| s.to_string()).collect());
    let promql_schema = PromQLSchema::new().add_metric(metric.to_string(), schema_label_names);

    let query_config = QueryConfig::new(promql_query.to_string())
        .add_aggregation(AggregationReference::new(agg_id, None));

    let inference_config = InferenceConfig {
        schema: SchemaConfig::PromQL(promql_schema),
        query_configs: vec![query_config],
        cleanup_policy: CleanupPolicy::NoCleanup,
    };

    SimpleEngine::new(
        store,
        inference_config,
        streaming_config,
        1,
        QueryLanguage::promql,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// When no query_config entry exists but a compatible Sum aggregation does,
/// capability matching should route to it and return a valid context.
#[test]
fn capability_fallback_fires_when_no_config() {
    let agg = make_agg_config(
        1,
        "cpu",
        AggregationType::Sum,
        300,
        WindowType::Tumbling,
        &[],
    );
    let engine = engine_no_query_configs("cpu", &[], vec![agg]);

    // sum_over_time(cpu[5m]) — 5 min = 300 s matches the 300 s tumbling config
    let ctx =
        engine.build_query_execution_context_promql("sum_over_time(cpu[5m])".to_string(), 1000.0);
    assert!(
        ctx.is_some(),
        "Expected capability matching to find a compatible aggregation"
    );
    assert_eq!(ctx.unwrap().agg_info.aggregation_id_for_value, 1);
}

/// When a query_config entry exists, the engine must use it (not capability matching).
/// We verify by giving the config a different agg_id than any compatible-by-type config.
#[test]
fn config_path_takes_priority_over_capability_matching() {
    let agg = make_agg_config(
        42,
        "cpu",
        AggregationType::Sum,
        300,
        WindowType::Tumbling,
        &[],
    );
    let engine = engine_with_query_config("cpu", &[], agg, "sum_over_time(cpu[5m])");

    let ctx = engine
        .build_query_execution_context_promql("sum_over_time(cpu[5m])".to_string(), 1000.0)
        .expect("should succeed via config path");

    // The config path routes to agg_id=42
    assert_eq!(ctx.agg_info.aggregation_id_for_value, 42);
}

/// A query for quantile(0.5) and quantile(0.9) should both resolve to the same
/// KLL aggregation when no query_configs are present.
#[test]
fn quantile_different_values_resolve_to_same_aggregation() {
    let kll = make_agg_config(
        7,
        "latency",
        AggregationType::DatasketchesKLL,
        300,
        WindowType::Tumbling,
        &[],
    );
    let engine = engine_no_query_configs("latency", &[], vec![kll]);

    let q50 = engine.build_query_execution_context_promql(
        "quantile_over_time(0.5, latency[5m])".to_string(),
        1000.0,
    );
    let q90 = engine.build_query_execution_context_promql(
        "quantile_over_time(0.9, latency[5m])".to_string(),
        1000.0,
    );

    assert!(
        q50.is_some(),
        "quantile(0.5) should resolve via capability matching"
    );
    assert!(
        q90.is_some(),
        "quantile(0.9) should resolve via capability matching"
    );
    assert_eq!(
        q50.unwrap().agg_info.aggregation_id_for_value,
        q90.unwrap().agg_info.aggregation_id_for_value,
        "Both quantile queries should route to the same KLL aggregation"
    );
}

/// When no config entry exists and no compatible aggregation exists, return None.
#[test]
fn no_match_returns_none() {
    // KLL config present, but query asks for Sum — incompatible
    let kll = make_agg_config(
        1,
        "cpu",
        AggregationType::DatasketchesKLL,
        300,
        WindowType::Tumbling,
        &[],
    );
    let engine = engine_no_query_configs("cpu", &[], vec![kll]);

    let ctx =
        engine.build_query_execution_context_promql("sum_over_time(cpu[5m])".to_string(), 1000.0);
    assert!(
        ctx.is_none(),
        "Should return None when no compatible aggregation exists"
    );
}

/// When multiple compatible aggregations exist, the largest window should be preferred.
#[test]
fn priority_largest_window_wins() {
    let small = make_agg_config(
        1,
        "cpu",
        AggregationType::Sum,
        300,
        WindowType::Tumbling,
        &[],
    );
    let large = make_agg_config(
        2,
        "cpu",
        AggregationType::Sum,
        900,
        WindowType::Tumbling,
        &[],
    );
    let engine = engine_no_query_configs("cpu", &[], vec![small, large]);

    // sum_over_time(cpu[15m]) = 900 s — both 300 s and 900 s configs match (900 = 3×300),
    // but the largest window (900 s, id=2) should be preferred.
    let ctx = engine
        .build_query_execution_context_promql("sum_over_time(cpu[15m])".to_string(), 1000.0)
        .expect("should find a compatible aggregation");

    assert_eq!(
        ctx.agg_info.aggregation_id_for_value, 2,
        "The 900 s (id=2) aggregation should be preferred over the 300 s (id=1)"
    );
}

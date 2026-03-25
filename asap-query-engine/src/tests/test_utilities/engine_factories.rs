//! Engine factory helpers for integration tests
//!
//! Provides reusable construction helpers for SimpleEngine + SimpleMapStore
//! populated with various accumulator types. Unlike TestConfigBuilder which
//! hardcodes "SumAccumulator", these helpers build AggregationConfig with
//! the correct aggregation_type string.

use crate::data_model::{
    AggregationConfig, AggregationReference, CleanupPolicy, InferenceConfig, KeyByLabelValues,
    PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage, SchemaConfig, StreamingConfig,
};
use crate::engines::query_result::InstantVectorElement;
use crate::engines::simple_engine::SimpleEngine;
use crate::stores::simple_map_store::SimpleMapStore;
use crate::stores::Store;
use crate::AggregateCore;
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::HashMap;
use std::sync::Arc;

/// Data to insert into a store: (label_values, accumulator)
pub type AccumulatorData = Vec<(Option<Vec<String>>, Box<dyn AggregateCore>)>;

/// Creates a SimpleEngine with a single aggregation populated with given data.
///
/// # Arguments
/// * `metric` - Metric name
/// * `aggregation_type` - Accumulator type string (e.g. "SumAccumulator", "DatasketchesKLLAccumulator")
/// * `grouping_labels` - Label names for GROUP BY
/// * `data` - Vec of (label_values, accumulator) pairs to insert
/// * `promql_query` - The PromQL query string
pub fn create_engine_single_pop(
    metric: &str,
    aggregation_type: &str,
    grouping_labels: Vec<&str>,
    data: AccumulatorData,
    promql_query: &str,
) -> SimpleEngine {
    create_engine_single_pop_with_aggregated(
        metric,
        aggregation_type,
        grouping_labels,
        vec![],
        data,
        promql_query,
    )
}

/// Creates a SimpleEngine with aggregated labels (sub-key labels within the accumulator).
///
/// Use for self-keyed multi-population accumulators (Multiple* types) where
/// `aggregated_labels` are the labels that key the accumulator internally
/// (e.g. "endpoint" within a MultipleIncrease accumulator).
pub fn create_engine_single_pop_with_aggregated(
    metric: &str,
    aggregation_type: &str,
    grouping_labels: Vec<&str>,
    aggregated_labels: Vec<&str>,
    data: AccumulatorData,
    promql_query: &str,
) -> SimpleEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();
    let aggregated_label_strings: Vec<String> =
        aggregated_labels.iter().map(|s| s.to_string()).collect();
    let all_schema_labels: Vec<String> = grouping_label_strings
        .iter()
        .chain(aggregated_label_strings.iter())
        .cloned()
        .collect();

    let mut aggregation_configs = HashMap::new();
    let agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type: aggregation_type.to_string(),
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::new(aggregated_label_strings),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: "tumbling".to_string(),
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        read_count_threshold: None,
        table_name: None,
        value_column: None,
    };
    aggregation_configs.insert(1u64, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
    });

    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));

    // Insert data
    let timestamp = 1_000_000_u64;
    for (label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp, timestamp, key, 1);
        store.insert_precomputed_output(output, acc).unwrap();
    }

    // Create inference config — schema includes grouping + rollup labels
    let promql_schema =
        PromQLSchema::new().add_metric(metric.to_string(), KeyByLabelNames::new(all_schema_labels));

    let query_config = QueryConfig::new(promql_query.to_string())
        .add_aggregation(AggregationReference::new(1, None));

    let inference_config = InferenceConfig {
        schema: SchemaConfig::PromQL(promql_schema),
        query_configs: vec![query_config],
        cleanup_policy: CleanupPolicy::NoCleanup,
    };

    SimpleEngine::new(
        store,
        // None,
        inference_config,
        streaming_config,
        1,
        QueryLanguage::promql,
    )
}

/// Creates a SimpleEngine with dual-input (separate value and keys aggregations).
///
/// # Arguments
/// * `metric` - Metric name
/// * `value_agg_type` - Accumulator type for values (e.g. "HydraKllSketchAccumulator")
/// * `key_agg_type` - Accumulator type for keys (e.g. "DeltaSetAggregator")
/// * `grouping_labels` - Store GROUP BY columns
/// * `aggregated_labels` - Labels that key the accumulator internally (tracked by DeltaSet)
/// * `value_data` - Data for value aggregation (agg_id=1)
/// * `keys_data` - Data for keys aggregation (agg_id=2)
/// * `promql_query` - The PromQL query string
#[allow(clippy::too_many_arguments)]
pub fn create_engine_dual_input(
    metric: &str,
    value_agg_type: &str,
    key_agg_type: &str,
    grouping_labels: Vec<&str>,
    aggregated_labels: Vec<&str>,
    value_data: AccumulatorData,
    keys_data: AccumulatorData,
    promql_query: &str,
) -> SimpleEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();
    let aggregated_label_strings: Vec<String> =
        aggregated_labels.iter().map(|s| s.to_string()).collect();
    let all_labels: Vec<String> = grouping_label_strings
        .iter()
        .chain(aggregated_label_strings.iter())
        .cloned()
        .collect();

    let mut aggregation_configs = HashMap::new();

    // Value aggregation (id=1)
    let value_agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type: value_agg_type.to_string(),
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: "tumbling".to_string(),
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        read_count_threshold: None,
        table_name: None,
        value_column: None,
    };
    aggregation_configs.insert(1u64, value_agg_config);

    // Keys aggregation (id=2)
    let keys_agg_config = AggregationConfig {
        aggregation_id: 2,
        aggregation_type: key_agg_type.to_string(),
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::new(aggregated_label_strings),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: "tumbling".to_string(),
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        read_count_threshold: None,
        table_name: None,
        value_column: None,
    };
    aggregation_configs.insert(2u64, keys_agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
    });

    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));

    // Insert value data
    let timestamp = 1_000_000_u64;
    for (label_values_opt, acc) in value_data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp, timestamp, key, 1);
        store.insert_precomputed_output(output, acc).unwrap();
    }

    // Insert keys data
    for (label_values_opt, acc) in keys_data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp, timestamp, key, 2);
        store.insert_precomputed_output(output, acc).unwrap();
    }

    // Create inference config
    let promql_schema =
        PromQLSchema::new().add_metric(metric.to_string(), KeyByLabelNames::new(all_labels));

    let query_config = QueryConfig::new(promql_query.to_string())
        .add_aggregation(AggregationReference::new(1, None))
        .add_aggregation(AggregationReference::new(2, None));

    let inference_config = InferenceConfig {
        schema: SchemaConfig::PromQL(promql_schema),
        query_configs: vec![query_config],
        cleanup_policy: CleanupPolicy::NoCleanup,
    };

    SimpleEngine::new(
        store,
        // None,
        inference_config,
        streaming_config,
        1,
        QueryLanguage::promql,
    )
}

/// Creates a single-pop engine with data at multiple timestamps for testing merge.
#[allow(clippy::type_complexity)]
pub fn create_engine_multi_timestamp(
    metric: &str,
    aggregation_type: &str,
    grouping_labels: Vec<&str>,
    data: Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>,
    promql_query: &str,
) -> SimpleEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();

    let mut aggregation_configs = HashMap::new();
    let agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type: aggregation_type.to_string(),
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: "tumbling".to_string(),
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        read_count_threshold: None,
        table_name: None,
        value_column: None,
    };
    aggregation_configs.insert(1u64, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
    });

    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));

    for (timestamp, label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp - 1000, timestamp, key, 1);
        store.insert_precomputed_output(output, acc).unwrap();
    }

    let promql_schema = PromQLSchema::new().add_metric(
        metric.to_string(),
        KeyByLabelNames::new(grouping_label_strings),
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
        // None,
        inference_config,
        streaming_config,
        1,
        QueryLanguage::promql,
    )
}

/// Creates a single-pop engine with data at multiple timestamps and configurable window.
///
/// Like `create_engine_multi_timestamp` but allows setting `window_size` and `window_type`
/// on the AggregationConfig (needed for temporal queries like `sum_over_time(metric[5s])`).
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub fn create_engine_multi_timestamp_with_window(
    metric: &str,
    aggregation_type: &str,
    grouping_labels: Vec<&str>,
    data: Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>,
    promql_query: &str,
    window_size: u64,
    window_type: &str,
) -> SimpleEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();

    let mut aggregation_configs = HashMap::new();
    let agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type: aggregation_type.to_string(),
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size,
        slide_interval: 1,
        window_type: window_type.to_string(),
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        read_count_threshold: None,
        table_name: None,
        value_column: None,
    };
    aggregation_configs.insert(1u64, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
    });

    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));

    for (timestamp, label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp - 1000, timestamp, key, 1);
        store.insert_precomputed_output(output, acc).unwrap();
    }

    let promql_schema = PromQLSchema::new().add_metric(
        metric.to_string(),
        KeyByLabelNames::new(grouping_label_strings),
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
        // None,
        inference_config,
        streaming_config,
        1,
        QueryLanguage::promql,
    )
}

/// Execute both old pipeline and new plan-based path, compare results with epsilon tolerance.
pub async fn assert_old_new_match(engine: &SimpleEngine, query: &str, query_time_sec: f64) {
    let context = engine
        .build_query_execution_context_promql(query.to_string(), query_time_sec)
        .expect("Failed to build context");

    let old_results = engine
        .execute_query_pipeline(&context, false)
        .expect("Old pipeline failed");

    let new_results = engine
        .execute_plan(&context)
        .await
        .expect("New plan path failed");

    assert_eq!(
        old_results.len(),
        new_results.len(),
        "Result count mismatch: old={}, new={}",
        old_results.len(),
        new_results.len()
    );

    let old_map: HashMap<Vec<String>, f64> = old_results
        .iter()
        .map(|r| (r.labels.labels.clone(), r.value))
        .collect();

    let new_map: HashMap<Vec<String>, f64> = new_results
        .iter()
        .map(|r| (r.labels.labels.clone(), r.value))
        .collect();

    for (key, old_value) in &old_map {
        let new_value = new_map
            .get(key)
            .unwrap_or_else(|| panic!("Key {:?} missing from new results", key));
        assert!(
            (old_value - new_value).abs() < 1e-10,
            "Value mismatch for key {:?}: old={}, new={}",
            key,
            old_value,
            new_value
        );
    }

    for key in new_map.keys() {
        assert!(
            old_map.contains_key(key),
            "Extra key {:?} in new results",
            key
        );
    }
}

/// Convenience wrapper to execute via the new plan path.
pub async fn execute_new_plan(
    engine: &SimpleEngine,
    query: &str,
    query_time_sec: f64,
) -> Vec<InstantVectorElement> {
    let context = engine
        .build_query_execution_context_promql(query.to_string(), query_time_sec)
        .expect("Failed to build context");

    engine
        .execute_plan(&context)
        .await
        .expect("execute_plan failed")
}

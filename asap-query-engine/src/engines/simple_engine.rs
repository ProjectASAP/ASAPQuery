use crate::data_model::{
    InferenceConfig, KeyByLabelValues, QueryConfig, QueryLanguage, SchemaConfig, StreamingConfig,
};
use crate::engines::query_result::{InstantVectorElement, QueryResult, RangeVectorElement};
// use crate::stores::promsketch_store::{
//     self, is_usampling_function, metrics as ps_metrics, PromSketchStore,
// };
use crate::stores::{Store, TimestampedBucketsMap};
use core::panic;
use promql_utilities::get_is_collapsable;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

use crate::AggregateCore;

use promql_utilities::ast_matching::{PromQLMatchResult, PromQLPattern, PromQLPatternBuilder};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryPatternType, Statistic};
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels, get_statistics_to_compute,
};

use sql_utilities::ast_matching::QueryType;
use sql_utilities::ast_matching::{SQLPatternMatcher, SQLPatternParser, SQLQuery};
use sql_utilities::sqlhelper::{AggregationInfo, SQLQueryData};
use sqlparser::dialect::*;
use sqlparser::parser::Parser as parser;

// SQL issue: refactor simpleengine to create matchresult similar to SQLquerydata

// Type alias for merged outputs (single aggregate per key after merging)
type MergedOutputsMap = HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>;

/// Information about bucket timeline for a single key (used for gap detection)
/// Aggregation IDs and types for key and value
#[derive(Debug, Clone)]
pub struct AggregationIdInfo {
    pub aggregation_id_for_key: u64,
    pub aggregation_id_for_value: u64,
    pub aggregation_type_for_key: String,
    pub aggregation_type_for_value: String,
}

/// Metadata extracted from a query, independent of query language
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    /// Labels that will appear in the query output
    pub query_output_labels: KeyByLabelNames,
    /// The primary statistic to compute (sum, max, quantile, etc.)
    pub statistic_to_compute: Statistic,
    /// Additional parameters (e.g., "quantile" -> "0.95", "k" -> "10")
    pub query_kwargs: HashMap<String, String>,
}

/// Parameters for a single store query
#[derive(Debug, Clone)]
pub struct StoreQueryParams {
    pub metric: String,
    pub aggregation_id: u64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    /// true for sliding windows (exact match), false for tumbling (range)
    pub is_exact_query: bool,
}

/// Complete plan for querying store (values + optional separate keys)
#[derive(Debug, Clone)]
pub struct StoreQueryPlan {
    pub values_query: StoreQueryParams,
    /// Some when key and value use different aggregations (DeltaSet/SetAggregator)
    pub keys_query: Option<StoreQueryParams>,
}

/// Timestamps for query execution
#[derive(Debug, Clone)]
pub struct QueryTimestamps {
    pub start_timestamp: u64,
    pub end_timestamp: u64,
}

/// Complete execution context for a query
#[derive(Debug, Clone)]
pub struct QueryExecutionContext {
    pub metric: String,
    pub metadata: QueryMetadata,
    pub store_plan: StoreQueryPlan,
    pub agg_info: AggregationIdInfo,
    /// Whether to merge multiple precomputes (true for temporal queries)
    pub do_merge: bool,
    #[allow(dead_code)]
    pub spatial_filter: String,
    pub query_time: u64,
    /// Spatial grouping labels from the value aggregation config.
    /// These are the store GROUP BY columns.
    pub grouping_labels: KeyByLabelNames,
    /// Aggregated labels from the value aggregation config.
    /// These are labels that "key" an accumulator/sketch internally
    /// (e.g. endpoint within a MultipleIncrease accumulator).
    pub aggregated_labels: KeyByLabelNames,
}

/// Parameters for a range query
#[derive(Debug, Clone)]
pub struct RangeQueryParams {
    pub start: u64, // start timestamp in ms
    pub end: u64,   // end timestamp in ms
    pub step: u64,  // step in ms
}

/// Extended execution context for range queries
#[derive(Debug, Clone)]
pub struct RangeQueryExecutionContext {
    /// Base context (metric, metadata, store_plan, etc.)
    pub base: QueryExecutionContext,
    /// Range-specific parameters
    pub range_params: RangeQueryParams,
    /// Number of buckets per step (step / tumbling_window)
    pub buckets_per_step: usize,
    /// Number of buckets in lookback window
    pub lookback_bucket_count: usize,
    /// Tumbling window size in ms
    pub tumbling_window_ms: u64,
}

// /// Parsed components of a sketch query, extracted either via the PromQL AST
// /// parser (for standard functions) or via regex (for custom functions like
// /// `entropy_over_time` that the promql-parser crate doesn't recognize).
// struct SketchQueryComponents {
//     func_name: String,
//     metric: String,
//     range_seconds: u64,
//     /// Extra numeric argument (e.g. quantile value). 0.0 when unused.
//     args: f64,
// }

/// Simple query engine for processing PromQL-like queries against precomputed data
pub struct SimpleEngine {
    store: Arc<dyn Store>,
    // promsketch_store: Option<Arc<PromSketchStore>>,
    inference_config: InferenceConfig,
    streaming_config: Arc<StreamingConfig>,
    prometheus_scrape_interval: u64,
    controller_patterns: HashMap<QueryPatternType, Vec<PromQLPattern>>,
    query_language: QueryLanguage,
}

impl SimpleEngine {
    pub fn new(
        store: Arc<dyn Store>,
        // promsketch_store: Option<Arc<PromSketchStore>>,
        inference_config: InferenceConfig,
        streaming_config: Arc<StreamingConfig>,
        prometheus_scrape_interval: u64,
        query_language: QueryLanguage,
    ) -> Self {
        // Create temporal pattern blocks
        let mut temporal_pattern_blocks = HashMap::new();
        temporal_pattern_blocks.insert(
            "quantile".to_string(),
            PromQLPatternBuilder::function(
                vec!["quantile_over_time"],
                vec![
                    PromQLPatternBuilder::number(None, Some("quantile_param")),
                    PromQLPatternBuilder::matrix_selector(
                        PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                        None,
                        Some("range_vector"),
                    ),
                ],
                Some("function"),
                Some("function_args"),
            ),
        );

        temporal_pattern_blocks.insert(
            "generic".to_string(),
            PromQLPatternBuilder::function(
                vec![
                    "sum_over_time",
                    "count_over_time",
                    "avg_over_time",
                    "min_over_time",
                    "max_over_time",
                    "increase",
                    "rate",
                    "entropy_over_time",
                    "distinct_over_time",
                    "l1_over_time",
                    "l2_over_time",
                    "stddev_over_time",
                    "stdvar_over_time",
                    "sum2_over_time",
                ],
                vec![PromQLPatternBuilder::matrix_selector(
                    PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                    None,
                    Some("range_vector"),
                )],
                Some("function"),
                Some("function_args"),
            ),
        );

        // Create spatial pattern blocks
        let mut spatial_pattern_blocks = HashMap::new();
        spatial_pattern_blocks.insert(
            "generic".to_string(),
            PromQLPatternBuilder::aggregation(
                vec!["sum", "count", "avg", "quantile", "min", "max", "topk"],
                PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                None,
                None,
                None,
                Some("aggregation"),
            ),
        );

        // Helper functions (these would be closures or separate methods)
        fn temporal_pattern(
            pattern_type: &str,
            blocks: &HashMap<String, Option<HashMap<String, Value>>>,
        ) -> PromQLPattern {
            PromQLPattern::new(blocks[pattern_type].clone())
        }

        fn spatial_pattern(
            pattern_type: &str,
            blocks: &HashMap<String, Option<HashMap<String, Value>>>,
        ) -> PromQLPattern {
            PromQLPattern::new(blocks[pattern_type].clone())
        }

        fn spatial_of_temporal_pattern(
            temporal_block: &Option<HashMap<String, Value>>,
        ) -> PromQLPattern {
            let pattern = PromQLPatternBuilder::aggregation(
                vec!["sum", "count", "avg", "quantile", "min", "max"],
                temporal_block.clone(),
                None,
                None,
                None,
                Some("aggregation"),
            );
            PromQLPattern::new(pattern)
        }

        // Create controller patterns
        let mut controller_patterns = HashMap::new();
        controller_patterns.insert(
            QueryPatternType::OnlyTemporal,
            vec![
                temporal_pattern("quantile", &temporal_pattern_blocks),
                temporal_pattern("generic", &temporal_pattern_blocks),
            ],
        );
        controller_patterns.insert(
            QueryPatternType::OnlySpatial,
            vec![spatial_pattern("generic", &spatial_pattern_blocks)],
        );
        controller_patterns.insert(
            QueryPatternType::OneTemporalOneSpatial,
            vec![
                spatial_of_temporal_pattern(&temporal_pattern_blocks["quantile"]),
                spatial_of_temporal_pattern(&temporal_pattern_blocks["generic"]),
            ],
        );

        Self {
            store,
            // promsketch_store,
            inference_config,
            streaming_config,
            prometheus_scrape_interval,
            controller_patterns,
            query_language,
        }
    }

    /// Convert query timestamp (seconds) to data timestamp (milliseconds)
    pub fn convert_query_time_to_data_time(query_time: f64) -> u64 {
        (query_time * 1000.0) as u64
    }

    /// Finds the query configuration for a given query string
    fn find_query_config(&self, query: &str) -> Option<&QueryConfig> {
        self.inference_config
            .query_configs
            .iter()
            .find(|config| config.query == query)
    }

    /// Finds the query configuration for a SQL query using structural pattern matching.
    ///
    /// Unlike `find_query_config` (which does exact string comparison), this method parses
    /// each template in query_configs and compares it structurally against the incoming
    /// query_data — ignoring absolute timestamps and comparing only metric, aggregation,
    /// labels, time column name, and duration.
    fn find_query_config_sql(&self, query_data: &SQLQueryData) -> Option<&QueryConfig> {
        let schema = match &self.inference_config.schema {
            SchemaConfig::SQL(sql_schema) => sql_schema,
            _ => return None,
        };

        self.inference_config.query_configs.iter().find(|config| {
            let template_statements =
                match parser::parse_sql(&GenericDialect {}, config.query.as_str()) {
                    Ok(stmts) => stmts,
                    Err(_) => return false,
                };
            let template_data =
                match SQLPatternParser::new(schema, 0.0).parse_query(&template_statements) {
                    Some(data) => data,
                    None => return false,
                };
            query_data.matches_sql_pattern(&template_data)
        })
    }

    /// Validates and potentially aligns end timestamp based on query pattern
    fn validate_and_align_end_timestamp(
        &self,
        mut end_timestamp: u64,
        query_pattern_type: QueryPatternType,
    ) -> u64 {
        let interval_ms = self.prometheus_scrape_interval * 1000;

        if !end_timestamp.is_multiple_of(interval_ms) {
            warn!(
                "Query end timestamp {} is not aligned with Prometheus scrape interval of {} seconds. \
                 This may lead to inaccurate results.",
                end_timestamp, self.prometheus_scrape_interval
            );
        }

        // For OnlySpatial, align end_timestamp to nearest scrape interval
        if query_pattern_type == QueryPatternType::OnlySpatial
            && !end_timestamp.is_multiple_of(interval_ms)
        {
            let aligned_end_timestamp = (end_timestamp / interval_ms) * interval_ms;
            debug!(
                "OnlySpatial query: Aligning end_timestamp from {} to {} using scrape interval of {} seconds",
                end_timestamp, aligned_end_timestamp, self.prometheus_scrape_interval
            );
            end_timestamp = aligned_end_timestamp;
        }

        end_timestamp
    }

    /// Calculates start timestamp for PromQL queries
    fn calculate_start_timestamp_promql(
        &self,
        end_timestamp: u64,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> u64 {
        match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                let range_seconds = match_result.get_range_duration().unwrap().num_seconds() as u64;
                end_timestamp - (range_seconds * 1000)
            }
            QueryPatternType::OnlySpatial => {
                end_timestamp - (self.prometheus_scrape_interval * 1000)
            }
        }
    }

    /// Calculates start timestamp for SQL queries
    fn calculate_start_timestamp_sql(
        &self,
        end_timestamp: u64,
        query_pattern_type: QueryPatternType,
        match_result: &SQLQuery,
    ) -> u64 {
        match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                let scrape_intervals =
                    match_result.query_data[0].time_info.clone().get_duration() as u64;
                end_timestamp - (scrape_intervals * self.prometheus_scrape_interval * 1000)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                let scrape_intervals =
                    match_result.query_data[1].time_info.clone().get_duration() as u64;
                end_timestamp - (scrape_intervals * self.prometheus_scrape_interval * 1000)
            }
            QueryPatternType::OnlySpatial => {
                end_timestamp - (self.prometheus_scrape_interval * 1000)
            }
        }
    }

    /// Calculates and validates query timestamps for PromQL
    fn calculate_query_timestamps_promql(
        &self,
        query_time: u64,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> QueryTimestamps {
        let mut end_timestamp = if let Some(at_modifier) = match_result
            .tokens
            .get("metric")
            .and_then(|t| t.metric.as_ref())
            .and_then(|m| m.at_modifier)
        {
            at_modifier * 1000
        } else {
            query_time
        };

        end_timestamp = self.validate_and_align_end_timestamp(end_timestamp, query_pattern_type);
        let start_timestamp =
            self.calculate_start_timestamp_promql(end_timestamp, query_pattern_type, match_result);

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    /// Calculates and validates query timestamps for SQL
    fn calculate_query_timestamps_sql(
        &self,
        query_time: u64,
        query_pattern_type: QueryPatternType,
        match_result: &SQLQuery,
    ) -> QueryTimestamps {
        let mut end_timestamp = query_time;
        end_timestamp = self.validate_and_align_end_timestamp(end_timestamp, query_pattern_type);
        let start_timestamp =
            self.calculate_start_timestamp_sql(end_timestamp, query_pattern_type, match_result);

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    /// Extracts quantile parameter from PromQL match result
    fn extract_quantile_param_promql(
        &self,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Option<String> {
        let quantile_value = match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                match_result
                    .tokens
                    .get("function_args")
                    .and_then(|token| token.function.as_ref())
                    .and_then(|func| func.args.first())
            }
            QueryPatternType::OnlySpatial => match_result
                .tokens
                .get("aggregation")
                .and_then(|token| token.aggregation.as_ref())
                .and_then(|agg| agg.param.as_ref()),
        };

        quantile_value.map(|s| s.to_string())
    }

    /// Extracts quantile parameter from SQL match result
    fn extract_quantile_param_sql(&self, match_result: &SQLQuery) -> Option<String> {
        match_result
            .query_data
            .first()
            .map(|data| data.aggregation_info.get_args()[0].to_string())
    }

    /// Extracts topk k parameter from PromQL match result
    fn extract_topk_param(
        &self,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Result<String, String> {
        match query_pattern_type {
            QueryPatternType::OnlySpatial => match_result
                .tokens
                .get("aggregation")
                .and_then(|token| token.aggregation.as_ref())
                .and_then(|agg| agg.param.as_ref())
                .map(|s| s.to_string())
                .ok_or_else(|| "Missing k parameter for top-k query".to_string()),
            _ => Err(format!(
                "Top-k statistic is only supported for OnlySpatial pattern, found {:?}",
                query_pattern_type
            )),
        }
    }

    /// Builds query kwargs (quantile, k, etc.) for PromQL queries
    fn build_query_kwargs_promql(
        &self,
        statistic: &Statistic,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Result<HashMap<String, String>, String> {
        let mut query_kwargs = HashMap::new();

        match statistic {
            Statistic::Quantile => {
                let quantile = self
                    .extract_quantile_param_promql(query_pattern_type, match_result)
                    .ok_or_else(|| "Missing quantile parameter for quantile query".to_string())?;
                debug!("Extracted quantile value: {:?}", quantile);
                query_kwargs.insert("quantile".to_string(), quantile);
            }
            Statistic::Topk => {
                let k = self.extract_topk_param(query_pattern_type, match_result)?;
                debug!("Extracted k value: {:?}", k);
                query_kwargs.insert("k".to_string(), k);
            }
            _ => {}
        }

        Ok(query_kwargs)
    }

    /// Builds query kwargs for SQL queries
    fn build_query_kwargs_sql(
        &self,
        statistic: &Statistic,
        match_result: &SQLQuery,
    ) -> Result<HashMap<String, String>, String> {
        let mut query_kwargs = HashMap::new();

        if *statistic == Statistic::Quantile {
            let quantile = self
                .extract_quantile_param_sql(match_result)
                .ok_or_else(|| "Missing quantile parameter for quantile query".to_string())?;
            query_kwargs.insert("quantile".to_string(), quantile);
        }
        // Note: SQL doesn't support topk limiting yet

        Ok(query_kwargs)
    }

    /// Creates query parameters for separate keys query
    fn create_keys_query_params(
        &self,
        metric: &str,
        end_timestamp: u64,
        agg_info: &AggregationIdInfo,
    ) -> Result<StoreQueryParams, String> {
        let (start_timestamp, end_timestamp) = match agg_info.aggregation_type_for_key.as_str() {
            "DeltaSetAggregator" => {
                // All keys from beginning of time
                (0, end_timestamp)
            }
            "SetAggregator" => {
                // Latest window only
                let window_size = self
                    .streaming_config
                    .get_aggregation_config(agg_info.aggregation_id_for_key)
                    .map(|config| config.window_size * 1000)
                    .ok_or_else(|| {
                        format!(
                            "Failed to get window size for aggregation {}",
                            agg_info.aggregation_id_for_key
                        )
                    })?;
                (end_timestamp - window_size, end_timestamp)
            }
            other => {
                return Err(format!("Unsupported key aggregation type: {}", other));
            }
        };

        Ok(StoreQueryParams {
            metric: metric.to_string(),
            aggregation_id: agg_info.aggregation_id_for_key,
            start_timestamp,
            end_timestamp,
            is_exact_query: false, // Keys always use range queries
        })
    }

    /// Creates a plan for querying the store based on aggregation configuration
    fn create_store_query_plan(
        &self,
        metric: &str,
        timestamps: &QueryTimestamps,
        agg_info: &AggregationIdInfo,
    ) -> Result<StoreQueryPlan, String> {
        // Get aggregation config for value to determine window type
        let aggregation_config_for_value = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .ok_or_else(|| {
                format!(
                    "Aggregation config not found for aggregation_id: {}",
                    agg_info.aggregation_id_for_value
                )
            })?;

        let window_type = &aggregation_config_for_value.window_type;
        let is_exact_query = window_type == "sliding";

        // Determine start/end for values query based on window type
        let (values_start, values_end) = if is_exact_query {
            // Sliding window: exact window match
            let exact_start =
                timestamps.end_timestamp - (aggregation_config_for_value.window_size * 1000);
            (exact_start, timestamps.end_timestamp)
        } else {
            // Tumbling window: range query
            (timestamps.start_timestamp, timestamps.end_timestamp)
        };

        let values_query = StoreQueryParams {
            metric: metric.to_string(),
            aggregation_id: agg_info.aggregation_id_for_value,
            start_timestamp: values_start,
            end_timestamp: values_end,
            is_exact_query,
        };

        // Determine if we need a separate keys query
        let keys_query = if agg_info.aggregation_id_for_key != agg_info.aggregation_id_for_value {
            Some(self.create_keys_query_params(metric, timestamps.end_timestamp, agg_info)?)
        } else {
            None
        };

        Ok(StoreQueryPlan {
            values_query,
            keys_query,
        })
    }

    /// Executes a single store query based on parameters
    fn execute_store_query(
        &self,
        params: &StoreQueryParams,
    ) -> Result<TimestampedBucketsMap, String> {
        debug!(
            "Querying store: metric={}, agg_id={}, range=[{}, {}], exact={}",
            params.metric,
            params.aggregation_id,
            params.start_timestamp,
            params.end_timestamp,
            params.is_exact_query
        );

        let store_query_start_time = Instant::now();

        let result = if params.is_exact_query {
            debug!(
                "Sliding window query: Looking for exact window [{}, {}]",
                params.start_timestamp, params.end_timestamp
            );
            let res = self.store.query_precomputed_output_exact(
                &params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
            );
            if let Ok(ref outputs) = res {
                let store_query_duration = store_query_start_time.elapsed();
                debug!(
                    "Sliding window exact query took: {:.2}ms, found {} unique keys",
                    store_query_duration.as_secs_f64() * 1000.0,
                    outputs.len()
                );
            }
            res
        } else {
            debug!(
                "Tumbling window query: range [{}, {}]",
                params.start_timestamp, params.end_timestamp
            );
            let res = self.store.query_precomputed_output(
                &params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
            );
            if res.is_ok() {
                let store_query_duration = store_query_start_time.elapsed();
                debug!(
                    "Tumbling window range query took: {:.2}ms",
                    store_query_duration.as_secs_f64() * 1000.0
                );
            }
            res
        };

        result.map_err(|e| {
            format!(
                "Error querying store for metric {}, agg {}, range [{}, {}]: {}",
                params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
                e
            )
        })
    }

    /// Executes the full store query plan and returns merged results
    fn execute_and_merge_store_queries(
        &self,
        plan: &StoreQueryPlan,
        do_merge: bool,
        agg_info: &AggregationIdInfo,
    ) -> Result<(MergedOutputsMap, Option<MergedOutputsMap>), String> {
        // Query and merge values
        let values_map = self.execute_store_query(&plan.values_query).map_err(|e| {
            warn!("Error querying store for values: {}", e);
            e
        })?;

        if values_map.is_empty() {
            return Err(format!(
                "No precomputed outputs found for metric: {}, aggregation_id: {}",
                plan.values_query.metric, plan.values_query.aggregation_id
            ));
        }

        debug!("Store query returned {} unique keys", values_map.len());

        let merge_start_time = Instant::now();
        let window_type = if plan.values_query.is_exact_query {
            "sliding"
        } else {
            "tumbling"
        };

        let merged_values = if plan.values_query.is_exact_query {
            // Sliding window: no merge needed, extract buckets from timestamped data
            debug!("Sliding window mode: Skipping merge (expecting 1 precompute per key)");
            values_map
                .into_iter()
                .map(|(key, timestamped_buckets)| {
                    if timestamped_buckets.len() != 1 {
                        warn!(
                            "Sliding window expected 1 precompute per key, found {}. Using first.",
                            timestamped_buckets.len()
                        );
                    }
                    // Extract bucket from timestamped tuple
                    let (_, bucket) = timestamped_buckets.into_iter().next().unwrap();
                    (key, bucket.as_ref().clone_boxed_core())
                })
                .collect()
        } else {
            // Tumbling window: merge needed
            debug!("Tumbling window mode: Merging {} outputs", values_map.len());
            self.merge_precomputed_outputs(
                &values_map,
                do_merge,
                agg_info.aggregation_type_for_value.clone(),
            )
        };

        let merge_duration = merge_start_time.elapsed();
        debug!(
            "[LATENCY] Precomputed output processing ({}): {:.2}ms, resulted in {} merged outputs",
            if window_type == "sliding" {
                "no merge"
            } else {
                "merge"
            },
            merge_duration.as_secs_f64() * 1000.0,
            merged_values.len()
        );

        // Query and merge keys if needed
        let merged_keys = if let Some(keys_params) = &plan.keys_query {
            let keys_store_query_start_time = Instant::now();
            let keys_map = self.execute_store_query(keys_params).map_err(|e| {
                warn!("Error querying store for keys: {}", e);
                e
            })?;
            debug!(
                "[LATENCY] Keys store query (metric: {}, agg: {}): {}ms",
                &keys_params.metric,
                keys_params.aggregation_id,
                keys_store_query_start_time.elapsed().as_millis()
            );
            debug!("Keys query returned {} unique keys", keys_map.len());

            let keys_merge_start_time = Instant::now();
            let merged = self.merge_precomputed_outputs(
                &keys_map,
                do_merge,
                agg_info.aggregation_type_for_key.clone(),
            );
            debug!(
                "[LATENCY] Keys merge operation: {:.2}ms, resulted in {} merged outputs",
                keys_merge_start_time.elapsed().as_secs_f64() * 1000.0,
                merged.len()
            );
            Some(merged)
        } else {
            None
        };

        Ok((merged_values, merged_keys))
    }

    /// Collects all results based on whether keys are separate or not
    fn collect_all_results(
        &self,
        merged_values: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        merged_keys: Option<&HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
        enable_topk_limiting: bool,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        if let Some(keys_map) = merged_keys {
            // Separate keys and values
            self.collect_results_separate_keys(merged_values, keys_map, statistic, query_kwargs)
        } else {
            // Same aggregation for keys and values
            self.collect_results_same_aggregation(
                merged_values,
                statistic,
                query_kwargs,
                enable_topk_limiting,
            )
        }
    }

    /// Executes the complete query pipeline: plan, execute, collect, and format
    pub fn execute_query_pipeline(
        &self,
        context: &QueryExecutionContext,
        enable_topk: bool,
    ) -> Result<Vec<InstantVectorElement>, String> {
        // Step 1: Execute the query plan (already created in context.store_plan)
        let (merged_values, merged_keys) = self.execute_and_merge_store_queries(
            &context.store_plan,
            context.do_merge,
            &context.agg_info,
        )?;

        // Step 2: Collect results
        let unformatted_results_start_time = Instant::now();
        let unformatted_results = self.collect_all_results(
            &merged_values,
            merged_keys.as_ref(),
            &context.metadata.statistic_to_compute,
            &context.metadata.query_kwargs,
            enable_topk, // SQL=false, PromQL=true
        )?;
        debug!(
            "[LATENCY] Unformatted results collection: {:.2}ms",
            unformatted_results_start_time.elapsed().as_secs_f64() * 1000.0
        );

        // Step 3: Format results
        let results_start_time = Instant::now();
        let results = self.format_final_results(
            unformatted_results,
            &context.metadata.statistic_to_compute,
            &context.metric,
            enable_topk, // SQL=false, PromQL=true
        );
        debug!(
            "[LATENCY] Results collection: {}ms",
            results_start_time.elapsed().as_millis()
        );

        Ok(results)
    }

    /// Execute a query using the plan-based approach (for testing)
    ///
    /// This is an alternative execution path that uses DataFusion logical/physical
    /// plans instead of the existing execute_query_pipeline.
    ///
    /// # Arguments
    /// * `context` - The query execution context
    ///
    /// # Returns
    /// A Result containing the query results or an error
    #[allow(dead_code)]
    pub async fn execute_plan(
        &self,
        context: &QueryExecutionContext,
    ) -> Result<Vec<InstantVectorElement>, String> {
        use datafusion::execution::context::SessionContext;
        use datafusion::physical_plan::collect;

        use super::physical::conversion::record_batch_to_result_map;

        let total_start = Instant::now();

        // 1. Build logical plan from context
        let plan_build_start = Instant::now();
        let logical_plan = context
            .to_logical_plan()
            .map_err(|e| format!("Failed to build logical plan: {}", e))?;
        debug!(
            "[LATENCY] DataFusion: logical plan build: {:.2}ms",
            plan_build_start.elapsed().as_secs_f64() * 1000.0
        );
        debug!(
            "DataFusion logical plan:\n{}",
            logical_plan.display_indent()
        );

        // 2. Create session context with our custom extension planner
        let physical_plan_start = Instant::now();
        let session_ctx = SessionContext::new();
        #[allow(deprecated)]
        let state = session_ctx.state().with_query_planner(std::sync::Arc::new(
            super::physical::CustomQueryPlanner::new(self.store.clone()),
        ));

        // 3. Create physical plan
        let physical_plan = state
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| format!("Failed to create physical plan: {}", e))?;
        debug!(
            "[LATENCY] DataFusion: physical plan creation: {:.2}ms",
            physical_plan_start.elapsed().as_secs_f64() * 1000.0
        );

        // 4. Execute
        let execute_start = Instant::now();
        let task_ctx = session_ctx.task_ctx();
        let batches = collect(physical_plan, task_ctx)
            .await
            .map_err(|e| format!("Failed to execute plan: {}", e))?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        debug!(
            "[LATENCY] DataFusion: plan execution: {:.2}ms, {} batch(es), {} total rows",
            execute_start.elapsed().as_secs_f64() * 1000.0,
            batches.len(),
            total_rows
        );

        // 5. Convert results
        let convert_start = Instant::now();
        let label_names: Vec<&str> = context
            .metadata
            .query_output_labels
            .labels
            .iter()
            .map(String::as_str)
            .collect();

        let mut all_results: HashMap<Option<KeyByLabelValues>, f64> = HashMap::new();
        for batch in &batches {
            let batch_results = record_batch_to_result_map(batch, &label_names, "value")
                .map_err(|e| format!("Failed to convert results: {}", e))?;
            all_results.extend(batch_results);
        }
        debug!(
            "[LATENCY] DataFusion: result conversion: {:.2}ms, {} output rows",
            convert_start.elapsed().as_secs_f64() * 1000.0,
            all_results.len()
        );

        // 6. Format results
        let format_start = Instant::now();
        let results = self.format_final_results(
            all_results,
            &context.metadata.statistic_to_compute,
            &context.metric,
            false,
        );
        debug!(
            "[LATENCY] DataFusion: result formatting: {:.2}ms, {} results",
            format_start.elapsed().as_secs_f64() * 1000.0,
            results.len()
        );

        debug!(
            "[LATENCY] DataFusion: total execute_plan: {:.2}ms",
            total_start.elapsed().as_secs_f64() * 1000.0
        );

        Ok(results)
    }

    /// Formats unformatted results into final InstantVectorElement format
    /// For topk queries (when enabled), sorts by value and prepends metric name to keys
    fn format_final_results(
        &self,
        unformatted_results: HashMap<Option<KeyByLabelValues>, f64>,
        statistic: &Statistic,
        metric: &str,
        enable_topk_formatting: bool,
    ) -> Vec<InstantVectorElement> {
        let sorted_results: Vec<(Option<KeyByLabelValues>, f64)> =
            if *statistic == Statistic::Topk && enable_topk_formatting {
                // Sort by value descending for topk
                let mut sorted: Vec<_> = unformatted_results.into_iter().collect();
                sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

                // Prepend metric name to each key's label values
                sorted
                    .into_iter()
                    .map(|(key_opt, value)| {
                        let updated_key = key_opt.map(|mut key| {
                            let mut new_labels = vec![metric.to_string()];
                            new_labels.extend(key.labels);
                            key.labels = new_labels;
                            key
                        });
                        (updated_key, value)
                    })
                    .collect()
            } else {
                unformatted_results.into_iter().collect()
            };

        sorted_results
            .into_iter()
            .filter_map(|(key, value)| key.map(|k| InstantVectorElement::new(k, value)))
            .collect()
    }

    fn sql_get_is_collapsable(
        &self,
        temporal_aggregation: &AggregationInfo,
        spatial_aggregation: &AggregationInfo,
    ) -> bool {
        match spatial_aggregation.get_name() {
            "SUM" => matches!(
                temporal_aggregation.get_name(),
                "SUM" | "COUNT" // Note: "increase" and "rate" are commented out in Python
            ),
            "MIN" => temporal_aggregation.get_name() == "MIN",
            "MAX" => temporal_aggregation.get_name() == "MAX",
            _ => false,
        }
    }

    fn get_aggregation_id_info(&self, query_config: &QueryConfig) -> AggregationIdInfo {
        let query_config_aggregations = &query_config.aggregations;
        let mut aggregation_id_for_key: Option<u64> = None;
        let mut aggregation_id_for_value: Option<u64> = None;
        let mut aggregation_type_for_key: Option<String> = None;
        let mut aggregation_type_for_value: Option<String> = None;

        if query_config_aggregations.is_empty() {
            panic!("Query config for query has no aggregations defined",);
        } else if query_config_aggregations.len() > 2 {
            panic!("Query config with > 2 aggregations is not supported");
        } else if query_config_aggregations.len() == 2 {
            for aggregation in query_config_aggregations {
                let aggregation_type = self
                    .streaming_config
                    .get_aggregation_config(aggregation.aggregation_id)
                    .map(|config| config.aggregation_type.clone());

                if aggregation_type.as_ref().unwrap() == "DeltaSetAggregator"
                    || aggregation_type.as_ref().unwrap() == "SetAggregator"
                {
                    if aggregation_id_for_key.is_some() {
                        panic!("Aggregation ID for key must be None");
                    }
                    if aggregation_type_for_key.is_some() {
                        panic!("Aggregation type for key must be None");
                    }
                    aggregation_id_for_key = Some(aggregation.aggregation_id);
                    aggregation_type_for_key = aggregation_type;
                } else {
                    if aggregation_id_for_value.is_some() {
                        panic!("Aggregation ID for value must be None");
                    }
                    aggregation_id_for_value = Some(aggregation.aggregation_id);
                    aggregation_type_for_value = aggregation_type;
                }
            }
        } else {
            aggregation_id_for_key = Some(query_config_aggregations[0].aggregation_id);
            aggregation_id_for_value = aggregation_id_for_key;
            // aggregation_type_for_key = Some(query_config_aggregations[0].aggregation_type.clone());
            aggregation_type_for_key = self
                .streaming_config
                .get_aggregation_config(aggregation_id_for_key.unwrap())
                .map(|config| config.aggregation_type.clone());
            aggregation_type_for_value = self
                .streaming_config
                .get_aggregation_config(aggregation_id_for_value.unwrap())
                .map(|config| config.aggregation_type.clone());
        }

        // check for None
        if aggregation_id_for_key.is_none() || aggregation_id_for_value.is_none() {
            panic!("Aggregation IDs must not be None");
        }

        AggregationIdInfo {
            aggregation_id_for_key: aggregation_id_for_key.unwrap(),
            aggregation_id_for_value: aggregation_id_for_value.unwrap(),
            aggregation_type_for_key: aggregation_type_for_key.unwrap(),
            aggregation_type_for_value: aggregation_type_for_value.unwrap(),
        }
    }

    pub fn handle_query_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let context = self.build_query_execution_context_sql(query, time)?;
        // Execute complete query pipeline
        let results = self
            .execute_query_pipeline(&context, false) // SQL: topk disabled
            .map_err(|e| {
                warn!("Query execution failed: {}", e);
                e
            })
            .ok()?;

        Some((
            context.metadata.query_output_labels,
            QueryResult::vector(results, context.query_time),
        ))
    }

    pub fn build_query_execution_context_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        // Get SQL schema from inference config
        let schema = match &self.inference_config.schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => {
                warn!("SQL query requested but config has PromQL schema");
                return None;
            }
            &SchemaConfig::ElasticQueryDSL => todo!(),
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
        };

        let statements = parser::parse_sql(&GenericDialect {}, query.as_str()).unwrap();
        let query_data = SQLPatternParser::new(&schema, time).parse_query(&statements);

        let query_data = match query_data {
            Some(data) => data,
            None => {
                debug!("Could not parse query");
                return None;
            }
        };

        let matcher = SQLPatternMatcher::new(schema, self.prometheus_scrape_interval as f64);
        let match_result = matcher.query_info_to_pattern(&query_data);

        debug!("Match result: {:?}", match_result);
        debug!("Validity: {}", match_result.is_valid());

        if !match_result.is_valid() {
            return None;
        }

        // Handle SpatioTemporal queries separately - they bypass QueryPatternType mapping
        if match_result.query_type == vec![QueryType::SpatioTemporal] {
            let query_time = Self::convert_query_time_to_data_time(
                query_data.time_info.get_start() + query_data.time_info.get_duration(),
            );
            return self.build_spatiotemporal_context(&match_result, query_time, &query_data);
        }

        let query_pattern_type = match &match_result.query_type[..] {
            [x] => match x {
                QueryType::Spatial => QueryPatternType::OnlySpatial,
                QueryType::TemporalGeneric => QueryPatternType::OnlyTemporal,
                QueryType::TemporalQuantile => QueryPatternType::OnlyTemporal,
                QueryType::SpatioTemporal => unreachable!("SpatioTemporal handled above"),
            },
            [x, y] => match (x, y) {
                (QueryType::Spatial, QueryType::TemporalGeneric) => {
                    QueryPatternType::OneTemporalOneSpatial
                }
                (QueryType::Spatial, QueryType::TemporalQuantile) => {
                    QueryPatternType::OneTemporalOneSpatial
                }
                _ => panic!("Unsupported query type found"),
            },
            _ => panic!("Unsupported query type found"),
        };

        let query_config = self.find_query_config_sql(&query_data)?;

        // For nested queries (spatial of temporal), the outer query has no time clause,
        // so we need to use the inner (temporal) query's time_info to compute query_time
        let query_time = match query_pattern_type {
            QueryPatternType::OneTemporalOneSpatial => {
                let inner_time_info = &match_result.query_data[1].time_info;
                Self::convert_query_time_to_data_time(
                    inner_time_info.get_start() + inner_time_info.get_duration(),
                )
            }
            _ => Self::convert_query_time_to_data_time(
                query_data.time_info.get_start() + query_data.time_info.get_duration(),
            ),
        };

        //     self.handle_sql_temporal_aggregation(
        //         query_config,
        //         &match_result,
        //         query_time,
        //         query_pattern_type,
        //     )
        // }

        // fn handle_sql_temporal_aggregation(
        //     &self,
        //     query_config: &QueryConfig,
        //     match_result: &SQLQuery,
        //     query_time: u64,
        //     query_pattern_type: QueryPatternType,
        // ) -> Option<(KeyByLabelNames, QueryResult)> {
        // Labels

        let query_output_labels = match &match_result.query_type.len() {
            // Potentially change SQLQueryType
            1 => {
                // For non-nested queries, output associated labels
                let labels = &match_result.query_data[0].labels;

                KeyByLabelNames::new(labels.clone().into_iter().collect())
            }
            2 => {
                // Extract spatial aggregation output labels using AST-based approach
                let temporal_labels = &match_result.query_data[1].labels;
                let spatial_labels = &match_result.query_data[0].labels;

                let temporal_aggregation = &match_result.query_data[1].aggregation_info;
                let spatial_aggregation = &match_result.query_data[0].aggregation_info;

                match self.sql_get_is_collapsable(temporal_aggregation, spatial_aggregation) {
                    // If false: get all labels, which are all temporal labels. If true, get only spatial labels
                    false => KeyByLabelNames::new(temporal_labels.clone().into_iter().collect()),
                    true => KeyByLabelNames::new(spatial_labels.clone().into_iter().collect()),
                }
            }
            _ => {
                warn!("Invalid query type: {}", query_pattern_type);
                KeyByLabelNames::new(Vec::new())
            }
        };

        // Statistic - determine based on query pattern type
        let statistic_name = match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                // Use the temporal aggregation (first subquery)
                match_result.query_data[0]
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
            QueryPatternType::OneTemporalOneSpatial => {
                // Use the temporal aggregation (second subquery contains temporal)
                match_result.query_data[1]
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
            QueryPatternType::OnlySpatial => {
                // Use the spatial aggregation (first subquery)
                match_result.query_data[0]
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
        };

        let statistics_to_compute: Vec<Statistic> = if statistic_name == "avg" {
            vec![Statistic::Sum, Statistic::Count]
        } else if let Ok(stat) = statistic_name.parse::<Statistic>() {
            vec![stat]
        } else {
            panic!("Unsupported statistic: {}", statistic_name);
        };

        if statistics_to_compute.len() != 1 {
            panic!(
                "Expected exactly one statistic to compute, found {}",
                statistics_to_compute.len()
            );
        }
        let statistic_to_compute = statistics_to_compute.first().unwrap();

        let query_kwargs = self
            .build_query_kwargs_sql(statistic_to_compute, &match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        // Create query metadata
        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute: *statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Time
        let timestamps =
            self.calculate_query_timestamps_sql(query_time, query_pattern_type, &match_result);

        // Precomputed output

        let agg_info = self.get_aggregation_id_info(query_config);

        let metric = &match_result.query_data[0].metric;

        let spatial_filter = if query_pattern_type == QueryPatternType::OneTemporalOneSpatial {
            match_result.query_data[0]
                .labels
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        } else {
            String::new()
        };

        // Create query plan and execute values query
        let query_plan = self
            .create_store_query_plan(metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        // Create execution context
        // do_merge is true for temporal queries (OnlyTemporal or OneTemporalOneSpatial)
        let do_merge = query_pattern_type == QueryPatternType::OnlyTemporal
            || query_pattern_type == QueryPatternType::OneTemporalOneSpatial;

        let grouping_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_output_labels.clone());

        let aggregated_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric: metric.to_string(),
            metadata,
            store_plan: query_plan.clone(),
            agg_info: agg_info.clone(),
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })

        // TODO: Handle spatial aggregation for OneTemporalOneSpatial when not collapsable
    }

    /// Build execution context for SpatioTemporal queries.
    /// These queries span multiple scrape intervals but GROUP BY a subset of labels.
    fn build_spatiotemporal_context(
        &self,
        match_result: &SQLQuery,
        query_time: u64,
        query_data: &SQLQueryData,
    ) -> Option<QueryExecutionContext> {
        let query_config = self.find_query_config_sql(query_data)?;

        // Output labels are the GROUP BY columns (subset of all labels)
        let query_output_labels = KeyByLabelNames::new(
            match_result.query_data[0]
                .labels
                .clone()
                .into_iter()
                .collect(),
        );

        // Get the statistic from the aggregation
        let statistic_name = match_result.query_data[0]
            .aggregation_info
            .get_name()
            .to_lowercase();

        let statistics_to_compute: Vec<Statistic> = if statistic_name == "avg" {
            vec![Statistic::Sum, Statistic::Count]
        } else if let Ok(stat) = statistic_name.parse::<Statistic>() {
            vec![stat]
        } else {
            panic!("Unsupported statistic: {}", statistic_name);
        };

        if statistics_to_compute.len() != 1 {
            panic!(
                "Expected exactly one statistic to compute, found {}",
                statistics_to_compute.len()
            );
        }
        let statistic_to_compute = statistics_to_compute.first().unwrap();

        let query_kwargs = self
            .build_query_kwargs_sql(statistic_to_compute, match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute: *statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Calculate timestamps - similar to OnlyTemporal
        let end_timestamp =
            self.validate_and_align_end_timestamp(query_time, QueryPatternType::OnlyTemporal);
        let scrape_intervals = match_result.query_data[0].time_info.get_duration() as u64;
        let start_timestamp =
            end_timestamp - (scrape_intervals * self.prometheus_scrape_interval * 1000);

        let timestamps = QueryTimestamps {
            start_timestamp,
            end_timestamp,
        };

        let agg_info = self.get_aggregation_id_info(query_config);
        let metric = &match_result.query_data[0].metric;

        let query_plan = self
            .create_store_query_plan(metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        // SpatioTemporal queries need merging (like temporal queries)
        let grouping_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_output_labels.clone());

        let aggregated_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric: metric.to_string(),
            metadata,
            store_plan: query_plan,
            agg_info,
            do_merge: true,
            spatial_filter: String::new(),
            query_time,
            grouping_labels,
            aggregated_labels,
        })
    }

    /// Handle a query following Python's unified architecture
    // pub async fn handle_query(
    pub fn handle_query(&self, query: String, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        match self.query_language {
            QueryLanguage::promql => self.handle_query_promql(query, time),
            QueryLanguage::sql => self.handle_query_sql(query, time),
            QueryLanguage::elastic_querydsl => self.handle_query_elastic(),
            QueryLanguage::elastic_sql => self.handle_query_sql(query, time),
        }
    }

    pub fn handle_query_elastic(&self) -> Option<(KeyByLabelNames, QueryResult)> {
        None
    }

    // /// Try to extract sketch query components from a PromQL query string.
    // ///
    // /// Attempts the standard AST parser first. If that fails (e.g. for custom
    // /// sketch-only functions), falls back to a lightweight regex extraction for
    // /// patterns like `func(metric[range])` and `func(number, metric[range])`.
    // /// Extract just the sketch function name from a query without full evaluation.
    // fn extract_sketch_func_name(&self, query: &str) -> Option<String> {
    //     self.parse_sketch_query_components(query)
    //         .map(|c| c.func_name)
    // }

    // fn parse_sketch_query_components(&self, query: &str) -> Option<SketchQueryComponents> {
    //     // --- Path A: standard PromQL parser + pattern matching ---
    //     if let Some(components) = self.parse_sketch_via_ast(query) {
    //         return Some(components);
    //     }

    //     // --- Path B: regex fallback for custom sketch functions ---
    //     self.parse_sketch_via_regex(query)
    // }

    // /// Parse sketch components using the standard PromQL AST parser.
    // fn parse_sketch_via_ast(&self, query: &str) -> Option<SketchQueryComponents> {
    //     let ast = match promql_parser::parser::parse(query) {
    //         Ok(ast) => ast,
    //         Err(_) => return None,
    //     };

    //     let mut found_match = None;
    //     for (pattern_type, patterns) in &self.controller_patterns {
    //         for pattern in patterns {
    //             let match_result = pattern.matches(&ast);
    //             if match_result.matches {
    //                 found_match = Some((*pattern_type, match_result));
    //                 break;
    //             }
    //         }
    //         if found_match.is_some() {
    //             break;
    //         }
    //     }

    //     let (query_pattern_type, match_result) = found_match?;

    //     if query_pattern_type != QueryPatternType::OnlyTemporal {
    //         debug!(
    //             "Sketch query (AST): pattern type {:?} is not OnlyTemporal, skipping for '{}'",
    //             query_pattern_type, query
    //         );
    //         return None;
    //     }

    //     let func_name = match_result.get_function_name()?;
    //     promsketch_store::promsketch_func_map(&func_name)?;

    //     let (metric, spatial_filter) = get_metric_and_spatial_filter(&match_result);
    //     let metric = if spatial_filter.is_empty() {
    //         metric
    //     } else {
    //         format!("{}{{{}}}", metric, spatial_filter)
    //     };

    //     let range_seconds = match_result.get_range_duration()?.num_seconds() as u64;

    //     let args = if func_name == "quantile_over_time" {
    //         self.extract_quantile_param_promql(query_pattern_type, &match_result)
    //             .and_then(|s| s.parse::<f64>().ok())
    //             .unwrap_or(0.5)
    //     } else {
    //         0.0
    //     };

    //     Some(SketchQueryComponents {
    //         func_name,
    //         metric,
    //         range_seconds,
    //         args,
    //     })
    // }

    // /// Regex fallback for custom sketch functions the PromQL parser doesn't know.
    // ///
    // /// Matches two forms:
    // ///   - `func_name(metric[duration])`                  (generic)
    // ///   - `func_name(number, metric[duration])`          (quantile)
    // ///   - `func_name(metric{filter}[duration])`          (with label filter)
    // fn parse_sketch_via_regex(&self, query: &str) -> Option<SketchQueryComponents> {
    //     use regex::Regex;

    //     // quantile form: quantile_over_time(0.5, metric{...}[5m])
    //     let quantile_re =
    //         Regex::new(r"^(\w+)\(\s*([0-9.]+)\s*,\s*(\w+(?:\{[^}]*\})?)\[(\d+)([smhd])\]\s*\)$")
    //             .ok()?;

    //     // generic form: func(metric{...}[5m])
    //     let generic_re =
    //         Regex::new(r"^(\w+)\(\s*(\w+(?:\{[^}]*\})?)\[(\d+)([smhd])\]\s*\)$").ok()?;

    //     if let Some(caps) = quantile_re.captures(query.trim()) {
    //         let func_name = caps[1].to_string();
    //         promsketch_store::promsketch_func_map(&func_name)?;
    //         let args: f64 = caps[2].parse().ok()?;
    //         let metric = caps[3].to_string();
    //         let range_seconds = Self::parse_duration_to_seconds(&caps[4], &caps[5])?;
    //         debug!(
    //             "Sketch query (regex/quantile): parsed {} with metric={}, range={}s, args={}",
    //             func_name, metric, range_seconds, args
    //         );
    //         return Some(SketchQueryComponents {
    //             func_name,
    //             metric,
    //             range_seconds,
    //             args,
    //         });
    //     }

    //     if let Some(caps) = generic_re.captures(query.trim()) {
    //         let func_name = caps[1].to_string();
    //         promsketch_store::promsketch_func_map(&func_name)?;
    //         let metric = caps[2].to_string();
    //         let range_seconds = Self::parse_duration_to_seconds(&caps[3], &caps[4])?;
    //         debug!(
    //             "Sketch query (regex/generic): parsed {} with metric={}, range={}s",
    //             func_name, metric, range_seconds
    //         );
    //         return Some(SketchQueryComponents {
    //             func_name,
    //             metric,
    //             range_seconds,
    //             args: 0.0,
    //         });
    //     }

    //     None
    // }

    // /// Convert a numeric value + unit suffix into seconds.
    // fn parse_duration_to_seconds(value: &str, unit: &str) -> Option<u64> {
    //     let n: u64 = value.parse().ok()?;
    //     let multiplier = match unit {
    //         "s" => 1,
    //         "m" => 60,
    //         "h" => 3600,
    //         "d" => 86400,
    //         _ => return None,
    //     };
    //     Some(n * multiplier)
    // }

    // /// Try to handle a PromQL query via the sketch shortcut path.
    // /// Returns Some if the query is sketch-backed and PromSketchStore is available.
    // /// Returns None to fall through to the precomputed pipeline.
    // fn handle_sketch_query_promql(
    //     &self,
    //     query: &str,
    //     time: f64,
    // ) -> Option<(KeyByLabelNames, QueryResult)> {
    //     let ps = self.promsketch_store.as_ref()?;

    //     let components = match self.parse_sketch_query_components(query) {
    //         Some(c) => c,
    //         None => {
    //             debug!(
    //                 "Sketch query: could not parse sketch components from '{}'",
    //                 query
    //             );
    //             return None;
    //         }
    //     };

    //     let eval_start = Instant::now();

    //     let query_time = Self::convert_query_time_to_data_time(time);
    //     let end = query_time;
    //     let start = end.saturating_sub(components.range_seconds * 1000);

    //     debug!(
    //         "Sketch query: evaluating {}({}) range=[{}, {}] args={}",
    //         components.func_name, components.metric, start, end, components.args
    //     );

    //     let results = match ps.eval_matching(
    //         &components.func_name,
    //         &components.metric,
    //         components.args,
    //         start,
    //         end,
    //     ) {
    //         Ok(r) => r,
    //         Err(e) => {
    //             warn!(
    //                 "Sketch query: eval_matching failed for {}({}): {}",
    //                 components.func_name, components.metric, e
    //             );
    //             ps_metrics::SKETCH_QUERIES_TOTAL
    //                 .with_label_values(&["miss"])
    //                 .inc();
    //             return None;
    //         }
    //     };

    //     if results.is_empty() {
    //         debug!(
    //             "Sketch query: no matching series with data for {}({}), falling through",
    //             components.func_name, components.metric
    //         );
    //         ps_metrics::SKETCH_QUERIES_TOTAL
    //             .with_label_values(&["miss"])
    //             .inc();
    //         return None;
    //     }

    //     ps_metrics::SKETCH_QUERIES_TOTAL
    //         .with_label_values(&["hit"])
    //         .inc();
    //     ps_metrics::SKETCH_QUERY_DURATION.observe(eval_start.elapsed().as_secs_f64());

    //     info!(
    //         "Sketch query: {}({}) returned {} series results",
    //         components.func_name,
    //         components.metric,
    //         results.len()
    //     );

    //     let elements: Vec<InstantVectorElement> = results
    //         .into_iter()
    //         .map(|(labels_str, value)| {
    //             let labels = KeyByLabelValues::new_with_labels(vec![labels_str]);
    //             InstantVectorElement::new(labels, value)
    //         })
    //         .collect();

    //     let output_labels = KeyByLabelNames::new(vec!["__name__".to_string()]);
    //     Some((output_labels, QueryResult::vector(elements, query_time)))
    // }

    pub fn handle_query_promql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let query_start_time = Instant::now();
        debug!("Handling query: {} at time {}", query, time);

        let context = self.build_query_execution_context_promql(query, time)?;

        debug!(
            "Querying store for metric: {}, aggregation_id: {}, range: [{}, {}]",
            context.metric,
            context.agg_info.aggregation_id_for_value,
            context.store_plan.values_query.start_timestamp,
            context.store_plan.values_query.end_timestamp
        );

        // TODO: Make handle_query_promql (and handle_query) async and use .await directly
        // instead of blocking. See execute_plan for the async implementation.
        // Execute complete query pipeline
        //let results = tokio::task::block_in_place(|| {
        //    tokio::runtime::Handle::current().block_on(self.execute_plan(&context))
        //})
        let results = self
            .execute_query_pipeline(&context, true) // PromQL: topk enabled
            .map_err(|e| {
                warn!("Query execution failed: {}", e);
                e
            })
            .ok()?;

        let result = Some((
            context.metadata.query_output_labels,
            QueryResult::vector(results, context.query_time),
        ));

        // Determine query routing order based on function type.
        // USampling functions prefer the precomputed path first (sketch fallback),
        // while EHUniv/EHKLL functions prefer the sketch path first.
        // let prefer_precomputed = self
        //     .extract_sketch_func_name(&query)
        //     .is_some_and(|name| is_usampling_function(&name));

        // if !prefer_precomputed {
        //     // Non-USampling sketch functions: try sketch path first
        //     if let Some(result) = self.handle_sketch_query_promql(&query, time) {
        //         let total_query_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch query handling took: {:.2}ms",
        //             total_query_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        // // Precomputed pipeline
        // let precomputed_result = (|| -> Option<(KeyByLabelNames, QueryResult)> {
        //     let context = self.build_query_execution_context_promql(query.clone(), time)?;

        //     debug!(
        //         "Querying store for metric: {}, aggregation_id: {}, range: [{}, {}]",
        //         context.metric,
        //         context.agg_info.aggregation_id_for_value,
        //         context.store_plan.values_query.start_timestamp,
        //         context.store_plan.values_query.end_timestamp
        //     );

        //     let results = self
        //         .execute_query_pipeline(&context, true) // PromQL: topk enabled
        //         .map_err(|e| {
        //             warn!("Query execution failed: {}", e);
        //             e
        //         })
        //         .ok()?;

        //     Some((
        //         context.metadata.query_output_labels,
        //         QueryResult::vector(results, context.query_time),
        //     ))
        // })();

        // if precomputed_result.is_some() {
        //     let total_query_duration = query_start_time.elapsed();
        //     debug!(
        //         "Total query handling took: {:.2}ms",
        //         total_query_duration.as_secs_f64() * 1000.0
        //     );
        //     return precomputed_result;
        // }

        // // Fallback: USampling functions try sketch if precomputed had no data
        // if prefer_precomputed {
        //     if let Some(result) = self.handle_sketch_query_promql(&query, time) {
        //         let total_query_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch fallback query handling took: {:.2}ms",
        //             total_query_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        let total_query_duration = query_start_time.elapsed();
        debug!(
            "Total query handling took: {:.2}ms (no results)",
            total_query_duration.as_secs_f64() * 1000.0
        );
        result
    }

    pub fn build_query_execution_context_promql(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        // Track query configuration processing latency
        let config_start_time = Instant::now();

        let query_config = self.find_query_config(&query)?;

        let config_duration = config_start_time.elapsed();
        debug!(
            "[LATENCY] Query configuration processing: {:.2}ms",
            config_duration.as_secs_f64() * 1000.0
        );

        let query_time = Self::convert_query_time_to_data_time(time);

        // Parse PromQL AST using promql-parser crate
        let parse_start_time = Instant::now();
        let ast = match promql_parser::parser::parse(&query) {
            Ok(ast) => {
                let parse_duration = parse_start_time.elapsed();
                debug!(
                    "PromQL parsing took: {:.2}ms",
                    parse_duration.as_secs_f64() * 1000.0
                );
                ast
            }
            Err(e) => {
                warn!("Failed to parse PromQL query '{}': {}", query, e);
                return None;
            }
        };

        let pattern_match_start_time = Instant::now();

        let mut found_match = None;
        for (pattern_type, patterns) in &self.controller_patterns {
            for pattern in patterns {
                debug!(
                    "Trying pattern type: {:?} for query: {}",
                    pattern_type, query
                );
                let match_result = pattern.matches(&ast);
                debug!("Match result: {:?}", match_result);
                if match_result.matches {
                    found_match = Some((*pattern_type, match_result));
                    break;
                }
            }
            if found_match.is_some() {
                break;
            }
        }

        let (query_pattern_type, match_result) = match found_match {
            Some((pt, result)) => {
                let pattern_match_duration = pattern_match_start_time.elapsed();
                debug!(
                    "Pattern matching took: {:.2}ms",
                    pattern_match_duration.as_secs_f64() * 1000.0
                );
                (pt, result)
            }
            None => {
                warn!("No matching pattern found for query: {}", query);
                return None;
            }
        };

        debug!("Found matching query config for: {}", query);

        // Track query metadata setup latency
        let query_metadata_start_time = Instant::now();

        // Extract metric and spatial filter using AST-based approach
        // SQL issue: table name and filter label names, return empty filter for now but compute later
        let (metric, spatial_filter) = get_metric_and_spatial_filter(&match_result);

        // Get all labels from inference config for this metric
        let promql_schema = match &self.inference_config.schema {
            SchemaConfig::PromQL(schema) => schema,
            SchemaConfig::SQL(_) => {
                warn!("PromQL query requested but config has SQL schema");
                return None;
            }
            &SchemaConfig::ElasticQueryDSL => {
                warn!("PromQL query requested but config has ElasticQueryDSL schema");
                return None;
            }
            SchemaConfig::ElasticSQL(_) => {
                warn!("PromQL query requested but config has ElasticSQL schema");
                return None;
            }
        };
        let all_labels = promql_schema
            .get_labels(&metric)
            .cloned()
            .unwrap_or_else(|| {
                warn!(
                    "No metric configuration found for '{}', using empty labels",
                    metric
                );
                panic!("No metric configuration found");
            });

        // Determine query output labels based on pattern type
        // TODO: should we be returning this and using it to convert to final HTTP response?
        let mut query_output_labels = match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                // For temporal-only queries, output all labels
                all_labels.clone()
            }
            QueryPatternType::OnlySpatial => {
                // Extract spatial aggregation output labels using AST-based approach
                get_spatial_aggregation_output_labels(&match_result, &all_labels)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                // Extract spatial aggregation output labels for combined queries
                let temporal_aggregation = match_result.get_function_name().unwrap();
                let spatial_aggregation = match_result.get_aggregation_op().unwrap();
                // iff temporal outer labels issubset of spatial inner labels, collapse
                // SQL issue: take into account labels from the query, not needed at present because only uses promql translations
                match get_is_collapsable(&temporal_aggregation, &spatial_aggregation) {
                    false => all_labels.clone(),
                    true => get_spatial_aggregation_output_labels(&match_result, &all_labels),
                }
            }
        };

        let timestamps =
            self.calculate_query_timestamps_promql(query_time, query_pattern_type, &match_result);

        // Extract statistics to compute using AST-based approach
        let statistics_to_compute = get_statistics_to_compute(query_pattern_type, &match_result);
        if statistics_to_compute.len() != 1 {
            panic!(
                "Expected exactly one statistic to compute, found {}",
                statistics_to_compute.len()
            );
        }
        let statistic_to_compute = statistics_to_compute.first().unwrap();

        // For topk queries, prepend "__name__" to query_output_labels
        if *statistic_to_compute == Statistic::Topk {
            let mut new_labels = vec!["__name__".to_string()];
            new_labels.extend(query_output_labels.labels);
            query_output_labels = KeyByLabelNames::new(new_labels);
        }

        let query_kwargs = self
            .build_query_kwargs_promql(statistic_to_compute, query_pattern_type, &match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let query_metadata_duration = query_metadata_start_time.elapsed();
        debug!(
            "[LATENCY] Query metadata calculation: {:.2}ms",
            query_metadata_duration.as_secs_f64() * 1000.0
        );

        // Create query metadata
        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute: *statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Track aggregation configuration processing latency
        let agg_config_start_time = Instant::now();

        let agg_info = self.get_aggregation_id_info(query_config);

        let agg_config_duration = agg_config_start_time.elapsed();
        debug!(
            "[LATENCY] Aggregation configuration processing: {:.2}ms",
            agg_config_duration.as_secs_f64() * 1000.0
        );

        // Create query plan (determines window type and calculates timestamps)
        let query_plan = self
            .create_store_query_plan(&metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        // let window_type = if query_plan.values_query.is_exact_query {
        //     "sliding"
        // } else {
        //     "tumbling"
        // };

        // Create execution context
        // do_merge is true for temporal queries (OnlyTemporal or OneTemporalOneSpatial)
        let do_merge = query_pattern_type == QueryPatternType::OnlyTemporal
            || query_pattern_type == QueryPatternType::OneTemporalOneSpatial;

        let grouping_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_output_labels.clone());

        let aggregated_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric: metric.clone(),
            metadata,
            store_plan: query_plan.clone(),
            agg_info: agg_info.clone(),
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })

        // TODO: Handle spatial aggregation for OneTemporalOneSpatial when not collapsable
    }

    /// Merge precomputed outputs (extracts buckets from timestamped data)
    fn merge_precomputed_outputs(
        &self,
        precomputed_outputs_map: &TimestampedBucketsMap,
        do_merge: bool,
        aggregation_type: String,
    ) -> HashMap<Option<KeyByLabelValues>, Box<dyn crate::data_model::AggregateCore>> {
        #[cfg(feature = "extra_debugging")]
        let start_time = Instant::now();
        #[cfg(feature = "extra_debugging")]
        debug!("Starting merge for {} keys", precomputed_outputs_map.len());
        #[cfg(feature = "extra_debugging")]
        debug!(
            "do_merge: {}, aggregation_type: {}",
            do_merge, aggregation_type
        );

        // Merge if: temporal query OR DeltaSetAggregator (which accumulates keys over time)
        let should_merge = do_merge || aggregation_type == "DeltaSetAggregator";

        let mut merged = HashMap::with_capacity(precomputed_outputs_map.len());

        for (idx, (key, timestamped_buckets)) in precomputed_outputs_map.iter().enumerate() {
            #[cfg(feature = "extra_debugging")]
            debug!(
                "Processing key {} of {}: {:?}",
                idx + 1,
                precomputed_outputs_map.len(),
                key
            );
            #[cfg(feature = "extra_debugging")]
            debug!(
                "  Number of precomputes for this key: {}",
                timestamped_buckets.len()
            );

            if !timestamped_buckets.is_empty() {
                // Extract just the buckets (without timestamps) for merging
                let precomputes: Vec<Box<dyn AggregateCore>> = timestamped_buckets
                    .iter()
                    .map(|(_, bucket)| bucket.clone_boxed_core())
                    .collect();

                if should_merge {
                    #[cfg(feature = "extra_debugging")]
                    debug!("  Merging accumulators (should_merge=true)");
                    #[cfg(feature = "extra_debugging")]
                    let merge_start = Instant::now();
                    let merged_accumulator = self.merge_accumulators(&precomputes);
                    #[cfg(feature = "extra_debugging")]
                    let merge_duration = merge_start.elapsed();
                    #[cfg(feature = "extra_debugging")]
                    debug!(
                        "  Merge completed in {:.2}ms, result type: {}",
                        merge_duration.as_secs_f64() * 1000.0,
                        merged_accumulator.get_accumulator_type()
                    );
                    merged.insert(key.clone(), merged_accumulator);
                } else {
                    assert_eq!(
                        precomputes.len(),
                        1,
                        "Spatial queries should have exactly 1 precompute per key"
                    );
                    merged.insert(key.clone(), precomputes[0].clone_boxed_core());
                }
            }
        }

        #[cfg(feature = "extra_debugging")]
        let total_duration = start_time.elapsed();
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[LATENCY] Complete merge operation: {:.2}ms, merged {} keys",
            total_duration.as_secs_f64() * 1000.0,
            merged.len()
        );

        merged
    }

    /// Merge multiple accumulators using the merge_with method from AggregateCore trait
    /// This follows the Python merge_accumulators approach
    fn merge_accumulators(
        &self,
        accumulators: &[Box<dyn crate::data_model::AggregateCore>],
    ) -> Box<dyn crate::data_model::AggregateCore> {
        if accumulators.is_empty() {
            panic!("No accumulators to merge");
        }

        if accumulators.len() == 1 {
            return accumulators[0].clone_boxed_core();
        }

        // Try to use optimized batch merge for KLL accumulators
        if !accumulators.is_empty()
            && accumulators[0].get_accumulator_type() == "DatasketchesKLLAccumulator"
        {
            use crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;

            match DatasketchesKLLAccumulator::merge_multiple(accumulators) {
                Ok(merged) => return Box::new(merged),
                Err(e) => {
                    warn!(
                        "Batch merge failed: {}. Falling back to sequential merge.",
                        e
                    );
                    // Fall through to sequential merge below
                }
            }
        }

        // Try to use optimized batch merge for CountMinSketch accumulators
        if !accumulators.is_empty()
            && accumulators[0].get_accumulator_type() == "CountMinSketchAccumulator"
        {
            use crate::precompute_operators::count_min_sketch_accumulator::CountMinSketchAccumulator;

            match CountMinSketchAccumulator::merge_multiple(accumulators) {
                Ok(merged) => return Box::new(merged),
                Err(e) => {
                    warn!(
                        "Batch merge failed: {}. Falling back to sequential merge.",
                        e
                    );
                    // Fall through to sequential merge below
                }
            }
        }

        // Fallback: sequential merge for other accumulator types
        // (Still benefits from Phase 1 optimization of merge_with)
        let mut result = accumulators[0].clone_boxed_core();

        for accumulator in &accumulators[1..] {
            match result.merge_with(accumulator.as_ref()) {
                Ok(merged) => {
                    result = merged;
                }
                Err(e) => {
                    warn!("Failed to merge accumulator: {}. Using existing result.", e);
                    // Continue with the current result if merge fails
                }
            }
        }

        result
    }

    /// Collects results when key and value use different aggregations
    fn collect_results_separate_keys(
        &self,
        merged_values: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        merged_keys: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        let mut unformatted_results = HashMap::new();

        for (key, precompute) in merged_keys {
            let keys_for_this_precompute = precompute
                .get_keys()
                .ok_or_else(|| "Keys required for separate aggregation".to_string())?;

            for key_for_this_precompute in keys_for_this_precompute {
                let value_precompute = merged_values
                    .get(key)
                    .ok_or_else(|| format!("No value for key: {:?}", key))?;

                let value = self
                    .query_precompute_for_statistic(
                        value_precompute.as_ref(),
                        statistic,
                        &Some(key_for_this_precompute.clone()),
                        query_kwargs,
                    )
                    .map_err(|e| format!("Query failed: {}", e))?;

                unformatted_results.insert(Some(key_for_this_precompute.clone()), value);
            }
        }

        Ok(unformatted_results)
    }

    /// Collects results when key and value use same aggregation
    fn collect_results_same_aggregation(
        &self,
        merged_outputs: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
        enable_topk_limiting: bool,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        let mut unformatted_results = HashMap::new();

        for (key, precompute) in merged_outputs {
            if let Some(unwrapped_keys) = precompute.get_keys() {
                let keys_to_process = if enable_topk_limiting {
                    self.limit_keys_for_topk(unwrapped_keys, statistic, query_kwargs)?
                } else {
                    unwrapped_keys
                };

                for key_for_this_precompute in keys_to_process {
                    let value = self
                        .query_precompute_for_statistic(
                            precompute.as_ref(),
                            statistic,
                            &Some(key_for_this_precompute.clone()),
                            query_kwargs,
                        )
                        .map_err(|e| format!("Query failed: {}", e))?;

                    unformatted_results.insert(Some(key_for_this_precompute.clone()), value);
                }
            } else {
                let value = self
                    .query_precompute_for_statistic(
                        precompute.as_ref(),
                        statistic,
                        &None,
                        query_kwargs,
                    )
                    .map_err(|e| format!("Query failed: {}", e))?;

                unformatted_results.insert(key.clone(), value);
            }
        }

        Ok(unformatted_results)
    }

    /// Limits keys for topk queries
    fn limit_keys_for_topk(
        &self,
        keys: Vec<KeyByLabelValues>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<Vec<KeyByLabelValues>, String> {
        if *statistic != Statistic::Topk {
            return Ok(keys);
        }

        let k_str = query_kwargs
            .get("k")
            .ok_or_else(|| "Missing k parameter for topk".to_string())?;

        let k = k_str
            .parse::<usize>()
            .map_err(|_| format!("Failed to parse k: '{}'", k_str))?;

        Ok(keys.into_iter().take(k).collect())
    }

    /// Query a precompute for a specific statistic
    /// This follows the Python approach where precompute.query(statistic, key) is called
    fn query_precompute_for_statistic(
        &self,
        precompute: &dyn AggregateCore,
        statistic: &Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // Handle different accumulator types and statistics using the trait methods
        // TODO: change this logic to just check Single vs MultipleSubpopulationAggregate
        match precompute.get_accumulator_type() {
            "SumAccumulator" => {
                if let Some(sum_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::sum_accumulator::SumAccumulator>() {
                    use crate::data_model::SingleSubpopulationAggregate;
                    sum_acc.query(*statistic, None)
                } else {
                    Err("Failed to downcast to SumAccumulator".into())
                }
            }
            "MinMaxAccumulator" => {
                if let Some(minmax_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::min_max_accumulator::MinMaxAccumulator>() {
                    use crate::data_model::SingleSubpopulationAggregate;
                    minmax_acc.query(*statistic, None)
                } else {
                    Err("Failed to downcast to MinMaxAccumulator".into())
                }
            }
            "IncreaseAccumulator" => {
                if let Some(inc_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::increase_accumulator::IncreaseAccumulator>() {
                    use crate::data_model::SingleSubpopulationAggregate;
                    inc_acc.query(*statistic, None)
                } else {
                    Err("Failed to downcast to IncreaseAccumulator".into())
                }
            }
            "MultipleSumAccumulator" => {
                if let Some(multi_sum_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::multiple_sum_accumulator::MultipleSumAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        multi_sum_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for MultipleSumAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to MultipleSumAccumulator".into())
                }
            }
            "MultipleMinMaxAccumulator" => {
                if let Some(multi_minmax_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::multiple_min_max_accumulator::MultipleMinMaxAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        multi_minmax_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for MultipleMinMaxAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to MultipleMinMaxAccumulator".into())
                }
            }
            "MultipleIncreaseAccumulator" => {
                if let Some(multi_inc_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::multiple_increase_accumulator::MultipleIncreaseAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        multi_inc_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for MultipleIncreaseAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to MultipleIncreaseAccumulator".into())
                }
            }
            "CountMinSketchAccumulator" => {
                if let Some(cms_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::count_min_sketch_accumulator::CountMinSketchAccumulator>() {
                    use crate::data_model::MultipleSubpopulationAggregate;
                    if let Some(key_val) = key {
                        cms_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for CountMinSketchAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to CountMinSketchAccumulator".into())
                }
            }
            "CountMinSketchWithHeapAccumulator" => {
                if let Some(cms_heap_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::count_min_sketch_with_heap_accumulator::CountMinSketchWithHeapAccumulator>() {
                    use crate::data_model::MultipleSubpopulationAggregate;
                    if let Some(key_val) = key {
                        cms_heap_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for CountMinSketchWithHeapAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to CountMinSketchWithHeapAccumulator".into())
                }
            }
            "DatasketchesKLLAccumulator" => {
                if let Some(kll_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator>() {
                    use crate::data_model::SingleSubpopulationAggregate;
                    kll_acc.query(*statistic, Some(query_kwargs))
                } else {
                    Err("Failed to downcast to DatasketchesKLLAccumulator".into())
                }
            }
            "HydraKllSketchAccumulator" => {
                if let Some(hydra_kll_acc) = precompute.as_any()
                    .downcast_ref::<crate::precompute_operators::hydra_kll_accumulator::HydraKllSketchAccumulator>()
                {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        hydra_kll_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        Err("Key required for HydraKllSketchAccumulator".into())
                    }
                } else {
                    Err("Failed to downcast to HydraKllSketchAccumulator".into())
                }
            }
            "DeltaSetAggregatorAccumulator" => {
                if let Some(delta_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        delta_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        // For DeltaSetAggregatorAccumulator without a key, return the union size
                        Ok((delta_acc.added.union(&delta_acc.removed).count()) as f64)
                    }
                } else {
                    Err("Failed to downcast to DeltaSetAggregatorAccumulator".into())
                }
            }
            "SetAggregatorAccumulator" => {
                if let Some(set_acc) = precompute.as_any().downcast_ref::<crate::precompute_operators::set_aggregator_accumulator::SetAggregatorAccumulator>() {
                    if let Some(key_val) = key {
                        use crate::data_model::MultipleSubpopulationAggregate;
                        set_acc.query(*statistic, key_val, Some(query_kwargs))
                    } else {
                        // For SetAggregatorAccumulator without a key, return the set size
                        Ok(set_acc.added.len() as f64)
                    }
                } else {
                    Err("Failed to downcast to SetAggregatorAccumulator".into())
                }
            }
            _ => {
                Err(format!("Unknown accumulator type: {}", precompute.get_accumulator_type()).into())
            }
        }
    }

    // ============================================================
    // Range Query Support
    // ============================================================

    /// Validate range query parameters
    fn validate_range_query_params(
        &self,
        start: u64,
        end: u64,
        step: u64,
        tumbling_window_ms: u64,
    ) -> Result<(), String> {
        if start >= end {
            return Err("start must be before end".to_string());
        }
        if step == 0 {
            return Err("step must be positive".to_string());
        }
        if !step.is_multiple_of(tumbling_window_ms) {
            return Err(format!(
                "step ({} ms) must be a multiple of tumbling window size ({} ms)",
                step, tumbling_window_ms
            ));
        }
        Ok(())
    }

    /// Build execution context for range query
    pub fn build_range_query_execution_context_promql(
        &self,
        query: String,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<RangeQueryExecutionContext> {
        // First, build the base instant query context (reuse existing logic)
        // Use 'end' as the reference time for parsing
        let base_context = self.build_query_execution_context_promql(query, end)?;

        // Convert to milliseconds
        let start_ms = Self::convert_query_time_to_data_time(start);
        let end_ms = Self::convert_query_time_to_data_time(end);
        let step_ms = (step * 1000.0) as u64;

        // Get window size
        let tumbling_window_ms = self
            .streaming_config
            .get_aggregation_config(base_context.agg_info.aggregation_id_for_value)
            .map(|config| config.window_size * 1000)?;

        // Validate parameters
        self.validate_range_query_params(start_ms, end_ms, step_ms, tumbling_window_ms)
            .map_err(|e| {
                warn!("Range query validation failed: {}", e);
                e
            })
            .ok()?;

        // Calculate lookback from the base context's store plan
        let lookback_ms = base_context.store_plan.values_query.end_timestamp
            - base_context.store_plan.values_query.start_timestamp;

        let buckets_per_step = (step_ms / tumbling_window_ms) as usize;
        let lookback_bucket_count = (lookback_ms / tumbling_window_ms) as usize;

        // Modify the store plan to cover the entire range
        let mut extended_store_plan = base_context.store_plan.clone();
        extended_store_plan.values_query.start_timestamp = start_ms.saturating_sub(lookback_ms);
        extended_store_plan.values_query.end_timestamp = end_ms;
        // Range queries always use range fetch, not exact
        extended_store_plan.values_query.is_exact_query = false;

        Some(RangeQueryExecutionContext {
            base: QueryExecutionContext {
                store_plan: extended_store_plan,
                ..base_context
            },
            range_params: RangeQueryParams {
                start: start_ms,
                end: end_ms,
                step: step_ms,
            },
            buckets_per_step,
            lookback_bucket_count,
            tumbling_window_ms,
        })
    }

    // /// Try to handle a PromQL range query via the sketch shortcut path.
    // /// Returns Some if the query is sketch-backed and PromSketchStore is available.
    // /// Returns None to fall through to the precomputed pipeline.
    // fn handle_sketch_range_query_promql(
    //     &self,
    //     query: &str,
    //     start: f64,
    //     end: f64,
    //     step: f64,
    // ) -> Option<(KeyByLabelNames, QueryResult)> {
    //     let ps = self.promsketch_store.as_ref()?;

    //     let components = match self.parse_sketch_query_components(query) {
    //         Some(c) => c,
    //         None => {
    //             debug!(
    //                 "Sketch range query: could not parse sketch components from '{}'",
    //                 query
    //             );
    //             return None;
    //         }
    //     };

    //     let eval_start = Instant::now();
    //     let range_ms = components.range_seconds * 1000;

    //     // Convert query params to ms
    //     let start_ms = Self::convert_query_time_to_data_time(start);
    //     let end_ms = Self::convert_query_time_to_data_time(end);
    //     let step_ms = (step * 1000.0) as u64;

    //     if step_ms == 0 || start_ms >= end_ms {
    //         warn!(
    //             "Sketch range query: invalid params step_ms={}, start_ms={}, end_ms={}",
    //             step_ms, start_ms, end_ms
    //         );
    //         return None;
    //     }

    //     // Get all matching series labels
    //     let series_labels = ps.matching_series_labels(&components.metric);
    //     if series_labels.is_empty() {
    //         debug!(
    //             "Sketch range query: no matching series for {}, falling through",
    //             components.metric
    //         );
    //         return None;
    //     }

    //     info!(
    //         "Sketch range query: {}({}) over [{}, {}] step {} with {} series",
    //         components.func_name,
    //         components.metric,
    //         start_ms,
    //         end_ms,
    //         step_ms,
    //         series_labels.len()
    //     );

    //     // For each matching series, iterate over time steps
    //     let mut range_elements: Vec<RangeVectorElement> = Vec::new();

    //     for series_label in &series_labels {
    //         let labels = KeyByLabelValues::new_with_labels(vec![series_label.clone()]);
    //         let mut element = RangeVectorElement::new(labels);

    //         let mut current_time = start_ms;
    //         while current_time <= end_ms {
    //             let step_end = current_time;
    //             let step_start = step_end.saturating_sub(range_ms);

    //             match ps.eval(
    //                 &components.func_name,
    //                 series_label,
    //                 components.args,
    //                 step_start,
    //                 step_end,
    //             ) {
    //                 Ok(value) => element.add_sample(current_time, value),
    //                 Err(e) => {
    //                     debug!(
    //                         "Sketch range query: eval failed for {} at t={}: {}",
    //                         series_label, current_time, e
    //                     );
    //                 }
    //             }

    //             current_time += step_ms;
    //         }

    //         if !element.samples.is_empty() {
    //             range_elements.push(element);
    //         }
    //     }

    //     if range_elements.is_empty() {
    //         debug!(
    //             "Sketch range query: all series produced empty results for {}({})",
    //             components.func_name, components.metric
    //         );
    //         ps_metrics::SKETCH_QUERIES_TOTAL
    //             .with_label_values(&["miss"])
    //             .inc();
    //         return None;
    //     }

    //     ps_metrics::SKETCH_QUERIES_TOTAL
    //         .with_label_values(&["hit"])
    //         .inc();
    //     ps_metrics::SKETCH_QUERY_DURATION.observe(eval_start.elapsed().as_secs_f64());

    //     let output_labels = KeyByLabelNames::new(vec!["__name__".to_string()]);
    //     Some((output_labels, QueryResult::matrix(range_elements)))
    // }

    /// Main entry point for range queries
    pub fn handle_range_query_promql(
        &self,
        query: String,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let query_start_time = Instant::now();
        debug!(
            "Handling range query: {} from {} to {} step {}",
            query, start, end, step
        );

        let context = self.build_range_query_execution_context_promql(query, start, end, step)?;

        // Execute range query pipeline
        let results: Vec<RangeVectorElement> = self
            .execute_range_query_pipeline(&context)
            .map_err(|e| {
                warn!("Range query execution failed: {}", e);
                e
            })
            .ok()?;

        // // Determine query routing order based on function type.
        // // USampling functions prefer the precomputed path first (sketch fallback),
        // // while EHUniv/EHKLL functions prefer the sketch path first.
        // let prefer_precomputed = self
        //     .extract_sketch_func_name(&query)
        //     .is_some_and(|name| is_usampling_function(&name));

        // if !prefer_precomputed {
        //     // Non-USampling sketch functions: try sketch path first
        //     if let Some(result) = self.handle_sketch_range_query_promql(&query, start, end, step) {
        //         let total_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch range query handling took: {:.2}ms",
        //             total_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        // // Precomputed pipeline
        // let precomputed_result = (|| -> Option<(KeyByLabelNames, QueryResult)> {
        //     let context =
        //         self.build_range_query_execution_context_promql(query.clone(), start, end, step)?;

        //     let results: Vec<RangeVectorElement> = self
        //         .execute_range_query_pipeline(&context)
        //         .map_err(|e| {
        //             warn!("Range query execution failed: {}", e);
        //             e
        //         })
        //         .ok()?;

        //     Some((
        //         context.base.metadata.query_output_labels,
        //         QueryResult::matrix(results),
        //     ))
        // })();

        // // Fallback: USampling functions try sketch if precomputed had no data
        // if prefer_precomputed {
        //     if let Some(result) = self.handle_sketch_range_query_promql(&query, start, end, step) {
        //         let total_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch fallback range query handling took: {:.2}ms",
        //             total_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        let total_duration = query_start_time.elapsed();
        debug!(
            "Total range query handling took: {:.2}ms",
            total_duration.as_secs_f64() * 1000.0
        );

        Some((
            context.base.metadata.query_output_labels,
            QueryResult::matrix(results),
        ))
    }

    /// Execute the range query pipeline
    fn execute_range_query_pipeline(
        &self,
        context: &RangeQueryExecutionContext,
    ) -> Result<Vec<crate::engines::query_result::RangeVectorElement>, String> {
        use crate::engines::query_result::RangeVectorElement;
        use crate::engines::window_merger::create_window_merger;

        // Step 1: Fetch all data needed for the entire range
        let all_data = self.execute_store_query(&context.base.store_plan.values_query)?;

        if all_data.is_empty() {
            return Err(format!("No data found for metric: {}", context.base.metric));
        }

        debug!(
            "Range query: fetched {} keys, {} total buckets",
            all_data.len(),
            all_data.values().map(|v| v.len()).sum::<usize>()
        );

        let mut results: HashMap<KeyByLabelValues, RangeVectorElement> = HashMap::new();

        // Determine accumulator type for merger selection
        let accumulator_type = &context.base.agg_info.aggregation_type_for_value;

        // Calculate step parameters
        let step_ms = context.range_params.step;
        let start_ms = context.range_params.start;
        let end_ms = context.range_params.end;
        let buckets_per_step = context.buckets_per_step;
        let lookback_bucket_count = context.lookback_bucket_count;

        let window_mode = if buckets_per_step <= lookback_bucket_count {
            "sliding (slide <= size)"
        } else {
            "hopping (slide > size)"
        };
        debug!(
            "Range query params: start={}, end={}, step_ms={}, tumbling_window_ms={}, \
             buckets_per_step (slide)={}, lookback_bucket_count (size)={}, mode={}",
            start_ms,
            end_ms,
            step_ms,
            context.tumbling_window_ms,
            buckets_per_step,
            lookback_bucket_count,
            window_mode
        );

        // Process each key independently
        for (key_opt, timestamped_buckets) in &all_data {
            let key = match key_opt {
                Some(k) => k.clone(),
                None => continue, // Skip None keys for now
            };

            // Build lookup: bucket_start_timestamp -> bucket for O(1) access
            let bucket_map: HashMap<u64, &dyn AggregateCore> = timestamped_buckets
                .iter()
                .map(|((start, _), bucket)| (*start, bucket.as_ref()))
                .collect();

            debug!(
                "Key {:?}: built bucket_map with {} entries, timestamps: {:?}",
                key,
                bucket_map.len(),
                bucket_map.keys().collect::<Vec<_>>()
            );

            // Create result element for this key
            let mut element = RangeVectorElement::new(key.clone());

            // Calculate window parameters
            let tumbling_window_ms = context.tumbling_window_ms;
            let lookback_ms = (lookback_bucket_count as u64) * tumbling_window_ms;

            debug!(
                "Key {:?}: range [{}, {}], step={}, lookback_ms={}, tumbling_window_ms={}",
                key, start_ms, end_ms, step_ms, lookback_ms, tumbling_window_ms
            );

            // Iterate by OUTPUT timestamp, not by bucket index
            let mut current_time = start_ms;
            while current_time <= end_ms {
                // Window covers [current_time - lookback_ms, current_time)
                // This means we look at buckets that START within this range
                let window_start = current_time.saturating_sub(lookback_ms);

                // Collect all AVAILABLE buckets in this window (skip missing ones)
                let mut window_buckets: Vec<Box<dyn AggregateCore>> = Vec::new();

                let mut t = window_start;
                while t < current_time {
                    if let Some(bucket) = bucket_map.get(&t) {
                        window_buckets.push((*bucket).clone_boxed_core());
                    }
                    // If bucket missing at timestamp t, just skip it (partial data is okay)
                    t += tumbling_window_ms;
                }

                if !window_buckets.is_empty() {
                    // Merge available buckets
                    let mut merger = create_window_merger(accumulator_type);
                    merger.initialize(window_buckets);

                    match merger.get_merged() {
                        Ok(merged) => {
                            // Query statistic and emit sample at current_time
                            match self.query_precompute_for_statistic(
                                merged.as_ref(),
                                &context.base.metadata.statistic_to_compute,
                                &Some(key.clone()),
                                &context.base.metadata.query_kwargs,
                            ) {
                                Ok(value) => {
                                    debug!(
                                        "Key {:?}: emitting sample (t={}, value={})",
                                        key, current_time, value
                                    );
                                    element.add_sample(current_time, value);
                                }
                                Err(e) => {
                                    debug!(
                                        "Failed to query statistic at t={} for key {:?}: {}",
                                        current_time, key, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            debug!(
                                "Failed to get merged result at t={} for key {:?}: {}",
                                current_time, key, e
                            );
                        }
                    }
                } else {
                    // No data at all for this window - skip sample
                    debug!(
                        "Key {:?}: skipping sample at {} - no data in window [{}, {})",
                        key, current_time, window_start, current_time
                    );
                }

                current_time += step_ms;
            }

            debug!(
                "Key {:?}: finished with {} samples",
                key,
                element.samples.len()
            );

            // Only include keys with samples
            if !element.samples.is_empty() {
                results.insert(key, element);
            }
        }

        // Convert to Vec
        Ok(results.into_values().collect())
    }
}

#[cfg(test)]
mod range_query_tests {
    use crate::data_model::{AggregateCore, KeyByLabelValues, SerializableToSink};
    use crate::engines::window_merger::NaiveMerger;
    use serde_json::Value;
    use std::any::Any;

    /// Mock accumulator that stores a unique ID to detect stale window reuse
    #[derive(Clone, Debug)]
    struct MockBucketAccumulator {
        bucket_id: u64,
        value: f64,
    }

    impl MockBucketAccumulator {
        fn new(bucket_id: u64, value: f64) -> Self {
            Self { bucket_id, value }
        }
    }

    impl SerializableToSink for MockBucketAccumulator {
        fn serialize_to_json(&self) -> Value {
            serde_json::json!({"bucket_id": self.bucket_id, "value": self.value})
        }

        fn serialize_to_bytes(&self) -> Vec<u8> {
            format!("{}:{}", self.bucket_id, self.value).into_bytes()
        }
    }

    impl AggregateCore for MockBucketAccumulator {
        fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
            Box::new(self.clone())
        }

        fn type_name(&self) -> &'static str {
            "MockBucketAccumulator"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn merge_with(
            &self,
            other: &dyn AggregateCore,
        ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
            if let Some(other_mock) = other.as_any().downcast_ref::<MockBucketAccumulator>() {
                // Sum values, keep max bucket_id to track which buckets are in window
                Ok(Box::new(MockBucketAccumulator::new(
                    self.bucket_id.max(other_mock.bucket_id),
                    self.value + other_mock.value,
                )))
            } else {
                Err("Cannot merge with different accumulator type".into())
            }
        }

        fn get_accumulator_type(&self) -> &'static str {
            "MockBucketAccumulator"
        }

        fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
            None
        }
    }

    /// Simulates the sliding window loop from execute_range_query_pipeline
    /// Returns: Vec of (timestamp, merged_value, max_bucket_id_in_window)
    fn simulate_sliding_window(
        buckets: Vec<Box<dyn AggregateCore>>,
        lookback_bucket_count: usize,
        buckets_per_step: usize,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Vec<(u64, f64, u64)> {
        use crate::engines::window_merger::WindowMerger;

        let mut results = Vec::new();

        if buckets.len() < lookback_bucket_count {
            return results;
        }

        let mut merger = NaiveMerger::new();

        // Initialize with first window
        let initial_window: Vec<_> = buckets[0..lookback_bucket_count]
            .iter()
            .map(|b| b.clone_boxed_core())
            .collect();
        merger.initialize(initial_window);

        let mut bucket_index = lookback_bucket_count;
        let mut current_time = start_ms;

        while current_time <= end_ms {
            // Query current window
            if let Ok(merged) = merger.get_merged() {
                if let Some(mock) = merged.as_any().downcast_ref::<MockBucketAccumulator>() {
                    results.push((current_time, mock.value, mock.bucket_id));
                }
            }

            // Slide window for next step
            current_time += step_ms;

            if current_time <= end_ms {
                if bucket_index + buckets_per_step <= buckets.len() {
                    let new_buckets: Vec<_> = buckets
                        [bucket_index..bucket_index + buckets_per_step]
                        .iter()
                        .map(|b| b.clone_boxed_core())
                        .collect();
                    merger.slide(buckets_per_step, new_buckets);
                    bucket_index += buckets_per_step;
                } else {
                    // Not enough buckets to continue - stop to avoid stale data
                    break;
                }
            }
        }

        results
    }

    /// Simulates sliding window with proper timestamp alignment for missing data.
    /// This accounts for the scenario where the store returns fewer buckets than
    /// expected because data is missing at the start of the query range.
    ///
    /// # Arguments
    /// * `expected_bucket_count` - How many buckets we would have if data was complete
    fn simulate_sliding_window_with_alignment(
        buckets: Vec<Box<dyn AggregateCore>>,
        lookback_bucket_count: usize,
        buckets_per_step: usize,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        expected_bucket_count: usize,
    ) -> Vec<(u64, f64, u64)> {
        use crate::engines::window_merger::WindowMerger;

        let mut results = Vec::new();

        // Check if we have enough buckets for at least one window
        if buckets.len() < lookback_bucket_count {
            return results;
        }

        // Calculate missing data offset
        let missing_buckets = expected_bucket_count.saturating_sub(buckets.len());
        let tumbling_window_ms = step_ms / (buckets_per_step as u64);

        // First valid sample is offset by missing buckets (data missing at the start)
        let first_valid_sample_ms = start_ms + (missing_buckets as u64) * tumbling_window_ms;

        // Round up to step boundary if needed
        let first_sample_ms = if first_valid_sample_ms <= start_ms {
            start_ms
        } else {
            let offset = first_valid_sample_ms - start_ms;
            if offset.is_multiple_of(step_ms) {
                first_valid_sample_ms
            } else {
                start_ms + ((offset / step_ms) + 1) * step_ms
            }
        };

        // When we have missing buckets at the start, we need to figure out where to
        // start reading from the available buckets. The missing buckets are conceptually
        // at the beginning, so we start reading from the first available bucket.
        //
        // However, if we rounded up to a step boundary, we may need to skip some
        // additional buckets from what we have.
        let extra_offset_ms = first_sample_ms.saturating_sub(first_valid_sample_ms);
        let extra_buckets_to_skip = (extra_offset_ms / tumbling_window_ms) as usize;

        // Check if we have enough data for at least one window after any extra skip
        if extra_buckets_to_skip + lookback_bucket_count > buckets.len() {
            return results;
        }

        let mut merger = NaiveMerger::new();

        // Initialize with window at adjusted position
        let initial_window: Vec<_> = buckets
            [extra_buckets_to_skip..extra_buckets_to_skip + lookback_bucket_count]
            .iter()
            .map(|b| b.clone_boxed_core())
            .collect();
        merger.initialize(initial_window);

        let mut bucket_index = extra_buckets_to_skip + lookback_bucket_count;
        let mut current_time = first_sample_ms;

        while current_time <= end_ms {
            // Query current window
            if let Ok(merged) = merger.get_merged() {
                if let Some(mock) = merged.as_any().downcast_ref::<MockBucketAccumulator>() {
                    results.push((current_time, mock.value, mock.bucket_id));
                }
            }

            // Slide window for next step
            current_time += step_ms;

            if current_time <= end_ms {
                if bucket_index + buckets_per_step <= buckets.len() {
                    let new_buckets: Vec<_> = buckets
                        [bucket_index..bucket_index + buckets_per_step]
                        .iter()
                        .map(|b| b.clone_boxed_core())
                        .collect();
                    merger.slide(buckets_per_step, new_buckets);
                    bucket_index += buckets_per_step;
                } else {
                    break;
                }
            }
        }

        results
    }

    #[test]
    fn test_sliding_window_sufficient_buckets() {
        // Setup: 7 buckets, lookback=5, step=1
        // Should produce 3 valid samples
        let buckets: Vec<Box<dyn AggregateCore>> = (0..7)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms (3 steps: 1000, 2000, 3000)
            1000, // step_ms
        );

        assert_eq!(results.len(), 3, "Should produce 3 samples");

        // Window 1: buckets [0,1,2,3,4], max_id=4, value=50
        assert_eq!(results[0], (1000, 50.0, 4));
        // Window 2: buckets [1,2,3,4,5], max_id=5, value=50
        assert_eq!(results[1], (2000, 50.0, 5));
        // Window 3: buckets [2,3,4,5,6], max_id=6, value=50
        assert_eq!(results[2], (3000, 50.0, 6));
    }

    #[test]
    fn test_sliding_window_insufficient_buckets_stops_early() {
        // 6 buckets, lookback=5, step=1
        // Requesting 3 timestamps but only have data for 2
        // Should stop early rather than produce stale samples
        let buckets: Vec<Box<dyn AggregateCore>> = (0..6)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms (requests 3 steps: 1000, 2000, 3000)
            1000, // step_ms
        );

        println!("Results: {:?}", results);

        // Should only produce 2 valid samples (not 3 with stale data)
        assert_eq!(
            results.len(),
            2,
            "Should only produce 2 samples when data is insufficient for 3rd"
        );

        // Window 1: buckets [0,1,2,3,4], max_id=4
        assert_eq!(results[0], (1000, 50.0, 4));
        // Window 2: buckets [1,2,3,4,5], max_id=5
        assert_eq!(results[1], (2000, 50.0, 5));
        // No window 3 - not enough buckets to slide
    }

    #[test]
    fn test_sliding_window_exactly_enough_buckets() {
        // 5 buckets, lookback=5, step=1
        // Should produce exactly 1 sample (initial window only, can't slide)
        let buckets: Vec<Box<dyn AggregateCore>> = (0..5)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms
            1000, // step_ms
        );

        println!("Results with exactly enough buckets: {:?}", results);

        // Should produce only 1 sample - can't slide without more buckets
        assert_eq!(results.len(), 1, "Should produce exactly 1 sample");
        assert_eq!(results[0], (1000, 50.0, 4));
    }

    #[test]
    fn test_sliding_window_multi_bucket_step() {
        // 10 buckets, lookback=4, step=2 buckets at a time
        // Should produce samples at positions requiring new data
        let buckets: Vec<Box<dyn AggregateCore>> = (0..10)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window(
            buckets, 4,    // lookback_bucket_count
            2,    // buckets_per_step (slide 2 at a time)
            1000, // start_ms
            4000, // end_ms (4 steps)
            1000, // step_ms
        );

        // Initial: [0,1,2,3], max_id=3
        // After slide 1: [2,3,4,5], max_id=5
        // After slide 2: [4,5,6,7], max_id=7
        // After slide 3: [6,7,8,9], max_id=9
        assert_eq!(results.len(), 4, "Should produce 4 samples");
        assert_eq!(results[0].2, 3, "Window 1 max_id should be 3");
        assert_eq!(results[1].2, 5, "Window 2 max_id should be 5");
        assert_eq!(results[2].2, 7, "Window 3 max_id should be 7");
        assert_eq!(results[3].2, 9, "Window 4 max_id should be 9");
    }

    #[test]
    fn test_sliding_window_missing_data_at_start_aligns_timestamps() {
        // Scenario: Query requests timestamps 1000, 2000, 3000
        // But only 5 buckets exist (enough for 1 sample), not 7 (for 3 samples)
        // lookback=5, step=1 bucket
        // Expected buckets for [1000, 3000]: 7 (5 for first window + 2 steps)
        // Actual buckets: 5 (missing 2 at start)
        // Missing 2 buckets = 2000ms offset
        // First valid sample at: 1000 + 2000 = 3000ms

        let buckets: Vec<Box<dyn AggregateCore>> = (0..5)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window_with_alignment(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms
            1000, // step_ms
            7,    // expected_bucket_count for full range
        );

        // Should have 1 sample at timestamp 3000, NOT at 1000
        assert_eq!(results.len(), 1, "Should produce 1 sample");
        assert_eq!(results[0].0, 3000, "Sample should be at t=3000, not t=1000");
    }

    #[test]
    fn test_sliding_window_missing_data_rounds_to_step_boundary() {
        // Query: start=0, end=6000, step=2000 (timestamps: 0, 2000, 4000, 6000)
        // Lookback: 4 buckets, step: 2 buckets
        // Expected buckets: 4 + 6 = 10 buckets for full range
        // Actual: 7 buckets (missing 3 at start)
        // Missing 3 buckets = 3000ms offset
        // First valid sample time = 0 + 3000 = 3000ms
        // But 3000 is not on step boundary, so round UP to 4000ms

        let buckets: Vec<Box<dyn AggregateCore>> = (0..7)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window_with_alignment(
            buckets, 4,    // lookback_bucket_count
            2,    // buckets_per_step (2000ms step / 1000ms tumbling = 2)
            0,    // start_ms
            6000, // end_ms
            2000, // step_ms
            10,   // expected_bucket_count
        );

        // First sample at 4000 (rounded up from 3000), second at 6000
        assert_eq!(results.len(), 2, "Should produce 2 samples");
        assert_eq!(results[0].0, 4000, "First sample at step boundary 4000");
        assert_eq!(results[1].0, 6000, "Second sample at 6000");
    }

    #[test]
    fn test_sliding_window_full_data_starts_at_query_start() {
        // All data present - should behave same as before (start at start_ms)
        // lookback=5, step=1, query [1000, 3000] = 3 samples
        // Expected buckets: 7, Actual: 7 (no missing data)

        let buckets: Vec<Box<dyn AggregateCore>> = (0..7)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window_with_alignment(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms
            1000, // step_ms
            7,    // expected_bucket_count (matches actual - no missing data)
        );

        assert_eq!(results.len(), 3, "Should produce 3 samples");
        assert_eq!(results[0].0, 1000, "First sample at query start");
        assert_eq!(results[1].0, 2000);
        assert_eq!(results[2].0, 3000);
    }

    #[test]
    fn test_sliding_window_insufficient_data_for_any_window_returns_empty() {
        // lookback=5 but only 3 buckets - can't form even one window
        let buckets: Vec<Box<dyn AggregateCore>> = (0..3)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window_with_alignment(
            buckets, 5, // lookback_bucket_count (need 5, have 3)
            1, 1000, 5000, 1000, 9,
        );

        assert_eq!(
            results.len(),
            0,
            "No samples when insufficient data for any window"
        );
    }

    // ============================================================================
    // Tests for timestamp-based lookup implementation (handles gaps in data)
    // ============================================================================

    /// Simulates the timestamp-based lookup approach from execute_range_query_pipeline.
    /// This is the new implementation that handles gaps in data correctly.
    ///
    /// # Arguments
    /// * `timestamped_buckets` - Vec of (bucket_start_timestamp, bucket)
    /// * `lookback_bucket_count` - Number of buckets in each window
    /// * `tumbling_window_ms` - Duration of each tumbling window bucket
    /// * `start_ms` - Query start time
    /// * `end_ms` - Query end time
    /// * `step_ms` - Step between output samples
    ///
    /// # Returns
    /// Vec of (timestamp, merged_value, max_bucket_id_in_window)
    fn simulate_timestamp_based_lookup(
        timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)>,
        lookback_bucket_count: usize,
        tumbling_window_ms: u64,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Vec<(u64, f64, u64)> {
        use crate::engines::window_merger::WindowMerger;
        use std::collections::HashMap;

        let mut results = Vec::new();

        // Build lookup: bucket_start_timestamp -> bucket for O(1) access
        let bucket_map: HashMap<u64, &Box<dyn AggregateCore>> = timestamped_buckets
            .iter()
            .map(|(start, bucket)| (*start, bucket))
            .collect();

        let lookback_ms = (lookback_bucket_count as u64) * tumbling_window_ms;

        // Iterate by OUTPUT timestamp, not by bucket index
        let mut current_time = start_ms;
        while current_time <= end_ms {
            // Window covers [current_time - lookback_ms, current_time)
            let window_start = current_time.saturating_sub(lookback_ms);

            // Collect all AVAILABLE buckets in this window (skip missing ones)
            let mut window_buckets: Vec<Box<dyn AggregateCore>> = Vec::new();

            let mut t = window_start;
            while t < current_time {
                if let Some(bucket) = bucket_map.get(&t) {
                    window_buckets.push((*bucket).clone_boxed_core());
                }
                t += tumbling_window_ms;
            }

            if !window_buckets.is_empty() {
                // Merge available buckets
                let mut merger = NaiveMerger::new();
                merger.initialize(window_buckets);

                if let Ok(merged) = merger.get_merged() {
                    if let Some(mock) = merged.as_any().downcast_ref::<MockBucketAccumulator>() {
                        results.push((current_time, mock.value, mock.bucket_id));
                    }
                }
            }
            // If no buckets available, skip this sample (no entry in results)

            current_time += step_ms;
        }

        results
    }

    #[test]
    fn test_timestamp_lookup_missing_data_at_start() {
        // Scenario: Query range [1000, 5000] with step=1000, lookback=3 buckets
        // Tumbling window = 1000ms
        // Expected buckets for full window coverage starting at t=1000:
        //   - t=1000 needs buckets at -2000, -1000, 0 (before query range)
        // But data only exists at t=3000, 4000, 5000
        //
        // Sample at t=1000: window [1000-3000, 1000) = [-2000, 1000) -> no buckets -> skip
        // Sample at t=2000: window [2000-3000, 2000) = [-1000, 2000) -> no buckets -> skip
        // Sample at t=3000: window [3000-3000, 3000) = [0, 3000) -> no buckets -> skip
        // Sample at t=4000: window [4000-3000, 4000) = [1000, 4000) -> bucket at 3000 -> emit
        // Sample at t=5000: window [5000-3000, 5000) = [2000, 5000) -> buckets at 3000, 4000 -> emit

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (3000, Box::new(MockBucketAccumulator::new(3, 10.0))),
            (4000, Box::new(MockBucketAccumulator::new(4, 10.0))),
            (5000, Box::new(MockBucketAccumulator::new(5, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            1000, // start_ms
            5000, // end_ms
            1000, // step_ms
        );

        // Should skip samples at 1000, 2000, 3000 (no data in window)
        // Should emit samples at 4000 (partial data) and 5000 (partial data)
        assert_eq!(
            results.len(),
            2,
            "Should produce 2 samples (skipping early ones with no data)"
        );
        assert_eq!(results[0].0, 4000, "First sample at t=4000");
        assert_eq!(results[0].1, 10.0, "Value at t=4000 (1 bucket)");
        assert_eq!(results[1].0, 5000, "Second sample at t=5000");
        assert_eq!(results[1].1, 20.0, "Value at t=5000 (2 buckets merged)");
    }

    #[test]
    fn test_timestamp_lookup_missing_data_in_middle() {
        // Scenario: Buckets at t=1000, 2000, 4000, 5000 (missing t=3000)
        // Query range [4000, 6000], step=1000, lookback=3 buckets
        // Tumbling window = 1000ms
        //
        // Sample at t=4000: window [1000, 4000) -> buckets at 1000, 2000 (missing 3000) -> 2 buckets
        // Sample at t=5000: window [2000, 5000) -> buckets at 2000, 4000 (missing 3000) -> 2 buckets
        // Sample at t=6000: window [3000, 6000) -> buckets at 4000, 5000 (missing 3000) -> 2 buckets

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (1000, Box::new(MockBucketAccumulator::new(1, 10.0))),
            (2000, Box::new(MockBucketAccumulator::new(2, 10.0))),
            // Missing bucket at 3000
            (4000, Box::new(MockBucketAccumulator::new(4, 10.0))),
            (5000, Box::new(MockBucketAccumulator::new(5, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            4000, // start_ms
            6000, // end_ms
            1000, // step_ms
        );

        // All samples should be emitted with partial data (missing bucket is skipped)
        assert_eq!(
            results.len(),
            3,
            "Should produce 3 samples with partial data"
        );

        // t=4000: window [1000, 4000) contains buckets 1000, 2000 -> value=20, max_id=2
        assert_eq!(results[0].0, 4000);
        assert_eq!(results[0].1, 20.0, "2 buckets merged");
        assert_eq!(results[0].2, 2, "max bucket_id = 2");

        // t=5000: window [2000, 5000) contains buckets 2000, 4000 -> value=20, max_id=4
        assert_eq!(results[1].0, 5000);
        assert_eq!(results[1].1, 20.0, "2 buckets merged");
        assert_eq!(results[1].2, 4, "max bucket_id = 4");

        // t=6000: window [3000, 6000) contains buckets 4000, 5000 -> value=20, max_id=5
        assert_eq!(results[2].0, 6000);
        assert_eq!(results[2].1, 20.0, "2 buckets merged");
        assert_eq!(results[2].2, 5, "max bucket_id = 5");
    }

    #[test]
    fn test_timestamp_lookup_all_data_missing_for_window() {
        // Scenario: Query window where no buckets exist at all
        // Buckets at t=10000, 11000, 12000
        // Query range [1000, 3000], step=1000, lookback=3 buckets
        // All windows have no data -> should skip all samples

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (10000, Box::new(MockBucketAccumulator::new(10, 10.0))),
            (11000, Box::new(MockBucketAccumulator::new(11, 10.0))),
            (12000, Box::new(MockBucketAccumulator::new(12, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            1000, // start_ms
            3000, // end_ms
            1000, // step_ms
        );

        assert_eq!(
            results.len(),
            0,
            "Should produce 0 samples when all windows have no data"
        );
    }

    #[test]
    fn test_timestamp_lookup_full_data_matches_expected() {
        // Scenario: Full data available, should behave like contiguous case
        // Buckets at t=0, 1000, 2000, 3000, 4000
        // Query range [3000, 5000], step=1000, lookback=3 buckets
        //
        // Sample at t=3000: window [0, 3000) -> buckets 0, 1000, 2000 -> value=30
        // Sample at t=4000: window [1000, 4000) -> buckets 1000, 2000, 3000 -> value=30
        // Sample at t=5000: window [2000, 5000) -> buckets 2000, 3000, 4000 -> value=30

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (0, Box::new(MockBucketAccumulator::new(0, 10.0))),
            (1000, Box::new(MockBucketAccumulator::new(1, 10.0))),
            (2000, Box::new(MockBucketAccumulator::new(2, 10.0))),
            (3000, Box::new(MockBucketAccumulator::new(3, 10.0))),
            (4000, Box::new(MockBucketAccumulator::new(4, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            3000, // start_ms
            5000, // end_ms
            1000, // step_ms
        );

        assert_eq!(results.len(), 3, "Should produce 3 samples");

        assert_eq!(results[0], (3000, 30.0, 2), "t=3000: buckets 0,1,2");
        assert_eq!(results[1], (4000, 30.0, 3), "t=4000: buckets 1,2,3");
        assert_eq!(results[2], (5000, 30.0, 4), "t=5000: buckets 2,3,4");
    }

    #[test]
    fn test_timestamp_lookup_sparse_data() {
        // Scenario: Very sparse data - only every 3rd bucket exists
        // Buckets at t=0, 3000, 6000, 9000
        // Query range [3000, 9000], step=3000, lookback=3 buckets (3000ms)
        //
        // Sample at t=3000: window [0, 3000) -> bucket 0 -> value=10
        // Sample at t=6000: window [3000, 6000) -> bucket 3000 -> value=10
        // Sample at t=9000: window [6000, 9000) -> bucket 6000 -> value=10

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (0, Box::new(MockBucketAccumulator::new(0, 10.0))),
            (3000, Box::new(MockBucketAccumulator::new(3, 10.0))),
            (6000, Box::new(MockBucketAccumulator::new(6, 10.0))),
            (9000, Box::new(MockBucketAccumulator::new(9, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            3000, // start_ms
            9000, // end_ms
            3000, // step_ms
        );

        assert_eq!(
            results.len(),
            3,
            "Should produce 3 samples with sparse data"
        );

        // Each window only has 1 bucket because data is sparse
        assert_eq!(
            results[0],
            (3000, 10.0, 0),
            "t=3000: only bucket 0 in window"
        );
        assert_eq!(
            results[1],
            (6000, 10.0, 3),
            "t=6000: only bucket 3 in window"
        );
        assert_eq!(
            results[2],
            (9000, 10.0, 6),
            "t=9000: only bucket 6 in window"
        );
    }

    #[test]
    fn test_timestamp_lookup_missing_data_at_end() {
        // Scenario: Data missing at end of query range
        // Buckets at t=0, 1000, 2000
        // Query range [3000, 6000], step=1000, lookback=3 buckets
        //
        // Sample at t=3000: window [0, 3000) -> buckets 0, 1000, 2000 -> full data
        // Sample at t=4000: window [1000, 4000) -> buckets 1000, 2000 -> partial (missing 3000)
        // Sample at t=5000: window [2000, 5000) -> bucket 2000 -> partial
        // Sample at t=6000: window [3000, 6000) -> no buckets -> skip

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (0, Box::new(MockBucketAccumulator::new(0, 10.0))),
            (1000, Box::new(MockBucketAccumulator::new(1, 10.0))),
            (2000, Box::new(MockBucketAccumulator::new(2, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            3000, // start_ms
            6000, // end_ms
            1000, // step_ms
        );

        assert_eq!(
            results.len(),
            3,
            "Should produce 3 samples (last one skipped)"
        );

        assert_eq!(results[0], (3000, 30.0, 2), "t=3000: full window");
        assert_eq!(
            results[1],
            (4000, 20.0, 2),
            "t=4000: partial window (2 buckets)"
        );
        assert_eq!(
            results[2],
            (5000, 10.0, 2),
            "t=5000: partial window (1 bucket)"
        );
        // t=6000 is skipped because no data
    }
}

#[cfg(test)]
mod sketch_query_tests {
    // use crate::data_model::{CleanupPolicy, InferenceConfig, QueryLanguage, StreamingConfig};
    // use crate::engines::simple_engine::SimpleEngine;
    // use crate::stores::promsketch_store::PromSketchStore;
    // use crate::stores::{Store, TimestampedBucketsMap};
    // use std::collections::HashMap;
    // use std::sync::Arc;

    // /// Minimal no-op store — sketch queries bypass the store entirely
    // struct NoOpStore;

    // impl Store for NoOpStore {
    //     fn query_precomputed_output(
    //         &self,
    //         _: &str,
    //         _: u64,
    //         _: u64,
    //         _: u64,
    //     ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn query_precomputed_output_exact(
    //         &self,
    //         _: &str,
    //         _: u64,
    //         _: u64,
    //         _: u64,
    //     ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn insert_precomputed_output(
    //         &self,
    //         _: crate::data_model::PrecomputedOutput,
    //         _: Box<dyn crate::data_model::AggregateCore>,
    //     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn insert_precomputed_output_batch(
    //         &self,
    //         _: Vec<(
    //             crate::data_model::PrecomputedOutput,
    //             Box<dyn crate::data_model::AggregateCore>,
    //         )>,
    //     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn get_earliest_timestamp_per_aggregation_id(
    //         &self,
    //     ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
    //         Ok(HashMap::new())
    //     }
    //     fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         Ok(())
    //     }
    // }

    // /// Helper: create an engine with a populated PromSketchStore.
    // /// Inserts data points 1..=100 into a series with labels = `series_key`.
    // fn engine_with_sketch_data(series_key: &str) -> SimpleEngine {
    //     let ps = Arc::new(PromSketchStore::with_default_config());
    //     ps.ensure_all_sketches(series_key).unwrap();
    //     for i in 1..=100u64 {
    //         ps.sketch_insert(series_key, i, i as f64).unwrap();
    //     }

    //     let inference_config =
    //         InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());

    //     SimpleEngine::new(
    //         Arc::new(NoOpStore),
    //         Some(ps),
    //         inference_config,
    //         streaming_config,
    //         15,
    //         QueryLanguage::promql,
    //     )
    // }

    // // ---- Instant query tests ----

    // #[test]
    // fn test_sketch_instant_entropy_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Query at time 0.1s (= 100ms) with a 100ms range
    //     let result = engine.handle_query_promql("entropy_over_time(mymetric[100s])".into(), 0.1);
    //     assert!(result.is_some(), "entropy_over_time should return a result");
    //     let (labels, qr) = result.unwrap();
    //     assert!(!labels.labels.is_empty());
    //     if let crate::engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty(), "should have at least one result");
    //         let val = iv.values[0].value;
    //         assert!(val >= 0.0, "entropy should be non-negative, got {}", val);
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_quantile_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     let result =
    //         engine.handle_query_promql("quantile_over_time(0.5, mymetric[100s])".into(), 0.1);
    //     assert!(
    //         result.is_some(),
    //         "quantile_over_time should return a result"
    //     );
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty());
    //         let val = iv.values[0].value;
    //         // Median of 1..100 should be roughly 50
    //         assert!(
    //             val > 20.0 && val < 80.0,
    //             "median should be roughly 50, got {}",
    //             val
    //         );
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_avg_over_time() {
    //     let engine = engine_with_sketch_data("cpu");
    //     let result = engine.handle_query_promql("avg_over_time(cpu[100s])".into(), 0.1);
    //     assert!(result.is_some(), "avg_over_time should return a result");
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty());
    //         let val = iv.values[0].value;
    //         // avg of 1..100 = 50.5
    //         assert!(val > 30.0 && val < 70.0, "avg should be ~50.5, got {}", val);
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_without_store() {
    //     // Engine with promsketch_store = None
    //     let inference_config =
    //         InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());
    //     let engine = SimpleEngine::new(
    //         Arc::new(NoOpStore),
    //         None,
    //         inference_config,
    //         streaming_config,
    //         15,
    //         QueryLanguage::promql,
    //     );
    //     // Sketch function should fall through (return None) without panicking
    //     let result = engine.handle_sketch_query_promql("entropy_over_time(metric[5m])", 100.0);
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_for_non_sketch_function() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // "rate" is not sketch-backed, so should return None from sketch path
    //     let result = engine.handle_sketch_query_promql("rate(mymetric[100s])", 0.1);
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_for_missing_series() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Query a metric that doesn't exist in the sketch store
    //     let result = engine.handle_sketch_query_promql("entropy_over_time(nonexistent[100s])", 0.1);
    //     assert!(result.is_none());
    // }

    // ---- Range query tests ----

    // #[test]
    // fn test_sketch_range_entropy_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Range query: start=0.01, end=0.1 (10ms to 100ms), step=0.01 (10ms)
    //     // with a 50ms window [50s range]
    //     let result = engine.handle_range_query_promql(
    //         "entropy_over_time(mymetric[50s])".into(),
    //         0.01,
    //         0.1,
    //         0.01,
    //     );
    //     assert!(
    //         result.is_some(),
    //         "sketch range query should return a result"
    //     );
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::engines::query_result::QueryResult::Matrix(rv) = qr {
    //         assert!(!rv.values.is_empty(), "should have at least one series");
    //         let samples = &rv.values[0].samples;
    //         assert!(
    //             samples.len() > 1,
    //             "range query should produce multiple samples, got {}",
    //             samples.len()
    //         );
    //         for sample in samples {
    //             assert!(
    //                 sample.value >= 0.0,
    //                 "entropy should be non-negative, got {}",
    //                 sample.value
    //             );
    //         }
    //     } else {
    //         panic!("expected Matrix result");
    //     }
    // }

    // #[test]
    // fn test_sketch_range_returns_none_without_store() {
    //     let inference_config =
    //         InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());
    //     let engine = SimpleEngine::new(
    //         Arc::new(NoOpStore),
    //         None,
    //         inference_config,
    //         streaming_config,
    //         15,
    //         QueryLanguage::promql,
    //     );
    //     let result = engine.handle_sketch_range_query_promql(
    //         "entropy_over_time(metric[5m])",
    //         0.0,
    //         100.0,
    //         10.0,
    //     );
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_range_returns_none_for_non_sketch_function() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     let result =
    //         engine.handle_sketch_range_query_promql("rate(mymetric[100s])", 0.01, 0.1, 0.01);
    //     assert!(result.is_none());
    // }
}

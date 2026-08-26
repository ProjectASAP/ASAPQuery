mod elastic;
mod promql;
mod sql;

use crate::data_model::{
    AggregationIdInfo, InferenceConfig, KeyByLabelValues, QueryConfig, QueryLanguage,
    StreamingConfig,
};
use crate::engines::query_result::{InstantVectorElement, QueryResult};
// use crate::stores::promsketch_store::{
//     self, is_usampling_function, metrics as ps_metrics, PromSketchStore,
// };
use crate::stores::{Store, TimestampedBucketsMap};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::{debug, warn};

use crate::precompute_operators::AccumulatorError;
use crate::AggregateCore;

use asap_types::enums::WindowType;
use promql_utilities::ast_matching::{PromQLPattern, PromQLPatternBuilder};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::get_is_collapsable;
use promql_utilities::query_logics::enums::{
    AggregationOperator, AggregationType, PromQLFunction, Statistic,
};
use serde_json::Value;

// Type alias for merged outputs (single aggregate per key after merging)
type MergedOutputsMap = HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>;

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
    /// Milliseconds since epoch.
    pub start_timestamp: u64,
    /// Milliseconds since epoch.
    pub end_timestamp: u64,
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
    /// Milliseconds since epoch.
    pub start_timestamp: u64,
    /// Milliseconds since epoch.
    pub end_timestamp: u64,
}

/// Complete execution context for a query
#[derive(Debug, Clone)]
pub struct QueryExecutionContext {
    pub metric: String,
    pub metadata: QueryMetadata,
    pub store_plan: StoreQueryPlan,
    pub agg_info: AggregationIdInfo,
    /// The value aggregation's WindowType -- Sliding fetches/merges a single
    /// already-complete window; Tumbling sums the disjoint buckets in range.
    pub value_window_type: WindowType,
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
    /// The value aggregation's `WindowType`. Picks how the per-step loop
    /// composes a step's window from `bucket_map`: Sliding buckets are each
    /// already a complete `window_size_ms`-wide merged window (see
    /// `worker.rs::merge_panes_for_window`), so a step takes exactly the one
    /// bucket at `current_time - lookback_ms` (`lookback_ms` ==
    /// `window_size_ms` here); Tumbling buckets are genuinely disjoint, so a
    /// step sums every bucket `scan_window` finds across the lookback span
    /// (#608).
    pub window_type: WindowType,
    /// The value aggregation's actual `window_size_ms`, independent of
    /// `tumbling_window_ms` (which is `bucket_step_ms`, not the window
    /// size). Used only to assert `lookback_ms == window_size_ms` for
    /// Sliding before `single_window` relies on that equality (#608 review).
    pub window_size_ms: u64,
    /// Same as `window_type`, for the keys aggregation -- `None` when
    /// there's no separate `keys_query`. Can legitimately differ from
    /// `window_type` (e.g. a Sliding SetAggregator keys aggregation paired
    /// with a Tumbling value aggregation, or vice versa).
    pub keys_window_type: Option<WindowType>,
    /// Same as `window_size_ms`, for the keys aggregation. `None` under the
    /// same condition as `keys_window_type`.
    pub keys_window_size_ms: Option<u64>,
    /// Per-step lookback for the keys aggregation (#583): `keys_query.end -
    /// keys_query.start` from the instant window `create_keys_query_params`
    /// computed before widening. `None` when there's no separate
    /// `keys_query`. Reused, unmodified, as the per-step
    /// `current_time.saturating_sub(keys_lookback_ms)` window start for
    /// every output step -- this single value is what makes `SetAggregator`
    /// a normal sliding window and `DeltaSetAggregator` always replay from
    /// `0`, with no `AggregationType` branching needed here.
    pub keys_lookback_ms: Option<u64>,
    /// Bucket width for the keys aggregation, separate from
    /// `tumbling_window_ms` (which is the *value* aggregation's width) --
    /// the two can legitimately differ.
    pub keys_tumbling_window_ms: Option<u64>,
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
    /// Updated at runtime via update_inference_config(). RwLock provides interior
    /// mutability since SimpleEngine is shared behind Arc<SimpleEngine>.
    inference_config: RwLock<InferenceConfig>,
    /// Updated at runtime via update_streaming_config(). Readers briefly lock to
    /// clone the Arc pointer, then use without holding the lock.
    streaming_config: RwLock<Arc<StreamingConfig>>,
    data_ingestion_interval_ms: u64,
    controller_patterns: Vec<PromQLPattern>,
    query_language: QueryLanguage,
}

impl SimpleEngine {
    pub fn new(
        store: Arc<dyn Store>,
        // promsketch_store: Option<Arc<PromSketchStore>>,
        inference_config: InferenceConfig,
        streaming_config: Arc<StreamingConfig>,
        data_ingestion_interval_ms: u64,
        query_language: QueryLanguage,
    ) -> Self {
        // Create temporal pattern blocks
        let mut temporal_pattern_blocks = HashMap::new();
        temporal_pattern_blocks.insert(
            "quantile".to_string(),
            PromQLPatternBuilder::function(
                vec![PromQLFunction::QuantileOverTime.as_str()],
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
        let spatial_ops_all: Vec<&str> = [
            AggregationOperator::Sum,
            AggregationOperator::Count,
            AggregationOperator::Avg,
            AggregationOperator::Quantile,
            AggregationOperator::Min,
            AggregationOperator::Max,
            AggregationOperator::Topk,
        ]
        .map(AggregationOperator::as_str)
        .to_vec();
        spatial_pattern_blocks.insert(
            "generic".to_string(),
            PromQLPatternBuilder::aggregation(
                spatial_ops_all,
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

        // A spatial aggregation wrapping a temporal function only collapses into a
        // single equivalent statistic for specific (function, op) pairs (see
        // `get_is_collapsable`) — e.g. `sum(min_over_time(x[5m]))` cannot be served
        // by a single precomputed statistic the way `sum(sum_over_time(x[5m]))` can.
        // Building one narrow pattern per collapsable pair (rather than a broad
        // any-op-wrapping-any-function pattern) means a non-collapsable combination
        // never structurally matches at all, instead of matching and then silently
        // dropping the outer aggregation (see #508).
        let one_temporal_one_spatial_collapsable_patterns: Vec<PromQLPattern> = [
            PromQLFunction::Rate,
            PromQLFunction::Increase,
            PromQLFunction::SumOverTime,
            PromQLFunction::CountOverTime,
            PromQLFunction::AvgOverTime,
            PromQLFunction::MinOverTime,
            PromQLFunction::MaxOverTime,
            PromQLFunction::QuantileOverTime,
        ]
        .into_iter()
        .flat_map(|func| {
            [
                AggregationOperator::Sum,
                AggregationOperator::Count,
                AggregationOperator::Avg,
                AggregationOperator::Quantile,
                AggregationOperator::Min,
                AggregationOperator::Max,
            ]
            .into_iter()
            .filter_map(move |op| {
                if !get_is_collapsable(func, op) {
                    return None;
                }
                let range_vector = PromQLPatternBuilder::matrix_selector(
                    PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                    None,
                    Some("range_vector"),
                );
                let function_pattern = PromQLPatternBuilder::function(
                    vec![func.as_str()],
                    vec![range_vector],
                    Some("function"),
                    Some("function_args"),
                );
                let pattern = PromQLPatternBuilder::aggregation(
                    vec![op.as_str()],
                    function_pattern,
                    None,
                    None,
                    None,
                    Some("aggregation"),
                );
                Some(PromQLPattern::new(pattern))
            })
        })
        .collect();

        // Create controller patterns: tried in order until one matches.
        let mut controller_patterns = vec![
            temporal_pattern("quantile", &temporal_pattern_blocks),
            temporal_pattern("generic", &temporal_pattern_blocks),
            spatial_pattern("generic", &spatial_pattern_blocks),
        ];
        controller_patterns.extend(one_temporal_one_spatial_collapsable_patterns);

        Self {
            store,
            // promsketch_store,
            inference_config: RwLock::new(inference_config),
            streaming_config: RwLock::new(streaming_config),
            data_ingestion_interval_ms,
            controller_patterns,
            query_language,
        }
    }

    /// Replace the inference config at runtime. Called by the applier task after
    /// the planner fires.
    ///
    /// NOTE: streaming_config and inference_config are applied to their respective
    /// components independently (not atomically). A brief window may exist where
    /// the precompute engine has a new streaming_config but this engine still uses
    /// the old inference_config, causing query misses that fall back to Prometheus.
    pub fn update_inference_config(&self, new_config: InferenceConfig) {
        *self.inference_config.write().unwrap() = new_config;
    }

    /// Replace the streaming config at runtime. Called by the applier task after
    /// the planner fires.
    pub fn update_streaming_config(&self, new_config: Arc<StreamingConfig>) {
        *self.streaming_config.write().unwrap() = new_config;
    }

    /// Convert query timestamp (seconds) to data timestamp (milliseconds)
    pub fn convert_query_time_to_data_time(query_time: f64) -> u64 {
        (query_time * 1000.0) as u64
    }

    /// Finds the query configuration for a given query string
    fn find_query_config(&self, query: &str) -> Option<QueryConfig> {
        self.inference_config
            .read()
            .unwrap()
            .query_configs
            .iter()
            .find(|config| config.query == query)
            .cloned()
    }

    /// Creates query parameters for separate keys query
    fn create_keys_query_params(
        &self,
        metric: &str,
        end_timestamp: u64,
        agg_info: &AggregationIdInfo,
    ) -> Result<StoreQueryParams, String> {
        let (start_timestamp, end_timestamp) = match agg_info.aggregation_type_for_key {
            AggregationType::DeltaSetAggregator => {
                // All keys from beginning of time
                (0, end_timestamp)
            }
            AggregationType::SetAggregator => {
                // Latest window only
                let window_size = self
                    .streaming_config
                    .read()
                    .unwrap()
                    .get_aggregation_config(agg_info.aggregation_id_for_key)
                    .map(|config| config.window_size_ms)
                    .ok_or_else(|| {
                        format!(
                            "Failed to get window size for aggregation {}",
                            agg_info.aggregation_id_for_key
                        )
                    })?;
                (end_timestamp - window_size, end_timestamp)
            }
            other => {
                return Err(format!("Unsupported key aggregation type: {other:?}"));
            }
        };

        // Keys always fetch via the window-grid walk (execute_store_query),
        // never a single exact-window lookup -- this is an explicit,
        // permanent choice, not a WindowType derivation: a keys query
        // conceptually always needs to see the key's own bucket(s), not "the
        // one window ending now."
        Ok(StoreQueryParams {
            metric: metric.to_string(),
            aggregation_id: agg_info.aggregation_id_for_key,
            start_timestamp,
            end_timestamp,
        })
    }

    /// Creates a plan for querying the store based on aggregation configuration.
    /// Also derives `do_merge`: true when the requested time range spans more
    /// than one stored window, i.e. `range_ms > window_size_ms`.
    ///
    /// Returns the value aggregation's `WindowType` alongside the plan --
    /// callers need it again later (e.g. to pick merge semantics) and it's
    /// cheaper to hand back what was already looked up here than to
    /// re-fetch the aggregation config.
    fn create_store_query_plan(
        &self,
        metric: &str,
        timestamps: &QueryTimestamps,
        agg_info: &AggregationIdInfo,
    ) -> Result<(StoreQueryPlan, bool, WindowType), String> {
        let sc = self.streaming_config.read().unwrap().clone();
        // Get aggregation config for value to determine window type
        let aggregation_config_for_value = sc
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .ok_or_else(|| {
                format!(
                    "Aggregation config not found for aggregation_id: {}",
                    agg_info.aggregation_id_for_value
                )
            })?;

        let window_type = aggregation_config_for_value.window_type;
        let range_ms = timestamps.end_timestamp - timestamps.start_timestamp;
        let do_merge = range_ms > aggregation_config_for_value.window_size_ms;

        // Determine start/end for values query based on window type. For
        // Sliding, narrow to exactly the one window ending "now" --
        // execute_store_query's window-grid walk degenerates to a single
        // exact lookup when given a range exactly one window wide, so this
        // narrowing (not a separate flag) is what makes it an "exact" fetch.
        let (values_start, values_end) = if window_type == WindowType::Sliding {
            let exact_start =
                timestamps.end_timestamp - aggregation_config_for_value.window_size_ms;
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
        };

        // Determine if we need a separate keys query
        let keys_query = if agg_info.aggregation_id_for_key != agg_info.aggregation_id_for_value {
            Some(self.create_keys_query_params(metric, timestamps.end_timestamp, agg_info)?)
        } else {
            None
        };

        Ok((
            StoreQueryPlan {
                values_query,
                keys_query,
            },
            do_merge,
            window_type,
        ))
    }

    /// The bucket-map grid width for scanning an aggregation's stored
    /// buckets: `slide_interval_ms`, not `window_size_ms`.
    /// `precompute_engine/window_manager.rs` persists buckets on the
    /// `slide_interval_ms` grid unconditionally (its `panes_for_window`
    /// steps by `slide_interval_ms`, regardless of `WindowType`) — for
    /// Tumbling aggregations the two are equal by construction, so this is
    /// a no-op there, but for Sliding aggregations with
    /// `slide_interval_ms < window_size_ms`, stepping by `window_size_ms`
    /// walks straight past real buckets and silently drops them (#600).
    /// Mirrors `WindowManager::new`'s `slide_interval_ms == 0` fallback so a
    /// config that leaves the field unset is still treated as Tumbling.
    fn bucket_step_ms(config: &asap_types::AggregationConfig) -> u64 {
        if config.slide_interval_ms == 0 {
            config.window_size_ms
        } else {
            config.slide_interval_ms
        }
    }

    /// Walks the aggregation's window grid (`bucket_step_ms` apart, each
    /// window `window_size_ms` wide, per `WindowManager::window_start_for`)
    /// and looks up every grid position in `[start_timestamp, end_timestamp)`
    /// with an exact match, merging the sparse per-window results. A range
    /// exactly one window wide degenerates to a single exact lookup -- an
    /// instant Sliding-window fetch gets "the one window ending now" this
    /// way, by being narrowed to one window's width before calling
    /// (`create_store_query_plan`), not via a separate exact/scan flag.
    fn scan_windows_via_exact(
        &self,
        params: &StoreQueryParams,
    ) -> Result<TimestampedBucketsMap, String> {
        let sc = self.streaming_config.read().unwrap().clone();
        let config = sc
            .get_aggregation_config(params.aggregation_id)
            .ok_or_else(|| {
                format!(
                    "Aggregation config not found for aggregation_id: {}",
                    params.aggregation_id
                )
            })?;
        // DeltaSetAggregator keys queries span [0, end_timestamp] --
        // "all keys ever seen" (see create_keys_query_params) -- a nominal
        // range the grid-walk below can't cover cheaply. The tolerant scan
        // stays fast here regardless of nominal range width (it short-
        // circuits per-epoch via time_bounds() and binary-searches within
        // surviving epochs), so keep using it for this one case.
        if config.aggregation_type == AggregationType::DeltaSetAggregator {
            return self
                .store
                .query_precomputed_output(
                    &params.metric,
                    params.aggregation_id,
                    params.start_timestamp,
                    params.end_timestamp,
                )
                .map_err(|e| {
                    format!(
                        "Error querying store for metric {}, agg {}, range [{}, {}]: {}",
                        params.metric,
                        params.aggregation_id,
                        params.start_timestamp,
                        params.end_timestamp,
                        e
                    )
                });
        }

        let window_size_ms = config.window_size_ms;
        let step_ms = Self::bucket_step_ms(config);

        if window_size_ms == 0 || step_ms == 0 || params.start_timestamp > params.end_timestamp {
            return Ok(HashMap::new());
        }

        let mut windows: Vec<crate::stores::TimestampRange> = Vec::new();
        let mut window_start = params.start_timestamp.div_ceil(step_ms) * step_ms;
        while window_start + window_size_ms <= params.end_timestamp {
            windows.push((window_start, window_start + window_size_ms));
            window_start += step_ms;
        }

        // #609: one batched store call for the whole grid instead of one
        // query_precomputed_output_exact call per window.
        self.store
            .query_precomputed_output_exact_batch(&params.metric, params.aggregation_id, &windows)
            .map_err(|e| {
                format!(
                    "Error querying store for metric {}, agg {}, {} windows in [{}, {}]: {}",
                    params.metric,
                    params.aggregation_id,
                    windows.len(),
                    params.start_timestamp,
                    params.end_timestamp,
                    e
                )
            })
    }

    /// Executes a single store query based on parameters
    fn execute_store_query(
        &self,
        params: &StoreQueryParams,
    ) -> Result<TimestampedBucketsMap, String> {
        debug!(
            "Querying store: metric={}, agg_id={}, range=[{}, {}]",
            params.metric, params.aggregation_id, params.start_timestamp, params.end_timestamp,
        );

        let store_query_start_time = Instant::now();
        let result = self.scan_windows_via_exact(params);
        if let Ok(ref outputs) = result {
            let store_query_duration = store_query_start_time.elapsed();
            debug!(
                "Window-grid query took: {:.2}ms, found {} unique keys",
                store_query_duration.as_secs_f64() * 1000.0,
                outputs.len()
            );
        }
        result
    }

    /// Executes the full store query plan and returns merged results
    fn execute_and_merge_store_queries(
        &self,
        plan: &StoreQueryPlan,
        do_merge: bool,
        agg_info: &AggregationIdInfo,
        value_window_type: WindowType,
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

        let merged_values = if value_window_type == WindowType::Sliding {
            // Sliding window: expected exactly 1 precompute per key today
            // (ponytail: hardcoded, #554 will make >1 legitimate — don't
            // block on it). The store can legitimately return more than
            // expected for one exact window; merge whatever came back
            // instead of arbitrarily keeping the first and dropping the
            // rest (see #567).
            const EXPECTED_BUCKETS_PER_KEY: usize = 1;
            debug!("Sliding window mode: merging {} keys", values_map.len());
            for timestamped_buckets in values_map.values() {
                if timestamped_buckets.is_empty() {
                    continue;
                }
                if timestamped_buckets.len() != EXPECTED_BUCKETS_PER_KEY {
                    warn!(
                        "Sliding window expected {} precompute(s) per key, found {}. Merging all.",
                        EXPECTED_BUCKETS_PER_KEY,
                        timestamped_buckets.len()
                    );
                }
            }
            // Sliding windows always merge (all buckets belong to one
            // logical window) — reuse the same merge path as Tumbling.
            self.merge_precomputed_outputs(&values_map, true, agg_info.aggregation_type_for_value)
        } else {
            // Tumbling window: merge needed
            debug!("Tumbling window mode: Merging {} outputs", values_map.len());
            self.merge_precomputed_outputs(
                &values_map,
                do_merge,
                agg_info.aggregation_type_for_value,
            )
        };

        let merge_duration = merge_start_time.elapsed();
        let did_merge = value_window_type == WindowType::Sliding
            || do_merge
            || agg_info.aggregation_type_for_value == AggregationType::DeltaSetAggregator;
        debug!(
            "[LATENCY] Precomputed output processing ({}): {:.2}ms, resulted in {} merged outputs",
            if did_merge { "merge" } else { "no merge" },
            merge_duration.as_secs_f64() * 1000.0,
            merged_values.len()
        );

        // Query and merge keys if needed
        let merged_keys = self.fetch_and_merge_keys(
            &plan.keys_query,
            agg_info.aggregation_type_for_key,
            do_merge,
        )?;

        Ok((merged_values, merged_keys))
    }

    /// Fetches and merges the keys side of a dual-population query plan, if
    /// present. Used by `execute_and_merge_store_queries` (instant) only —
    /// `execute_range_query_pipeline` (range) fetches keys raw via
    /// `execute_store_query` and merges them per output step instead (#583),
    /// since a single merged snapshot can't answer "what did the key set
    /// look like at an earlier timestamp."
    fn fetch_and_merge_keys(
        &self,
        keys_query: &Option<StoreQueryParams>,
        key_aggregation_type: AggregationType,
        do_merge: bool,
    ) -> Result<Option<MergedOutputsMap>, String> {
        let Some(keys_params) = keys_query else {
            return Ok(None);
        };

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
        let merged = self.merge_precomputed_outputs(&keys_map, do_merge, key_aggregation_type);
        debug!(
            "[LATENCY] Keys merge operation: {:.2}ms, resulted in {} merged outputs",
            keys_merge_start_time.elapsed().as_secs_f64() * 1000.0,
            merged.len()
        );
        Ok(Some(merged))
    }

    /// Collects all results based on whether keys are separate or not
    /// Resolves a value group's expansion keys and queries `statistic` for
    /// each, returning `(key, value)` pairs. Shared by both
    /// `collect_results_*` (instant, called once per group) and
    /// `execute_range_query_pipeline` (range, called once per group per
    /// output step) -- the single place "how do keys get resolved and
    /// queried" is decided, so the two pipelines can't drift apart on it
    /// again (#570, #582, #587, #597 were all instances of exactly that
    /// drift). See #581.
    ///
    /// - `value_precompute`: `None` means a dual-population group whose keys
    ///   accumulator has data but whose value accumulator doesn't -- skipped
    ///   with a warning, not a hard failure (#597).
    /// - `keys_precompute`: `Some` for dual-population groups (a separate
    ///   keys aggregation exists) -- its `get_keys()` supplies the expansion
    ///   keys, and `value_precompute`'s own `get_keys()` is never consulted
    ///   (#587). `get_keys()` returning `None` (e.g. a DeltaSetAggregator
    ///   invariant violation) skips the group with a warning, not a hard
    ///   failure. `None` for single-population groups -- `value_precompute`'s
    ///   own `get_keys()` takes priority if present (e.g. a top-k heap);
    ///   otherwise exactly one row is emitted using `fallback_key` verbatim
    ///   (the store-level group key, which may itself be `None` for a fully
    ///   ungrouped query).
    /// - A resolved key whose `query_precompute_for_statistic` call fails
    ///   (e.g. keys/value skew for a dual-population metric) is skipped with
    ///   a warning; the rest of the group's keys still return.
    fn resolve_and_query_group(
        &self,
        value_precompute: Option<&dyn AggregateCore>,
        keys_precompute: Option<&dyn AggregateCore>,
        fallback_key: &Option<KeyByLabelValues>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Vec<(Option<KeyByLabelValues>, f64)> {
        let Some(value_precompute) = value_precompute else {
            warn!(
                "Group {:?} has keys data but no value data -- skipping this group instead of \
                 failing the whole query (#597)",
                fallback_key
            );
            return Vec::new();
        };

        let resolved_keys: Vec<Option<KeyByLabelValues>> = match keys_precompute {
            Some(kp) => match kp.get_keys() {
                Some(keys) => keys.into_iter().map(Some).collect(),
                None => {
                    warn!(
                        "Group {:?}'s keys accumulator produced no resolvable key set -- \
                         skipping this group instead of failing the whole query",
                        fallback_key
                    );
                    return Vec::new();
                }
            },
            None => match value_precompute.get_keys() {
                Some(keys) => keys.into_iter().map(Some).collect(),
                None => vec![fallback_key.clone()],
            },
        };

        resolved_keys
            .into_iter()
            .filter_map(|key| {
                match self.query_precompute_for_statistic(
                    value_precompute,
                    statistic,
                    &key,
                    query_kwargs,
                ) {
                    Ok(value) => Some((key, value)),
                    Err(e) => {
                        warn!(
                            "Failed to query statistic for key {:?} in group {:?}: {} -- \
                             skipping this key instead of failing the whole query",
                            key, fallback_key, e
                        );
                        None
                    }
                }
            })
            .collect()
    }

    fn collect_all_results(
        &self,
        merged_values: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        merged_keys: Option<&HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        if let Some(keys_map) = merged_keys {
            // Separate keys and values
            self.collect_results_separate_keys(merged_values, keys_map, statistic, query_kwargs)
        } else {
            // Same aggregation for keys and values
            self.collect_results_same_aggregation(merged_values, statistic, query_kwargs)
        }
    }

    /// Executes the complete query pipeline: plan, execute, collect, and format.
    ///
    /// The two top-k flags are deliberately separate because the two engines
    /// need different halves of the behaviour:
    ///   * `enable_topk_limiting` — enumerate candidate keys from the sketch
    ///     heap and truncate to `k` during collection. Used by both PromQL and
    ///     SQL top-k (it's what makes the heap actually drive the result set).
    ///   * `enable_topk_formatting` — sort by value descending AND prepend the
    ///     metric name to each key's labels. This is PromQL `topk(...)` output
    ///     shape only; SQL returns bare `(group-by columns, value)` rows and
    ///     applies its own ORDER BY / LIMIT, so SQL leaves this `false`.
    pub fn execute_query_pipeline(
        &self,
        context: &QueryExecutionContext,
        enable_topk_limiting: bool,
        enable_topk_formatting: bool,
    ) -> Result<Vec<InstantVectorElement>, String> {
        // Step 1: Execute the query plan (already created in context.store_plan)
        let (merged_values, merged_keys) = self.execute_and_merge_store_queries(
            &context.store_plan,
            context.do_merge,
            &context.agg_info,
            context.value_window_type,
        )?;

        // Step 2: Collect results
        let unformatted_results_start_time = Instant::now();
        let unformatted_results = self.collect_all_results(
            &merged_values,
            merged_keys.as_ref(),
            &context.metadata.statistic_to_compute,
            &context.metadata.query_kwargs,
        )?;
        debug!(
            "[LATENCY] Unformatted results collection: {:.2}ms",
            unformatted_results_start_time.elapsed().as_secs_f64() * 1000.0
        );

        // Step 3: Format results
        let results_start_time = Instant::now();
        let mut results = self.format_final_results(
            unformatted_results,
            &context.metadata.statistic_to_compute,
            &context.metric,
            enable_topk_formatting,
        );
        // Truncate to k when limiting is active (heap may carry heap_size > k
        // candidates; the query only asked for the top k).
        if enable_topk_limiting {
            if let Some(k) = context
                .metadata
                .query_kwargs
                .get("k")
                .and_then(|s| s.parse::<usize>().ok())
            {
                results.truncate(k);
            }
        }
        debug!(
            "[LATENCY] Results collection: {}ms",
            results_start_time.elapsed().as_millis()
        );

        Ok(results)
    }

    /// Formats unformatted results into final InstantVectorElement format.
    ///
    /// For top-k queries the rows are always sorted by value descending (that's
    /// the semantics of top-k, and lets the caller truncate to `k` correctly
    /// regardless of HashMap iteration order). `enable_topk_formatting`
    /// additionally prepends the metric name to each key's labels — this is the
    /// PromQL `topk(...)` output shape only; SQL leaves it `false` so rows stay
    /// as bare `(group-by columns, value)`.
    fn format_final_results(
        &self,
        unformatted_results: HashMap<Option<KeyByLabelValues>, f64>,
        statistic: &Statistic,
        metric: &str,
        enable_topk_formatting: bool,
    ) -> Vec<InstantVectorElement> {
        let sorted_results: Vec<(Option<KeyByLabelValues>, f64)> = if *statistic == Statistic::Topk
        {
            // Sort by value descending for topk (independent of output formatting).
            let mut sorted: Vec<_> = unformatted_results.into_iter().collect();
            sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

            if enable_topk_formatting {
                // Prepend metric name to each key's label values (PromQL shape).
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
                sorted
            }
        } else {
            unformatted_results.into_iter().collect()
        };

        sorted_results
            .into_iter()
            .filter_map(|(key, value)| key.map(|k| InstantVectorElement::new(k, value)))
            .collect()
    }

    /// Parse a lowercase aggregation name into exactly one `Statistic`.
    ///
    /// Returns `None` (with a warning) if the name is not a recognised
    /// `AggregationOperator` or if it maps to a number of statistics other
    /// than one. Centralises the three previously-scattered copies of this
    /// logic, which had inconsistent error handling (silent empty vec, panic,
    /// and warn+return-None).
    fn parse_single_statistic(statistic_name: &str) -> Option<Statistic> {
        let stats = statistic_name
            .parse::<AggregationOperator>()
            .map(|o| o.to_statistics())
            .unwrap_or_else(|_| {
                warn!("Unsupported statistic name: '{}'", statistic_name);
                vec![]
            });
        if stats.len() != 1 {
            warn!(
                "Expected exactly one statistic for '{}', found {}",
                statistic_name,
                stats.len()
            );
            return None;
        }
        stats.into_iter().next()
    }

    fn get_aggregation_id_info(
        &self,
        query_config: &QueryConfig,
    ) -> Result<AggregationIdInfo, String> {
        let query_config_aggregations = &query_config.aggregations;

        if query_config_aggregations.is_empty() {
            return Err("Query config has no aggregations defined".to_string());
        }
        if query_config_aggregations.len() > 2 {
            return Err("Query config with > 2 aggregations is not supported".to_string());
        }

        let mut aggregation_id_for_key: Option<u64> = None;
        let mut aggregation_id_for_value: Option<u64> = None;
        let mut aggregation_type_for_key: Option<AggregationType> = None;
        let mut aggregation_type_for_value: Option<AggregationType> = None;

        let sc = self.streaming_config.read().unwrap().clone();
        if query_config_aggregations.len() == 2 {
            for aggregation in query_config_aggregations {
                let aggregation_type = sc
                    .get_aggregation_config(aggregation.aggregation_id)
                    .map(|config| config.aggregation_type)
                    .ok_or_else(|| {
                        format!(
                            "No streaming config for aggregation_id {}",
                            aggregation.aggregation_id
                        )
                    })?;

                if matches!(
                    aggregation_type,
                    AggregationType::DeltaSetAggregator | AggregationType::SetAggregator
                ) {
                    if aggregation_id_for_key.is_some() {
                        return Err(
                            "Query config has two key-type aggregations (expected at most one)"
                                .to_string(),
                        );
                    }
                    aggregation_id_for_key = Some(aggregation.aggregation_id);
                    aggregation_type_for_key = Some(aggregation_type);
                } else {
                    if aggregation_id_for_value.is_some() {
                        return Err(
                            "Query config has two value-type aggregations (expected at most one)"
                                .to_string(),
                        );
                    }
                    aggregation_id_for_value = Some(aggregation.aggregation_id);
                    aggregation_type_for_value = Some(aggregation_type);
                }
            }
        } else {
            // Single aggregation: key and value share the same aggregation
            let id = query_config_aggregations[0].aggregation_id;
            let agg_type = sc
                .get_aggregation_config(id)
                .map(|config| config.aggregation_type)
                .ok_or_else(|| format!("No streaming config for aggregation_id {id}"))?;
            aggregation_id_for_key = Some(id);
            aggregation_id_for_value = Some(id);
            aggregation_type_for_key = Some(agg_type);
            aggregation_type_for_value = Some(agg_type);
        }

        Ok(AggregationIdInfo {
            aggregation_id_for_key: aggregation_id_for_key
                .ok_or("aggregation_id_for_key was not set")?,
            aggregation_id_for_value: aggregation_id_for_value
                .ok_or("aggregation_id_for_value was not set")?,
            aggregation_type_for_key: aggregation_type_for_key
                .ok_or("aggregation_type_for_key was not set")?,
            aggregation_type_for_value: aggregation_type_for_value
                .ok_or("aggregation_type_for_value was not set")?,
        })
    }

    /// Execute the query pipeline for an already-built context.
    ///
    /// Shared by `handle_query_sql`, `handle_query_elastic`, and `handle_query_promql`.
    fn execute_context(
        &self,
        context: QueryExecutionContext,
        enable_topk_limiting: bool,
        enable_topk_formatting: bool,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let results = self
            .execute_query_pipeline(&context, enable_topk_limiting, enable_topk_formatting)
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

    /// Handle a query following Python's unified architecture
    // pub async fn handle_query(
    pub fn handle_query(&self, query: String, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        match self.query_language {
            QueryLanguage::promql => self.handle_query_promql(query, time),
            QueryLanguage::sql => self.handle_query_sql(query, time),
            QueryLanguage::elastic_querydsl => self.handle_query_elastic(query, time),
            QueryLanguage::elastic_sql => self.handle_query_sql(query, time),
        }
    }

    /// Merge precomputed outputs (extracts buckets from timestamped data)
    fn merge_precomputed_outputs(
        &self,
        precomputed_outputs_map: &TimestampedBucketsMap,
        do_merge: bool,
        aggregation_type: AggregationType,
    ) -> HashMap<Option<KeyByLabelValues>, Box<dyn crate::data_model::AggregateCore>> {
        #[cfg(feature = "extra_debugging")]
        let start_time = Instant::now();
        #[cfg(feature = "extra_debugging")]
        debug!("Starting merge for {} keys", precomputed_outputs_map.len());
        #[cfg(feature = "extra_debugging")]
        debug!(
            "do_merge: {}, aggregation_type: {:?}",
            do_merge, aggregation_type
        );

        // Merge if: temporal query OR DeltaSetAggregator (which accumulates keys over time)
        let should_merge = do_merge || aggregation_type == AggregationType::DeltaSetAggregator;

        let mut merged = HashMap::with_capacity(precomputed_outputs_map.len());

        for (key, timestamped_buckets) in precomputed_outputs_map.iter() {
            if timestamped_buckets.is_empty() {
                warn!(
                    "Store returned key {:?} with no precompute buckets; skipping",
                    key
                );
            } else {
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
                    match self.merge_accumulators(precomputes) {
                        Ok(merged_accumulator) => {
                            #[cfg(feature = "extra_debugging")]
                            let merge_duration = merge_start.elapsed();
                            #[cfg(feature = "extra_debugging")]
                            debug!(
                                "  Merge completed in {:.2}ms, result type: {}",
                                merge_duration.as_secs_f64() * 1000.0,
                                merged_accumulator.get_accumulator_type()
                            );
                            merged.insert(key.clone(), merged_accumulator);
                        }
                        Err(e) => {
                            warn!("Failed to merge accumulators for key {:?}: {}", key, e);
                        }
                    }
                } else {
                    // Spatial queries (do_merge=false) normally see exactly 1
                    // precompute per key. A range query's widened fetch can
                    // surface more than expected even when do_merge was
                    // computed false for the base instant range, and future
                    // Sliding-window support may make >1 legitimate here too
                    // — warn and merge instead of asserting/panicking.
                    if precomputes.len() != 1 {
                        warn!(
                            "Spatial query expected 1 precompute per key {:?}, found {}. Merging anyway.",
                            key,
                            precomputes.len()
                        );
                    }
                    match self.merge_accumulators(precomputes) {
                        Ok(merged_accumulator) => {
                            merged.insert(key.clone(), merged_accumulator);
                        }
                        Err(e) => {
                            warn!("Failed to merge accumulators for key {:?}: {}", key, e);
                        }
                    }
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
        accumulators: Vec<Box<dyn crate::data_model::AggregateCore>>,
    ) -> Result<Box<dyn crate::data_model::AggregateCore>, AccumulatorError> {
        if accumulators.is_empty() {
            return Err(AccumulatorError::EmptySlice);
        }

        // Move rather than clone in the common single-bucket case (this owns
        // `accumulators`, unlike NaiveMerger which merges from a borrowed
        // Vec it needs to keep around for the next slide()).
        if accumulators.len() == 1 {
            return Ok(accumulators.into_iter().next().unwrap());
        }

        crate::engines::merge_utils::merge_accumulators_batch(&accumulators)
            .map_err(|e| AccumulatorError::MergeFailed(e.to_string()))
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

        for (group_key, keys_precompute) in merged_keys {
            let value_precompute = merged_values.get(group_key).map(|b| b.as_ref());
            for (key, value) in self.resolve_and_query_group(
                value_precompute,
                Some(keys_precompute.as_ref()),
                group_key,
                statistic,
                query_kwargs,
            ) {
                unformatted_results.insert(key, value);
            }
        }

        Ok(unformatted_results)
    }

    /// Collects results when key and value use same aggregation.
    ///
    /// For keyed accumulators (incl. `CountMinSketchWithHeap`) this enumerates
    /// every candidate key the accumulator exposes. Top-k ordering/truncation is
    /// applied later (sort in `format_final_results`, truncate-to-k in
    /// `execute_query_pipeline`) so we must NOT pre-truncate here — the sketch
    /// heap can hold more than `k` candidates and is not value-sorted, so
    /// dropping keys now could discard a true top-k member.
    fn collect_results_same_aggregation(
        &self,
        merged_outputs: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        let mut unformatted_results = HashMap::new();

        for (group_key, value_precompute) in merged_outputs {
            for (key, value) in self.resolve_and_query_group(
                Some(value_precompute.as_ref()),
                None,
                group_key,
                statistic,
                query_kwargs,
            ) {
                unformatted_results.insert(key, value);
            }
        }

        Ok(unformatted_results)
    }

    fn query_precompute_for_statistic(
        &self,
        precompute: &dyn AggregateCore,
        statistic: &Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        precompute.query_statistic(*statistic, key, query_kwargs)
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

    /// Builds a lookup from bucket start-timestamp to every bucket sharing
    /// that start. A Sliding aggregation can legitimately return more than
    /// one bucket per start timestamp (#567/#570) — every one of them must
    /// be merged, not just the last one collected here. Used identically by
    /// `execute_range_query_pipeline` for both the value side and (#583)
    /// the keys side.
    fn build_bucket_map(
        buckets: &[crate::stores::TimestampedBucket],
    ) -> HashMap<u64, Vec<&dyn AggregateCore>> {
        let mut bucket_map: HashMap<u64, Vec<&dyn AggregateCore>> = HashMap::new();
        for ((start, _), bucket) in buckets {
            bucket_map.entry(*start).or_default().push(bucket.as_ref());
        }
        bucket_map
    }

    /// Collects every bucket in `bucket_map` whose start falls in
    /// `[window_start, window_end)`, stepping by `step_increment`. Missing
    /// buckets at a given start are skipped (partial data is okay). Used
    /// identically by `execute_range_query_pipeline` for both the value
    /// side and (#583) the keys side — the only difference between the two
    /// call sites is which map/lookback/bucket-width they pass in.
    fn scan_window(
        bucket_map: &HashMap<u64, Vec<&dyn AggregateCore>>,
        window_start: u64,
        window_end: u64,
        step_increment: u64,
    ) -> Vec<Box<dyn AggregateCore>> {
        // A zero step never advances `t`, so the loop below would never
        // terminate. This is the single caller-facing guard for that hazard
        // -- callers upstream may also validate their own step sources, but
        // this function has two callers (values, keys) and should not rely
        // on either of them to have done so. Kept active in release builds
        // (assert!, not debug_assert!): a hung query is a production
        // incident, not just a debug-time nicety.
        assert!(
            step_increment > 0,
            "scan_window: step_increment must be nonzero, or this loop never terminates"
        );
        let mut window_buckets: Vec<Box<dyn AggregateCore>> = Vec::new();
        let mut t = window_start;
        while t < window_end {
            if let Some(buckets) = bucket_map.get(&t) {
                window_buckets.extend(buckets.iter().map(|b| b.clone_boxed_core()));
            }
            t += step_increment;
        }
        window_buckets
    }

    /// Returns whatever bucket(s) `bucket_map` has at exactly
    /// `window_start`, or empty if none. Unlike `scan_window`, does not walk
    /// or sum multiple grid positions: for a Sliding aggregation, the bucket
    /// at `window_start` is already the complete, correctly-merged answer
    /// for its window (`worker.rs::merge_panes_for_window` pre-merges before
    /// storing), so summing it with neighboring positions would double-count
    /// overlapping data (#608). Used identically by
    /// `execute_range_query_pipeline` for both the value side and the keys
    /// side.
    fn single_window(
        bucket_map: &HashMap<u64, Vec<&dyn AggregateCore>>,
        window_start: u64,
    ) -> Vec<Box<dyn AggregateCore>> {
        bucket_map
            .get(&window_start)
            .map(|buckets| buckets.iter().map(|b| b.clone_boxed_core()).collect())
            .unwrap_or_default()
    }

    /// Picks how a step's window is composed from `bucket_map`: Sliding ->
    /// `single_window` (one lookup); Tumbling -> `scan_window`
    /// (scan-and-sum). Used identically by `execute_range_query_pipeline`
    /// for both the value side and the keys side (#608).
    fn window_buckets_for_step(
        bucket_map: &HashMap<u64, Vec<&dyn AggregateCore>>,
        window_start: u64,
        window_end: u64,
        step_increment: u64,
        window_type: WindowType,
    ) -> Vec<Box<dyn AggregateCore>> {
        if window_type == WindowType::Sliding {
            Self::single_window(bucket_map, window_start)
        } else {
            Self::scan_window(bucket_map, window_start, window_end, step_increment)
        }
    }

    /// Execute the range query pipeline.
    ///
    /// `enable_topk_limiting`/`enable_topk_formatting` mirror
    /// `execute_query_pipeline`'s flags of the same name (see that method's
    /// doc comment) -- both no-ops unless
    /// `context.base.metadata.statistic_to_compute == Statistic::Topk`. The
    /// actual ranking/truncation is delegated to `apply_range_topk` below;
    /// see its doc comment for why range's version can't just reuse
    /// instant's `format_final_results` truncate-once shape.
    fn execute_range_query_pipeline(
        &self,
        context: &RangeQueryExecutionContext,
        enable_topk_limiting: bool,
        enable_topk_formatting: bool,
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

        // #583: fetch keys raw (no merge). Unlike keys, values have always
        // been fetched raw here and merged per-step below (see the loop);
        // keys used to go through fetch_and_merge_keys, which collapses
        // every fetched bucket into ONE snapshot before this function ever
        // sees it. That collapse is the bug: once buckets are merged
        // together there's no way to ask what the key set looked like at
        // any specific earlier timestamp. Fetching raw and merging per-step,
        // mirroring the values loop, is the fix.
        let keys_raw_data: Option<TimestampedBucketsMap> = match &context.base.store_plan.keys_query
        {
            Some(keys_query) => Some(self.execute_store_query(keys_query)?),
            None => None,
        };

        let mut results: HashMap<KeyByLabelValues, RangeVectorElement> = HashMap::new();

        // Determine accumulator type for merger selection
        let accumulator_type = &context.base.agg_info.aggregation_type_for_value;
        let key_accumulator_type = context.base.agg_info.aggregation_type_for_key;

        // Calculate step parameters
        let step_ms = context.range_params.step;
        let start_ms = context.range_params.start;
        let end_ms = context.range_params.end;
        let buckets_per_step = context.buckets_per_step;
        let lookback_bucket_count = context.lookback_bucket_count;
        let tumbling_window_ms = context.tumbling_window_ms;
        let lookback_ms = (lookback_bucket_count as u64) * tumbling_window_ms;
        let window_type = context.window_type;
        // single_window's correctness for Sliding depends on this equality
        // holding -- it looks up exactly one bucket at
        // `current_time - lookback_ms` and trusts that position to be the
        // step's whole window. Active assert (not debug_assert!): a broken
        // equality here means silently wrong data, the same failure mode
        // #608 fixed, not just a debug-time nicety (#608 review).
        assert!(
            window_type != WindowType::Sliding || lookback_ms == context.window_size_ms,
            "Sliding range query: lookback_ms ({lookback_ms}) must equal window_size_ms \
             ({}) -- single_window's per-step lookup is only correct under this invariant",
            context.window_size_ms
        );
        let keys_lookback_ms = context.keys_lookback_ms;
        let keys_tumbling_window_ms = context.keys_tumbling_window_ms;
        let keys_window_type = context.keys_window_type;
        let keys_window_size_ms = context.keys_window_size_ms;

        // Named distinctly from `WindowType` (Sliding/Tumbling, picks how a
        // step's window is composed from `bucket_map` below -- one lookup vs.
        // a scan-and-sum, see #608) -- this describes step-to-step overlap in
        // the OUTPUT iteration, an unrelated concept that happens to reuse
        // the words "sliding"/"hopping". See #581.
        let step_overlap_mode = if buckets_per_step <= lookback_bucket_count {
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
            tumbling_window_ms,
            buckets_per_step,
            lookback_bucket_count,
            step_overlap_mode
        );

        // Whether the value accumulator's own get_keys() is even consulted
        // depends on the query SHAPE (dual- vs single-population), not on a
        // per-group fallback — mirrors collect_all_results exactly:
        //   - dual-population (KeysSource::PerStep below, separate
        //     keys_query present): always expand via the keys aggregation's
        //     per-step merge (#583). The value accumulator's own get_keys()
        //     is never consulted, even if the value accumulator itself
        //     happens to be self-keyed (e.g. a CountMinSketchWithHeap value
        //     paired with a DeltaSetAggregator keys aggregation is a real
        //     capability-matched config, see sql.rs). Otherwise a
        //     self-keyed value accumulator's own (possibly different,
        //     window-to-window-shifting) keys would silently override the
        //     keys aggregation's expansion. See #587 review.
        //   - single-population (KeysSource::Fixed below, no separate
        //     keys_query): the value accumulator's own get_keys() takes
        //     priority whenever present (#584, self-keyed accumulators like
        //     top-k), evaluated AFTER merging the window's value buckets
        //     (a top-k heap's keys can depend on that window's data),
        //     falling back to the store-level group key otherwise.
        // PerStep bundles everything a dual-population group's per-step
        // resolution needs (bucket_map, lookback_ms, tumbling_window_ms) in
        // one place, built once at group-construction time — rather than
        // three separate Option fields at function scope that only
        // happened to be Some together by convention, each re-unwrapped via
        // .expect() on every iteration of the per-step loop. Making the
        // invalid state (PerStep present but one companion value missing)
        // unrepresentable is the same reasoning that motivated this enum
        // over two raw Option fields in the first place — just applied all
        // the way through instead of partway.
        enum KeysSource<'a> {
            Fixed(Option<KeyByLabelValues>),
            PerStep {
                bucket_map: HashMap<u64, Vec<&'a dyn AggregateCore>>,
                lookback_ms: u64,
                tumbling_window_ms: u64,
                window_type: WindowType,
            },
        }

        // Resolve, for every value group, which groups exist at all (a
        // one-time operation — see design doc) and where their expansion
        // keys come from. A group with keys data but no value data
        // anywhere in the queried range is skipped with a warning instead
        // of failing the whole range query (#583; previously
        // `.ok_or_else(...)?` here hard-failed everything for one missing
        // group). See #582 review for collect_results_separate_keys parity.
        let groups: Vec<(&Vec<crate::stores::TimestampedBucket>, KeysSource)> = match &keys_raw_data
        {
            Some(keys_map) => {
                // keys_raw_data is Some, so context.keys_lookback_ms /
                // context.keys_tumbling_window_ms are guaranteed Some too
                // (both derived from the same keys_query.is_some() check in
                // finish_range_context) -- resolved once here instead of
                // re-unwrapped per group per step.
                let keys_lookback_ms =
                    keys_lookback_ms.expect("keys_raw_data implies keys_lookback_ms is Some");
                let keys_tumbling_window_ms = keys_tumbling_window_ms
                    .expect("keys_raw_data implies keys_tumbling_window_ms is Some");
                let keys_window_type =
                    keys_window_type.expect("keys_raw_data implies keys_window_type is Some");
                let keys_window_size_ms =
                    keys_window_size_ms.expect("keys_raw_data implies keys_window_size_ms is Some");
                // Same invariant as the value side's assert above, for the
                // keys aggregation (#608 review).
                assert!(
                    keys_window_type != WindowType::Sliding
                        || keys_lookback_ms == keys_window_size_ms,
                    "Sliding range query: keys_lookback_ms ({keys_lookback_ms}) must equal \
                     keys_window_size_ms ({keys_window_size_ms}) -- single_window's per-step \
                     keys lookup is only correct under this invariant"
                );
                keys_map
                    .iter()
                    .filter_map(
                        |(group_key, raw_keys_buckets)| match all_data.get(group_key) {
                            Some(timestamped_buckets) => Some((
                                timestamped_buckets,
                                KeysSource::PerStep {
                                    bucket_map: Self::build_bucket_map(raw_keys_buckets),
                                    lookback_ms: keys_lookback_ms,
                                    tumbling_window_ms: keys_tumbling_window_ms,
                                    window_type: keys_window_type,
                                },
                            )),
                            None => {
                                warn!(
                                    "Range query: group {:?} has keys data but no value data \
                             anywhere in the queried range — skipping this group instead \
                             of failing the whole query (#583)",
                                    group_key
                                );
                                None
                            }
                        },
                    )
                    .collect()
            }
            // #584/#587: keep every group, including group_key=None — that's
            // exactly where a self-keyed single-population accumulator
            // (e.g. top-k) is typically stored. An empty fallback list here
            // is fine; the per-step loop below tries the value
            // accumulator's own get_keys() first and only falls back to
            // this list.
            None => all_data
                .iter()
                .map(|(group_key, buckets)| (buckets, KeysSource::Fixed(group_key.clone())))
                .collect(),
        };

        // Process each value group independently
        for (timestamped_buckets, keys_source) in groups {
            let bucket_map = Self::build_bucket_map(timestamped_buckets);

            debug!(
                "Group with {} start-timestamps ({} keys start-timestamps)",
                bucket_map.len(),
                match &keys_source {
                    KeysSource::PerStep { bucket_map, .. } => bucket_map.len(),
                    KeysSource::Fixed(_) => 0,
                }
            );

            // Iterate by OUTPUT timestamp, not by bucket index
            let mut current_time = start_ms;
            while current_time <= end_ms {
                // #583: dual-population groups resolve their expansion keys
                // from the keys aggregation, per step — not a single
                // snapshot reused for every step. If nothing resolves at
                // this step, skip it before ever touching the value merge
                // below (avoids wasted merge work on steps outside the
                // key's lifetime). Fixed (single-population) groups have no
                // separate keys accumulator to merge here at all.
                let keys_precompute: Option<Box<dyn AggregateCore>> = match &keys_source {
                    KeysSource::PerStep {
                        bucket_map: keys_bucket_map,
                        lookback_ms: keys_lookback_ms,
                        tumbling_window_ms: keys_tumbling_window_ms,
                        window_type: keys_window_type,
                    } => {
                        let keys_window_start = current_time.saturating_sub(*keys_lookback_ms);
                        let keys_window_buckets = Self::window_buckets_for_step(
                            keys_bucket_map,
                            keys_window_start,
                            current_time,
                            *keys_tumbling_window_ms,
                            *keys_window_type,
                        );

                        if keys_window_buckets.is_empty() {
                            debug!(
                                "No keys data in window at t={} — skipping this step for this group",
                                current_time
                            );
                            current_time += step_ms;
                            continue;
                        }

                        let mut key_merger = create_window_merger(key_accumulator_type);
                        key_merger.initialize(keys_window_buckets);
                        match key_merger.get_merged() {
                            Ok(merged_keys) => Some(merged_keys),
                            Err(e) => {
                                warn!("Failed to merge keys at t={}: {}", current_time, e);
                                current_time += step_ms;
                                continue;
                            }
                        }
                    }
                    KeysSource::Fixed(_) => None,
                };

                // Window covers [current_time - lookback_ms, current_time)
                // This means we look at buckets that START within this range
                let window_start = current_time.saturating_sub(lookback_ms);

                let window_buckets = Self::window_buckets_for_step(
                    &bucket_map,
                    window_start,
                    current_time,
                    tumbling_window_ms,
                    window_type,
                );

                if window_buckets.is_empty() {
                    // No data at all for this window - skip sample
                    debug!(
                        "Skipping sample at {} - no data in window [{}, {})",
                        current_time, window_start, current_time
                    );
                    current_time += step_ms;
                    continue;
                }

                let mut merger = create_window_merger(*accumulator_type);
                merger.initialize(window_buckets);

                let merged = match merger.get_merged() {
                    Ok(merged) => merged,
                    Err(e) => {
                        debug!("Failed to get merged result at t={}: {}", current_time, e);
                        current_time += step_ms;
                        continue;
                    }
                };

                let fallback_key = match &keys_source {
                    KeysSource::Fixed(fallback_key) => fallback_key.clone(),
                    KeysSource::PerStep { .. } => None,
                };

                // See the note above KeysSource: dual-population always
                // resolves via keys_precompute; single-population lets the
                // value accumulator's own get_keys() (read after merging
                // this window, since e.g. a top-k heap's keys depend on the
                // window's data) take priority, falling back to
                // fallback_key otherwise. Same resolver instant uses
                // (resolve_and_query_group) -- see #581.
                for (key, value) in self.resolve_and_query_group(
                    Some(merged.as_ref()),
                    keys_precompute.as_deref(),
                    &fallback_key,
                    &context.base.metadata.statistic_to_compute,
                    &context.base.metadata.query_kwargs,
                ) {
                    // A fully unlabeled result (fallback_key was None and
                    // the value accumulator has no self-keys) has no
                    // RangeVectorElement representation (labels:
                    // KeyByLabelValues, not Option) -- matches today's
                    // behavior of producing no sample for this combination.
                    let Some(key) = key else { continue };
                    results
                        .entry(key.clone())
                        .or_insert_with(|| RangeVectorElement::new(key))
                        .add_sample(current_time, value);
                }

                current_time += step_ms;
            }
        }

        Ok(self.apply_range_topk(
            results,
            &context.base.metadata.statistic_to_compute,
            &context.base.metadata.query_kwargs,
            &context.base.metric,
            enable_topk_formatting,
            enable_topk_limiting,
        ))
    }

    /// Applies PromQL top-k semantics to a range query's raw per-group
    /// results. No-op unless `statistic == Statistic::Topk` (mirrors
    /// `format_final_results`).
    ///
    /// This is deliberately NOT a straight port of `format_final_results`
    /// (sort all groups once by value, then truncate to k): that shape only
    /// works because instant queries have exactly one value per group. A
    /// range query's `RangeVectorElement` carries many per-timestamp
    /// samples, and real PromQL `topk(k, range_vector)` semantics rank
    /// independently AT EACH timestamp -- the surviving key set can differ
    /// from step to step. So this ranks/truncates per-timestamp
    /// ("step-major"), across all groups, as its own pass over the
    /// already-assembled results -- rather than restructuring the group-major
    /// fetch/merge loop above into a step-major shape. Issue #581's own
    /// scoping decided the fetch/merge loop itself becomes step-major only
    /// as part of stage E, the full instant/range pipeline collapse (not
    /// done here, deliberately -- this is stage-E prep). Doing the ranking
    /// as a separate pass gets the same correctness (each timestamp's kept
    /// set is decided across all groups, never one group at a time) without
    /// front-running that larger, separately-staged restructure.
    fn apply_range_topk(
        &self,
        mut results: HashMap<KeyByLabelValues, crate::engines::query_result::RangeVectorElement>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
        metric: &str,
        enable_topk_formatting: bool,
        enable_topk_limiting: bool,
    ) -> Vec<crate::engines::query_result::RangeVectorElement> {
        if *statistic != Statistic::Topk {
            return results.into_values().collect();
        }

        // Limiting MUST run before formatting: it matches
        // `kept_timestamps_by_key`'s keys (read from each element's
        // `labels` field) against `results`' own HashMap keys via
        // `retain`. Formatting rewrites `elem.labels` (the field) without
        // touching the HashMap's outer key, so if formatting ran first the
        // two would no longer agree and `retain` would drop every group.
        if enable_topk_limiting {
            if let Some(k) = query_kwargs.get("k").and_then(|s| s.parse::<usize>().ok()) {
                use std::collections::HashSet;

                // Step-major ranking: group every group's samples by
                // timestamp first, so each timestamp's top-k decision sees
                // every group's value at that timestamp.
                let mut by_timestamp: HashMap<u64, Vec<(KeyByLabelValues, f64)>> = HashMap::new();
                for elem in results.values() {
                    for sample in &elem.samples {
                        by_timestamp
                            .entry(sample.timestamp)
                            .or_default()
                            .push((elem.labels.clone(), sample.value));
                    }
                }

                let mut kept_timestamps_by_key: HashMap<KeyByLabelValues, HashSet<u64>> =
                    HashMap::new();
                for (timestamp, mut candidates) in by_timestamp {
                    candidates
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                    candidates.truncate(k);
                    for (key, _) in candidates {
                        kept_timestamps_by_key
                            .entry(key)
                            .or_default()
                            .insert(timestamp);
                    }
                }

                results.retain(|key, _| kept_timestamps_by_key.contains_key(key));
                for elem in results.values_mut() {
                    // `keep` is built only from timestamps that already
                    // appear in this same element's `samples` (see the
                    // `by_timestamp` loop above), and is non-empty for every
                    // key that survives the `retain` just above -- so this
                    // filter can never leave `elem.samples` empty.
                    let keep = &kept_timestamps_by_key[&elem.labels];
                    elem.samples.retain(|s| keep.contains(&s.timestamp));
                }
            }
        }

        if enable_topk_formatting {
            // Prepend metric name to each key's label values (PromQL shape),
            // same rewrite as format_final_results does for instant. Safe to
            // mutate `elem.labels` now -- nothing below matches it back
            // against the HashMap's outer key.
            for elem in results.values_mut() {
                let mut new_labels = vec![metric.to_string()];
                new_labels.extend(elem.labels.labels.clone());
                elem.labels.labels = new_labels;
            }
        }

        results.into_values().collect()
    }
}

#[cfg(test)]
mod range_query_tests {
    use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues, SerializableToSink};
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

        fn get_accumulator_type(&self) -> AggregationType {
            AggregationType::Sum
        }

        fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
            None
        }

        fn query_statistic(
            &self,
            _statistic: promql_utilities::query_logics::enums::Statistic,
            _key: &Option<KeyByLabelValues>,
            _query_kwargs: &std::collections::HashMap<String, String>,
        ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
            Err("MockBucketAccumulator does not support query_statistic".into())
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

/// Issue #596 regression tests: `SimpleEngine::merge_accumulators` (private,
/// tested here as a descendant module of `simple_engine`) and
/// `NaiveMerger::merge_all` (public, via the `WindowMerger` trait) must both
/// take the CMS/KLL batch-merge fast path and, on failure or inapplicability,
/// fall back to a sequential fold that ABORTS on the first `merge_with`
/// error rather than warning-and-continuing or silently dropping a bucket.
///
/// These tests only observe behavior through the public `WindowMerger` API
/// and `SimpleEngine::merge_accumulators`'s public signature (its body is
/// intentionally not read), plus a manually-computed oracle fold, so they
/// don't assume which code path either implementation actually takes for a
/// given input.
#[cfg(test)]
mod merge_accumulators_regression_tests_596 {
    use crate::data_model::{
        AggregateCore, AggregationType, CleanupPolicy, InferenceConfig, KeyByLabelValues,
        PrecomputedOutput, QueryLanguage, SerializableToSink, StreamingConfig,
    };
    use crate::engines::simple_engine::SimpleEngine;
    use crate::engines::window_merger::{NaiveMerger, WindowMerger};
    use crate::precompute_operators::{
        AccumulatorError, CountMinSketchAccumulator, DatasketchesKLLAccumulator, SumAccumulator,
    };
    use crate::stores::{Store, TimestampedBucketsMap};
    use crate::tests::test_utilities::{
        cms_from_matrix, oracle_sequential_fold, PoisonableAccumulator,
    };
    use serde_json::Value;
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Store that must never actually be hit: `merge_accumulators` merges
    /// accumulators already in hand, it doesn't query storage.
    struct NoOpStore;

    impl Store for NoOpStore {
        fn insert_precomputed_output(
            &self,
            _: PrecomputedOutput,
            _: Box<dyn AggregateCore>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            panic!("NoOpStore should not be called by merge_accumulators tests");
        }
        fn insert_precomputed_output_batch(
            &self,
            _: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            panic!("NoOpStore should not be called by merge_accumulators tests");
        }
        fn query_precomputed_output(
            &self,
            _: &str,
            _: u64,
            _: u64,
            _: u64,
        ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
            panic!("NoOpStore should not be called by merge_accumulators tests");
        }
        fn query_precomputed_output_exact(
            &self,
            _: &str,
            _: u64,
            _: u64,
            _: u64,
        ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
            panic!("NoOpStore should not be called by merge_accumulators tests");
        }
        fn query_precomputed_output_exact_batch(
            &self,
            _: &str,
            _: u64,
            _: &[crate::stores::TimestampRange],
        ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
            panic!("NoOpStore should not be called by merge_accumulators tests");
        }
        fn get_earliest_timestamp_per_aggregation_id(
            &self,
        ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(HashMap::new())
        }
        fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
    }

    fn test_engine() -> SimpleEngine {
        let inference_config =
            InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
        let streaming_config = Arc::new(StreamingConfig::default());
        SimpleEngine::new(
            Arc::new(NoOpStore),
            inference_config,
            streaming_config,
            15,
            QueryLanguage::promql,
        )
    }

    fn naive_merger_result(
        buckets: Vec<Box<dyn AggregateCore>>,
    ) -> Result<Box<dyn AggregateCore>, String> {
        let mut merger = NaiveMerger::new();
        merger.initialize(buckets);
        merger.get_merged()
    }

    // ---- Property 1: cross-path equivalence (CMS, KLL) ----

    #[test]
    fn merge_accumulators_naive_merger_and_oracle_agree_on_cms_batch() {
        let cms_boxes: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(cms_from_matrix(
                vec![vec![5.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]],
                2,
                3,
            )),
            Box::new(cms_from_matrix(
                vec![vec![2.0, 3.0, 0.0], vec![0.0, 0.0, 4.0]],
                2,
                3,
            )),
            Box::new(cms_from_matrix(
                vec![vec![0.0, 0.0, 7.0], vec![1.0, 0.0, 0.0]],
                2,
                3,
            )),
        ];

        let oracle = oracle_sequential_fold(&cms_boxes);
        let oracle_sketch = oracle
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap()
            .inner
            .sketch();

        let engine = test_engine();
        let engine_input: Vec<Box<dyn AggregateCore>> = cms_boxes.to_vec();
        let engine_result = engine
            .merge_accumulators(engine_input)
            .expect("SimpleEngine::merge_accumulators should merge a same-typed CMS batch");
        let engine_sketch = engine_result
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap()
            .inner
            .sketch();

        let naive_input: Vec<Box<dyn AggregateCore>> = cms_boxes.to_vec();
        let naive_result = naive_merger_result(naive_input)
            .expect("NaiveMerger should merge a same-typed CMS batch");
        let naive_sketch = naive_result
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap()
            .inner
            .sketch();

        assert_eq!(
            engine_sketch, oracle_sketch,
            "SimpleEngine::merge_accumulators must match a manual sequential merge_with fold"
        );
        assert_eq!(
            naive_sketch, oracle_sketch,
            "NaiveMerger must match a manual sequential merge_with fold"
        );
        assert_eq!(
            engine_sketch, naive_sketch,
            "SimpleEngine::merge_accumulators and NaiveMerger must agree on the same CMS batch"
        );
    }

    #[test]
    fn merge_accumulators_naive_merger_and_oracle_agree_on_kll_batch() {
        let mut k1 = DatasketchesKLLAccumulator::new(200);
        for i in 1..=5 {
            k1.update(i as f64);
        }
        let mut k2 = DatasketchesKLLAccumulator::new(200);
        for i in 6..=10 {
            k2.update(i as f64);
        }
        let mut k3 = DatasketchesKLLAccumulator::new(200);
        for i in 11..=15 {
            k3.update(i as f64);
        }
        let boxes: Vec<Box<dyn AggregateCore>> = vec![Box::new(k1), Box::new(k2), Box::new(k3)];

        let oracle = oracle_sequential_fold(&boxes);
        let oracle_kll = oracle
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();
        let oracle_count = oracle_kll.inner.count();
        let oracle_min = oracle_kll.get_quantile(0.0);
        let oracle_max = oracle_kll.get_quantile(1.0);

        let engine = test_engine();
        let engine_input: Vec<Box<dyn AggregateCore>> = boxes.to_vec();
        let engine_result = engine
            .merge_accumulators(engine_input)
            .expect("SimpleEngine::merge_accumulators should merge a same-typed KLL batch");
        let engine_kll = engine_result
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();

        let naive_input: Vec<Box<dyn AggregateCore>> = boxes.to_vec();
        let naive_result = naive_merger_result(naive_input)
            .expect("NaiveMerger should merge a same-typed KLL batch");
        let naive_kll = naive_result
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();

        assert_eq!(engine_kll.inner.count() as usize, oracle_count as usize);
        assert_eq!(naive_kll.inner.count() as usize, oracle_count as usize);
        assert_eq!(engine_kll.get_quantile(0.0), oracle_min);
        assert_eq!(naive_kll.get_quantile(0.0), oracle_min);
        assert_eq!(engine_kll.get_quantile(1.0), oracle_max);
        assert_eq!(naive_kll.get_quantile(1.0), oracle_max);
        assert_eq!(
            engine_kll.inner.count() as usize,
            naive_kll.inner.count() as usize,
            "SimpleEngine::merge_accumulators and NaiveMerger must agree on total merged count"
        );
    }

    // ---- Property 2: fold order / non-commutativity ----

    /// Mock accumulator whose `merge_with` concatenates logs and adopts the
    /// right operand's `last` field -- deliberately non-commutative so a
    /// reversed, shuffled, or otherwise-reordered fold is detectable in the
    /// final `log`, independent of any real accumulator's semantics.
    #[derive(Clone, Debug)]
    struct MockOrderedLogAccumulator {
        log: Vec<i32>,
        last: i32,
    }

    impl MockOrderedLogAccumulator {
        fn new(v: i32) -> Self {
            Self {
                log: vec![v],
                last: v,
            }
        }
    }

    impl SerializableToSink for MockOrderedLogAccumulator {
        fn serialize_to_json(&self) -> Value {
            serde_json::json!({"log": self.log, "last": self.last})
        }
        fn serialize_to_bytes(&self) -> Vec<u8> {
            Vec::new()
        }
    }

    impl AggregateCore for MockOrderedLogAccumulator {
        fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
            Box::new(self.clone())
        }
        fn type_name(&self) -> &'static str {
            "MockOrderedLogAccumulator"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn merge_with(
            &self,
            other: &dyn AggregateCore,
        ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
            let other_m = other
                .as_any()
                .downcast_ref::<MockOrderedLogAccumulator>()
                .ok_or("Cannot merge with different accumulator type")?;
            let mut log = self.log.clone();
            log.extend(other_m.log.iter().copied());
            Ok(Box::new(MockOrderedLogAccumulator {
                log,
                last: other_m.last,
            }))
        }
        fn get_accumulator_type(&self) -> AggregationType {
            AggregationType::Sum
        }
        fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
            None
        }
        fn query_statistic(
            &self,
            _statistic: promql_utilities::query_logics::enums::Statistic,
            _key: &Option<KeyByLabelValues>,
            _query_kwargs: &std::collections::HashMap<String, String>,
        ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
            Err("MockOrderedLogAccumulator does not support query_statistic".into())
        }
    }

    #[test]
    fn merge_accumulators_and_naive_merger_fold_left_to_right_in_input_order() {
        let inputs: Vec<Box<dyn AggregateCore>> = (1..=5)
            .map(|i| Box::new(MockOrderedLogAccumulator::new(i)) as Box<dyn AggregateCore>)
            .collect();
        let expected_log = vec![1, 2, 3, 4, 5];

        let engine = test_engine();
        let engine_input: Vec<Box<dyn AggregateCore>> = inputs.to_vec();
        let engine_result = engine
            .merge_accumulators(engine_input)
            .expect("engine merge of ordered-log mocks should succeed");
        let engine_mock = engine_result
            .as_any()
            .downcast_ref::<MockOrderedLogAccumulator>()
            .unwrap();

        let naive_input: Vec<Box<dyn AggregateCore>> = inputs.to_vec();
        let naive_result = naive_merger_result(naive_input)
            .expect("NaiveMerger merge of ordered-log mocks should succeed");
        let naive_mock = naive_result
            .as_any()
            .downcast_ref::<MockOrderedLogAccumulator>()
            .unwrap();

        assert_eq!(
            engine_mock.log, expected_log,
            "SimpleEngine::merge_accumulators must fold left-to-right in input order"
        );
        assert_eq!(
            naive_mock.log, expected_log,
            "NaiveMerger must fold left-to-right in input order"
        );
        assert_eq!(engine_mock.last, 5);
        assert_eq!(naive_mock.last, 5);
    }

    // ---- Property 3: abort-on-error, not silent-drop ----

    /// Mock accumulator whose `merge_with` fails under a condition the test
    /// fully controls (a `poisoned` flag), independent of any real
    /// accumulator's library-specific error conditions.
    #[test]
    fn merge_accumulators_and_naive_merger_abort_on_mid_batch_error_not_silent_drop() {
        // Buckets 1 and 2 merge fine; bucket 3 poisons the fold partway
        // through; bucket 4 would also merge fine if reached. A correct
        // implementation aborts the ENTIRE merge (Err), not just drops
        // bucket 3 and returns Ok(merge(1, 2, 4)) or Ok(merge(1, 2)).
        let make_buckets = || -> Vec<Box<dyn AggregateCore>> {
            vec![
                Box::new(PoisonableAccumulator {
                    id: 1,
                    poisoned: false,
                }),
                Box::new(PoisonableAccumulator {
                    id: 2,
                    poisoned: false,
                }),
                Box::new(PoisonableAccumulator {
                    id: 3,
                    poisoned: true,
                }),
                Box::new(PoisonableAccumulator {
                    id: 4,
                    poisoned: false,
                }),
            ]
        };

        let engine = test_engine();
        let engine_result = engine.merge_accumulators(make_buckets());
        assert!(
            engine_result.is_err(),
            "SimpleEngine::merge_accumulators must abort the whole merge (Err), not \
             silently drop the failed bucket and return a partial Ok"
        );

        let naive_result = naive_merger_result(make_buckets());
        assert!(
            naive_result.is_err(),
            "NaiveMerger must abort the whole merge (Err), not silently drop the failed \
             bucket and return a partial Ok"
        );

        assert_eq!(
            engine_result.is_err(),
            naive_result.is_err(),
            "SimpleEngine::merge_accumulators and NaiveMerger must agree on abort behavior"
        );
    }

    // ---- Property 4: batch-merge-failure fallback still errors consistently ----

    #[test]
    fn merge_accumulators_and_naive_merger_cms_wrong_type_mixed_in_errors_consistently() {
        let make_buckets = || -> Vec<Box<dyn AggregateCore>> {
            vec![
                Box::new(cms_from_matrix(vec![vec![1.0, 0.0], vec![0.0, 1.0]], 2, 2)),
                Box::new(cms_from_matrix(vec![vec![2.0, 0.0], vec![0.0, 2.0]], 2, 2)),
                Box::new(SumAccumulator::new()),
            ]
        };

        let engine = test_engine();
        let engine_result = engine.merge_accumulators(make_buckets());
        assert!(
            engine_result.is_err(),
            "SimpleEngine::merge_accumulators must reject a CMS batch with an \
             incompatible accumulator type mixed in"
        );

        let naive_result = naive_merger_result(make_buckets());
        assert!(
            naive_result.is_err(),
            "NaiveMerger must reject a CMS batch with an incompatible accumulator type \
             mixed in"
        );
    }

    #[test]
    fn merge_accumulators_and_naive_merger_kll_wrong_type_mixed_in_errors_consistently() {
        let make_buckets = || -> Vec<Box<dyn AggregateCore>> {
            let mut kll1 = DatasketchesKLLAccumulator::new(200);
            kll1.update(1.0);
            let mut kll2 = DatasketchesKLLAccumulator::new(200);
            kll2.update(2.0);
            vec![
                Box::new(kll1),
                Box::new(kll2),
                Box::new(SumAccumulator::new()),
            ]
        };

        let engine = test_engine();
        let engine_result = engine.merge_accumulators(make_buckets());
        assert!(
            engine_result.is_err(),
            "SimpleEngine::merge_accumulators must reject a KLL batch with an \
             incompatible accumulator type mixed in"
        );

        let naive_result = naive_merger_result(make_buckets());
        assert!(
            naive_result.is_err(),
            "NaiveMerger must reject a KLL batch with an incompatible accumulator type \
             mixed in"
        );
    }

    // ---- Property 5: single-accumulator and large-batch edge sizes ----

    #[test]
    fn merge_accumulators_single_cms_accumulator_passes_through_unchanged() {
        let matrix = vec![vec![3.0, 0.0, 5.0], vec![0.0, 7.0, 0.0]];
        let cms = cms_from_matrix(matrix.clone(), 2, 3);

        let engine = test_engine();
        let result = engine
            .merge_accumulators(vec![Box::new(cms)])
            .expect("single-bucket CMS merge should succeed");
        let merged_cms = result
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap();

        assert_eq!(merged_cms.inner.sketch(), matrix);
    }

    #[test]
    fn merge_accumulators_single_kll_accumulator_passes_through_unchanged() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for i in 1..=7 {
            kll.update(i as f64);
        }
        let expected_count = kll.inner.count();
        let expected_min = kll.get_quantile(0.0);
        let expected_max = kll.get_quantile(1.0);

        let engine = test_engine();
        let result = engine
            .merge_accumulators(vec![Box::new(kll)])
            .expect("single-bucket KLL merge should succeed");
        let merged_kll = result
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();

        assert_eq!(merged_kll.inner.count() as usize, expected_count as usize);
        assert_eq!(merged_kll.get_quantile(0.0), expected_min);
        assert_eq!(merged_kll.get_quantile(1.0), expected_max);
    }

    #[test]
    fn merge_accumulators_large_cms_batch_merges_every_bucket_not_just_a_prefix() {
        const N: usize = 80;
        let mut boxes: Vec<Box<dyn AggregateCore>> = Vec::with_capacity(N);
        for i in 0..N {
            // Each bucket sets exactly one cell to 1.0. The merged row-0
            // total can only equal N if every bucket actually contributed --
            // a fast path that truncates to a prefix (or skips entirely)
            // would under-count.
            let col = i % 3;
            let mut row0 = vec![0.0, 0.0, 0.0];
            row0[col] = 1.0;
            boxes.push(Box::new(cms_from_matrix(
                vec![row0, vec![0.0, 0.0, 0.0]],
                2,
                3,
            )));
        }

        let engine = test_engine();
        let result = engine
            .merge_accumulators(boxes)
            .expect("large CMS batch should merge");
        let merged_cms = result
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap();
        let total: f64 = merged_cms.inner.sketch()[0].iter().sum();

        assert_eq!(
            total, N as f64,
            "merged CMS row-0 total mass ({total}) must equal the number of buckets \
             ({N}) -- a truncated or skipped fast path would under-count"
        );
    }

    #[test]
    fn merge_accumulators_large_kll_batch_merges_every_bucket_not_just_a_prefix() {
        const N: usize = 70;
        const PER_BUCKET: usize = 4;
        let mut boxes: Vec<Box<dyn AggregateCore>> = Vec::with_capacity(N);
        for i in 0..N {
            let mut kll = DatasketchesKLLAccumulator::new(200);
            for j in 0..PER_BUCKET {
                kll.update((i * PER_BUCKET + j) as f64);
            }
            boxes.push(Box::new(kll));
        }

        let engine = test_engine();
        let result = engine
            .merge_accumulators(boxes)
            .expect("large KLL batch should merge");
        let merged_kll = result
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();

        assert_eq!(
            merged_kll.inner.count() as usize,
            N * PER_BUCKET,
            "merged KLL total count must reflect every bucket's updates -- a truncated \
             or skipped fast path would under-count"
        );
    }

    // ---- Property 6: empty input ----

    #[test]
    fn merge_accumulators_rejects_empty_input_without_panicking() {
        let engine = test_engine();
        let result = engine.merge_accumulators(vec![]);
        match result {
            Err(AccumulatorError::EmptySlice) => {}
            Err(other) => {
                panic!("expected AccumulatorError::EmptySlice for empty input, got: {other:?}")
            }
            Ok(_) => panic!("merge_accumulators must reject empty input with Err, got Ok"),
        }
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

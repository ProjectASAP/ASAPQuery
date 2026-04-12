//! Elasticsearch DSL query language handler for SimpleEngine.
//!
//! Contains all Elastic DSL-specific context building and query dispatch.

use super::SimpleEngine;
use super::{QueryExecutionContext, QueryMetadata, QueryTimestamps};
use crate::engines::query_result::QueryResult;
use elastic_dsl_utilities::pattern::parse_and_classify;
use elastic_dsl_utilities::types::{EsDslQueryPattern, GroupBySpec, MetricAggType};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use std::collections::HashMap;
use tracing::{debug, warn};

impl SimpleEngine {
    pub fn handle_query_elastic(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let context = self.build_query_execution_context_elastic(query, time)?;
        debug!(
            "Built execution context for ElasticSearch query {:?}",
            context
        );
        self.execute_context(context, false)
    }

    pub fn build_query_execution_context_elastic(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        let query_time = Self::convert_query_time_to_data_time(time);

        // 1. Parse query DSL somehow. Elasticsearch DSL crate does not support deserializing, but maybe can use Opensearch instead?
        // 2. Determine whether query is supported using some AST representation or hardcoded pattern matching.
        let query_pattern: EsDslQueryPattern =
            parse_and_classify(&query).unwrap_or(EsDslQueryPattern::Unknown);
        match query_pattern {
            EsDslQueryPattern::Unknown => {
                debug!("Could not parse query into known pattern");
                return None;
            }
            _ => {
                debug!("Parsed query pattern: {:?}", query_pattern);
            }
        }

        // 3. Convert parsed query into execution context components (labels, statistic, kwargs, metadata, store query plan, etc.)

        // TODO: Figure out how to handle query configuration for ElasticSearch queries.
        let query_config = self.find_query_config(&query)?;
        let agg_info = self
            .get_aggregation_id_info(query_config)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let do_merge = true; // No "instant" queries in ElasticSearch supported for now, so we always need to merge.

        let (metric, query_metadata) = self.build_query_metadata_elastic(&query_pattern)?;

        let spatial_filter = String::new(); // Placeholder - extract from query if applicable

        // TODO: Need way to parse ES DSL "date math".
        let timestamps = self.resolve_query_time_range_elastic(query_time, query_pattern);

        let query_plan = self
            .create_store_query_plan(&metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        let grouping_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_metadata.query_output_labels.clone());

        let aggregated_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric,
            metadata: query_metadata,
            store_plan: query_plan.clone(),
            agg_info: agg_info.clone(),
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })
    }

    fn build_query_metadata_elastic(
        &self,
        query_pattern: &EsDslQueryPattern,
    ) -> Option<(String, QueryMetadata)> {
        // Constructs QueryMetadata based on the parsed ES DSL query pattern. This includes determining the
        // metric to query, the statistic to compute, and any relevant query kwargs (e.g. quantile value for percentiles).

        // Figure out aggregation type and what labels are included in output.
        // By default, we only include grouping labels in the output for ES DSL.

        // Take first aggregation by default since current engine doesn't support multiple aggregations in a single query.
        let aggregation = query_pattern.get_metric_aggs()?.first()?.clone();

        // By default, we only include grouping labels in the output for ES DSL.
        let query_output_labels = match query_pattern.get_groupby_spec() {
            Some(GroupBySpec::Terms { field }) => KeyByLabelNames::new(vec![field.clone()]),
            Some(GroupBySpec::MultiTerms { fields }) => KeyByLabelNames::new(fields.to_vec()),
            None => KeyByLabelNames::empty(),
        };

        let metric = aggregation.field.clone();

        // Map ElasticSearch aggregation types to our internal Statistic enum.
        let statistic_to_compute = match aggregation.agg_type {
            MetricAggType::Percentiles => Statistic::Quantile,
            MetricAggType::Avg => Statistic::Rate,
            MetricAggType::Sum => Statistic::Sum,
            MetricAggType::Min => Statistic::Min,
            MetricAggType::Max => Statistic::Max,
        };

        let mut query_kwargs = HashMap::new(); // Placeholder - build based on query and statistic
        if aggregation.agg_type == MetricAggType::Percentiles {
            // Extract quantile value from aggregation parameters and add to query_kwargs
            if let Some(params) = &aggregation.params {
                if let Some(percents) = params.get("percents") {
                    // Get first value from percents array since we only support one quantile argument for now.
                    let quantile = percents
                        .as_array()
                        .and_then(|arr| arr.first())
                        .and_then(|v| v.as_f64());
                    // ES percentiles are specified as values between 0 and 100, but we want to convert to 0-1 range for our internal representation.
                    query_kwargs.insert("quantile".to_string(), (quantile? / 100.0).to_string());
                }
            }
        }

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };
        Some((metric, metadata))
    }

    pub fn resolve_query_time_range_elastic(
        &self,
        query_time: u64,
        query_pattern: EsDslQueryPattern,
    ) -> QueryTimestamps {
        // Resolves the actual start and end timestamps into milliseconds for an ElasticSearch query
        // based on the provided query_time and the time range specified in the ES DSL query pattern (if any).
        // If no time range is specified, default to entire history up to query_time.

        let mut start_timestamp: u64 = 0;
        let mut end_timestamp: u64 = query_time;

        let time_range = query_pattern.get_time_range();
        if let Some(tr) = time_range {
            if let Some(resolved_range) = tr.resolve_epoch_millis(query_time as i64) {
                debug!(
                    "Parsed time range from query: start={} end={}",
                    resolved_range.gte_ms.unwrap_or(0),
                    resolved_range.lte_ms.unwrap_or(0)
                );
                start_timestamp = resolved_range.gte_ms.unwrap_or(0) as u64;
                end_timestamp = resolved_range.lte_ms.unwrap_or(query_time as i64) as u64;
            } else {
                debug!("Failed to resolve time range from query");
            }
        };

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }
}

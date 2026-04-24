//! Elasticsearch DSL query language handler for SimpleEngine.
//!
//! Contains all Elastic DSL-specific context building and query dispatch.

use super::SimpleEngine;
use super::{QueryExecutionContext, QueryMetadata, QueryTimestamps};
use crate::engines::query_result::QueryResult;
use elastic_dsl_utilities::ast_parsing::{
    self, AggregationType, ElasticDSLQueryInfo, GroupBySpec, Predicate,
};
use elastic_dsl_utilities::datemath::range_query_to_time_range;
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
        // 2. Determine whether query is supported using some AST representation or hardcoded pattern matching (do this throughout context) - parsing validates basic structure.
        let query_info = ast_parsing::extract_info::extract_query_info(&query)?;

        // 3. Convert parsed query into execution context components (labels, statistic, kwargs, metadata, store query plan, etc.)

        // TODO: Figure out how to handle query configuration for ElasticSearch queries.
        let query_config = self.find_query_config(&query)?;
        let agg_info = self
            .get_aggregation_id_info(&query_config)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let do_merge = true; // No "instant" queries in ElasticSearch supported for now, so we always need to merge.

        let (metric, query_metadata) = self.build_query_metadata_elastic(&query_info)?;

        let spatial_filter = String::new(); // Placeholder - extract from query if applicable

        // Parse time range information from first query predicate if available, otherwise default to entire history up to query_time.
        let timestamps = self.resolve_query_time_range_elastic(query_time, query_info);

        let query_plan = self
            .create_store_query_plan(&metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        let sc = self.streaming_config.read().unwrap().clone();
        let grouping_labels = sc
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_metadata.query_output_labels.clone());

        let aggregated_labels = sc
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
        query_info: &ElasticDSLQueryInfo,
    ) -> Option<(String, QueryMetadata)> {
        // Constructs QueryMetadata based on the parsed ES DSL query pattern. This includes determining the
        // metric to query, the statistic to compute, and any relevant query kwargs (e.g. quantile value for percentiles).

        // Figure out aggregation type and what labels are included in output.
        // By default, we only include grouping labels in the output for ES DSL.

        // Take first aggregation by default since current engine doesn't support multiple aggregations in a single query.
        let aggregation = query_info.aggregation.clone();

        // By default, we only include grouping labels in the output for ES DSL.
        let query_output_labels = match &query_info.group_by_buckets {
            Some(GroupBySpec::Fields(fields)) => KeyByLabelNames::new(fields.clone()),
            Some(GroupBySpec::Filters(_)) => {
                return None;
            } // We don't support filter-based group by in ES DSL for now, so return None to indicate unsupported query pattern.
            None => KeyByLabelNames::empty(),
        };

        let metric = query_info.target_field.clone();

        // Map ElasticSearch aggregation types to our internal Statistic enum.
        let statistic_to_compute = match aggregation {
            AggregationType::Percentiles(_) => Statistic::Quantile,
            AggregationType::Avg => Statistic::Rate,
            AggregationType::Sum => Statistic::Sum,
            AggregationType::Min => Statistic::Min,
            AggregationType::Max => Statistic::Max,
        };

        let mut query_kwargs = HashMap::new();
        if let AggregationType::Percentiles(percents) = aggregation {
            // Get first value from percents array since we only support one quantile argument for now.
            let quantile = percents.first()?;
            // ES percentiles are specified as values between 0 and 100, but we want to convert to 0-1 range for our internal representation.
            query_kwargs.insert("quantile".to_string(), (quantile / 100.0).to_string());
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
        query_info: ElasticDSLQueryInfo,
    ) -> QueryTimestamps {
        // Resolves the actual start and end timestamps into milliseconds for an ElasticSearch query
        // based on the provided query_time and the time range specified in the ES DSL query pattern (if any).
        // If no time range is specified, default to entire history up to query_time.

        let mut start_timestamp: u64 = 0;
        let mut end_timestamp: u64 = query_time;

        let predicate = query_info.predicates.first(); // For now, we only look at the first predicate for time range information. We can extend this to support multiple predicates and more complex logic in the future.

        match predicate {
            Some(Predicate::Range {
                field: _,
                gte: _,
                lte: _,
            }) => {
                // If we have a range predicate, we can try to extract time range information from it.
                // For now, we assume that any range predicate applies to the timestamp field, but we could add more complex logic here to determine which field is the timestamp field.
                debug!(
                    "Found range predicate in query, attempting to extract time range information"
                );
                if let Some(resolved_range) =
                    range_query_to_time_range(predicate.unwrap(), query_time as i64)
                {
                    debug!(
                        "Parsed time range from range predicate: start={} end={}",
                        resolved_range.gte_ms.unwrap_or(0),
                        resolved_range.lte_ms.unwrap_or(0)
                    );
                    start_timestamp = resolved_range.gte_ms.unwrap_or(0) as u64;
                    end_timestamp = resolved_range.lte_ms.unwrap_or(query_time as i64) as u64;
                } else {
                    debug!("Failed to resolve time range from range predicate");
                }
            }
            Some(_) => {
                debug!("First predicate is not a range predicate, so we won't be able to extract time range information from it");
            }
            None => {
                debug!("No predicates found in query, so we won't be able to extract time range information");
            }
        }

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }
}

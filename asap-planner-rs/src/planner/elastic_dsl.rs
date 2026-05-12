use asap_types::enums::{CleanupPolicy, WindowType};
use elastic_dsl_utilities::ast_parsing::{
    extract_query_info, AggregationType as ElasticAggregationType,
};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, QueryTreatmentType, Statistic};

use crate::config::input::SketchParameterOverrides;
use crate::error::ControllerError;
use crate::planner::agg_config::{build_agg_configs_for_statistics, IntermediateAggConfig};
use crate::planner::cleanup::get_sql_cleanup_param;
use crate::planner::sketch::build_sketch_parameters;
use crate::planner::window::IntermediateWindowConfig;
use crate::StreamingEngine;

pub struct ElasticSingleQueryProcessor {
    query_string: String,
    t_repeat: u64,
    #[allow(dead_code)]
    data_ingestion_interval: u64,
    index: String,
    #[allow(dead_code)]
    streaming_engine: StreamingEngine,
    sketch_parameters: Option<SketchParameterOverrides>,
    cleanup_policy: CleanupPolicy,
}

impl ElasticSingleQueryProcessor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_string: String,
        t_repeat: u64,
        data_ingestion_interval: u64,
        index: String,
        streaming_engine: StreamingEngine,
        sketch_parameters: Option<SketchParameterOverrides>,
        cleanup_policy: CleanupPolicy,
    ) -> Self {
        Self {
            query_string,
            t_repeat,
            data_ingestion_interval,
            index,
            streaming_engine,
            sketch_parameters,
            cleanup_policy,
        }
    }

    pub fn get_streaming_aggregation_configs(
        &self,
    ) -> Result<(Vec<IntermediateAggConfig>, Option<u64>), ControllerError> {
        // Parse and extract query info using utilities
        let query_info = extract_query_info(&self.query_string).ok_or_else(|| {
            ControllerError::ElasticDSLParse(format!(
                "Failed to parse Elasticsearch DSL query: {}",
                self.query_string
            ))
        })?;

        // Get aggregation type and statistics
        let (treatment_type, statistics) = get_elastic_statistics(&query_info.aggregation)?;

        // Build window config (always tumbling for Elasticsearch queries)
        let window_cfg = IntermediateWindowConfig {
            window_size: self.t_repeat,
            slide_interval: self.t_repeat,
            window_type: WindowType::Tumbling,
        };

        // Extract target field and group by information
        let target_field = query_info.target_field.clone();

        // Determine spatial routing from group_by_buckets
        let (spatial_output, rollup) = match &query_info.group_by_buckets {
            Some(bucket_spec) => {
                let group_fields = get_group_by_fields(bucket_spec);
                let spatial = KeyByLabelNames::new(group_fields);
                // For Elasticsearch, all potentially available fields become rollup
                // when they're not in the group by
                (spatial.clone(), spatial)
            }
            None => (KeyByLabelNames::empty(), KeyByLabelNames::empty()),
        };

        let configs = build_agg_configs_for_statistics(
            &statistics,
            treatment_type,
            &spatial_output,
            &rollup,
            &window_cfg,
            &query_info.target_field,
            Some(&self.index),
            Some(&target_field),
            "", // Elasticsearch doesn't have spatial filters like SQL
            |agg_type: AggregationType, agg_sub_type: &str| {
                build_sketch_parameters(
                    agg_type,
                    agg_sub_type,
                    None,
                    self.sketch_parameters.as_ref(),
                )
            },
        )
        .map_err(ControllerError::ElasticDSLParse)?;

        // Calculate cleanup param based on query's time window
        let t_lookback = self.t_repeat; // Default to repetition delay
        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_sql_cleanup_param(self.cleanup_policy, t_lookback, self.t_repeat)
                    .map_err(ControllerError::PlannerError)?,
            )
        };

        Ok((configs, cleanup_param))
    }
}

/// Map Elasticsearch aggregation types to statistics and treatment types
fn get_elastic_statistics(
    agg_type: &ElasticAggregationType,
) -> Result<(QueryTreatmentType, Vec<Statistic>), ControllerError> {
    match agg_type {
        ElasticAggregationType::Avg => {
            // AVG requires SUM and COUNT
            Ok((
                QueryTreatmentType::Exact,
                vec![Statistic::Sum, Statistic::Count],
            ))
        }
        ElasticAggregationType::Sum => Ok((QueryTreatmentType::Approximate, vec![Statistic::Sum])),
        ElasticAggregationType::Min => Ok((QueryTreatmentType::Exact, vec![Statistic::Min])),
        ElasticAggregationType::Max => Ok((QueryTreatmentType::Exact, vec![Statistic::Max])),
        ElasticAggregationType::Percentiles(percents) => {
            // For percentiles, we use quantile statistic
            // Check that we have valid percentiles
            if percents.is_empty() {
                return Err(ControllerError::UnsupportedElasticDSLQuery(
                    "Percentiles aggregation must specify percentile values".to_string(),
                ));
            }
            Ok((QueryTreatmentType::Approximate, vec![Statistic::Quantile]))
        }
    }
}

/// Extract field names from group by specification
fn get_group_by_fields(
    bucket_spec: &elastic_dsl_utilities::ast_parsing::GroupBySpec,
) -> Vec<String> {
    use elastic_dsl_utilities::ast_parsing::GroupBySpec;
    match bucket_spec {
        GroupBySpec::Fields(fields) => fields.clone(),
        GroupBySpec::Filters(predicates) => {
            // For filter-based grouping, we extract field names from predicates
            let mut fields = Vec::new();
            for predicate in predicates {
                match predicate {
                    elastic_dsl_utilities::ast_parsing::Predicate::Term { field, .. } => {
                        if !fields.contains(field) {
                            fields.push(field.clone());
                        }
                    }
                    elastic_dsl_utilities::ast_parsing::Predicate::Range { field, .. } => {
                        if !fields.contains(field) {
                            fields.push(field.clone());
                        }
                    }
                }
            }
            fields
        }
    }
}

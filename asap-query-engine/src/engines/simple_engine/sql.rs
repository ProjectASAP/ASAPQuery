//! SQL query language handler for SimpleEngine.
//!
//! Contains all SQL-specific context building, pattern matching, and query dispatch.

use super::SimpleEngine;
use super::{QueryExecutionContext, QueryMetadata, QueryTimestamps};
use crate::data_model::{AggregationIdInfo, QueryConfig, SchemaConfig};
use crate::engines::query_result::{InstantVector, InstantVectorElement, QueryResult};
use asap_types::query_requirements::QueryRequirements;
use asap_types::utils::normalize_spatial_filter;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryPatternType, Statistic};
use sql_utilities::ast_matching::QueryType;
use sql_utilities::ast_matching::{SQLPatternMatcher, SQLPatternParser, SQLQuery};
use sql_utilities::sqlhelper::{AggregationInfo, OrderByItem, SQLQueryData};
use sqlparser::dialect::*;
use sqlparser::parser::Parser as parser;
use std::collections::HashMap;
use tracing::{debug, warn};

/// SQL-only post-processing produced alongside a `QueryExecutionContext`:
/// rules for ordering and truncating the final result vector.
///
/// Lives outside `QueryExecutionContext` so that PromQL/Elastic engines —
/// which share that context but have no SQL-level ORDER BY / LIMIT — never
/// have to know about these fields.
#[derive(Debug, Clone, Default)]
pub struct SqlPostProcessing {
    /// Alias of the aggregate function in SELECT, e.g. `agg(v) AS p99`.
    /// Used so `ORDER BY p99` resolves to `element.value`.
    pub aggregation_alias: Option<String>,
    /// `ORDER BY` items in source order. Empty when no ORDER BY clause is present.
    pub order_by: Vec<OrderByItem>,
    /// `LIMIT N`. None when no LIMIT clause is present.
    pub limit: Option<u64>,
}

impl SqlPostProcessing {
    fn from_query_data(query_data: &SQLQueryData) -> Self {
        Self {
            aggregation_alias: query_data.aggregation_alias.clone(),
            order_by: query_data.order_by.clone(),
            limit: query_data.limit,
        }
    }

    /// Returns `true` when there's no ordering or truncation to apply, so the
    /// caller can short-circuit any deconstruction of the result.
    fn is_noop(&self) -> bool {
        self.order_by.is_empty() && self.limit.is_none()
    }

    /// Apply ORDER BY + LIMIT to a `QueryResult`. Only `QueryResult::Vector`
    /// is rewritten; matrices pass through unchanged (range queries don't
    /// flow through `handle_query_sql`).
    pub fn apply(&self, output_labels: &KeyByLabelNames, result: QueryResult) -> QueryResult {
        if self.is_noop() {
            return result;
        }
        match result {
            QueryResult::Vector(InstantVector { values, timestamp }) => {
                let values = sort_and_truncate_instant_vector(
                    values,
                    &output_labels.labels,
                    self.aggregation_alias.as_deref(),
                    &self.order_by,
                    self.limit,
                );
                QueryResult::Vector(InstantVector { values, timestamp })
            }
            other => other,
        }
    }
}

/// Sort and truncate a `Vec<InstantVectorElement>` per `ORDER BY` / `LIMIT`.
///
/// Each `OrderByItem.column` resolves to either:
///   * the aggregate alias → compare by `element.value`
///   * a `label_names` entry → compare lexicographically by `element.labels[idx]`
///
/// Items that don't match either category are silently skipped (the SQL parser
/// already rejects unknown identifiers, so reaching this branch indicates only
/// a mismatch between schema config and runtime labels). When `order_by` is
/// empty and `limit` is `None`, the result vector is returned unchanged.
fn sort_and_truncate_instant_vector(
    mut results: Vec<InstantVectorElement>,
    label_names: &[String],
    aggregation_alias: Option<&str>,
    order_by: &[OrderByItem],
    limit: Option<u64>,
) -> Vec<InstantVectorElement> {
    if !order_by.is_empty() {
        // Pre-resolve each ORDER BY key once. KeyByLabelNames::new sorts the names
        // alphabetically and InstantVectorElement.labels is parallel to that vector,
        // so positional indexing is sound.
        let resolved: Vec<(Option<usize>, bool)> = order_by
            .iter()
            .filter_map(|item| {
                if aggregation_alias == Some(item.column.as_str()) {
                    Some((None, item.ascending))
                } else {
                    label_names
                        .iter()
                        .position(|n| n == &item.column)
                        .map(|i| (Some(i), item.ascending))
                }
            })
            .collect();

        results.sort_by(|a, b| {
            for &(target, asc) in &resolved {
                let ord = match target {
                    None => a
                        .value
                        .partial_cmp(&b.value)
                        .unwrap_or(std::cmp::Ordering::Equal),
                    Some(idx) => {
                        let av = a.labels.labels.get(idx).map(String::as_str).unwrap_or("");
                        let bv = b.labels.labels.get(idx).map(String::as_str).unwrap_or("");
                        av.cmp(bv)
                    }
                };
                let ord = if asc { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(limit) = limit {
        results.truncate(limit as usize);
    }

    results
}

impl SimpleEngine {
    /// Finds the query configuration for a SQL query using structural pattern matching.
    ///
    /// Unlike `find_query_config` (which does exact string comparison), this method parses
    /// each template in query_configs and compares it structurally against the incoming
    /// query_data — ignoring absolute timestamps and comparing only metric, aggregation,
    /// labels, time column name, and duration.
    fn find_query_config_sql(&self, query_data: &SQLQueryData) -> Option<QueryConfig> {
        let ic = self.inference_config.read().unwrap();
        let schema = match &ic.schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            _ => return None,
        };

        ic.query_configs
            .iter()
            .find(|config| {
                let template_statements =
                    match parser::parse_sql(&GenericDialect {}, config.query.as_str()) {
                        Ok(stmts) => stmts,
                        Err(_) => return false,
                    };
                let template_data =
                    match SQLPatternParser::new(&schema, 0.0).parse_query(&template_statements) {
                        Some(data) => data,
                        None => return false,
                    };
                query_data.matches_sql_pattern(&template_data)
            })
            .cloned()
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
                let duration_secs = match_result
                    .outer_data()
                    .expect("OnlyTemporal pattern guarantees outer_data is present")
                    .time_info
                    .clone()
                    .get_duration() as u64;
                end_timestamp - (duration_secs * 1000)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                let duration_secs = match_result
                    .inner_data()
                    .expect("OneTemporalOneSpatial pattern guarantees inner_data is present")
                    .time_info
                    .clone()
                    .get_duration() as u64;
                end_timestamp - (duration_secs * 1000)
            }
            QueryPatternType::OnlySpatial => {
                end_timestamp - (self.prometheus_scrape_interval * 1000)
            }
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

    /// Extracts quantile parameter from SQL match result
    fn extract_quantile_param_sql(&self, match_result: &SQLQuery) -> Option<String> {
        match_result
            .query_data
            .first()
            .map(|data| data.aggregation_info.get_args()[0].to_string())
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

    /// Extract QueryRequirements from a parsed SQL match result.
    /// Used as the fallback path when no query_configs entry is found.
    fn build_query_requirements_sql(
        &self,
        match_result: &SQLQuery,
        query_pattern_type: QueryPatternType,
    ) -> QueryRequirements {
        let query_data = match_result
            .outer_data()
            .expect("build_query_requirements_sql called on valid SQLQuery");
        let metric = query_data.metric.clone();

        let statistic_name = match query_pattern_type {
            QueryPatternType::OneTemporalOneSpatial => match_result
                .inner_data()
                .expect("OneTemporalOneSpatial pattern guarantees inner_data is present")
                .aggregation_info
                .get_name()
                .to_lowercase(),
            _ => query_data.aggregation_info.get_name().to_lowercase(),
        };

        let statistics: Vec<Statistic> = Self::parse_single_statistic(&statistic_name)
            .into_iter()
            .collect();

        let data_range_ms = match query_pattern_type {
            QueryPatternType::OnlySpatial => None,
            QueryPatternType::OnlyTemporal => {
                let duration_secs = query_data.time_info.clone().get_duration() as u64;
                Some(duration_secs * 1000)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                let duration_secs = match_result
                    .inner_data()
                    .expect("OneTemporalOneSpatial pattern guarantees inner_data is present")
                    .time_info
                    .clone()
                    .get_duration() as u64;
                Some(duration_secs * 1000)
            }
        };

        let grouping_labels = KeyByLabelNames::new(query_data.labels.clone().into_iter().collect());

        QueryRequirements {
            metric,
            statistics,
            data_range_ms,
            grouping_labels,
            spatial_filter_normalized: normalize_spatial_filter(""),
        }
    }

    pub fn handle_query_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let (context, post) =
            self.build_query_execution_context_sql_with_post_processing(query, time)?;
        let (output_labels, result) = self.execute_context(context, false)?;
        let result = post.apply(&output_labels, result);
        Some((output_labels, result))
    }

    /// Public entry point retained for tests that only need the execution
    /// context (e.g. assertions on `agg_info` or `metadata`). Discards the
    /// SQL post-processing side-channel since it isn't applied without a
    /// `QueryResult` to operate on.
    pub fn build_query_execution_context_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        self.build_query_execution_context_sql_with_post_processing(query, time)
            .map(|(ctx, _)| ctx)
    }

    /// Internal: parses + plans a SQL query and returns both the execution
    /// context (shared with PromQL/Elastic engines) and the SQL-only
    /// post-processing rules (ORDER BY / LIMIT / alias resolution).
    fn build_query_execution_context_sql_with_post_processing(
        &self,
        query: String,
        time: f64,
    ) -> Option<(QueryExecutionContext, SqlPostProcessing)> {
        // Get SQL schema from inference config
        let schema = match &self.inference_config.read().unwrap().schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => {
                warn!("SQL query requested but config has PromQL schema");
                return None;
            }
            &SchemaConfig::ElasticQueryDSL(_) => todo!(),
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

        // ORDER BY / LIMIT / aggregate alias are presentational and SQL-specific.
        // They live alongside the (engine-shared) `QueryExecutionContext` rather than
        // inside it. Built once here from the parsed `query_data` and returned with
        // every successful path below.
        let post = SqlPostProcessing::from_query_data(&query_data);

        // Handle SpatioTemporal queries separately - they bypass QueryPatternType mapping
        if match_result.query_type == vec![QueryType::SpatioTemporal] {
            let query_time = Self::convert_query_time_to_data_time(
                query_data.time_info.get_start() + query_data.time_info.get_duration(),
            );
            let ctx = self.build_spatiotemporal_context(&match_result, query_time, &query_data)?;
            return Some((ctx, post));
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
                _ => return None,
            },
            _ => return None,
        };

        // For nested queries (spatial of temporal), the outer query has no time clause,
        // so we need to use the inner (temporal) query's time_info to compute query_time
        let query_time = match query_pattern_type {
            QueryPatternType::OneTemporalOneSpatial => {
                let inner_time_info = &match_result.inner_data()?.time_info;
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
                let labels = &match_result.outer_data()?.labels;

                KeyByLabelNames::new(labels.clone().into_iter().collect())
            }
            2 => {
                // Extract spatial aggregation output labels using AST-based approach
                let temporal_labels = &match_result.inner_data()?.labels;
                let spatial_labels = &match_result.outer_data()?.labels;

                let temporal_aggregation = &match_result.inner_data()?.aggregation_info;
                let spatial_aggregation = &match_result.outer_data()?.aggregation_info;

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
                match_result
                    .outer_data()?
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
            QueryPatternType::OneTemporalOneSpatial => {
                // Use the temporal aggregation (second subquery contains temporal)
                match_result
                    .inner_data()?
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
            QueryPatternType::OnlySpatial => {
                // Use the spatial aggregation (first subquery)
                match_result
                    .outer_data()?
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
        };

        let statistic_to_compute = Self::parse_single_statistic(&statistic_name)?;

        let query_kwargs = self
            .build_query_kwargs_sql(&statistic_to_compute, &match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        // Create query metadata
        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Time
        let timestamps =
            self.calculate_query_timestamps_sql(query_time, query_pattern_type, &match_result);

        // Resolve aggregation: try pre-configured query_configs first, fall back to capability matching.
        let agg_info: AggregationIdInfo = if let Some(config) =
            self.find_query_config_sql(&query_data)
        {
            self.get_aggregation_id_info(&config)
                .map_err(|e| {
                    warn!("{}", e);
                    e
                })
                .ok()?
        } else {
            warn!("No query_config entry for SQL query. Attempting capability-based matching.");
            let requirements = self.build_query_requirements_sql(&match_result, query_pattern_type);
            self.streaming_config
                .read()
                .unwrap()
                .clone()
                .find_compatible_aggregation(&requirements)?
        };

        let metric = &match_result.outer_data()?.metric;

        let spatial_filter = if query_pattern_type == QueryPatternType::OneTemporalOneSpatial {
            match_result
                .outer_data()?
                .labels
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        } else {
            String::new()
        };

        let do_merge = query_pattern_type == QueryPatternType::OnlyTemporal
            || query_pattern_type == QueryPatternType::OneTemporalOneSpatial;

        let ctx = self.build_sql_execution_context_tail(
            metric,
            &timestamps,
            metadata,
            agg_info,
            do_merge,
            spatial_filter,
            query_time,
        )?;
        Some((ctx, post))
    }

    /// Shared context-building tail for both SQL context builders.
    ///
    /// Called by `build_query_execution_context_sql` and `build_spatiotemporal_context`
    /// after labels, statistic, metadata, timestamps, and `agg_info` are resolved.
    /// Builds the query plan, derives grouping/aggregated labels, and returns the
    /// final `QueryExecutionContext`.
    #[allow(clippy::too_many_arguments)]
    fn build_sql_execution_context_tail(
        &self,
        metric: &str,
        timestamps: &QueryTimestamps,
        metadata: QueryMetadata,
        agg_info: AggregationIdInfo,
        do_merge: bool,
        spatial_filter: String,
        query_time: u64,
    ) -> Option<QueryExecutionContext> {
        let query_plan = self
            .create_store_query_plan(metric, timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        let sc = self.streaming_config.read().unwrap().clone();
        let grouping_labels = sc
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| metadata.query_output_labels.clone());

        let aggregated_labels = sc
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric: metric.to_string(),
            metadata,
            store_plan: query_plan,
            agg_info,
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })
    }

    /// Build execution context for SpatioTemporal queries.
    /// These queries span multiple scrape intervals but GROUP BY a subset of labels.
    fn build_spatiotemporal_context(
        &self,
        match_result: &SQLQuery,
        query_time: u64,
        query_data: &SQLQueryData,
    ) -> Option<QueryExecutionContext> {
        // Output labels are the GROUP BY columns (subset of all labels)
        let query_output_labels = KeyByLabelNames::new(
            match_result
                .outer_data()?
                .labels
                .clone()
                .into_iter()
                .collect(),
        );

        // Get the statistic from the aggregation
        let statistic_name = match_result
            .outer_data()?
            .aggregation_info
            .get_name()
            .to_lowercase();

        let statistic_to_compute = Self::parse_single_statistic(&statistic_name)?;

        let query_kwargs = self
            .build_query_kwargs_sql(&statistic_to_compute, match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Calculate timestamps - similar to OnlyTemporal
        let end_timestamp =
            self.validate_and_align_end_timestamp(query_time, QueryPatternType::OnlyTemporal);
        let duration_secs = match_result.outer_data()?.time_info.get_duration() as u64;
        let start_timestamp = end_timestamp - (duration_secs * 1000);

        let timestamps = QueryTimestamps {
            start_timestamp,
            end_timestamp,
        };

        // Resolve aggregation: try pre-configured query_configs first, fall back to capability matching.
        let agg_info: AggregationIdInfo = if let Some(config) =
            self.find_query_config_sql(query_data)
        {
            self.get_aggregation_id_info(&config)
                .map_err(|e| {
                    warn!("{}", e);
                    e
                })
                .ok()?
        } else {
            warn!(
                    "No query_config entry for SQL spatio-temporal query. Attempting capability-based matching."
                );
            let requirements =
                self.build_query_requirements_sql(match_result, QueryPatternType::OnlyTemporal);
            self.streaming_config
                .read()
                .unwrap()
                .clone()
                .find_compatible_aggregation(&requirements)?
        };
        let metric = &match_result.outer_data()?.metric;

        self.build_sql_execution_context_tail(
            metric,
            &timestamps,
            metadata,
            agg_info,
            true,
            String::new(),
            query_time,
        )
    }
}

#[cfg(test)]
mod sort_and_truncate_tests {
    use super::sort_and_truncate_instant_vector;
    use super::SqlPostProcessing;
    use crate::data_model::KeyByLabelValues;
    use crate::engines::query_result::{InstantVector, InstantVectorElement, QueryResult};
    use promql_utilities::data_model::KeyByLabelNames;
    use sql_utilities::sqlhelper::OrderByItem;

    fn elem(labels: &[&str], value: f64) -> InstantVectorElement {
        InstantVectorElement {
            labels: KeyByLabelValues::new_with_labels(
                labels.iter().map(|s| s.to_string()).collect(),
            ),
            value,
        }
    }

    fn label_names(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn no_orderby_no_limit_returns_unchanged() {
        let input = vec![elem(&["a"], 1.0), elem(&["b"], 2.0)];
        let result =
            sort_and_truncate_instant_vector(input.clone(), &label_names(&["L"]), None, &[], None);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].labels.labels, input[0].labels.labels);
        assert_eq!(result[1].labels.labels, input[1].labels.labels);
    }

    #[test]
    fn order_by_aggregate_desc_with_limit() {
        // Mirrors the user's netflow query shape: ORDER BY <agg alias> DESC LIMIT N.
        // Build 5 rows with values 1..=5 and assert top-3 in descending order.
        let input = vec![
            elem(&["a"], 1.0),
            elem(&["b"], 5.0),
            elem(&["c"], 3.0),
            elem(&["d"], 2.0),
            elem(&["e"], 4.0),
        ];
        let order_by = vec![OrderByItem {
            column: "p99".to_string(),
            ascending: false,
        }];
        let result = sort_and_truncate_instant_vector(
            input,
            &label_names(&["L"]),
            Some("p99"),
            &order_by,
            Some(3),
        );
        assert_eq!(result.len(), 3);
        let values: Vec<f64> = result.iter().map(|e| e.value).collect();
        assert_eq!(values, vec![5.0, 4.0, 3.0]);
    }

    #[test]
    fn order_by_label_ascending_default() {
        // ORDER BY <group-by column> with no ASC/DESC defaults to ascending.
        let input = vec![elem(&["c"], 1.0), elem(&["a"], 2.0), elem(&["b"], 3.0)];
        let order_by = vec![OrderByItem {
            column: "L".to_string(),
            ascending: true,
        }];
        let result =
            sort_and_truncate_instant_vector(input, &label_names(&["L"]), None, &order_by, None);
        let labels: Vec<&str> = result.iter().map(|e| e.labels.labels[0].as_str()).collect();
        assert_eq!(labels, vec!["a", "b", "c"]);
    }

    #[test]
    fn order_by_multi_key_uses_secondary_for_ties() {
        // Primary: L1 ASC. Secondary: value DESC. Tied L1 values should be
        // broken by descending value. labels are [L1, L2] alphabetical ⇒ index 0 = L1.
        let input = vec![
            elem(&["x", "i"], 1.0),
            elem(&["x", "j"], 5.0),
            elem(&["a", "k"], 3.0),
            elem(&["a", "l"], 7.0),
        ];
        let order_by = vec![
            OrderByItem {
                column: "L1".to_string(),
                ascending: true,
            },
            OrderByItem {
                column: "p99".to_string(),
                ascending: false,
            },
        ];
        let result = sort_and_truncate_instant_vector(
            input,
            &label_names(&["L1", "L2"]),
            Some("p99"),
            &order_by,
            None,
        );
        let expected: Vec<(&str, f64)> = vec![("a", 7.0), ("a", 3.0), ("x", 5.0), ("x", 1.0)];
        let actual: Vec<(&str, f64)> = result
            .iter()
            .map(|e| (e.labels.labels[0].as_str(), e.value))
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn limit_only_no_orderby_truncates_in_place() {
        let input = vec![elem(&["a"], 1.0), elem(&["b"], 2.0), elem(&["c"], 3.0)];
        let result =
            sort_and_truncate_instant_vector(input, &label_names(&["L"]), None, &[], Some(2));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].labels.labels[0], "a");
        assert_eq!(result[1].labels.labels[0], "b");
    }

    #[test]
    fn nan_values_do_not_panic() {
        // partial_cmp returns None for NaN; we map to Equal to keep the comparator total.
        let input = vec![elem(&["a"], f64::NAN), elem(&["b"], 1.0), elem(&["c"], 2.0)];
        let order_by = vec![OrderByItem {
            column: "p99".to_string(),
            ascending: false,
        }];
        let result = sort_and_truncate_instant_vector(
            input,
            &label_names(&["L"]),
            Some("p99"),
            &order_by,
            None,
        );
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn sql_post_processing_default_is_noop() {
        // Default == no ORDER BY, no LIMIT, no alias. apply() must hand back the
        // exact QueryResult unchanged (no allocation, no reorder).
        let post = SqlPostProcessing::default();
        let input = vec![elem(&["c"], 3.0), elem(&["a"], 1.0), elem(&["b"], 2.0)];
        let labels = KeyByLabelNames::new(vec!["L".to_string()]);
        let result = QueryResult::Vector(InstantVector {
            values: input.clone(),
            timestamp: 1234,
        });
        let out = post.apply(&labels, result);
        let QueryResult::Vector(v) = out else {
            panic!("expected vector");
        };
        let values: Vec<&str> = v
            .values
            .iter()
            .map(|e| e.labels.labels[0].as_str())
            .collect();
        assert_eq!(values, vec!["c", "a", "b"]);
        assert_eq!(v.timestamp, 1234);
    }

    #[test]
    fn sql_post_processing_applies_orderby_desc_limit() {
        // End-to-end check at the SqlPostProcessing layer: the wrapper unpacks
        // the vector, sorts and truncates, and re-wraps preserving timestamp.
        let post = SqlPostProcessing {
            aggregation_alias: Some("p99".to_string()),
            order_by: vec![OrderByItem {
                column: "p99".to_string(),
                ascending: false,
            }],
            limit: Some(2),
        };
        let labels = KeyByLabelNames::new(vec!["L".to_string()]);
        let input = vec![
            elem(&["a"], 1.0),
            elem(&["b"], 5.0),
            elem(&["c"], 3.0),
            elem(&["d"], 2.0),
        ];
        let result = QueryResult::Vector(InstantVector {
            values: input,
            timestamp: 9999,
        });
        let out = post.apply(&labels, result);
        let QueryResult::Vector(v) = out else {
            panic!("expected vector");
        };
        assert_eq!(v.timestamp, 9999);
        let values: Vec<f64> = v.values.iter().map(|e| e.value).collect();
        assert_eq!(values, vec![5.0, 3.0]);
    }
}

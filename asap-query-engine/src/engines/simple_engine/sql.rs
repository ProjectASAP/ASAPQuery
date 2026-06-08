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

/// How a top-k query weights each observation fed into the heavy-hitter sketch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TopkWeighting {
    /// `COUNT(col)`: every matching row contributes weight 1, so the heap ranks
    /// keys by event frequency (`count_events: true`).
    Count,
    /// `SUM(col)`: every matching row contributes weight = `col`, so the heap
    /// ranks keys by summed value (`count_events: false`).
    ///
    /// Assumes **non-negative** summands: `CountMinSketch` is a frequency sketch
    /// and cannot represent negative weights, so a `SUM` over a column that can
    /// go negative would produce meaningless estimates.
    Sum,
}

/// A detected SQL top-k query: the `LIMIT k` plus how the sketch is weighted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqlTopk {
    pub k: u64,
    pub weighting: TopkWeighting,
}

impl SqlTopk {
    /// `count_events` flag the backing `CountMinSketchWithHeap` must use:
    /// `true` for COUNT (unit weight), `false` for SUM (value weight).
    pub fn count_events(&self) -> bool {
        matches!(self.weighting, TopkWeighting::Count)
    }
}

/// Detect a SQL top-k query and return its `k` plus sketch weighting.
///
/// Recognises the heavy-hitter shape that `CountMinSketchWithHeap` serves:
///
/// ```sql
/// SELECT <key>, COUNT(<col>) AS <alias>   -- or SUM(<col>)
/// FROM <table> WHERE <1s window>
/// GROUP BY <key>
/// ORDER BY <alias> DESC
/// LIMIT k
/// ```
///
/// The grouping key (`<key>`) becomes the *aggregated* dimension inside the
/// sketch's heap — not a precompute partition key — so a single sketch per
/// window tracks the top keys by event count (COUNT) or summed value (SUM).
///
/// The SQL parser only accepts identifier ORDER BY targets, so the descending
/// order must reference the aggregate's alias (e.g. `transfer_events`), not the
/// `COUNT(col)` / `SUM(col)` expression itself.
pub(crate) fn detect_sql_topk(query_data: &SQLQueryData) -> Option<SqlTopk> {
    let k = query_data.limit?;
    // Need a GROUP BY key to rank and an ORDER BY to define "top".
    if query_data.labels.is_empty() || query_data.order_by.is_empty() {
        return None;
    }
    // CountMinSketchWithHeap tracks heavy hitters by COUNT (unit weight) or
    // SUM (value weight). Any other aggregate (MIN/MAX/quantile/...) cannot be
    // served by the additive frequency sketch.
    let name = query_data.aggregation_info.get_name();
    let weighting = if name.eq_ignore_ascii_case("count") {
        TopkWeighting::Count
    } else if name.eq_ignore_ascii_case("sum") {
        TopkWeighting::Sum
    } else {
        return None;
    };
    // Primary ordering must be the aggregate alias, descending (largest first).
    let primary = &query_data.order_by[0];
    if primary.ascending {
        return None;
    }
    if query_data.aggregation_alias.as_deref() != Some(primary.column.as_str()) {
        return None;
    }
    Some(SqlTopk { k, weighting })
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
        topk: Option<SqlTopk>,
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

        // For top-k the requirement is `Statistic::Topk` (→ CountMinSketchWithHeap)
        // and the grouping is empty: the GROUP BY column is the sketch's
        // *aggregated* (heavy-hitter) dimension, held inside one sketch per
        // window, not a precompute partition key.
        let is_topk = topk.is_some();
        let statistics: Vec<Statistic> = if is_topk {
            vec![Statistic::Topk]
        } else {
            Self::parse_single_statistic(&statistic_name)
                .into_iter()
                .collect()
        };

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

        let grouping_labels = if is_topk {
            KeyByLabelNames::empty()
        } else {
            KeyByLabelNames::new(query_data.labels.clone().into_iter().collect())
        };

        QueryRequirements {
            metric,
            statistics,
            data_range_ms,
            grouping_labels,
            spatial_filter_normalized: normalize_spatial_filter(""),
            // COUNT top-k needs a `count_events: true` sketch; SUM top-k needs a
            // `count_events: false` (value-weighted) one. This disambiguates two
            // CountMinSketchWithHeap configs on the same metric during matching.
            topk_count_events: topk.map(|t| t.count_events()),
        }
    }

    pub fn handle_query_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let (context, post) =
            self.build_query_execution_context_sql_with_post_processing(query, time)?;
        let is_topk = context.metadata.statistic_to_compute == Statistic::Topk;
        // Top-k: enable heap-based limiting (truncate to k) but NOT PromQL-style
        // metric-name formatting; the sketch heap already produces the ranked
        // `(group-by key, count)` rows, so SQL ORDER BY / LIMIT post-processing
        // would be redundant and is skipped.
        let (output_labels, result) = self.execute_context(context, is_topk, false)?;
        let result = if is_topk {
            result
        } else {
            post.apply(&output_labels, result)
        };
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

        // Top-k (CountMinSketchWithHeap) applies to flat single-layer queries:
        // COUNT/SUM ... GROUP BY <key> ORDER BY <agg alias> DESC LIMIT k.
        // Nested patterns attach ORDER BY / LIMIT to the outer SELECT; `query_data`
        // from parse is the outer layer, while the temporal aggregate lives in
        // `inner_data` for OneTemporalOneSpatial. Running detect_sql_topk on the
        // outer layer would mis-classify spatial rollups as top-k.
        //
        // Single-interval windows (duration == scrape interval) classify as
        // `OnlySpatial` in the pattern matcher even though they are flat temporal
        // reads, so both `OnlyTemporal` and `OnlySpatial` must run detection.
        let topk = match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OnlySpatial => {
                detect_sql_topk(&query_data)
            }
            QueryPatternType::OneTemporalOneSpatial => None,
        };
        if topk.is_some_and(|t| t.weighting == TopkWeighting::Sum) {
            warn!(
                "SUM top-k assumes non-negative values; results are undefined for columns with negative entries"
            );
        }
        let statistic_to_compute = if topk.is_some() {
            Statistic::Topk
        } else {
            Self::parse_single_statistic(&statistic_name)?
        };

        let mut query_kwargs = self
            .build_query_kwargs_sql(&statistic_to_compute, &match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;
        if let Some(topk) = topk {
            query_kwargs.insert("k".to_string(), topk.k.to_string());
        }

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
        let agg_info: AggregationIdInfo =
            if let Some(config) = self.find_query_config_sql(&query_data) {
                self.get_aggregation_id_info(&config)
                    .map_err(|e| {
                        warn!("{}", e);
                        e
                    })
                    .ok()?
            } else {
                warn!("No query_config entry for SQL query. Attempting capability-based matching.");
                let requirements =
                    self.build_query_requirements_sql(&match_result, query_pattern_type, topk);
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
            let requirements = self.build_query_requirements_sql(
                match_result,
                QueryPatternType::OnlyTemporal,
                None,
            );
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
mod detect_topk_tests {
    use super::{detect_sql_topk, SqlTopk, TopkWeighting};
    use sql_utilities::ast_matching::SQLPatternParser;
    use sql_utilities::sqlhelper::{SQLSchema, Table};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashSet;

    /// Parse a SQL string into `SQLQueryData` against a netflow-shaped schema.
    /// Returns `None` if the parser rejects the query (e.g. unsupported ORDER BY).
    fn parse(sql: &str) -> Option<sql_utilities::sqlhelper::SQLQueryData> {
        let value_cols: HashSet<String> = ["pkt_len"].iter().map(|s| s.to_string()).collect();
        let labels: HashSet<String> = ["srcip", "dstip", "proto"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let table = Table::new(
            "netflow_table".to_string(),
            "time".to_string(),
            value_cols,
            labels,
        );
        let schema = SQLSchema::new(vec![table]);
        let statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
        SQLPatternParser::new(&schema, 0.0).parse_query(&statements)
    }

    const WINDOW: &str =
        "WHERE time BETWEEN DATEADD(s, -1, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10'";

    #[test]
    fn count_order_by_alias_desc_limit_is_topk() {
        let sql = format!(
            "SELECT srcip, COUNT(pkt_len) AS transfer_events FROM netflow_table {WINDOW} \
             GROUP BY srcip ORDER BY transfer_events DESC LIMIT 10"
        );
        let qd = parse(&sql).expect("valid topk query should parse");
        assert_eq!(
            detect_sql_topk(&qd),
            Some(SqlTopk {
                k: 10,
                weighting: TopkWeighting::Count,
            }),
            "COUNT top-k must use unit (count_events) weighting",
        );
    }

    #[test]
    fn sum_order_by_alias_desc_limit_is_topk() {
        let sql = format!(
            "SELECT srcip, SUM(pkt_len) AS total FROM netflow_table {WINDOW} \
             GROUP BY srcip ORDER BY total DESC LIMIT 10"
        );
        let qd = parse(&sql).expect("valid sum top-k query should parse");
        let detected = detect_sql_topk(&qd).expect("SUM ORDER BY DESC LIMIT is top-k");
        assert_eq!(detected.k, 10);
        assert_eq!(
            detected.weighting,
            TopkWeighting::Sum,
            "SUM top-k must use value (count_events=false) weighting",
        );
        assert!(
            !detected.count_events(),
            "SUM top-k maps to a count_events: false sketch",
        );
    }

    #[test]
    fn missing_limit_is_not_topk() {
        let sql = format!(
            "SELECT srcip, COUNT(pkt_len) AS transfer_events FROM netflow_table {WINDOW} \
             GROUP BY srcip ORDER BY transfer_events DESC"
        );
        let qd = parse(&sql).expect("query should parse");
        assert_eq!(detect_sql_topk(&qd), None, "no LIMIT ⇒ not top-k");
    }

    #[test]
    fn ascending_order_is_not_topk() {
        let sql = format!(
            "SELECT srcip, COUNT(pkt_len) AS transfer_events FROM netflow_table {WINDOW} \
             GROUP BY srcip ORDER BY transfer_events ASC LIMIT 10"
        );
        let qd = parse(&sql).expect("query should parse");
        assert_eq!(
            detect_sql_topk(&qd),
            None,
            "ASC ordering is bottom-k, not top-k"
        );
    }

    #[test]
    fn no_order_by_is_not_topk() {
        let sql = format!(
            "SELECT srcip, COUNT(pkt_len) AS transfer_events FROM netflow_table {WINDOW} \
             GROUP BY srcip LIMIT 10"
        );
        let qd = parse(&sql).expect("query should parse");
        assert_eq!(
            detect_sql_topk(&qd),
            None,
            "LIMIT without ORDER BY is not top-k"
        );
    }

    #[test]
    fn min_aggregate_is_not_topk() {
        // Only the additive sketch-friendly aggregates (COUNT/SUM) are top-k;
        // MIN/MAX/quantile cannot be served by CountMinSketchWithHeap.
        let sql = format!(
            "SELECT srcip, MIN(pkt_len) AS smallest FROM netflow_table {WINDOW} \
             GROUP BY srcip ORDER BY smallest DESC LIMIT 10"
        );
        let qd = parse(&sql).expect("query should parse");
        assert_eq!(
            detect_sql_topk(&qd),
            None,
            "only COUNT/SUM map to CMS-with-heap top-k"
        );
    }

    #[test]
    fn order_by_group_key_is_not_topk() {
        // Ordering by the group-by key (not the count) is a plain sorted listing.
        let sql = format!(
            "SELECT srcip, COUNT(pkt_len) AS transfer_events FROM netflow_table {WINDOW} \
             GROUP BY srcip ORDER BY srcip DESC LIMIT 10"
        );
        let qd = parse(&sql).expect("query should parse");
        assert_eq!(detect_sql_topk(&qd), None);
    }

    #[test]
    fn nested_outer_layer_would_match_detect_sql_topk() {
        // Spatial-over-temporal: ORDER BY / LIMIT sit on the outer SELECT, so the
        // parsed top-level `query_data` looks like SUM top-k even though the temporal
        // aggregate is in the subquery. The engine must not promote this to Topk.
        let sql = format!(
            "SELECT srcip, SUM(bytes) AS rollup FROM ( \
               SELECT srcip, dstip, SUM(pkt_len) AS bytes FROM netflow_table {WINDOW} \
               GROUP BY srcip, dstip \
             ) sub GROUP BY srcip ORDER BY rollup DESC LIMIT 10"
        );
        let qd = parse(&sql).expect("nested query should parse");
        assert!(
            detect_sql_topk(&qd).is_some(),
            "outer SELECT alone matches the top-k shape (this is why OneTemporalOneSpatial is gated)",
        );
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

/// End-to-end tests for SQL top-k queries served by `CountMinSketchWithHeap`.
///
/// Exercises the full path for `SELECT srcip, COUNT(pkt_len) AS k FROM
/// netflow_table WHERE <1s window> GROUP BY srcip ORDER BY k DESC LIMIT n`:
///   * SQL detection promotes it to `Statistic::Topk`.
///   * The single `CountMinSketchWithHeap` aggregation resolves self-keyed
///     (key id == value id), so the sketch heap enumerates candidate `srcip`s.
///   * The pipeline sorts by count descending and truncates to `n`, without
///     PromQL-style metric-name prefixing (rows stay bare `(srcip, count)`).
///
/// Lives here alongside `detect_topk_tests` / `sort_and_truncate_tests` so all
/// SQL top-k coverage is co-located in the SQL handler. Unlike those pure-fn
/// modules this one builds a real `SimpleEngine` + store and runs the pipeline,
/// since the top-k execution path skips `SqlPostProcessing::apply` (its ordering
/// happens in `format_final_results` and truncation in `execute_query_pipeline`).
#[cfg(test)]
mod topk_pipeline_tests {
    use super::SimpleEngine;
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        PrecomputedOutput, QueryConfig, QueryLanguage, SchemaConfig, StreamingConfig, WindowType,
    };
    use crate::precompute_operators::CountMinSketchWithHeapAccumulator;
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;
    use sql_utilities::sqlhelper::{SQLSchema, Table};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    const AGG_ID: u64 = 101;
    const METRIC: &str = "netflow_table";
    // '2025-10-01 00:00:10' (UTC) in seconds.
    const QUERY_TIME: f64 = 1_759_276_810.0;

    /// Build a SQL engine whose only aggregation is a self-keyed
    /// `CountMinSketchWithHeap` over `netflow_table`, grouped globally (no
    /// partition labels) and aggregating the `srcip` heavy-hitter dimension.
    /// Returns the engine plus a handle to the shared store for inserting
    /// precomputed sketches.
    fn build_topk_engine() -> (SimpleEngine, Arc<SimpleMapStore>) {
        // Template stored in the inference config. Matches incoming top-k queries
        // structurally (ORDER BY / LIMIT / aliases are ignored by SQL pattern
        // matching), and references a single `CountMinSketchWithHeap` aggregation
        // so the engine resolves it self-keyed.
        let template = "SELECT srcip, COUNT(pkt_len) FROM netflow_table \
             WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY srcip";

        let value_cols: HashSet<String> = ["pkt_len"].iter().map(|s| s.to_string()).collect();
        let labels: HashSet<String> = ["srcip", "dstip", "proto"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let table = Table::new(METRIC.to_string(), "time".to_string(), value_cols, labels);
        let sql_schema = SQLSchema::new(vec![table]);

        let query_config = QueryConfig::new(template.to_string())
            .add_aggregation(AggregationReference::new(AGG_ID, None));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(sql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        let agg_config = AggregationConfig {
            aggregation_id: AGG_ID,
            aggregation_type: AggregationType::CountMinSketchWithHeap,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            // Empty grouping: one global sketch. The GROUP BY column (`srcip`)
            // is the sketch's *aggregated* heavy-hitter dimension, not a
            // precompute partition key.
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::new(vec!["srcip".to_string()]),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size: 1,
            slide_interval: 1,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: METRIC.to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };

        let mut agg_configs = HashMap::new();
        agg_configs.insert(AGG_ID, agg_config);
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: agg_configs,
        });

        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        let engine = SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config,
            1, // 1s scrape interval ⇒ the 1s window classifies as OnlySpatial
            QueryLanguage::sql,
        );
        (engine, store)
    }

    /// Incoming top-k query over a 1-second absolute window.
    fn topk_query(limit: u64) -> String {
        format!(
            "SELECT srcip, COUNT(pkt_len) AS transfer_events FROM netflow_table \
             WHERE time BETWEEN DATEADD(s, -1, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' \
             GROUP BY srcip ORDER BY transfer_events DESC LIMIT {limit}"
        )
    }

    /// Incoming SUM top-k query over a 1-second absolute window.
    fn sum_topk_query(limit: u64) -> String {
        format!(
            "SELECT srcip, SUM(pkt_len) AS total_bytes FROM netflow_table \
             WHERE time BETWEEN DATEADD(s, -1, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' \
             GROUP BY srcip ORDER BY total_bytes DESC LIMIT {limit}"
        )
    }

    /// Build a SQL engine whose only aggregation is a self-keyed, value-weighted
    /// (`count_events: false`) `CountMinSketchWithHeap` over `netflow_table`,
    /// referenced by a single-aggregation `SUM(pkt_len)` query_config. Mirrors
    /// `build_topk_engine` but for SUM top-k, so the engine resolves it
    /// self-keyed via the query_config path (the same path COUNT uses).
    fn build_sum_topk_engine() -> SimpleEngine {
        let template = "SELECT srcip, SUM(pkt_len) FROM netflow_table \
             WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY srcip";

        let value_cols: HashSet<String> = ["pkt_len"].iter().map(|s| s.to_string()).collect();
        let labels: HashSet<String> = ["srcip", "dstip", "proto"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let table = Table::new(METRIC.to_string(), "time".to_string(), value_cols, labels);
        let sql_schema = SQLSchema::new(vec![table]);

        let query_config = QueryConfig::new(template.to_string())
            .add_aggregation(AggregationReference::new(AGG_ID, None));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(sql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        // count_events: false ⇒ the heap is weighted by the summed value rather
        // than the event count (SUM semantics).
        let mut parameters = HashMap::new();
        parameters.insert("count_events".to_string(), serde_json::json!(false));
        let agg_config = AggregationConfig {
            aggregation_id: AGG_ID,
            aggregation_type: AggregationType::CountMinSketchWithHeap,
            aggregation_sub_type: String::new(),
            parameters,
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::new(vec!["srcip".to_string()]),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size: 1,
            slide_interval: 1,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: METRIC.to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };

        let mut agg_configs = HashMap::new();
        agg_configs.insert(AGG_ID, agg_config);
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: agg_configs,
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            1,
            QueryLanguage::sql,
        )
    }

    #[test]
    fn sum_topk_resolves_self_keyed_heap() {
        // SUM(col) ORDER BY DESC LIMIT k is a top-k query and, like COUNT,
        // resolves self-keyed through the single-aggregation query_config path.
        let engine = build_sum_topk_engine();
        let context = engine
            .build_query_execution_context_sql(sum_topk_query(5), QUERY_TIME)
            .expect("SUM top-k should build a context via the query_config path");

        assert_eq!(
            context.metadata.statistic_to_compute,
            Statistic::Topk,
            "SUM ... ORDER BY <alias> DESC LIMIT n must be promoted to Topk",
        );
        assert_eq!(
            context.metadata.query_kwargs.get("k").map(String::as_str),
            Some("5"),
        );
        // Self-keyed: heap supplies both keys and values, so key id == value id
        // and no separate keys query is planned.
        assert_eq!(
            context.agg_info.aggregation_id_for_key,
            context.agg_info.aggregation_id_for_value,
        );
        assert_eq!(context.agg_info.aggregation_id_for_value, AGG_ID);
        assert!(context.store_plan.keys_query.is_none());
    }

    #[test]
    fn detects_topk_and_resolves_self_keyed_heap() {
        let (engine, _store) = build_topk_engine();
        let context = engine
            .build_query_execution_context_sql(topk_query(10), QUERY_TIME)
            .expect("top-k query should build a context via the query_config path");

        assert_eq!(
            context.metadata.statistic_to_compute,
            Statistic::Topk,
            "ORDER BY <count alias> DESC LIMIT n must be promoted to Topk",
        );
        assert_eq!(
            context.metadata.query_kwargs.get("k").map(String::as_str),
            Some("10"),
            "LIMIT should be threaded through as the `k` kwarg",
        );
        // Self-keyed: the heap supplies both keys and counts, so no separate
        // key aggregation / keys query is planned.
        assert_eq!(
            context.agg_info.aggregation_id_for_key,
            context.agg_info.aggregation_id_for_value,
        );
        assert!(context.store_plan.keys_query.is_none());
    }

    #[test]
    fn returns_top_k_srcips_sorted_descending() {
        let (engine, store) = build_topk_engine();

        // Build the context first so we can insert the sketch into exactly the
        // window the store plan will query.
        let context = engine
            .build_query_execution_context_sql(topk_query(10), QUERY_TIME)
            .expect("context should build");
        let window = &context.store_plan.values_query;

        // 15 distinct srcips with strictly increasing counts 10, 20, ... 150.
        // A width-1024 / depth-3 sketch makes collisions among 15 keys
        // effectively impossible, so estimates equal the inserted counts.
        let mut sketch = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        for i in 1..=15u64 {
            let srcip = format!("10.0.0.{i}");
            sketch.inner.update(&srcip, (i * 10) as f64);
        }

        let output =
            PrecomputedOutput::new(window.start_timestamp, window.end_timestamp, None, AGG_ID);
        store
            .insert_precomputed_output(output, Box::new(sketch))
            .expect("insert should succeed");

        // enable_topk_limiting=true (truncate to k via heap), formatting=false
        // (SQL rows stay bare, no __name__ prefix).
        let results = engine
            .execute_query_pipeline(&context, true, false)
            .expect("pipeline should produce results");

        assert_eq!(results.len(), 10, "LIMIT 10 must truncate to 10 rows");

        // Sorted by count descending.
        for pair in results.windows(2) {
            assert!(
                pair[0].value >= pair[1].value,
                "results must be sorted by count descending: {} then {}",
                pair[0].value,
                pair[1].value,
            );
        }

        // Highest count first; bare single-label rows (no metric-name prefix).
        assert_eq!(results[0].labels.labels, vec!["10.0.0.15".to_string()]);
        assert_eq!(results[0].value, 150.0);
        for element in &results {
            assert_eq!(
                element.labels.labels.len(),
                1,
                "SQL top-k rows carry only the GROUP BY column, never a metric prefix",
            );
        }

        // The returned set is exactly the 10 largest srcips (6..=15).
        let returned: HashSet<String> =
            results.iter().map(|e| e.labels.labels[0].clone()).collect();
        let expected: HashSet<String> = (6..=15u64).map(|i| format!("10.0.0.{i}")).collect();
        assert_eq!(returned, expected);
    }
}

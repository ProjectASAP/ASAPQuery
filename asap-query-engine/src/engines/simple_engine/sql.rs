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
use promql_utilities::query_logics::enums::Statistic;
use sql_utilities::ast_matching::{
    detect_sql_topk, SQLPatternMatcher, SQLPatternParser, SQLQuery, SqlTopk, TopkWeighting,
};
use sql_utilities::sqlhelper::{OrderByItem, SQLQueryData};
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

    /// Aligns `end_timestamp` down to the nearest data-ingestion-interval
    /// boundary, unconditionally. Unlike the shared, PromQL-oriented
    /// `validate_and_align_end_timestamp` (which only snaps for
    /// `OnlySpatial`), SQL end timestamps come from explicit `BETWEEN`
    /// clauses and should already be interval-aligned, so this is a no-op
    /// in the common case and a safety net otherwise — for every SQL query
    /// shape, not just a subset.
    fn align_end_timestamp_sql(&self, end_timestamp: u64) -> u64 {
        let interval_ms = self.data_ingestion_interval_ms;
        if end_timestamp.is_multiple_of(interval_ms) {
            return end_timestamp;
        }
        let aligned = (end_timestamp / interval_ms) * interval_ms;
        warn!(
            "SQL query end timestamp {} is not aligned with data ingestion interval of {} ms; \
             aligning down to {}.",
            end_timestamp, interval_ms, aligned
        );
        aligned
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

    /// Extract QueryRequirements from a parsed SQL match result.
    /// Used as the fallback path when no query_configs entry is found.
    ///
    /// `data_range_ms` is always the query's own requested duration: for a
    /// single-scrape-interval query this equals `data_ingestion_interval_ms`
    /// by construction (that's exactly the matcher's classification
    /// boundary), so this is a plain identity, not a special case.
    fn build_query_requirements_sql(
        &self,
        match_result: &SQLQuery,
        topk: Option<SqlTopk>,
    ) -> QueryRequirements {
        let query_data = match_result
            .outer_data()
            .expect("build_query_requirements_sql called on valid SQLQuery");
        let metric = query_data.metric.clone();

        let statistic_name = query_data.aggregation_info.get_name().to_lowercase();

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

        let data_range_ms = (query_data.time_info.clone().get_duration() * 1000.0).round() as u64;

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

        // SQLPatternMatcher (sql_utilities, out of scope for this rename) divides a
        // seconds-denominated SQL query duration by this value — convert back to seconds.
        let matcher =
            SQLPatternMatcher::new(schema, self.data_ingestion_interval_ms as f64 / 1000.0);
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

        // Every valid (non-nested) SQL query is handled uniformly by
        // `build_spatiotemporal_context`, regardless of duration or GROUP BY
        // shape. An unmatched query (e.g. no time column) leaves `match_result`
        // empty, so `outer_data()` inside `build_spatiotemporal_context`
        // returns `None` and this propagates via `?` — no separate check needed.
        let query_time = Self::convert_query_time_to_data_time(
            query_data.time_info.get_start() + query_data.time_info.get_duration(),
        );
        let ctx = self.build_spatiotemporal_context(&match_result, query_time, &query_data)?;
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
        spatial_filter: String,
        query_time: u64,
    ) -> Option<QueryExecutionContext> {
        let (query_plan, do_merge) = self
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

        // SpatioTemporal queries are a single (non-nested) SELECT layer, same
        // shape `detect_sql_topk` expects, so top-k detection applies directly.
        let topk = detect_sql_topk(query_data);
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
            .build_query_kwargs_sql(&statistic_to_compute, match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;
        if let Some(topk) = topk {
            query_kwargs.insert("k".to_string(), topk.k.to_string());
        }

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Calculate timestamps
        let end_timestamp = self.align_end_timestamp_sql(query_time);
        let duration_ms =
            (match_result.outer_data()?.time_info.get_duration() * 1000.0).round() as u64;
        let start_timestamp = end_timestamp - duration_ms;

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
            let requirements = self.build_query_requirements_sql(match_result, topk);
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
            String::new(),
            query_time,
        )
    }
}

#[cfg(test)]
mod detect_topk_tests {
    use sql_utilities::ast_matching::{detect_sql_topk, SQLPatternParser, SqlTopk, TopkWeighting};
    use sql_utilities::sqlhelper::{
        AggregationInfo, OrderByItem, SQLQueryData, SQLSchema, Table, TimeInfo,
    };
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
    fn alias_case_mismatch_still_detects_topk() {
        // The parser path can normalize/canonicalize identifiers; verify directly on
        // SQLQueryData that alias matching in detect_sql_topk is case-insensitive.
        let qd = SQLQueryData {
            aggregation_info: AggregationInfo::new(
                "COUNT".to_string(),
                "pkt_len".to_string(),
                vec![],
            ),
            aggregation_alias: Some("transfer_events".to_string()),
            metric: "netflow_table".to_string(),
            labels: HashSet::from(["srcip".to_string()]),
            time_info: TimeInfo::new("time".to_string(), 0.0, 1.0),
            subquery: None,
            order_by: vec![OrderByItem {
                column: "TRANSFER_EVENTS".to_string(),
                ascending: false,
            }],
            limit: Some(10),
        };
        assert_eq!(
            detect_sql_topk(&qd),
            Some(SqlTopk {
                k: 10,
                weighting: TopkWeighting::Count,
            }),
        );
    }

    #[test]
    fn zero_limit_is_not_topk() {
        let sql = format!(
            "SELECT srcip, COUNT(pkt_len) AS transfer_events FROM netflow_table {WINDOW} \
             GROUP BY srcip ORDER BY transfer_events DESC LIMIT 0"
        );
        let qd = parse(&sql).expect("query should parse");
        assert_eq!(detect_sql_topk(&qd), None, "LIMIT 0 is not top-k");
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
            "outer SELECT alone matches the top-k shape — this is why nested queries must be \
             rejected (NestedQueryUnsupported) before topk detection ever runs on them",
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

/// End-to-end SQL top-k pipeline tests for `CountMinSketchWithHeap`.
///
/// Covers both resolution paths:
///   * **query_config** — self-keyed single-aggregation reference
///   * **capability matching** — heap + paired `DeltaSetAggregator`, no query_config
///
/// Example query shape:
/// ```sql
/// SELECT srcip, COUNT(pkt_len) AS transfer_events
/// FROM netflow_table WHERE <1s window> GROUP BY srcip ORDER BY transfer_events DESC LIMIT n
/// ```
/// SQL detection promotes it to `Statistic::Topk`. On the query_config path the
/// heap is self-keyed; on the capability path a separate key aggregation is paired.
/// The pipeline sorts by value descending and truncates to `n`, without PromQL-style
/// metric-name prefixing (rows stay bare `(srcip, count)`).
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
            window_size_ms: 1000,
            slide_interval_ms: 1000,
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
            1000, // 1s scrape interval ⇒ the 1s window classifies as OnlySpatial
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

    /// Build a SQL engine whose only aggregation is a self-keyed
    /// `CountMinSketchWithHeap` precomputed over the *full 2-second* window,
    /// referenced by a query_config template with a matching 2s duration —
    /// same shape as `build_topk_engine`, just sized to a SpatioTemporal
    /// window instead of a single-scrape-interval one.
    ///
    /// Self-keyed resolution (`aggregation_id_for_key ==
    /// aggregation_id_for_value`) only happens via this query_config path
    /// (`find_query_config_sql` / `get_aggregation_id_info`'s single-reference
    /// case). The capability-matching fallback always pairs
    /// `CountMinSketchWithHeap` with a separate key aggregation (see
    /// `count_topk_capability_fallback_pairs_heap_with_key_agg`) — it doesn't
    /// know a heap can be self-keyed, so a SpatioTemporal top-k query with no
    /// matching query_config would fail to resolve today. That gap is
    /// tracked separately; this test targets `build_spatiotemporal_context`'s
    /// top-k *detection* (issue #498), not the capability-matching fallback.
    fn build_spatiotemporal_topk_engine() -> (SimpleEngine, Arc<SimpleMapStore>) {
        let template = "SELECT srcip, COUNT(pkt_len) FROM netflow_table \
             WHERE time BETWEEN DATEADD(s, -2, NOW()) AND NOW() GROUP BY srcip";

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
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::new(vec!["srcip".to_string()]),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms: 2000,
            slide_interval_ms: 2000,
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
            1000, // 1s scrape interval ⇒ a 2s query window classifies as SpatioTemporal
            QueryLanguage::sql,
        );
        (engine, store)
    }

    /// Incoming top-k query over a 2-second window grouped by a *subset* of
    /// labels (`srcip` only, out of `srcip`/`dstip`/`proto`) — the shape that
    /// classifies as `SpatioTemporal` rather than `OnlySpatial`.
    fn spatiotemporal_topk_query(limit: u64) -> String {
        format!(
            "SELECT srcip, COUNT(pkt_len) AS transfer_events FROM netflow_table \
             WHERE time BETWEEN DATEADD(s, -2, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' \
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
            window_size_ms: 1000,
            slide_interval_ms: 1000,
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
            1000,
            QueryLanguage::sql,
        )
    }

    const HEAP_COUNT_ID: u64 = 111;
    const HEAP_SUM_ID: u64 = 112;
    const HEAP_DEFAULT_ID: u64 = 113;
    const KEY_AGG_ID: u64 = 211;

    fn netflow_sql_schema() -> SQLSchema {
        let value_cols: HashSet<String> = ["pkt_len"].iter().map(|s| s.to_string()).collect();
        let labels: HashSet<String> = ["srcip", "dstip", "proto"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let table = Table::new(METRIC.to_string(), "time".to_string(), value_cols, labels);
        SQLSchema::new(vec![table])
    }

    /// `CountMinSketchWithHeap` for capability-matching tests. When `count_events`
    /// is `None`, the parameter is omitted so the config relies on the default
    /// (`count_events: true`).
    fn make_heap_agg(id: u64, count_events: Option<bool>) -> AggregationConfig {
        let mut parameters = HashMap::new();
        if let Some(count_events) = count_events {
            parameters.insert("count_events".to_string(), serde_json::json!(count_events));
        }
        AggregationConfig {
            aggregation_id: id,
            aggregation_type: AggregationType::CountMinSketchWithHeap,
            aggregation_sub_type: String::new(),
            parameters,
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::new(vec!["srcip".to_string()]),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms: 1000,
            slide_interval_ms: 1000,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: METRIC.to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        }
    }

    fn make_delta_set_key_agg(id: u64) -> AggregationConfig {
        AggregationConfig {
            aggregation_id: id,
            aggregation_type: AggregationType::DeltaSetAggregator,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms: 1000,
            slide_interval_ms: 1000,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: METRIC.to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        }
    }

    /// Engine with **no** query_configs so top-k resolves via capability matching.
    /// Always provisions a paired `DeltaSetAggregator` key aggregation.
    fn build_capability_fallback_engine(heap_configs: Vec<AggregationConfig>) -> SimpleEngine {
        let mut agg_configs = HashMap::new();
        for heap in &heap_configs {
            agg_configs.insert(heap.aggregation_id, heap.clone());
        }
        agg_configs.insert(KEY_AGG_ID, make_delta_set_key_agg(KEY_AGG_ID));

        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: agg_configs,
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        let inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(netflow_sql_schema()),
            query_configs: vec![],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };
        SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            1000,
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

    /// `build_spatiotemporal_context` (issue #498) must run the same top-k
    /// detection as the `OnlyTemporal`/`OnlySpatial` path: a `SpatioTemporal`
    /// query (multi-interval window, subset of labels) shaped like
    /// `COUNT ... GROUP BY <key> ORDER BY <alias> DESC LIMIT k` still resolves
    /// to `Statistic::Topk` with `k` threaded through, self-keyed to the same
    /// sketch the query_config template resolves for plain COUNT.
    #[test]
    fn spatiotemporal_query_detects_topk_and_resolves_self_keyed_heap() {
        let (engine, _store) = build_spatiotemporal_topk_engine();
        let context = engine
            .build_query_execution_context_sql(spatiotemporal_topk_query(10), QUERY_TIME)
            .expect("SpatioTemporal top-k query should build a context via the query_config path");

        assert_eq!(
            context.metadata.statistic_to_compute,
            Statistic::Topk,
            "ORDER BY <count alias> DESC LIMIT n must be promoted to Topk even under SpatioTemporal classification",
        );
        assert_eq!(
            context.metadata.query_kwargs.get("k").map(String::as_str),
            Some("10"),
            "LIMIT should be threaded through as the `k` kwarg",
        );
        assert_eq!(
            context.agg_info.aggregation_id_for_key, context.agg_info.aggregation_id_for_value,
            "self-keyed: the heap supplies both keys and counts",
        );
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

    #[test]
    fn count_topk_capability_fallback_pairs_heap_with_key_agg() {
        let engine =
            build_capability_fallback_engine(vec![make_heap_agg(HEAP_COUNT_ID, Some(true))]);
        let context = engine
            .build_query_execution_context_sql(topk_query(10), QUERY_TIME)
            .expect("COUNT top-k should resolve via capability matching");

        assert_eq!(context.metadata.statistic_to_compute, Statistic::Topk);
        assert_eq!(
            context.agg_info.aggregation_id_for_value, HEAP_COUNT_ID,
            "count-weighted heap must be the value aggregation",
        );
        assert_eq!(
            context.agg_info.aggregation_id_for_key, KEY_AGG_ID,
            "multi-population top-k must pair heap with DeltaSetAggregator",
        );
        assert_ne!(
            context.agg_info.aggregation_id_for_key,
            context.agg_info.aggregation_id_for_value,
        );
        assert!(
            context.store_plan.keys_query.is_some(),
            "capability fallback plans a separate keys query",
        );
    }

    #[test]
    fn count_topk_capability_fallback_picks_count_weighted_when_both_heaps_exist() {
        let engine = build_capability_fallback_engine(vec![
            make_heap_agg(HEAP_COUNT_ID, Some(true)),
            make_heap_agg(HEAP_SUM_ID, Some(false)),
        ]);
        let context = engine
            .build_query_execution_context_sql(topk_query(10), QUERY_TIME)
            .expect("COUNT top-k should pick the count_events: true sketch");

        assert_eq!(
            context.agg_info.aggregation_id_for_value, HEAP_COUNT_ID,
            "COUNT top-k must not pick the sum-weighted sketch when both exist",
        );
    }

    #[test]
    fn count_topk_capability_fallback_defaults_count_events_true() {
        // Heap omits `count_events`; matcher treats that as count semantics.
        let engine = build_capability_fallback_engine(vec![make_heap_agg(HEAP_DEFAULT_ID, None)]);
        let context = engine
            .build_query_execution_context_sql(topk_query(10), QUERY_TIME)
            .expect("COUNT top-k should match a sketch with default count_events");

        assert_eq!(
            context.agg_info.aggregation_id_for_value, HEAP_DEFAULT_ID,
            "default (no flag) heap must serve COUNT top-k",
        );
    }

    #[test]
    fn sum_topk_capability_fallback_picks_value_weighted_heap() {
        let engine = build_capability_fallback_engine(vec![
            make_heap_agg(HEAP_COUNT_ID, Some(true)),
            make_heap_agg(HEAP_SUM_ID, Some(false)),
        ]);
        let context = engine
            .build_query_execution_context_sql(sum_topk_query(5), QUERY_TIME)
            .expect("SUM top-k should resolve via capability matching");

        assert_eq!(context.metadata.statistic_to_compute, Statistic::Topk);
        assert_eq!(
            context.agg_info.aggregation_id_for_value, HEAP_SUM_ID,
            "SUM top-k must pick the count_events: false sketch",
        );
        assert_eq!(context.agg_info.aggregation_id_for_key, KEY_AGG_ID);
        assert!(context.store_plan.keys_query.is_some());
    }

    #[test]
    fn sum_topk_capability_fallback_rejects_count_only_default_heap() {
        // Only a default (count-weighted) sketch exists; SUM top-k cannot be served.
        let engine = build_capability_fallback_engine(vec![make_heap_agg(HEAP_DEFAULT_ID, None)]);
        assert!(
            engine
                .build_query_execution_context_sql(sum_topk_query(5), QUERY_TIME)
                .is_none(),
            "SUM top-k must not fall back to a count_events-default sketch",
        );
    }
}

/// `build_spatiotemporal_context`'s end_timestamp snap: SQL always snaps a
/// misaligned end_timestamp down to the nearest data-ingestion-interval
/// boundary, for every SQL query shape including genuine multi-interval
/// SpatioTemporal queries (PromQL's `align_end_timestamp_promql` mirrors this
/// unconditional behavior too, see #508).
#[cfg(test)]
mod spatiotemporal_timestamp_alignment_tests {
    use super::SimpleEngine;
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        QueryConfig, QueryLanguage, SchemaConfig, StreamingConfig, WindowType,
    };
    use crate::stores::simple_map_store::SimpleMapStore;
    use chrono::{Local, TimeZone};
    use promql_utilities::data_model::KeyByLabelNames;
    use sql_utilities::sqlhelper::{SQLSchema, Table};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    /// The SQL literal-date parser reads timestamps in the local timezone, so
    /// the wall-clock second value that lands on (or off) a 300ms boundary
    /// depends on the machine's TZ offset. Rather than hardcode an
    /// assumed-UTC epoch, compute each candidate second's epoch-ms directly
    /// via `chrono::Local`, so the test is correct under any CI timezone.
    fn local_epoch_ms(second: u32) -> i64 {
        Local
            .with_ymd_and_hms(2025, 10, 1, 0, 0, second)
            .single()
            .expect("2025-10-01 00:00:xx is unambiguous in any timezone")
            .timestamp()
            * 1000
    }

    /// A SpatioTemporal SQL engine (GROUP BY a subset of labels, 2s window)
    /// with a 300ms scrape interval — deliberately not a divisor of 1000ms,
    /// so a whole-second literal timestamp is misaligned unless its seconds
    /// value happens to be a multiple of 0.3s.
    fn build_engine() -> SimpleEngine {
        let labels: HashSet<String> = ["L1", "L2"].iter().map(|s| s.to_string()).collect();
        let value_cols: HashSet<String> = ["value"].iter().map(|s| s.to_string()).collect();
        let table = Table::new(
            "cpu_usage".to_string(),
            "time".to_string(),
            value_cols,
            labels,
        );
        let sql_schema = SQLSchema::new(vec![table]);

        const AGG_ID: u64 = 1;
        let template = "SELECT L1, SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -2, NOW()) AND NOW() GROUP BY L1";
        let query_config = QueryConfig::new(template.to_string())
            .add_aggregation(AggregationReference::new(AGG_ID, None));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(sql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        let agg_config = AggregationConfig {
            aggregation_id: AGG_ID,
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(vec!["L1".to_string()]),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms: 2000,
            slide_interval_ms: 2000,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: "cpu_usage".to_string(),
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
            300, // scrape interval, ms — not a divisor of 1000
            QueryLanguage::sql,
        )
    }

    #[test]
    fn misaligned_end_timestamp_is_snapped_down() {
        // Find a whole second in [0, 12) whose epoch-ms is NOT a multiple of
        // 300ms (any TZ offset used by the local-date parser is itself a
        // multiple of 300ms, so such a second exists in every timezone).
        let (second, end_ms) = (0..12u32)
            .map(|s| (s, local_epoch_ms(s)))
            .find(|(_, ms)| ms % 300 != 0)
            .expect("a misaligned second must exist in any timezone");
        let expected_end_ms = (end_ms / 300) * 300;

        let query = format!(
            "SELECT L1, SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -2, '2025-10-01 00:00:{second:02}') \
             AND '2025-10-01 00:00:{second:02}' GROUP BY L1"
        );
        let context = build_engine()
            .build_query_execution_context_sql(query, 0.0)
            .expect("SpatioTemporal query should build a context");

        let window = &context.store_plan.values_query;
        assert_eq!(
            window.end_timestamp, expected_end_ms as u64,
            "misaligned end_timestamp must be snapped down to the nearest 300ms boundary"
        );
        assert_eq!(
            window.start_timestamp,
            (expected_end_ms - 2000) as u64,
            "start_timestamp must be the snapped end_timestamp minus the query's own 2s duration"
        );
    }

    #[test]
    fn already_aligned_end_timestamp_is_unchanged() {
        // Find a whole second in [0, 12) whose epoch-ms already lands on a
        // 300ms boundary.
        let (second, end_ms) = (0..12u32)
            .map(|s| (s, local_epoch_ms(s)))
            .find(|(_, ms)| ms % 300 == 0)
            .expect("an aligned second must exist in any timezone");

        let query = format!(
            "SELECT L1, SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -2, '2025-10-01 00:00:{second:02}') \
             AND '2025-10-01 00:00:{second:02}' GROUP BY L1"
        );
        let context = build_engine()
            .build_query_execution_context_sql(query, 0.0)
            .expect("SpatioTemporal query should build a context");

        let window = &context.store_plan.values_query;
        assert_eq!(
            window.end_timestamp, end_ms as u64,
            "an already-aligned end_timestamp must be left unchanged (snap is a no-op)"
        );
        assert_eq!(window.start_timestamp, (end_ms - 2000) as u64);
    }
}

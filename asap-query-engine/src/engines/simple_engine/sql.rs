//! SQL query language handler for SimpleEngine.
//!
//! Contains all SQL-specific context building, pattern matching, and query dispatch.

use super::SimpleEngine;
use super::{QueryExecutionContext, QueryMetadata, QueryTimestamps, StoreQueryParams};
use crate::data_model::{AggregationIdInfo, KeyByLabelValues, QueryConfig, SchemaConfig};
use crate::engines::query_result::{
    InstantVector, InstantVectorElement, QueryResult, RangeVectorElement,
};
use asap_types::query_requirements::QueryRequirements;
use asap_types::utils::normalize_spatial_filter;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, Statistic};
use sql_utilities::ast_matching::{
    detect_sql_topk, SQLPatternMatcher, SQLPatternParser, SQLQuery, SqlTopk, TopkWeighting,
};
use sql_utilities::ast_matching::pattern_rewrites::{
    build_derived_ratio_surrogates, build_grand_total_pct_surrogate,
    build_group_array_distinct_surrogate,
    build_group_by_having_count_surrogate, build_hidden_countif_having_surrogates,
    build_two_stage_histogram_inner_surrogate, build_weekly_moas_histogram_inner_surrogate,
    parse_correlated_in_subquery_query, parse_weekly_moas_histogram_query,
    build_moas_surrogate, build_multi_aggregate_surrogates,
    build_running_total_surrogate, build_select_distinct_surrogate,
    looks_like_moas_registered_sql, looks_like_moas_sql, parse_bucketed_topn_query,
    parse_derived_ratio_query, parse_grand_total_pct_query, parse_group_array_distinct_query,
    parse_group_by_having_count_query, parse_hidden_countif_having_query,
    parse_two_stage_histogram_query, parse_computed_group_by_query, find_matching_close_paren,
    parse_arg_agg_query, parse_moas_query, parse_multi_aggregate_query, parse_order_by_and_limit,
    parse_running_total_query, parse_select_distinct_query, rewrite_computed_group_by_query,
    rewrite_arrayzip_edge_explode_query,
    rewrite_flat_token_explode_query, rewrite_lag_gap_query, rewrite_lag_transition_query,
    rewrite_arg_agg_query, rewrite_raw_value_agg_query,
    rewrite_token_explode_query,
    rewrite_token_select_query,
    ArgAggMatch, GroupArrayDistinctMatch, MultiAggregateMatch, RatioDenominator,
};
use sql_utilities::sqlhelper::{
    HavingFilter, OrderByItem, SQLBucketedCountIfQueryData, SQLQueryData,
};
use sqlparser::dialect::*;
use sqlparser::parser::Parser as parser;
use std::collections::{HashMap, HashSet};
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
    /// `HAVING <aggregation_alias> <op> <literal>` - filters groups by the
    /// aggregate's own computed value (`element.value`) before ORDER BY /
    /// LIMIT are applied, matching SQL's own evaluation order.
    pub having: Option<HavingFilter>,
}

impl SqlPostProcessing {
    fn from_query_data(query_data: &SQLQueryData) -> Self {
        Self {
            aggregation_alias: query_data.aggregation_alias.clone(),
            order_by: query_data.order_by.clone(),
            limit: query_data.limit,
            having: query_data.having.clone(),
        }
    }

    /// Returns `true` when there's no filtering, ordering, or truncation to
    /// apply, so the caller can short-circuit any deconstruction of the
    /// result.
    fn is_noop(&self) -> bool {
        self.order_by.is_empty() && self.limit.is_none() && self.having.is_none()
    }

    /// Apply HAVING + ORDER BY + LIMIT to a `QueryResult`, in that order -
    /// HAVING filters groups by their aggregate value first, then the
    /// survivors are sorted and truncated. Only `QueryResult::Vector` is
    /// rewritten; matrices pass through unchanged (range queries don't flow
    /// through `handle_query_sql`).
    pub fn apply(&self, output_labels: &KeyByLabelNames, result: QueryResult) -> QueryResult {
        if self.is_noop() {
            return result;
        }
        match result {
            QueryResult::Vector(InstantVector {
                mut values,
                timestamp,
                has_value,
            }) => {
                if let Some(having) = &self.having {
                    values.retain(|e| having.op.evaluate(e.value, having.value));
                }
                let values = sort_and_truncate_instant_vector(
                    values,
                    &output_labels.labels,
                    self.aggregation_alias.as_deref(),
                    &self.order_by,
                    self.limit,
                );
                QueryResult::Vector(InstantVector {
                    values,
                    timestamp,
                    has_value,
                })
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
                        // Numeric-if-both-parse, else lexicographic - a
                        // label ORDER BY on a computed integer column (e.g.
                        // `length(splitByChar(...))`) needs "2" before
                        // "10", which plain string comparison gets wrong.
                        match (av.parse::<f64>(), bv.parse::<f64>()) {
                            (Ok(an), Ok(bn)) => {
                                an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal)
                            }
                            _ => av.cmp(bv),
                        }
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
        spatial_filter: Option<&str>,
    ) -> QueryRequirements {
        let query_data = match_result
            .outer_data()
            .expect("build_query_requirements_sql called on valid SQLQuery");
        // AggregationConfig.metric is derived as "{table_name}.{value_column}"
        // for SQL configs (see aggregation_config.rs's from_yaml deserialization,
        // "Derive metric from table_name.value_column for internal use") - the
        // bare table name query_data.metric alone never matches any registered
        // config here, so every capability-matching lookup for a SQL query
        // failed with "no compatible aggregation found" regardless of shape.
        //
        // query_data.metric is already the right table name for both cases.
        // For a plain COUNT it's the real table unchanged. For an aggregation
        // over a non-value column (uniqExact(prefix), quantile over a derived
        // length, etc.), rewrite_raw_value_agg_query (called earlier, in
        // build_query_execution_context_sql_with_post_processing) already
        // rewrote this query's FROM clause to the synthetic
        // "derived_value_{col}_{table}" surrogate table before it was parsed
        // into match_result - see that function's doc comment. Re-deriving
        // "derived_value_{col}_{table}" here from an already-derived
        // query_data.metric double-prefixes it
        // (derived_value_x_derived_value_x_table), which is exactly why the
        // capability-matching lookup still failed after the first attempt at
        // this fix.
        let value_column_name = query_data.aggregation_info.get_value_column_name();
        let metric = format!("{}.{}", query_data.metric, value_column_name);

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
            // Was hardcoded to normalize_spatial_filter("") regardless of the
            // query's actual filter, so spatial_filter_compatible always
            // rejected any config with a non-empty registered filter (i.e.
            // virtually every real query, since almost all of them filter on
            // `collector = '...'`). Can't be recovered from `query_data` above
            // (that's match_result.outer_data(), the PATTERN-flattened view -
            // query_info_to_pattern's flatten_query_info step drops
            // spatial_filter entirely when building it) - the caller must
            // pass the original, unflattened query's filter through instead.
            spatial_filter_normalized: normalize_spatial_filter(spatial_filter.unwrap_or("")),
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
        // Bucketed countIf (e.g. `SELECT toStartOfFiveMinutes(timestamp) AS
        // bucket, countIf(operation = 'A') AS anns ... GROUP BY bucket`) must
        // be tried on the ORIGINAL, un-rewritten query text, before anything
        // else touches it. The computed-GROUP-BY rewrite below matches ANY
        // 2+-item SELECT with a computed first expression - including this
        // shape's own bucket function - and replaces it with a bare alias;
        // once that happens, parse_bucketed_countif_query can no longer see
        // the bucket function it needs to recognize the shape at all, and
        // the classic single-aggregate parser it would fall through to
        // doesn't understand `countIf(...)` as an aggregate either. Mirrors
        // get_streaming_aggregation_configs's own dispatch priority (planner
        // side), which checks this exact shape before computed-group-by for
        // the same reason.
        if let Some(result) = self.handle_bucketed_countif_sql(&query, time) {
            return Some(result);
        }

        // Must run on the RAW query, before any rewrite (including the one
        // right below) replaces a computed-GROUP-BY expression with its bare
        // alias - see `build_query_execution_context_sql_with_post_processing`'s
        // doc comment for why this can only be detected here, once, and
        // threaded down explicitly.
        let computed_group_by_alias = parse_computed_group_by_query(&query).map(|m| m.alias);

        let query = self.rewrite_recognized_pattern(&query);

        if let Some(result) = self.handle_moas_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_select_distinct_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_arg_agg_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_avg_sql(&query, computed_group_by_alias.as_deref(), time) {
            return Some(result);
        }

        if let Some(result) = self.handle_group_by_having_count_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_hidden_countif_having_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_two_stage_histogram_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_correlated_in_subquery_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_weekly_moas_histogram_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_grand_total_pct_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_running_total_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_bucketed_topn_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) = self.handle_derived_ratio_sql(&query, time) {
            return Some(result);
        }

        if let Some(result) =
            self.handle_multi_aggregate_sql(&query, computed_group_by_alias.as_deref(), time)
        {
            return Some(result);
        }

        let (context, post) = self.build_query_execution_context_sql_with_post_processing(
            query,
            computed_group_by_alias.as_deref(),
            time,
        )?;
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

    /// Tries each recognized complex-SQL-shape rewrite in turn (lag-
    /// transition, token-select, token-explode, raw/computed-value
    /// aggregate) and returns the first one that fires, or the original
    /// query unchanged if none do. These are the same detectors
    /// asap-planner-rs uses to decide what to build - shared via
    /// sql_utilities::ast_matching::pattern_rewrites so the two can never
    /// silently disagree about what a given raw query means. An instance
    /// method (not the free function it once was) because the
    /// raw/computed-value-aggregate rewrite needs the live schema to check
    /// whether a column is already a real value column - a check the
    /// planner's own copy of this logic also makes.
    pub(crate) fn rewrite_recognized_pattern(&self, sql: &str) -> String {
        // MOAS has its own detection + surrogate-building (parse_moas_query /
        // handle_moas_sql) that must run on the *raw* query text. The
        // tokenized MOAS shape also satisfies looks_like_token_select_sql
        // (both tokenize as_path and index [-1]), so without this early
        // return the token-select rewrite below would mangle a MOAS query
        // into a broken surrogate before MOAS ever saw the original text -
        // and every caller of this function (not just handle_query_sql)
        // needs that guarantee, since this is the single shared rewrite
        // entry point.
        if looks_like_moas_sql(sql) {
            return sql.to_string();
        }
        if let Some(rewritten) = rewrite_lag_transition_query(sql) {
            warn!("lag-transition rewrite produced SQL: {}", rewritten);
            return rewritten;
        }
        if let Some(rewritten) = rewrite_lag_gap_query(sql) {
            warn!("lag-gap rewrite produced SQL: {}", rewritten);
            return rewritten;
        }
        if let Some(rewritten) = rewrite_token_select_query(sql) {
            warn!("token-select rewrite produced SQL: {}", rewritten);
            return rewritten;
        }
        if let Some(rewritten) = rewrite_token_explode_query(sql) {
            warn!("token-explode rewrite produced SQL: {}", rewritten);
            return rewritten;
        }
        if let Some(rewritten) = rewrite_flat_token_explode_query(sql) {
            warn!("flat token-explode rewrite produced SQL: {}", rewritten);
            return rewritten;
        }
        if let Some(rewritten) = rewrite_arrayzip_edge_explode_query(sql) {
            warn!("arrayZip edge-explode rewrite produced SQL: {}", rewritten);
            return rewritten;
        }
        if let Some(rewritten) = rewrite_computed_group_by_query(sql) {
            warn!("computed-GROUP-BY rewrite produced SQL: {}", rewritten);
            return rewritten;
        }
        // MIN/MAX/SUM/AVG/uniqExact over a raw (non-value) column, or a
        // computed expression / trivial toString wrapper around one - see
        // rewrite_raw_value_agg_query's doc comment. Needs the live schema,
        // unlike every rewrite above.
        {
            let schema = match &self.inference_config.read().unwrap().schema {
                SchemaConfig::SQL(sql_schema) => Some(sql_schema.clone()),
                SchemaConfig::ElasticSQL(sql_schema) => Some(sql_schema.clone()),
                _ => None,
            };
            if let Some(schema) = schema {
                if let Some(rewritten) = rewrite_raw_value_agg_query(sql, &schema) {
                    warn!("raw/computed-value-aggregate rewrite produced SQL: {}", rewritten);
                    return rewritten;
                }
                if let Some(rewritten) = rewrite_arg_agg_query(sql, &schema) {
                    warn!("arg-aggregate rewrite produced SQL: {}", rewritten);
                    return rewritten;
                }
            }
        }
        sql.to_string()
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
        self.build_query_execution_context_sql_with_post_processing(query, None, time)
            .map(|(ctx, _)| ctx)
    }

    fn find_query_config_sql_moas(&self) -> Option<QueryConfig> {
        let ic = self.inference_config.read().unwrap();

        ic.query_configs
            .iter()
            .find(|config| looks_like_moas_registered_sql(&config.query))
            .cloned()
    }

    fn handle_moas_sql(&self, query: &str, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        if !looks_like_moas_sql(query) {
            return None;
        }

        warn!("MOAS handler matched query");

        let schema = match &self.inference_config.read().unwrap().schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => {
                warn!("MOAS handler: non-SQL schema");
                return None;
            }
            &SchemaConfig::ElasticQueryDSL(_) => {
                warn!("MOAS handler: ElasticQueryDSL schema");
                return None;
            }
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
        };

        let m = match parse_moas_query(query) {
            Some(m) => m,
            None => {
                warn!("MOAS handler: could not parse MOAS query");
                return None;
            }
        };
        let surrogate = build_moas_surrogate(&m);
        warn!("MOAS surrogate query: {}", surrogate);

        let statements = match parser::parse_sql(&GenericDialect {}, surrogate.as_str()) {
            Ok(statements) => statements,
            Err(e) => {
                warn!("MOAS handler: could not parse surrogate query: {}", e);
                return None;
            }
        };

        let query_data = match SQLPatternParser::new(&schema, time).parse_query(&statements) {
            Some(qd) => qd,
            None => {
                warn!("MOAS handler: SQLPatternParser rejected surrogate query");
                return None;
            }
        };

        let query_config = match self.find_query_config_sql_moas() {
            Some(config) => config,
            None => {
                warn!("MOAS handler: no MOAS query_config found");
                return None;
            }
        };

        let aggregation_id = match query_config.aggregations.first() {
            Some(agg) => agg.aggregation_id,
            None => {
                warn!("MOAS handler: query_config has no aggregations");
                return None;
            }
        };

        warn!(
            "MOAS handler: using aggregation_id={} metric={} duration_s={}",
            aggregation_id,
            query_data.metric,
            query_data.time_info.get_duration()
        );

        let end_timestamp = self.align_end_timestamp_sql(Self::convert_query_time_to_data_time(
            query_data.time_info.get_start() + query_data.time_info.get_duration(),
        ));
        let duration_ms = (query_data.time_info.get_duration() * 1000.0).round() as u64;
        let start_timestamp = match end_timestamp.checked_sub(duration_ms) {
            Some(ts) => ts,
            None => {
                warn!("MOAS handler: invalid start/end timestamps");
                return None;
            }
        };

        let params = StoreQueryParams {
            metric: query_data.metric.clone(),
            aggregation_id,
            start_timestamp,
            end_timestamp,
            is_exact_query: false,
        };

        let timestamped_map = match self.execute_store_query(&params) {
            Ok(map) => map,
            Err(e) => {
                warn!("MOAS handler: store query failed: {}", e);
                return None;
            }
        };

        warn!(
            "MOAS handler: store returned {} {} groups",
            timestamped_map.len(),
            m.group_by
        );

        let mut rows: Vec<(String, Vec<String>)> = Vec::new();

        for (group_key, timestamped_buckets) in timestamped_map {
            let Some(group_key_values) = group_key else {
                continue;
            };

            let group_value = group_key_values
                .get(0)
                .map(|s| s.to_string())
                .unwrap_or_default();
            if group_value.is_empty() {
                continue;
            }

            let mut origins: HashSet<String> = HashSet::new();

            for (_bucket, precompute) in timestamped_buckets {
                if let Some(keys) = precompute.get_keys() {
                    for key in keys {
                        if let Some(origin) = key.get(0) {
                            if !origin.is_empty() {
                                origins.insert(origin.to_string());
                            }
                        }
                    }
                }
            }

            if origins.len() > 1 {
                let mut origins_vec: Vec<String> = origins.into_iter().collect();
                origins_vec.sort();
                rows.push((group_value, origins_vec));
            }
        }

        warn!("MOAS handler: produced {} MOAS rows", rows.len());

        rows.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(&b.0)));

        let values: Vec<InstantVectorElement> = rows
            .into_iter()
            .map(|(group_value, origins)| {
                let origin_count = origins.len() as f64;
                let origins_joined = origins.join(",");
                InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(vec![group_value, origins_joined]),
                    origin_count,
                )
            })
            .collect();

        let output_labels = KeyByLabelNames::new(vec![m.group_by.clone(), "origins".to_string()]);

        Some((output_labels, QueryResult::vector(values, end_timestamp)))
    }

    /// `SELECT DISTINCT <col> FROM ... WHERE ...`, no GROUP BY - see
    /// `SelectDistinctMatch` / `get_select_distinct_streaming_aggregation_configs`
    /// on the planner side. The registered precompute is a `SetAggregator`
    /// with an empty grouping key tracking `col`'s exact distinct values (the
    /// same swap MOAS makes for `COUNT(DISTINCT label)` under a GROUP BY,
    /// just with no outer group), so this reads back every distinct value
    /// across the whole time window - one output row per value, no numeric
    /// aggregate at all.
    fn handle_select_distinct_sql(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_select_distinct_query(query)?;

        warn!("SELECT DISTINCT handler matched query");

        let schema = match &self.inference_config.read().unwrap().schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => {
                warn!("SELECT DISTINCT handler: non-SQL schema");
                return None;
            }
            &SchemaConfig::ElasticQueryDSL(_) => {
                warn!("SELECT DISTINCT handler: ElasticQueryDSL schema");
                return None;
            }
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
        };

        let surrogate = build_select_distinct_surrogate(&m);

        let statements = match parser::parse_sql(&GenericDialect {}, surrogate.as_str()) {
            Ok(statements) => statements,
            Err(e) => {
                warn!("SELECT DISTINCT handler: could not parse surrogate query: {}", e);
                return None;
            }
        };

        let query_data = match SQLPatternParser::new(&schema, time).parse_query(&statements) {
            Some(qd) => qd,
            None => {
                warn!("SELECT DISTINCT handler: SQLPatternParser rejected surrogate query");
                return None;
            }
        };

        // The ordinary structural matcher, not a dedicated lookup like
        // MOAS's find_query_config_sql_moas - the surrogate is exactly the
        // classic single-aggregate CARDINALITY shape, and matches_sql_pattern
        // now compares spatial_filter too, so this correctly finds *this*
        // query's own registered config rather than colliding with another
        // SELECT DISTINCT query over a different filter.
        let query_config = match self.find_query_config_sql(&query_data) {
            Some(config) => config,
            None => {
                warn!("SELECT DISTINCT handler: no query_config found");
                return None;
            }
        };

        let aggregation_id = match query_config.aggregations.first() {
            Some(agg) => agg.aggregation_id,
            None => {
                warn!("SELECT DISTINCT handler: query_config has no aggregations");
                return None;
            }
        };

        let end_timestamp = self.align_end_timestamp_sql(Self::convert_query_time_to_data_time(
            query_data.time_info.get_start() + query_data.time_info.get_duration(),
        ));
        let duration_ms = (query_data.time_info.get_duration() * 1000.0).round() as u64;
        let start_timestamp = match end_timestamp.checked_sub(duration_ms) {
            Some(ts) => ts,
            None => {
                warn!("SELECT DISTINCT handler: invalid start/end timestamps");
                return None;
            }
        };

        let params = StoreQueryParams {
            metric: query_data.metric.clone(),
            aggregation_id,
            start_timestamp,
            end_timestamp,
            is_exact_query: false,
        };

        let timestamped_map = match self.execute_store_query(&params) {
            Ok(map) => map,
            Err(e) => {
                warn!("SELECT DISTINCT handler: store query failed: {}", e);
                return None;
            }
        };

        let mut distinct_values: HashSet<String> = HashSet::new();
        // Empty grouping key -> one global group; unlike MOAS (which skips a
        // `None` group_key as "not a real prefix"), every group here is the
        // one we want, so both `None` and `Some(_)` are read.
        for (_group_key, timestamped_buckets) in timestamped_map {
            for (_bucket, precompute) in timestamped_buckets {
                if let Some(keys) = precompute.get_keys() {
                    for key in keys {
                        if let Some(value) = key.get(0) {
                            if !value.is_empty() {
                                distinct_values.insert(value.to_string());
                            }
                        }
                    }
                }
            }
        }

        warn!(
            "SELECT DISTINCT handler: produced {} distinct values",
            distinct_values.len()
        );

        let mut values_vec: Vec<String> = distinct_values.into_iter().collect();
        values_vec.sort();

        let elements: Vec<InstantVectorElement> = values_vec
            .into_iter()
            .map(|v| InstantVectorElement::new(KeyByLabelValues::new_with_labels(vec![v]), 0.0))
            .collect();

        let output_labels = KeyByLabelNames::new(vec![m.column.clone()]);
        let (order_by, limit) = parse_order_by_and_limit(&m.order_by_and_limit);
        let post = SqlPostProcessing {
            aggregation_alias: None,
            order_by,
            limit,
            having: None,
        };
        let result = post.apply(
            &output_labels,
            QueryResult::vector_without_value(elements, end_timestamp),
        );

        Some((output_labels, result))
    }

    /// `argMax(x, <time_column>)` / `argMin(x, <time_column>)` - see
    /// `ArgAggMatch`'s doc comment for the shape and
    /// `get_arg_agg_streaming_aggregation_configs`'s for the planner-side
    /// half. `MultipleArgAccumulator`'s real result is a string, which
    /// `AggregateCore::query_statistic` (numeric-only) can't return, so it
    /// gets its own dedicated handler rather than the classic
    /// `execute_context` path - the result is rendered as an extra LABEL
    /// column (via `query_statistic_string` + `vector_without_value`,
    /// exactly like `SELECT DISTINCT`'s own label-only result above),
    /// alongside the `GROUP BY` column, rather than a numeric `value`.
    fn handle_arg_agg_sql(&self, query: &str, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        let (m, values, end_timestamp) = self.try_execute_arg_agg_branch(query, time)?;

        let elements: Vec<InstantVectorElement> = values
            .into_iter()
            .map(|(mut labels, arg_value)| {
                labels.push(arg_value);
                InstantVectorElement::new(KeyByLabelValues::new_with_labels(labels), 0.0)
            })
            .collect();

        let output_labels = KeyByLabelNames::new(vec![m.group_by_col.clone(), m.alias.clone()]);
        let limit = m.limit.as_ref().and_then(|s| s.parse::<u64>().ok());
        let post = SqlPostProcessing {
            aggregation_alias: None,
            order_by: Vec::new(),
            limit,
            having: None,
        };
        let result = post.apply(
            &output_labels,
            QueryResult::vector_without_value(elements, end_timestamp),
        );

        Some((output_labels, result))
    }

    /// Splits a query containing a standalone `avg(<expr>)` aggregate into
    /// two surrogates - one with `avg(<expr>)` replaced by `sum(<expr>)`,
    /// one with the SAME call replaced by `count(<expr>)`- otherwise
    /// textually identical (same GROUP BY / WHERE / everything else).
    /// Case-insensitive on the function name; returns `None` if no `avg(`
    /// is found or its parens don't balance.
    ///
    /// The count half is NOT `count(*)`: when `<expr>` is a raw/computed
    /// column (not already a real value column - the usual case, e.g.
    /// `length(splitByChar(...))`), `get_raw_value_agg_streaming_aggregation_configs`
    /// (planner side) registers BOTH of AVG's [Sum, Count] statistics on
    /// the SAME derived-value virtual table, via the SAME
    /// `build_agg_configs_for_statistics` call. `count(*)` never triggers
    /// that derived-table rewrite at query time (`rewrite_raw_value_agg_query`'s
    /// trigger list is `MIN/MAX/SUM/AVG/CARDINALITY/QUANTILE` - COUNT isn't
    /// in it, since counting real rows never needs the indirection), so it
    /// stays on the original base table and can never find that registration
    /// - `count(<expr>)` does trigger it, correctly landing on the same
    /// derived table AVG's own sum half uses.
    ///
    /// Since COUNT itself isn't in that trigger list either, `count(<expr>)`
    /// can't do this via the ordinary rewrite path in one step. Disguises it
    /// as `sum(<expr>)` (which DOES trigger the rewrite), runs the shared
    /// rewrite once to land on the correct derived table, then swaps the
    /// surviving `sum(` back to `count(` in the result - a query already
    /// naming a real value column on that derived table, which
    /// `rewrite_raw_value_agg_query` correctly no-ops on if run again
    /// (`schema.is_valid_value_column` already true), so this is safe to
    /// hand to the ordinary `build_query_execution_context_sql_with_post_processing`
    /// path afterward without any special handling there.
    fn split_avg_query(&self, query: &str) -> Option<(String, String)> {
        let lower = query.to_lowercase();
        let avg_idx = lower.find("avg(")?;
        let open_paren = avg_idx + "avg".len();
        let close_paren = find_matching_close_paren(query, open_paren)?;
        let before = &query[..avg_idx];
        let call_with_parens = &query[open_paren..=close_paren];
        let after = &query[close_paren + 1..];
        let sum_sql = format!("{before}sum{call_with_parens}{after}");

        let rewritten = self.rewrite_recognized_pattern(&sum_sql);
        let rewritten_lower = rewritten.to_lowercase();
        let sum_idx = rewritten_lower.find("sum(")?;
        let count_sql = format!("{}count{}", &rewritten[..sum_idx], &rewritten[sum_idx + 3..]);

        Some((sum_sql, count_sql))
    }

    /// Computes a standalone `avg(<expr>)` aggregate by independently
    /// executing the equivalent `sum(<expr>)` and `count(*)` queries -
    /// each through the exact same machinery any ordinary single-aggregate
    /// query already uses - and dividing the two results per group key.
    ///
    /// There is no `Statistic::Avg`: `AggregationOperator::Avg` maps to
    /// `[Statistic::Sum, Statistic::Count]` (see its doc comment), and
    /// nowhere in the execution pipeline - `QueryExecutionContext`,
    /// `AggregationIdInfo`, `StoreQueryPlan` - is there a way to carry two
    /// aggregation ids for one query or a step that merges/divides two
    /// results; each of those types carries exactly one statistic end to
    /// end. Composing two complete, already-correct single-aggregate
    /// executions and dividing their outputs sidesteps teaching that whole
    /// pipeline a new dual-aggregation concept for what is, in the end,
    /// pure post-processing arithmetic - at the cost of computing the
    /// underlying sum and count as two separate store round-trips instead
    /// of one.
    ///
    /// Deliberately skips each sub-query's OWN `SqlPostProcessing`
    /// (ORDER BY / LIMIT/HAVING would be parsed off of `sum(...)`/`count(*)`
    /// text and target the wrong quantity) - callers apply the ORIGINAL
    /// query's post-processing to the divided result instead.
    fn try_execute_avg_branch(
        &self,
        query: &str,
        computed_group_by_alias: Option<&str>,
        time: f64,
    ) -> Option<(KeyByLabelNames, InstantVector)> {
        let (sum_sql, count_sql) = self.split_avg_query(query)?;

        let (sum_context, _sum_post) = self.build_query_execution_context_sql_with_post_processing(
            sum_sql,
            computed_group_by_alias,
            time,
        )?;
        let (sum_labels, sum_result) = self.execute_context(sum_context, false, false)?;
        let QueryResult::Vector(sum_vector) = sum_result else {
            warn!("avg handler: sum branch did not produce an instant vector");
            return None;
        };

        let (count_context, _count_post) = self.build_query_execution_context_sql_with_post_processing(
            count_sql,
            computed_group_by_alias,
            time,
        )?;
        let (_count_labels, count_result) = self.execute_context(count_context, false, false)?;
        let QueryResult::Vector(count_vector) = count_result else {
            warn!("avg handler: count branch did not produce an instant vector");
            return None;
        };

        let count_by_key: HashMap<Vec<String>, f64> = count_vector
            .values
            .into_iter()
            .map(|e| (e.labels.labels, e.value))
            .collect();

        let end_timestamp = sum_vector.timestamp;
        let values: Vec<InstantVectorElement> = sum_vector
            .values
            .into_iter()
            .filter_map(|e| {
                let count = *count_by_key.get(&e.labels.labels)?;
                if count == 0.0 {
                    return None;
                }
                Some(InstantVectorElement::new(e.labels, e.value / count))
            })
            .collect();

        Some((
            sum_labels,
            InstantVector {
                values,
                timestamp: end_timestamp,
                has_value: true,
            },
        ))
    }

    /// Standalone `avg(<expr>)` with no other aggregate in the same query
    /// (q013/q187's shape: a computed-GROUP-BY key plus one avg) - the
    /// `avg(x), count(*)` shape (q035/q091/q145) instead goes through
    /// `handle_multi_aggregate_sql`, which special-cases the avg branch the
    /// same way. Applies the query's OWN ORDER BY / LIMIT to the divided
    /// result, since `try_execute_avg_branch` deliberately skips post-
    /// processing on its two internal sub-queries.
    fn handle_avg_sql(
        &self,
        query: &str,
        computed_group_by_alias: Option<&str>,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        if !query.to_lowercase().contains("avg(") {
            return None;
        }
        // Multiple aggregates (avg alongside count/sum/etc.) belong to
        // handle_multi_aggregate_sql instead - only claim the truly
        // standalone shape here.
        if parse_multi_aggregate_query(query).is_some() {
            return None;
        }

        let (output_labels, vector) =
            self.try_execute_avg_branch(query, computed_group_by_alias, time)?;

        // `query` at this point has already been through rewrite_recognized_pattern,
        // so a computed-GROUP-BY alias (if any) is already bare - parse_computed_group_by_query
        // can no longer recognize the shape to hand back order_by_and_limit (same
        // reasoning as build_query_execution_context_sql_with_post_processing's
        // doc comment). ORDER BY / LIMIT always trail everything else in every
        // shape this handler claims, so just take the tail from whichever
        // keyword appears first.
        let lower = query.to_lowercase();
        let tail_start = ["order by", "limit"]
            .iter()
            .filter_map(|kw| lower.find(kw))
            .min();
        let order_by_and_limit = tail_start
            .map(|i| query[i..].trim_end_matches(';').trim())
            .unwrap_or("");
        let (order_by, limit) = parse_order_by_and_limit(order_by_and_limit);
        let post = SqlPostProcessing {
            aggregation_alias: None,
            order_by,
            limit,
            having: None,
        };
        let result = post.apply(&output_labels, QueryResult::Vector(vector));

        Some((output_labels, result))
    }

    /// Shared core of the ARGMAX/ARGMIN shape: parses `query` as an
    /// `ArgAggMatch`, reads and merges the underlying accumulator's state
    /// across every window the query range spans, and returns each group
    /// key's `query_statistic_string` result. Used both by the standalone
    /// single-aggregate handler above and by the multi-aggregate
    /// "all-labels" combiner below, which needs exactly this per-branch
    /// (group key -> arg-value string) mapping - not a fully rendered
    /// result - to build one combined row per key across several branches.
    fn try_execute_arg_agg_branch(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(ArgAggMatch, HashMap<Vec<String>, String>, u64)> {
        let m = parse_arg_agg_query(query)?;

        let schema = match &self.inference_config.read().unwrap().schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => return None,
            &SchemaConfig::ElasticQueryDSL(_) => return None,
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
        };

        let statements = parser::parse_sql(&GenericDialect {}, query).ok()?;
        let query_data = SQLPatternParser::new(&schema, time).parse_query(&statements)?;

        // Sanity check: the generic parser's `get_aggregation` accepts a
        // 2-argument aggregate call by silently keeping only the first
        // argument (see its own doc comment on the "other aggregations"
        // branch) - confirm what it actually saw really is ARGMAX/ARGMIN
        // before treating query_data as this handler's own shape.
        if !matches!(query_data.aggregation_info.get_name(), "ARGMAX" | "ARGMIN") {
            return None;
        }

        let query_config = self.find_query_config_sql(&query_data)?;
        let aggregation_id = query_config.aggregations.first()?.aggregation_id;

        let end_timestamp = self.align_end_timestamp_sql(Self::convert_query_time_to_data_time(
            query_data.time_info.get_start() + query_data.time_info.get_duration(),
        ));
        let duration_ms = (query_data.time_info.get_duration() * 1000.0).round() as u64;
        let start_timestamp = end_timestamp.checked_sub(duration_ms)?;

        let params = StoreQueryParams {
            metric: query_data.metric.clone(),
            aggregation_id,
            start_timestamp,
            end_timestamp,
            is_exact_query: false,
        };

        let timestamped_map = self
            .execute_store_query(&params)
            .map_err(|e| {
                warn!("arg-aggregate handler: store query failed: {}", e);
                e
            })
            .ok()?;

        // Merge across every window the query range spans into one
        // accumulator per group key - same reasoning, same shared utility,
        // as the classic numeric path (MinMax etc).
        let merged =
            self.merge_precomputed_outputs(&timestamped_map, true, AggregationType::MultipleArg);

        let statistic = if m.is_max { Statistic::ArgMax } else { Statistic::ArgMin };

        let mut values: HashMap<Vec<String>, String> = HashMap::new();
        for (key, precompute) in &merged {
            let Some(key) = key else {
                continue;
            };
            let arg_value = match precompute.query_statistic_string(
                statistic,
                &Some(key.clone()),
                &HashMap::new(),
            ) {
                Ok(v) => v,
                Err(e) => {
                    warn!("arg-aggregate handler: query_statistic_string failed: {}", e);
                    continue;
                }
            };
            values.insert(key.labels.clone(), arg_value);
        }

        Some((m, values, end_timestamp))
    }

    /// Core of the groupArray(DISTINCT col) shape (optionally `GROUP BY
    /// <group_col>`) - see `GroupArrayDistinctMatch`. Reads the exact
    /// `SetAggregator` set `get_group_array_distinct_streaming_aggregation_configs`
    /// registers (planner side) and, per group key, formats its member
    /// values as a ClickHouse-style `['a','b']` array string. No FROM-
    /// clause rewrite needed first (unlike `try_execute_arg_agg_branch`) -
    /// this mechanism registers directly against the query's own real
    /// table, the same way MOAS/SELECT DISTINCT do, rather than a derived
    /// virtual table.
    fn try_execute_group_array_distinct_branch(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(GroupArrayDistinctMatch, HashMap<Vec<String>, String>, u64)> {
        let m = parse_group_array_distinct_query(query)?;

        let schema = match &self.inference_config.read().unwrap().schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => return None,
            &SchemaConfig::ElasticQueryDSL(_) => return None,
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
        };

        let surrogate = build_group_array_distinct_surrogate(&m);

        let statements = parser::parse_sql(&GenericDialect {}, surrogate.as_str()).ok()?;
        let query_data = SQLPatternParser::new(&schema, time).parse_query(&statements)?;

        let query_config = self.find_query_config_sql(&query_data)?;
        let aggregation_id = query_config.aggregations.first()?.aggregation_id;

        let end_timestamp = self.align_end_timestamp_sql(Self::convert_query_time_to_data_time(
            query_data.time_info.get_start() + query_data.time_info.get_duration(),
        ));
        let duration_ms = (query_data.time_info.get_duration() * 1000.0).round() as u64;
        let start_timestamp = end_timestamp.checked_sub(duration_ms)?;

        let params = StoreQueryParams {
            metric: query_data.metric.clone(),
            aggregation_id,
            start_timestamp,
            end_timestamp,
            is_exact_query: false,
        };

        let timestamped_map = self
            .execute_store_query(&params)
            .map_err(|e| {
                warn!("groupArray(DISTINCT) handler: store query failed: {}", e);
                e
            })
            .ok()?;

        let mut per_group: HashMap<Vec<String>, HashSet<String>> = HashMap::new();
        for (group_key, timestamped_buckets) in timestamped_map {
            // Empty grouping (m.group_by is None) treats every bucket as
            // the one global group, same as handle_select_distinct_sql; a
            // real grouping column (m.group_by is Some) discards a missing
            // key, same as handle_moas_sql.
            let row_key: Vec<String> = if m.group_by.is_some() {
                match group_key {
                    Some(k) if !k.labels.is_empty() => k.labels,
                    _ => continue,
                }
            } else {
                Vec::new()
            };

            let entry = per_group.entry(row_key).or_default();
            for (_bucket, precompute) in timestamped_buckets {
                if let Some(keys) = precompute.get_keys() {
                    for key in keys {
                        if let Some(value) = key.get(0) {
                            if !value.is_empty() {
                                entry.insert(value.to_string());
                            }
                        }
                    }
                }
            }
        }

        let values: HashMap<Vec<String>, String> = per_group
            .into_iter()
            .map(|(key, set)| {
                let mut sorted: Vec<String> = set.into_iter().collect();
                sorted.sort();
                (key, Self::format_clickhouse_string_array(&sorted))
            })
            .collect();

        Some((m, values, end_timestamp))
    }

    /// Renders a ClickHouse-style `['a','b']` array literal - the closest
    /// text representation of groupArray(DISTINCT ...)'s actual return
    /// type available through this label-column-only rendering path (see
    /// `handle_multi_aggregate_all_labels_sql`'s doc comment).
    fn format_clickhouse_string_array(values: &[String]) -> String {
        let quoted: Vec<String> = values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "\\'")))
            .collect();
        format!("[{}]", quoted.join(","))
    }

    /// `round(<agg> * <mult> / sum(<same agg>) OVER (), <decimals>) AS
    /// <alias>` - each group's share of the whole result set's total, see
    /// `GrandTotalPctMatch`. Executes the single base-aggregate surrogate
    /// the planner registered, then computes the total (and each row's
    /// percentage of it) entirely at serve time from that surrogate's own
    /// already-fetched per-key results - no second surrogate, no
    /// ingest-time work, unlike lagInFrame-style window functions which
    /// need a value from a DIFFERENT row (previous-in-partition) and so
    /// genuinely need per-row ingest-time state.
    fn handle_grand_total_pct_sql(&self, query: &str, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_grand_total_pct_query(query)?;
        let surrogate = build_grand_total_pct_surrogate(&m);

        let (context, post) =
            self.build_query_execution_context_sql_with_post_processing(surrogate, None, time)?;
        let (output_labels, result) = self.execute_context(context, false, false)?;
        let result = post.apply(&output_labels, result);

        let QueryResult::Vector(InstantVector { values, timestamp, .. }) = result else {
            warn!("grand-total-pct handler: expected instant vector result");
            return None;
        };

        let total: f64 = values.iter().map(|e| e.value).sum();
        let factor = 10f64.powi(m.decimals as i32);

        let new_values: Vec<InstantVectorElement> = values
            .into_iter()
            .map(|element| {
                let agg_value = element.value;
                let pct = if total == 0.0 {
                    0.0
                } else {
                    ((agg_value * m.multiplier / total) * factor).round() / factor
                };
                let mut label_values = element.labels.labels;
                label_values.push(agg_value.to_string());
                InstantVectorElement::new(KeyByLabelValues::new_with_labels(label_values), pct)
            })
            .collect();

        let mut output_label_names = output_labels.labels;
        output_label_names.push(m.agg_alias.clone());
        let output_labels = KeyByLabelNames {
            labels: output_label_names,
        };

        let (order_by, limit) = parse_order_by_and_limit(&m.order_by_and_limit);
        let final_post = SqlPostProcessing {
            aggregation_alias: Some(m.pct_alias.clone()),
            order_by,
            limit,
            having: None,
        };
        let result = final_post.apply(&output_labels, QueryResult::vector(new_values, timestamp));

        Some((output_labels, result))
    }

    /// `row_number() OVER (PARTITION BY <bucket> ORDER BY <cnt> DESC) AS rnk
    /// ... WHERE rnk <= N`, see `BucketedTopNMatch`. The planner registered
    /// one aggregation (key column grouped, window size forced to the
    /// bucket size); this queries it once PER bucket, with the bucket's own
    /// [start, start+bucket_ms) substituted into a fresh copy of the
    /// surrogate each time - since that range exactly equals the
    /// registered window size, the ordinary execution path serves it as a
    /// single un-merged window (ordinary correctness already exercised by
    /// any classic query whose own duration equals its window size), so no
    /// new store-layer code is needed. Ranking and top-N selection happen
    /// here, in memory, per bucket's small already-fetched result.
    fn handle_bucketed_topn_sql(&self, query: &str, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        use chrono::{DateTime, NaiveDateTime, Utc};

        let m = parse_bucketed_topn_query(query)?;

        let start_ms = NaiveDateTime::parse_from_str(&m.start, "%Y-%m-%d %H:%M:%S")
            .ok()?
            .and_utc()
            .timestamp_millis();
        let end_ms = NaiveDateTime::parse_from_str(&m.end, "%Y-%m-%d %H:%M:%S")
            .ok()?
            .and_utc()
            .timestamp_millis();
        let bucket_ms = m.bucket_ms as i64;
        if bucket_ms <= 0 || start_ms >= end_ms {
            return None;
        }

        // hour -> Vec<(key, count)>, in first-seen (bucket) order so the
        // final ORDER BY hour, rnk renders buckets chronologically even
        // though HashMap iteration itself is unordered.
        let mut bucket_order: Vec<String> = Vec::new();
        let mut per_bucket: HashMap<String, Vec<(String, f64)>> = HashMap::new();

        let mut bucket_start_ms = start_ms;
        while bucket_start_ms < end_ms {
            let bucket_end_ms = (bucket_start_ms + bucket_ms).min(end_ms);
            let bucket_start_str = DateTime::<Utc>::from_timestamp_millis(bucket_start_ms)?
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();
            let bucket_end_str = DateTime::<Utc>::from_timestamp_millis(bucket_end_ms)?
                .format("%Y-%m-%d %H:%M:%S")
                .to_string();

            let bucket_from_where = m
                .from_where
                .replacen(&m.start, &bucket_start_str, 1)
                .replacen(&m.end, &bucket_end_str, 1);
            let surrogate = format!(
                "SELECT {key}, count(*) AS {alias} {from_where} GROUP BY {key}",
                key = m.key_col,
                alias = m.cnt_alias,
                from_where = bucket_from_where,
            );

            if let Some((output_labels, result)) = self
                .build_query_execution_context_sql_with_post_processing(surrogate, None, time)
                .and_then(|(context, post)| {
                    self.execute_context(context, false, false)
                        .map(|(labels, result)| (labels.clone(), post.apply(&labels, result)))
                })
            {
                if let QueryResult::Vector(InstantVector { values, .. }) = result {
                    if !values.is_empty() {
                        let key_idx = output_labels.labels.iter().position(|l| l == &m.key_col);
                        let rows: Vec<(String, f64)> = values
                            .into_iter()
                            .map(|element| {
                                let key_val = key_idx
                                    .and_then(|i| element.labels.labels.get(i).cloned())
                                    .unwrap_or_default();
                                (key_val, element.value)
                            })
                            .collect();
                        bucket_order.push(bucket_start_str.clone());
                        per_bucket.insert(bucket_start_str, rows);
                    }
                }
            }

            bucket_start_ms += bucket_ms;
        }

        let mut values: Vec<InstantVectorElement> = Vec::new();
        for bucket_label in &bucket_order {
            let mut rows = per_bucket.remove(bucket_label).unwrap_or_default();
            rows.sort_by(|a, b| {
                if m.descending {
                    b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                }
            });
            for (rnk, (key_val, cnt)) in rows.into_iter().take(m.n as usize).enumerate() {
                let rnk = (rnk + 1) as f64;
                let label_values = vec![bucket_label.clone(), key_val, cnt.to_string()];
                values.push(InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(label_values),
                    rnk,
                ));
            }
        }

        let output_labels = KeyByLabelNames {
            labels: vec![m.bucket_alias.clone(), m.key_col.clone(), m.cnt_alias.clone()],
        };

        let (order_by, limit) = parse_order_by_and_limit(&m.outer_order_by_and_limit);
        let final_post = SqlPostProcessing {
            aggregation_alias: Some(m.rnk_alias.clone()),
            order_by,
            limit,
            having: None,
        };
        let result = final_post.apply(
            &output_labels,
            QueryResult::vector(values, Self::convert_query_time_to_data_time(time)),
        );

        Some((output_labels, result))
    }

    /// `sum(<agg_alias>) OVER (ORDER BY <col>) AS <alias>` over a subquery
    /// that's itself the plain classic single-aggregate shape, see
    /// `RunningTotalMatch`. Executes the inner surrogate (unchanged - it IS
    /// the classic shape), sorts its own results into the window's own
    /// ORDER BY, walks a cumulative sum, then applies the OUTER query's own
    /// ORDER BY/LIMIT for final rendering (which may differ from the
    /// window's sort - the running total's VALUES are fixed by the window
    /// order; only their final display order can differ).
    fn handle_running_total_sql(&self, query: &str, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_running_total_query(query)?;
        let surrogate = build_running_total_surrogate(&m);

        let (context, _post) =
            self.build_query_execution_context_sql_with_post_processing(surrogate, None, time)?;
        let (output_labels, result) = self.execute_context(context, false, false)?;

        let QueryResult::Vector(InstantVector { mut values, timestamp, .. }) = result else {
            warn!("running-total handler: expected instant vector result");
            return None;
        };

        // window_order_col is always either the aggregate alias (the
        // element's numeric .value) or a group-by column (a label) - both
        // resolvable the same way sort_and_truncate_instant_vector does.
        if m.window_order_col == m.agg_alias {
            values.sort_by(|a, b| {
                b.value
                    .partial_cmp(&a.value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            if m.window_order_ascending {
                values.reverse();
            }
        } else if let Some(idx) = output_labels.labels.iter().position(|l| l == &m.window_order_col) {
            values.sort_by(|a, b| {
                let av = a.labels.labels.get(idx).map(String::as_str).unwrap_or("");
                let bv = b.labels.labels.get(idx).map(String::as_str).unwrap_or("");
                av.cmp(bv)
            });
            if !m.window_order_ascending {
                values.reverse();
            }
        }

        let mut running = 0.0;
        let new_values: Vec<InstantVectorElement> = values
            .into_iter()
            .map(|element| {
                let agg_value = element.value;
                running += agg_value;
                let mut label_values = element.labels.labels;
                label_values.push(agg_value.to_string());
                InstantVectorElement::new(KeyByLabelValues::new_with_labels(label_values), running)
            })
            .collect();

        let mut output_label_names = output_labels.labels;
        output_label_names.push(m.agg_alias.clone());
        let output_labels = KeyByLabelNames {
            labels: output_label_names,
        };

        let (order_by, limit) = parse_order_by_and_limit(&m.outer_order_by_and_limit);
        let final_post = SqlPostProcessing {
            aggregation_alias: Some(m.running_alias.clone()),
            order_by,
            limit,
            having: None,
        };
        let result = final_post.apply(&output_labels, QueryResult::vector(new_values, timestamp));

        Some((output_labels, result))
    }

    /// `SELECT <cols> FROM ... GROUP BY <same cols> HAVING count(*) <op>
    /// <n>` - no aggregate anywhere in the SELECT list, see
    /// `GroupByHavingCountMatch` / the planner's own handling of this
    /// shape. Builds and executes the same `count(*) AS __having_count__
    /// ... HAVING __having_count__ <op> <n>` surrogate the planner
    /// registered - reusing the ordinary classic single-aggregate + HAVING
    /// serving path unchanged - then drops the hidden count column from
    /// the result (`vector_without_value`, the same mechanism SELECT
    /// DISTINCT uses), since the original query never asked for it.
    fn handle_group_by_having_count_sql(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_group_by_having_count_query(query)?;
        let surrogate = build_group_by_having_count_surrogate(&m);

        let (context, post) =
            self.build_query_execution_context_sql_with_post_processing(surrogate, None, time)?;
        let (output_labels, result) = self.execute_context(context, false, false)?;
        let result = post.apply(&output_labels, result);

        let QueryResult::Vector(InstantVector { values, timestamp, .. }) = result else {
            warn!("group-by-having-count handler: expected instant vector result");
            return None;
        };

        Some((
            output_labels,
            QueryResult::vector_without_value(values, timestamp),
        ))
    }

    /// `SELECT <col>, <agg>(<arg>) AS <alias> FROM ... GROUP BY <col>
    /// HAVING countIf(<cond1>) <op1> <n1> [AND countIf(<cond2>) <op2>
    /// <n2> ...]` - see `HiddenCountifHavingMatch`'s doc comment (q113's
    /// "peers with only withdrawals, no announcements" shape). Executes
    /// the exposed aggregate's own surrogate plus one per hidden countIf
    /// (the same split the planner registered - `build_hidden_countif_having_surrogates`
    /// is shared between both sides), filters the exposed rows by the
    /// hidden branches' resolved values, and renders a result shaped
    /// exactly like an ordinary classic single-aggregate query - the
    /// hidden branches never appear as output columns, only as a filter.
    fn handle_hidden_countif_having_sql(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_hidden_countif_having_query(query)?;
        let surrogates = build_hidden_countif_having_surrogates(&m);

        let mut per_surrogate: Vec<(KeyByLabelNames, InstantVector)> = Vec::new();
        for surrogate in &surrogates {
            let (context, post) = self.build_query_execution_context_sql_with_post_processing(
                surrogate.clone(),
                None,
                time,
            )?;
            let (output_labels, result) = self.execute_context(context, false, false)?;
            let result = post.apply(&output_labels, result);
            let QueryResult::Vector(vector) = result else {
                warn!("hidden-countIf-HAVING handler: expected instant vector result");
                return None;
            };
            per_surrogate.push((output_labels, vector));
        }

        let (exposed_labels, exposed_vector) = per_surrogate.remove(0);
        let end_timestamp = exposed_vector.timestamp;

        let hidden_maps: Vec<HashMap<Vec<String>, f64>> = per_surrogate
            .iter()
            .map(|(_labels, vector)| {
                vector
                    .values
                    .iter()
                    .map(|e| (e.labels.labels.clone(), e.value))
                    .collect()
            })
            .collect();

        let values: Vec<InstantVectorElement> = exposed_vector
            .values
            .into_iter()
            .filter(|element| {
                m.hidden.iter().zip(hidden_maps.iter()).all(|((_, op, threshold), map)| {
                    let actual = map.get(&element.labels.labels).copied().unwrap_or(0.0);
                    match op.as_str() {
                        ">" => actual > *threshold,
                        ">=" => actual >= *threshold,
                        "<" => actual < *threshold,
                        "<=" => actual <= *threshold,
                        "=" => actual == *threshold,
                        "!=" | "<>" => actual != *threshold,
                        _ => true,
                    }
                })
            })
            .collect();

        let (order_by, limit) = parse_order_by_and_limit(&m.order_by_and_limit);
        let post = SqlPostProcessing {
            aggregation_alias: Some(m.alias.clone()),
            order_by,
            limit,
            having: None,
        };
        let result = post.apply(&exposed_labels, QueryResult::vector(values, end_timestamp));

        Some((exposed_labels, result))
    }

    /// `SELECT <outer_col>, count(*) AS <outer_alias> FROM (SELECT
    /// <inner_col>, count(*) AS <inner_alias> FROM ... GROUP BY
    /// <inner_col>) GROUP BY <outer_col>` - see `TwoStageHistogramMatch`'s
    /// doc comment (q078's "how many prefixes had N updates" shape). The
    /// inner query is executed through the ordinary classic pipeline
    /// (exactly as if it had been asked for standalone), then its own
    /// result rows are locally re-aggregated into a histogram - no second
    /// precompute read at all, since the inner query's result set already
    /// carries everything the outer stage needs.
    fn handle_two_stage_histogram_sql(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_two_stage_histogram_query(query)?;
        let inner_surrogate = build_two_stage_histogram_inner_surrogate(&m);

        let (context, post) = self.build_query_execution_context_sql_with_post_processing(
            inner_surrogate,
            None,
            time,
        )?;
        let (_inner_labels, inner_result) = self.execute_context(context, false, false)?;
        let inner_result = post.apply(&_inner_labels, inner_result);
        let QueryResult::Vector(inner_vector) = inner_result else {
            warn!("two-stage histogram handler: expected instant vector result");
            return None;
        };

        let mut histogram: HashMap<String, f64> = HashMap::new();
        for element in &inner_vector.values {
            let bucket = element.value.to_string();
            *histogram.entry(bucket).or_insert(0.0) += 1.0;
        }

        let values: Vec<InstantVectorElement> = histogram
            .into_iter()
            .map(|(bucket, tally)| {
                InstantVectorElement::new(KeyByLabelValues::new_with_labels(vec![bucket]), tally)
            })
            .collect();

        let output_labels = KeyByLabelNames::new(vec![m.outer_group_col.clone()]);
        let (order_by, limit) = parse_order_by_and_limit(&m.order_by_and_limit);
        let post = SqlPostProcessing {
            aggregation_alias: Some(m.outer_alias.clone()),
            order_by,
            limit,
            having: None,
        };
        let result = post.apply(
            &output_labels,
            QueryResult::vector(values, inner_vector.timestamp),
        );

        Some((output_labels, result))
    }

    /// Resolves a query text through the same small set of handlers used
    /// to *register* each half of a correlated-IN-subquery split (see
    /// `handle_correlated_in_subquery_sql`) - SELECT DISTINCT, argMax,
    /// then the ordinary classic pipeline (which already covers plain
    /// GROUP BY aggregates and top-k internally). Not the full
    /// `handle_query_sql` dispatch chain: only these three shapes are
    /// ever registered for a correlated-subquery half (see
    /// `get_streaming_aggregation_configs`'s dispatch order and
    /// `normalize_bare_group_by_query`'s doc comment on the planner side),
    /// so trying the rest would just be wasted work.
    fn resolve_correlated_half(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        // `handle_query_sql` always runs this before its own dispatch cascade
        // (see its `let query = self.rewrite_recognized_pattern(&query);`) -
        // an ArgMax half needs the same treatment here, since `handle_arg_agg_sql`
        // -> `try_execute_arg_agg_branch` parses its input directly with no
        // table-name swap of its own; `rewrite_arg_agg_query` (called from
        // within this cascade) is what points it at the derived-value table
        // the planner actually registered the aggregation under. Skipping
        // this made the ArgMax half silently fall through to the generic
        // classic path below, which doesn't know ArgMax's "pack the arg
        // value as an extra label" result shape - producing labels that
        // don't contain the join column at all and making the caller's
        // later `.position(|l| l == &m.join_col)` fail.
        let query = self.rewrite_recognized_pattern(query);
        let query = query.as_str();
        if let Some(result) = self.handle_select_distinct_sql(query, time) {
            return Some(result);
        }
        if let Some(result) = self.handle_arg_agg_sql(query, time) {
            return Some(result);
        }
        let (context, post) = self.build_query_execution_context_sql_with_post_processing(
            query.to_string(),
            None,
            time,
        )?;
        let (output_labels, result) = self.execute_context(context, false, false)?;
        let result = post.apply(&output_labels, result);
        Some((output_labels, result))
    }

    /// `<col> [NOT] IN (SELECT ...)` as a top-level WHERE/HAVING clause -
    /// see `CorrelatedInSubqueryMatch`'s doc comment (q089/q129/q141/q160's
    /// "outer query filtered by set membership in an independently-
    /// summarizable inner query" shape). Both halves are read completely
    /// independently through `resolve_correlated_half` - each was
    /// registered as its own standalone precompute, with no relationship
    /// to each other in the store - and joined here in memory: the
    /// inner's own result rows become a membership set (keyed by whichever
    /// label position matches the join column), and the outer's rows are
    /// filtered by membership (or non-membership, for NOT IN).
    fn handle_correlated_in_subquery_sql(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_correlated_in_subquery_query(query)?;

        let (inner_labels, inner_result) = self.resolve_correlated_half(&m.inner_query, time)?;
        let inner_col_pos = inner_labels.labels.iter().position(|l| l == &m.join_col)?;
        let QueryResult::Vector(inner_vector) = inner_result else {
            warn!("correlated-IN-subquery handler: expected instant vector result (inner)");
            return None;
        };
        let members: HashSet<String> = inner_vector
            .values
            .iter()
            .filter_map(|e| e.labels.labels.get(inner_col_pos).cloned())
            .collect();

        let (outer_labels, outer_result) = self.resolve_correlated_half(&m.outer_query, time)?;
        let outer_col_pos = outer_labels.labels.iter().position(|l| l == &m.join_col)?;
        let QueryResult::Vector(outer_vector) = outer_result else {
            warn!("correlated-IN-subquery handler: expected instant vector result (outer)");
            return None;
        };

        let has_value = outer_vector.has_value;
        let end_timestamp = outer_vector.timestamp;
        let values: Vec<InstantVectorElement> = outer_vector
            .values
            .into_iter()
            .filter(|element| {
                let is_member = element
                    .labels
                    .labels
                    .get(outer_col_pos)
                    .is_some_and(|v| members.contains(v));
                if m.negated {
                    !is_member
                } else {
                    is_member
                }
            })
            .collect();

        let result = if has_value {
            QueryResult::vector(values, end_timestamp)
        } else {
            QueryResult::vector_without_value(values, end_timestamp)
        };

        Some((outer_labels, result))
    }

    /// `SELECT <week_col>, count(*) AS <outer_alias> FROM (SELECT
    /// toStartOfWeek(<time_col>) AS <week_col>, <prefix_col>,
    /// uniqExact(<origin_col>) AS <cnt_alias> FROM ... GROUP BY <week_col>,
    /// <prefix_col> HAVING <cnt_alias> > <n>) GROUP BY <week_col>` - see
    /// `WeeklyMoasHistogramMatch`'s doc comment (q133's "how many prefixes
    /// were MOAS'd each week" shape). Reads the registered SetAggregator
    /// exactly like `handle_moas_sql` does (one exact origin-set per
    /// (week, prefix) key), but tallies a histogram over `week_col`
    /// instead of rendering each qualifying key's own origin list -
    /// there's no per-prefix output here, only a per-week count of how
    /// many prefixes passed the origins-count HAVING filter.
    fn handle_weekly_moas_histogram_sql(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_weekly_moas_histogram_query(query)?;
        let surrogate = build_weekly_moas_histogram_inner_surrogate(&m);

        let schema = match &self.inference_config.read().unwrap().schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => return None,
            &SchemaConfig::ElasticQueryDSL(_) => return None,
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
        };

        let statements = parser::parse_sql(&GenericDialect {}, surrogate.as_str()).ok()?;
        let query_data = SQLPatternParser::new(&schema, time).parse_query(&statements)?;
        let query_config = self.find_query_config_sql(&query_data)?;
        let aggregation_id = query_config.aggregations.first()?.aggregation_id;

        let end_timestamp = self.align_end_timestamp_sql(Self::convert_query_time_to_data_time(
            query_data.time_info.get_start() + query_data.time_info.get_duration(),
        ));
        let duration_ms = (query_data.time_info.get_duration() * 1000.0).round() as u64;
        let start_timestamp = end_timestamp.checked_sub(duration_ms)?;

        let params = StoreQueryParams {
            metric: query_data.metric.clone(),
            aggregation_id,
            start_timestamp,
            end_timestamp,
            is_exact_query: false,
        };

        let timestamped_map = self
            .execute_store_query(&params)
            .map_err(|e| {
                warn!("weekly-MOAS-histogram handler: store query failed: {}", e);
                e
            })
            .ok()?;

        // Registration groups by [week_col, prefix_col] via
        // KeyByLabelNames::new (alphabetically sorted), so the group key's
        // own label positions must be resolved the same way rather than
        // assumed - see try_execute_group_array_distinct_branch and
        // handle_correlated_in_subquery_sql for the same "look up by name,
        // not position" reasoning.
        let grouping = KeyByLabelNames::new(vec![m.week_col.clone(), m.prefix_col.clone()]);
        let week_pos = grouping.labels.iter().position(|l| l == &m.week_col)?;

        let mut histogram: HashMap<String, f64> = HashMap::new();
        for (group_key, timestamped_buckets) in timestamped_map {
            let Some(group_key_values) = group_key else {
                continue;
            };
            let Some(week_value) = group_key_values.get(week_pos) else {
                continue;
            };
            let week_value = week_value.clone();

            let mut origins: HashSet<String> = HashSet::new();
            for (_bucket, precompute) in timestamped_buckets {
                if let Some(keys) = precompute.get_keys() {
                    for key in keys {
                        if let Some(origin) = key.get(0) {
                            if !origin.is_empty() {
                                origins.insert(origin.to_string());
                            }
                        }
                    }
                }
            }

            let count = origins.len() as f64;
            let passes = match m.having_op.as_str() {
                ">" => count > m.having_threshold,
                ">=" => count >= m.having_threshold,
                "<" => count < m.having_threshold,
                "<=" => count <= m.having_threshold,
                "=" => count == m.having_threshold,
                "!=" | "<>" => count != m.having_threshold,
                _ => false,
            };
            if passes {
                *histogram.entry(week_value).or_insert(0.0) += 1.0;
            }
        }

        let values: Vec<InstantVectorElement> = histogram
            .into_iter()
            .map(|(week, tally)| {
                InstantVectorElement::new(KeyByLabelValues::new_with_labels(vec![week]), tally)
            })
            .collect();

        let output_labels = KeyByLabelNames::new(vec![m.week_col.clone()]);
        let (order_by, limit) = parse_order_by_and_limit(&m.order_by_and_limit);
        let post = SqlPostProcessing {
            aggregation_alias: Some(m.outer_alias.clone()),
            order_by,
            limit,
            having: None,
        };
        let result = post.apply(&output_labels, QueryResult::vector(values, end_timestamp));

        Some((output_labels, result))
    }

    /// Finds the query configuration for a bucketed countIf SQL query.
    ///
    /// This is parallel to `find_query_config_sql`: the classic SQL path matches
    /// one aggregate, while bucketed countIf has one time-bucket expression and
    /// multiple independent count outputs.
    fn find_query_config_sql_bucketed(
        &self,
        query_data: &SQLBucketedCountIfQueryData,
    ) -> Option<QueryConfig> {
        let ic = self.inference_config.read().unwrap();
        let schema = match &ic.schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
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

                let template_data = match SQLPatternParser::new(&schema, 0.0)
                    .parse_bucketed_countif_query(&template_statements)
                {
                    Some(data) => data,
                    None => return false,
                };

                query_data.matches_bucketed_pattern(&template_data)
            })
            .cloned()
    }

    fn handle_bucketed_countif_sql(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let schema = match &self.inference_config.read().unwrap().schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => return None,
            &SchemaConfig::ElasticQueryDSL(_) => return None,
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
        };

        let statements = match parser::parse_sql(&GenericDialect {}, query) {
            Ok(statements) => statements,
            Err(_) => return None,
        };

        let bucketed =
            SQLPatternParser::new(&schema, time).parse_bucketed_countif_query(&statements)?;

        let query_config = self.find_query_config_sql_bucketed(&bucketed)?;

        if query_config.aggregations.len() < bucketed.outputs.len() {
            warn!(
                "Bucketed countIf query has {} outputs but query_config only has {} aggregation refs",
                bucketed.outputs.len(),
                query_config.aggregations.len()
            );
            return None;
        }

        let end_timestamp = self.align_end_timestamp_sql(Self::convert_query_time_to_data_time(
            bucketed.time_info.get_start() + bucketed.time_info.get_duration(),
        ));
        let duration_ms = (bucketed.time_info.get_duration() * 1000.0).round() as u64;
        let raw_start_timestamp = end_timestamp.checked_sub(duration_ms)?;

        // The precompute store keys tumbling windows by bucket_ms-aligned
        // absolute timestamps (bucket_start_ts = floor((t - offset) /
        // bucket_ms) * bucket_ms + offset - see WindowManager::window_start_for,
        // which this mirrors; offset is nonzero only for toStartOfWeek, see
        // its doc comment). raw_start_timestamp is only guaranteed aligned to
        // data_ingestion_interval_ms via align_end_timestamp_sql above, which can
        // differ from bucket_ms. Without this, the read loop below probes
        // timestamps that never land on a real stored key and every sample
        // silently reads back as 0.0.
        let start_timestamp = if bucketed.bucket_ms == 0 {
            raw_start_timestamp
        } else {
            let offset = bucketed.bucket_offset_ms;
            ((raw_start_timestamp - offset) / bucketed.bucket_ms) * bucketed.bucket_ms + offset
        };

        let mut range_elements = Vec::new();

        for (idx, output) in bucketed.outputs.iter().enumerate() {
            let aggregation_id = query_config.aggregations[idx].aggregation_id;
            let statistic = if output.cardinality_column.is_some() {
                Statistic::Cardinality
            } else {
                Statistic::Count
            };

            let params = StoreQueryParams {
                metric: bucketed.metric.clone(),
                aggregation_id,
                start_timestamp,
                end_timestamp,
                is_exact_query: false,
            };

            let timestamped_map = self
                .execute_store_query(&params)
                .map_err(|e| {
                    warn!(
                        "Failed to execute bucketed countIf store query for '{}': {}",
                        output.alias, e
                    );
                    e
                })
                .ok()?;

            let mut values_by_bucket: HashMap<u64, f64> = HashMap::new();
            for (_key, timestamped_buckets) in timestamped_map {
                for ((bucket_start_ts, _bucket_end_ts), precompute) in timestamped_buckets {
                    let scalar_key = Some(KeyByLabelValues::new_with_labels(vec![]));

                    let value = self
                        .query_precompute_for_statistic(
                            precompute.as_ref(),
                            &statistic,
                            &scalar_key,
                            &HashMap::new(),
                        )
                        .map_err(|e| {
                            warn!(
                                "Failed to query bucketed countIf precompute for '{}': {}",
                                output.alias, e
                            );
                            e
                        })
                        .ok()?;

                    values_by_bucket.insert(bucket_start_ts, value);
                }
            }

            let mut element =
                RangeVectorElement::new(KeyByLabelValues::new_with_labels(vec![output
                    .alias
                    .clone()]));

            let mut ts = start_timestamp;
            while ts < end_timestamp {
                let value = values_by_bucket.get(&ts).copied().unwrap_or(0.0);
                element.add_sample(ts, value);
                ts += bucketed.bucket_ms;
            }

            range_elements.push(element);
        }

        // ORDER BY / LIMIT post-processing: every element in
        // `range_elements` shares the same time-ordered sample sequence by
        // construction (the loop above walks the same bucket timestamps
        // for each output), so a single permutation - computed once
        // against whichever output the ORDER BY names, or against the
        // timestamps themselves for the bucket column - applies identically
        // to every element and keeps them aligned by position.
        if !range_elements.is_empty() {
            let n = range_elements[0].samples.len();
            let mut order: Vec<usize> = (0..n).collect();

            if let Some(item) = bucketed.order_by.first() {
                let out_idx = bucketed.outputs.iter().position(|o| o.alias == item.column);
                let value_of = |i: usize| -> f64 {
                    match out_idx {
                        Some(idx) => range_elements[idx].samples[i].value,
                        None => range_elements[0].samples[i].timestamp as f64,
                    }
                };
                order.sort_by(|&a, &b| {
                    let ord = value_of(a)
                        .partial_cmp(&value_of(b))
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if item.ascending {
                        ord
                    } else {
                        ord.reverse()
                    }
                });
            }

            if let Some(limit) = bucketed.limit {
                order.truncate(limit as usize);
            }

            let is_identity = order.len() == n && order.iter().enumerate().all(|(i, &o)| i == o);
            if !is_identity {
                for element in &mut range_elements {
                    element.samples = order.iter().map(|&i| element.samples[i].clone()).collect();
                }
            }
        }

        let output_labels = KeyByLabelNames::new(vec!["output".to_string()]);
        let result = if bucketed.bucket_is_date {
            QueryResult::matrix_with_date_buckets(range_elements)
        } else {
            QueryResult::matrix(range_elements)
        };
        Some((output_labels, result))
    }

    /// Alias of a `<expr> AS <alias>` SELECT-list item, e.g.
    /// "uniqExact(prefix) AS distinct_prefixes" -> "distinct_prefixes".
    fn alias_of(expr: &str) -> Option<String> {
        let lower = expr.to_lowercase();
        let as_idx = lower.rfind(" as ")?;
        Some(expr[as_idx + 4..].trim().to_string())
    }

    /// Serves a derived-ratio query (e.g. `countIf(op='W') / greatest(countIf(op='A'), 1)
    /// AS ratio`, or `count(*) / 6.0 AS avg_per_hour`) by running its 1-2
    /// underlying aggregates as independent single-aggregate surrogates -
    /// same split/registration `handle_multi_aggregate_sql` uses - then
    /// computing the ratio (and, if present, the `<alias>+<alias> <op> <n>`
    /// HAVING) from their joined per-key results. A key present in one
    /// surrogate's output but not the other's (e.g. a peer with
    /// announcements but zero withdrawals) is treated as 0 on the missing
    /// side, matching countIf's own "no matching rows -> 0" semantics.
    fn handle_derived_ratio_sql(&self, query: &str, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_derived_ratio_query(query)?;
        let surrogates = build_derived_ratio_surrogates(&m);

        let mut per_surrogate: Vec<(KeyByLabelNames, InstantVector)> = Vec::new();
        for surrogate in &surrogates {
            let Some((context, post)) = self.build_query_execution_context_sql_with_post_processing(
                surrogate.clone(),
                None,
                time,
            ) else {
                warn!("derived-ratio handler: failed to build execution context for surrogate");
                return None;
            };
            let Some((output_labels, result)) = self.execute_context(context, false, false) else {
                warn!("derived-ratio handler: failed to execute context for surrogate");
                return None;
            };
            let result = post.apply(&output_labels, result);
            let QueryResult::Vector(vector) = result else {
                warn!("derived-ratio handler: expected instant vector result");
                return None;
            };
            per_surrogate.push((output_labels, vector));
        }

        // surrogates[0] is always the numerator (build_derived_ratio_surrogates'
        // fixed order); surrogates[1], if present, is the denominator.
        let (numerator_labels, numerator_vector) = &per_surrogate[0];
        let end_timestamp = numerator_vector.timestamp;
        let group_label_names = numerator_labels.labels.clone();

        let numerator_map: HashMap<Vec<String>, f64> = numerator_vector
            .values
            .iter()
            .map(|e| (e.labels.labels.clone(), e.value))
            .collect();
        let denominator_map: Option<HashMap<Vec<String>, f64>> = per_surrogate
            .get(1)
            .map(|(_, vector)| {
                vector
                    .values
                    .iter()
                    .map(|e| (e.labels.labels.clone(), e.value))
                    .collect()
            });

        let mut all_keys: HashSet<Vec<String>> = numerator_map.keys().cloned().collect();
        if let Some(dm) = &denominator_map {
            all_keys.extend(dm.keys().cloned());
        }

        let mut output_label_names = group_label_names;
        if m.numerator.exposed_alias.is_some() {
            output_label_names.push(Self::alias_of(&surrogates[0]).unwrap_or_default());
        }
        if let RatioDenominator::Aggregate { component, .. } = &m.denominator {
            if component.exposed_alias.is_some() {
                output_label_names.push(Self::alias_of(&surrogates[1]).unwrap_or_default());
            }
        }

        let mut values: Vec<InstantVectorElement> = Vec::new();
        for key in all_keys {
            let numerator_value = numerator_map.get(&key).copied().unwrap_or(0.0);
            let denominator_raw = match &m.denominator {
                RatioDenominator::Constant(_) => None,
                RatioDenominator::Aggregate { .. } => Some(
                    denominator_map
                        .as_ref()
                        .and_then(|dm| dm.get(&key))
                        .copied()
                        .unwrap_or(0.0),
                ),
            };

            if let Some((op, threshold)) = &m.having_sum {
                // Only reachable when the denominator is an exposed
                // aggregate (see parse_derived_ratio_query), so
                // denominator_raw is always Some here.
                let sum = numerator_value + denominator_raw.unwrap_or(0.0);
                let keep = match op.as_str() {
                    ">" => sum > *threshold,
                    ">=" => sum >= *threshold,
                    "<" => sum < *threshold,
                    "<=" => sum <= *threshold,
                    "=" => sum == *threshold,
                    "!=" | "<>" => sum != *threshold,
                    _ => true,
                };
                if !keep {
                    continue;
                }
            }

            let denominator_value = match &m.denominator {
                RatioDenominator::Constant(c) => *c,
                RatioDenominator::Aggregate { floor, .. } => {
                    let raw = denominator_raw.unwrap_or(0.0);
                    floor.map_or(raw, |f| raw.max(f))
                }
            };
            let mut ratio = if denominator_value == 0.0 {
                0.0
            } else {
                (numerator_value * m.multiplier) / denominator_value
            };
            if let Some(decimals) = m.decimals {
                let factor = 10f64.powi(decimals as i32);
                ratio = (ratio * factor).round() / factor;
            }

            let mut label_values = key.clone();
            if m.numerator.exposed_alias.is_some() {
                label_values.push(numerator_value.to_string());
            }
            if let RatioDenominator::Aggregate { component, .. } = &m.denominator {
                if component.exposed_alias.is_some() {
                    label_values.push(denominator_raw.unwrap_or(0.0).to_string());
                }
            }
            values.push(InstantVectorElement::new(
                KeyByLabelValues::new_with_labels(label_values),
                ratio,
            ));
        }

        let output_labels = KeyByLabelNames {
            labels: output_label_names,
        };

        let (order_by, limit) = parse_order_by_and_limit(&m.order_by_and_limit);
        let post = SqlPostProcessing {
            aggregation_alias: Some(m.ratio_alias.clone()),
            order_by,
            limit,
            having: None,
        };
        let result = post.apply(&output_labels, QueryResult::vector(values, end_timestamp));

        Some((output_labels, result))
    }

    /// Serves a multi-aggregate classic query (e.g. `SELECT k1, k2, count()
    /// AS a, uniqExact(v) AS b FROM ... GROUP BY k1, k2`) by splitting it
    /// into N independent single-aggregate surrogates - the same split the
    /// planner uses to plan and separately register each one (see
    /// generate_sql_plan in the planner crate) - and running each through
    /// the existing single-aggregate execution path unchanged. Each
    /// surrogate's own text is what's registered in inference_config.yaml,
    /// so the ordinary structural matcher (find_query_config_sql) finds it
    /// directly. Results are merged back into one row per group key: the
    /// first aggregate's value stays the primary numeric value, and every
    /// other aggregate's value is appended to the output labels (matching
    /// the format handle_moas_sql already established for "more than one
    /// output value per row").
    fn handle_multi_aggregate_sql(
        &self,
        query: &str,
        computed_group_by_alias: Option<&str>,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let m = parse_multi_aggregate_query(query)?;

        warn!(
            "multi-aggregate handler matched query with {} aggregates",
            m.aggregate_exprs.len()
        );

        let surrogates = build_multi_aggregate_surrogates(&m);

        // At least one branch is non-numeric (ARGMAX/ARGMIN or
        // GROUPARRAY(DISTINCT ...)) - the "last aggregate becomes the
        // row's primary f64 value" design below has no numeric value to
        // promote in that case, so route to the dedicated all-labels
        // combiner instead. Existing all-numeric multi-aggregate queries
        // are unaffected: this check is false for every one of them, and
        // they keep taking the unchanged code path below.
        if m.aggregate_exprs
            .iter()
            .any(|e| Self::is_arg_agg_expr(e) || Self::is_group_array_distinct_expr(e))
        {
            return self.handle_multi_aggregate_all_labels_sql(
                &m,
                &surrogates,
                computed_group_by_alias,
                time,
            );
        }

        let mut per_aggregate: Vec<(KeyByLabelNames, InstantVector)> = Vec::new();
        for surrogate in &surrogates {
            // avg(...) has no first-class Statistic (it's always [Sum, Count]
            // under the hood - see try_execute_avg_branch's doc comment) and
            // can't go through the ordinary single-aggregate execution path
            // at all. Detect it here, per-branch, the same way ARGMAX/
            // groupArray(DISTINCT ...) are special-cased in
            // handle_multi_aggregate_all_labels_sql, so a query like
            // `avg(x), count(*)` still gets its avg branch as one ordinary
            // (labels, InstantVector) entry alongside the count branch.
            if surrogate.to_lowercase().contains("avg(") {
                let Some((output_labels, vector)) =
                    self.try_execute_avg_branch(surrogate, computed_group_by_alias, time)
                else {
                    warn!("multi-aggregate handler: failed to compute avg branch for surrogate");
                    return None;
                };
                per_aggregate.push((output_labels, vector));
                continue;
            }

            let Some((context, post)) = self.build_query_execution_context_sql_with_post_processing(
                surrogate.clone(),
                computed_group_by_alias,
                time,
            ) else {
                warn!("multi-aggregate handler: failed to build execution context for surrogate");
                return None;
            };
            let Some((output_labels, result)) = self.execute_context(context, false, false) else {
                warn!("multi-aggregate handler: failed to execute context for surrogate");
                return None;
            };
            let result = post.apply(&output_labels, result);
            let QueryResult::Vector(vector) = result else {
                warn!("multi-aggregate handler: expected instant vector result");
                return None;
            };
            per_aggregate.push((output_labels, vector));
        }

        // The HTTP renderer (format_success_response in clickhouse_http.rs)
        // always writes `output_labels` columns first and the
        // InstantVectorElement's numeric `value` field last. To reproduce
        // the original SELECT list's column order - group-by columns, then
        // every aggregate expression in its original position - the LAST
        // aggregate's value has to be the one that lands in `value`; every
        // earlier aggregate becomes an ordinary label column instead,
        // appended (in original order) right after the group-by columns.
        // Putting `aggregate_exprs[0]` in `value` here previously rendered
        // it *last* instead of first whenever there were 2+ extra aliases -
        // a silent column-order swap caught by q094's countIf pair.
        let last = per_aggregate.pop()?;
        let (last_labels, last_vector) = last;
        let end_timestamp = last_vector.timestamp;

        // The grouping-label *order* here comes from whichever surrogate's
        // own execution context produced it (KeyByLabelNames may reorder
        // relative to the SELECT list), not from `m.group_by_cols` - every
        // surrogate shares the same grouping structure, so any one of them
        // (here, the first remaining, falling back to the popped last if
        // there was only one surrogate to begin with) determines it.
        let group_label_names = per_aggregate
            .first()
            .map(|(labels, _)| labels.labels.clone())
            .unwrap_or_else(|| last_labels.labels.clone());

        let extra_maps: Vec<HashMap<Vec<String>, f64>> = per_aggregate
            .iter()
            .map(|(_labels, vector)| {
                vector
                    .values
                    .iter()
                    .map(|element| (element.labels.labels.clone(), element.value))
                    .collect()
            })
            .collect();

        let mut output_label_names = group_label_names;
        for expr in m.aggregate_exprs.iter().take(m.aggregate_exprs.len() - 1) {
            output_label_names.push(Self::alias_of(expr).unwrap_or_else(|| expr.clone()));
        }
        // Deliberately not KeyByLabelNames::new(...): that constructor sorts
        // alphabetically, but the values below are built in this exact
        // (group-columns-then-extra-aliases) order, not alphabetical order -
        // ORDER BY resolution (sort_and_truncate_instant_vector) looks a
        // column up by *position* in this list and indexes into the values
        // with that position, so the two must stay in lockstep.
        let output_labels = KeyByLabelNames {
            labels: output_label_names,
        };

        let mut values: Vec<InstantVectorElement> = last_vector
            .values
            .into_iter()
            .map(|element| {
                let mut label_values = element.labels.labels.clone();
                for map in &extra_maps {
                    let v = map.get(&element.labels.labels).copied().unwrap_or(0.0);
                    label_values.push(v.to_string());
                }
                InstantVectorElement::new(KeyByLabelValues::new_with_labels(label_values), element.value)
            })
            .collect();

        // Resolves one exposed alias's value for a single row - either the
        // last aggregate (element.value) or one of the earlier ones
        // (a label, by position in output_label_names). Shared by HAVING
        // filtering and the order_by_sum derived sort key below; both need
        // "look up any of this query's own aliases per row", which
        // SqlPostProcessing's generic aggregation_alias/label resolution
        // doesn't cover (it only knows about ONE alias, the primary value).
        let last_alias = Self::alias_of(&m.aggregate_exprs[m.aggregate_exprs.len() - 1]);
        let resolve_alias = |element: &InstantVectorElement, alias: &str| -> f64 {
            if last_alias.as_deref() == Some(alias) {
                element.value
            } else {
                output_labels
                    .labels
                    .iter()
                    .position(|l| l == alias)
                    .and_then(|pos| element.labels.labels.get(pos))
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0)
            }
        };

        if !m.having.is_empty() {
            values.retain(|element| {
                m.having.iter().all(|(alias, op, threshold)| {
                    let actual = resolve_alias(element, alias);
                    match op.as_str() {
                        ">" => actual > *threshold,
                        ">=" => actual >= *threshold,
                        "<" => actual < *threshold,
                        "<=" => actual <= *threshold,
                        "=" => actual == *threshold,
                        "!=" | "<>" => actual != *threshold,
                        _ => true,
                    }
                })
            });
        }

        warn!("multi-aggregate handler: produced {} rows", values.len());

        let (mut order_by, limit) = parse_order_by_and_limit(&m.order_by_and_limit);
        if let Some((alias1, alias2, descending)) = &m.order_by_sum {
            values.sort_by(|a, b| {
                let av = resolve_alias(a, alias1) + resolve_alias(a, alias2);
                let bv = resolve_alias(b, alias1) + resolve_alias(b, alias2);
                let ord = av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal);
                if *descending { ord.reverse() } else { ord }
            });
            // Already sorted above - SqlPostProcessing's generic order_by
            // resolution can't express a summed-alias key, so don't hand it
            // one; it still applies LIMIT below.
            order_by.clear();
        }
        let post = SqlPostProcessing {
            aggregation_alias: last_alias,
            order_by,
            limit,
            having: None,
        };
        let result = post.apply(&output_labels, QueryResult::vector(values, end_timestamp));

        Some((output_labels, result))
    }

    /// True when `expr` is an ARGMAX/ARGMIN aggregate expression - one of
    /// two non-numeric multi-aggregate branch shapes (see
    /// `handle_multi_aggregate_all_labels_sql`'s doc comment; the other is
    /// `is_group_array_distinct_expr`).
    fn is_arg_agg_expr(expr: &str) -> bool {
        let upper = expr.to_uppercase();
        upper.contains("ARGMAX(") || upper.contains("ARGMIN(")
    }

    /// True when `expr` is a `groupArray(DISTINCT ...)` aggregate
    /// expression - the other non-numeric multi-aggregate branch shape
    /// (see `is_arg_agg_expr`).
    fn is_group_array_distinct_expr(expr: &str) -> bool {
        let upper = expr.to_uppercase();
        upper.contains("GROUPARRAY(") && upper.contains("DISTINCT")
    }

    /// Routed to from `handle_multi_aggregate_sql` when at least one
    /// aggregate expression is non-numeric (ARGMAX/ARGMIN or
    /// GROUPARRAY(DISTINCT ...)). Unlike the numeric path - which promotes
    /// the last aggregate's f64 `value` and demotes every earlier one to a
    /// label column - there is no numeric value left to promote once any
    /// branch is string-valued, so EVERY branch's result becomes a
    /// trailing label column instead, mirroring
    /// `handle_select_distinct_sql`'s labels-only rendering
    /// (`vector_without_value`/`has_value: false`). Numeric branches are
    /// stringified via `.to_string()` so all columns line up as strings.
    fn handle_multi_aggregate_all_labels_sql(
        &self,
        m: &MultiAggregateMatch,
        surrogates: &[String],
        computed_group_by_alias: Option<&str>,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let mut group_label_names: Option<Vec<String>> = None;
        let mut end_timestamp: u64 = 0;
        // One HashMap<group-key, stringified-branch-value> per aggregate
        // expression, in the same order as m.aggregate_exprs/surrogates.
        let mut branch_maps: Vec<HashMap<Vec<String>, String>> = Vec::new();

        for (expr, surrogate) in m.aggregate_exprs.iter().zip(surrogates.iter()) {
            if Self::is_arg_agg_expr(expr) {
                // try_execute_arg_agg_branch expects an already-rewritten
                // query (FROM pointing at the derived arg table) - the
                // standalone handle_arg_agg_sql gets that for free from
                // handle_query_sql's top-level rewrite_recognized_pattern
                // call before it ever sees the query, but a freshly built
                // surrogate here has not been through that yet.
                let rewritten = self.rewrite_recognized_pattern(surrogate);
                let (arg_match, values, ts) =
                    self.try_execute_arg_agg_branch(&rewritten, time)?;
                if group_label_names.is_none() {
                    group_label_names = Some(vec![arg_match.group_by_col.clone()]);
                }
                end_timestamp = end_timestamp.max(ts);
                branch_maps.push(values);
            } else if Self::is_group_array_distinct_expr(expr) {
                // No FROM-clause rewrite needed first - unlike the arg-agg
                // branch above, this mechanism registers directly against
                // the query's own real table (see
                // try_execute_group_array_distinct_branch's doc comment).
                let (gad_match, values, ts) =
                    self.try_execute_group_array_distinct_branch(surrogate, time)?;
                if group_label_names.is_none() {
                    group_label_names = Some(gad_match.group_by.clone().into_iter().collect());
                }
                end_timestamp = end_timestamp.max(ts);
                branch_maps.push(values);
            } else {
                let (context, post) = self.build_query_execution_context_sql_with_post_processing(
                    surrogate.clone(),
                    computed_group_by_alias,
                    time,
                )?;
                let (output_labels, result) = self.execute_context(context, false, false)?;
                let result = post.apply(&output_labels, result);
                let QueryResult::Vector(vector) = result else {
                    warn!("multi-aggregate all-labels handler: expected instant vector result");
                    return None;
                };
                if group_label_names.is_none() {
                    group_label_names = Some(output_labels.labels.clone());
                }
                end_timestamp = end_timestamp.max(vector.timestamp);
                let map: HashMap<Vec<String>, String> = vector
                    .values
                    .into_iter()
                    .map(|element| (element.labels.labels.clone(), element.value.to_string()))
                    .collect();
                branch_maps.push(map);
            }
        }

        let group_label_names = group_label_names?;

        // Union of group keys across every branch, preserving first-seen
        // order - a key one branch's own filter excluded still needs a row
        // if any other branch produced it, matching the numeric path's
        // `unwrap_or(0.0)` default-fill behavior instead of silently
        // dropping the row.
        let mut all_keys: Vec<Vec<String>> = Vec::new();
        let mut seen: HashSet<Vec<String>> = HashSet::new();
        for map in &branch_maps {
            for key in map.keys() {
                if seen.insert(key.clone()) {
                    all_keys.push(key.clone());
                }
            }
        }

        let mut output_label_names = group_label_names;
        for expr in &m.aggregate_exprs {
            output_label_names.push(Self::alias_of(expr).unwrap_or_else(|| expr.clone()));
        }
        let output_labels = KeyByLabelNames {
            labels: output_label_names,
        };

        let mut values: Vec<InstantVectorElement> = all_keys
            .into_iter()
            .map(|key| {
                let mut label_values = key.clone();
                for map in &branch_maps {
                    label_values.push(map.get(&key).cloned().unwrap_or_default());
                }
                InstantVectorElement::new(KeyByLabelValues::new_with_labels(label_values), 0.0)
            })
            .collect();

        // Same by-position alias lookup as the numeric path's resolve_alias,
        // minus the "last aggregate is the live f64 value" special case -
        // every alias here is a label column, so HAVING (always a numeric
        // comparison) parses the label text back to f64.
        let resolve_alias = |element: &InstantVectorElement, alias: &str| -> f64 {
            output_labels
                .labels
                .iter()
                .position(|l| l == alias)
                .and_then(|pos| element.labels.labels.get(pos))
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        };

        if !m.having.is_empty() {
            values.retain(|element| {
                m.having.iter().all(|(alias, op, threshold)| {
                    let actual = resolve_alias(element, alias);
                    match op.as_str() {
                        ">" => actual > *threshold,
                        ">=" => actual >= *threshold,
                        "<" => actual < *threshold,
                        "<=" => actual <= *threshold,
                        "=" => actual == *threshold,
                        "!=" | "<>" => actual != *threshold,
                        _ => true,
                    }
                })
            });
        }

        let (order_by, limit) = parse_order_by_and_limit(&m.order_by_and_limit);
        let post = SqlPostProcessing {
            aggregation_alias: None,
            order_by,
            limit,
            having: None,
        };
        let result = post.apply(
            &output_labels,
            QueryResult::vector_without_value(values, end_timestamp),
        );

        Some((output_labels, result))
    }

    /// Internal: parses + plans a SQL query and returns both the execution
    /// context (shared with PromQL/Elastic engines) and the SQL-only
    /// post-processing rules (ORDER BY / LIMIT / alias resolution).
    /// `computed_group_by_alias`: set when the ORIGINAL query (before any
    /// rewriting, all the way back at `handle_query_sql`'s entry) matched a
    /// computed-GROUP-BY shape (e.g. `SELECT toDate(timestamp) AS day,
    /// count(*) ... GROUP BY day`) - see `SQLSchema::with_extra_metadata_column`'s
    /// doc comment for why this needs to widen schema validation below.
    /// Callers must detect this on the RAW query text themselves and pass it
    /// through: by the time any rewrite (including this function's own
    /// `rewrite_recognized_pattern` call just below) has run once, the
    /// computed expression is already gone - replaced by the bare alias -
    /// so re-detecting the shape from an already-rewritten string (what an
    /// earlier version of this fix tried, from inside this function) never
    /// matches. `handle_query_sql` rewrites its own `query` before dispatching
    /// to every handler, including the multi-aggregate path, which calls
    /// this function once per sub-aggregate surrogate - so every caller of
    /// this function only ever sees an already-rewritten string too, and the
    /// detection genuinely has to happen at the top of the call chain, once,
    /// on text none of them have touched yet.
    fn build_query_execution_context_sql_with_post_processing(
        &self,
        query: String,
        computed_group_by_alias: Option<&str>,
        time: f64,
    ) -> Option<(QueryExecutionContext, SqlPostProcessing)> {
        let query = self.rewrite_recognized_pattern(&query);

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

        // Widen the schema copy used for THIS query's validation only, or
        // `flatten_query_info` rejects the rewritten query with
        // `InvalidAggregationLabel` despite it being fully supported.
        let schema = match computed_group_by_alias {
            Some(alias) => schema.with_extra_metadata_column(alias),
            None => schema,
        };

        let statements = match parser::parse_sql(&GenericDialect {}, query.as_str()) {
            Ok(statements) => statements,
            Err(e) => {
                debug!(
                    "Could not parse query after rewrite_recognized_pattern: {} (query: {})",
                    e, query
                );
                return None;
            }
        };
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
            let requirements = self.build_query_requirements_sql(
                match_result,
                topk,
                query_data.spatial_filter.as_deref(),
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
            spatial_filter: None,
            labels: HashSet::from(["srcip".to_string()]),
            time_info: TimeInfo::new("time".to_string(), 0.0, 1.0),
            subquery: None,
            order_by: vec![OrderByItem {
                column: "TRANSFER_EVENTS".to_string(),
                ascending: false,
            }],
            limit: Some(10),
            having: None,
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
            has_value: true,
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
            having: None,
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
            has_value: true,
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
    // AggregationConfig.metric is "{table_name}.{value_column}" for SQL
    // configs (see aggregation_config.rs's from_yaml deserialization) - the
    // capability-matching fixtures below must use this same combined form,
    // not the bare table name, to match what build_query_requirements_sql
    // constructs from a real query.
    const CAPABILITY_METRIC: &str = "netflow_table.pkt_len";
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
            offset_ms: 0,
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
        let streaming_config = Arc::new(StreamingConfig::new(agg_configs));

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
            offset_ms: 0,
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
        let streaming_config = Arc::new(StreamingConfig::new(agg_configs));

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
            offset_ms: 0,
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
        let streaming_config = Arc::new(StreamingConfig::new(agg_configs));
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
            offset_ms: 0,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: CAPABILITY_METRIC.to_string(),
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
            offset_ms: 0,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: CAPABILITY_METRIC.to_string(),
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

        let streaming_config = Arc::new(StreamingConfig::new(agg_configs));
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
            offset_ms: 0,
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
        let streaming_config = Arc::new(StreamingConfig::new(agg_configs));
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

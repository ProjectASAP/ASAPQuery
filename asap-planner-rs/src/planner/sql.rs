use std::collections::HashSet;

use asap_types::enums::{CleanupPolicy, WindowType};
use asap_types::query_config::{
    QueryTimeAggregation, QueryTimeAggregationOperator, QueryTimeAggregationParameter,
    QueryTimeGrouping, QueryTimeGroupingMode,
};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, QueryTreatmentType, Statistic};
use sql_utilities::ast_matching::sqlhelper::{
    detect_sql_topk, SQLQueryData, SqlTopk, Table, TimeInfo,
};
use sql_utilities::ast_matching::sqlpattern_matcher::SQLPatternMatcher;
use sql_utilities::ast_matching::sqlpattern_parser::SQLPatternParser;
use sql_utilities::ast_matching::SQLSchema;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::config::input::{SketchParameterOverrides, TableDefinition, WindowingConfig};
use crate::error::ControllerError;
use crate::planner::agg_config::{build_agg_configs_for_statistics, IntermediateAggConfig};
use crate::planner::cleanup::get_sql_cleanup_param;
use crate::planner::promql::NestedAggregationPlan;
use crate::planner::sketch::build_sketch_parameters;
use crate::planner::window::IntermediateWindowConfig;
use crate::StreamingEngine;

pub struct SQLSingleQueryProcessor {
    query_string: String,
    t_repeat_ms: u64,
    data_ingestion_interval_ms: u64,
    table_definitions: Vec<TableDefinition>,
    #[allow(dead_code)]
    streaming_engine: StreamingEngine,
    sketch_parameters: Option<SketchParameterOverrides>,
    cleanup_policy: CleanupPolicy,
    windowing: Option<WindowingConfig>,
}

impl SQLSingleQueryProcessor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_string: String,
        t_repeat_ms: u64,
        data_ingestion_interval_ms: u64,
        table_definitions: Vec<TableDefinition>,
        streaming_engine: StreamingEngine,
        sketch_parameters: Option<SketchParameterOverrides>,
        cleanup_policy: CleanupPolicy,
        windowing: Option<WindowingConfig>,
    ) -> Self {
        Self {
            query_string,
            t_repeat_ms,
            data_ingestion_interval_ms,
            table_definitions,
            streaming_engine,
            sketch_parameters,
            cleanup_policy,
            windowing,
        }
    }

    pub fn get_streaming_aggregation_configs(
        &self,
        query_evaluation_time: f64,
    ) -> Result<(Vec<IntermediateAggConfig>, Option<u64>), ControllerError> {
        let (query_data, sql_topk) = parse_sql_query(
            &self.query_string,
            &self.table_definitions,
            self.data_ingestion_interval_ms,
            query_evaluation_time,
        )?;

        // Determine fields from query vecs
        let agg_info = &query_data.aggregation_info;
        let labels = &query_data.labels;
        let table_name = &query_data.metric;

        let value_column = agg_info.get_value_column_name().to_string();

        // Compute window
        let mut window_cfg = compute_sql_window(
            &query_data.time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;
        let data_range_ms = sql_data_range_ms(&query_data);
        crate::planner::window::apply_windowing_override(
            &mut window_cfg,
            data_range_ms,
            0,
            self.windowing.as_ref(),
        )?;

        // Get all metadata columns for the table
        let all_metadata = get_all_metadata_columns(&self.table_definitions, table_name)?;

        // Label routing
        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());
        let treatment_type = get_sql_treatment_type(agg_info.get_name());
        let statistics = sql_statistics(&query_data, sql_topk)?;
        let rollup = if statistics.contains(&Statistic::Cardinality) {
            // Distinct target is value_column, not a rollup label dimension.
            KeyByLabelNames::empty()
        } else {
            all_metadata.difference(&spatial_output)
        };

        let topk_k = sql_topk.map(|t| t.k);
        let topk_count_events = sql_topk.map(|t| t.count_events());

        let mut configs = build_agg_configs_for_statistics(
            &statistics,
            treatment_type,
            &spatial_output,
            // SQL topk has no by/without-style bucketing clause -- always a
            // single global ranking (see comment in build_query_requirements_sql).
            None,
            &rollup,
            &window_cfg,
            table_name,
            Some(table_name),
            Some(&value_column),
            "",
            |agg_type: AggregationType, agg_sub_type: &str| {
                build_sketch_parameters(
                    agg_type,
                    agg_sub_type,
                    topk_k,
                    topk_count_events,
                    self.sketch_parameters.as_ref(),
                )
            },
        )
        .map_err(ControllerError::SqlParse)?;

        if let Some(count_events) = topk_count_events {
            for cfg in &mut configs {
                if cfg.aggregation_type == AggregationType::CountMinSketchWithHeap {
                    // Heap-only self-keyed layout: the GROUP BY column is tracked
                    // inside the sketch's aggregated dimension, not as a partition key.
                    cfg.grouping_labels = KeyByLabelNames::empty();
                    cfg.aggregated_labels = spatial_output.clone();
                    // map_statistic_to_precompute_operator() emits the placeholder
                    // sub_type "topk"; the real SUM/COUNT weighting is only known
                    // here, from the detected topk clause (#670).
                    cfg.aggregation_sub_type =
                        if count_events { "count" } else { "sum" }.to_string();
                }
            }
        }

        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_sql_cleanup_param(self.cleanup_policy, data_range_ms, self.t_repeat_ms)
                    .map_err(ControllerError::PlannerError)?,
            )
        };

        Ok((configs, cleanup_param))
    }

    /// For a two-layer query, its inner layer (planned like any single-level
    /// query) and the outer layer as a query-time aggregation over it. `None`
    /// for a single-level query.
    pub fn nested_aggregation_plan(
        &self,
        query_evaluation_time: f64,
    ) -> Result<Option<NestedAggregationPlan>, ControllerError> {
        let schema = build_sql_schema(&self.table_definitions);
        let (stmts, qdata) =
            parse_sql_statements(&self.query_string, &schema, query_evaluation_time)?;
        if qdata.subquery.is_none() {
            return Ok(None);
        }

        let inner_pattern = sql_pattern_matcher(schema, self.data_ingestion_interval_ms)
            .nested_query_to_pattern(&qdata);
        if !inner_pattern.is_valid() {
            return Err(ControllerError::SqlParse(
                inner_pattern.msg.unwrap_or_default(),
            ));
        }

        let planned_subquery = inner_query_sql(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "nested query has no subquery in FROM: {}",
                self.query_string
            ))
        })?;
        Ok(Some(NestedAggregationPlan {
            planned_subquery,
            query_time_aggregations: vec![sql_query_time_aggregation(&qdata)?],
        }))
    }

    /// A processor for `query` with this one's settings.
    pub fn make_subquery_processor(&self, query: String) -> Self {
        Self::new(
            query,
            self.t_repeat_ms,
            self.data_ingestion_interval_ms,
            self.table_definitions.clone(),
            self.streaming_engine,
            self.sketch_parameters.clone(),
            self.cleanup_policy,
            self.windowing.clone(),
        )
    }
}

/// Parses `query` into its statements and pattern-parser data.
fn parse_sql_statements(
    query: &str,
    schema: &SQLSchema,
    query_evaluation_time: f64,
) -> Result<(Vec<Statement>, SQLQueryData), ControllerError> {
    let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, query)
        .map_err(|e| ControllerError::SqlParse(e.to_string()))?;
    let qdata = SQLPatternParser::new(schema, query_evaluation_time)
        .parse_query(&stmts)
        .ok_or_else(|| ControllerError::SqlParse(format!("Failed to parse SQL query: {query}")))?;
    Ok((stmts, qdata))
}

/// SQLPatternMatcher.scrape_interval is in seconds (SQL timestamps are seconds-based).
fn sql_pattern_matcher(schema: SQLSchema, data_ingestion_interval_ms: u64) -> SQLPatternMatcher {
    SQLPatternMatcher::new(schema, data_ingestion_interval_ms as f64 / 1000.0)
}

/// The SQL text of the subquery in the outer layer's FROM.
fn inner_query_sql(stmts: &[Statement]) -> Option<String> {
    let [Statement::Query(query)] = stmts else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    match &select.from.first()?.relation {
        TableFactor::Derived { subquery, .. } => Some(subquery.to_string()),
        _ => None,
    }
}

/// The query-time aggregation an outer SQL layer applies to its inner
/// layer's rows: its aggregate, grouped by its GROUP BY (all rows without one).
fn sql_query_time_aggregation(
    outer: &SQLQueryData,
) -> Result<QueryTimeAggregation, ControllerError> {
    let info = &outer.aggregation_info;
    let operator = match info.get_name() {
        "SUM" => QueryTimeAggregationOperator::Sum,
        "COUNT" => QueryTimeAggregationOperator::Count,
        "AVG" => QueryTimeAggregationOperator::Avg,
        "MIN" => QueryTimeAggregationOperator::Min,
        "MAX" => QueryTimeAggregationOperator::Max,
        "QUANTILE" => QueryTimeAggregationOperator::Quantile,
        other => {
            return Err(ControllerError::SqlParse(format!(
                "unsupported outer aggregation: {other}"
            )))
        }
    };
    let parameter = match operator {
        QueryTimeAggregationOperator::Quantile => {
            let phi = info
                .get_args()
                .first()
                .and_then(|arg| arg.parse::<f64>().ok())
                .ok_or_else(|| {
                    ControllerError::SqlParse(format!(
                        "QUANTILE needs a numeric level, got {:?}",
                        info.get_args()
                    ))
                })?;
            Some(QueryTimeAggregationParameter::Float(phi))
        }
        _ => None,
    };
    let grouping = if outer.labels.is_empty() {
        QueryTimeGrouping {
            mode: QueryTimeGroupingMode::All,
            labels: Vec::new(),
        }
    } else {
        let mut labels: Vec<String> = outer.labels.iter().cloned().collect();
        labels.sort();
        QueryTimeGrouping {
            mode: QueryTimeGroupingMode::By,
            labels,
        }
    };

    let aggregation = QueryTimeAggregation {
        operator,
        grouping,
        parameter,
    };
    aggregation.validate().map_err(ControllerError::SqlParse)?;
    Ok(aggregation)
}

/// Parses `query` and matches it against `tables`: a single-level query's
/// data, and its top-k clause if it is one.
pub(crate) fn parse_sql_query(
    query: &str,
    tables: &[TableDefinition],
    data_ingestion_interval_ms: u64,
    query_evaluation_time: f64,
) -> Result<(SQLQueryData, Option<SqlTopk>), ControllerError> {
    let schema = build_sql_schema(tables);
    let (_, qdata) = parse_sql_statements(query, &schema, query_evaluation_time)?;
    let sql_query =
        sql_pattern_matcher(schema, data_ingestion_interval_ms).query_info_to_pattern(&qdata);
    if !sql_query.is_valid() {
        return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
    }
    let n = sql_query.query_data.len();
    let Ok([query_data]) = <[SQLQueryData; 1]>::try_from(sql_query.query_data) else {
        return Err(ControllerError::SqlParse(format!(
            "Nested SQL queries (n={n}) are not supported"
        )));
    };
    // Top-k needs ORDER BY / LIMIT from the parser; SQLPatternMatcher drops
    // them from its query data.
    Ok((query_data, detect_sql_topk(&qdata)))
}

/// The statistics `query_data` needs: top-k, or its aggregation's.
pub(crate) fn sql_statistics(
    query_data: &SQLQueryData,
    topk: Option<SqlTopk>,
) -> Result<Vec<Statistic>, ControllerError> {
    match topk {
        Some(_) => Ok(vec![Statistic::Topk]),
        None => get_sql_statistics(query_data.aggregation_info.get_name()),
    }
}

/// SQLPatternParser always produces second-based durations; convert to ms.
/// For a single-scrape-interval query this equals data_ingestion_interval_ms
/// by construction (the matcher's classification boundary), so this is a
/// plain unconditional formula, not a special case.
pub(crate) fn sql_data_range_ms(query_data: &SQLQueryData) -> u64 {
    (query_data.time_info.get_duration() * 1000.0).round() as u64
}

fn build_sql_schema(tables: &[TableDefinition]) -> SQLSchema {
    let table_vec: Vec<Table> = tables
        .iter()
        .map(|t| {
            Table::new(
                t.name.clone(),
                t.time_column.clone(),
                t.value_columns.iter().cloned().collect::<HashSet<_>>(),
                t.metadata_columns.iter().cloned().collect::<HashSet<_>>(),
            )
        })
        .collect();
    SQLSchema::new(table_vec)
}

fn get_sql_treatment_type(name: &str) -> QueryTreatmentType {
    match name.to_uppercase().as_str() {
        "MIN" | "MAX" => QueryTreatmentType::Exact,
        _ => QueryTreatmentType::Approximate,
    }
}

fn get_sql_statistics(name: &str) -> Result<Vec<Statistic>, ControllerError> {
    match name.to_uppercase().as_str() {
        "QUANTILE" => Ok(vec![Statistic::Quantile]),
        "SUM" => Ok(vec![Statistic::Sum]),
        "COUNT" => Ok(vec![Statistic::Count]),
        "AVG" => Ok(vec![Statistic::Sum, Statistic::Count]),
        "MIN" => Ok(vec![Statistic::Min]),
        "MAX" => Ok(vec![Statistic::Max]),
        "CARDINALITY" => Ok(vec![Statistic::Cardinality]),
        other => Err(ControllerError::SqlParse(format!(
            "Unsupported aggregation: {}",
            other
        ))),
    }
}

/// Enforces `t_repeat_ms >= data_ingestion_interval_ms` (can't refresh faster
/// than raw ingestion) and, for a genuinely multi-interval query,
/// `duration_ms >= t_repeat_ms` (a precompute window must not outlive the
/// query range it's sized for) — rather than silently picking
/// `data_ingestion_interval_ms` or `t_repeat_ms` depending on query shape.
///
/// Relaxation, kept consistent with the PromQL side
/// (`asap-planner-rs/src/planner/window.rs::set_window_parameters`): when
/// `duration_ms == data_ingestion_interval_ms` exactly (the query's own range
/// is exactly one scrape interval), the query only ever concerns a single
/// precomputed bucket — it asks for "the current/latest bucket," not "the
/// last N buckets" — so re-reading that one bucket less often than it's
/// produced is always safe, and the `duration_ms >= t_repeat_ms` upper bound
/// is skipped. `window_size_ms` is `data_ingestion_interval_ms` in that case,
/// or `t_repeat_ms` otherwise.
///
/// Old code used `t_repeat_ms` uncapped even when it exceeded the query's
/// own duration (see `temporal_sum_t600` in `sql_integration.rs`, updated
/// alongside the original version of this change to expect a `PlannerError`
/// instead — still correct, since that test's duration (300s) differs from
/// `data_ingestion_interval_ms` (15s), so it isn't the relaxed case).
fn compute_sql_window(
    time_info: &TimeInfo,
    data_ingestion_interval_ms: u64,
    t_repeat_ms: u64,
) -> Result<IntermediateWindowConfig, ControllerError> {
    if t_repeat_ms < data_ingestion_interval_ms {
        return Err(ControllerError::PlannerError(format!(
            "t_repeat_ms ({t_repeat_ms}ms) must be >= data_ingestion_interval_ms ({data_ingestion_interval_ms}ms)"
        )));
    }
    let duration_ms = (time_info.get_duration() * 1000.0).round() as u64;
    let window_size_ms = if duration_ms == data_ingestion_interval_ms {
        data_ingestion_interval_ms
    } else {
        if duration_ms < t_repeat_ms {
            return Err(ControllerError::PlannerError(format!(
                "query duration ({duration_ms}ms) must be >= t_repeat_ms ({t_repeat_ms}ms)"
            )));
        }
        t_repeat_ms
    };
    Ok(IntermediateWindowConfig {
        window_size_ms,
        slide_interval_ms: window_size_ms,
        window_type: WindowType::Tumbling,
    })
}

fn get_all_metadata_columns(
    table_definitions: &[TableDefinition],
    table_name: &str,
) -> Result<KeyByLabelNames, ControllerError> {
    let table = table_definitions
        .iter()
        .find(|t| t.name == table_name)
        .ok_or_else(|| ControllerError::UnknownTable(table_name.to_string()))?;
    Ok(KeyByLabelNames::new(table.metadata_columns.clone()))
}

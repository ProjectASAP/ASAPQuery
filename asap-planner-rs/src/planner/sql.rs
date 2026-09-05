use std::collections::HashSet;

use asap_types::enums::{CleanupPolicy, WindowType};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, QueryTreatmentType, Statistic};
use sql_utilities::ast_matching::sqlhelper::{detect_sql_topk, Table, TimeInfo};
use sql_utilities::ast_matching::sqlpattern_matcher::SQLPatternMatcher;
use sql_utilities::ast_matching::sqlpattern_parser::SQLPatternParser;
use sql_utilities::ast_matching::SQLSchema;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::config::input::{SketchParameterOverrides, TableDefinition, WindowingConfig};
use crate::error::ControllerError;
use crate::planner::agg_config::{build_agg_configs_for_statistics, IntermediateAggConfig};
use crate::planner::cleanup::get_sql_cleanup_param;
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
        let schema = build_sql_schema(&self.table_definitions);

        // Parse SQL
        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &self.query_string)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        // Parse query into SQLQueryData
        let qdata = SQLPatternParser::new(&schema, query_evaluation_time)
            .parse_query(&stmts)
            .ok_or_else(|| {
                ControllerError::SqlParse(format!(
                    "Failed to parse SQL query: {}",
                    self.query_string
                ))
            })?;

        // Match query to pattern
        // SQLPatternMatcher.scrape_interval is in seconds (SQL timestamps are seconds-based).
        let sql_query = SQLPatternMatcher::new(
            schema.clone(),
            self.data_ingestion_interval_ms as f64 / 1000.0,
        )
        .query_info_to_pattern(&qdata);

        if !sql_query.is_valid() {
            return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
        }

        let n = sql_query.query_data.len();

        if n != 1 {
            return Err(ControllerError::SqlParse(format!(
                "Nested SQL queries (n={}) are not supported",
                n
            )));
        }

        // Determine fields from query vecs
        let agg_info = &sql_query.query_data[0].aggregation_info;
        let labels = &sql_query.query_data[0].labels;
        let table_name = &sql_query.query_data[0].metric;

        let value_column = agg_info.get_value_column_name().to_string();

        // Compute window
        let mut window_cfg = compute_sql_window(
            &sql_query.query_data[0].time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;
        let data_range_ms =
            (sql_query.query_data[0].time_info.get_duration() * 1000.0).round() as u64;
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
        // Top-k needs ORDER BY / LIMIT from the parser; SQLPatternMatcher drops them
        // when building `sql_query.query_data[0]`, so use `qdata` not query_data[0].
        let sql_topk = detect_sql_topk(&qdata);
        let treatment_type = get_sql_treatment_type(agg_info.get_name());
        let statistics = if sql_topk.is_some() {
            vec![Statistic::Topk]
        } else {
            get_sql_statistics(agg_info.get_name())?
        };
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

        // SQLPatternParser always produces second-based durations; convert to ms.
        // For a single-scrape-interval query this equals data_ingestion_interval_ms
        // by construction (the matcher's classification boundary), so this is a
        // plain unconditional formula, not a special case.
        let t_lookback_ms =
            (sql_query.query_data[0].time_info.get_duration() * 1000.0).round() as u64;

        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_sql_cleanup_param(self.cleanup_policy, t_lookback_ms, self.t_repeat_ms)
                    .map_err(ControllerError::PlannerError)?,
            )
        };

        Ok((configs, cleanup_param))
    }
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

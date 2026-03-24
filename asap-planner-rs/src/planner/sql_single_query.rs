use std::collections::{HashMap, HashSet};

use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use serde_json::Value;
use sketch_db_common::enums::CleanupPolicy;
use sql_utilities::ast_matching::sqlhelper::Table;
use sql_utilities::ast_matching::sqlpattern_matcher::{QueryType, SQLPatternMatcher};
use sql_utilities::ast_matching::sqlpattern_parser::SQLPatternParser;
use sql_utilities::ast_matching::SQLSchema;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::config::input::{SketchParameterOverrides, TableDefinition};
use crate::error::ControllerError;
use crate::planner::logics::{
    get_sql_cleanup_param, set_subpopulation_labels, IntermediateWindowConfig,
};
use crate::planner::single_query::IntermediateAggConfig;
use crate::StreamingEngine;

// Default sketch parameters (mirrored from logics.rs)
const DEFAULT_CMS_DEPTH: u64 = 3;
const DEFAULT_CMS_WIDTH: u64 = 1024;
const DEFAULT_KLL_K: u64 = 20;
const DEFAULT_HYDRA_ROW: u64 = 3;
const DEFAULT_HYDRA_COL: u64 = 1024;
const DEFAULT_HYDRA_K: u64 = 20;

pub struct SQLSingleQueryProcessor {
    query_string: String,
    t_repeat: u64,
    data_ingestion_interval: u64,
    table_definitions: Vec<TableDefinition>,
    #[allow(dead_code)]
    streaming_engine: StreamingEngine,
    sketch_parameters: Option<SketchParameterOverrides>,
    cleanup_policy: CleanupPolicy,
}

impl SQLSingleQueryProcessor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_string: String,
        t_repeat: u64,
        data_ingestion_interval: u64,
        table_definitions: Vec<TableDefinition>,
        streaming_engine: StreamingEngine,
        sketch_parameters: Option<SketchParameterOverrides>,
        cleanup_policy: CleanupPolicy,
    ) -> Self {
        Self {
            query_string,
            t_repeat,
            data_ingestion_interval,
            table_definitions,
            streaming_engine,
            sketch_parameters,
            cleanup_policy,
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
        let sql_query = SQLPatternMatcher::new(schema.clone(), self.data_ingestion_interval as f64)
            .query_info_to_pattern(&qdata);

        if !sql_query.is_valid() {
            return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
        }

        let n = sql_query.query_data.len();

        // Determine fields from query vecs
        let (agg_info, query_type, labels, table_name) = if n == 1 {
            (
                &sql_query.query_data[0].aggregation_info,
                &sql_query.query_type[0],
                &sql_query.query_data[0].labels,
                &sql_query.query_data[0].metric,
            )
        } else {
            // N=2: [Spatial, TemporalX]
            (
                &sql_query.query_data[1].aggregation_info,
                &sql_query.query_type[1],
                &sql_query.query_data[0].labels,
                &sql_query.query_data[1].metric,
            )
        };

        let value_column = agg_info.get_value_column_name().to_string();

        // Compute window
        let window_cfg =
            compute_sql_window(query_type, self.data_ingestion_interval, self.t_repeat);

        // Get all metadata columns for the table
        let all_metadata = get_all_metadata_columns(&self.table_definitions, table_name)?;

        // Label routing
        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());
        let rollup = all_metadata.difference(&spatial_output);

        let mut configs: Vec<IntermediateAggConfig> = Vec::new();

        // For each (agg_type, sub_type) pair
        for (agg_type, agg_sub_type) in sql_agg_to_operators(agg_info.get_name())? {
            let mut grouping = KeyByLabelNames::empty();
            let mut aggregated = KeyByLabelNames::empty();

            if agg_type == "DeltaSetAggregator" {
                // Should not happen since we inject DeltaSet separately
                grouping = spatial_output.clone();
                aggregated = KeyByLabelNames::empty();
            } else {
                let statistic = sql_agg_name_to_statistic(agg_info.get_name(), &agg_sub_type);
                set_subpopulation_labels(
                    statistic,
                    &agg_type,
                    &spatial_output,
                    &mut rollup.clone(),
                    &mut grouping,
                    &mut aggregated,
                );
            }

            // If CountMinSketch, prepend a DeltaSetAggregator
            if agg_type == "CountMinSketch" {
                let delta_params = get_sql_precompute_operator_parameters(
                    "DeltaSetAggregator",
                    self.sketch_parameters.as_ref(),
                )
                .map_err(ControllerError::PlannerError)?;

                configs.push(IntermediateAggConfig {
                    aggregation_type: "DeltaSetAggregator".to_string(),
                    aggregation_sub_type: String::new(),
                    window_type: window_cfg.window_type.clone(),
                    window_size: window_cfg.window_size,
                    slide_interval: window_cfg.slide_interval,
                    spatial_filter: String::new(),
                    metric: table_name.clone(),
                    table_name: Some(table_name.clone()),
                    value_column: Some(value_column.clone()),
                    parameters: delta_params,
                    rollup_labels: rollup.clone(),
                    grouping_labels: spatial_output.clone(),
                    aggregated_labels: KeyByLabelNames::empty(),
                });
            }

            let parameters =
                get_sql_precompute_operator_parameters(&agg_type, self.sketch_parameters.as_ref())
                    .map_err(ControllerError::PlannerError)?;

            configs.push(IntermediateAggConfig {
                aggregation_type: agg_type,
                aggregation_sub_type: agg_sub_type,
                window_type: window_cfg.window_type.clone(),
                window_size: window_cfg.window_size,
                slide_interval: window_cfg.slide_interval,
                spatial_filter: String::new(),
                metric: table_name.clone(),
                table_name: Some(table_name.clone()),
                value_column: Some(value_column.clone()),
                parameters,
                rollup_labels: rollup.clone(),
                grouping_labels: grouping,
                aggregated_labels: aggregated,
            });
        }

        // Compute t_lookback
        let t_lookback = match query_type {
            QueryType::Spatial => self.data_ingestion_interval,
            _ => {
                if n == 1 {
                    sql_query.query_data[0].time_info.get_duration() as u64
                } else {
                    sql_query.query_data[1].time_info.get_duration() as u64
                }
            }
        };

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

fn sql_agg_to_operators(name: &str) -> Result<Vec<(String, String)>, ControllerError> {
    match name.to_uppercase().as_str() {
        "QUANTILE" => Ok(vec![("DatasketchesKLL".into(), "".into())]),
        "SUM" => Ok(vec![("CountMinSketch".into(), "sum".into())]),
        "COUNT" => Ok(vec![("CountMinSketch".into(), "count".into())]),
        "AVG" => Ok(vec![
            ("CountMinSketch".into(), "sum".into()),
            ("CountMinSketch".into(), "count".into()),
        ]),
        "MIN" => Ok(vec![("MultipleMinMax".into(), "min".into())]),
        "MAX" => Ok(vec![("MultipleMinMax".into(), "max".into())]),
        other => Err(ControllerError::SqlParse(format!(
            "Unsupported aggregation: {}",
            other
        ))),
    }
}

fn sql_agg_name_to_statistic(name: &str, sub_type: &str) -> Statistic {
    match name.to_uppercase().as_str() {
        "QUANTILE" => Statistic::Quantile,
        "SUM" => Statistic::Sum,
        "COUNT" => Statistic::Count,
        "AVG" => {
            if sub_type == "sum" {
                Statistic::Sum
            } else {
                Statistic::Count
            }
        }
        "MIN" => Statistic::Min,
        "MAX" => Statistic::Max,
        _ => Statistic::Sum,
    }
}

fn compute_sql_window(
    query_type: &QueryType,
    data_ingestion_interval: u64,
    t_repeat: u64,
) -> IntermediateWindowConfig {
    let window_size = match query_type {
        QueryType::Spatial => data_ingestion_interval,
        _ => t_repeat,
    };
    IntermediateWindowConfig {
        window_size,
        slide_interval: window_size,
        window_type: "tumbling".to_string(),
    }
}

fn get_sql_precompute_operator_parameters(
    aggregation_type: &str,
    sketch_params: Option<&SketchParameterOverrides>,
) -> Result<HashMap<String, Value>, String> {
    match aggregation_type {
        "Increase" | "MinMax" | "Sum" | "MultipleIncrease" | "MultipleMinMax" | "MultipleSum"
        | "DeltaSetAggregator" | "SetAggregator" => Ok(HashMap::new()),

        "CountMinSketch" => {
            let depth = sketch_params
                .and_then(|p| p.count_min_sketch.as_ref())
                .map(|p| p.depth)
                .unwrap_or(DEFAULT_CMS_DEPTH);
            let width = sketch_params
                .and_then(|p| p.count_min_sketch.as_ref())
                .map(|p| p.width)
                .unwrap_or(DEFAULT_CMS_WIDTH);
            let mut m = HashMap::new();
            m.insert("depth".to_string(), Value::Number(depth.into()));
            m.insert("width".to_string(), Value::Number(width.into()));
            Ok(m)
        }

        "DatasketchesKLL" => {
            let k = sketch_params
                .and_then(|p| p.datasketches_kll.as_ref())
                .map(|p| p.k)
                .unwrap_or(DEFAULT_KLL_K);
            let mut m = HashMap::new();
            m.insert("K".to_string(), Value::Number(k.into()));
            Ok(m)
        }

        "HydraKLL" => {
            let row_num = sketch_params
                .and_then(|p| p.hydra_kll.as_ref())
                .map(|p| p.row_num)
                .unwrap_or(DEFAULT_HYDRA_ROW);
            let col_num = sketch_params
                .and_then(|p| p.hydra_kll.as_ref())
                .map(|p| p.col_num)
                .unwrap_or(DEFAULT_HYDRA_COL);
            let k = sketch_params
                .and_then(|p| p.hydra_kll.as_ref())
                .map(|p| p.k)
                .unwrap_or(DEFAULT_HYDRA_K);
            let mut m = HashMap::new();
            m.insert("row_num".to_string(), Value::Number(row_num.into()));
            m.insert("col_num".to_string(), Value::Number(col_num.into()));
            m.insert("k".to_string(), Value::Number(k.into()));
            Ok(m)
        }

        other => Err(format!("Aggregation type {} not supported", other)),
    }
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

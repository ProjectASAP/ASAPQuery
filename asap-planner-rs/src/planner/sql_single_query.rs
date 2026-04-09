use std::collections::HashSet;

use asap_types::enums::{CleanupPolicy, WindowType};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryTreatmentType, Statistic};
use sql_utilities::ast_matching::sqlhelper::Table;
use sql_utilities::ast_matching::sqlpattern_matcher::{QueryType, SQLPatternMatcher};
use sql_utilities::ast_matching::sqlpattern_parser::SQLPatternParser;
use sql_utilities::ast_matching::SQLSchema;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::config::input::{SketchParameterOverrides, TableDefinition};
use crate::error::ControllerError;
use crate::planner::logics::{
    build_sketch_parameters, get_sql_cleanup_param, IntermediateWindowConfig,
};
use crate::planner::single_query::{build_agg_configs_for_statistics, IntermediateAggConfig};
use crate::StreamingEngine;

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

        if n != 1 {
            return Err(ControllerError::SqlParse(format!(
                "Nested SQL queries (n={}) are not supported",
                n
            )));
        }

        // Determine fields from query vecs
        let agg_info = &sql_query.query_data[0].aggregation_info;
        let query_type = &sql_query.query_type[0];
        let labels = &sql_query.query_data[0].labels;
        let table_name = &sql_query.query_data[0].metric;

        let value_column = agg_info.get_value_column_name().to_string();

        // Compute window
        let window_cfg =
            compute_sql_window(query_type, self.data_ingestion_interval, self.t_repeat);

        // Get all metadata columns for the table
        let all_metadata = get_all_metadata_columns(&self.table_definitions, table_name)?;

        // Label routing
        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());
        let rollup = all_metadata.difference(&spatial_output);

        let treatment_type = get_sql_treatment_type(agg_info.get_name());
        let statistics = get_sql_statistics(agg_info.get_name())?;

        let configs = build_agg_configs_for_statistics(
            &statistics,
            treatment_type,
            &spatial_output,
            &rollup,
            &window_cfg,
            table_name,
            Some(table_name),
            Some(&value_column),
            "",
            |agg_type, agg_sub_type| {
                build_sketch_parameters(
                    agg_type,
                    agg_sub_type,
                    None,
                    self.sketch_parameters.as_ref(),
                )
            },
        )
        .map_err(ControllerError::SqlParse)?;

        let t_lookback = match query_type {
            QueryType::Spatial => self.data_ingestion_interval,
            _ => sql_query.query_data[0].time_info.get_duration() as u64,
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
        other => Err(ControllerError::SqlParse(format!(
            "Unsupported aggregation: {}",
            other
        ))),
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
        window_type: WindowType::Tumbling,
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

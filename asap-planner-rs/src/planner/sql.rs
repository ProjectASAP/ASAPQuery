use std::collections::HashSet;

use asap_types::computed_label::ComputedLabelConfig;
use asap_types::enums::{CleanupPolicy, WindowType};
use asap_types::stateful_transition::StatefulTransitionConfig;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, QueryTreatmentType, Statistic};
use sql_utilities::ast_matching::pattern_rewrites::{
    build_lag_transition_surrogate, build_moas_surrogate, build_token_explode_surrogate,
    build_token_select_surrogate, looks_like_exact_only_sql, looks_like_lag_transition_sql,
    looks_like_moas_sql, looks_like_token_explode_sql, looks_like_token_select_sql,
    parse_lag_transition_query, parse_moas_query, parse_token_explode_query,
    parse_token_select_query,
};
use sql_utilities::ast_matching::sqlhelper::{
    detect_sql_topk, SQLBucketedCountIfQueryData, Table, TimeInfo,
};
use sql_utilities::ast_matching::sqlpattern_matcher::SQLPatternMatcher;
use sql_utilities::ast_matching::sqlpattern_parser::SQLPatternParser;
use sql_utilities::ast_matching::SQLSchema;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::config::input::{SketchParameterOverrides, TableDefinition};
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
    ) -> Self {
        Self {
            query_string,
            t_repeat_ms,
            data_ingestion_interval_ms,
            table_definitions,
            streaming_engine,
            sketch_parameters,
            cleanup_policy,
        }
    }

    /// True when this query has no GROUP BY and no aggregate function - a raw
    /// row scan or DISTINCT listing that no precomputed summary can ever
    /// answer, no matter how the pattern matchers below are extended.
    /// Callers should punt these rather than calling
    /// `get_streaming_aggregation_configs`, which would otherwise fail with
    /// a generic "Failed to parse SQL query" error indistinguishable from an
    /// actual planner bug. See `looks_like_exact_only_sql`.
    pub fn is_exact_only(&self) -> bool {
        looks_like_exact_only_sql(&self.query_string)
    }

    pub fn get_streaming_aggregation_configs(
        &self,
        query_evaluation_time: f64,
    ) -> Result<
        (
            Vec<IntermediateAggConfig>,
            Option<u64>,
            Option<StatefulTransitionConfig>,
            // Some(surrogate) when the raw query_string can't be parsed as a
            // template by SQLPatternParser (e.g. it's a CTE/window-function
            // query) and a simplified query must be registered in
            // inference_config.yaml instead, or the query-time matcher will
            // never find this aggregation (it can't parse the raw template
            // either, so nothing would ever match against it).
            Option<String>,
            // Computed labels (e.g. origin-ASN extraction) this query needs
            // at ingest time, as (label_name, config) pairs.
            Vec<(String, ComputedLabelConfig)>,
        ),
        ControllerError,
    > {
        let schema = build_sql_schema(&self.table_definitions);

        // Parse SQL
        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &self.query_string)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(&schema, query_evaluation_time);

        // Native bucketed countIf time-series path.
        //
        // This is intentionally parallel to the classic SQLQueryData path because
        // the classic model tracks exactly one aggregate. A bucketed countIf query
        // has one time-bucket expression and multiple conditional count outputs.
        if let Some(bucketed) = parser.parse_bucketed_countif_query(&stmts) {
            let (configs, cleanup) =
                self.get_bucketed_countif_streaming_aggregation_configs(&bucketed)?;
            return Ok((configs, cleanup, None, None, Vec::new()));
        }

        // Native MOAS path:
        //   prefix -> exact set(origin_asn)
        //
        // The classic SQLQueryData parser tracks one aggregate, but Q8 has both
        // COUNT(DISTINCT origin_asn) and DISTINCT_SET(origin_asn). The planner
        // lowers this query to one SetAggregator precompute.
        if looks_like_moas_sql(&self.query_string) {
            // Same reasoning as the lag-transition path below: the raw MOAS
            // SQL (COUNT(DISTINCT ...) + DISTINCT_SET/groupUniqArray) isn't
            // parseable by SQLPatternParser either, so the surrogate must be
            // what's registered as the query-time matching template too.
            return self.get_moas_streaming_aggregation_configs(&schema, query_evaluation_time);
        }

        // Lag-transition path (e.g. ClickHouse `lagInFrame(...) OVER (PARTITION
        // BY ...)`): the raw SQL is a CTE + window function the classic
        // SQLQueryData model can't represent at all. Detect it, translate it
        // into a plain aggregation over its derived event stream (reusing the
        // exact surrogate-query approach the MOAS path above already uses),
        // and additionally emit the StatefulTransitionConfig that tells the
        // precompute engine how to actually maintain that derived stream —
        // previously nothing auto-generated this; it had to be hand-written.
        if looks_like_lag_transition_sql(&self.query_string) {
            let (configs, cleanup, stateful_transition, surrogate) = self
                .get_lag_transition_streaming_aggregation_configs(
                    &schema,
                    query_evaluation_time,
                )?;
            return Ok((configs, cleanup, stateful_transition, surrogate, Vec::new()));
        }

        // Token-select path (e.g. origin-ASN extraction: a nested subquery
        // that tokenizes as_path and indexes the last matching token). The
        // classic SQLQueryData parser can't represent the nested subquery
        // either, so - same shape as the two paths above - detect it,
        // translate to a plain aggregation over the (now-computed) label,
        // and emit the ComputedLabelConfig that tells ingest how to actually
        // derive that label from the raw column. Previously nothing
        // auto-generated this; a human had to hand-write the
        // computed_label_cols entry.
        if looks_like_token_select_sql(&self.query_string) {
            return self
                .get_token_select_streaming_aggregation_configs(&schema, query_evaluation_time);
        }

        // Token-explode path: same tokenization building block as above, but
        // every matching token becomes its own row (arrayJoin) instead of
        // indexing just the last one.
        if looks_like_token_explode_sql(&self.query_string) {
            return self
                .get_token_explode_streaming_aggregation_configs(&schema, query_evaluation_time);
        }

        // Multi-aggregate queries (2+ aggregate expressions over one GROUP
        // BY) are handled one level up, in generate_sql_plan: split into N
        // independent single-aggregate surrogates, each registered and
        // planned separately (a fresh SQLSingleQueryProcessor per surrogate,
        // recursing into this same function). That keeps each surrogate
        // individually findable by the query engine's ordinary
        // find_query_config_sql structural matcher at serve time, rather
        // than only through its capability-matching fallback (which doesn't
        // carry the surrogate's spatial filter and so can't match a query
        // with a WHERE clause) - see handle_multi_aggregate_sql in the query
        // engine for the serve-time half of this split.

        // Parse query into SQLQueryData
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!("Failed to parse SQL query: {}", self.query_string))
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
        let window_cfg = compute_sql_window(
            &sql_query.query_data[0].time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
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
            qdata.spatial_filter.as_deref().unwrap_or(""),
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

        if sql_topk.is_some() {
            for cfg in &mut configs {
                if cfg.aggregation_type == AggregationType::CountMinSketchWithHeap {
                    // Heap-only self-keyed layout: the GROUP BY column is tracked
                    // inside the sketch's aggregated dimension, not as a partition key.
                    cfg.grouping_labels = KeyByLabelNames::empty();
                    cfg.aggregated_labels = spatial_output.clone();
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

        Ok((configs, cleanup_param, None, None, Vec::new()))
    }

    fn get_moas_streaming_aggregation_configs(
        &self,
        schema: &SQLSchema,
        query_evaluation_time: f64,
    ) -> Result<
        (
            Vec<IntermediateAggConfig>,
            Option<u64>,
            Option<StatefulTransitionConfig>,
            Option<String>,
            Vec<(String, ComputedLabelConfig)>,
        ),
        ControllerError,
    > {
        let m = parse_moas_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse MOAS SQL query: {}",
                self.query_string
            ))
        })?;

        let surrogate = build_moas_surrogate(&m);

        // As with token-select, a computed label doesn't exist as a real
        // schema column - schema validation needs to know about it before
        // planning this one query, or it fails even though ingest will
        // genuinely produce it once the emitted ComputedLabelConfig below
        // takes effect. Only needed for the tokenized MOAS shape; the
        // literal shape's label is already a real column.
        let augmented_tables;
        let augmented_schema;
        let schema = if m.computed_label.is_some() {
            let mut tables = self.table_definitions.clone();
            for t in &mut tables {
                if !t.metadata_columns.iter().any(|c| c == &m.label) {
                    t.metadata_columns.push(m.label.clone());
                }
            }
            augmented_tables = tables;
            augmented_schema = build_sql_schema(&augmented_tables);
            &augmented_schema
        } else {
            schema
        };

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!("Failed to parse MOAS surrogate query: {surrogate}"))
        })?;

        let sql_query = SQLPatternMatcher::new(
            schema.clone(),
            self.data_ingestion_interval_ms as f64 / 1000.0,
        )
        .query_info_to_pattern(&qdata);

        if !sql_query.is_valid() {
            return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
        }

        if sql_query.query_data.len() != 1 {
            return Err(ControllerError::SqlParse(format!(
                "MOAS surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let agg_info = &q.aggregation_info;
        let labels = &q.labels;
        let table_name = &q.metric;
        let value_column = agg_info.get_value_column_name().to_string();

        if agg_info.get_name() != "CARDINALITY" || value_column != m.label {
            return Err(ControllerError::SqlParse(format!(
                "MOAS path expected COUNT(DISTINCT {}), got {}({})",
                m.label,
                agg_info.get_name(),
                value_column
            )));
        }

        let window_cfg = compute_sql_window(
            &q.time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;

        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());

        let mut configs = build_agg_configs_for_statistics(
            &[Statistic::Cardinality],
            QueryTreatmentType::Approximate,
            &spatial_output,
            &KeyByLabelNames::empty(),
            &window_cfg,
            table_name,
            Some(table_name),
            Some(&value_column),
            qdata.spatial_filter.as_deref().unwrap_or(""),
            |agg_type: AggregationType, agg_sub_type: &str| {
                build_sketch_parameters(
                    agg_type,
                    agg_sub_type,
                    None,
                    None,
                    self.sketch_parameters.as_ref(),
                )
            },
        )
        .map_err(ControllerError::SqlParse)?;

        for cfg in &mut configs {
            // Replace the default CARDINALITY/HLL lowering with an exact set-valued
            // summary. MOAS needs the actual origin list, not only its cardinality.
            cfg.aggregation_type = AggregationType::SetAggregator;
            cfg.aggregation_sub_type = "".to_string();
            cfg.parameters.clear();
            cfg.grouping_labels = KeyByLabelNames::new(vec![m.group_by.clone()]);
            cfg.aggregated_labels = KeyByLabelNames::new(vec![m.label.clone()]);
            cfg.rollup_labels = KeyByLabelNames::empty();
            cfg.value_column = Some("__event_count__".to_string());
        }

        let t_lookback_ms = (q.time_info.get_duration() * 1000.0).round() as u64;
        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_sql_cleanup_param(self.cleanup_policy, t_lookback_ms, self.t_repeat_ms)
                    .map_err(ControllerError::PlannerError)?,
            )
        };

        let computed_labels = match m.computed_label {
            Some((source_col, filter_regex)) => vec![(
                m.label.clone(),
                ComputedLabelConfig {
                    r#type: "token_select".to_string(),
                    source_col,
                    tokenizer: Some("whitespace".to_string()),
                    filter_regex: Some(filter_regex),
                    select: Some("last".to_string()),
                    on_missing: Some("skip_sample".to_string()),
                },
            )],
            None => Vec::new(),
        };

        Ok((configs, cleanup_param, None, Some(surrogate), computed_labels))
    }

    fn get_lag_transition_streaming_aggregation_configs(
        &self,
        schema: &SQLSchema,
        query_evaluation_time: f64,
    ) -> Result<
        (
            Vec<IntermediateAggConfig>,
            Option<u64>,
            Option<StatefulTransitionConfig>,
            Option<String>,
        ),
        ControllerError,
    > {
        let m = parse_lag_transition_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse lag-transition SQL query: {}",
                self.query_string
            ))
        })?;

        let derived_metric = m.derived_metric();

        // Same surrogate-query trick as MOAS above: translate the pattern into
        // a plain query over its derived stream, then let the ordinary
        // classic-path machinery (parse -> match -> build_agg_configs_for_statistics)
        // do the rest, so top-k/ordinary-count selection logic isn't duplicated.
        // build_lag_transition_surrogate is the same function the query engine
        // calls at serve time (via rewrite_lag_transition_query) - using the
        // shared builder instead of a local format! is what guarantees the
        // registered template and the runtime rewrite can never drift apart.
        let surrogate = build_lag_transition_surrogate(&m);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse lag-transition surrogate query: {surrogate}"
            ))
        })?;

        let sql_query = SQLPatternMatcher::new(
            schema.clone(),
            self.data_ingestion_interval_ms as f64 / 1000.0,
        )
        .query_info_to_pattern(&qdata);

        if !sql_query.is_valid() {
            return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
        }
        if sql_query.query_data.len() != 1 {
            return Err(ControllerError::SqlParse(format!(
                "Lag-transition surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let agg_info = &q.aggregation_info;
        let labels = &q.labels;
        let table_name = &q.metric;
        let value_column = agg_info.get_value_column_name().to_string();

        let window_cfg = compute_sql_window(
            &q.time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;
        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());

        let sql_topk = detect_sql_topk(&qdata);
        let treatment_type = get_sql_treatment_type(agg_info.get_name());
        let statistics = if sql_topk.is_some() {
            vec![Statistic::Topk]
        } else {
            get_sql_statistics(agg_info.get_name())?
        };
        let topk_k = sql_topk.map(|t| t.k);
        let topk_count_events = sql_topk.map(|t| t.count_events());

        let mut configs = build_agg_configs_for_statistics(
            &statistics,
            treatment_type,
            &spatial_output,
            &KeyByLabelNames::empty(),
            &window_cfg,
            table_name,
            Some(table_name),
            Some(&value_column),
            qdata.spatial_filter.as_deref().unwrap_or(""),
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

        if sql_topk.is_some() {
            for cfg in &mut configs {
                if cfg.aggregation_type == AggregationType::CountMinSketchWithHeap {
                    cfg.grouping_labels = KeyByLabelNames::empty();
                    cfg.aggregated_labels = spatial_output.clone();
                }
            }
        }

        let t_lookback_ms = (q.time_info.get_duration() * 1000.0).round() as u64;
        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_sql_cleanup_param(self.cleanup_policy, t_lookback_ms, self.t_repeat_ms)
                    .map_err(ControllerError::PlannerError)?,
            )
        };

        let stateful_transition = StatefulTransitionConfig {
            metric_name: derived_metric,
            partition_by: m.partition_by,
            state_column: m.state_column,
            previous_alias: m.previous_alias,
            predicate: m.predicate,
            emit_labels: vec![m.group_label],
        };

        Ok((
            configs,
            cleanup_param,
            Some(stateful_transition),
            Some(surrogate),
        ))
    }

    fn get_token_select_streaming_aggregation_configs(
        &self,
        _schema: &SQLSchema,
        query_evaluation_time: f64,
    ) -> Result<
        (
            Vec<IntermediateAggConfig>,
            Option<u64>,
            Option<StatefulTransitionConfig>,
            Option<String>,
            Vec<(String, ComputedLabelConfig)>,
        ),
        ControllerError,
    > {
        let m = parse_token_select_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse token-select SQL query: {}",
                self.query_string
            ))
        })?;

        // The computed label (e.g. origin_asn) doesn't exist as a real column
        // in any table definition - it's synthesized at ingest time by the
        // ComputedLabelConfig this function also emits. Schema validation
        // needs to already know about it, or the surrogate below fails with
        // "attempt to aggregate by columns {label}, which are not present
        // for metric X" even though the label will genuinely exist by the
        // time ingest runs. Register it on a cloned schema used only for
        // planning this one query.
        let mut augmented_tables = self.table_definitions.clone();
        for t in &mut augmented_tables {
            if !t.metadata_columns.iter().any(|c| c == &m.label) {
                t.metadata_columns.push(m.label.clone());
            }
        }
        let schema = &build_sql_schema(&augmented_tables);

        // Treat the computed label as an ordinary column of the base metric:
        // the label itself replaces the nested subquery entirely, and
        // {where_clause} is the inner subquery's real filter (the outer
        // `WHERE length(...) > 0` guard is dropped - that's exactly what
        // on_missing: skip_sample already means at ingest time). Shared
        // builder, same reasoning as the lag-transition path above.
        let surrogate = build_token_select_surrogate(&m);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse token-select surrogate query: {surrogate}"
            ))
        })?;

        let sql_query = SQLPatternMatcher::new(
            schema.clone(),
            self.data_ingestion_interval_ms as f64 / 1000.0,
        )
        .query_info_to_pattern(&qdata);

        if !sql_query.is_valid() {
            return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
        }
        if sql_query.query_data.len() != 1 {
            return Err(ControllerError::SqlParse(format!(
                "Token-select surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let agg_info = &q.aggregation_info;
        let labels = &q.labels;
        let table_name = &q.metric;
        let value_column = agg_info.get_value_column_name().to_string();

        let window_cfg = compute_sql_window(
            &q.time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;
        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());

        let sql_topk = detect_sql_topk(&qdata);
        let treatment_type = get_sql_treatment_type(agg_info.get_name());
        let statistics = if sql_topk.is_some() {
            vec![Statistic::Topk]
        } else {
            get_sql_statistics(agg_info.get_name())?
        };
        let rollup = if statistics.contains(&Statistic::Cardinality) {
            KeyByLabelNames::empty()
        } else {
            get_all_metadata_columns(&augmented_tables, table_name)?.difference(&spatial_output)
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
            qdata.spatial_filter.as_deref().unwrap_or(""),
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

        if sql_topk.is_some() {
            for cfg in &mut configs {
                if cfg.aggregation_type == AggregationType::CountMinSketchWithHeap {
                    cfg.grouping_labels = KeyByLabelNames::empty();
                    cfg.aggregated_labels = spatial_output.clone();
                }
            }
        }

        let t_lookback_ms = (q.time_info.get_duration() * 1000.0).round() as u64;
        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_sql_cleanup_param(self.cleanup_policy, t_lookback_ms, self.t_repeat_ms)
                    .map_err(ControllerError::PlannerError)?,
            )
        };

        let computed_label = ComputedLabelConfig {
            r#type: "token_select".to_string(),
            source_col: m.source_col,
            tokenizer: Some("whitespace".to_string()),
            filter_regex: Some(m.filter_regex),
            select: Some("last".to_string()),
            on_missing: Some("skip_sample".to_string()),
        };

        Ok((
            configs,
            cleanup_param,
            None,
            Some(surrogate),
            vec![(m.label, computed_label)],
        ))
    }

    fn get_token_explode_streaming_aggregation_configs(
        &self,
        _schema: &SQLSchema,
        query_evaluation_time: f64,
    ) -> Result<
        (
            Vec<IntermediateAggConfig>,
            Option<u64>,
            Option<StatefulTransitionConfig>,
            Option<String>,
            Vec<(String, ComputedLabelConfig)>,
        ),
        ControllerError,
    > {
        let m = parse_token_explode_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse token-explode SQL query: {}",
                self.query_string
            ))
        })?;

        let mut augmented_tables = self.table_definitions.clone();
        for t in &mut augmented_tables {
            if !t.metadata_columns.iter().any(|c| c == &m.label) {
                t.metadata_columns.push(m.label.clone());
            }
        }
        let schema = &build_sql_schema(&augmented_tables);

        // Shared builder - same function the query engine calls at serve time.
        let surrogate = build_token_explode_surrogate(&m);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse token-explode surrogate query: {surrogate}"
            ))
        })?;

        let sql_query = SQLPatternMatcher::new(
            schema.clone(),
            self.data_ingestion_interval_ms as f64 / 1000.0,
        )
        .query_info_to_pattern(&qdata);

        if !sql_query.is_valid() {
            return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
        }
        if sql_query.query_data.len() != 1 {
            return Err(ControllerError::SqlParse(format!(
                "Token-explode surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let agg_info = &q.aggregation_info;
        let labels = &q.labels;
        let table_name = &q.metric;
        let value_column = agg_info.get_value_column_name().to_string();

        let window_cfg = compute_sql_window(
            &q.time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;
        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());

        let sql_topk = detect_sql_topk(&qdata);
        let treatment_type = get_sql_treatment_type(agg_info.get_name());
        let statistics = if sql_topk.is_some() {
            vec![Statistic::Topk]
        } else {
            get_sql_statistics(agg_info.get_name())?
        };
        let rollup = if statistics.contains(&Statistic::Cardinality) {
            KeyByLabelNames::empty()
        } else {
            get_all_metadata_columns(&augmented_tables, table_name)?.difference(&spatial_output)
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
            qdata.spatial_filter.as_deref().unwrap_or(""),
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

        if sql_topk.is_some() {
            for cfg in &mut configs {
                if cfg.aggregation_type == AggregationType::CountMinSketchWithHeap {
                    cfg.grouping_labels = KeyByLabelNames::empty();
                    cfg.aggregated_labels = spatial_output.clone();
                }
            }
        }

        let t_lookback_ms = (q.time_info.get_duration() * 1000.0).round() as u64;
        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_sql_cleanup_param(self.cleanup_policy, t_lookback_ms, self.t_repeat_ms)
                    .map_err(ControllerError::PlannerError)?,
            )
        };

        let computed_label = ComputedLabelConfig {
            r#type: "token_explode".to_string(),
            source_col: m.source_col,
            tokenizer: Some("whitespace".to_string()),
            filter_regex: Some(m.filter_regex),
            select: None,
            on_missing: Some("skip_sample".to_string()),
        };

        Ok((
            configs,
            cleanup_param,
            None,
            Some(surrogate),
            vec![(m.label, computed_label)],
        ))
    }


    fn get_bucketed_countif_streaming_aggregation_configs(
        &self,
        bucketed: &SQLBucketedCountIfQueryData,
    ) -> Result<(Vec<IntermediateAggConfig>, Option<u64>), ControllerError> {
        if bucketed.bucket_ms == 0 {
            return Err(ControllerError::PlannerError(
                "bucket size must be positive".to_string(),
            ));
        }

        if bucketed.bucket_ms < self.data_ingestion_interval_ms {
            return Err(ControllerError::PlannerError(format!(
                "bucket size ({}ms) must be >= data_ingestion_interval_ms ({}ms)",
                bucketed.bucket_ms, self.data_ingestion_interval_ms
            )));
        }

        if bucketed.bucket_ms % self.data_ingestion_interval_ms != 0 {
            return Err(ControllerError::PlannerError(format!(
                "bucket size ({}ms) must be a multiple of data_ingestion_interval_ms ({}ms)",
                bucketed.bucket_ms, self.data_ingestion_interval_ms
            )));
        }

        if bucketed.outputs.is_empty() {
            return Err(ControllerError::SqlParse(
                "bucketed countIf query has no outputs".to_string(),
            ));
        }

        let table_name = &bucketed.metric;

        // Bucketed countIf produces scalar counts per bucket. The bucket dimension
        // is time, handled by window/range execution, not a metadata label.
        //
        // Do not roll up over all metadata columns here; otherwise the planner
        // creates key-enumeration aggregations for a query whose output is just
        // one scalar value per bucket per countIf output.
        let spatial_output = KeyByLabelNames::empty();
        let rollup = KeyByLabelNames::empty();

        let window_cfg = IntermediateWindowConfig {
            window_size_ms: bucketed.bucket_ms,
            slide_interval_ms: bucketed.bucket_ms,
            window_type: WindowType::Tumbling,
        };

        let mut configs = Vec::new();

        for output in &bucketed.outputs {
            let spatial_filter =
                combine_spatial_filters(bucketed.base_spatial_filter.as_deref(), &output.filter);

            let mut output_configs = build_agg_configs_for_statistics(
                &[Statistic::Count],
                get_sql_treatment_type("COUNT"),
                &spatial_output,
                &rollup,
                &window_cfg,
                table_name,
                Some(table_name),
                Some("__event_count__"),
                &spatial_filter,
                |agg_type: AggregationType, agg_sub_type: &str| {
                    build_sketch_parameters(
                        agg_type,
                        agg_sub_type,
                        None,
                        None,
                        self.sketch_parameters.as_ref(),
                    )
                },
            )
            .map_err(ControllerError::SqlParse)?;

            // The generic Count path may include a key-enumeration aggregation
            // so grouped queries can discover candidate keys. Bucketed countIf
            // outputs are scalar counts per time bucket, so no key-discovery
            // aggregation is needed.
            output_configs.retain(|cfg| {
                !matches!(
                    cfg.aggregation_type,
                    AggregationType::DeltaSetAggregator | AggregationType::SetAggregator
                )
            });

            configs.append(&mut output_configs);
        }

        let t_lookback_ms = (bucketed.time_info.get_duration() * 1000.0).round() as u64;
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

// ---------------------------------------------------------------------------
// Lag-transition pattern (e.g. ClickHouse `lagInFrame(...) OVER (PARTITION BY
// ... ORDER BY ...)` wrapped in a CTE with an outer countIf) → derived event
// stream + StatefulTransitionConfig.
//
// This mirrors the shape the query engine's rewrite_lag_transition_query
// (engines/simple_engine/sql.rs) already produces at serve time - but
// previously nothing on the planning side auto-generated the
// StatefulTransitionConfig that makes that rewrite valid; a human had to
// notice the pattern and hand-write it into streaming_config.yaml. This is
// the automated version: the planner detects the pattern from raw SQL and
// emits both a normal aggregation config (via the same surrogate-query path
// MOAS already uses) and the StatefulTransitionConfig, so the CTE/window-
// function complexity never has to be understood by the rest of the planner.
// ---------------------------------------------------------------------------

// The lag-transition / token-select / token-explode detectors used to be
// defined here directly; they're now shared with the query engine (which
// needs the identical detection+rewrite logic at serve time) via
// sql_utilities::ast_matching::pattern_rewrites, imported above.

fn combine_spatial_filters(base: Option<&str>, extra: &str) -> String {
    match (base, extra.trim()) {
        (Some(b), e) if !b.trim().is_empty() && !e.is_empty() => {
            format!("{} AND {}", b.trim(), e)
        }
        (Some(b), _) if !b.trim().is_empty() => b.trim().to_string(),
        (_, e) => e.to_string(),
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

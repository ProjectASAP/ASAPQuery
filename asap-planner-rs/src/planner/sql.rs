use std::collections::HashSet;

use asap_types::computed_label::ComputedLabelConfig;
use asap_types::derived_value::{DerivedValueConfig, DerivedValueKind};
use asap_types::enums::{CleanupPolicy, WindowType};
use asap_types::stateful_transition::StatefulTransitionConfig;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, QueryTreatmentType, Statistic};
use sql_utilities::ast_matching::pattern_rewrites::{
    build_bucketed_topn_surrogate, build_lag_gap_surrogate, parse_bucketed_topn_query,
    parse_lag_gap_query, build_computed_group_by_surrogate, build_lag_transition_surrogate,
    build_group_array_distinct_surrogate, build_moas_surrogate,
    build_select_distinct_surrogate, build_token_explode_surrogate, build_token_select_surrogate,
    looks_like_arrayzip_edge_explode_sql,
    looks_like_exact_only_sql, looks_like_group_array_distinct_sql, looks_like_lag_transition_sql,
    looks_like_moas_sql,
    looks_like_flat_token_explode_sql, looks_like_token_explode_sql, looks_like_token_select_sql,
    parse_arrayzip_edge_explode_query, parse_flat_token_explode_query,
    looks_like_unevaluable_where_subquery_sql, parse_computed_group_by_query,
    parse_group_array_distinct_query, parse_lag_transition_query, parse_moas_query,
    parse_select_distinct_query,
    parse_token_explode_query, parse_token_select_query, scalar_aggregate_has_unusable_value_arg,
    scalar_aggregate_value_column, parse_arg_agg_query, parse_computed_value_agg_query,
    build_weekly_moas_histogram_inner_surrogate, parse_weekly_moas_histogram_query,
    replace_from_table, strip_query_string_cast_wrapper, HAVING_COUNT_ALIAS,
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
        if looks_like_exact_only_sql(&self.query_string) {
            return true;
        }
        // A scalar aggregate whose argument is the table's own time column
        // (e.g. `min(timestamp)`) isn't aggregating a value column at all -
        // `get_streaming_aggregation_configs` would reject it deep inside
        // with `InvalidValueCol` instead of punting cleanly. Catch it here,
        // at the same point every other unplannable shape is caught.
        if let Some(col) = scalar_aggregate_value_column(&self.query_string) {
            if self
                .table_definitions
                .iter()
                .any(|t| t.time_column.eq_ignore_ascii_case(&col))
            {
                return true;
            }
        }
        // A scalar aggregate whose argument is a multi-argument call (e.g.
        // `uniqExact(prefix, operation, as_path)`) or a computed expression
        // (e.g. `uniqExact(splitByChar(' ', as_path)[1])`) has no single
        // bare-column value the classic parser can use either - same
        // treatment, same reason.
        if scalar_aggregate_has_unusable_value_arg(&self.query_string) {
            return true;
        }
        false
    }

    /// True when the WHERE clause contains a nested SELECT the ingest-time
    /// spatial filter can't evaluate - see
    /// `looks_like_unevaluable_where_subquery_sql`. Same treatment as
    /// `is_exact_only`: punt to ClickHouse rather than build a summary
    /// against a filter that can never match correctly.
    pub fn has_unevaluable_where_subquery(&self) -> bool {
        looks_like_unevaluable_where_subquery_sql(&self.query_string)
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

        // SELECT DISTINCT path: a plain distinct-value listing, no GROUP BY
        // - see SelectDistinctMatch. Same swap MOAS makes just above
        // (approximate CARDINALITY -> exact SetAggregator), with an empty
        // grouping key instead of an outer GROUP BY column - one global
        // exact set, not one per group.
        if parse_select_distinct_query(&self.query_string).is_some() {
            return self.get_select_distinct_streaming_aggregation_configs(
                &schema,
                query_evaluation_time,
            );
        }

        // groupArray(DISTINCT col) path: an exact distinct-value list,
        // optionally grouped - see GroupArrayDistinctMatch. Same swap as
        // the two paths above (approximate CARDINALITY -> exact
        // SetAggregator), generalized to an optional single GROUP BY
        // column instead of always-grouped (MOAS) or never-grouped (SELECT
        // DISTINCT). Always reached via the multi-aggregate split (see the
        // comment on that path below) since every query needing this so
        // far pairs it with its own uniqExact(col) sibling.
        if looks_like_group_array_distinct_sql(&self.query_string) {
            return self.get_group_array_distinct_streaming_aggregation_configs(
                &schema,
                query_evaluation_time,
            );
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

        // Note: lag-gap (`dateDiff(... lagInFrame(...) OVER (...) ...) AS
        // gap`, see LagGapMatch) is dispatched from generator.rs instead of
        // here, unlike every other pattern above - it needs to register a
        // whole new virtual TableDefinition for the derived metric (so
        // "gap" resolves as a real value column both at plan time and when
        // the engine re-parses the surrogate at serve time), which this
        // function's shared 5-tuple return type has no slot for. Threading
        // that extra table through here would touch every caller of this
        // function; generator.rs's own per-mechanism dispatch (see
        // get_lag_gap_streaming_aggregation_configs, called directly with
        // its own wider return type) keeps the blast radius to just this
        // one mechanism.

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

        // arrayZip edge-explode (q067's AS-path adjacency/"edge" shape) -
        // checked BEFORE flat token-explode below: its raw SQL also
        // contains both "arrayjoin(" and "splitbychar(" (nested inside
        // arrayZip/arraySlice), so looks_like_flat_token_explode_sql would
        // otherwise claim it first, and its own inner parser would then
        // fail (the arrayJoin's argument is arrayZip(...), not
        // splitByChar(...) directly), turning a should-punt-cleanly-and-
        // try-the-next-mechanism case into a hard Err instead.
        if looks_like_arrayzip_edge_explode_sql(&self.query_string) {
            return self.get_arrayzip_edge_explode_streaming_aggregation_configs(
                &schema,
                query_evaluation_time,
            );
        }

        // Token-explode path: same tokenization building block as above, but
        // every matching token becomes its own row (arrayJoin) instead of
        // indexing just the last one. Covers both the nested-subquery +
        // regex-filtered shape and the flat, unfiltered one (q101's
        // `arrayJoin(splitByChar(...)) AS asn` directly in a top-level
        // SELECT, no subquery, no arrayFilter condition) -
        // get_token_explode_streaming_aggregation_configs tries both
        // parsers so every downstream builder/register/serve path only
        // has to handle one shape (TokenExplodeMatch), not two.
        if looks_like_token_explode_sql(&self.query_string)
            || looks_like_flat_token_explode_sql(&self.query_string)
        {
            return self
                .get_token_explode_streaming_aggregation_configs(&schema, query_evaluation_time);
        }

        // Computed-expression GROUP BY (e.g. `splitByChar('/', prefix)[2]
        // AS prefix_len ... GROUP BY prefix_len`): the group key is derived
        // directly in the outer SELECT, no nested subquery - same building
        // block as token-select/token-explode above, just a different
        // detection shape.
        if parse_computed_group_by_query(&self.query_string).is_some() {
            return self.get_computed_group_by_streaming_aggregation_configs(
                &schema,
                query_evaluation_time,
            );
        }

        // Bucketed top-N (e.g. `row_number() OVER (PARTITION BY hour ORDER
        // BY cnt DESC) AS rnk ... WHERE rnk <= 5`): a real time bucket
        // (unlike the cyclical toHour/toDayOfWeek shapes computed_group_by
        // handles, which collapse the range) GROUP-BY'd together with a
        // real key column - see BucketedTopNMatch.
        if let Some(bm) = parse_bucketed_topn_query(&self.query_string) {
            let surrogate = build_bucketed_topn_surrogate(&bm);
            let (configs, cleanup) = self.get_bucketed_topn_streaming_aggregation_configs(
                &bm,
                &schema,
                query_evaluation_time,
            )?;
            return Ok((configs, cleanup, None, Some(surrogate), Vec::new()));
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
        // The bare-key-HAVING surrogate (see GroupByHavingCountMatch) always
        // aliases its hidden count as HAVING_COUNT_ALIAS. An exact-equality
        // HAVING filter is maximally sensitive to CountMinSketch noise (any
        // overestimate at all flips the result), unlike a range-tolerant
        // HAVING filter where a small overestimate rarely changes the
        // outcome - so this shape alone opts into exact (MultipleSum)
        // treatment instead of the default approximate COUNT treatment,
        // without changing behavior for ordinary user COUNT/HAVING queries.
        let treatment_type = if qdata.aggregation_alias.as_deref() == Some(HAVING_COUNT_ALIAS) {
            QueryTreatmentType::Exact
        } else {
            get_sql_treatment_type(agg_info.get_name())
        };
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

    /// `SELECT DISTINCT <col> FROM ... WHERE ...` with no GROUP BY - see
    /// `SelectDistinctMatch`. Same swap MOAS makes (approximate CARDINALITY
    /// -> exact SetAggregator), just with an empty grouping key instead of
    /// an outer GROUP BY column: one global exact set, not one set per
    /// group.
    fn get_select_distinct_streaming_aggregation_configs(
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
        let m = parse_select_distinct_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse SELECT DISTINCT SQL query: {}",
                self.query_string
            ))
        })?;

        let surrogate = build_select_distinct_surrogate(&m);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse SELECT DISTINCT surrogate query: {surrogate}"
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
                "SELECT DISTINCT surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let agg_info = &q.aggregation_info;
        let table_name = &q.metric;
        let value_column = agg_info.get_value_column_name().to_string();

        if agg_info.get_name() != "CARDINALITY" || value_column != m.column {
            return Err(ControllerError::SqlParse(format!(
                "SELECT DISTINCT path expected uniqExact({}), got {}({})",
                m.column,
                agg_info.get_name(),
                value_column
            )));
        }

        let window_cfg = compute_sql_window(
            &q.time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;

        let mut configs = build_agg_configs_for_statistics(
            &[Statistic::Cardinality],
            QueryTreatmentType::Approximate,
            &KeyByLabelNames::empty(),
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
            // Replace the default CARDINALITY/HLL lowering with an exact
            // set-valued summary, the same swap MOAS makes - a `SELECT
            // DISTINCT` needs the actual values, not only their count.
            cfg.aggregation_type = AggregationType::SetAggregator;
            cfg.aggregation_sub_type = "".to_string();
            cfg.parameters.clear();
            cfg.grouping_labels = KeyByLabelNames::empty();
            cfg.aggregated_labels = KeyByLabelNames::new(vec![m.column.clone()]);
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

        Ok((configs, cleanup_param, None, Some(surrogate), Vec::new()))
    }

    /// `groupArray(DISTINCT <col>) AS <alias>`, optionally `GROUP BY
    /// <group_col>` - see `GroupArrayDistinctMatch`. Same CARDINALITY ->
    /// SetAggregator swap `get_select_distinct_streaming_aggregation_configs`
    /// and `get_moas_streaming_aggregation_configs` both make, generalized
    /// to an optional single grouping column: `m.group_by.is_some()` picks
    /// MOAS's one-grouping-column shape, `None` picks SELECT DISTINCT's
    /// empty-grouping-key shape.
    fn get_group_array_distinct_streaming_aggregation_configs(
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
        let m = parse_group_array_distinct_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse groupArray(DISTINCT ...) SQL query: {}",
                self.query_string
            ))
        })?;

        let surrogate = build_group_array_distinct_surrogate(&m);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse groupArray(DISTINCT ...) surrogate query: {surrogate}"
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
                "groupArray(DISTINCT ...) surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let agg_info = &q.aggregation_info;
        let table_name = &q.metric;
        let value_column = agg_info.get_value_column_name().to_string();

        if agg_info.get_name() != "CARDINALITY" || value_column != m.column {
            return Err(ControllerError::SqlParse(format!(
                "groupArray(DISTINCT ...) path expected uniqExact({}), got {}({})",
                m.column,
                agg_info.get_name(),
                value_column
            )));
        }

        let window_cfg = compute_sql_window(
            &q.time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;

        let grouping_output = match &m.group_by {
            Some(group_by) => KeyByLabelNames::new(vec![group_by.clone()]),
            None => KeyByLabelNames::empty(),
        };

        let mut configs = build_agg_configs_for_statistics(
            &[Statistic::Cardinality],
            QueryTreatmentType::Approximate,
            &grouping_output,
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
            // Replace the default CARDINALITY/HLL lowering with an exact
            // set-valued summary - groupArray(DISTINCT ...) needs the
            // actual values, not only their count (the count is served by
            // its own separate uniqExact(...) sibling surrogate).
            cfg.aggregation_type = AggregationType::SetAggregator;
            cfg.aggregation_sub_type = "".to_string();
            cfg.parameters.clear();
            cfg.grouping_labels = grouping_output.clone();
            cfg.aggregated_labels = KeyByLabelNames::new(vec![m.column.clone()]);
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

        Ok((configs, cleanup_param, None, Some(surrogate), Vec::new()))
    }

    /// `SELECT <week_col>, count(*) AS <outer_alias> FROM (SELECT
    /// toStartOfWeek(<time_col>) AS <week_col>, <prefix_col>,
    /// uniqExact(<origin_col>) AS <cnt_alias> FROM ... GROUP BY <week_col>,
    /// <prefix_col> HAVING <cnt_alias> > <n>) GROUP BY <week_col>` - q133's
    /// weekly MOAS histogram, see `WeeklyMoasHistogramMatch`'s doc comment.
    /// Only the INNER query is registered: MOAS generalized with an extra
    /// weekly-bucket grouping dimension (same CARDINALITY -> SetAggregator
    /// swap `get_moas_streaming_aggregation_configs` makes, just with two
    /// grouping columns instead of one) - a dedicated function rather than
    /// a change to that one, to keep zero risk to MOAS's own already-
    /// passing single-column shape. The outer histogram stage is computed
    /// entirely at serve time from the inner's own result set - see
    /// handle_weekly_moas_histogram_sql in the query engine.
    pub(crate) fn get_weekly_moas_histogram_streaming_aggregation_configs(
        &self,
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
        let m = parse_weekly_moas_histogram_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse weekly-MOAS-histogram SQL query: {}",
                self.query_string
            ))
        })?;

        let surrogate = build_weekly_moas_histogram_inner_surrogate(&m);

        // The week column doesn't exist as a real schema column - it's
        // synthesized at ingest time by the ComputedLabelConfig this
        // function also emits. Schema validation needs to already know
        // about it, or the surrogate below fails - same reasoning as
        // get_computed_group_by_streaming_aggregation_configs.
        let mut augmented_tables = self.table_definitions.clone();
        for t in &mut augmented_tables {
            if !t.metadata_columns.iter().any(|c| c == &m.week_col) {
                t.metadata_columns.push(m.week_col.clone());
            }
        }
        let schema = &build_sql_schema(&augmented_tables);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse weekly-MOAS-histogram surrogate query: {surrogate}"
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
                "weekly-MOAS-histogram surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let agg_info = &q.aggregation_info;
        let labels = &q.labels;
        let table_name = &q.metric;
        let value_column = agg_info.get_value_column_name().to_string();

        if agg_info.get_name() != "CARDINALITY" || value_column != m.origin_col {
            return Err(ControllerError::SqlParse(format!(
                "weekly-MOAS-histogram path expected uniqExact({}), got {}({})",
                m.origin_col,
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
            cfg.aggregation_type = AggregationType::SetAggregator;
            cfg.aggregation_sub_type = "".to_string();
            cfg.parameters.clear();
            cfg.grouping_labels =
                KeyByLabelNames::new(vec![m.week_col.clone(), m.prefix_col.clone()]);
            cfg.aggregated_labels = KeyByLabelNames::new(vec![m.origin_col.clone()]);
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

        let computed_label = ComputedLabelConfig {
            r#type: "week_start_bucket".to_string(),
            source_col: m.time_col,
            tokenizer: None,
            filter_regex: None,
            select: None,
            on_missing: Some("skip_sample".to_string()),
        };

        Ok((
            configs,
            cleanup_param,
            None,
            Some(surrogate),
            vec![(m.week_col, computed_label)],
        ))
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
            gap_unit: None,
            min_gap: None,
        };

        Ok((
            configs,
            cleanup_param,
            Some(stateful_transition),
            Some(surrogate),
        ))
    }

    /// `dateDiff('second', lagInFrame(<col>) OVER (PARTITION BY <p> ORDER BY
    /// <col>), <col>) AS <gap_alias>`, then an outer aggregate over
    /// `<gap_alias>` (e.g. `quantile(0.5)(gap) AS median_gap_seconds`) - see
    /// `LagGapMatch`. Same surrogate-over-derived-stream trick as
    /// `get_lag_transition_streaming_aggregation_configs`, but the
    /// `StatefulTransitionConfig` is built in gap mode (`gap_unit`/
    /// `min_gap` set, `predicate`/`previous_alias` unused) so the derived
    /// stream carries the numeric gap itself instead of a boolean-triggered
    /// event - letting the classic path's OWN outer aggregate (quantile,
    /// avg, max, ...) run over it unchanged.
    /// Called directly from generator.rs, not dispatched from within
    /// `get_streaming_aggregation_configs` like every other pattern - its
    /// return type carries an extra `TableDefinition` (the derived metric's
    /// virtual table, needed so "gap" resolves as a real value column both
    /// now and when the engine re-parses the surrogate at serve time) that
    /// the shared 5-tuple has no slot for.
    pub(crate) fn get_lag_gap_streaming_aggregation_configs(
        &self,
        query_evaluation_time: f64,
    ) -> Result<
        (
            Vec<IntermediateAggConfig>,
            Option<u64>,
            Option<StatefulTransitionConfig>,
            String,
            TableDefinition,
        ),
        ControllerError,
    > {
        let m = parse_lag_gap_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse lag-gap SQL query: {}",
                self.query_string
            ))
        })?;

        let derived_metric = m.derived_metric();
        let surrogate = build_lag_gap_surrogate(&m);

        // Unlike lag-transition's count() (no value-column argument at
        // all), quantile(p)(gap) needs "gap" to resolve as a real value
        // column - the classic parser validates aggregate value-column
        // names against the schema (unlike GROUP BY columns, which get the
        // "augment metadata_columns" treatment elsewhere in this file).
        // The derived metric isn't one of the configured tables, so build a
        // one-off virtual TableDefinition for it, same reasoning as
        // token-select's schema augmentation just applied to a whole new
        // table instead of an existing one's columns. This same
        // TableDefinition is returned to the caller, which must persist it
        // into the generated config - see get_lag_gap_streaming_aggregation_configs's
        // doc comment.
        let derived_table = TableDefinition {
            name: derived_metric.clone(),
            time_column: "timestamp".to_string(),
            value_columns: vec![m.gap_alias.clone()],
            metadata_columns: vec![m.partition_col.clone()],
        };
        let mut augmented_tables = self.table_definitions.clone();
        augmented_tables.push(derived_table.clone());
        let schema = &build_sql_schema(&augmented_tables);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse lag-gap surrogate query: {surrogate}"
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
                "Lag-gap surrogate must produce one query layer, got {}",
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

        let treatment_type = get_sql_treatment_type(agg_info.get_name());
        let statistics = get_sql_statistics(agg_info.get_name())?;

        let configs = build_agg_configs_for_statistics(
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
                build_sketch_parameters(agg_type, agg_sub_type, None, None, self.sketch_parameters.as_ref())
            },
        )
        .map_err(ControllerError::SqlParse)?;

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
            partition_by: vec![m.partition_col.clone()],
            state_column: m.state_column,
            previous_alias: String::new(),
            predicate: String::new(),
            emit_labels: vec![m.partition_col],
            gap_unit: Some(m.gap_unit),
            min_gap: m.min_gap,
        };

        Ok((
            configs,
            cleanup_param,
            Some(stateful_transition),
            surrogate,
            derived_table,
        ))
    }

    /// `min`/`max`/`sum`/`avg` over a column that's real (it's the table's
    /// time column, or a metadata column like `med`) but isn't in the
    /// table's declared `value_columns` - so the classic parser's schema
    /// validation rejects it outright (`InvalidValueCol`) even though the
    /// aggregate itself is perfectly well-defined. Reusing the table's one
    /// shared ingest value for this would silently corrupt every
    /// count()/countIf() already registered on it (see `DerivedValueConfig`'s
    /// doc comment for why), so - same move as lag-gap above - this routes
    /// the aggregate through its own one-off virtual table, backed by its
    /// own derived ingest stream that just re-emits this one column's value.
    ///
    /// Returns `Ok(None)` when the query doesn't need this treatment at all
    /// (the aggregate's column is already a real value column, the aggregate
    /// isn't one of the four this applies to, or the column isn't real at
    /// all - e.g. a computed expression, a different failure this mechanism
    /// isn't meant to paper over) - callers should fall through to the
    /// ordinary classic path, which already handles those correctly.
    /// Returns `Err` only once this mechanism has committed to applying
    /// (the column genuinely needs a derived table) but something
    /// afterwards didn't work out; callers should treat that the same as
    /// `Ok(None)` and let the existing punting path report it, rather than
    /// adding a second, differently-worded error for what's ultimately the
    /// same "can't plan this" outcome.
    pub(crate) fn get_raw_value_agg_streaming_aggregation_configs(
        &self,
        query_evaluation_time: f64,
    ) -> Result<
        Option<(
            Vec<IntermediateAggConfig>,
            Option<u64>,
            DerivedValueConfig,
            String,
            TableDefinition,
        )>,
        ControllerError,
    > {
        let schema = build_sql_schema(&self.table_definitions);
        // `avg(toFloat64OrZero(toString(med)))` etc: a trivial format
        // wrapper around a bare column, not a real computation - strip it
        // down to `avg(med)` before parsing so the classic aggregation
        // parser (which only accepts a bare identifier as an aggregate's
        // argument) sees the bare column it actually is.
        let unwrapped = strip_query_string_cast_wrapper(&self.query_string);
        let query_to_parse = unwrapped.as_deref().unwrap_or(&self.query_string);
        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, query_to_parse)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;
        let parser = SQLPatternParser::new(&schema, query_evaluation_time);
        let Some(qdata) = parser.parse_query(&stmts) else {
            return Ok(None);
        };

        let agg_info = &qdata.aggregation_info;
        if !matches!(
            agg_info.get_name(),
            "MIN" | "MAX" | "SUM" | "AVG" | "CARDINALITY" | "QUANTILE"
        ) {
            return Ok(None);
        }
        let raw_col = agg_info.get_value_column_name().to_string();
        let table_name = qdata.metric.clone();

        if schema.is_valid_value_column(&table_name, &raw_col) {
            // Already a real value column - nothing for this mechanism to do.
            return Ok(None);
        }
        let Some(original_table) = self.table_definitions.iter().find(|t| t.name == table_name)
        else {
            return Ok(None);
        };
        let is_time_col = original_table.time_column == raw_col;
        let is_metadata_col = original_table.metadata_columns.iter().any(|c| c == &raw_col);
        if !is_time_col && !is_metadata_col {
            return Ok(None);
        }

        let derived_metric = format!("derived_value_{}_{}", raw_col, table_name);
        let surrogate = replace_from_table(query_to_parse, &derived_metric);

        let derived_table = TableDefinition {
            name: derived_metric.clone(),
            time_column: original_table.time_column.clone(),
            value_columns: vec![raw_col.clone()],
            metadata_columns: original_table.metadata_columns.clone(),
        };
        let mut augmented_tables = self.table_definitions.clone();
        augmented_tables.push(derived_table.clone());
        let augmented_schema = build_sql_schema(&augmented_tables);

        let surrogate_stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;
        let surrogate_parser = SQLPatternParser::new(&augmented_schema, query_evaluation_time);
        let sq_data = surrogate_parser
            .parse_query(&surrogate_stmts)
            .ok_or_else(|| {
                ControllerError::SqlParse(format!(
                    "Failed to parse raw-value-aggregate surrogate query: {surrogate}"
                ))
            })?;

        let sql_query = SQLPatternMatcher::new(
            augmented_schema.clone(),
            self.data_ingestion_interval_ms as f64 / 1000.0,
        )
        .query_info_to_pattern(&sq_data);

        if !sql_query.is_valid() {
            return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
        }
        if sql_query.query_data.len() != 1 {
            return Err(ControllerError::SqlParse(format!(
                "Raw-value-aggregate surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let labels = &q.labels;
        let value_column = q.aggregation_info.get_value_column_name().to_string();

        let window_cfg = compute_sql_window(
            &q.time_info,
            self.data_ingestion_interval_ms,
            self.t_repeat_ms,
        )?;
        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());

        // `uniqExact`/`uniq`/`uniqCombined` all normalize to the same
        // "CARDINALITY" aggregation name upstream (sqlpattern_parser.rs),
        // discarding the exactness the SQL text actually declared - the
        // default lowering below would otherwise always pick the
        // approximate HLL sketch, silently serving an approximate answer
        // for a query that explicitly asked to be exact. Recover that
        // distinction here, straight from the original SQL text (this
        // function is always called with a surrogate representing exactly
        // one aggregate - see `build_multi_aggregate_surrogates` - so a
        // plain substring check is unambiguous).
        let treatment_type = if q.aggregation_info.get_name() == "CARDINALITY"
            && self.query_string.to_lowercase().contains("uniqexact(")
        {
            QueryTreatmentType::Exact
        } else {
            get_sql_treatment_type(q.aggregation_info.get_name())
        };
        let statistics = get_sql_statistics(q.aggregation_info.get_name())?;

        let mut configs = build_agg_configs_for_statistics(
            &statistics,
            treatment_type,
            &spatial_output,
            &KeyByLabelNames::empty(),
            &window_cfg,
            &derived_metric,
            Some(&derived_metric),
            Some(&value_column),
            q.spatial_filter.as_deref().unwrap_or(""),
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

        // `set_subpopulation_labels`'s binary choice (all of `spatial_output`
        // goes to *either* grouping_labels *or* aggregated_labels) has no way
        // to express what an exact per-group cardinality needs: BOTH the
        // query's own GROUP BY columns (`spatial_output`, e.g. "day") in
        // grouping_labels, to partition the store one SetAggregator instance
        // per group, AND the counted column itself (`value_column`, e.g.
        // "prefix") in aggregated_labels, so ingest-time row routing knows
        // which column's value to insert as a set member. Same two-labels-
        // at-once shape MOAS's own SetAggregator registration sets manually
        // for the same reason - see its override in this same file.
        if treatment_type == QueryTreatmentType::Exact
            && q.aggregation_info.get_name() == "CARDINALITY"
        {
            for cfg in &mut configs {
                cfg.grouping_labels = spatial_output.clone();
                cfg.aggregated_labels = KeyByLabelNames::new(vec![value_column.clone()]);
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

        let derived_value_config = DerivedValueConfig {
            metric_name: derived_metric,
            source_column: raw_col,
            kind: DerivedValueKind::Numeric,
        };

        Ok(Some((
            configs,
            cleanup_param,
            derived_value_config,
            surrogate,
            derived_table,
        )))
    }

    /// Same reasoning as `get_raw_value_agg_streaming_aggregation_configs`,
    /// one layer further out: MIN/MAX/SUM/AVG/uniqExact over a *computed*
    /// expression (`avg(length(splitByChar(' ', as_path)))`,
    /// `uniqExact(toDate(timestamp))`) rather than a bare column -
    /// `get_raw_value_agg_streaming_aggregation_configs` never sees these at
    /// all, since `SQLPatternParser::parse_query`'s aggregation parser only
    /// accepts a bare identifier as an aggregate's argument and returns
    /// `None` outright for anything else.
    ///
    /// Two-stage lowering: first swap the computed expression for a
    /// synthetic column name (`parse_computed_value_agg_query`, reusing the
    /// same `parse_computed_label_shape` recognizer `computed_group_by`
    /// uses for GROUP BY keys) and register it as a metadata column via
    /// `ComputedLabelConfig` - by ingest time it genuinely is a real
    /// (string) column. Then hand the resulting bare-column surrogate to a
    /// fresh `SQLSingleQueryProcessor` and delegate to
    /// `get_raw_value_agg_streaming_aggregation_configs` for the second
    /// stage (raw/metadata column not registered as a value column -> its
    /// own derived-value virtual table), exactly as if the query had
    /// referenced that synthetic column directly.
    pub(crate) fn get_computed_value_agg_streaming_aggregation_configs(
        &self,
        query_evaluation_time: f64,
    ) -> Result<
        Option<(
            Vec<IntermediateAggConfig>,
            Option<u64>,
            DerivedValueConfig,
            ComputedLabelConfig,
            String,
            TableDefinition,
        )>,
        ControllerError,
    > {
        let Some(m) = parse_computed_value_agg_query(&self.query_string) else {
            return Ok(None);
        };

        // The source column the computed expression reads from must be a
        // real metadata column somewhere in this query's table(s) - if it
        // isn't (a typo, a column from an unrelated table), this isn't a
        // shape worth committing to; let the ordinary punting path report
        // it plainly instead of this mechanism inventing a derived table
        // over a column that was never real.
        if !self.table_definitions.iter().any(|t| {
            t.time_column == m.source_col || t.metadata_columns.iter().any(|c| c == &m.source_col)
        }) {
            return Ok(None);
        }

        let computed_label = ComputedLabelConfig {
            r#type: m.label_type.to_string(),
            source_col: m.source_col.clone(),
            tokenizer: m.tokenizer.clone(),
            filter_regex: None,
            select: m.select.clone(),
            on_missing: Some("skip_sample".to_string()),
        };

        let augmented_tables: Vec<TableDefinition> = self
            .table_definitions
            .iter()
            .map(|t| {
                let mut t = t.clone();
                let sources_it = t.time_column == m.source_col
                    || t.metadata_columns.iter().any(|c| c == &m.source_col);
                if sources_it && !t.metadata_columns.iter().any(|c| c == &m.synthetic_label) {
                    t.metadata_columns.push(m.synthetic_label.clone());
                }
                t
            })
            .collect();

        let stage_two = SQLSingleQueryProcessor::new(
            m.surrogate.clone(),
            self.t_repeat_ms,
            self.data_ingestion_interval_ms,
            augmented_tables,
            self.streaming_engine,
            self.sketch_parameters.clone(),
            self.cleanup_policy,
        );

        let Some((configs, cleanup_param, derived_value_config, inner_surrogate, derived_table)) =
            stage_two.get_raw_value_agg_streaming_aggregation_configs(query_evaluation_time)?
        else {
            return Ok(None);
        };

        Ok(Some((
            configs,
            cleanup_param,
            derived_value_config,
            computed_label,
            inner_surrogate,
            derived_table,
        )))
    }

    /// `argMax(x, <time_column>)` / `argMin(x, <time_column>)` - remembers
    /// the value of `x` from whichever row had the largest/smallest
    /// timestamp, per GROUP BY key. See `ArgAggMatch`'s doc comment for the
    /// exact shape matched and why the comparison column is restricted to
    /// the table's own time column.
    ///
    /// Routes through the same "derived-value virtual table" mechanism as
    /// `get_raw_value_agg_streaming_aggregation_configs`, but can't reuse
    /// that function directly: argMax/argMin has no representation in the
    /// classic single-aggregate model at all (two arguments, a string
    /// result) for `SQLPatternParser` to parse, even as a bare column. A
    /// placeholder `count(*)` surrogate over the derived table (same FROM/
    /// WHERE/GROUP BY) is parsed instead, purely to reuse its
    /// time_info/spatial_filter/labels extraction - the placeholder's own
    /// aggregation_info is discarded; the real statistic (`ArgMax`/
    /// `ArgMin`) and derived value column are already known from `m`.
    pub(crate) fn get_arg_agg_streaming_aggregation_configs(
        &self,
        query_evaluation_time: f64,
    ) -> Result<
        Option<(
            Vec<IntermediateAggConfig>,
            Option<u64>,
            DerivedValueConfig,
            String,
            TableDefinition,
        )>,
        ControllerError,
    > {
        let Some(m) = parse_arg_agg_query(&self.query_string) else {
            return Ok(None);
        };

        let Some(original_table) = self
            .table_definitions
            .iter()
            .find(|t| t.time_column == m.cmp_col)
        else {
            return Ok(None);
        };
        let table_name = original_table.name.clone();

        // arg_col must be a real metadata column - not the time column
        // itself (that's a different, meaningless shape: "the timestamp
        // at the row with the max timestamp" is just the max timestamp,
        // already served by MIN/MAX) and not already a value column
        // (argMax over the table's own designated value column isn't a
        // shape any target query uses; left unbuilt).
        if !original_table.metadata_columns.iter().any(|c| c == &m.arg_col) {
            return Ok(None);
        }

        let derived_metric = format!("derived_value_arg_{}_{}", m.arg_col, table_name);
        let derived_table = TableDefinition {
            name: derived_metric.clone(),
            time_column: original_table.time_column.clone(),
            value_columns: vec![m.arg_col.clone()],
            metadata_columns: original_table.metadata_columns.clone(),
        };

        let derived_value_config = DerivedValueConfig {
            metric_name: derived_metric.clone(),
            source_column: m.arg_col.clone(),
            kind: if m.is_max {
                DerivedValueKind::ArgMax
            } else {
                DerivedValueKind::ArgMin
            },
        };

        let limit_clause = m.limit.as_deref().map(|n| format!(" LIMIT {n}")).unwrap_or_default();
        let placeholder = format!(
            "SELECT {group_by}, count(*) AS __arg_agg_placeholder__ {from_where} GROUP BY {group_by}{limit}",
            group_by = m.group_by_col,
            from_where = m.from_where,
            limit = limit_clause,
        );
        let surrogate = replace_from_table(&placeholder, &derived_metric);

        let mut augmented_tables = self.table_definitions.clone();
        augmented_tables.push(derived_table.clone());
        let augmented_schema = build_sql_schema(&augmented_tables);

        let surrogate_stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;
        let surrogate_parser = SQLPatternParser::new(&augmented_schema, query_evaluation_time);
        let sq_data = surrogate_parser.parse_query(&surrogate_stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse arg-aggregate placeholder surrogate: {surrogate}"
            ))
        })?;

        let sql_query = SQLPatternMatcher::new(
            augmented_schema.clone(),
            self.data_ingestion_interval_ms as f64 / 1000.0,
        )
        .query_info_to_pattern(&sq_data);

        if !sql_query.is_valid() {
            return Err(ControllerError::SqlParse(sql_query.msg.unwrap_or_default()));
        }
        if sql_query.query_data.len() != 1 {
            return Err(ControllerError::SqlParse(format!(
                "Arg-aggregate placeholder surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }

        let q = &sql_query.query_data[0];
        let labels = &q.labels;

        let window_cfg = compute_sql_window(&q.time_info, self.data_ingestion_interval_ms, self.t_repeat_ms)?;
        let spatial_output = KeyByLabelNames::new(labels.iter().cloned().collect::<Vec<_>>());

        let statistic = if m.is_max { Statistic::ArgMax } else { Statistic::ArgMin };
        let treatment_type = get_sql_treatment_type(if m.is_max { "ARGMAX" } else { "ARGMIN" });

        let configs = build_agg_configs_for_statistics(
            &[statistic],
            treatment_type,
            &spatial_output,
            &KeyByLabelNames::empty(),
            &window_cfg,
            &derived_metric,
            Some(&derived_metric),
            Some(&m.arg_col),
            q.spatial_filter.as_deref().unwrap_or(""),
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

        let t_lookback_ms = (q.time_info.get_duration() * 1000.0).round() as u64;
        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_sql_cleanup_param(self.cleanup_policy, t_lookback_ms, self.t_repeat_ms)
                    .map_err(ControllerError::PlannerError)?,
            )
        };

        // Register the query text exactly as the analyst wrote the
        // aggregate (agg_expr/alias intact), just with FROM pointed at the
        // derived table - so the serve-time matcher, which re-parses this
        // same shape via the identical detection path, finds it.
        let registered = format!(
            "SELECT {group_by}, {agg_name}({arg_col}, {cmp_col}) AS {alias} {from_where} GROUP BY {group_by}{limit}",
            group_by = m.group_by_col,
            agg_name = if m.is_max { "argMax" } else { "argMin" },
            arg_col = m.arg_col,
            cmp_col = m.cmp_col,
            from_where = m.from_where,
            alias = m.alias,
            limit = limit_clause,
        );
        let registered = replace_from_table(&registered, &derived_metric);

        Ok(Some((
            configs,
            cleanup_param,
            derived_value_config,
            registered,
            derived_table,
        )))
    }

    /// `GROUP BY <computed_expr> AS <alias>` where the group key is derived
    /// directly in the outer SELECT (no nested subquery) - see
    /// `ComputedGroupByMatch`. Structurally the same "lower the computed
    /// expression to a real ingest-time column, then plan the resulting
    /// query through the ordinary classic single-aggregate path" strategy
    /// `get_token_select_streaming_aggregation_configs` above uses, just for
    /// a directly-written expression instead of a tokenized-subquery one.
    fn get_computed_group_by_streaming_aggregation_configs(
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
        let m = parse_computed_group_by_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse computed-GROUP-BY SQL query: {}",
                self.query_string
            ))
        })?;

        // The computed alias doesn't exist as a real column in any table
        // definition - it's synthesized at ingest time by the
        // ComputedLabelConfig this function also emits. Schema validation
        // needs to already know about it, or the surrogate below fails.
        // Register it on a cloned schema used only for planning this one
        // query, same reasoning as the token-select path above.
        let mut augmented_tables = self.table_definitions.clone();
        for t in &mut augmented_tables {
            if !t.metadata_columns.iter().any(|c| c == &m.alias) {
                t.metadata_columns.push(m.alias.clone());
            }
        }
        let schema = &build_sql_schema(&augmented_tables);

        let surrogate = build_computed_group_by_surrogate(&m);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse computed-GROUP-BY surrogate query: {surrogate}"
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
                "Computed-GROUP-BY surrogate must produce one query layer, got {}",
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
            r#type: m.label_type.to_string(),
            source_col: m.source_col,
            tokenizer: m.tokenizer,
            filter_regex: None,
            select: m.select,
            on_missing: Some("skip_sample".to_string()),
        };

        Ok((
            configs,
            cleanup_param,
            None,
            Some(surrogate),
            vec![(m.alias, computed_label)],
        ))
    }

    /// `SELECT <computed-expr> AS <alias>, <AGG>(<computed-expr-2>) AS
    /// <out> FROM ... GROUP BY <alias>` - a computed GROUP BY key (see
    /// `ComputedGroupByMatch`) paired with an aggregate whose own value
    /// argument is ALSO a computed expression (q013's `toDate(timestamp)
    /// AS day, avg(length(splitByChar(...)))`; q187's the same shape with
    /// `toStartOfFiveMinutes`). Neither
    /// `get_computed_group_by_streaming_aggregation_configs` (which hands
    /// the aggregate's argument straight to the classic parser - fine for
    /// a bare-column aggregate like `count(*)`, but not a nested one) nor
    /// `get_computed_value_agg_streaming_aggregation_configs` (which has
    /// no GROUP BY of its own) covers this combination alone. Composes
    /// both: the GROUP BY alias gets the ordinary metadata-column
    /// treatment first (so it's already in the schema `stage_two` builds
    /// its derived table from), then the aggregate's own computed value is
    /// routed through the same derived-value virtual-table mechanism
    /// `get_raw_value_agg_streaming_aggregation_configs` already uses
    /// standalone - its own `TableDefinition::metadata_columns` copy from
    /// the (already alias-augmented) original table carries the GROUP BY
    /// alias onto the derived table too, so the aggregate's own GROUP BY
    /// resolves against it directly.
    ///
    /// Returns `Ok(None)` when the query isn't this exact combined shape
    /// (either half missing, or the value argument turns out to already be
    /// a bare/real column) - callers should fall through to the two
    /// simpler mechanisms above, which already handle those correctly.
    pub(crate) fn get_computed_group_by_with_computed_value_streaming_aggregation_configs(
        &self,
        query_evaluation_time: f64,
    ) -> Result<
        Option<(
            Vec<IntermediateAggConfig>,
            Option<u64>,
            DerivedValueConfig,
            String,
            TableDefinition,
            Vec<(String, ComputedLabelConfig)>,
        )>,
        ControllerError,
    > {
        let Some(m) = parse_computed_group_by_query(&self.query_string) else {
            return Ok(None);
        };
        let group_by_surrogate = build_computed_group_by_surrogate(&m);
        let Some(m2) = parse_computed_value_agg_query(&group_by_surrogate) else {
            return Ok(None);
        };

        let augmented_tables: Vec<TableDefinition> = self
            .table_definitions
            .iter()
            .map(|t| {
                let mut t = t.clone();
                if !t.metadata_columns.iter().any(|c| c == &m.alias) {
                    t.metadata_columns.push(m.alias.clone());
                }
                // get_raw_value_agg_streaming_aggregation_configs (called
                // below) only treats m2's synthetic column as derivable
                // when it's already a real metadata column of the table it
                // sources from - same condition
                // get_computed_value_agg_streaming_aggregation_configs's
                // own augmentation uses standalone.
                let sources_it = t.time_column == m2.source_col
                    || t.metadata_columns.iter().any(|c| c == &m2.source_col);
                if sources_it && !t.metadata_columns.iter().any(|c| c == &m2.synthetic_label) {
                    t.metadata_columns.push(m2.synthetic_label.clone());
                }
                t
            })
            .collect();

        let stage_two = SQLSingleQueryProcessor::new(
            m2.surrogate.clone(),
            self.t_repeat_ms,
            self.data_ingestion_interval_ms,
            augmented_tables,
            self.streaming_engine,
            self.sketch_parameters.clone(),
            self.cleanup_policy,
        );

        let Some((configs, cleanup_param, derived_value_config, inner_surrogate, derived_table)) =
            stage_two.get_raw_value_agg_streaming_aggregation_configs(query_evaluation_time)?
        else {
            return Ok(None);
        };

        let group_by_label = ComputedLabelConfig {
            r#type: m.label_type.to_string(),
            source_col: m.source_col,
            tokenizer: m.tokenizer,
            filter_regex: None,
            select: m.select,
            on_missing: Some("skip_sample".to_string()),
        };
        let value_label = ComputedLabelConfig {
            r#type: m2.label_type.to_string(),
            source_col: m2.source_col,
            tokenizer: m2.tokenizer,
            filter_regex: None,
            select: m2.select,
            on_missing: Some("skip_sample".to_string()),
        };

        let value_source_column = derived_value_config.source_column.clone();

        Ok(Some((
            configs,
            cleanup_param,
            derived_value_config,
            inner_surrogate,
            derived_table,
            vec![(m.alias, group_by_label), (value_source_column, value_label)],
        )))
    }

    /// `SELECT <computed-expr> AS <alias>, <AGG>(<raw-col>) ... GROUP BY
    /// <alias>` - a computed GROUP BY key (see `ComputedGroupByMatch`)
    /// paired with an aggregate over an ORDINARY, already-existing column
    /// that just isn't registered as a `value_column` (q012's `toDate(timestamp)
    /// AS day, uniqExact(prefix)`: "prefix" is a real metadata column of
    /// `bgp`, not a computed expression).
    ///
    /// `get_computed_group_by_streaming_aggregation_configs` handles this
    /// query SHAPE (it doesn't require `parse_computed_value_agg_query` to
    /// also match) but not its SEMANTICS: it registers the aggregation
    /// directly against the ORIGINAL table with the raw column name as
    /// `value_column`, the same way it correctly handles a real value
    /// column or COUNT's synthetic `__event_count__`. But at query time,
    /// `rewrite_raw_value_agg_query` unconditionally rewrites ANY
    /// MIN/MAX/SUM/AVG/CARDINALITY/QUANTILE over a non-value column - day-
    /// grouped or not - onto a `derived_value_<col>_<table>` virtual table
    /// (see that function's doc comment). Registration time and query time
    /// modeled the same query differently: every day/week-grouped
    /// uniqExact/AVG/MIN/MAX-over-a-raw-column query registered a config on
    /// the wrong metric name and could never be found at serve time -
    /// `InvalidValueCol` fired via the "unknown metric" branch of
    /// `flatten_query_info`'s value-column check, not a real name mismatch.
    ///
    /// Mirrors `get_computed_group_by_with_computed_value_streaming_aggregation_configs`
    /// structurally (alias-augmented `stage_two` over the group-by
    /// surrogate), but skips straight to
    /// `get_raw_value_agg_streaming_aggregation_configs` instead of first
    /// parsing a computed value expression - that function already checks
    /// `schema.is_valid_value_column` itself and returns `Ok(None)` when
    /// the column doesn't need derived-table treatment (a real value
    /// column, or COUNT's synthetic column), so this safely returns
    /// `Ok(None)` too in that case and lets
    /// `get_computed_group_by_streaming_aggregation_configs` handle it as
    /// before.
    pub(crate) fn get_computed_group_by_with_raw_value_streaming_aggregation_configs(
        &self,
        query_evaluation_time: f64,
    ) -> Result<
        Option<(
            Vec<IntermediateAggConfig>,
            Option<u64>,
            DerivedValueConfig,
            String,
            TableDefinition,
            Vec<(String, ComputedLabelConfig)>,
        )>,
        ControllerError,
    > {
        let Some(m) = parse_computed_group_by_query(&self.query_string) else {
            return Ok(None);
        };
        let group_by_surrogate = build_computed_group_by_surrogate(&m);

        let augmented_tables: Vec<TableDefinition> = self
            .table_definitions
            .iter()
            .map(|t| {
                let mut t = t.clone();
                if !t.metadata_columns.iter().any(|c| c == &m.alias) {
                    t.metadata_columns.push(m.alias.clone());
                }
                t
            })
            .collect();

        let stage_two = SQLSingleQueryProcessor::new(
            group_by_surrogate,
            self.t_repeat_ms,
            self.data_ingestion_interval_ms,
            augmented_tables,
            self.streaming_engine,
            self.sketch_parameters.clone(),
            self.cleanup_policy,
        );

        let Some((configs, cleanup_param, derived_value_config, inner_surrogate, derived_table)) =
            stage_two.get_raw_value_agg_streaming_aggregation_configs(query_evaluation_time)?
        else {
            return Ok(None);
        };

        let group_by_label = ComputedLabelConfig {
            r#type: m.label_type.to_string(),
            source_col: m.source_col,
            tokenizer: m.tokenizer,
            filter_regex: None,
            select: m.select,
            on_missing: Some("skip_sample".to_string()),
        };

        Ok(Some((
            configs,
            cleanup_param,
            derived_value_config,
            inner_surrogate,
            derived_table,
            vec![(m.alias, group_by_label)],
        )))
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
        let m = parse_token_explode_query(&self.query_string)
            .or_else(|| parse_flat_token_explode_query(&self.query_string))
            .ok_or_else(|| {
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

    /// `arrayJoin(arrayZip(<slice1>, <slice2>)) AS <label>` - see
    /// `parse_arrayzip_edge_explode_query`'s doc comment for the exact
    /// shape (q067's AS-path adjacency/"edge" extraction). Structurally
    /// identical to `get_token_explode_streaming_aggregation_configs`
    /// above - same surrogate shape, same classic single-aggregate
    /// pipeline once the label is a real column - kept as its own
    /// function rather than merged into that one because the registered
    /// `ComputedLabelConfig.type` differs ("token_pair_explode", not
    /// "token_explode") and `TokenExplodeMatch` has no field to carry that
    /// choice through a shared code path without risking the two shapes
    /// already relying on that function.
    fn get_arrayzip_edge_explode_streaming_aggregation_configs(
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
        let m = parse_arrayzip_edge_explode_query(&self.query_string).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse arrayZip edge-explode SQL query: {}",
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

        let surrogate = build_token_explode_surrogate(&m);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;

        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse arrayZip edge-explode surrogate query: {surrogate}"
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
                "arrayZip edge-explode surrogate must produce one query layer, got {}",
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
            r#type: "token_pair_explode".to_string(),
            source_col: m.source_col,
            tokenizer: Some("whitespace".to_string()),
            filter_regex: None,
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
            offset_ms: bucketed.bucket_offset_ms,
        };

        let mut configs = Vec::new();

        for output in &bucketed.outputs {
            let (statistic, treatment, value_col, spatial_filter): (_, _, _, String) =
                match &output.cardinality_column {
                    Some(col) => (
                        Statistic::Cardinality,
                        get_sql_treatment_type("CARDINALITY"),
                        col.as_str(),
                        bucketed.base_spatial_filter.clone().unwrap_or_default(),
                    ),
                    None => (
                        Statistic::Count,
                        get_sql_treatment_type("COUNT"),
                        "__event_count__",
                        combine_spatial_filters(
                            bucketed.base_spatial_filter.as_deref(),
                            &output.filter,
                        ),
                    ),
                };

            let mut output_configs = build_agg_configs_for_statistics(
                &[statistic],
                treatment,
                &spatial_output,
                &rollup,
                &window_cfg,
                table_name,
                Some(table_name),
                Some(value_col),
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

            // The generic Count/Cardinality path may include a key-enumeration
            // aggregation so grouped queries can discover candidate keys.
            // Bucketed outputs are scalar values per time bucket, so no
            // key-discovery aggregation is needed.
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

    /// Bucketed top-N (e.g. `row_number() OVER (PARTITION BY hour ORDER BY
    /// cnt DESC) AS rnk ... WHERE rnk <= 5`, see `BucketedTopNMatch`):
    /// registers ONE aggregation - the key column as the grouping label,
    /// COUNT with Exact (not approximate) treatment since ranking is
    /// sensitive to noise the same way exact-equality HAVING was (see
    /// HAVING_COUNT_ALIAS) - with its window size FORCED to the bucket
    /// size, same trick get_bucketed_countif_streaming_aggregation_configs
    /// uses above. Parsing the surrogate through the ordinary
    /// SQLPatternParser/Matcher (rather than hand-building everything) gets
    /// spatial_filter/labels/t_lookback_ms for free from the surrogate's
    /// own (still full-range) WHERE clause - only window_cfg is overridden.
    fn get_bucketed_topn_streaming_aggregation_configs(
        &self,
        m: &sql_utilities::ast_matching::pattern_rewrites::BucketedTopNMatch,
        schema: &SQLSchema,
        query_evaluation_time: f64,
    ) -> Result<(Vec<IntermediateAggConfig>, Option<u64>), ControllerError> {
        let surrogate = build_bucketed_topn_surrogate(m);

        let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, &surrogate)
            .map_err(|e| ControllerError::SqlParse(e.to_string()))?;
        let parser = SQLPatternParser::new(schema, query_evaluation_time);
        let qdata = parser.parse_query(&stmts).ok_or_else(|| {
            ControllerError::SqlParse(format!(
                "Failed to parse bucketed-topn surrogate query: {surrogate}"
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
                "Bucketed-topn surrogate must produce one query layer, got {}",
                sql_query.query_data.len()
            )));
        }
        let q = &sql_query.query_data[0];
        let table_name = &q.metric;

        if m.bucket_ms < self.data_ingestion_interval_ms
            || m.bucket_ms % self.data_ingestion_interval_ms != 0
        {
            return Err(ControllerError::PlannerError(format!(
                "bucket size ({}ms) must be a multiple of data_ingestion_interval_ms ({}ms)",
                m.bucket_ms, self.data_ingestion_interval_ms
            )));
        }

        let spatial_output = KeyByLabelNames::new(vec![m.key_col.clone()]);
        let rollup = get_all_metadata_columns(&self.table_definitions, table_name)?
            .difference(&spatial_output);
        let window_cfg = IntermediateWindowConfig {
            window_size_ms: m.bucket_ms,
            slide_interval_ms: m.bucket_ms,
            window_type: WindowType::Tumbling,
            offset_ms: 0,
        };

        let configs = build_agg_configs_for_statistics(
            &[Statistic::Count],
            QueryTreatmentType::Exact,
            &spatial_output,
            &rollup,
            &window_cfg,
            table_name,
            Some(table_name),
            Some("__event_count__"),
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

        let t_lookback_ms = (q.time_info.get_duration() * 1000.0).round() as u64;
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

/// Replaces the table reference right after the query's first `FROM` with
fn get_sql_treatment_type(name: &str) -> QueryTreatmentType {
    match name.to_uppercase().as_str() {
        "MIN" | "MAX" | "ARGMAX" | "ARGMIN" => QueryTreatmentType::Exact,
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
        offset_ms: 0,
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

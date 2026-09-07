use asap_types::computed_label::ComputedLabelConfig;
use asap_types::derived_value::DerivedValueConfig;
use asap_types::enums::CleanupPolicy;
use asap_types::stateful_transition::StatefulTransitionConfig;
use indexmap::IndexMap;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::input::{SQLControllerConfig, TableDefinition};
use crate::error::ControllerError;
use crate::generator::{
    build_aggregation_entry, build_queries_yaml, GeneratorOutput, PuntedQuery, KEY_AGGREGATIONS,
    KEY_CLEANUP_POLICY, KEY_METADATA_COLUMNS, KEY_NAME, KEY_QUERIES, KEY_TABLES, KEY_TIME_COLUMN,
    KEY_VALUE_COLUMNS,
};
use crate::planner::agg_config::IntermediateAggConfig;
use crate::planner::sql::SQLSingleQueryProcessor;
use crate::StreamingEngine;
use sql_utilities::ast_matching::pattern_rewrites::{
    looks_like_lag_gap_sql, build_derived_ratio_surrogates, build_grand_total_pct_surrogate,
    build_group_by_having_count_surrogate, build_hidden_countif_having_surrogates,
    build_multi_aggregate_surrogates,
    build_running_total_surrogate, is_ingest_filter_fully_enforceable, parse_derived_ratio_query,
    parse_grand_total_pct_query, parse_group_by_having_count_query,
    build_two_stage_histogram_inner_surrogate, build_weekly_moas_histogram_inner_surrogate,
    parse_correlated_in_subquery_query,
    parse_hidden_countif_having_query, parse_multi_aggregate_query,
    parse_running_total_query, parse_two_stage_histogram_query,
    parse_weekly_moas_histogram_query,
};

pub struct SQLRuntimeOptions {
    pub streaming_engine: StreamingEngine,
    pub query_evaluation_time: Option<f64>,
    pub data_ingestion_interval_ms: u64,
}

pub fn generate_sql_plan(
    config: &SQLControllerConfig,
    opts: &SQLRuntimeOptions,
) -> Result<GeneratorOutput, ControllerError> {
    let eval_time: f64 = opts.query_evaluation_time.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    });

    let cleanup_policy = config
        .aggregate_cleanup
        .as_ref()
        .and_then(|c| c.policy)
        .unwrap_or(CleanupPolicy::ReadBased);

    // Validate T % data_ingestion_interval_ms == 0
    for qg in &config.query_groups {
        if qg.repetition_delay_ms % opts.data_ingestion_interval_ms != 0 {
            return Err(ControllerError::PlannerError(format!(
                "repetition_delay_ms {} is not a multiple of data_ingestion_interval_ms {}",
                qg.repetition_delay_ms, opts.data_ingestion_interval_ms
            )));
        }
    }

    // Validate that all tables have metadata_columns populated (either from config
    // or filled in by from_file_with_discovery before reaching here).
    for t in &config.tables {
        if t.metadata_columns.is_empty() {
            return Err(ControllerError::PlannerError(format!(
                "Table '{}' has no metadata_columns. List them in the config file \
                 or pass --clickhouse-url for auto-discovery.",
                t.name
            )));
        }
    }

    // Check for duplicate queries
    let mut seen_queries = std::collections::HashSet::new();
    for qg in &config.query_groups {
        for q in &qg.queries {
            if !seen_queries.insert(q.clone()) {
                return Err(ControllerError::DuplicateQuery(q.clone()));
            }
        }
    }

    // Dedup map: identifying_key -> IntermediateAggConfig
    let mut dedup_map: IndexMap<String, IntermediateAggConfig> = IndexMap::new();
    // query_string -> Vec<(key, cleanup_param)>
    let mut query_keys_map: IndexMap<String, Vec<(String, Option<u64>)>> = IndexMap::new();
    // Stateful transitions the planner auto-detected (e.g. lagInFrame queries),
    // deduped by derived metric_name - multiple queries referencing the same
    // derived stream only need one operator maintaining it.
    let mut stateful_transitions: IndexMap<String, StatefulTransitionConfig> = IndexMap::new();
    // Computed labels the planner auto-detected (e.g. origin-ASN extraction),
    // deduped by label name.
    let mut computed_label_cols: IndexMap<String, ComputedLabelConfig> = IndexMap::new();
    // Virtual tables for derived metric streams (currently just lag-gap's
    // "gap" value column) that don't correspond to any user-configured
    // table - deduped by table name so multiple lag-gap queries sharing a
    // derived metric only register it once.
    let mut extra_tables: IndexMap<String, TableDefinition> = IndexMap::new();
    // Derived-value ingest streams (e.g. `min(timestamp)`, `avg(med)`) the
    // planner auto-detected, deduped by derived metric_name.
    let mut derived_value_cols: IndexMap<String, DerivedValueConfig> = IndexMap::new();
    // Queries with no GROUP BY and no aggregate function - raw/DISTINCT row
    // scans no precomputed summary can ever answer. Left out of both
    // dedup_map and query_keys_map entirely, so they never appear in
    // inference_config.yaml; at query time the local engine simply won't
    // recognize them and (if forward_unsupported_queries is enabled) they
    // fall through to the ClickHouse fallback for an exact answer.
    let mut punted_queries: Vec<PuntedQuery> = Vec::new();

    for qg in &config.query_groups {
        for query_string in &qg.queries {
            // Bare-key HAVING (e.g. `SELECT prefix FROM ... GROUP BY
            // prefix HAVING count(*) = 1`): no aggregate anywhere in the
            // SELECT list, a hidden count drives the filter. Lowered to a
            // surrogate that DOES carry a real aggregate (`count(*) AS
            // __having_count__ ... HAVING __having_count__ <op> <n>`) and
            // planned through the ordinary classic single-aggregate +
            // HAVING path, registered with a template override so the
            // engine's structural matcher finds it; the engine drops the
            // hidden count column from the served output (see
            // handle_group_by_having_count_sql).
            if let Some(hm) = parse_group_by_having_count_query(query_string) {
                let surrogate = build_group_by_having_count_surrogate(&hm);
                let sub_processor = SQLSingleQueryProcessor::new(
                    surrogate.clone(),
                    qg.repetition_delay_ms,
                    opts.data_ingestion_interval_ms,
                    config.tables.clone(),
                    opts.streaming_engine,
                    config.sketch_parameters.clone(),
                    cleanup_policy,
                );
                match sub_processor.get_streaming_aggregation_configs(eval_time) {
                    Ok((configs, cleanup_param, stateful_transition, template_override, labels))
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter)) =>
                    {
                        if let Some(st) = stateful_transition {
                            stateful_transitions
                                .entry(st.metric_name.clone())
                                .or_insert(st);
                        }
                        for (label_name, cfg) in labels {
                            computed_label_cols.entry(label_name).or_insert(cfg);
                        }
                        let mut keys_for_query = Vec::new();
                        for config_item in configs {
                            let key = config_item.identifying_key();
                            keys_for_query.push((key.clone(), cleanup_param));
                            dedup_map.entry(key).or_insert(config_item);
                        }
                        let registered_query = template_override.unwrap_or(surrogate);
                        query_keys_map.insert(registered_query, keys_for_query);
                    }
                    Ok(_) => {
                        tracing::warn!(
                            query = %query_string,
                            "punting query: bare-key HAVING surrogate's WHERE clause \
                             contains a predicate the ingest-time spatial filter can't \
                             enforce; relying on the ClickHouse fallback for an exact \
                             answer instead of silently serving every row in the window"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            query = %query_string,
                            error = %e,
                            "punting query: bare-key HAVING surrogate couldn't be \
                             planned; relying on the ClickHouse fallback for an exact \
                             answer instead of aborting the whole planning run"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                }
                continue;
            }

            // Derived ratio (e.g. `countIf(op='W') / greatest(countIf(op='A'), 1)
            // AS ratio`, or `count(*) / 6.0 AS avg_per_hour`): one or two
            // aggregate expressions combined by division in the SELECT list.
            // Not a shape SQLQueryData can represent directly (it tracks one
            // aggregate), but its 1-2 underlying aggregates split into the
            // same independent single-aggregate surrogates multi-aggregate
            // uses below - only the ratio arithmetic (and, if present, the
            // `<alias>+<alias> <op> <n>` HAVING) is genuinely new, and both
            // are computed at serve time from the surrogates' joined results
            // (see handle_derived_ratio_sql in the query engine). Checked
            // before the multi-aggregate branch since a ratio's SELECT list
            // would otherwise also satisfy multi-aggregate's "2+ aggregate
            // expressions" shape and be mis-split there instead (losing the
            // division and any HAVING).
            if let Some(rm) = parse_derived_ratio_query(query_string) {
                let surrogates = build_derived_ratio_surrogates(&rm);
                let mut per_surrogate_results = Vec::new();
                let mut plan_failed = false;
                for surrogate in surrogates {
                    let sub_processor = SQLSingleQueryProcessor::new(
                        surrogate.clone(),
                        qg.repetition_delay_ms,
                        opts.data_ingestion_interval_ms,
                        config.tables.clone(),
                        opts.streaming_engine,
                        config.sketch_parameters.clone(),
                        cleanup_policy,
                    );

                    match sub_processor.get_streaming_aggregation_configs(eval_time) {
                        Ok(result)
                            if result
                                .0
                                .iter()
                                .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter)) =>
                        {
                            per_surrogate_results.push((surrogate, result));
                        }
                        Ok(_) => {
                            tracing::warn!(
                                query = %query_string,
                                surrogate = %surrogate,
                                "punting derived-ratio query: a split-out surrogate's \
                                 WHERE clause contains a predicate the ingest-time \
                                 spatial filter can't enforce; relying on the \
                                 ClickHouse fallback for an exact answer instead of \
                                 silently serving every row in the window"
                            );
                            plan_failed = true;
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(
                                query = %query_string,
                                surrogate = %surrogate,
                                error = %e,
                                "punting derived-ratio query: one of its split-out \
                                 single-aggregate surrogates couldn't be planned; \
                                 relying on the ClickHouse fallback for an exact \
                                 answer instead of a partial precompute"
                            );
                            plan_failed = true;
                            break;
                        }
                    }
                }

                if plan_failed {
                    punted_queries.push(PuntedQuery {
                        query: query_string.clone(),
                    });
                    continue;
                }

                for (
                    surrogate,
                    (configs, cleanup_param, stateful_transition, template_override, labels),
                ) in per_surrogate_results
                {
                    if let Some(st) = stateful_transition {
                        stateful_transitions
                            .entry(st.metric_name.clone())
                            .or_insert(st);
                    }
                    for (label_name, cfg) in labels {
                        computed_label_cols.entry(label_name).or_insert(cfg);
                    }

                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    let registered_query = template_override.unwrap_or(surrogate);
                    query_keys_map.insert(registered_query, keys_for_query);
                }
                continue;
            }

            // Grand-total percentage (e.g. `round(count(*) * 100.0 /
            // sum(count(*)) OVER (), 2) AS pct`): an aggregate-of-aggregate
            // window function, but a specific one - an empty `OVER ()` over
            // the SAME aggregate as the exposed column means "this row's
            // share of everyone's total", answerable from a single
            // classic-shape surrogate with no ingest-time work at all - the
            // total is just the sum of that surrogate's own per-key results,
            // computed at serve time (see handle_grand_total_pct_sql).
            // Checked before multi-aggregate since its round(...) item would
            // otherwise also look like "another aggregate expression" there
            // and get mis-split.
            if let Some(gm) = parse_grand_total_pct_query(query_string) {
                let surrogate = build_grand_total_pct_surrogate(&gm);
                let sub_processor = SQLSingleQueryProcessor::new(
                    surrogate.clone(),
                    qg.repetition_delay_ms,
                    opts.data_ingestion_interval_ms,
                    config.tables.clone(),
                    opts.streaming_engine,
                    config.sketch_parameters.clone(),
                    cleanup_policy,
                );
                match sub_processor.get_streaming_aggregation_configs(eval_time) {
                    Ok((configs, cleanup_param, stateful_transition, template_override, labels))
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter)) =>
                    {
                        if let Some(st) = stateful_transition {
                            stateful_transitions
                                .entry(st.metric_name.clone())
                                .or_insert(st);
                        }
                        for (label_name, cfg) in labels {
                            computed_label_cols.entry(label_name).or_insert(cfg);
                        }
                        let mut keys_for_query = Vec::new();
                        for config_item in configs {
                            let key = config_item.identifying_key();
                            keys_for_query.push((key.clone(), cleanup_param));
                            dedup_map.entry(key).or_insert(config_item);
                        }
                        let registered_query = template_override.unwrap_or(surrogate);
                        query_keys_map.insert(registered_query, keys_for_query);
                    }
                    Ok(_) => {
                        tracing::warn!(
                            query = %query_string,
                            "punting query: grand-total-percentage surrogate's WHERE \
                             clause contains a predicate the ingest-time spatial \
                             filter can't enforce; relying on the ClickHouse fallback \
                             for an exact answer instead of silently serving every \
                             row in the window"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            query = %query_string,
                            error = %e,
                            "punting query: grand-total-percentage surrogate couldn't \
                             be planned; relying on the ClickHouse fallback for an \
                             exact answer instead of aborting the whole planning run"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                }
                continue;
            }

            // Running total (e.g. `sum(cnt) OVER (ORDER BY cnt DESC) AS
            // running_total` over a subquery that's itself the plain
            // classic single-aggregate shape): same reasoning as
            // grand-total-pct just above - no ingest-time work needed, the
            // inner subquery IS the surrogate verbatim, and the cumulative
            // sum is computed at serve time by sorting its own results into
            // the window's ORDER BY (see handle_running_total_sql). Checked
            // alongside grand-total-pct, before multi-aggregate, for the
            // same "would otherwise be mis-split" reason.
            if let Some(rm) = parse_running_total_query(query_string) {
                let surrogate = build_running_total_surrogate(&rm);
                let sub_processor = SQLSingleQueryProcessor::new(
                    surrogate.clone(),
                    qg.repetition_delay_ms,
                    opts.data_ingestion_interval_ms,
                    config.tables.clone(),
                    opts.streaming_engine,
                    config.sketch_parameters.clone(),
                    cleanup_policy,
                );
                match sub_processor.get_streaming_aggregation_configs(eval_time) {
                    Ok((configs, cleanup_param, stateful_transition, template_override, labels))
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter)) =>
                    {
                        if let Some(st) = stateful_transition {
                            stateful_transitions
                                .entry(st.metric_name.clone())
                                .or_insert(st);
                        }
                        for (label_name, cfg) in labels {
                            computed_label_cols.entry(label_name).or_insert(cfg);
                        }
                        let mut keys_for_query = Vec::new();
                        for config_item in configs {
                            let key = config_item.identifying_key();
                            keys_for_query.push((key.clone(), cleanup_param));
                            dedup_map.entry(key).or_insert(config_item);
                        }
                        let registered_query = template_override.unwrap_or(surrogate);
                        query_keys_map.insert(registered_query, keys_for_query);
                    }
                    Ok(_) => {
                        tracing::warn!(
                            query = %query_string,
                            "punting query: running-total surrogate's WHERE clause \
                             contains a predicate the ingest-time spatial filter \
                             can't enforce; relying on the ClickHouse fallback for \
                             an exact answer instead of silently serving every row \
                             in the window"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            query = %query_string,
                            error = %e,
                            "punting query: running-total surrogate couldn't be \
                             planned; relying on the ClickHouse fallback for an \
                             exact answer instead of aborting the whole planning run"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                }
                continue;
            }

            // Multi-aggregate queries (2+ aggregate expressions over one
            // GROUP BY, e.g. `count()` and `uniqExact(...)` side by side)
            // can't be represented as a single SQLQueryData - split into N
            // independent single-aggregate surrogates and plan + register
            // each one separately, so the query engine's ordinary
            // structural matcher (find_query_config_sql) can find each
            // surrogate directly at serve time. The engine reconstructs the
            // identical split from the raw incoming query
            // (handle_multi_aggregate_sql) and merges the N results back
            // into one row per group.
            if let Some(mm) = parse_multi_aggregate_query(query_string) {
                // Plan every surrogate before registering any of them - a
                // partial precompute (some aggregates real, one missing
                // because e.g. it aggregates a computed expression like
                // `length(splitByChar(...))` rather than a plain value
                // column) can never be merged into a correct answer, so a
                // single unplannable surrogate must punt the whole original
                // query, not just drop that one piece silently.
                let surrogates = build_multi_aggregate_surrogates(&mm);

                // A computed-expression GROUP BY key (q091's
                // `length(splitByChar(' ', as_path)) AS path_len`) needs
                // its alias registered as a real metadata column BEFORE
                // any per-aggregate surrogate is planned - every surrogate
                // references it bare (`SELECT path_len, ... GROUP BY
                // path_len`), same as
                // get_computed_group_by_streaming_aggregation_configs's
                // single-aggregate treatment, just applied once here for
                // every surrogate in the split rather than the query as a
                // whole.
                let multi_agg_tables = if let Some((alias, label_type, source_col, tokenizer, select)) =
                    &mm.computed_group_by
                {
                    computed_label_cols.entry(alias.clone()).or_insert(ComputedLabelConfig {
                        r#type: label_type.to_string(),
                        source_col: source_col.clone(),
                        tokenizer: tokenizer.clone(),
                        filter_regex: None,
                        select: select.clone(),
                        on_missing: Some("skip_sample".to_string()),
                    });
                    config
                        .tables
                        .iter()
                        .map(|t| {
                            let mut t = t.clone();
                            if !t.metadata_columns.iter().any(|c| c == alias) {
                                t.metadata_columns.push(alias.clone());
                            }
                            t
                        })
                        .collect()
                } else {
                    config.tables.clone()
                };

                let mut per_surrogate_results = Vec::new();
                let mut plan_failed = false;
                for surrogate in surrogates {
                    let sub_processor = SQLSingleQueryProcessor::new(
                        surrogate.clone(),
                        qg.repetition_delay_ms,
                        opts.data_ingestion_interval_ms,
                        multi_agg_tables.clone(),
                        opts.streaming_engine,
                        config.sketch_parameters.clone(),
                        cleanup_policy,
                    );

                    // min/max/sum/avg over a column that's real but not a
                    // declared value column (e.g. `min(timestamp)`) needs
                    // its own derived-value virtual table - see
                    // get_raw_value_agg_streaming_aggregation_configs's doc
                    // comment. Tried first, before the ordinary path below,
                    // since the ordinary path would otherwise reject the
                    // column outright with InvalidValueCol.
                    if let Ok(Some((configs, cleanup_param, derived_value_config, inner_surrogate, derived_table))) =
                        sub_processor.get_raw_value_agg_streaming_aggregation_configs(eval_time)
                    {
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                        {
                            derived_value_cols
                                .entry(derived_value_config.metric_name.clone())
                                .or_insert(derived_value_config);
                            extra_tables
                                .entry(derived_table.name.clone())
                                .or_insert(derived_table);
                            per_surrogate_results.push((
                                surrogate,
                                (configs, cleanup_param, None, Some(inner_surrogate), Vec::new()),
                            ));
                            continue;
                        }
                    }

                    // Same idea, one layer further out: MIN/MAX/SUM/AVG/
                    // uniqExact over a *computed* expression (e.g.
                    // `avg(length(splitByChar(' ', as_path)))`) rather than
                    // a bare column - see
                    // get_computed_value_agg_streaming_aggregation_configs's
                    // doc comment. Tried right after the bare-column case
                    // above, for the same reason: the ordinary path below
                    // can't parse a computed expression as an aggregate
                    // argument at all.
                    if let Ok(Some((
                        configs,
                        cleanup_param,
                        derived_value_config,
                        computed_label,
                        inner_surrogate,
                        derived_table,
                    ))) = sub_processor.get_computed_value_agg_streaming_aggregation_configs(eval_time)
                    {
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                        {
                            let synthetic_label = derived_value_config.source_column.clone();
                            derived_value_cols
                                .entry(derived_value_config.metric_name.clone())
                                .or_insert(derived_value_config);
                            extra_tables
                                .entry(derived_table.name.clone())
                                .or_insert(derived_table);
                            per_surrogate_results.push((
                                surrogate,
                                (
                                    configs,
                                    cleanup_param,
                                    None,
                                    Some(inner_surrogate),
                                    vec![(synthetic_label, computed_label)],
                                ),
                            ));
                            continue;
                        }
                    }

                    // argMax(x, timestamp) / argMin(x, timestamp) - see
                    // get_arg_agg_streaming_aggregation_configs's doc
                    // comment. Its own placeholder-surrogate parse means it
                    // never collides with the two checks above (those bail
                    // immediately on a 2-argument aggregate).
                    if let Ok(Some((
                        configs,
                        cleanup_param,
                        derived_value_config,
                        registered,
                        derived_table,
                    ))) = sub_processor.get_arg_agg_streaming_aggregation_configs(eval_time)
                    {
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                        {
                            derived_value_cols
                                .entry(derived_value_config.metric_name.clone())
                                .or_insert(derived_value_config);
                            extra_tables
                                .entry(derived_table.name.clone())
                                .or_insert(derived_table);
                            per_surrogate_results.push((
                                surrogate,
                                (configs, cleanup_param, None, Some(registered), Vec::new()),
                            ));
                            continue;
                        }
                    }

                    match sub_processor.get_streaming_aggregation_configs(eval_time) {
                        Ok(result) if result.0.iter().all(|c| {
                            is_ingest_filter_fully_enforceable(&c.spatial_filter)
                        }) =>
                        {
                            per_surrogate_results.push((surrogate, result));
                        }
                        Ok(_) => {
                            tracing::warn!(
                                query = %query_string,
                                surrogate = %surrogate,
                                "punting multi-aggregate query: a split-out surrogate's \
                                 WHERE clause contains a predicate the ingest-time \
                                 spatial filter can't enforce (only column = 'literal' \
                                 / != / IN are supported); relying on the ClickHouse \
                                 fallback for an exact answer instead of silently \
                                 serving every row in the window"
                            );
                            plan_failed = true;
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(
                                query = %query_string,
                                surrogate = %surrogate,
                                error = %e,
                                "punting multi-aggregate query: one of its split-out \
                                 single-aggregate surrogates couldn't be planned; \
                                 relying on the ClickHouse fallback for an exact \
                                 answer instead of a partial precompute"
                            );
                            plan_failed = true;
                            break;
                        }
                    }
                }

                if plan_failed {
                    punted_queries.push(PuntedQuery {
                        query: query_string.clone(),
                    });
                    continue;
                }

                for (surrogate, (configs, cleanup_param, stateful_transition, template_override, labels)) in
                    per_surrogate_results
                {
                    if let Some(st) = stateful_transition {
                        stateful_transitions
                            .entry(st.metric_name.clone())
                            .or_insert(st);
                    }
                    for (label_name, cfg) in labels {
                        computed_label_cols.entry(label_name).or_insert(cfg);
                    }

                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    let registered_query = template_override.unwrap_or(surrogate);
                    query_keys_map.insert(registered_query, keys_for_query);
                }
                continue;
            }

            // Hidden-countIf HAVING (q113's "one exposed aggregate, HAVING
            // filters on countIf conditions never selected" shape) - see
            // HiddenCountifHavingMatch's doc comment. Same split-into-
            // independent-surrogates idea as multi-aggregate above, but
            // every surrogate here is always a trivial count()-shaped
            // aggregate (the exposed one, or a countIf folded into WHERE),
            // so it goes straight through the generic classic path with no
            // need for the raw/computed-value-agg or arg-agg early tries.
            if let Some(hm) = parse_hidden_countif_having_query(query_string) {
                let surrogates = build_hidden_countif_having_surrogates(&hm);
                let mut per_surrogate_results = Vec::new();
                let mut plan_failed = false;
                for surrogate in surrogates {
                    let sub_processor = SQLSingleQueryProcessor::new(
                        surrogate.clone(),
                        qg.repetition_delay_ms,
                        opts.data_ingestion_interval_ms,
                        config.tables.clone(),
                        opts.streaming_engine,
                        config.sketch_parameters.clone(),
                        cleanup_policy,
                    );
                    match sub_processor.get_streaming_aggregation_configs(eval_time) {
                        Ok(result)
                            if result
                                .0
                                .iter()
                                .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter)) =>
                        {
                            per_surrogate_results.push((surrogate, result));
                        }
                        _ => {
                            tracing::warn!(
                                query = %query_string,
                                surrogate = %surrogate,
                                "punting hidden-countIf-HAVING query: a split-out \
                                 surrogate couldn't be planned or has an unenforceable \
                                 WHERE clause; relying on the ClickHouse fallback for an \
                                 exact answer instead of a partial precompute"
                            );
                            plan_failed = true;
                            break;
                        }
                    }
                }

                if plan_failed {
                    punted_queries.push(PuntedQuery {
                        query: query_string.clone(),
                    });
                    continue;
                }

                for (
                    surrogate,
                    (configs, cleanup_param, stateful_transition, template_override, labels),
                ) in per_surrogate_results
                {
                    if let Some(st) = stateful_transition {
                        stateful_transitions
                            .entry(st.metric_name.clone())
                            .or_insert(st);
                    }
                    for (label_name, cfg) in labels {
                        computed_label_cols.entry(label_name).or_insert(cfg);
                    }
                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    let registered_query = template_override.unwrap_or(surrogate);
                    query_keys_map.insert(registered_query, keys_for_query);
                }
                continue;
            }

            // Two-stage histogram (q078's "how many prefixes had N
            // updates" shape) - see TwoStageHistogramMatch's doc comment.
            // Only the INNER query is a real precompute; it's an entirely
            // ordinary classic single-aggregate GROUP BY, registered as
            // itself with no special handling. The outer re-aggregation
            // happens purely at serve time (see
            // handle_two_stage_histogram_sql), reading the inner query's
            // own result set - nothing further to register for it here.
            if let Some(hm) = parse_two_stage_histogram_query(query_string) {
                let inner_surrogate = build_two_stage_histogram_inner_surrogate(&hm);
                let sub_processor = SQLSingleQueryProcessor::new(
                    inner_surrogate.clone(),
                    qg.repetition_delay_ms,
                    opts.data_ingestion_interval_ms,
                    config.tables.clone(),
                    opts.streaming_engine,
                    config.sketch_parameters.clone(),
                    cleanup_policy,
                );
                match sub_processor.get_streaming_aggregation_configs(eval_time) {
                    Ok((configs, cleanup_param, stateful_transition, template_override, labels))
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter)) =>
                    {
                        if let Some(st) = stateful_transition {
                            stateful_transitions
                                .entry(st.metric_name.clone())
                                .or_insert(st);
                        }
                        for (label_name, cfg) in labels {
                            computed_label_cols.entry(label_name).or_insert(cfg);
                        }
                        let mut keys_for_query = Vec::new();
                        for config_item in configs {
                            let key = config_item.identifying_key();
                            keys_for_query.push((key.clone(), cleanup_param));
                            dedup_map.entry(key).or_insert(config_item);
                        }
                        let registered_query = template_override.unwrap_or(inner_surrogate);
                        query_keys_map.insert(registered_query, keys_for_query);
                    }
                    _ => {
                        tracing::warn!(
                            query = %query_string,
                            surrogate = %inner_surrogate,
                            "punting two-stage-histogram query: its inner surrogate \
                             couldn't be planned or has an unenforceable WHERE clause; \
                             relying on the ClickHouse fallback for an exact answer"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                }
                continue;
            }

            // Weekly MOAS histogram (q133's "how many prefixes were MOAS'd
            // each week" shape) - see WeeklyMoasHistogramMatch's doc
            // comment. Only the inner query is a real precompute (MOAS
            // generalized with a weekly-bucket grouping dimension); the
            // outer histogram is computed at serve time from its result
            // set - see handle_weekly_moas_histogram_sql.
            if let Some(wm) = parse_weekly_moas_histogram_query(query_string) {
                let processor_for_weekly_moas = SQLSingleQueryProcessor::new(
                    query_string.clone(),
                    qg.repetition_delay_ms,
                    opts.data_ingestion_interval_ms,
                    config.tables.clone(),
                    opts.streaming_engine,
                    config.sketch_parameters.clone(),
                    cleanup_policy,
                );
                match processor_for_weekly_moas
                    .get_weekly_moas_histogram_streaming_aggregation_configs(eval_time)
                {
                    Ok((configs, cleanup_param, stateful_transition, template_override, labels))
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter)) =>
                    {
                        if let Some(st) = stateful_transition {
                            stateful_transitions
                                .entry(st.metric_name.clone())
                                .or_insert(st);
                        }
                        for (label_name, cfg) in labels {
                            computed_label_cols.entry(label_name).or_insert(cfg);
                        }
                        let inner_surrogate = build_weekly_moas_histogram_inner_surrogate(&wm);
                        let mut keys_for_query = Vec::new();
                        for config_item in configs {
                            let key = config_item.identifying_key();
                            keys_for_query.push((key.clone(), cleanup_param));
                            dedup_map.entry(key).or_insert(config_item);
                        }
                        let registered_query = template_override.unwrap_or(inner_surrogate);
                        query_keys_map.insert(registered_query, keys_for_query);
                    }
                    _ => {
                        tracing::warn!(
                            query = %query_string,
                            "punting weekly-MOAS-histogram query: its inner surrogate \
                             couldn't be planned or has an unenforceable WHERE clause; \
                             relying on the ClickHouse fallback for an exact answer"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                }
                continue;
            }

            // Lag-gap (e.g. `dateDiff('second', lagInFrame(timestamp) OVER
            // (PARTITION BY peer_ip ORDER BY timestamp), timestamp) AS gap`,
            // then `quantile(0.5)(gap)` outside): dispatched here rather
            // than through the shared SQLSingleQueryProcessor::
            // get_streaming_aggregation_configs path every other mechanism
            // uses, because it needs to register a whole new virtual table
            // for the derived "gap" metric (so it resolves as a real value
            // column both now and when the engine re-parses the surrogate
            // at serve time) - see get_lag_gap_streaming_aggregation_configs's
            // doc comment for why that doesn't fit the shared return type.
            if looks_like_lag_gap_sql(query_string) {
                let sub_processor = SQLSingleQueryProcessor::new(
                    query_string.clone(),
                    qg.repetition_delay_ms,
                    opts.data_ingestion_interval_ms,
                    config.tables.clone(),
                    opts.streaming_engine,
                    config.sketch_parameters.clone(),
                    cleanup_policy,
                );
                match sub_processor.get_lag_gap_streaming_aggregation_configs(eval_time) {
                    Ok((configs, cleanup_param, stateful_transition, surrogate, derived_table))
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter)) =>
                    {
                        if let Some(st) = stateful_transition {
                            stateful_transitions
                                .entry(st.metric_name.clone())
                                .or_insert(st);
                        }
                        extra_tables
                            .entry(derived_table.name.clone())
                            .or_insert(derived_table);

                        let mut keys_for_query = Vec::new();
                        for config_item in configs {
                            let key = config_item.identifying_key();
                            keys_for_query.push((key.clone(), cleanup_param));
                            dedup_map.entry(key).or_insert(config_item);
                        }
                        query_keys_map.insert(surrogate, keys_for_query);
                    }
                    Ok(_) => {
                        tracing::warn!(
                            query = %query_string,
                            "punting query: lag-gap surrogate's WHERE clause contains \
                             a predicate the ingest-time spatial filter can't enforce; \
                             relying on the ClickHouse fallback for an exact answer \
                             instead of silently serving every row in the window"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            query = %query_string,
                            error = %e,
                            "punting query: lag-gap surrogate couldn't be planned; \
                             relying on the ClickHouse fallback for an exact answer \
                             instead of aborting the whole planning run"
                        );
                        punted_queries.push(PuntedQuery {
                            query: query_string.clone(),
                        });
                    }
                }
                continue;
            }

            // Correlated IN/NOT IN subquery (q089/q129/q141/q160's "outer
            // query filtered by set membership in an independently-
            // summarizable inner query" shape) - see
            // CorrelatedInSubqueryMatch's doc comment. Neither half needs
            // a new planning mechanism: each is registered exactly as if
            // it had been submitted as its own standalone query - arg_agg
            // tried first (only q160's argMax outer needs it), falling
            // back to the generic classic path (which already covers
            // plain GROUP BY aggregates, SELECT DISTINCT, and top-k
            // internally). See handle_correlated_in_subquery_sql in the
            // query engine for the serve-time join.
            if let Some(cm) = parse_correlated_in_subquery_query(query_string) {
                let mut register_half = |text: String| -> bool {
                    let sub_processor = SQLSingleQueryProcessor::new(
                        text.clone(),
                        qg.repetition_delay_ms,
                        opts.data_ingestion_interval_ms,
                        config.tables.clone(),
                        opts.streaming_engine,
                        config.sketch_parameters.clone(),
                        cleanup_policy,
                    );

                    if let Ok(Some((
                        configs,
                        cleanup_param,
                        derived_value_config,
                        registered,
                        derived_table,
                    ))) = sub_processor.get_arg_agg_streaming_aggregation_configs(eval_time)
                    {
                        if configs
                            .iter()
                            .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                        {
                            derived_value_cols
                                .entry(derived_value_config.metric_name.clone())
                                .or_insert(derived_value_config);
                            extra_tables
                                .entry(derived_table.name.clone())
                                .or_insert(derived_table);
                            let mut keys_for_query = Vec::new();
                            for config_item in configs {
                                let key = config_item.identifying_key();
                                keys_for_query.push((key.clone(), cleanup_param));
                                dedup_map.entry(key).or_insert(config_item);
                            }
                            query_keys_map.insert(registered, keys_for_query);
                            return true;
                        }
                    }

                    match sub_processor.get_streaming_aggregation_configs(eval_time) {
                        Ok((configs, cleanup_param, stateful_transition, template_override, labels))
                            if configs.iter().all(|c| {
                                is_ingest_filter_fully_enforceable(&c.spatial_filter)
                            }) =>
                        {
                            if let Some(st) = stateful_transition {
                                stateful_transitions
                                    .entry(st.metric_name.clone())
                                    .or_insert(st);
                            }
                            for (label_name, cfg) in labels {
                                computed_label_cols.entry(label_name).or_insert(cfg);
                            }
                            let mut keys_for_query = Vec::new();
                            for config_item in configs {
                                let key = config_item.identifying_key();
                                keys_for_query.push((key.clone(), cleanup_param));
                                dedup_map.entry(key).or_insert(config_item);
                            }
                            let registered_query = template_override.unwrap_or(text);
                            query_keys_map.insert(registered_query, keys_for_query);
                            true
                        }
                        _ => false,
                    }
                };

                let outer_ok = register_half(cm.outer_query.clone());
                let inner_ok = register_half(cm.inner_query.clone());
                if !outer_ok || !inner_ok {
                    tracing::warn!(
                        query = %query_string,
                        outer = %cm.outer_query,
                        inner = %cm.inner_query,
                        outer_ok = outer_ok,
                        inner_ok = inner_ok,
                        "punting correlated-IN-subquery query: one half (outer or \
                         inner) couldn't be planned; relying on the ClickHouse \
                         fallback for an exact answer instead of a partial precompute"
                    );
                    punted_queries.push(PuntedQuery {
                        query: query_string.clone(),
                    });
                }
                continue;
            }

            let processor = SQLSingleQueryProcessor::new(
                query_string.clone(),
                qg.repetition_delay_ms,
                opts.data_ingestion_interval_ms,
                config.tables.clone(),
                opts.streaming_engine,
                config.sketch_parameters.clone(),
                cleanup_policy,
            );

            // min/max/sum/avg over a column that's real but not a declared
            // value column (e.g. `min(timestamp)`, `avg(med)`) needs its own
            // derived-value virtual table - see
            // get_raw_value_agg_streaming_aggregation_configs's doc comment.
            // Tried before is_exact_only() below, which would otherwise
            // punt a no-GROUP-BY `min(timestamp)` as "no aggregate function"
            // before this mechanism ever got a chance to look at it.
            if let Ok(Some((configs, cleanup_param, derived_value_config, inner_surrogate, derived_table))) =
                processor.get_raw_value_agg_streaming_aggregation_configs(eval_time)
            {
                if configs
                    .iter()
                    .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                {
                    derived_value_cols
                        .entry(derived_value_config.metric_name.clone())
                        .or_insert(derived_value_config);
                    extra_tables
                        .entry(derived_table.name.clone())
                        .or_insert(derived_table);
                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    query_keys_map.insert(inner_surrogate, keys_for_query);
                    continue;
                }
            }

            // Same idea, one layer further out: MIN/MAX/SUM/AVG/uniqExact
            // over a *computed* expression (e.g. `uniqExact(toDate(timestamp))`)
            // rather than a bare column - see
            // get_computed_value_agg_streaming_aggregation_configs's doc
            // comment. Tried right after the bare-column case above, before
            // is_exact_only(), for the same reason.
            if let Ok(Some((
                configs,
                cleanup_param,
                derived_value_config,
                computed_label,
                inner_surrogate,
                derived_table,
            ))) = processor.get_computed_value_agg_streaming_aggregation_configs(eval_time)
            {
                if configs
                    .iter()
                    .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                {
                    computed_label_cols
                        .entry(derived_value_config.source_column.clone())
                        .or_insert(computed_label);
                    derived_value_cols
                        .entry(derived_value_config.metric_name.clone())
                        .or_insert(derived_value_config);
                    extra_tables
                        .entry(derived_table.name.clone())
                        .or_insert(derived_table);
                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    query_keys_map.insert(inner_surrogate, keys_for_query);
                    continue;
                }
            }

            // Computed GROUP BY key + an aggregate whose OWN value is ALSO
            // computed (q013/q187's `toDate(timestamp) AS day,
            // avg(length(splitByChar(...)))`) - see
            // get_computed_group_by_with_computed_value_streaming_aggregation_configs's
            // doc comment. Tried before the plain computed-group-by/
            // multi-aggregate checks below (neither handles this
            // combination alone).
            if let Ok(Some((
                configs,
                cleanup_param,
                derived_value_config,
                inner_surrogate,
                derived_table,
                computed_labels,
            ))) = processor
                .get_computed_group_by_with_computed_value_streaming_aggregation_configs(eval_time)
            {
                if configs
                    .iter()
                    .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                {
                    for (col, label) in computed_labels {
                        computed_label_cols.entry(col).or_insert(label);
                    }
                    derived_value_cols
                        .entry(derived_value_config.metric_name.clone())
                        .or_insert(derived_value_config);
                    extra_tables
                        .entry(derived_table.name.clone())
                        .or_insert(derived_table);
                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    query_keys_map.insert(inner_surrogate, keys_for_query);
                    continue;
                }
            }

            // Computed GROUP BY key + an aggregate over an ORDINARY column
            // that isn't registered as a value column (q012's
            // `toDate(timestamp) AS day, uniqExact(prefix)` - "prefix" is a
            // real metadata column, not a computed expression, so the
            // combined handler just above doesn't match it) - see
            // get_computed_group_by_with_raw_value_streaming_aggregation_configs's
            // doc comment for why the plain computed-group-by handler below
            // can't be trusted with this shape either.
            if let Ok(Some((
                configs,
                cleanup_param,
                derived_value_config,
                inner_surrogate,
                derived_table,
                computed_labels,
            ))) = processor
                .get_computed_group_by_with_raw_value_streaming_aggregation_configs(eval_time)
            {
                if configs
                    .iter()
                    .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                {
                    for (col, label) in computed_labels {
                        computed_label_cols.entry(col).or_insert(label);
                    }
                    derived_value_cols
                        .entry(derived_value_config.metric_name.clone())
                        .or_insert(derived_value_config);
                    extra_tables
                        .entry(derived_table.name.clone())
                        .or_insert(derived_table);
                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    query_keys_map.insert(inner_surrogate, keys_for_query);
                    continue;
                }
            }

            // argMax(x, timestamp) / argMin(x, timestamp) - see
            // get_arg_agg_streaming_aggregation_configs's doc comment.
            if let Ok(Some((
                configs,
                cleanup_param,
                derived_value_config,
                registered,
                derived_table,
            ))) = processor.get_arg_agg_streaming_aggregation_configs(eval_time)
            {
                if configs
                    .iter()
                    .all(|c| is_ingest_filter_fully_enforceable(&c.spatial_filter))
                {
                    derived_value_cols
                        .entry(derived_value_config.metric_name.clone())
                        .or_insert(derived_value_config);
                    extra_tables
                        .entry(derived_table.name.clone())
                        .or_insert(derived_table);
                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    query_keys_map.insert(registered, keys_for_query);
                    continue;
                }
            }

            if processor.is_exact_only() {
                tracing::warn!(
                    query = %query_string,
                    "punting query: no aggregate function and no GROUP BY, so no \
                     precomputed summary can answer it; relying on the ClickHouse \
                     fallback (forward_unsupported_queries) for an exact answer"
                );
                punted_queries.push(PuntedQuery {
                    query: query_string.clone(),
                });
                continue;
            }

            if processor.has_unevaluable_where_subquery() {
                tracing::warn!(
                    query = %query_string,
                    "punting query: WHERE clause contains a nested SELECT the \
                     ingest-time spatial filter can't evaluate; relying on the \
                     ClickHouse fallback (forward_unsupported_queries) for an \
                     exact answer instead of building a summary against a \
                     filter that could never match correctly"
                );
                punted_queries.push(PuntedQuery {
                    query: query_string.clone(),
                });
                continue;
            }

            // Deliberately still `?`, not caught-and-punted: a failure here
            // can mean either "this SQL shape isn't recognized by any
            // pattern" (safe to punt) or "this query references a table/
            // column that doesn't exist in the config, or an invalid
            // window" (a real config bug that must surface loudly) - both
            // go through the exact same ControllerError::SqlParse /
            // PlannerError path with no way to tell them apart here, so a
            // blanket catch would silently hide genuine misconfigurations
            // (confirmed: it broke query_referencing_unknown_table_returns_error
            // and 3 sibling tests). Shapes we've specifically identified as
            // "recognized but structurally unplannable" get a proactive,
            // narrow is_exact_only() check instead (e.g. scalar_aggregate_
            // has_unusable_value_arg below) so they punt before ever
            // reaching this call.
            let (configs, cleanup_param, stateful_transition, template_override, labels) =
                processor.get_streaming_aggregation_configs(eval_time)?;

            if configs
                .iter()
                .any(|c| !is_ingest_filter_fully_enforceable(&c.spatial_filter))
            {
                tracing::warn!(
                    query = %query_string,
                    "punting query: WHERE clause contains a predicate the ingest-time \
                     spatial filter can't enforce (only column = 'literal' / != / IN \
                     are supported); relying on the ClickHouse fallback for an exact \
                     answer instead of silently serving every row in the window"
                );
                punted_queries.push(PuntedQuery {
                    query: query_string.clone(),
                });
                continue;
            }

            if let Some(st) = stateful_transition {
                stateful_transitions
                    .entry(st.metric_name.clone())
                    .or_insert(st);
            }
            for (label_name, cfg) in labels {
                computed_label_cols.entry(label_name).or_insert(cfg);
            }

            let mut keys_for_query = Vec::new();
            for config_item in configs {
                let key = config_item.identifying_key();
                keys_for_query.push((key.clone(), cleanup_param));
                dedup_map.entry(key).or_insert(config_item);
            }
            // Some query shapes (lagInFrame CTEs, MOAS's DISTINCT_SET) aren't
            // parseable by SQLPatternParser at all, so the raw query can never
            // be matched against at query time either - the surrogate that was
            // actually planned against must be what's registered here, or the
            // query-time matcher will never find this aggregation no matter
            // how correctly the runtime rewrites the incoming query.
            let registered_query = template_override.unwrap_or_else(|| query_string.clone());
            query_keys_map.insert(registered_query, keys_for_query);
        }
    }

    // Assign sequential IDs
    let mut id_map: HashMap<String, u32> = HashMap::new();
    for (idx, key) in dedup_map.keys().enumerate() {
        id_map.insert(key.clone(), idx as u32 + 1);
    }

    let extra_tables_vec: Vec<TableDefinition> = extra_tables.values().cloned().collect();
    let streaming_yaml =
        build_sql_streaming_yaml(
            config,
            &dedup_map,
            &id_map,
            &stateful_transitions,
            &computed_label_cols,
            &derived_value_cols,
            &extra_tables_vec,
        )?;
    let extra_metadata_columns: Vec<String> = computed_label_cols.keys().cloned().collect();
    let inference_yaml = build_sql_inference_yaml(
        config,
        cleanup_policy,
        &query_keys_map,
        &id_map,
        &extra_metadata_columns,
        &extra_tables_vec,
    )?;

    Ok(GeneratorOutput {
        punted_queries,
        streaming_yaml,
        inference_yaml,
        aggregation_count: dedup_map.len(),
        query_count: query_keys_map.len(),
    })
}

/// `extra_metadata_columns` is every computed-label name the planner
/// auto-detected (e.g. "origin_asn") across the whole workload. It has to
/// land in the *emitted* tables list, not just be used locally while
/// building one query's aggregation config: this is the schema the engine
/// itself rebuilds from streaming_config.yaml/inference_config.yaml to
/// parse and match incoming queries at serve time. A computed label that's
/// valid enough to plan against but never makes it into this list is
/// invisible to the engine's own schema validation, so even a perfectly
/// rewritten runtime query fails the same "not present for metric" check
/// the planner would have hit without its own local augmentation.
/// `extra_tables` is every derived-metric virtual table the planner
/// auto-detected (currently just lag-gap's per-query "gap" table) - for the
/// same "the engine rebuilds its schema from this emitted list" reason
/// `extra_metadata_columns` above needs to land here, a derived table valid
/// enough to plan against but missing from this list is invisible to the
/// engine's own schema validation at serve time.
fn build_tables_yaml(
    config: &SQLControllerConfig,
    extra_metadata_columns: &[String],
    extra_tables: &[TableDefinition],
) -> Vec<YamlValue> {
    let table_to_yaml = |t: &TableDefinition, extra_metadata: &[String]| {
        let mut map = serde_yaml::Mapping::new();
        map.insert(
            YamlValue::String(KEY_NAME.to_string()),
            YamlValue::String(t.name.clone()),
        );
        map.insert(
            YamlValue::String(KEY_TIME_COLUMN.to_string()),
            YamlValue::String(t.time_column.clone()),
        );
        map.insert(
            YamlValue::String(KEY_VALUE_COLUMNS.to_string()),
            YamlValue::Sequence(
                t.value_columns
                    .iter()
                    .map(|c| YamlValue::String(c.clone()))
                    .collect(),
            ),
        );
        let mut metadata_columns: Vec<String> = t.metadata_columns.clone();
        for extra in extra_metadata {
            if !metadata_columns.iter().any(|c| c == extra) {
                metadata_columns.push(extra.clone());
            }
        }
        map.insert(
            YamlValue::String(KEY_METADATA_COLUMNS.to_string()),
            YamlValue::Sequence(
                metadata_columns
                    .iter()
                    .map(|c| YamlValue::String(c.clone()))
                    .collect(),
            ),
        );
        YamlValue::Mapping(map)
    };

    config
        .tables
        .iter()
        .map(|t| table_to_yaml(t, extra_metadata_columns))
        // Derived-metric tables are self-contained (their own metadata
        // columns already cover what they need, e.g. the partition column)
        // - the computed-label augmentation only applies to real,
        // user-configured tables.
        .chain(extra_tables.iter().map(|t| table_to_yaml(t, &[])))
        .collect()
}

fn build_sql_streaming_yaml(
    config: &SQLControllerConfig,
    dedup_map: &IndexMap<String, IntermediateAggConfig>,
    id_map: &HashMap<String, u32>,
    stateful_transitions: &IndexMap<String, StatefulTransitionConfig>,
    computed_label_cols: &IndexMap<String, ComputedLabelConfig>,
    derived_value_cols: &IndexMap<String, DerivedValueConfig>,
    extra_tables: &[TableDefinition],
) -> Result<YamlValue, ControllerError> {
    let aggregations: Vec<YamlValue> = dedup_map
        .iter()
        .map(|(key, cfg)| build_aggregation_entry(id_map[key], cfg))
        .collect();

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String(KEY_AGGREGATIONS.to_string()),
        YamlValue::Sequence(aggregations),
    );
    let extra_metadata_columns: Vec<String> = computed_label_cols.keys().cloned().collect();
    root.insert(
        YamlValue::String(KEY_TABLES.to_string()),
        YamlValue::Sequence(build_tables_yaml(config, &extra_metadata_columns, extra_tables)),
    );

    if !stateful_transitions.is_empty() {
        let entries: Result<Vec<YamlValue>, ControllerError> = stateful_transitions
            .values()
            .map(|st| {
                serde_yaml::to_value(st)
                    .map_err(|e| ControllerError::PlannerError(e.to_string()))
            })
            .collect();
        root.insert(
            YamlValue::String("stateful_transitions".to_string()),
            YamlValue::Sequence(entries?),
        );
    }

    if !computed_label_cols.is_empty() {
        let mut labels_map = serde_yaml::Mapping::new();
        for (label_name, cfg) in computed_label_cols.iter() {
            let value = serde_yaml::to_value(cfg)
                .map_err(|e| ControllerError::PlannerError(e.to_string()))?;
            labels_map.insert(YamlValue::String(label_name.clone()), value);
        }
        root.insert(
            YamlValue::String("computed_label_cols".to_string()),
            YamlValue::Mapping(labels_map),
        );
    }

    if !derived_value_cols.is_empty() {
        let entries: Result<Vec<YamlValue>, ControllerError> = derived_value_cols
            .values()
            .map(|dv| {
                serde_yaml::to_value(dv)
                    .map_err(|e| ControllerError::PlannerError(e.to_string()))
            })
            .collect();
        root.insert(
            YamlValue::String("derived_value_cols".to_string()),
            YamlValue::Sequence(entries?),
        );
    }

    Ok(YamlValue::Mapping(root))
}

fn build_sql_inference_yaml(
    config: &SQLControllerConfig,
    cleanup_policy: CleanupPolicy,
    query_keys_map: &IndexMap<String, Vec<(String, Option<u64>)>>,
    id_map: &HashMap<String, u32>,
    extra_metadata_columns: &[String],
    extra_tables: &[TableDefinition],
) -> Result<YamlValue, ControllerError> {
    let mut cleanup_map = serde_yaml::Mapping::new();
    cleanup_map.insert(
        YamlValue::String(KEY_NAME.to_string()),
        YamlValue::String(cleanup_policy.to_string()),
    );

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String(KEY_CLEANUP_POLICY.to_string()),
        YamlValue::Mapping(cleanup_map),
    );
    root.insert(
        YamlValue::String(KEY_QUERIES.to_string()),
        YamlValue::Sequence(build_queries_yaml(cleanup_policy, query_keys_map, id_map)),
    );
    root.insert(
        YamlValue::String(KEY_TABLES.to_string()),
        YamlValue::Sequence(build_tables_yaml(config, extra_metadata_columns, extra_tables)),
    );

    Ok(YamlValue::Mapping(root))
}

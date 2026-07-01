//! Tests for SQL query pattern matching against inference_config templates.
//!
//! Verifies that incoming SQL queries with absolute timestamps are correctly matched
//! against NOW()-based template queries in the inference_config.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        QueryConfig, QueryLanguage, SchemaConfig, StreamingConfig, WindowType,
    };
    use crate::engines::simple_engine::SimpleEngine;
    use crate::stores::simple_map_store::SimpleMapStore;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;
    use sql_utilities::sqlhelper::{SQLSchema, Table};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    /// Build a minimal SQL SimpleEngine with one template query config.
    ///
    /// * `template_sql`  — the NOW()-based query stored in inference_config
    /// * `agg_id`         — aggregation id
    /// * `window_size_ms` — window size in ms
    fn build_sql_engine(template_sql: &str, agg_id: u64, window_size_ms: u64) -> SimpleEngine {
        // Schema: cpu_usage table
        let labels: HashSet<String> = ["L1", "L2", "L3", "L4"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let value_cols: HashSet<String> = ["value"].iter().map(|s| s.to_string()).collect();
        let table = Table::new(
            "cpu_usage".to_string(),
            "time".to_string(),
            value_cols,
            labels.clone(),
        );
        let sql_schema = SQLSchema::new(vec![table]);

        // Query config with the template
        let query_config = QueryConfig::new(template_sql.to_string())
            .add_aggregation(AggregationReference::new(agg_id, None));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(sql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        // Streaming config
        let agg_config = AggregationConfig {
            aggregation_id: agg_id,
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(
                ["L1", "L2", "L3", "L4"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            ),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms,
            slide_interval_ms: window_size_ms,
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
        agg_configs.insert(agg_id, agg_config);
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

    #[test]
    fn test_temporal_query_matches_now_template() {
        // Template in inference_config uses NOW()
        let template = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4";
        let engine = build_sql_engine(template, 1, 10_000);

        // Incoming query uses absolute timestamps for the same 10s window
        let incoming = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4";
        let query_time = 1727740810.0_f64; // '2025-10-01 00:00:10' as unix seconds

        let context = engine.build_query_execution_context_sql(incoming.to_string(), query_time);
        assert!(
            context.is_some(),
            "Expected build_query_execution_context_sql to return Some, got None. \
             The incoming query with absolute timestamps was not matched against the NOW() template."
        );
    }

    #[test]
    fn test_spatiotemporal_query_matches_now_template() {
        // SpatioTemporal: same metric, spans multiple intervals, GROUP BY subset of labels
        let template = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1";
        let engine = build_sql_engine(template, 1, 10_000);

        let incoming = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1";
        let query_time = 1727740810.0_f64;

        let context = engine.build_query_execution_context_sql(incoming.to_string(), query_time);
        assert!(
            context.is_some(),
            "Expected build_query_execution_context_sql to return Some for spatiotemporal query, got None."
        );
    }

    /// Characterization test for #487 Step 3: pins today's `build_spatiotemporal_context`
    /// output (labels, statistic, window) so the upcoming dispatch merge (folding
    /// `SpatioTemporal` into the shared non-nested path) can be checked against it —
    /// this assertion must still pass, unchanged, after the merge.
    #[test]
    fn spatiotemporal_query_context_is_stable_before_dispatch_merge() {
        let template = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1";
        let engine = build_sql_engine(template, 1, 10_000);

        let incoming = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1";
        let query_time = 1727740810.0_f64;

        let context = engine
            .build_query_execution_context_sql(incoming.to_string(), query_time)
            .expect("SpatioTemporal query should build a context");

        assert_eq!(context.metadata.statistic_to_compute, Statistic::Sum);
        assert_eq!(
            context.metadata.query_output_labels,
            KeyByLabelNames::new(vec!["L1".to_string()])
        );
        let start = context.store_plan.values_query.start_timestamp;
        let end = context.store_plan.values_query.end_timestamp;
        assert_eq!(
            end - start,
            10_000,
            "window should span the full 10s query duration"
        );
    }

    /// #487 Step 3: a SpatioTemporal query (duration > 1 interval, partial GROUP BY)
    /// combined with ORDER BY/LIMIT should detect top-k the same way OnlyTemporal/
    /// OnlySpatial already do. Red today — `build_spatiotemporal_context` never calls
    /// `detect_sql_topk` — must go green once the dispatch merge lands.
    #[test]
    fn spatiotemporal_query_with_order_by_limit_detects_topk() {
        let template = "SELECT L1, SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1";
        let engine = build_sql_engine(template, 1, 10_000);

        let incoming = "SELECT L1, SUM(value) AS total FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1 ORDER BY total DESC LIMIT 10";
        let query_time = 1727740810.0_f64;

        let context = engine
            .build_query_execution_context_sql(incoming.to_string(), query_time)
            .expect("SpatioTemporal query with ORDER BY/LIMIT should build a context");

        assert_eq!(
            context.metadata.statistic_to_compute,
            Statistic::Topk,
            "SpatioTemporal queries should detect top-k just like OnlyTemporal/OnlySpatial do"
        );
    }

    #[test]
    fn test_spatial_query_matches_now_template() {
        // Spatial: window equals the scrape interval (1s), GROUP BY all labels
        let template = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2, L3, L4";
        // scrape_interval=1, window=1 → classified as Spatial by the matcher
        let engine = build_sql_engine(template, 1, 1000);

        let incoming = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4";
        let query_time = 1727740810.0_f64;

        let context = engine.build_query_execution_context_sql(incoming.to_string(), query_time);
        assert!(
            context.is_some(),
            "Expected build_query_execution_context_sql to return Some for spatial query, got None."
        );
    }

    #[test]
    fn test_temporal_quantile_query_matches_now_template() {
        // TemporalQuantile: QUANTILE aggregation, window > scrape interval, GROUP BY all labels
        let template = "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4";
        let engine = build_sql_engine(template, 1, 10_000);

        let incoming = "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4";
        let query_time = 1727740810.0_f64;

        let context = engine.build_query_execution_context_sql(incoming.to_string(), query_time);
        assert!(
            context.is_some(),
            "Expected build_query_execution_context_sql to return Some for temporal quantile query, got None."
        );
    }

    #[test]
    fn test_spatial_of_temporal_subquery_matches_now_template() {
        // Spatial-of-temporal: outer GROUP BY L1 (subset), inner GROUP BY all labels
        let template = "SELECT SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1";
        let engine = build_sql_engine(template, 1, 10_000);

        let incoming = "SELECT SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4) GROUP BY L1";
        let query_time = 1727740810.0_f64;

        let context = engine.build_query_execution_context_sql(incoming.to_string(), query_time);
        assert!(
            context.is_some(),
            "Expected build_query_execution_context_sql to return Some for spatial-of-temporal subquery, got None."
        );
    }

    #[test]
    fn nested_order_by_limit_is_not_topk() {
        // Outer ORDER BY + LIMIT on a spatial-over-temporal query must not be routed
        // through CountMinSketchWithHeap; the inner temporal SUM is not a flat top-k.
        let template = "SELECT L1, SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) sub GROUP BY L1";
        let engine = build_sql_engine(template, 1, 10_000);

        let incoming = "SELECT L1, SUM(result) AS rollup FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4) sub GROUP BY L1 ORDER BY rollup DESC LIMIT 10";
        let query_time = 1727740810.0_f64;

        let context = engine
            .build_query_execution_context_sql(incoming.to_string(), query_time)
            .expect("nested spatial-of-temporal query should build a context");
        assert_ne!(
            context.metadata.statistic_to_compute,
            Statistic::Topk,
            "top-k detection skips nested OneTemporalOneSpatial; outer ORDER BY LIMIT must stay on the plain SUM path",
        );
    }

    #[test]
    fn test_no_match_returns_none() {
        // Engine has a SUM template; incoming uses AVG — should never match
        let template = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4";
        let engine = build_sql_engine(template, 1, 10_000);

        let incoming = "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4";
        let query_time = 1727740810.0_f64;

        let context = engine.build_query_execution_context_sql(incoming.to_string(), query_time);
        assert!(
            context.is_none(),
            "Expected build_query_execution_context_sql to return None for a query that doesn't match the template, got Some."
        );
    }
}

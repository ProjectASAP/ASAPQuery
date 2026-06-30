//! Query Equivalence Tests
//!
//! Tests that semantically equivalent PromQL and SQL queries produce equivalent
//! internal logic (QueryExecutionContext) in the SimpleEngine.
//!
//! These tests verify parser equivalence, pattern matching, metadata extraction,
//! timestamp calculation, and aggregation selection - WITHOUT actually executing
//! queries against a store.

use crate::data_model::{QueryLanguage, WindowType};
use crate::engines::simple_engine::SimpleEngine;
use crate::stores::{Store, TimestampedBucketsMap};
use crate::tests::test_utilities::{assert_execution_context_equivalent, TestConfigBuilder};
use std::collections::HashMap;
use std::sync::Arc;

/// Minimal no-op store that panics if queried
///
/// This ensures that tests don't accidentally query the store.
/// Context building should not require store access.
struct NoOpStore;

impl Store for NoOpStore {
    fn query_precomputed_output(
        &self,
        _metric: &str,
        _aggregation_id: u64,
        _start_timestamp: u64,
        _end_timestamp: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        panic!("NoOpStore: query_precomputed_output should not be called in equivalence tests");
    }

    fn query_precomputed_output_exact(
        &self,
        _metric: &str,
        _aggregation_id: u64,
        _exact_start: u64,
        _exact_end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        panic!(
            "NoOpStore: query_precomputed_output_exact should not be called in equivalence tests"
        );
    }

    fn insert_precomputed_output(
        &self,
        _output: crate::data_model::PrecomputedOutput,
        _precompute: Box<dyn crate::data_model::AggregateCore>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        panic!("NoOpStore: insert_precomputed_output should not be called in equivalence tests");
    }

    fn insert_precomputed_output_batch(
        &self,
        _outputs: Vec<(
            crate::data_model::PrecomputedOutput,
            Box<dyn crate::data_model::AggregateCore>,
        )>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        panic!(
            "NoOpStore: insert_precomputed_output_batch should not be called in equivalence tests"
        );
    }

    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(HashMap::new())
    }

    fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temporal_sum_equivalence() {
        let scrape_interval_ms = 1000;
        let promql_query = "sum_over_time(cpu_usage[10s])";
        let sql_query = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4";
        let grouping_labels = vec!["L1", "L2", "L3", "L4"];
        let window_size_ms = 10_000;

        // Setup test configuration
        let (promql_config, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(grouping_labels)
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_temporal_query(
                promql_query,
                sql_query,
                1,
                window_size_ms,
                WindowType::Tumbling,
            )
            .build_both();

        // Create engines (they won't query the store)
        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            promql_config,
            streaming_config.clone(),
            scrape_interval_ms,
            QueryLanguage::promql,
        );

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            sql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::sql,
        );

        // Extract internal contexts
        let query_time_sec: f64 = 1_000.0; // Arbitrary timestamp in seconds

        let promql_context = promql_engine
            .build_query_execution_context_promql(promql_query.to_string(), query_time_sec)
            .expect("Failed to build PromQL context");

        let sql_context = sql_engine
            .build_query_execution_context_sql(sql_query.to_string(), query_time_sec)
            .expect("Failed to build SQL context");

        // Assert equivalence
        assert_execution_context_equivalent(&promql_context, &sql_context, "temporal_sum");
    }

    #[test]
    fn test_spatial_sum_equivalence() {
        let scrape_interval_ms = 1000;
        let promql_query = "sum(cpu_usage) by (L1, L2)";
        let sql_query = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2";
        let grouping_labels = vec!["L1", "L2"];
        let rollup_labels = vec!["L3", "L4"];

        // Setup test configuration
        let (promql_config, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(grouping_labels)
            .with_rollup_labels(rollup_labels)
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_spatial_query(promql_query, sql_query, 2)
            .build_both();

        // Create engines
        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            promql_config,
            streaming_config.clone(),
            scrape_interval_ms,
            QueryLanguage::promql,
        );

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            sql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::sql,
        );

        // Extract contexts
        let query_time_sec: f64 = 1_000.0; // Arbitrary timestamp in seconds

        let promql_context = promql_engine
            .build_query_execution_context_promql(promql_query.to_string(), query_time_sec)
            .expect("Failed to build PromQL context");

        let sql_context = sql_engine
            .build_query_execution_context_sql(sql_query.to_string(), query_time_sec)
            .expect("Failed to build SQL context");

        // Assert equivalence
        assert_execution_context_equivalent(&promql_context, &sql_context, "spatial_avg");
    }

    /// Regression test for issue #202.
    /// With scrape_interval=15s, a 150s SQL temporal query must produce a 150_000ms window.
    /// Bug: start = end - (150 * 15 * 1000) = end - 2_250_000ms (15× too wide).
    #[test]
    fn test_bug_sql_start_timestamp_multiplied_by_scrape_interval() {
        let scrape_interval_ms = 15_000u64;
        let window_size_ms = 150_000u64;
        let sql_query = "SELECT SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -150, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4";

        let (_, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(vec!["L1", "L2", "L3", "L4"])
            .with_scrape_interval_ms(scrape_interval_ms) // USED: propagated to SimpleEngine::data_ingestion_interval_ms
            .add_temporal_query(
                "sum_over_time(cpu_usage[150s])", // NOT USED: TestConfigBuilder requires a PromQL string; timestamp calculation is SQL-only
                sql_query,                        // USED: parsed to extract the 150s duration
                1, // NOT USED: agg_id just satisfies the builder; calculate_start_timestamp_sql never consults it
                window_size_ms, // NOT USED: stored in AggregationConfig, not read by timestamp calculation
                WindowType::Tumbling, // NOT USED: same as above
            )
            .build_both();

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            sql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::sql,
        );

        // query_time_sec is ignored for fixed-date SQL (only used for NOW() resolution)
        let sql_context = sql_engine
            .build_query_execution_context_sql(sql_query.to_string(), 0.0)
            .expect("Failed to build SQL context");

        let start_ms = sql_context.store_plan.values_query.start_timestamp;
        let end_ms = sql_context.store_plan.values_query.end_timestamp;
        let actual_window_ms = end_ms - start_ms;
        let expected_window_ms = window_size_ms; // already ms — this field is ms-typed end to end now

        assert_eq!(
            actual_window_ms, expected_window_ms,
            "SQL window is {}ms but should be {}ms — \
             get_duration() returns seconds, not a count of scrape intervals",
            actual_window_ms, expected_window_ms
        );
    }

    /// Issue #401: a half-open `time >= A AND time < B` SQL query must build the
    /// exact same execution context (window timestamps + aggregation) as the
    /// equivalent `BETWEEN A AND B` query. ClickHouse runs `>=`/`<` as a true
    /// half-open `[A, B)` scan — matching ASAP's window selection — so the two
    /// query forms answer the identical question.
    #[test]
    fn test_half_open_equivalent_to_between() {
        let scrape_interval_ms = 1000;
        let promql_query = "sum_over_time(cpu_usage[10s])";
        let between_query = "SELECT SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4";
        let half_open_query = "SELECT SUM(value) FROM cpu_usage \
             WHERE time >= DATEADD(s, -10, '2025-10-01 00:00:00') AND time < '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4";

        // The inference config stores the BETWEEN form as the template; the
        // half-open incoming query resolves against it (duration-based match).
        let (_, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(vec!["L1", "L2", "L3", "L4"])
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_temporal_query(promql_query, between_query, 1, 10_000, WindowType::Tumbling)
            .build_both();

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            sql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::sql,
        );

        // Fixed-date queries ignore query_time_sec (only NOW() consults it).
        let between_context = sql_engine
            .build_query_execution_context_sql(between_query.to_string(), 0.0)
            .expect("Failed to build BETWEEN context");
        let half_open_context = sql_engine
            .build_query_execution_context_sql(half_open_query.to_string(), 0.0)
            .expect("Failed to build half-open context");

        assert_execution_context_equivalent(
            &between_context,
            &half_open_context,
            "half_open_vs_between",
        );
    }

    /// Strict rejection at the engine boundary: a `>`/`<=` combination is not a
    /// recognized half-open range, so context building returns None (the engine
    /// then forwards or reports unsupported, rather than silently using a
    /// half-open window that wouldn't match the ClickHouse baseline).
    #[test]
    fn test_gt_lte_combination_not_matched() {
        let scrape_interval_ms = 1000;
        let promql_query = "sum_over_time(cpu_usage[10s])";
        let between_query = "SELECT SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4";
        let gt_lte_query = "SELECT SUM(value) FROM cpu_usage \
             WHERE time > DATEADD(s, -10, '2025-10-01 00:00:00') AND time <= '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4";

        let (_, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(vec!["L1", "L2", "L3", "L4"])
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_temporal_query(promql_query, between_query, 1, 10_000, WindowType::Tumbling)
            .build_both();

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            sql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::sql,
        );

        assert!(
            sql_engine
                .build_query_execution_context_sql(gt_lte_query.to_string(), 0.0)
                .is_none(),
            "`>`/`<=` must not be treated as a half-open range"
        );
    }

    #[test]
    fn test_spatial_of_temporal_sum_equivalence() {
        let scrape_interval_ms = 1000;
        let promql_query = "sum(sum_over_time(cpu_usage[10s])) by (L1)";
        let sql_query = "SELECT SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1";
        // let all_labels = vec!["L1", "L2", "L3", "L4"];
        let grouping_labels = vec!["L1"];
        let rollup_labels = vec!["L2", "L3", "L4"];
        let window_size_ms = 10_000;

        // Setup test configuration
        // Using SUM of SUM which is collapsable (spatial="sum", temporal="sum_over_time")
        let (promql_config, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(grouping_labels)
            .with_rollup_labels(rollup_labels)
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_spatial_of_temporal_query(promql_query, sql_query, 3, window_size_ms)
            .build_both();

        // Create engines
        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            promql_config,
            streaming_config.clone(),
            scrape_interval_ms,
            QueryLanguage::promql,
        );

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            sql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::sql,
        );

        // Extract contexts
        let query_time_sec: f64 = 1_000.0; // Arbitrary timestamp in seconds

        let promql_context = promql_engine
            .build_query_execution_context_promql(promql_query.to_string(), query_time_sec)
            .expect("Failed to build PromQL context");

        let sql_context = sql_engine
            .build_query_execution_context_sql(sql_query.to_string(), query_time_sec)
            .expect("Failed to build SQL context");

        // Assert equivalence
        assert_execution_context_equivalent(
            &promql_context,
            &sql_context,
            "spatial_of_temporal_sum",
        );
    }

    // --- Sub-second range vector (issue #398) ---
    // This is the actual capability the issue asks for: a `[500ms]` PromQL range
    // vector must produce a 500ms execution window, not round to 0 under the old
    // whole-seconds representation. This test would have failed before the
    // ms-precision rename (the engine's `.num_seconds()` truncation bug) and
    // must pass after it.
    #[test]
    fn sub_second_range_vector_produces_sub_second_window() {
        let scrape_interval_ms = 100;
        let promql_query = "sum_over_time(cpu_usage[500ms])";
        let window_size_ms = 500;

        let (promql_config, _, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(vec!["L1"])
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_temporal_query(
                promql_query,
                promql_query,
                1,
                window_size_ms,
                WindowType::Tumbling,
            )
            .build_both();

        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            promql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::promql,
        );

        let query_time_sec: f64 = 1000.0;
        let context = promql_engine
            .build_query_execution_context_promql(promql_query.to_string(), query_time_sec)
            .expect("Failed to build PromQL context for sub-second range vector");

        let start_ms = context.store_plan.values_query.start_timestamp;
        let end_ms = context.store_plan.values_query.end_timestamp;
        assert_eq!(
            end_ms - start_ms,
            500,
            "[500ms] range vector must produce a 500ms window, got [{start_ms}, {end_ms})"
        );
    }

    // --- do_merge derivation (issue #486) ---
    //
    // do_merge is derived in `create_store_query_plan` as `range_ms > window_size_ms`,
    // not from QueryPatternType. These three tests are exhaustive over the only
    // three patterns that exist, so together they pin down the iff:
    //   do_merge == true  iff  pattern is OnlyTemporal or OneTemporalOneSpatial
    //   do_merge == false iff  pattern is OnlySpatial
    // Each uses a window_size_ms smaller than the query's range so the derived
    // check reflects a realistic config (bucket finer than the requested range),
    // matching how OnlySpatial queries are always exactly one bucket wide
    // (range_ms == data_ingestion_interval_ms == window_size_ms, so do_merge is
    // always false there) while temporal/collapsable queries normally span many.

    #[test]
    fn test_do_merge_true_for_temporal_context() {
        let scrape_interval_ms = 1000;
        let promql_query = "sum_over_time(cpu_usage[5m])";
        let window_size_ms = 1000; // << 5m range

        let (promql_config, _, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(vec!["L1"])
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_temporal_query(
                promql_query,
                promql_query,
                1,
                window_size_ms,
                WindowType::Tumbling,
            )
            .build_both();

        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            promql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::promql,
        );

        let context = promql_engine
            .build_query_execution_context_promql(promql_query.to_string(), 1_000.0)
            .expect("Failed to build PromQL context");

        assert!(
            context.do_merge,
            "OnlyTemporal queries must have do_merge=true"
        );
    }

    #[test]
    fn test_do_merge_true_for_collapsable_context() {
        let scrape_interval_ms = 1000;
        let promql_query = "sum(sum_over_time(cpu_usage[5m])) by (L1)";
        let window_size_ms = 1000; // << 5m range

        let (promql_config, _, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(vec!["L1"])
            .with_rollup_labels(vec!["L2", "L3", "L4"])
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_spatial_of_temporal_query(promql_query, promql_query, 1, window_size_ms)
            .build_both();

        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            promql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::promql,
        );

        let context = promql_engine
            .build_query_execution_context_promql(promql_query.to_string(), 1_000.0)
            .expect("Failed to build PromQL context");

        assert!(
            context.do_merge,
            "OneTemporalOneSpatial (collapsable) queries must have do_merge=true"
        );
    }

    #[test]
    fn test_do_merge_false_for_spatial_context() {
        let scrape_interval_ms = 1000;
        let promql_query = "sum(cpu_usage) by (L1)";

        // add_spatial_query sets window_size_ms = scrape_interval_ms, and a spatial
        // query's range is exactly one scrape interval, so range_ms == window_size_ms.
        let (promql_config, _, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(vec!["L1"])
            .with_scrape_interval_ms(scrape_interval_ms)
            .add_spatial_query(promql_query, promql_query, 1)
            .build_both();

        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            promql_config,
            streaming_config,
            scrape_interval_ms,
            QueryLanguage::promql,
        );

        let context = promql_engine
            .build_query_execution_context_promql(promql_query.to_string(), 1_000.0)
            .expect("Failed to build PromQL context");

        assert!(
            !context.do_merge,
            "OnlySpatial queries must have do_merge=false"
        );
    }
}

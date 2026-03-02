//! Query Equivalence Tests
//!
//! Tests that semantically equivalent PromQL and SQL queries produce equivalent
//! internal logic (QueryExecutionContext) in the SimpleEngine.
//!
//! These tests verify parser equivalence, pattern matching, metadata extraction,
//! timestamp calculation, and aggregation selection - WITHOUT actually executing
//! queries against a store.

use crate::data_model::QueryLanguage;
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
        let scrape_interval = 1;
        let promql_query = "sum_over_time(cpu_usage[10s])";
        let sql_query = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4";
        let grouping_labels = vec!["L1", "L2", "L3", "L4"];
        let window_seconds = 10;

        // Setup test configuration
        let (promql_config, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(grouping_labels)
            .with_scrape_interval(scrape_interval)
            .add_temporal_query(promql_query, sql_query, 1, window_seconds, "tumbling")
            .build_both();

        // Create engines (they won't query the store)
        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            promql_config,
            streaming_config.clone(),
            scrape_interval,
            QueryLanguage::promql,
        );

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            sql_config,
            streaming_config,
            scrape_interval,
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
        let scrape_interval = 1;
        let promql_query = "sum(cpu_usage) by (L1, L2)";
        let sql_query = "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2";
        let grouping_labels = vec!["L1", "L2"];
        let rollup_labels = vec!["L3", "L4"];

        // Setup test configuration
        let (promql_config, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(grouping_labels)
            .with_rollup_labels(rollup_labels)
            .with_scrape_interval(scrape_interval)
            .add_spatial_query(promql_query, sql_query, 2)
            .build_both();

        // Create engines
        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            promql_config,
            streaming_config.clone(),
            scrape_interval,
            QueryLanguage::promql,
        );

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            sql_config,
            streaming_config,
            scrape_interval,
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

    #[test]
    fn test_spatial_of_temporal_sum_equivalence() {
        let scrape_interval = 1;
        let promql_query = "sum(sum_over_time(cpu_usage[10s])) by (L1)";
        let sql_query = "SELECT SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1";
        // let all_labels = vec!["L1", "L2", "L3", "L4"];
        let grouping_labels = vec!["L1"];
        let rollup_labels = vec!["L2", "L3", "L4"];
        let window_seconds = 10;

        // Setup test configuration
        // Using SUM of SUM which is collapsable (spatial="sum", temporal="sum_over_time")
        let (promql_config, sql_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(grouping_labels)
            .with_rollup_labels(rollup_labels)
            .with_scrape_interval(scrape_interval)
            .add_spatial_of_temporal_query(promql_query, sql_query, 3, window_seconds)
            .build_both();

        // Create engines
        let promql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            promql_config,
            streaming_config.clone(),
            scrape_interval,
            QueryLanguage::promql,
        );

        let sql_engine = SimpleEngine::new(
            Arc::new(NoOpStore),
            // None,
            sql_config,
            streaming_config,
            scrape_interval,
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
}

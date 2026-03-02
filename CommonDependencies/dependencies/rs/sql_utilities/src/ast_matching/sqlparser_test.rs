#[cfg(test)]
mod tests {
    // use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashSet;

    use crate::sqlhelper::{SQLQueryData, SQLSchema as Schema, Table};
    use crate::sqlpattern_matcher::{QueryError, QueryType, SQLPatternMatcher};
    use crate::sqlpattern_parser::SQLPatternParser;

    pub fn create_test_schema() -> Schema {
        let mut cpu_labels = HashSet::new();
        cpu_labels.insert("L1".to_string());
        cpu_labels.insert("L2".to_string());
        cpu_labels.insert("L3".to_string());
        cpu_labels.insert("L4".to_string());

        let mut mem_labels = HashSet::new();
        mem_labels.insert("L1".to_string());
        mem_labels.insert("L2".to_string());
        mem_labels.insert("L3".to_string());
        mem_labels.insert("L4".to_string());

        let cpu_table = Table::new(
            "cpu_usage".to_string(),
            "time".to_string(),
            HashSet::from(["value".to_string()]),
            cpu_labels,
        );
        let mem_table = Table::new(
            "mem_usage".to_string(),
            "ms".to_string(),
            HashSet::from(["mb".to_string()]),
            mem_labels,
        );

        Schema::new(vec![cpu_table, mem_table])
    }

    #[test]
    fn test_basic_parsing() {
        let schema = create_test_schema();
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let dialect = GenericDialect {};
        let sql = "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1";

        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        let query_data = SQLPatternParser::new(&schema, time).parse_query(&statements);

        assert!(query_data.is_some());
        let query = query_data.unwrap();
        assert_eq!(query.metric, "cpu_usage");
        assert_eq!(query.aggregation_info.get_name(), "AVG");
        assert!(query.labels.contains("L1"));
    }

    #[test]
    fn test_pattern_matching() {
        let schema = create_test_schema();
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let matcher = SQLPatternMatcher::new(schema.clone(), 1.0);

        let dialect = GenericDialect {};
        let sql = "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2, L3, L4";

        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Some(query_data) = SQLPatternParser::new(&schema, time).parse_query(&statements) {
            let result = matcher.query_info_to_pattern(&query_data);
            assert!(result.is_valid());
            assert_eq!(result.query_type, vec![QueryType::Spatial]);
        }
    }

    #[test]
    fn test_full_suite() {
        let tables = vec![Table::new(
            String::from("cpu_usage"),
            String::from("time"),
            HashSet::from([String::from("value")]),
            HashSet::from([
                String::from("L1"),
                String::from("L2"),
                String::from("L3"),
                String::from("L4"),
            ]),
        )];
        let schema = Schema::new(tables);
        let scrape_interval = 1.0;

        let test_queries = vec![
            (
                "dated_temporal_sum",
                "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1, L2, L3, L4",
                vec![QueryType::TemporalGeneric],
                None
            ),
            (
                "dated_temporal_quantile",
                "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1, L2, L3, L4",
                vec![QueryType::TemporalQuantile],
                None
            ),
            (
                "dated_spatial_avg",
                "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1, L2, L3, L4",
                vec![QueryType::Spatial],
                None
            ),
            (
                "dated_spatial_quantile",
                "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1",
                vec![QueryType::Spatial],
                None
            ),
            (
                "dated_spatial_of_temporal_quantile_max",
                "SELECT QUANTILE(0.95, value) FROM (SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1, L2, L3, L4) GROUP BY L1",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            // // Temporal queries
            (
                "temporal_quantile",
                "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
                vec![QueryType::TemporalQuantile],
                None
            ),
            (
                "temporal_sum",
                "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
                vec![QueryType::TemporalGeneric],
                None
            ),
            (
                "temporal_max",
                "SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
                vec![QueryType::TemporalGeneric],
                None
            ),
            (
                "temporal_min",
                "SELECT MIN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
                vec![QueryType::TemporalGeneric],
                None
            ),
            (
                "temporal_avg",
                "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
                vec![QueryType::TemporalGeneric],
                None
            ),
            // // // Spatial queries
            (
                "spatial_sum",
                "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1",
                vec![QueryType::Spatial],
                None
            ),
            (
                "spatial_max",
                "SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2",
                vec![QueryType::Spatial],
                None
            ),
            (
                "spatial_min",
                "SELECT MIN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2, L3",
                vec![QueryType::Spatial],
                None
            ),
            (
                "spatial_avg",
                "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
                vec![QueryType::Spatial],
                None
            ),
            (
                "spatial_quantile",
                "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1",
                vec![QueryType::Spatial],
                None
            ),
            // // // Spatial of temporal queries
            (
                "spatial_of_temporal_sum_sum",
                "SELECT SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_sum_min",
                "SELECT SUM(result) FROM (SELECT MIN(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_sum_max",
                "SELECT SUM(result) FROM (SELECT MAX(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_sum_avg",
                "SELECT SUM(result) FROM (SELECT AVG(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3, L4",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_max_sum",
                "SELECT MAX(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_max_min",
                "SELECT MAX(result) FROM (SELECT MIN(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_max_max",
                "SELECT MAX(result) FROM (SELECT MAX(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_max_avg",
                "SELECT MAX(result) FROM (SELECT AVG(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3, L4",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_quantile_max",
                "SELECT QUANTILE(0.95, value) FROM (SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_quantile_min",
                "SELECT QUANTILE(0.95, value) FROM (SELECT MIN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_quantile_sum",
                "SELECT QUANTILE(0.95, value) FROM (SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_quantile_avg",
                "SELECT QUANTILE(0.95, value) FROM (SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
            (
                "spatial_of_temporal_avg_quantile",
                "SELECT AVG(result) FROM (SELECT QUANTILE(0.95, value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2",
                vec![QueryType::Spatial, QueryType::TemporalQuantile],
                None
            ),
            (
                "spatial_of_temporal_quantile_quantile",
                "SELECT QUANTILE(0.95, value) FROM (SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3",
                vec![QueryType::Spatial, QueryType::TemporalQuantile],
                None
            ),
            // // // Error cases
            (
                "temporal_invalid_aggregation_label",
                "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, FAKE_LABEL",
                vec![],
                Some(QueryError::InvalidAggregationLabel)
            ),
            (
                "temporal_invalid_time_column",
                "SELECT SUM(value) FROM cpu_usage WHERE datetime BETWEEN NOW() AND DATEADD(s, -10, NOW()) GROUP BY L1, L2, L3, L4",
                vec![],
                Some(QueryError::InvalidTimeCol)
            ),
            (
                "temporal_invalid_value_column",
                "SELECT SUM(not_a_value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
                vec![],
                Some(QueryError::InvalidValueCol)
            ),
            // SpatioTemporal queries - span multiple scrape intervals but group by subset of labels
            (
                "spatiotemporal_sum",
                "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1",
                vec![QueryType::SpatioTemporal],
                None
            ),
            (
                "spatiotemporal_max",
                "SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2",
                vec![QueryType::SpatioTemporal],
                None
            ),
            (
                "spatiotemporal_min",
                "SELECT MIN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3",
                vec![QueryType::SpatioTemporal],
                None
            ),
            (
                "spatiotemporal_avg",
                "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1",
                vec![QueryType::SpatioTemporal],
                None
            ),
            (
                "spatiotemporal_quantile",
                "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2",
                vec![QueryType::SpatioTemporal],
                None
            ),
            (
                "temporal_illegal_aggregation_function",
                "SELECT HARMONIC_MEAN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3",
                vec![],
                Some(QueryError::IllegalAggregationFn)
            ),
            (
                "spatial_scrape_duration_too_small",
                "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN NOW() AND DATEADD(s, 0, NOW()) GROUP BY L1, L2",
                vec![],
                Some(QueryError::SpatialDurationSmall)
            ),
            (
                "temporal_percentile",
                "SELECT PERCENTILE(value, 95) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
                vec![QueryType::TemporalQuantile],
                None
            ),
            (
                "spatial_percentile",
                "SELECT PERCENTILE(value, 95) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1",
                vec![QueryType::Spatial],
                None
            ),
            (
                "spatiotemporal_percentile",
                "SELECT PERCENTILE(value, 95) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2",
                vec![QueryType::SpatioTemporal],
                None
            ),
            (
                "spatial_of_temporal_percentile_max",
                "SELECT PERCENTILE(value, 95) FROM (SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
                vec![QueryType::Spatial, QueryType::TemporalGeneric],
                None
            ),
        ];

        let mut successes = 0;
        let mut failures = 0;

        for (name, sql, expected_types, error) in test_queries {
            println!("Testing: {}", name);

            if let Some(query_data) = parse_sql_query(sql) {
                let matcher = SQLPatternMatcher::new(schema.clone(), scrape_interval);
                let result = matcher.query_info_to_pattern(&query_data);

                assert_eq!(result.query_type, expected_types);
                assert_eq!(result.error, error);

                if result.query_type == expected_types && result.error == error {
                    println!("✓ Passed");
                    successes += 1;
                } else {
                    println!("✗ Failed");
                    println!("expected type, error: {:?}, {:?}", expected_types, error);
                    println!(
                        "got type, error: {:?}, {:?}",
                        result.query_type, result.error
                    );
                    failures += 1;
                }
            } else {
                println!("✗ Failed to parse");
                failures += 1;
            }
        }

        println!("\nRESULTS\n=======");
        println!("Passed: {}", successes);
        println!("Failed: {}", failures);
    }

    pub fn parse_sql_query(sql: &str) -> Option<SQLQueryData> {
        let schema = create_test_schema();
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let dialect = sqlparser::dialect::ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql).ok()?;
        print!("Query: {sql}, AST: {statements:#?}\n");

        SQLPatternParser::new(&schema, time).parse_query(&statements)
    }
}

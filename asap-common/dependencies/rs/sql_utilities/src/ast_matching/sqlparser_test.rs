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

    pub fn parse_sql_query(sql: &str) -> Option<SQLQueryData> {
        let schema = create_test_schema();
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let dialect = sqlparser::dialect::ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql).ok()?;
        SQLPatternParser::new(&schema, time).parse_query(&statements)
    }

    /// Parse the query and run it through the matcher, asserting the expected outcome.
    fn check_query(sql: &str, expected_types: Vec<QueryType>, expected_error: Option<QueryError>) {
        let schema = create_test_schema();
        let matcher = SQLPatternMatcher::new(schema, 1.0);
        let query_data =
            parse_sql_query(sql).unwrap_or_else(|| panic!("Failed to parse query: {}", sql));
        let result = matcher.query_info_to_pattern(&query_data);
        assert_eq!(result.query_type, expected_types);
        assert_eq!(result.error, expected_error);
    }

    /// Multi-column SELECT: GROUP BY keys repeated as bare identifiers + one aggregate (ClickHouse style).
    #[test]
    fn test_multi_column_select_quantile_matches_single_aggregate_semantics() {
        let sql = "\
            SELECT L1, L2, L3, L4, quantile(0.99)(value) AS p99 \
            FROM cpu_usage \
            WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
            GROUP BY L1, L2, L3, L4";
        let q = parse_sql_query(sql).expect("multi-column quantile should parse");
        assert_eq!(q.metric, "cpu_usage");
        assert_eq!(q.aggregation_info.get_name(), "QUANTILE");
        assert_eq!(
            q.labels,
            HashSet::from_iter(
                ["L1", "L2", "L3", "L4"]
                    .map(|s| s.to_string())
            )
        );
    }

    #[test]
    fn test_order_by_with_grouped_count_is_accepted() {
        check_query(
            "SELECT L1, COUNT(value) AS c FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1 ORDER BY c DESC",
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    #[test]
    fn test_count_distinct_marks_distinct_arg() {
        let q = parse_sql_query(
            "SELECT L1, COUNT(DISTINCT value) AS distinct_values FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1",
        )
        .expect("count distinct should parse");
        assert_eq!(q.aggregation_info.get_name(), "COUNT");
        assert!(
            q.aggregation_info
                .get_args()
                .iter()
                .any(|arg| arg.eq_ignore_ascii_case("distinct")),
            "COUNT(DISTINCT ...) should carry a distinct marker in AggregationInfo args",
        );
    }

    // ── Basic smoke tests ────────────────────────────────────────────────────

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

    // ── Dated queries (fixed timestamp instead of NOW()) ─────────────────────

    #[test]
    fn test_dated_temporal_sum() {
        check_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_dated_temporal_quantile() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalQuantile],
            None,
        );
    }

    #[test]
    fn test_dated_spatial_avg() {
        check_query(
            "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1, L2, L3, L4",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_dated_spatial_quantile() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_dated_spatial_of_temporal_quantile_max() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM (SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    // ── Temporal queries ─────────────────────────────────────────────────────

    #[test]
    fn test_temporal_quantile() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalQuantile],
            None,
        );
    }

    #[test]
    fn test_temporal_sum() {
        check_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_temporal_max() {
        check_query(
            "SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_temporal_min() {
        check_query(
            "SELECT MIN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_temporal_avg() {
        check_query(
            "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalGeneric],
            None,
        );
    }

    // ── Spatial queries ──────────────────────────────────────────────────────

    #[test]
    fn test_spatial_sum() {
        check_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_spatial_max() {
        check_query(
            "SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_spatial_min() {
        check_query(
            "SELECT MIN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2, L3",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_spatial_avg() {
        check_query(
            "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_spatial_quantile() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1",
            vec![QueryType::Spatial],
            None,
        );
    }

    // ── Spatial of temporal queries ──────────────────────────────────────────

    #[test]
    fn test_spatial_of_temporal_sum_sum() {
        check_query(
            "SELECT SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_sum_min() {
        check_query(
            "SELECT SUM(result) FROM (SELECT MIN(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_sum_max() {
        check_query(
            "SELECT SUM(result) FROM (SELECT MAX(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_sum_avg() {
        check_query(
            "SELECT SUM(result) FROM (SELECT AVG(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3, L4",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_max_sum() {
        check_query(
            "SELECT MAX(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_max_min() {
        check_query(
            "SELECT MAX(result) FROM (SELECT MIN(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_max_max() {
        check_query(
            "SELECT MAX(result) FROM (SELECT MAX(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_max_avg() {
        check_query(
            "SELECT MAX(result) FROM (SELECT AVG(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3, L4",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_quantile_max() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM (SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_quantile_min() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM (SELECT MIN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_quantile_sum() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM (SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_quantile_avg() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM (SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_avg_quantile() {
        check_query(
            "SELECT AVG(result) FROM (SELECT QUANTILE(0.95, value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2",
            vec![QueryType::Spatial, QueryType::TemporalQuantile],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_quantile_quantile() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM (SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1, L2, L3",
            vec![QueryType::Spatial, QueryType::TemporalQuantile],
            None,
        );
    }

    // ── SpatioTemporal queries ───────────────────────────────────────────────

    #[test]
    fn test_spatiotemporal_sum() {
        check_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1",
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    #[test]
    fn test_spatiotemporal_max() {
        check_query(
            "SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2",
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    #[test]
    fn test_spatiotemporal_min() {
        check_query(
            "SELECT MIN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3",
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    #[test]
    fn test_spatiotemporal_avg() {
        check_query(
            "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1",
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    #[test]
    fn test_spatiotemporal_quantile() {
        check_query(
            "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2",
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    // ── PERCENTILE syntax (Elasticsearch SQL compatible) ─────────────────────

    #[test]
    fn test_temporal_percentile() {
        check_query(
            "SELECT PERCENTILE(value, 95) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalQuantile],
            None,
        );
    }

    #[test]
    fn test_spatial_percentile() {
        check_query(
            "SELECT PERCENTILE(value, 95) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_spatiotemporal_percentile() {
        check_query(
            "SELECT PERCENTILE(value, 95) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2",
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    #[test]
    fn test_spatial_of_temporal_percentile_max() {
        check_query(
            "SELECT PERCENTILE(value, 95) FROM (SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    // ── ClickHouse parametric syntax: quantile(0.95)(column) ─────────────────
    // These currently fail — they drive the fix in sqlpattern_parser.rs.

    #[test]
    fn test_clickhouse_temporal_quantile() {
        check_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalQuantile],
            None,
        );
    }

    #[test]
    fn test_clickhouse_spatial_quantile() {
        check_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY L1",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_clickhouse_spatiotemporal_quantile() {
        check_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2",
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    #[test]
    fn test_clickhouse_spatial_of_temporal_quantile_max() {
        check_query(
            "SELECT quantile(0.95)(value) FROM (SELECT MAX(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1",
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    // ── Error cases ──────────────────────────────────────────────────────────

    #[test]
    fn test_error_invalid_aggregation_label() {
        check_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, FAKE_LABEL",
            vec![],
            Some(QueryError::InvalidAggregationLabel),
        );
    }

    #[test]
    fn test_error_invalid_time_column() {
        // Bug: the parser currently returns None for an invalid time column instead of
        // letting the matcher return InvalidTimeCol. check_query will panic until fixed.
        check_query(
            "SELECT SUM(value) FROM cpu_usage WHERE datetime BETWEEN NOW() AND DATEADD(s, -10, NOW()) GROUP BY L1, L2, L3, L4",
            vec![],
            Some(QueryError::InvalidTimeCol),
        );
    }

    #[test]
    fn test_error_invalid_value_column() {
        check_query(
            "SELECT SUM(not_a_value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4",
            vec![],
            Some(QueryError::InvalidValueCol),
        );
    }

    #[test]
    fn test_error_illegal_aggregation_function() {
        check_query(
            "SELECT HARMONIC_MEAN(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3",
            vec![],
            Some(QueryError::IllegalAggregationFn),
        );
    }

    #[test]
    fn test_error_spatial_scrape_duration_too_small() {
        check_query(
            "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN NOW() AND DATEADD(s, 0, NOW()) GROUP BY L1, L2",
            vec![],
            Some(QueryError::SpatialDurationSmall),
        );
    }

    // ── matches_sql_pattern tests ─────────────────────────────────────────────

    #[test]
    fn test_matches_now_vs_absolute_timestamp() {
        // Same 10s window, same metric/agg/labels — should match
        let template = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        let incoming = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4"
        ).unwrap();
        assert!(incoming.matches_sql_pattern(&template));
    }

    #[test]
    fn test_no_match_different_duration() {
        let template = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        let incoming = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -5, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        assert!(!incoming.matches_sql_pattern(&template));
    }

    #[test]
    fn test_no_match_different_metric() {
        let template = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        let incoming = parse_sql_query(
            "SELECT SUM(mb) FROM mem_usage WHERE ms BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        assert!(!incoming.matches_sql_pattern(&template));
    }

    #[test]
    fn test_no_match_different_aggregation() {
        let template = parse_sql_query(
            "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        let incoming = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        assert!(!incoming.matches_sql_pattern(&template));
    }

    #[test]
    fn test_no_match_different_labels() {
        let template = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        let incoming = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1"
        ).unwrap();
        assert!(!incoming.matches_sql_pattern(&template));
    }

    #[test]
    fn test_no_match_different_time_column() {
        // cpu_usage uses "time", mem_usage uses "ms" — query same metric but wrong time col
        let template = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        // Force a different time column by using mem_usage schema (col: ms) but same duration
        let incoming = parse_sql_query(
            "SELECT SUM(mb) FROM mem_usage WHERE ms BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        // Different metric AND time column — must not match
        assert!(!incoming.matches_sql_pattern(&template));
    }

    #[test]
    fn test_no_match_different_quantile_args() {
        let template = parse_sql_query(
            "SELECT QUANTILE(0.95, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        let incoming = parse_sql_query(
            "SELECT QUANTILE(0.99, value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        assert!(!incoming.matches_sql_pattern(&template));
    }

    #[test]
    fn test_matches_subquery_now_vs_absolute() {
        // Spatial-of-temporal: outer has no time clause (UNUSED), inner has time clause
        let template = parse_sql_query(
            "SELECT SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4) GROUP BY L1"
        ).unwrap();
        let incoming = parse_sql_query(
            "SELECT SUM(result) FROM (SELECT SUM(value) AS result FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4) GROUP BY L1"
        ).unwrap();
        assert!(incoming.matches_sql_pattern(&template));
    }
}

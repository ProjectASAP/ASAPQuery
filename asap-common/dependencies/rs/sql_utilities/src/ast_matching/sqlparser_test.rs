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

    // ── ClickHouse parametric syntax + explicit BETWEEN timestamps ────────────
    // These verify that a fully ClickHouse-compatible query (no DATEADD, no NOW())
    // is parseable by ASAP: quantile(q)(col) + BETWEEN 'start' AND 'end'.

    #[test]
    fn test_clickhouse_explicit_datetime_temporal_quantile() {
        check_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN '2025-10-01 00:00:00' AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalQuantile],
            None,
        );
    }

    #[test]
    // ASAP-only: parse_datetime accepts the Z suffix (interprets as UTC), but ClickHouse
    // rejects it with TYPE_MISMATCH when comparing against a DateTime column.
    // Do not use Z-suffix strings in queries intended for both systems.
    fn test_asap_only_iso_z_temporal_quantile() {
        check_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN '2025-10-01T00:00:00Z' AND '2025-10-01T00:00:10Z' GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalQuantile],
            None,
        );
    }

    #[test]
    // Both ASAP (parse_datetime) and ClickHouse treat ISO-without-Z as local server time.
    // They agree only when running in the same timezone; prefer 'YYYY-MM-DD HH:MM:SS'
    // (space format) to avoid this implicit dependency.
    fn test_iso_no_z_treated_as_local_time_temporal_quantile() {
        check_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN '2025-10-01T00:00:00' AND '2025-10-01T00:00:10' GROUP BY L1, L2, L3, L4",
            vec![QueryType::TemporalQuantile],
            None,
        );
    }

    #[test]
    fn test_clickhouse_explicit_datetime_spatial_quantile() {
        check_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN '2025-10-01 00:00:00' AND '2025-10-01 00:00:01' GROUP BY L1",
            vec![QueryType::Spatial],
            None,
        );
    }

    #[test]
    fn test_clickhouse_explicit_matches_now_template() {
        // A ClickHouse-style query (explicit timestamps, parametric quantile) must
        // match a stored DATEADD(NOW()) template of the same shape.
        let template = parse_sql_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2, L3, L4"
        ).unwrap();
        let incoming = parse_sql_query(
            "SELECT quantile(0.95)(value) FROM cpu_usage WHERE time BETWEEN '2025-10-01 00:00:00' AND '2025-10-01 00:00:10' GROUP BY L1, L2, L3, L4"
        ).unwrap();
        assert!(incoming.matches_sql_pattern(&template));
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

    #[test]
    fn test_order_by_is_rejected() {
        assert!(parse_sql_query(
            "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2 ORDER BY L1"
        ).is_none());
    }

    // ── scrape_interval > 1s regression tests (issue #201) ───────────────────

    fn check_query_with_interval(
        sql: &str,
        scrape_interval: f64,
        expected_types: Vec<QueryType>,
        expected_error: Option<QueryError>,
    ) {
        let schema = create_test_schema();
        let matcher = SQLPatternMatcher::new(schema, scrape_interval);
        let query_data =
            parse_sql_query(sql).unwrap_or_else(|| panic!("Failed to parse query: {}", sql));
        let result = matcher.query_info_to_pattern(&query_data);
        assert_eq!(result.query_type, expected_types);
        assert_eq!(result.error, expected_error);
    }

    /// scraped_intervals = 15/15 = 1.0; bug fires 1.0 < 15.0 → false positive error
    #[test]
    fn test_bug_201_single_interval_spatial_query_not_rejected() {
        check_query_with_interval(
            "SELECT AVG(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -15, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4",
            15.0,
            vec![QueryType::Spatial],
            None,
        );
    }

    /// scraped_intervals = 30/15 = 2.0; bug fires 2.0 < 15.0 → false positive error
    #[test]
    fn test_bug_201_two_interval_temporal_query_not_rejected() {
        check_query_with_interval(
            "SELECT SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -30, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4",
            15.0,
            vec![QueryType::TemporalGeneric],
            None,
        );
    }

    /// scraped_intervals = 30/15 = 2.0 with QUANTILE agg → TemporalQuantile
    #[test]
    fn test_bug_201_temporal_quantile_not_rejected() {
        check_query_with_interval(
            "SELECT QUANTILE(0.95, value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -30, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4",
            15.0,
            vec![QueryType::TemporalQuantile],
            None,
        );
    }

    /// scraped_intervals = 30/15 = 2.0 with subset of labels → SpatioTemporal
    #[test]
    fn test_bug_201_spatiotemporal_not_rejected() {
        check_query_with_interval(
            "SELECT SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -30, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
             GROUP BY L1",
            15.0,
            vec![QueryType::SpatioTemporal],
            None,
        );
    }

    /// Spatial-of-temporal: outer has UNUSED time (not checked), inner scraped_intervals = 30/15 = 2.0
    #[test]
    fn test_bug_201_spatial_of_temporal_not_rejected() {
        check_query_with_interval(
            "SELECT SUM(result) FROM \
             (SELECT SUM(value) AS result FROM cpu_usage \
              WHERE time BETWEEN DATEADD(s, -30, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
              GROUP BY L1, L2, L3, L4) \
             GROUP BY L1",
            15.0,
            vec![QueryType::Spatial, QueryType::TemporalGeneric],
            None,
        );
    }

    /// scraped_intervals = 14/15 = 0.93 < 1.0 → should still be rejected (guard still works)
    #[test]
    fn test_bug_201_sub_interval_query_still_rejected() {
        check_query_with_interval(
            "SELECT AVG(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -14, '2025-10-01 00:00:00') AND '2025-10-01 00:00:00' \
             GROUP BY L1, L2, L3, L4",
            15.0,
            vec![],
            Some(QueryError::SpatialDurationSmall),
        );
    }
}

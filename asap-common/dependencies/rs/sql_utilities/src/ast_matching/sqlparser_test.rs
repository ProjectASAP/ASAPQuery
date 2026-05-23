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
    fn test_order_by_groupby_column_default_ascending() {
        // Bare `ORDER BY L1` (no ASC/DESC) defaults to ascending. The order_by item
        // must reflect that the column is a GROUP BY key.
        let q = parse_sql_query(
            "SELECT AVG(value) FROM cpu_usage WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() GROUP BY L1, L2 ORDER BY L1",
        )
        .expect("ORDER BY group-by column should parse");
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.order_by[0].column, "L1");
        assert!(q.order_by[0].ascending);
        assert_eq!(q.limit, None);
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

    // ── Multi-projection SELECT (group cols + aggregate) ─────────────────────
    //
    // ClickHouse and standard SQL allow `SELECT g1, g2, agg(v) FROM t GROUP BY g1, g2`
    // (one row per group with the grouping keys included alongside the aggregate).
    // The pattern parser must also accept this shape and produce the same structural
    // SQLQueryData as the single-projection form `SELECT agg(v) FROM t GROUP BY g1, g2`.

    #[test]
    fn test_multi_projection_groupcols_then_aggregate() {
        let query = parse_sql_query(
            "SELECT L1, L2, L3, L4, SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1, L2, L3, L4",
        )
        .expect("multi-projection SELECT with group cols + aggregate should parse");
        assert_eq!(query.metric, "cpu_usage");
        assert_eq!(query.aggregation_info.get_name(), "SUM");
        assert!(query.labels.contains("L1"));
        assert!(query.labels.contains("L4"));
    }

    #[test]
    fn test_multi_projection_aggregate_first() {
        let query = parse_sql_query(
            "SELECT SUM(value), L1, L2, L3, L4 FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1, L2, L3, L4",
        )
        .expect("aggregate-first multi-projection SELECT should parse");
        assert_eq!(query.aggregation_info.get_name(), "SUM");
    }

    #[test]
    fn test_multi_projection_quantile_clickhouse_syntax() {
        // The exact shape of the user's netflow query: ClickHouse parametric quantile
        // with grouping columns alongside the aggregate in SELECT.
        let query = parse_sql_query(
            "SELECT L1, L2, quantile(0.99)(value) AS p99 FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -11, NOW()) AND DATEADD(s, -10, NOW()) \
             GROUP BY L1, L2",
        )
        .expect("multi-projection ClickHouse parametric quantile should parse");
        assert_eq!(query.aggregation_info.get_name(), "QUANTILE");
        assert_eq!(query.aggregation_info.get_args()[0], "0.99");
    }

    #[test]
    fn test_multi_projection_matches_single_projection_template() {
        // A template registered as single-projection should structurally match an
        // incoming query that lists the group cols in SELECT alongside the aggregate.
        let template = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1, L2, L3, L4",
        )
        .expect("single-projection template should parse");
        let incoming = parse_sql_query(
            "SELECT L1, L2, L3, L4, SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' \
             GROUP BY L1, L2, L3, L4",
        )
        .expect("multi-projection incoming should parse");
        assert!(incoming.matches_sql_pattern(&template));
    }

    #[test]
    fn test_multi_projection_rejects_two_aggregates() {
        // Two aggregate functions in the projection list — the parser only tracks one
        // statistic so this must be rejected to avoid silently dropping one.
        assert!(parse_sql_query(
            "SELECT SUM(value), AVG(value), L1 FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1",
        )
        .is_none());
    }

    #[test]
    fn test_multi_projection_rejects_arbitrary_expr() {
        // Non-identifier, non-function projection items (computed expressions, literals, …)
        // are not supported by the pattern model and must be rejected.
        assert!(parse_sql_query(
            "SELECT (L1 + 1), SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1",
        )
        .is_none());
    }

    #[test]
    fn test_multi_projection_rejects_select_col_not_in_groupby() {
        // L2 is in SELECT but not in GROUP BY. Standard SQL rejects this; we must too,
        // otherwise the column would be silently dropped from the output.
        assert!(parse_sql_query(
            "SELECT L2, SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1",
        )
        .is_none());
    }

    #[test]
    fn test_multi_projection_accepts_select_subset_of_groupby() {
        // SELECT lists a subset of group-by keys (L1) while the GROUP BY uses two
        // (L1, L2). Allowed: every SELECT identifier is in GROUP BY; the remaining
        // group-by key is just absent from the projection.
        let query = parse_sql_query(
            "SELECT L1, SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1, L2",
        )
        .expect("SELECT subset of GROUP BY should parse");
        assert!(query.labels.contains("L1"));
        assert!(query.labels.contains("L2"));
        assert_eq!(query.aggregation_info.get_name(), "SUM");
    }

    // ── ORDER BY / LIMIT support ─────────────────────────────────────────────
    //
    // The parser must accept ORDER BY (possibly multi-column, with optional ASC/DESC)
    // and LIMIT N, capturing them in SQLQueryData for the engine to apply post-aggregation.
    // ORDER BY columns must reference either the aggregate alias or a GROUP BY column.
    // The aggregate alias is captured separately so `ORDER BY <alias>` can resolve.

    #[test]
    fn test_order_by_aggregate_alias_desc_limit_n() {
        // Top-N user case: ORDER BY <agg alias> DESC LIMIT 10.
        let q = parse_sql_query(
            "SELECT L1, L2, QUANTILE(0.99, value) AS p99 FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -11, NOW()) AND DATEADD(s, -10, NOW()) \
             GROUP BY L1, L2 \
             ORDER BY p99 DESC LIMIT 10",
        )
        .expect("ORDER BY <agg alias> DESC LIMIT N should parse");
        assert_eq!(q.aggregation_alias.as_deref(), Some("p99"));
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.order_by[0].column, "p99");
        assert!(!q.order_by[0].ascending);
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn test_order_by_multiple_columns_mixed_directions() {
        let q = parse_sql_query(
            "SELECT QUANTILE(0.99, value) AS p99 FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() \
             GROUP BY L1, L2 \
             ORDER BY L1 ASC, p99 DESC",
        )
        .expect("multi-column ORDER BY with mixed directions should parse");
        assert_eq!(q.order_by.len(), 2);
        assert_eq!(q.order_by[0].column, "L1");
        assert!(q.order_by[0].ascending);
        assert_eq!(q.order_by[1].column, "p99");
        assert!(!q.order_by[1].ascending);
    }

    #[test]
    fn test_limit_only_no_orderby() {
        let q = parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1 \
             LIMIT 5",
        )
        .expect("LIMIT without ORDER BY should parse");
        assert!(q.order_by.is_empty());
        assert_eq!(q.limit, Some(5));
    }

    #[test]
    fn test_order_by_unknown_column_rejected() {
        // mystery_col is neither the aggregate alias nor a GROUP BY column.
        assert!(parse_sql_query(
            "SELECT QUANTILE(0.99, value) AS p99 FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1 \
             ORDER BY mystery_col",
        )
        .is_none());
    }

    #[test]
    fn test_order_by_expression_rejected() {
        assert!(parse_sql_query(
            "SELECT QUANTILE(0.99, value) AS p99 FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1 \
             ORDER BY p99 + 1",
        )
        .is_none());
    }

    #[test]
    fn test_limit_with_offset_rejected() {
        // OFFSET is not supported (no pagination semantics in the precompute model).
        assert!(parse_sql_query(
            "SELECT SUM(value) FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1 \
             LIMIT 5 OFFSET 3",
        )
        .is_none());
    }

    #[test]
    fn test_matches_template_ignores_order_by_and_limit() {
        // A registered template without ORDER BY / LIMIT must still match an incoming
        // query that adds them — they're presentational, not structural.
        let template = parse_sql_query(
            "SELECT QUANTILE(0.99, value) AS p99 FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, NOW()) AND NOW() \
             GROUP BY L1, L2",
        )
        .unwrap();
        let incoming = parse_sql_query(
            "SELECT QUANTILE(0.99, value) AS top FROM cpu_usage \
             WHERE time BETWEEN DATEADD(s, -10, '2025-10-01 00:00:10') AND '2025-10-01 00:00:10' \
             GROUP BY L1, L2 \
             ORDER BY top DESC LIMIT 25",
        )
        .unwrap();
        assert!(incoming.matches_sql_pattern(&template));
    }
}

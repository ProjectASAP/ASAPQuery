use asap_planner::{ControllerError, SQLController, SQLRuntimeOptions, StreamingEngine};

// ── helpers ──────────────────────────────────────────────────────────────────

fn sql_opts() -> SQLRuntimeOptions {
    SQLRuntimeOptions {
        streaming_engine: StreamingEngine::Arroyo,
        // Fixed evaluation time so NOW()-relative timestamps are deterministic.
        query_evaluation_time: Some(1_000_000.0),
        data_ingestion_interval: 15,
    }
}

/// Single-query config with a 3-column metadata schema.
///
/// Schema: metrics_table
///   time_column       : time
///   value_columns     : [cpu_usage]
///   metadata_columns  : [hostname, datacenter, region]
/// data_ingestion_interval = 15 s
fn one_query_config(query: &str, t_repeat: u64) -> String {
    format!(
        r#"
tables:
  - name: metrics_table
    time_column: time
    value_columns: [cpu_usage]
    metadata_columns: [hostname, datacenter, region]
query_groups:
  - id: 1
    repetition_delay: {t_repeat}
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        {query}
aggregate_cleanup:
  policy: read_based
"#
    )
}

// ── single-query happy-path tests ─────────────────────────────────────────────
//
// All queries: SELECT <agg>(cpu_usage) FROM metrics_table
//              WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW()
//              GROUP BY datacenter
//
// With 3 metadata columns [hostname, datacenter, region] and GROUP BY datacenter:
//   rollup    = [hostname, region]  (the two NOT in GROUP BY)
//   grouping  = [datacenter]
//   aggregated = []                 (no label-level aggregation dimension in SQL)
//
// Each test checks ALL observable plan properties:
//   - streaming_aggregation_count
//   - inference_query_count (always 1)
//   - aggregation types present
//   - tumbling window size
//   - table_name and value_column
//   - label routing: rollup / grouping / aggregated
//   - inference cleanup param

// ── aggregate-type coverage (T = 300 s, range = 300 s) ───────────────────────

/// SUM is Approximate → CountMinSketch + DeltaSetAggregator.
/// window = range = 300 s, cleanup = ceil(300 / 300) = 1.
#[test]
fn temporal_sum() {
    let q = "SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 2);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("CountMinSketch"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("CountMinSketch"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("CountMinSketch", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "grouping"),
        Vec::<String>::new()
    );
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "aggregated"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

/// COUNT is Approximate → CountMinSketch + DeltaSetAggregator (identical plan to SUM).
/// window = 300 s, cleanup = 1.
#[test]
fn temporal_count() {
    let q = "SELECT COUNT(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 2);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("CountMinSketch"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("CountMinSketch"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("CountMinSketch", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "grouping"),
        Vec::<String>::new()
    );
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "aggregated"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

/// MIN is Exact → MultipleMinMax only (no DeltaSetAggregator).
/// window = 300 s, cleanup = 1.
#[test]
fn temporal_min() {
    let q = "SELECT MIN(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("MultipleMinMax"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("MultipleMinMax"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("MultipleMinMax"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("MultipleMinMax", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("MultipleMinMax", "grouping"),
        Vec::<String>::new()
    );
    assert_eq!(
        out.aggregation_labels("MultipleMinMax", "aggregated"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

/// MAX is Exact → MultipleMinMax only (identical plan to MIN).
/// window = 300 s, cleanup = 1.
#[test]
fn temporal_max() {
    let q = "SELECT MAX(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("MultipleMinMax"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("MultipleMinMax"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("MultipleMinMax"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("MultipleMinMax", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("MultipleMinMax", "grouping"),
        Vec::<String>::new()
    );
    assert_eq!(
        out.aggregation_labels("MultipleMinMax", "aggregated"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

/// AVG decomposes into SUM + COUNT → 2 CountMinSketch + 1 DeltaSetAggregator (deduped) = 3 total.
/// window = 300 s, cleanup = 1.
#[test]
fn temporal_avg() {
    let q = "SELECT AVG(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 3);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("CountMinSketch"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("CountMinSketch"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("CountMinSketch", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "grouping"),
        Vec::<String>::new()
    );
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "aggregated"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

/// QUANTILE (Clickhouse parametric syntax) → DatasketchesKLL only (no DeltaSetAggregator).
/// window = 300 s, cleanup = 1.
#[test]
fn temporal_quantile() {
    let q = "SELECT quantile(0.95)(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("DatasketchesKLL"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("DatasketchesKLL"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("DatasketchesKLL"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("DatasketchesKLL", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("DatasketchesKLL", "grouping"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(
        out.aggregation_labels("DatasketchesKLL", "aggregated"),
        Vec::<String>::new()
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

// ── Elastic SQL syntax variants ───────────────────────────────────────────────
//
// These three tests exercise syntactic forms used by Elastic:
//   1. PERCENTILE(col, integer_percentile)  — integer 0–100, col-first arg order
//   2. DATEADD('s', …)                      — quoted unit string
//   3. CAST('…' AS DATETIME)                — absolute timestamp bounds
//
// All produce the same plan as temporal_quantile: DatasketchesKLL, no
// DeltaSetAggregator, one streaming config, window = T = 300 s, cleanup = 1.

/// PERCENTILE(col, 95) is the Elastic aggregation syntax.
/// The parser normalises it to QUANTILE internally, so the plan is identical
/// to temporal_quantile (DatasketchesKLL, grouping = [datacenter], rollup = [hostname, region]).
#[test]
fn temporal_quantile_percentile_syntax() {
    let q = "SELECT PERCENTILE(cpu_usage, 95) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("DatasketchesKLL"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("DatasketchesKLL"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("DatasketchesKLL"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("DatasketchesKLL", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("DatasketchesKLL", "grouping"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(
        out.aggregation_labels("DatasketchesKLL", "aggregated"),
        Vec::<String>::new()
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

/// DATEADD('s', …) — quoted unit string (Elastic style) vs unquoted `s`.
/// The parser accepts both forms; the plan must be identical to temporal_quantile.
#[test]
fn temporal_quantile_quoted_dateadd_unit() {
    let q = "SELECT PERCENTILE(cpu_usage, 95) FROM metrics_table WHERE time BETWEEN DATEADD('s', -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("DatasketchesKLL"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("DatasketchesKLL"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("DatasketchesKLL"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("DatasketchesKLL", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("DatasketchesKLL", "grouping"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(
        out.aggregation_labels("DatasketchesKLL", "aggregated"),
        Vec::<String>::new()
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

/// Full Elastic syntax: PERCENTILE(col, N) + DATEADD('s', …) + CAST('…' AS DATETIME).
/// Absolute timestamp bounds replace NOW(); the 300 s range still yields cleanup = 1.
#[test]
fn temporal_quantile_cast_datetime_bounds() {
    let q = "SELECT PERCENTILE(cpu_usage, 95) FROM metrics_table WHERE time BETWEEN DATEADD('s', -300, CAST('2024-01-01T00:05:00Z' AS DATETIME)) AND CAST('2024-01-01T00:05:00Z' AS DATETIME) GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("DatasketchesKLL"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300));
    assert_eq!(
        out.aggregation_table_name("DatasketchesKLL"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("DatasketchesKLL"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("DatasketchesKLL", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("DatasketchesKLL", "grouping"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(
        out.aggregation_labels("DatasketchesKLL", "aggregated"),
        Vec::<String>::new()
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

// ── T-value variants for SUM (range = 300 s fixed) ───────────────────────────
//
// These three tests use the same query and differ only in repetition_delay (T).
// Window size = T (the repetition delay), not the query range.
// They verify cleanup scales correctly with T and that T > range is valid.

/// T = 30 s < range: window = 30, cleanup = ceil(300 / 30) = 10.
#[test]
fn temporal_sum_t30() {
    let q = "SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 30), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 2);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(30));
    assert_eq!(
        out.aggregation_table_name("CountMinSketch"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("CountMinSketch"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("CountMinSketch", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "grouping"),
        Vec::<String>::new()
    );
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "aggregated"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(out.inference_cleanup_param(q), Some(10));
}

/// T = 300 s == range: covered by temporal_sum above (cleanup = 1).
/// T = 600 s > range: window = 600, cleanup = ceil(300 / 600) = 1.
/// T larger than the query range is valid; the result is still 1 retained window.
#[test]
fn temporal_sum_t600() {
    let q = "SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let out = SQLController::from_yaml(&one_query_config(q, 600), sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 2);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(600));
    assert_eq!(
        out.aggregation_table_name("CountMinSketch"),
        Some("metrics_table".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("CountMinSketch"),
        Some("cpu_usage".to_string())
    );
    let mut rollup = out.aggregation_labels("CountMinSketch", "rollup");
    rollup.sort();
    assert_eq!(rollup, vec!["hostname".to_string(), "region".to_string()]);
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "grouping"),
        Vec::<String>::new()
    );
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "aggregated"),
        vec!["datacenter".to_string()]
    );
    assert_eq!(out.inference_cleanup_param(q), Some(1));
}

// ── multi-query tests ─────────────────────────────────────────────────────────

/// MIN and MAX: distinct sub_types → 2 streaming configs (one "min", one "max"), 2 inference entries.
#[test]
fn two_queries_min_and_max_produce_separate_streaming_configs() {
    let yaml = r#"
tables:
  - name: metrics_table
    time_column: time
    value_columns: [cpu_usage]
    metadata_columns: [hostname, datacenter, region]
query_groups:
  - id: 1
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        SELECT MIN(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter
  - id: 2
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        SELECT MAX(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter
aggregate_cleanup:
  policy: read_based
"#;
    let out = SQLController::from_yaml(yaml, sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 2);
    assert_eq!(out.inference_query_count(), 2);
    assert!(out.has_aggregation_type_and_sub_type("MultipleMinMax", "min"));
    assert!(out.has_aggregation_type_and_sub_type("MultipleMinMax", "max"));
}

/// Two SUM queries on different value columns: neither pair deduped (different value_column).
/// 2 × (CMS + DeltaSet) = 4 configs, 2 inference entries.
#[test]
fn two_queries_different_value_columns_produce_four_configs() {
    let yaml = r#"
tables:
  - name: metrics_table
    time_column: time
    value_columns: [cpu_usage, memory_usage]
    metadata_columns: [hostname, datacenter, region]
query_groups:
  - id: 1
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter
      - >-
        SELECT SUM(memory_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter
aggregate_cleanup:
  policy: read_based
"#;
    let out = SQLController::from_yaml(yaml, sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 4);
    assert_eq!(out.inference_query_count(), 2);
}

/// Identical SQL queries in separate groups are rejected as duplicates, same as PromQL.
#[test]
fn identical_queries_across_groups_return_duplicate_error() {
    let q = "SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let yaml = format!(
        r#"
tables:
  - name: metrics_table
    time_column: time
    value_columns: [cpu_usage]
    metadata_columns: [hostname, datacenter, region]
query_groups:
  - id: 1
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        {q}
  - id: 2
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        {q}
aggregate_cleanup:
  policy: read_based
"#
    );
    let result = SQLController::from_yaml(&yaml, sql_opts())
        .unwrap()
        .generate();
    assert!(matches!(result, Err(ControllerError::DuplicateQuery(_))));
}

/// SUM queries with same table/column but different window sizes
/// Since they will both produce tumnbling windows of 300, we will only get 2 configs total
#[test]
fn two_queries_different_windows_are_deduped() {
    let yaml = r#"
tables:
  - name: metrics_table
    time_column: time
    value_columns: [cpu_usage]
    metadata_columns: [hostname, datacenter, region]
query_groups:
  - id: 1
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter
  - id: 2
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -600, NOW()) AND NOW() GROUP BY datacenter
aggregate_cleanup:
  policy: read_based
"#;
    let out = SQLController::from_yaml(yaml, sql_opts())
        .unwrap()
        .generate()
        .unwrap();

    assert_eq!(out.streaming_aggregation_count(), 2);
    assert_eq!(out.inference_query_count(), 2);
}

// ── output-file test ──────────────────────────────────────────────────────────

/// generate_to_dir writes both streaming_config.yaml and inference_config.yaml.
#[test]
fn generate_to_dir_writes_both_yaml_files() {
    let dir = tempfile::tempdir().unwrap();
    let q = "SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate_to_dir(dir.path())
        .unwrap();
    assert!(dir.path().join("streaming_config.yaml").exists());
    assert!(dir.path().join("inference_config.yaml").exists());
}

// ── error-path tests ──────────────────────────────────────────────────────────

#[test]
fn malformed_yaml_returns_parse_error() {
    let result = SQLController::from_yaml("{ invalid yaml :", sql_opts());
    assert!(matches!(result, Err(ControllerError::YamlParse(_))));
}

#[test]
fn malformed_sql_returns_sql_parse_error() {
    let q = "NOT VALID SQL AT ALL %%%";
    let result = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate();
    assert!(matches!(result, Err(ControllerError::SqlParse(_))));
}

#[test]
fn query_referencing_unknown_table_returns_error() {
    let q = "SELECT SUM(cpu_usage) FROM nonexistent_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let result = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate();
    assert!(matches!(
        result,
        Err(ControllerError::UnknownTable(_)) | Err(ControllerError::SqlParse(_))
    ));
}

#[test]
fn query_referencing_unknown_value_column_returns_sql_parse_error() {
    let q = "SELECT SUM(nonexistent_col) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let result = SQLController::from_yaml(&one_query_config(q, 300), sql_opts())
        .unwrap()
        .generate();
    assert!(matches!(result, Err(ControllerError::SqlParse(_))));
}

#[test]
fn duplicate_queries_in_same_group_return_error() {
    let q = "SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let yaml = format!(
        r#"
tables:
  - name: metrics_table
    time_column: time
    value_columns: [cpu_usage]
    metadata_columns: [hostname, datacenter, region]
query_groups:
  - id: 1
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        {q}
      - >-
        {q}
aggregate_cleanup:
  policy: read_based
"#
    );
    let result = SQLController::from_yaml(&yaml, sql_opts())
        .unwrap()
        .generate();
    assert!(matches!(result, Err(ControllerError::DuplicateQuery(_))));
}

#[test]
fn unknown_cleanup_policy_returns_planner_error() {
    let q = "SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY datacenter";
    let yaml = format!(
        r#"
tables:
  - name: metrics_table
    time_column: time
    value_columns: [cpu_usage]
    metadata_columns: [hostname, datacenter, region]
query_groups:
  - id: 1
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        {q}
aggregate_cleanup:
  policy: not_a_real_policy
"#
    );
    // Invalid policy is now caught at deserialization time (YamlParse) rather than at
    // generate() time (PlannerError), since the field is typed as Option<CleanupPolicy>.
    assert!(matches!(
        SQLController::from_yaml(&yaml, sql_opts()),
        Err(ControllerError::YamlParse(_))
    ));
}

/// T that is not a multiple of data_ingestion_interval is invalid: sketch windows
/// must align with the ingestion cadence.
/// data_ingestion_interval = 15 s, T = 200 s → 200 mod 15 ≠ 0 → PlannerError.
#[test]
fn t_not_multiple_of_data_ingestion_interval_returns_planner_error() {
    let q = "SELECT SUM(cpu_usage) FROM metrics_table WHERE time BETWEEN DATEADD(s, -200, NOW()) AND NOW() GROUP BY datacenter";
    let result = SQLController::from_yaml(&one_query_config(q, 200), sql_opts())
        .unwrap()
        .generate();
    assert!(matches!(result, Err(ControllerError::PlannerError(_))));
}

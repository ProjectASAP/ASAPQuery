use asap_planner::{Controller, ControllerError, PromQLSchema, RuntimeOptions, StreamingEngine};
use promql_utilities::data_model::KeyByLabelNames;
use std::path::Path;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn arroyo_opts() -> RuntimeOptions {
    RuntimeOptions {
        data_ingestion_interval_ms: 15_000,
        streaming_engine: StreamingEngine::Arroyo,
        enable_punting: false,
        range_duration_ms: 0,
        step_ms: 0,
    }
}

/// Standard test schema: http_requests_total with [instance, job, method, status].
fn http_requests_schema() -> PromQLSchema {
    PromQLSchema::new().add_metric(
        "http_requests_total".to_string(),
        KeyByLabelNames::new(vec![
            "instance".to_string(),
            "job".to_string(),
            "method".to_string(),
            "status".to_string(),
        ]),
    )
}

#[test]
fn config_file_sliding_window_override_generates_per_query_candidates() {
    let controller = Controller::from_file_with_schema(
        Path::new("tests/test_data/windowing/promql_sliding.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();

    let output = controller.generate().unwrap();
    let streaming: serde_yaml::Value =
        serde_yaml::from_str(&output.to_streaming_yaml_string().unwrap()).unwrap();
    let aggregations = streaming["aggregations"].as_sequence().unwrap();

    assert_eq!(output.inference_query_count(), 2);
    assert_eq!(aggregations.len(), 2);

    let mut candidates: Vec<(u64, u64, String)> = aggregations
        .iter()
        .map(|aggregation| {
            (
                aggregation["windowSizeMs"].as_u64().unwrap(),
                aggregation["slideIntervalMs"].as_u64().unwrap(),
                aggregation["windowType"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    candidates.sort();

    assert_eq!(
        candidates,
        vec![
            (60_000, 15_000, "sliding".to_string()),
            (120_000, 30_000, "sliding".to_string()),
        ]
    );
    assert!(candidates
        .iter()
        .all(|(_, _, window_type)| window_type != "tumbling"));
}

#[test]
fn sliding_window_config_requires_a_slide_divisor() {
    let result = Controller::from_yaml_with_schema(
        r#"
windowing:
  type: "sliding"
query_groups: []
"#,
        http_requests_schema(),
        arroyo_opts(),
    );
    let error = match result {
        Ok(_) => panic!("sliding config without a divisor should be rejected"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("windowing.slide_divisor is required for sliding windows"));
}

#[test]
fn sliding_window_validation_reports_all_invalid_queries() {
    let controller = Controller::from_yaml_with_schema(
        r#"
windowing:
  type: "sliding"
  slide_divisor: 3
query_groups:
  - id: 1
    queries:
      - "rate(http_requests_total[65s])"
    repetition_delay_ms: 65000
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
  - id: 2
    queries:
      - "rate(http_requests_total[55s])"
    repetition_delay_ms: 55000
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
"#,
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();

    let result = controller.generate();
    let error = match result {
        Ok(_) => panic!("invalid sliding windows should abort plan generation"),
        Err(error) => error,
    };
    let message = error.to_string();

    assert!(message.contains("rate(http_requests_total[65s])"));
    assert!(message.contains("rate(http_requests_total[55s])"));
}

#[test]
fn sliding_window_config_rejects_redundant_divisors() {
    let result = Controller::from_yaml_with_schema(
        r#"
windowing:
  type: "sliding"
  slide_divisor: 1
query_groups: []
"#,
        http_requests_schema(),
        arroyo_opts(),
    );
    let error = match result {
        Ok(_) => panic!("sliding config with divisor 1 should be rejected"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("windowing.slide_divisor must be at least 2, got 1"));
}

#[test]
fn tumbling_window_config_rejects_a_slide_divisor() {
    let result = Controller::from_yaml_with_schema(
        r#"
windowing:
  type: "tumbling"
  slide_divisor: 4
query_groups: []
"#,
        http_requests_schema(),
        arroyo_opts(),
    );
    let error = match result {
        Ok(_) => panic!("tumbling config with a divisor should be rejected"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("windowing.slide_divisor is only valid for sliding windows"));
}

#[test]
fn explicit_tumbling_window_override_keeps_tumbling_candidates() {
    let controller = Controller::from_yaml_with_schema(
        r#"
windowing:
  type: "tumbling"
query_groups:
  - id: 1
    queries:
      - "rate(http_requests_total[1m])"
    repetition_delay_ms: 60000
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
"#,
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();

    let output = controller.generate().unwrap();
    let streaming: serde_yaml::Value =
        serde_yaml::from_str(&output.to_streaming_yaml_string().unwrap()).unwrap();
    let aggregation = &streaming["aggregations"][0];

    assert_eq!(aggregation["windowType"].as_str(), Some("tumbling"));
    assert_eq!(
        aggregation["slideIntervalMs"].as_u64(),
        aggregation["windowSizeMs"].as_u64()
    );
    assert_ne!(aggregation["windowType"].as_str(), Some("sliding"));
}

/// Schema for binary arithmetic tests: errors_total and requests_total.
fn binary_arithmetic_schema() -> PromQLSchema {
    PromQLSchema::new()
        .add_metric(
            "errors_total".to_string(),
            KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()]),
        )
        .add_metric(
            "requests_total".to_string(),
            KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()]),
        )
}

/// Schema for nested binary arithmetic test: a_total, b_total, c_total.
fn nested_binary_arithmetic_schema() -> PromQLSchema {
    PromQLSchema::new()
        .add_metric(
            "a_total".to_string(),
            KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()]),
        )
        .add_metric(
            "b_total".to_string(),
            KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()]),
        )
        .add_metric(
            "c_total".to_string(),
            KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()]),
        )
}

// ─── query_log integration tests ─────────────────────────────────────────────

#[test]
fn query_log_instant_produces_valid_configs() {
    let c = Controller::from_query_log_with_schema(
        Path::new("tests/comparison/test_data/query_logs/instant_only.log"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.streaming_aggregation_count() > 0);
    assert!(out.inference_query_count() > 0);
}

#[test]
fn query_log_range_produces_valid_configs() {
    let c = Controller::from_query_log_with_schema(
        Path::new("tests/comparison/test_data/query_logs/range_only.log"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    // range_only.log has step=30 (seconds, native log format) → step_ms=30_000
    assert!(out.all_tumbling_window_sizes_leq(30_000));
}

#[test]
fn query_log_single_occurrence_excluded() {
    let c = Controller::from_query_log_with_schema(
        Path::new("tests/comparison/test_data/query_logs/single_occurrence.log"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.inference_query_count(), 0);
}

#[test]
fn query_log_malformed_lines_skipped() {
    // with_malformed.log has 5 valid entries for rate() interspersed with bad lines
    let c = Controller::from_query_log_with_schema(
        Path::new("tests/comparison/test_data/query_logs/with_malformed.log"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.inference_query_count() > 0);
}

#[test]
fn query_log_output_files_written() {
    let dir = tempfile::tempdir().unwrap();
    let c = Controller::from_query_log_with_schema(
        Path::new("tests/comparison/test_data/query_logs/instant_only.log"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    c.generate_to_dir(dir.path()).unwrap();
    assert!(dir.path().join("streaming_config.yaml").exists());
    assert!(dir.path().join("inference_config.yaml").exists());
}

#[test]
fn quantile_over_time_produces_kll() {
    // quantile_over_time groups by all labels → 1 DatasketchesKLL config
    // Arroyo/Flink maintains one sketch per unique label-value combination at runtime
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/quantile_over_time.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 1);
    assert!(out.has_aggregation_type("DatasketchesKLL"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
}

#[test]
fn rate_produces_multiple_increase_only() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/rate_increase.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 1);
    assert!(out.has_aggregation_type("MultipleIncrease"));
}

#[test]
fn only_spatial_window_equals_scrape_interval() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/spatial_quantile.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_eq(15_000));
}

#[test]
fn duplicate_aggregation_configs_are_deduped() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/deduplicated.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 2);
}

#[test]
fn topk_produces_count_min_sketch_with_heap() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/topk.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.has_aggregation_type("CountMinSketchWithHeap"));
}

#[test]
fn range_query_uses_effective_repeat() {
    let opts = RuntimeOptions {
        range_duration_ms: 3_600_000,
        step_ms: 30_000,
        ..arroyo_opts()
    };
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/range_query.yaml"),
        http_requests_schema(),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_leq(30_000));
}

#[test]
fn output_files_written_to_dir() {
    let dir = tempfile::tempdir().unwrap();
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/mixed_workload.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    c.generate_to_dir(dir.path()).unwrap();
    assert!(dir.path().join("streaming_config.yaml").exists());
    assert!(dir.path().join("inference_config.yaml").exists());
}

#[test]
fn rate_tumbling_window_size_equals_effective_repeat() {
    // For range queries, effective_repeat = min(t_repeat=300_000, step=30_000) = 30_000
    // Tumbling window size must equal effective_repeat (sliding is always disabled)
    let opts = RuntimeOptions {
        range_duration_ms: 3_600_000,
        step_ms: 30_000,
        ..arroyo_opts()
    };
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/rate_increase.yaml"),
        http_requests_schema(),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_eq(30_000));
}

#[test]
fn increase_tumbling_window_size_equals_effective_repeat() {
    // effective_repeat = min(t_repeat=300_000, step=30_000) = 30_000
    let opts = RuntimeOptions {
        range_duration_ms: 3_600_000,
        step_ms: 30_000,
        ..arroyo_opts()
    };
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/increase.yaml"),
        http_requests_schema(),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.has_aggregation_type("MultipleIncrease"));
    assert!(out.all_tumbling_window_sizes_eq(30_000));
}

#[test]
fn quantile_over_time_tumbling_window_size_equals_effective_repeat() {
    // effective_repeat = min(t_repeat=300_000, step=30_000) = 30_000
    let opts = RuntimeOptions {
        range_duration_ms: 3_600_000,
        step_ms: 30_000,
        ..arroyo_opts()
    };
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/quantile_over_time.yaml"),
        http_requests_schema(),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.has_aggregation_type("DatasketchesKLL"));
    assert!(out.all_tumbling_window_sizes_eq(30_000));
}

#[test]
fn sum_over_time_produces_count_min_sketch_with_delta_set() {
    // sum_over_time is Approximate → CountMinSketch + DeltaSetAggregator pairing
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/sum_over_time.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 2);
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DeltaSetAggregator"));
}

#[test]
fn sum_by_produces_count_min_sketch_with_delta_set() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/sum_by.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 2);
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DeltaSetAggregator"));
}

#[test]
fn sum_by_rollup_excludes_groupby_labels() {
    // sum by (job, method) → rollup gets labels NOT in by-clause
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/sum_by.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(
        out.aggregation_labels("CountMinSketch", "rollup"),
        vec!["instance", "status"]
    );
}

// --- Error-path tests ---

#[test]
fn unknown_cleanup_policy_returns_planner_error() {
    let yaml = r#"
query_groups:
  - id: 1
    queries:
      - "rate(http_requests_total[5m])"
    repetition_delay_ms: 300000
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
aggregate_cleanup:
  policy: "not_a_real_policy"
"#;
    // Invalid policy is now caught at deserialization time (YamlParse) rather than at
    // generate() time (PlannerError), since the field is typed as Option<CleanupPolicy>.
    assert!(matches!(
        Controller::from_yaml_with_schema(yaml, http_requests_schema(), arroyo_opts()),
        Err(ControllerError::YamlParse(_))
    ));
}

#[test]
fn duplicate_query_in_same_group_returns_error() {
    let yaml = r#"
query_groups:
  - id: 1
    queries:
      - "rate(http_requests_total[5m])"
      - "rate(http_requests_total[5m])"
    repetition_delay_ms: 300000
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
"#;
    let c = Controller::from_yaml_with_schema(yaml, http_requests_schema(), arroyo_opts()).unwrap();
    assert!(matches!(
        c.generate(),
        Err(ControllerError::DuplicateQuery(_))
    ));
}

#[test]
fn duplicate_query_across_groups_returns_error() {
    let yaml = r#"
query_groups:
  - id: 1
    queries:
      - "rate(http_requests_total[5m])"
    repetition_delay_ms: 300000
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
  - id: 2
    queries:
      - "rate(http_requests_total[5m])"
    repetition_delay_ms: 60000
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
"#;
    let c = Controller::from_yaml_with_schema(yaml, http_requests_schema(), arroyo_opts()).unwrap();
    assert!(matches!(
        c.generate(),
        Err(ControllerError::DuplicateQuery(_))
    ));
}

#[test]
fn query_referencing_unknown_metric_is_skipped_with_warning() {
    // Unknown metric no longer aborts the run; the query is silently skipped.
    let yaml = r#"
query_groups:
  - id: 1
    queries:
      - "rate(unknown_metric[5m])"
    repetition_delay_ms: 300000
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
"#;
    // Schema only knows about http_requests_total, not unknown_metric.
    let c = Controller::from_yaml_with_schema(yaml, http_requests_schema(), arroyo_opts()).unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.inference_query_count(), 0);
    assert_eq!(out.streaming_aggregation_count(), 0);
}

#[test]
fn malformed_yaml_returns_parse_error() {
    let result =
        Controller::from_yaml_with_schema("{ invalid yaml :", PromQLSchema::new(), arroyo_opts());
    assert!(matches!(result, Err(ControllerError::YamlParse(_))));
}

#[test]
fn metrics_field_in_yaml_is_accepted_as_hint() {
    // The `metrics` section is a backwards-compatible label hint used as a
    // fallback when Prometheus has no series for a metric. It must parse cleanly.
    let yaml = r#"
query_groups:
  - id: 1
    queries:
      - "rate(http_requests_total[5m])"
    repetition_delay_ms: 300000
metrics:
  - metric: "http_requests_total"
    labels: ["instance"]
"#;
    let result = Controller::from_yaml_with_schema(yaml, http_requests_schema(), arroyo_opts());
    assert!(result.is_ok());
}

// --- Overlapping window tests ---
// Queries where range vector > t_repeat: e.g. [5m] repeated every 60s.
// Windows are always tumbling (sliding disabled); the planner emits windowSize=t_repeat
// and the cleanup param tells the query engine how many windows to retain to cover the range.

#[test]
fn temporal_overlapping_window_size_equals_t_repeat() {
    // [5m] range repeated every 60_000ms → windowSizeMs = 60_000, not 300_000
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/temporal_overlapping.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_eq(60_000));
}

#[test]
fn temporal_overlapping_all_function_types_present() {
    // rate+increase → MultipleIncrease (deduped to 1), sum_over_time → CountMinSketch+DeltaSet,
    // quantile_over_time → DatasketchesKLL; 4 unique streaming aggregation configs total
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/temporal_overlapping.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 4);
    assert!(out.has_aggregation_type("MultipleIncrease"));
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DatasketchesKLL"));
}

#[test]
fn temporal_overlapping_cleanup_param_equals_range_over_repeat() {
    // t_lookback = 5m = 300s, effective_repeat = 60s → ceil(300/60) = 5
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/temporal_overlapping.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(
        out.inference_cleanup_param("rate(http_requests_total[5m])"),
        Some(5)
    );
    assert_eq!(
        out.inference_cleanup_param("increase(http_requests_total[5m])"),
        Some(5)
    );
    assert_eq!(
        out.inference_cleanup_param("sum_over_time(http_requests_total[5m])"),
        Some(5)
    );
    assert_eq!(
        out.inference_cleanup_param("quantile_over_time(0.99, http_requests_total[5m])"),
        Some(5)
    );
}

// --- Binary arithmetic tests ---

#[test]
fn binary_arithmetic_produces_two_leaf_configs() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic.yaml"),
        binary_arithmetic_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    // Two arms → two streaming aggregation configs
    assert_eq!(out.streaming_aggregation_count(), 2);
    // Two separate query_config entries (one per arm)
    assert_eq!(out.inference_query_count(), 2);
}

#[test]
fn binary_arithmetic_deduplicates_shared_arm() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic_dedup.yaml"),
        binary_arithmetic_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    // errors_total arm is shared — only 2 streaming configs total (not 3)
    assert_eq!(out.streaming_aggregation_count(), 2);
    // 2 query_config entries: rate(errors_total[5m]) and rate(requests_total[5m])
    assert_eq!(out.inference_query_count(), 2);
}

#[test]
fn nested_binary_arithmetic_produces_three_leaf_configs() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic_nested.yaml"),
        nested_binary_arithmetic_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 3);
    assert_eq!(out.inference_query_count(), 3);
}

#[test]
fn binary_arithmetic_scalar_constant_produces_one_leaf_config() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic_scalar.yaml"),
        binary_arithmetic_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    // Only the vector arm needs a streaming config; 100 is a literal
    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
}

#[test]
fn binary_arithmetic_with_non_acceleratable_arm_produces_no_configs() {
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic_non_acceleratable.yaml"),
        binary_arithmetic_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 0);
    assert_eq!(out.inference_query_count(), 0);
}

#[test]
fn temporal_overlapping_rate_increase_deduped() {
    // rate and increase produce identical MultipleIncrease configs → 1 streaming entry shared,
    // but inference config still tracks 4 queries separately
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/temporal_overlapping.yaml"),
        http_requests_schema(),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.inference_query_count(), 4);
    assert_eq!(out.streaming_aggregation_count(), 4); // not 5
}

// --- Sub-second scrape interval (issue #398) ---
// This is the actual capability the issue asks for: a 100ms scrape interval
// rounds to 0 under the old whole-seconds representation. These tests would
// have failed before the ms-precision rename and must pass after it.

#[test]
fn sub_second_scrape_interval_window_size_equals_scrape_interval() {
    // OnlySpatial query: window size = scrape interval (planner/window.rs).
    // 100ms would be indistinguishable from 0 under the old seconds-only model.
    let opts = RuntimeOptions {
        data_ingestion_interval_ms: 100,
        ..arroyo_opts()
    };
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/spatial_quantile.yaml"),
        http_requests_schema(),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_eq(100));
}

#[test]
fn sub_second_scrape_interval_round_trips_through_generated_yaml() {
    // The generated streaming_config.yaml must carry the sub-second value through
    // under the renamed wire key (windowSizeMs, not windowSize) — not silently
    // rounded or truncated.
    let opts = RuntimeOptions {
        data_ingestion_interval_ms: 100,
        ..arroyo_opts()
    };
    let c = Controller::from_file_with_schema(
        Path::new("tests/comparison/test_data/configs/spatial_quantile.yaml"),
        http_requests_schema(),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    let yaml = out.to_streaming_yaml_string().unwrap();
    assert!(
        yaml.contains("windowSizeMs: 100"),
        "expected a sub-second windowSizeMs in the generated YAML, got:\n{yaml}"
    );
}

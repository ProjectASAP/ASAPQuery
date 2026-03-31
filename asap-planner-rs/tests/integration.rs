use asap_planner::{Controller, ControllerError, RuntimeOptions, StreamingEngine};
use std::path::Path;

// ─── query_log integration tests ─────────────────────────────────────────────

#[test]
fn query_log_instant_produces_valid_configs() {
    let c = Controller::from_query_log(
        Path::new("tests/comparison/test_data/query_logs/instant_only.log"),
        Path::new("tests/comparison/test_data/metrics/http_requests.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.streaming_aggregation_count() > 0);
    assert!(out.inference_query_count() > 0);
}

#[test]
fn query_log_range_produces_valid_configs() {
    let c = Controller::from_query_log(
        Path::new("tests/comparison/test_data/query_logs/range_only.log"),
        Path::new("tests/comparison/test_data/metrics/http_requests.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    // range_only.log has step=30, so effective window size must be ≤ 30
    assert!(out.all_tumbling_window_sizes_leq(30));
}

#[test]
fn query_log_single_occurrence_excluded() {
    let c = Controller::from_query_log(
        Path::new("tests/comparison/test_data/query_logs/single_occurrence.log"),
        Path::new("tests/comparison/test_data/metrics/http_requests.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.inference_query_count(), 0);
}

#[test]
fn query_log_malformed_lines_skipped() {
    // with_malformed.log has 5 valid entries for rate() interspersed with bad lines
    let c = Controller::from_query_log(
        Path::new("tests/comparison/test_data/query_logs/with_malformed.log"),
        Path::new("tests/comparison/test_data/metrics/http_requests.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.inference_query_count() > 0);
}

#[test]
fn query_log_output_files_written() {
    let dir = tempfile::tempdir().unwrap();
    let c = Controller::from_query_log(
        Path::new("tests/comparison/test_data/query_logs/instant_only.log"),
        Path::new("tests/comparison/test_data/metrics/http_requests.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    c.generate_to_dir(dir.path()).unwrap();
    assert!(dir.path().join("streaming_config.yaml").exists());
    assert!(dir.path().join("inference_config.yaml").exists());
}

fn arroyo_opts() -> RuntimeOptions {
    RuntimeOptions {
        prometheus_scrape_interval: 15,
        streaming_engine: StreamingEngine::Arroyo,
        enable_punting: false,
        range_duration: 0,
        step: 0,
    }
}

#[test]
fn quantile_over_time_produces_kll() {
    // quantile_over_time groups by all labels → 1 DatasketchesKLL config
    // Arroyo/Flink maintains one sketch per unique label-value combination at runtime
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/quantile_over_time.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/rate_increase.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 1);
    assert!(out.has_aggregation_type("MultipleIncrease"));
}

#[test]
fn only_spatial_window_equals_scrape_interval() {
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/spatial_quantile.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_eq(15));
}

#[test]
fn duplicate_aggregation_configs_are_deduped() {
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/deduplicated.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 2);
}

#[test]
fn topk_produces_count_min_sketch_with_heap() {
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/topk.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.has_aggregation_type("CountMinSketchWithHeap"));
}

#[test]
fn range_query_uses_effective_repeat() {
    let opts = RuntimeOptions {
        range_duration: 3600,
        step: 30,
        ..arroyo_opts()
    };
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/range_query.yaml"),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_leq(30));
}

#[test]
fn output_files_written_to_dir() {
    let dir = tempfile::tempdir().unwrap();
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/mixed_workload.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    c.generate_to_dir(dir.path()).unwrap();
    assert!(dir.path().join("streaming_config.yaml").exists());
    assert!(dir.path().join("inference_config.yaml").exists());
}

#[test]
fn rate_tumbling_window_size_equals_effective_repeat() {
    // For range queries, effective_repeat = min(t_repeat=300, step=30) = 30
    // Tumbling window size must equal effective_repeat (sliding is always disabled)
    let opts = RuntimeOptions {
        range_duration: 3600,
        step: 30,
        ..arroyo_opts()
    };
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/rate_increase.yaml"),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_eq(30));
}

#[test]
fn increase_tumbling_window_size_equals_effective_repeat() {
    // effective_repeat = min(t_repeat=300, step=30) = 30
    let opts = RuntimeOptions {
        range_duration: 3600,
        step: 30,
        ..arroyo_opts()
    };
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/increase.yaml"),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.has_aggregation_type("MultipleIncrease"));
    assert!(out.all_tumbling_window_sizes_eq(30));
}

#[test]
fn quantile_over_time_tumbling_window_size_equals_effective_repeat() {
    // effective_repeat = min(t_repeat=300, step=30) = 30
    let opts = RuntimeOptions {
        range_duration: 3600,
        step: 30,
        ..arroyo_opts()
    };
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/quantile_over_time.yaml"),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.has_aggregation_type("DatasketchesKLL"));
    assert!(out.all_tumbling_window_sizes_eq(30));
}

#[test]
fn sum_over_time_produces_count_min_sketch_with_delta_set() {
    // sum_over_time is Approximate → CountMinSketch + DeltaSetAggregator pairing
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/sum_over_time.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/sum_by.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/sum_by.yaml"),
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
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
metrics:
  - metric: "http_requests_total"
    labels: ["instance"]
aggregate_cleanup:
  policy: "not_a_real_policy"
"#;
    let c = Controller::from_yaml(yaml, arroyo_opts()).unwrap();
    assert!(matches!(
        c.generate(),
        Err(ControllerError::PlannerError(_))
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
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
metrics:
  - metric: "http_requests_total"
    labels: ["instance"]
"#;
    let c = Controller::from_yaml(yaml, arroyo_opts()).unwrap();
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
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
  - id: 2
    queries:
      - "rate(http_requests_total[5m])"
    repetition_delay: 60
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
metrics:
  - metric: "http_requests_total"
    labels: ["instance"]
"#;
    let c = Controller::from_yaml(yaml, arroyo_opts()).unwrap();
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
    repetition_delay: 300
    controller_options:
      accuracy_sla: 0.99
      latency_sla: 1.0
metrics:
  - metric: "http_requests_total"
    labels: ["instance"]
"#;
    let c = Controller::from_yaml(yaml, arroyo_opts()).unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.inference_query_count(), 0);
    assert_eq!(out.streaming_aggregation_count(), 0);
}

#[test]
fn malformed_yaml_returns_parse_error() {
    let result = Controller::from_yaml("{ invalid yaml :", arroyo_opts());
    assert!(matches!(result, Err(ControllerError::YamlParse(_))));
}

// --- Overlapping window tests ---
// Queries where range vector > t_repeat: e.g. [5m] repeated every 60s.
// Windows are always tumbling (sliding disabled); the planner emits windowSize=t_repeat
// and the cleanup param tells the query engine how many windows to retain to cover the range.

#[test]
fn temporal_overlapping_window_size_equals_t_repeat() {
    // [5m] range repeated every 60s → windowSize = 60, not 300
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/temporal_overlapping.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert!(out.all_tumbling_window_sizes_eq(60));
}

#[test]
fn temporal_overlapping_all_function_types_present() {
    // rate+increase → MultipleIncrease (deduped to 1), sum_over_time → CountMinSketch+DeltaSet,
    // quantile_over_time → DatasketchesKLL; 4 unique streaming aggregation configs total
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/temporal_overlapping.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/temporal_overlapping.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic_dedup.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic_nested.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.streaming_aggregation_count(), 3);
    assert_eq!(out.inference_query_count(), 3);
}

#[test]
fn binary_arithmetic_scalar_constant_produces_one_leaf_config() {
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic_scalar.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/binary_arithmetic_non_acceleratable.yaml"),
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
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/temporal_overlapping.yaml"),
        arroyo_opts(),
    )
    .unwrap();
    let out = c.generate().unwrap();
    assert_eq!(out.inference_query_count(), 4);
    assert_eq!(out.streaming_aggregation_count(), 4); // not 5
}

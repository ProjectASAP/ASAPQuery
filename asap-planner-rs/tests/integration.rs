use asap_planner::{Controller, RuntimeOptions, StreamingEngine};
use std::path::Path;

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
fn punting_runs_without_panic() {
    let opts = RuntimeOptions {
        enable_punting: true,
        ..arroyo_opts()
    };
    let c = Controller::from_file(
        Path::new("tests/comparison/test_data/configs/mixed_workload.yaml"),
        opts,
    )
    .unwrap();
    let out = c.generate().unwrap();
    let _ = &out.punted_queries;
}

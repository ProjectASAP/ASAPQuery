//! End-to-end regression test for the one-shot NetFlow ingest failure mode.
//!
//! Scenario (reported in the field): a one-shot JSON ingest of NetFlow records
//! whose timestamps all fall within the **same second**. Every record lands in
//! a single 1-second tumbling window and no later timestamp ever arrives to
//! advance the watermark, so the window never closes via event-time. Before the
//! shutdown force-close, the store stayed empty even after the worker shut down.
//!
//! This test drives the real `JsonFileIngestSource → PrecomputeEngine → sink`
//! path. Crucially it sets `wall_clock_grace_period_ms = 0`, disabling the
//! wall-clock fallback, so the *only* thing that can close the window is the
//! shutdown force-close. A non-empty sink therefore proves the force-close is
//! what rescues the one-shot batch.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::enums::{AggregationType, WindowType};

use query_engine_rust::data_model::StreamingConfig;
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::CapturingOutputSink;
use query_engine_rust::precompute_engine::{
    IngestSource, JsonFileIngestConfig, JsonFileIngestSource, PrecomputeEngine, TimestampUnit,
};
use query_engine_rust::precompute_operators::sum_accumulator::SumAccumulator;

fn netflow_agg_config(metric: &str, window_size_ms: u64) -> AggregationConfig {
    AggregationConfig::new(
        1,
        AggregationType::Sum,
        "".to_string(),
        HashMap::new(),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        String::new(),
        window_size_ms,
        0,
        WindowType::Tumbling,
        metric.to_string(),
        metric.to_string(),
        None,
        None,
        None,
        None,
    )
}

fn engine_config_grace_disabled() -> PrecomputeEngineConfig {
    PrecomputeEngineConfig {
        num_workers: 2,
        allowed_lateness_ms: 0,
        max_buffer_per_series: 10_000,
        flush_interval_ms: 100,
        channel_buffer_size: 10_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
        // Disable the wall-clock fallback: with grace=0 the trailing window can
        // ONLY be closed by the shutdown force-close. This isolates the fix.
        wall_clock_grace_period_ms: 0,
    }
}

#[tokio::test]
async fn netflow_single_second_batch_is_not_lost_on_shutdown() {
    // --- Synthesize a NetFlow JSONL where every record is in the same second.
    // json_ingest parses "YYYY-MM-DD HH:MM:SS" at second resolution, so all
    // rows collapse onto one timestamp → one window, no watermark advance.
    let rows = [
        ("2024-06-01 12:00:00", "10.0.0.1", "10.0.0.2", 100.0),
        ("2024-06-01 12:00:00", "10.0.0.3", "10.0.0.4", 250.0),
        ("2024-06-01 12:00:00", "10.0.0.5", "10.0.0.6", 75.0),
        ("2024-06-01 12:00:00", "10.0.0.7", "10.0.0.8", 500.0),
        ("2024-06-01 12:00:00", "10.0.0.9", "10.0.0.10", 25.0),
    ];
    let expected_total_bytes: f64 = rows.iter().map(|r| r.3).sum();

    let path = std::env::temp_dir().join(format!(
        "netflow_single_second_{}.jsonl",
        std::process::id()
    ));
    {
        let mut f = std::fs::File::create(&path).expect("create temp netflow file");
        for (ts, src, dst, bytes) in rows.iter() {
            writeln!(
                f,
                r#"{{"timestamp":"{ts}","src_ip":"{src}","dst_ip":"{dst}","bytes":{bytes}}}"#
            )
            .unwrap();
        }
    }

    // --- Wire up the real engine: 1s tumbling Sum over `bytes`.
    let metric = "netflow_bytes";
    let mut agg_map = HashMap::new();
    agg_map.insert(1u64, netflow_agg_config(metric, 1_000));
    let streaming_config = Arc::new(StreamingConfig::new(agg_map));

    let json_cfg = JsonFileIngestConfig {
        path: path.to_string_lossy().to_string(),
        metric_name: metric.to_string(),
        value_col: "bytes".to_string(),
        label_cols: vec![], // aggregate all flows in the second together
        timestamp_col: "timestamp".to_string(),
        timestamp_unit: TimestampUnit::Seconds,
        batch_size: 1024,
        batch_delay_ms: 0,
    };

    let sink = Arc::new(CapturingOutputSink::new());
    let sources: Vec<Box<dyn IngestSource>> = vec![Box::new(JsonFileIngestSource::new(json_cfg))];
    let engine = PrecomputeEngine::new(
        engine_config_grace_disabled(),
        streaming_config,
        sink.clone(),
        sources,
    );

    // The JSON source reads the file then calls broadcast_shutdown(); engine.run()
    // awaits the source and then the workers, so when it returns the shutdown
    // force-close has already emitted into the sink.
    engine.run().await.expect("engine run failed");

    let _ = std::fs::remove_file(&path);

    // --- Verify: the one-second window reached the store.
    let captured = sink.drain();
    assert_eq!(
        captured.len(),
        1,
        "the single-second NetFlow window must be emitted on shutdown (got {} outputs) — \
         before the shutdown force-close this was 0 and the store stayed empty",
        captured.len()
    );

    let (output, acc) = &captured[0];
    // A 1-second tumbling window aligned to the epoch second.
    assert_eq!(
        output.end_timestamp - output.start_timestamp,
        1_000,
        "expected a 1s window, got [{}, {})",
        output.start_timestamp,
        output.end_timestamp
    );
    assert_eq!(
        output.start_timestamp % 1_000,
        0,
        "window must be second-aligned"
    );

    let sum_acc = acc
        .as_any()
        .downcast_ref::<SumAccumulator>()
        .expect("NetFlow Sum aggregation should emit a SumAccumulator");
    assert!(
        (sum_acc.sum - expected_total_bytes).abs() < 1e-9,
        "window must hold the summed bytes of all flows in the second: expected {}, got {}",
        expected_total_bytes,
        sum_acc.sum
    );
}

//! End-to-end integration tests: precompute engine output equivalence
//! with ArroYo sketch format.
//!
//! Each test:
//!  1. Starts a PrecomputeEngine backed by a CapturingOutputSink
//!  2. Sends Prometheus remote write samples via HTTP (Snappy-compressed protobuf)
//!  3. Advances the watermark past the window boundary to close it
//!  4. Drains captured outputs and verifies equivalence with ArroYo-format accumulators

use asap_types::aggregation_config::AggregationConfig;
use asap_types::enums::{AggregationType, WindowType};
use flate2::{write::GzEncoder, Compression};
use prost::Message;
use serde_json::json;
use sketch_core::kll::KllSketch;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use query_engine_rust::data_model::{PrecomputedOutput, StreamingConfig};
use query_engine_rust::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::CapturingOutputSink;
use query_engine_rust::precompute_engine::PrecomputeEngine;
use query_engine_rust::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
use query_engine_rust::precompute_operators::multiple_sum_accumulator::MultipleSumAccumulator;

// ─── helpers ────────────────────────────────────────────────────────────────

fn make_agg_config(
    id: u64,
    metric: &str,
    agg_type: AggregationType,
    agg_sub_type: &str,
    window_secs: u64,
    slide_secs: u64,
    grouping: Vec<&str>,
) -> AggregationConfig {
    make_agg_config_full(
        id,
        metric,
        agg_type,
        agg_sub_type,
        window_secs,
        slide_secs,
        grouping,
        vec![],
    )
}

#[allow(clippy::too_many_arguments)]
fn make_agg_config_full(
    id: u64,
    metric: &str,
    agg_type: AggregationType,
    agg_sub_type: &str,
    window_secs: u64,
    slide_secs: u64,
    grouping: Vec<&str>,
    aggregated: Vec<&str>,
) -> AggregationConfig {
    let window_type = if slide_secs == 0 || slide_secs == window_secs {
        WindowType::Tumbling
    } else {
        WindowType::Sliding
    };
    AggregationConfig::new(
        id,
        agg_type,
        agg_sub_type.to_string(),
        HashMap::new(),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(
            grouping.iter().map(|s| s.to_string()).collect(),
        ),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(
            aggregated.iter().map(|s| s.to_string()).collect(),
        ),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        String::new(),
        window_secs,
        slide_secs,
        window_type,
        metric.to_string(),
        metric.to_string(),
        None,
        None,
        None,
        None,
    )
}

fn make_timeseries(
    metric: &str,
    extra_labels: Vec<(&str, &str)>,
    ts_ms: i64,
    value: f64,
) -> TimeSeries {
    let mut labels = vec![Label {
        name: "__name__".into(),
        value: metric.into(),
    }];
    for (k, v) in extra_labels {
        labels.push(Label {
            name: k.into(),
            value: v.into(),
        });
    }
    TimeSeries {
        labels,
        samples: vec![Sample {
            value,
            timestamp: ts_ms,
        }],
    }
}

fn build_remote_write_body(timeseries: Vec<TimeSeries>) -> Vec<u8> {
    let write_req = WriteRequest { timeseries };
    let proto_bytes = write_req.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&proto_bytes)
        .expect("snappy compress failed")
}

async fn send_remote_write(client: &reqwest::Client, port: u16, timeseries: Vec<TimeSeries>) {
    let body = build_remote_write_body(timeseries);
    let resp = client
        .post(format!("http://localhost:{port}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .expect("HTTP send failed");
    assert!(
        resp.status().as_u16() == 204,
        "ingest returned unexpected status {}",
        resp.status()
    );
}

fn engine_config(port: u16) -> PrecomputeEngineConfig {
    PrecomputeEngineConfig {
        num_workers: 2,
        ingest_port: port,
        allowed_lateness_ms: 0,
        max_buffer_per_series: 10_000,
        flush_interval_ms: 100,
        channel_buffer_size: 10_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
    }
}

fn gzip_hex(bytes: &[u8]) -> String {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    hex::encode(encoder.finish().unwrap())
}

// ─── test 1: DatasketchesKLL output matches ArroYo KLL ──────────────────────

/// Full e2e: send KLL samples through the HTTP ingest → PrecomputeEngine stack,
/// then verify the emitted DatasketchesKLLAccumulator matches what ArroYo's
/// KllSketch::aggregate_kll would produce for the same values.
#[tokio::test]
async fn e2e_kll_output_matches_arroyo() {
    let port = 19400u16;
    let agg_id = 1u64;
    let window_secs = 10u64;
    let k = 20u16;

    let mut kll_config = make_agg_config(
        agg_id,
        "latency",
        AggregationType::DatasketchesKLL,
        "",
        window_secs,
        0,
        vec![],
    );
    kll_config
        .parameters
        .insert("K".to_string(), serde_json::Value::from(k as u64));

    let mut agg_map = HashMap::new();
    agg_map.insert(agg_id, kll_config);
    let streaming_config = Arc::new(StreamingConfig::new(agg_map.clone()));

    let sink = Arc::new(CapturingOutputSink::new());
    let engine = PrecomputeEngine::new(engine_config(port), streaming_config, sink.clone());
    tokio::spawn(async move {
        let _ = engine.run().await;
    });
    // Wait for the HTTP server to bind
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let client = reqwest::Client::new();
    let values = [10.0f64, 20.0, 30.0];

    // Three samples inside window [0ms, 10_000ms)
    for (i, &v) in values.iter().enumerate() {
        let ts_ms = (i as i64 + 1) * 1_000;
        send_remote_write(
            &client,
            port,
            vec![make_timeseries("latency", vec![], ts_ms, v)],
        )
        .await;
    }

    // Advance watermark past window end to trigger close
    send_remote_write(
        &client,
        port,
        vec![make_timeseries("latency", vec![], 15_000, 0.0)],
    )
    .await;

    // Wait for flush
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    let captured = sink.drain();
    assert_eq!(
        captured.len(),
        1,
        "expected exactly one closed window output; got {}",
        captured.len()
    );

    let (handcrafted_output, handcrafted_acc_box) = &captured[0];
    let handcrafted_acc = handcrafted_acc_box
        .as_any()
        .downcast_ref::<DatasketchesKLLAccumulator>()
        .expect("captured accumulator should be DatasketchesKLLAccumulator");

    // Build the ArroYo-format equivalent and deserialize it
    let arroyo_bytes =
        KllSketch::aggregate_kll(k, &values).expect("KllSketch::aggregate_kll failed");
    let arroyo_json = json!({
        "aggregation_id": agg_id,
        "window": { "start": "1970-01-01T00:00:00", "end": "1970-01-01T00:00:10" },
        "key": "",
        "precompute": gzip_hex(&arroyo_bytes),
    });
    let streaming_config_for_deser = StreamingConfig::new(agg_map);
    let (_arroyo_output, arroyo_acc_box) =
        PrecomputedOutput::deserialize_from_json_arroyo(&arroyo_json, &streaming_config_for_deser)
            .expect("ArroYo KLL deserialization failed");
    let arroyo_acc = arroyo_acc_box
        .as_any()
        .downcast_ref::<DatasketchesKLLAccumulator>()
        .expect("ArroYo payload should deserialize to DatasketchesKLLAccumulator");

    // Window metadata
    assert_eq!(handcrafted_output.aggregation_id, agg_id);
    assert_eq!(handcrafted_output.start_timestamp, 0);
    assert_eq!(handcrafted_output.end_timestamp, window_secs * 1_000);

    // Sketch contents
    assert_eq!(
        handcrafted_acc.inner.k, arroyo_acc.inner.k,
        "KLL k mismatch"
    );
    assert_eq!(
        handcrafted_acc.inner.count(),
        arroyo_acc.inner.count(),
        "KLL sample count mismatch"
    );
    for q in [0.0f64, 0.25, 0.5, 0.75, 1.0] {
        assert_eq!(
            handcrafted_acc.get_quantile(q),
            arroyo_acc.get_quantile(q),
            "KLL quantile {q} mismatch"
        );
    }
}

// ─── test 2: MultipleSum output matches ArroYo MultipleSum ──────────────────

/// Full e2e: send MultipleSum samples (grouped by "host") through the HTTP
/// ingest → PrecomputeEngine stack, then verify the emitted
/// MultipleSumAccumulator matches the ArroYo MessagePack-encoded sums map.
#[tokio::test]
async fn e2e_multiple_sum_output_matches_arroyo() {
    let port = 19401u16;
    let agg_id = 2u64;
    let window_secs = 10u64;

    let config = make_agg_config_full(
        agg_id,
        "cpu",
        AggregationType::MultipleSum,
        "sum",
        window_secs,
        0,
        vec![],       // grouping: none
        vec!["host"], // aggregated: host is the key INSIDE the sketch
    );
    let mut agg_map = HashMap::new();
    agg_map.insert(agg_id, config);
    let streaming_config = Arc::new(StreamingConfig::new(agg_map.clone()));

    let sink = Arc::new(CapturingOutputSink::new());
    let engine = PrecomputeEngine::new(engine_config(port), streaming_config, sink.clone());
    tokio::spawn(async move {
        let _ = engine.run().await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let client = reqwest::Client::new();

    // Three samples for host=A inside window [0ms, 10_000ms): sum = 1+2+3 = 6
    for (ts, v) in [(1_000i64, 1.0f64), (5_000, 2.0), (9_000, 3.0)] {
        send_remote_write(
            &client,
            port,
            vec![make_timeseries("cpu", vec![("host", "A")], ts, v)],
        )
        .await;
    }

    // Advance watermark to close the window
    send_remote_write(
        &client,
        port,
        vec![make_timeseries("cpu", vec![("host", "A")], 15_000, 0.0)],
    )
    .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    let captured = sink.drain();
    assert_eq!(
        captured.len(),
        1,
        "expected one closed window output; got {}",
        captured.len()
    );

    let (handcrafted_output, handcrafted_acc_box) = &captured[0];
    let handcrafted_acc = handcrafted_acc_box
        .as_any()
        .downcast_ref::<MultipleSumAccumulator>()
        .expect("captured accumulator should be MultipleSumAccumulator");

    // Build the ArroYo-format equivalent and deserialize it
    let mut expected_sums: HashMap<String, f64> = HashMap::new();
    expected_sums.insert("A".to_string(), 6.0);
    let arroyo_bytes = rmp_serde::to_vec(&expected_sums).expect("msgpack encoding failed");
    let arroyo_json = json!({
        "aggregation_id": agg_id,
        "window": { "start": "1970-01-01T00:00:00", "end": "1970-01-01T00:00:10" },
        "key": "A",
        "precompute": gzip_hex(&arroyo_bytes),
    });
    let streaming_config_for_deser = StreamingConfig::new(agg_map);
    let (_arroyo_output, arroyo_acc_box) =
        PrecomputedOutput::deserialize_from_json_arroyo(&arroyo_json, &streaming_config_for_deser)
            .expect("ArroYo MultipleSum deserialization failed");
    let arroyo_acc = arroyo_acc_box
        .as_any()
        .downcast_ref::<MultipleSumAccumulator>()
        .expect("ArroYo payload should deserialize to MultipleSumAccumulator");

    // Window metadata
    assert_eq!(handcrafted_output.aggregation_id, agg_id);
    assert_eq!(handcrafted_output.start_timestamp, 0);
    assert_eq!(handcrafted_output.end_timestamp, window_secs * 1_000);

    // Accumulator contents
    assert_eq!(
        handcrafted_acc.sums, arroyo_acc.sums,
        "MultipleSum sums map mismatch"
    );
}

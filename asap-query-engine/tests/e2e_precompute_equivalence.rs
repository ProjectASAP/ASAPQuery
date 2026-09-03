//! End-to-end integration tests: precompute engine output equivalence
//! with the wire-format sketch encoding.
//!
//! Each test:
//!  1. Starts a PrecomputeEngine backed by a CapturingOutputSink
//!  2. Sends Prometheus remote write samples via HTTP (Snappy-compressed protobuf)
//!  3. Advances the watermark past the window boundary to close it
//!  4. Drains captured outputs and verifies equivalence with wire-format accumulators

use asap_sketchlib::KllSketch;
use asap_types::aggregation_config::AggregationConfig;
use asap_types::enums::{AggregationType, CleanupPolicy, QueryLanguage, WindowType};
use flate2::{write::GzEncoder, Compression};
use prost::Message;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use query_engine_rust::data_model::{
    AggregationReference, InferenceConfig, PrecomputedOutput, PromQLSchema, QueryConfig,
    SchemaConfig, StreamingConfig,
};
use query_engine_rust::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::CapturingOutputSink;
use query_engine_rust::precompute_engine::{
    HttpIngestConfig, HttpIngestSource, IngestSource, PrecomputeEngine,
};
use query_engine_rust::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
use query_engine_rust::precompute_operators::multiple_sum_accumulator::MultipleSumAccumulator;
use query_engine_rust::{QueryResult, SimpleEngine, SimpleMapStore, Store};

// ─── helpers ────────────────────────────────────────────────────────────────

fn make_agg_config(
    id: u64,
    metric: &str,
    agg_type: AggregationType,
    agg_sub_type: &str,
    window_size_ms: u64,
    slide_interval_ms: u64,
    grouping: Vec<&str>,
) -> AggregationConfig {
    make_agg_config_full(
        id,
        metric,
        agg_type,
        agg_sub_type,
        window_size_ms,
        slide_interval_ms,
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
    window_size_ms: u64,
    slide_interval_ms: u64,
    grouping: Vec<&str>,
    aggregated: Vec<&str>,
) -> AggregationConfig {
    let window_type = if slide_interval_ms == 0 || slide_interval_ms == window_size_ms {
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
        window_size_ms,
        slide_interval_ms,
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

fn engine_config() -> PrecomputeEngineConfig {
    PrecomputeEngineConfig {
        num_workers: 2,
        allowed_lateness_ms: 0,
        max_buffer_per_series: 10_000,
        flush_interval_ms: 100,
        channel_buffer_size: 10_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
        // Strict event-time semantics for the equivalence comparison vs Arroyo
        // (disable the wall-clock fallback so timing can't perturb output).
        wall_clock_grace_period_ms: 0,
    }
}

fn gzip_hex(bytes: &[u8]) -> String {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    hex::encode(encoder.finish().unwrap())
}

struct PromqlPrecomputeFixture<'a> {
    port: u16,
    metric: &'a str,
    query: &'a str,
    aggregation_configs: Vec<AggregationConfig>,
    schema_labels: Vec<String>,
    samples: Vec<TimeSeries>,
    evaluation_time_seconds: f64,
    base_interval_ms: u64,
}

impl PromqlPrecomputeFixture<'_> {
    async fn run(self) -> QueryResult {
        let aggregation_ids: Vec<u64> = self
            .aggregation_configs
            .iter()
            .map(|config| config.aggregation_id)
            .collect();
        let streaming_config = Arc::new(StreamingConfig::new(
            self.aggregation_configs
                .into_iter()
                .map(|config| (config.aggregation_id, config))
                .collect(),
        ));
        let sink = Arc::new(CapturingOutputSink::new());
        let engine = PrecomputeEngine::new(
            engine_config(),
            streaming_config.clone(),
            sink.clone(),
            vec![Box::new(HttpIngestSource::new(HttpIngestConfig {
                port: self.port,
            }))],
        );
        tokio::spawn(async move {
            let _ = engine.run().await;
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        let client = reqwest::Client::new();
        for sample in self.samples {
            send_remote_write(&client, self.port, vec![sample]).await;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        for (output, accumulator) in sink.drain() {
            store
                .insert_precomputed_output(output, accumulator)
                .unwrap();
        }

        let query_config = aggregation_ids.into_iter().fold(
            QueryConfig::new(self.query.to_string()),
            |config, aggregation_id| {
                config.add_aggregation(AggregationReference::new(aggregation_id, None))
            },
        );
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(PromQLSchema::new().add_metric(
                self.metric.to_string(),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(
                    self.schema_labels,
                ),
            )),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };
        let query_engine = SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            self.base_interval_ms,
            QueryLanguage::promql,
        );

        query_engine
            .handle_query_promql(self.query.to_string(), self.evaluation_time_seconds)
            .unwrap_or_else(|| panic!("precomputed query should succeed: {}", self.query))
            .1
    }
}

// ─── test 1: DatasketchesKLL output matches wire-format KLL ─────────────────

/// Full e2e: send KLL samples through the HTTP ingest → PrecomputeEngine stack,
/// then verify the emitted DatasketchesKLLAccumulator matches what the wire-format
/// KllSketch::aggregate_kll would produce for the same values.
#[tokio::test]
async fn e2e_kll_output_matches_arroyo() {
    let port = 19400u16;
    let agg_id = 1u64;
    let window_size_ms = 10_000u64;
    let k = 20u16;

    let mut kll_config = make_agg_config(
        agg_id,
        "latency",
        AggregationType::DatasketchesKLL,
        "",
        window_size_ms,
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
    let sources: Vec<Box<dyn IngestSource>> =
        vec![Box::new(HttpIngestSource::new(HttpIngestConfig { port }))];
    let engine = PrecomputeEngine::new(engine_config(), streaming_config, sink.clone(), sources);
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

    // Build the wire-format equivalent and deserialize it
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
            .expect("wire-format KLL deserialization failed");
    let arroyo_acc = arroyo_acc_box
        .as_any()
        .downcast_ref::<DatasketchesKLLAccumulator>()
        .expect("wire-format payload should deserialize to DatasketchesKLLAccumulator");

    // Window metadata
    assert_eq!(handcrafted_output.aggregation_id, agg_id);
    assert_eq!(handcrafted_output.start_timestamp, 0);
    assert_eq!(handcrafted_output.end_timestamp, window_size_ms);

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

// ─── test 2: MultipleSum output matches wire-format MultipleSum ─────────────

/// Full e2e: send MultipleSum samples (grouped by "host") through the HTTP
/// ingest → PrecomputeEngine stack, then verify the emitted
/// MultipleSumAccumulator matches the wire-format MessagePack-encoded sums map.
#[tokio::test]
async fn e2e_multiple_sum_output_matches_arroyo() {
    let port = 19401u16;
    let agg_id = 2u64;
    let window_size_ms = 10_000u64;

    let config = make_agg_config_full(
        agg_id,
        "cpu",
        AggregationType::MultipleSum,
        "sum",
        window_size_ms,
        0,
        vec![],       // grouping: none
        vec!["host"], // aggregated: host is the key INSIDE the sketch
    );
    let mut agg_map = HashMap::new();
    agg_map.insert(agg_id, config);
    let streaming_config = Arc::new(StreamingConfig::new(agg_map.clone()));

    let sink = Arc::new(CapturingOutputSink::new());
    let sources: Vec<Box<dyn IngestSource>> =
        vec![Box::new(HttpIngestSource::new(HttpIngestConfig { port }))];
    let engine = PrecomputeEngine::new(engine_config(), streaming_config, sink.clone(), sources);
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

    // Build the wire-format equivalent and deserialize it
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
            .expect("wire-format MultipleSum deserialization failed");
    let arroyo_acc = arroyo_acc_box
        .as_any()
        .downcast_ref::<MultipleSumAccumulator>()
        .expect("wire-format payload should deserialize to MultipleSumAccumulator");

    // Window metadata
    assert_eq!(handcrafted_output.aggregation_id, agg_id);
    assert_eq!(handcrafted_output.start_timestamp, 0);
    assert_eq!(handcrafted_output.end_timestamp, window_size_ms);

    // Accumulator contents
    assert_eq!(
        handcrafted_acc.sums, arroyo_acc.sums,
        "MultipleSum sums map mismatch"
    );
}

#[tokio::test]
async fn e2e_sliding_precompute_outputs_compose_a_wider_query() {
    let port = 19402u16;
    let agg_id = 3u64;
    let window_size_ms = 5_000u64;
    let slide_interval_ms = 1_000u64;
    let metric = "requests";
    let query = "sum_over_time(requests[10s])";

    let config = make_agg_config(
        agg_id,
        metric,
        AggregationType::Sum,
        "",
        window_size_ms,
        slide_interval_ms,
        vec![],
    );
    let streaming_config = Arc::new(StreamingConfig::new(HashMap::from([(agg_id, config)])));
    let sink = Arc::new(CapturingOutputSink::new());
    let engine = PrecomputeEngine::new(
        engine_config(),
        streaming_config.clone(),
        sink.clone(),
        vec![Box::new(HttpIngestSource::new(HttpIngestConfig { port }))],
    );
    tokio::spawn(async move {
        let _ = engine.run().await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let client = reqwest::Client::new();
    for second in 1..10i64 {
        send_remote_write(
            &client,
            port,
            vec![make_timeseries(
                metric,
                vec![],
                second * 1_000,
                second as f64,
            )],
        )
        .await;
    }
    send_remote_write(
        &client,
        port,
        vec![make_timeseries(metric, vec![], 15_000, 0.0)],
    )
    .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));
    for (output, accumulator) in sink.drain() {
        store
            .insert_precomputed_output(output, accumulator)
            .unwrap();
    }
    let inference_config = InferenceConfig {
        schema: SchemaConfig::PromQL(
            PromQLSchema::new().add_metric(metric.to_string(), Default::default()),
        ),
        query_configs: vec![QueryConfig::new(query.to_string())
            .add_aggregation(AggregationReference::new(agg_id, None))],
        cleanup_policy: CleanupPolicy::NoCleanup,
    };
    let query_engine = SimpleEngine::new(
        store,
        inference_config,
        streaming_config,
        slide_interval_ms,
        QueryLanguage::promql,
    );

    let (_, result) = query_engine
        .handle_query_promql(query.to_string(), 10.0)
        .expect("worker-emitted Sliding windows should answer the wider query");
    let QueryResult::Vector(vector) = result else {
        panic!("expected instant vector result");
    };

    assert_eq!(vector.values.len(), 1);
    assert_eq!(vector.values[0].value, 45.0);
}

/// Regression for #698: PromQL evaluates a range at `T` over
/// `(T - range, T]`. The sample exactly at the lower bound must be excluded,
/// the sample at `T` must be included, and a sample after `T` must not leak
/// into the result even when it has already been ingested.
#[tokio::test]
async fn e2e_promql_sum_uses_open_closed_evaluation_window() {
    let port = 19403u16;
    let agg_id = 4u64;
    let window_size_ms = 1_000u64;
    let metric = "data";
    let query = "sum(data)";

    let config = make_agg_config(
        agg_id,
        metric,
        AggregationType::Sum,
        "",
        window_size_ms,
        0,
        vec![],
    );
    let samples = [
        (1_000, 100.0),   // excluded lower bound
        (1_500, 2.0),     // included interior
        (2_000, 3.0),     // included evaluation endpoint
        (2_500, 1_000.0), // excluded future sample
        (3_500, 0.0),     // advance the watermark so every relevant window closes
    ]
    .into_iter()
    .map(|(timestamp_ms, value)| make_timeseries(metric, vec![], timestamp_ms, value))
    .collect();
    let result = PromqlPrecomputeFixture {
        port,
        metric,
        query,
        aggregation_configs: vec![config],
        schema_labels: vec![],
        samples,
        evaluation_time_seconds: 2.0,
        base_interval_ms: window_size_ms,
    }
    .run()
    .await;
    let QueryResult::Vector(vector) = result else {
        panic!("expected instant vector result");
    };

    assert_eq!(vector.values.len(), 1);
    assert_eq!(vector.values[0].value, 5.0);
}

/// The #698 boundary contract applies independently to a query's value and
/// key precomputes. An endpoint series can only appear when both sides assign
/// its sample to the window ending at the evaluation timestamp.
#[tokio::test]
async fn e2e_promql_count_uses_open_closed_value_and_key_windows() {
    let port = 19404u16;
    let value_agg_id = 5u64;
    let key_agg_id = 6u64;
    let window_size_ms = 1_000u64;
    let metric = "events";
    let query = "count(events) by (host)";

    let mut value_config = make_agg_config_full(
        value_agg_id,
        metric,
        AggregationType::CountMinSketch,
        "count",
        window_size_ms,
        0,
        vec![],
        vec!["host"],
    );
    value_config
        .parameters
        .insert("depth".to_string(), json!(3_u64));
    value_config
        .parameters
        .insert("width".to_string(), json!(128_u64));
    let key_config = make_agg_config_full(
        key_agg_id,
        metric,
        AggregationType::SetAggregator,
        "",
        window_size_ms,
        0,
        vec![],
        vec!["host"],
    );
    let samples = [
        (1_000, "lower"),
        (1_500, "interior"),
        (2_000, "endpoint"),
        (2_500, "future"),
        (3_500, "watermark"),
    ]
    .into_iter()
    .map(|(timestamp_ms, host)| make_timeseries(metric, vec![("host", host)], timestamp_ms, 1.0))
    .collect();
    let result = PromqlPrecomputeFixture {
        port,
        metric,
        query,
        aggregation_configs: vec![value_config, key_config],
        schema_labels: vec!["host".to_string()],
        samples,
        evaluation_time_seconds: 2.0,
        base_interval_ms: window_size_ms,
    }
    .run()
    .await;
    let QueryResult::Vector(vector) = result else {
        panic!("expected instant vector result");
    };
    let returned: HashMap<String, f64> = vector
        .values
        .into_iter()
        .map(|element| {
            (
                element
                    .labels
                    .labels
                    .last()
                    .expect("host label should be present")
                    .clone(),
                element.value,
            )
        })
        .collect();

    assert_eq!(
        returned,
        HashMap::from([("interior".to_string(), 1.0), ("endpoint".to_string(), 1.0)])
    );
}

/// Temporal selectors use the same `(T - range, T]` ownership as ordinary
/// instant-vector aggregations. Using q=1 makes either leaked boundary value
/// unmistakable in the result.
#[tokio::test]
async fn e2e_quantile_over_time_uses_open_closed_evaluation_window() {
    let port = 19405u16;
    let agg_id = 7u64;
    let window_size_ms = 1_000u64;
    let metric = "latency";
    let query = "quantile_over_time(1.0, latency[1s])";

    let mut config = make_agg_config(
        agg_id,
        metric,
        AggregationType::DatasketchesKLL,
        "",
        window_size_ms,
        0,
        vec![],
    );
    config.parameters.insert("K".to_string(), json!(200_u64));
    let samples = [
        (1_000, 100.0),
        (1_500, 2.0),
        (2_000, 4.0),
        (2_500, 1_000.0),
        (3_500, 0.0),
    ]
    .into_iter()
    .map(|(timestamp_ms, value)| make_timeseries(metric, vec![], timestamp_ms, value))
    .collect();
    let result = PromqlPrecomputeFixture {
        port,
        metric,
        query,
        aggregation_configs: vec![config],
        schema_labels: vec![],
        samples,
        evaluation_time_seconds: 2.0,
        base_interval_ms: window_size_ms,
    }
    .run()
    .await;
    let QueryResult::Vector(vector) = result else {
        panic!("expected instant vector result");
    };

    assert_eq!(vector.values.len(), 1);
    assert_eq!(vector.values[0].value, 4.0);
}

/// Sliding precomputes keep their existing exact-cover composition while
/// samples on every slide boundary move to the pane ending at that boundary.
/// The shared 6s boundary must be counted once, not once per stored window.
#[tokio::test]
async fn e2e_sliding_query_uses_open_closed_boundaries_without_double_counting() {
    let port = 19406u16;
    let agg_id = 8u64;
    let window_size_ms = 5_000u64;
    let slide_interval_ms = 1_000u64;
    let metric = "sliding_data";
    let query = "sum_over_time(sliding_data[10s])";

    let config = make_agg_config(
        agg_id,
        metric,
        AggregationType::Sum,
        "",
        window_size_ms,
        slide_interval_ms,
        vec![],
    );
    let samples = [
        (1_000, 100.0),    // excluded lower bound
        (2_000, 2.0),      // first stored window
        (6_000, 3.0),      // shared boundary, included only in the first window
        (11_000, 5.0),     // included evaluation endpoint
        (12_000, 1_000.0), // excluded future sample
        (20_000, 0.0),     // close all relevant windows
    ]
    .into_iter()
    .map(|(timestamp_ms, value)| make_timeseries(metric, vec![], timestamp_ms, value))
    .collect();
    let result = PromqlPrecomputeFixture {
        port,
        metric,
        query,
        aggregation_configs: vec![config],
        schema_labels: vec![],
        samples,
        evaluation_time_seconds: 11.0,
        base_interval_ms: slide_interval_ms,
    }
    .run()
    .await;
    let QueryResult::Vector(vector) = result else {
        panic!("expected instant vector result");
    };

    assert_eq!(vector.values.len(), 1);
    assert_eq!(vector.values[0].value, 10.0);
}

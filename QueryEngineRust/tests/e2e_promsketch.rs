// /// End-to-end integration test for the PromSketch pipeline.
// ///
// /// Exercises the full path: config loading → store creation → sample insertion
// /// → sketch query (instant + range) → Prometheus metrics export.
// /// Also tests the Prometheus remote write HTTP endpoint end-to-end.
// use std::sync::Arc;

// use query_engine_rust::data_model::{
//     CleanupPolicy, InferenceConfig, QueryLanguage, StreamingConfig,
// };
// use query_engine_rust::drivers::ingest::prometheus_remote_write::{
//     Label, PrometheusRemoteWriteConfig, PrometheusRemoteWriteServer, Sample, TimeSeries,
//     WriteRequest,
// };
// use query_engine_rust::engines::{QueryResult, SimpleEngine};
// use query_engine_rust::stores::promsketch_store::config::PromSketchConfig;
// use query_engine_rust::stores::promsketch_store::metrics as ps_metrics;
// use query_engine_rust::PromSketchStore;

// /// Helper: build a SimpleEngine backed by a PromSketchStore (no precomputed data).
// fn make_engine(store: Arc<PromSketchStore>) -> SimpleEngine {
//     let inference_config = InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
//     let streaming_config = Arc::new(StreamingConfig::default());
//     let map_store = Arc::new(query_engine_rust::SimpleMapStore::new(
//         streaming_config.clone(),
//         CleanupPolicy::NoCleanup,
//     ));
//     SimpleEngine::new(
//         map_store,
//         Some(store),
//         inference_config,
//         streaming_config,
//         15_000,
//         QueryLanguage::promql,
//     )
// }

// // ─── 1. YAML config round-trip ────────────────────────────────────────────

// #[test]
// fn e2e_yaml_config_loading() {
//     let yaml = r#"
// eh_univ:
//   k: 40
//   time_window: 500000
// eh_kll:
//   k: 40
//   kll_k: 200
//   time_window: 500000
// sampling:
//   sample_rate: 0.3
//   time_window: 500000
// "#;
//     let mut tmp = tempfile::NamedTempFile::new().unwrap();
//     std::io::Write::write_all(&mut tmp, yaml.as_bytes()).unwrap();

//     let config = PromSketchConfig::from_yaml_file(tmp.path().to_str().unwrap()).unwrap();
//     assert_eq!(config.eh_univ.k, 40);
//     assert_eq!(config.eh_kll.kll_k, 200);
//     assert!((config.sampling.sample_rate - 0.3).abs() < f64::EPSILON);

//     // Store can be created from the loaded config
//     let store = PromSketchStore::new(config);
//     assert_eq!(store.num_series(), 0);
// }

// // ─── 2. Ingestion + instant query ─────────────────────────────────────────

// #[test]
// fn e2e_ingest_and_instant_query() {
//     let store = Arc::new(PromSketchStore::with_default_config());

//     // Insert samples for multiple series:
//     let labels = vec![
//         r#"test_metric{instance="i0",job="j0"}"#,
//         r#"test_metric{instance="i0",job="j1"}"#,
//         r#"test_metric{instance="i1",job="j0"}"#,
//         r#"test_metric{instance="i1",job="j1"}"#,
//     ];

//     // Auto-init all sketches for each series
//     for l in &labels {
//         store.ensure_all_sketches(l).unwrap();
//     }
//     assert_eq!(store.num_series(), 4);

//     // Insert exactly 1000 samples per series: timestamps 1000..=1999, value = timestamp
//     // Exact ground truth for values 1000..=1999:
//     //   count  = 1000
//     //   sum    = 1000 + 1001 + ... + 1999 = 1_499_500
//     //   mean   = 1499.5
//     //   median = 1499.5
//     //   min    = 1000
//     //   max    = 1999
//     //   stddev = sqrt(variance) where variance = (999*1000)/12 ≈ 83250 → stddev ≈ 288.6
//     //   entropy = log2(1000) ≈ 9.9658
//     let n = 1000u64;
//     let start = 1000u64;
//     let end = start + n - 1; // 1999

//     let exact_mean = (start + end) as f64 / 2.0; // 1499.5
//     let exact_sum: f64 = (start..=end).map(|v| v as f64).sum(); // 1_499_500
//     let exact_count = n as f64; // 1000
//     let exact_min = start as f64; // 1000
//     let exact_max = end as f64; // 1999
//     let exact_entropy = (n as f64).log2(); // 9.9658

//     println!(
//         "=== Inserting {} samples per series (values {}..={}) ===",
//         n, start, end
//     );
//     println!(
//         "  Exact mean={}, sum={}, count={}, min={}, max={}",
//         exact_mean, exact_sum, exact_count, exact_min, exact_max
//     );
//     println!("  Exact entropy(log2({}))={:.4}", n, exact_entropy);

//     for l in &labels {
//         for t in start..=end {
//             store.sketch_insert(l, t, t as f64).unwrap();
//         }
//     }

//     // Build engine — query at time=2.0s with range [1s] covers timestamps 1000..2000
//     let engine = make_engine(store.clone());

//     // ── quantile_over_time (EHKLL) — p50 ──
//     println!("\n--- quantile_over_time(0.5) [EHKLL] ---");
//     let result = engine.handle_query("quantile_over_time(0.5, test_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some(), "quantile_over_time should return data");
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector, got matrix"),
//     };
//     assert_eq!(elements.len(), 4, "should have 4 series");
//     for (i, e) in elements.iter().enumerate() {
//         let err_pct = ((e.value - exact_mean) / exact_mean * 100.0).abs();
//         println!(
//             "  series[{}]: got={:.1}, expected={:.1}, error={:.2}%",
//             i, e.value, exact_mean, err_pct
//         );
//         assert!(err_pct < 10.0, "p50 error too large: {:.2}%", err_pct);
//     }

//     // ── min_over_time (EHKLL) ──
//     println!("\n--- min_over_time [EHKLL] ---");
//     let result = engine.handle_query("min_over_time(test_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some(), "min_over_time should return data");
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector, got matrix"),
//     };
//     for (i, e) in elements.iter().enumerate() {
//         let err_pct = ((e.value - exact_min) / exact_min * 100.0).abs();
//         println!(
//             "  series[{}]: got={:.1}, expected={:.1}, error={:.2}%",
//             i, e.value, exact_min, err_pct
//         );
//     }

//     // ── max_over_time (EHKLL) ──
//     println!("\n--- max_over_time [EHKLL] ---");
//     let result = engine.handle_query("max_over_time(test_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some(), "max_over_time should return data");
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector, got matrix"),
//     };
//     for (i, e) in elements.iter().enumerate() {
//         let err_pct = ((e.value - exact_max) / exact_max * 100.0).abs();
//         println!(
//             "  series[{}]: got={:.1}, expected={:.1}, error={:.2}%",
//             i, e.value, exact_max, err_pct
//         );
//     }

//     // ── avg_over_time (USampling) ──
//     println!("\n--- avg_over_time [USampling] ---");
//     let result = engine.handle_query("avg_over_time(test_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some(), "avg_over_time should return data");
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector, got matrix"),
//     };
//     for (i, e) in elements.iter().enumerate() {
//         let err_pct = ((e.value - exact_mean) / exact_mean * 100.0).abs();
//         println!(
//             "  series[{}]: got={:.2}, expected={:.1}, error={:.2}%",
//             i, e.value, exact_mean, err_pct
//         );
//     }

//     // ── count_over_time (USampling) ──
//     println!("\n--- count_over_time [USampling] ---");
//     let result = engine.handle_query("count_over_time(test_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some(), "count_over_time should return data");
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector, got matrix"),
//     };
//     for (i, e) in elements.iter().enumerate() {
//         let err_pct = ((e.value - exact_count) / exact_count * 100.0).abs();
//         println!(
//             "  series[{}]: got={:.1}, expected={:.1}, error={:.2}%",
//             i, e.value, exact_count, err_pct
//         );
//     }

//     // ── sum_over_time (USampling) ──
//     println!("\n--- sum_over_time [USampling] ---");
//     let result = engine.handle_query("sum_over_time(test_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some(), "sum_over_time should return data");
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector, got matrix"),
//     };
//     for (i, e) in elements.iter().enumerate() {
//         let err_pct = ((e.value - exact_sum) / exact_sum * 100.0).abs();
//         println!(
//             "  series[{}]: got={:.1}, expected={:.1}, error={:.2}%",
//             i, e.value, exact_sum, err_pct
//         );
//     }

//     // ── entropy_over_time (EHUniv) ──
//     println!("\n--- entropy_over_time [EHUniv] ---");
//     let result = engine.handle_query("entropy_over_time(test_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some(), "entropy_over_time should return data");
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector, got matrix"),
//     };
//     for (i, e) in elements.iter().enumerate() {
//         let err_pct = ((e.value - exact_entropy) / exact_entropy * 100.0).abs();
//         println!(
//             "  series[{}]: got={:.4}, expected={:.4}, error={:.2}%",
//             i, e.value, exact_entropy, err_pct
//         );
//         assert!(
//             e.value >= 0.0,
//             "entropy should be non-negative: {}",
//             e.value
//         );
//     }

//     println!("\n=== Instant query e2e PASSED (1000 values) ===");
// }

// // ─── 3. Range query ───────────────────────────────────────────────────────

// #[test]
// fn e2e_range_query() {
//     let store = Arc::new(PromSketchStore::with_default_config());

//     let label = r#"rq_metric{host="a"}"#;
//     store.ensure_all_sketches(label).unwrap();

//     // Insert 10000 samples at timestamps 1000..=11000
//     for t in 1000u64..=11000 {
//         store.sketch_insert(label, t, t as f64).unwrap();
//     }

//     let engine = make_engine(store);

//     // Range query: quantile_over_time over [2s..11s] step 1s, with 1s window
//     let result = engine.handle_range_query_promql(
//         "quantile_over_time(0.5, rq_metric[1s])".to_string(),
//         2.0,  // start = 2000ms
//         11.0, // end = 11000ms
//         1.0,  // step = 1000ms
//     );
//     assert!(result.is_some(), "range query should return data");
//     let (_labels, qr) = result.unwrap();
//     let matrix = match &qr {
//         QueryResult::Matrix(rv) => &rv.values,
//         QueryResult::Vector(_) => panic!("expected matrix, got vector"),
//     };
//     assert_eq!(matrix.len(), 1, "one series");
//     assert!(
//         matrix[0].samples.len() >= 5,
//         "expected >=5 time steps, got {}",
//         matrix[0].samples.len()
//     );

//     println!("=== Range query e2e PASSED ===");
//     println!(
//         "  range query returned {} time steps",
//         matrix[0].samples.len()
//     );
//     for s in &matrix[0].samples {
//         println!("    t={} v={:.1}", s.timestamp, s.value);
//     }
// }

// // ─── 4. Prometheus metrics ────────────────────────────────────────────────

// #[test]
// fn e2e_prometheus_metrics() {
//     // Force-initialize all lazy_static metrics by touching them
//     let _ = ps_metrics::SERIES_TOTAL.get();
//     let _ = ps_metrics::SAMPLES_INGESTED_TOTAL.get();
//     let _ = ps_metrics::INGEST_ERRORS_TOTAL.get();
//     ps_metrics::INGEST_BATCH_DURATION.observe(0.0);
//     let _ = ps_metrics::SKETCH_QUERIES_TOTAL
//         .with_label_values(&["hit"])
//         .get();
//     let _ = ps_metrics::SKETCH_QUERIES_TOTAL
//         .with_label_values(&["miss"])
//         .get();
//     ps_metrics::SKETCH_QUERY_DURATION.observe(0.0);

//     // Read series gauge before
//     let series_before = ps_metrics::SERIES_TOTAL.get();

//     let store = Arc::new(PromSketchStore::with_default_config());
//     store.ensure_all_sketches("metrics_test{a=\"1\"}").unwrap();
//     store.ensure_all_sketches("metrics_test{a=\"2\"}").unwrap();

//     let series_after = ps_metrics::SERIES_TOTAL.get();
//     assert!(
//         series_after >= series_before + 2.0,
//         "SERIES_TOTAL should have increased by at least 2 (before={}, after={})",
//         series_before,
//         series_after
//     );

//     // Verify Prometheus text encoding works
//     let encoder = prometheus::TextEncoder::new();
//     let families = prometheus::gather();
//     let mut buf = Vec::new();
//     prometheus::Encoder::encode(&encoder, &families, &mut buf).unwrap();
//     let text = String::from_utf8(buf).unwrap();

//     assert!(
//         text.contains("promsketch_series_total"),
//         "metrics output should contain promsketch_series_total"
//     );
//     assert!(
//         text.contains("promsketch_samples_ingested_total"),
//         "metrics output should contain promsketch_samples_ingested_total"
//     );
//     assert!(
//         text.contains("promsketch_sketch_queries_total"),
//         "metrics output should contain promsketch_sketch_queries_total"
//     );
//     assert!(
//         text.contains("promsketch_sketch_query_duration_seconds"),
//         "metrics output should contain promsketch_sketch_query_duration_seconds"
//     );

//     println!("=== Prometheus metrics e2e PASSED ===");
//     println!(
//         "  Metrics text output ({} bytes) contains all expected metric names",
//         text.len()
//     );
//     // Print a snippet of the metrics output
//     for line in text.lines().filter(|l| l.starts_with("promsketch_")) {
//         println!("  {}", line);
//     }
// }

// // ─── 5. Sketch query metrics instrumentation ─────────────────────────────

// #[test]
// fn e2e_query_metrics_instrumentation() {
//     let store = Arc::new(PromSketchStore::with_default_config());
//     store.ensure_all_sketches("qm_metric{x=\"1\"}").unwrap();
//     for t in 1000u64..=2000 {
//         store
//             .sketch_insert("qm_metric{x=\"1\"}", t, t as f64)
//             .unwrap();
//     }

//     let engine = make_engine(store);

//     let hits_before = ps_metrics::SKETCH_QUERIES_TOTAL
//         .with_label_values(&["hit"])
//         .get();
//     let miss_before = ps_metrics::SKETCH_QUERIES_TOTAL
//         .with_label_values(&["miss"])
//         .get();

//     // This should be a hit
//     let result = engine.handle_query("quantile_over_time(0.5, qm_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some());

//     let hits_after = ps_metrics::SKETCH_QUERIES_TOTAL
//         .with_label_values(&["hit"])
//         .get();
//     assert!(
//         hits_after > hits_before,
//         "hit counter should increment (before={}, after={})",
//         hits_before,
//         hits_after
//     );

//     // This should be a miss (no data for this metric)
//     let result = engine.handle_query(
//         "quantile_over_time(0.5, nonexistent_metric[1s])".to_string(),
//         2.0,
//     );
//     assert!(result.is_none());

//     let miss_after = ps_metrics::SKETCH_QUERIES_TOTAL
//         .with_label_values(&["miss"])
//         .get();
//     assert!(
//         miss_after > miss_before,
//         "miss counter should increment (before={}, after={})",
//         miss_before,
//         miss_after
//     );

//     println!("=== Query metrics instrumentation e2e PASSED ===");
//     println!("  hit counter: {} -> {}", hits_before, hits_after);
//     println!("  miss counter: {} -> {}", miss_before, miss_after);
// }

// // ─── 6. Prometheus remote write endpoint ─────────────────────────────────

// /// Helper: encode a WriteRequest into snappy-compressed protobuf bytes
// /// (the standard Prometheus remote write wire format).
// fn encode_remote_write_body(write_req: &WriteRequest) -> Vec<u8> {
//     use prost::Message;
//     let proto_bytes = write_req.encode_to_vec();
//     snap::raw::Encoder::new()
//         .compress_vec(&proto_bytes)
//         .expect("snappy compression failed")
// }

// /// Helper: build a WriteRequest with N samples for a single series.
// fn build_write_request(
//     metric_name: &str,
//     extra_labels: &[(&str, &str)],
//     start_ts: i64,
//     count: usize,
// ) -> WriteRequest {
//     let mut labels = vec![Label {
//         name: "__name__".into(),
//         value: metric_name.into(),
//     }];
//     for (k, v) in extra_labels {
//         labels.push(Label {
//             name: k.to_string(),
//             value: v.to_string(),
//         });
//     }

//     let samples: Vec<Sample> = (0..count as i64)
//         .map(|i| Sample {
//             value: (start_ts + i) as f64,
//             timestamp: start_ts + i,
//         })
//         .collect();

//     WriteRequest {
//         timeseries: vec![TimeSeries { labels, samples }],
//     }
// }

// #[tokio::test]
// async fn e2e_prometheus_remote_write_endpoint() {
//     // Pick a random-ish port to avoid conflicts with other tests
//     let port: u16 = 19123;
//     let store = Arc::new(PromSketchStore::with_default_config());

//     // Start the remote write server in the background
//     let config = PrometheusRemoteWriteConfig {
//         port,
//         auto_init_sketches: true,
//     };
//     let server = PrometheusRemoteWriteServer::new(config, Some(store.clone()));
//     let server_handle = tokio::spawn(async move {
//         let _ = server.run().await;
//     });

//     // Give the server a moment to bind
//     tokio::time::sleep(std::time::Duration::from_millis(200)).await;

//     let client = reqwest::Client::new();
//     let url = format!("http://127.0.0.1:{}/api/v1/write", port);

//     // ── Send 1000 samples via remote write ──
//     // Values 1000..=1999, same ground truth as the instant query test
//     let n = 1000usize;
//     let start = 1000i64;
//     let end = start + n as i64 - 1; // 1999

//     let exact_mean = (start + end) as f64 / 2.0; // 1499.5
//     let exact_min = start as f64;
//     let exact_max = end as f64;

//     println!(
//         "=== Sending {} samples via Prometheus remote write to port {} ===",
//         n, port
//     );

//     // Send in batches of 200 to test multiple HTTP requests
//     let batch_size = 200;
//     for batch_start in (0..n).step_by(batch_size) {
//         let batch_count = batch_size.min(n - batch_start);
//         let ts_start = start + batch_start as i64;
//         let write_req = build_write_request(
//             "prw_metric",
//             &[("host", "node1"), ("env", "test")],
//             ts_start,
//             batch_count,
//         );
//         let body = encode_remote_write_body(&write_req);

//         let resp = client
//             .post(&url)
//             .body(body)
//             .send()
//             .await
//             .expect("failed to send remote write request");

//         assert_eq!(
//             resp.status().as_u16(),
//             204,
//             "remote write should return 204 No Content, got {}",
//             resp.status()
//         );
//     }

//     println!(
//         "  Sent {} samples in {} batches of {}",
//         n,
//         n / batch_size,
//         batch_size
//     );

//     // ── Verify data landed in the store ──
//     assert!(
//         store.num_series() >= 1,
//         "store should have at least 1 series"
//     );
//     println!(
//         "  Store has {} series after remote write ingestion",
//         store.num_series()
//     );

//     // ── Query via engine and check accuracy ──
//     let engine = make_engine(store.clone());

//     // quantile_over_time(0.5) — p50
//     println!("\n--- quantile_over_time(0.5) via remote write [EHKLL] ---");
//     let result = engine.handle_query("quantile_over_time(0.5, prw_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some(), "quantile query should return data");
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector, got matrix"),
//     };
//     assert_eq!(elements.len(), 1, "should have 1 series");
//     let err_pct = ((elements[0].value - exact_mean) / exact_mean * 100.0).abs();
//     println!(
//         "  p50: got={:.1}, expected={:.1}, error={:.2}%",
//         elements[0].value, exact_mean, err_pct
//     );
//     assert!(err_pct < 10.0, "p50 error too large: {:.2}%", err_pct);

//     // min_over_time
//     println!("\n--- min_over_time via remote write [EHKLL] ---");
//     let result = engine.handle_query("min_over_time(prw_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some());
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector"),
//     };
//     let err_pct = ((elements[0].value - exact_min) / exact_min * 100.0).abs();
//     println!(
//         "  min: got={:.1}, expected={:.1}, error={:.2}%",
//         elements[0].value, exact_min, err_pct
//     );

//     // max_over_time
//     println!("\n--- max_over_time via remote write [EHKLL] ---");
//     let result = engine.handle_query("max_over_time(prw_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some());
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector"),
//     };
//     let err_pct = ((elements[0].value - exact_max) / exact_max * 100.0).abs();
//     println!(
//         "  max: got={:.1}, expected={:.1}, error={:.2}%",
//         elements[0].value, exact_max, err_pct
//     );

//     // avg_over_time
//     println!("\n--- avg_over_time via remote write [USampling] ---");
//     let result = engine.handle_query("avg_over_time(prw_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some());
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector"),
//     };
//     let err_pct = ((elements[0].value - exact_mean) / exact_mean * 100.0).abs();
//     println!(
//         "  avg: got={:.2}, expected={:.1}, error={:.2}%",
//         elements[0].value, exact_mean, err_pct
//     );

//     // entropy_over_time
//     let exact_entropy = (n as f64).log2();
//     println!("\n--- entropy_over_time via remote write [EHUniv] ---");
//     let result = engine.handle_query("entropy_over_time(prw_metric[1s])".to_string(), 2.0);
//     assert!(result.is_some());
//     let (_labels, qr) = result.unwrap();
//     let elements = match &qr {
//         QueryResult::Vector(iv) => &iv.values,
//         QueryResult::Matrix(_) => panic!("expected vector"),
//     };
//     let err_pct = ((elements[0].value - exact_entropy) / exact_entropy * 100.0).abs();
//     println!(
//         "  entropy: got={:.4}, expected={:.4}, error={:.2}%",
//         elements[0].value, exact_entropy, err_pct
//     );

//     // ── Test error handling: send garbage body ──
//     println!("\n--- Error handling: invalid body ---");
//     let resp = client
//         .post(&url)
//         .body(b"not-valid-snappy-protobuf".to_vec())
//         .send()
//         .await
//         .expect("request should succeed at HTTP level");
//     assert_eq!(
//         resp.status().as_u16(),
//         400,
//         "invalid body should return 400 Bad Request"
//     );
//     println!("  Invalid body correctly returned 400");

//     // Cleanup
//     server_handle.abort();
//     println!("\n=== Prometheus remote write endpoint e2e PASSED ===");
// }

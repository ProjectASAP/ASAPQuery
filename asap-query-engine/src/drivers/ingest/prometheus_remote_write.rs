use prost::Message;

// use axum::{body::Bytes, extract::State, http::StatusCode, routing::post, Router};
// use std::sync::Arc;
// use tokio::net::TcpListener;
// use tracing::{debug, error, info, warn};

// // use crate::stores::promsketch_store::metrics as ps_metrics;
// // use crate::stores::promsketch_store::PromSketchStore;

// ---------------------------------------------------------------------------
// Protobuf message types (Prometheus remote write wire format)
// ---------------------------------------------------------------------------
// These mirror the upstream proto definitions in prometheus/prompb but are
// defined inline via prost derive macros so we don't need a .proto file or
// build script.

#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

// ---------------------------------------------------------------------------
// Label helpers
// ---------------------------------------------------------------------------

/// Convert a slice of Prometheus [`Label`] pairs into the canonical
/// `metric_name{key1="val1",key2="val2"}` string format.
///
/// The `__name__` label becomes the metric name prefix; the remaining labels
/// are sorted alphabetically by name.
pub fn labels_to_string(labels: &[Label]) -> String {
    let mut name: Option<&str> = None;
    let mut rest: Vec<(&str, &str)> = Vec::new();

    for l in labels {
        if l.name == "__name__" {
            name = Some(&l.value);
        } else {
            rest.push((&l.name, &l.value));
        }
    }

    rest.sort_by(|a, b| a.0.cmp(b.0));

    let metric = name.unwrap_or("");

    if rest.is_empty() {
        return metric.to_string();
    }

    let mut out = String::with_capacity(metric.len() + 2 + rest.len() * 16);
    out.push_str(metric);
    out.push('{');
    for (i, (k, v)) in rest.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(k);
        out.push_str("=\"");
        out.push_str(v);
        out.push('"');
    }
    out.push('}');
    out
}

// ---------------------------------------------------------------------------
// Decoded sample — the output of this driver
// ---------------------------------------------------------------------------

/// A single decoded sample ready for downstream consumption.
#[derive(Debug, Clone)]
pub struct DecodedSample {
    pub labels: String,
    pub timestamp_ms: i64,
    pub value: f64,
}

// ---------------------------------------------------------------------------
// Decode helpers
// ---------------------------------------------------------------------------

/// Snappy-decompress and protobuf-decode a raw Prometheus remote write body
/// into a flat list of [`DecodedSample`]s.
pub fn decode_prometheus_remote_write(
    body: &[u8],
) -> Result<Vec<DecodedSample>, PrometheusRemoteWriteError> {
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| PrometheusRemoteWriteError::SnappyDecompress(e.to_string()))?;

    let write_req = WriteRequest::decode(decompressed.as_slice())
        .map_err(|e| PrometheusRemoteWriteError::ProtobufDecode(e.to_string()))?;

    let mut samples = Vec::new();
    for ts in &write_req.timeseries {
        let labels_str = labels_to_string(&ts.labels);
        for s in &ts.samples {
            samples.push(DecodedSample {
                labels: labels_str.clone(),
                timestamp_ms: s.timestamp,
                value: s.value,
            });
        }
    }
    Ok(samples)
}

#[derive(Debug, thiserror::Error)]
pub enum PrometheusRemoteWriteError {
    #[error("snappy decompression failed: {0}")]
    SnappyDecompress(String),
    #[error("protobuf decode failed: {0}")]
    ProtobufDecode(String),
}

// // ---------------------------------------------------------------------------
// // Config
// // ---------------------------------------------------------------------------

// #[derive(Debug, Clone)]
// pub struct PrometheusRemoteWriteConfig {
//     pub port: u16,
//     pub auto_init_sketches: bool,
// }

// impl Default for PrometheusRemoteWriteConfig {
//     fn default() -> Self {
//         Self {
//             port: 9090,
//             auto_init_sketches: true,
//         }
//     }
// }

// // ---------------------------------------------------------------------------
// // Server
// // ---------------------------------------------------------------------------

// /// Shared state accessible by axum handlers.
// struct ServerState {
//     /// Running counter of ingested samples (for logging).
//     samples_ingested: std::sync::atomic::AtomicU64,
//     /// Optional PromSketchStore for sketch-based ingestion.
//     promsketch_store: Option<Arc<PromSketchStore>>,
//     /// Whether to auto-initialize all sketch types for new series.
//     auto_init_sketches: bool,
// }

// /// A standalone HTTP server that accepts Prometheus remote write requests.
// ///
// /// Decoded samples are logged at debug level. To wire them into a downstream
// /// store, extend the handler or wrap this driver with a callback.
// pub struct PrometheusRemoteWriteServer {
//     config: PrometheusRemoteWriteConfig,
//     promsketch_store: Option<Arc<PromSketchStore>>,
// }

// impl PrometheusRemoteWriteServer {
//     pub fn new(
//         config: PrometheusRemoteWriteConfig,
//         promsketch_store: Option<Arc<PromSketchStore>>,
//     ) -> Self {
//         Self {
//             config,
//             promsketch_store,
//         }
//     }

//     /// Start the server. Blocks until the listener is dropped or an error occurs.
//     pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         let state = Arc::new(ServerState {
//             samples_ingested: std::sync::atomic::AtomicU64::new(0),
//             promsketch_store: self.promsketch_store.clone(),
//             auto_init_sketches: self.config.auto_init_sketches,
//         });

//         let app = Router::new()
//             .route("/api/v1/write", post(handle_prometheus_remote_write))
//             .with_state(state);

//         let addr = format!("0.0.0.0:{}", self.config.port);
//         info!("Prometheus remote write server listening on {}", addr);

//         let listener = TcpListener::bind(&addr).await?;
//         axum::serve(listener, app).await?;
//         Ok(())
//     }
// }

// async fn handle_prometheus_remote_write(
//     State(state): State<Arc<ServerState>>,
//     body: Bytes,
// ) -> StatusCode {
//     let samples = match decode_prometheus_remote_write(&body) {
//         Ok(s) => s,
//         Err(e) => {
//             warn!("Failed to decode Prometheus remote write request: {e}");
//             return StatusCode::BAD_REQUEST;
//         }
//     };

//     let count = samples.len() as u64;
//     let total = state
//         .samples_ingested
//         .fetch_add(count, std::sync::atomic::Ordering::Relaxed)
//         + count;

//     debug!("Received {} samples ({} total ingested)", count, total);

//     if let Some(ref store) = state.promsketch_store {
//         let mut errors = 0u64;
//         for s in &samples {
//             debug!("  {} t={} v={}", s.labels, s.timestamp_ms, s.value);

//             if state.auto_init_sketches && store.get_or_create(&s.labels) {
//                 if let Err(e) = store.ensure_all_sketches(&s.labels) {
//                     error!("Failed to init sketches for {}: {e}", s.labels);
//                     errors += 1;
//                     continue;
//                 }
//             }

//             if let Err(e) = store.sketch_insert(&s.labels, s.timestamp_ms as u64, s.value) {
//                 error!("Failed to insert sample for {}: {e}", s.labels);
//                 errors += 1;
//             }
//         }
//         ps_metrics::SAMPLES_INGESTED_TOTAL.inc_by((count - errors) as f64);
//         if errors > 0 {
//             ps_metrics::INGEST_ERRORS_TOTAL.inc_by(errors as f64);
//         }
//     } else {
//         for s in &samples {
//             debug!("  {} t={} v={}", s.labels, s.timestamp_ms, s.value);
//         }
//     }

//     StatusCode::NO_CONTENT
// }

// // ---------------------------------------------------------------------------
// // Tests
// // ---------------------------------------------------------------------------

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_labels_to_string_basic() {
//         let labels = vec![
//             Label {
//                 name: "__name__".into(),
//                 value: "http_requests_total".into(),
//             },
//             Label {
//                 name: "method".into(),
//                 value: "GET".into(),
//             },
//             Label {
//                 name: "status".into(),
//                 value: "200".into(),
//             },
//         ];
//         assert_eq!(
//             labels_to_string(&labels),
//             "http_requests_total{method=\"GET\",status=\"200\"}"
//         );
//     }

//     #[test]
//     fn test_labels_to_string_sorted() {
//         let labels = vec![
//             Label {
//                 name: "__name__".into(),
//                 value: "cpu_usage".into(),
//             },
//             Label {
//                 name: "zone".into(),
//                 value: "us-east".into(),
//             },
//             Label {
//                 name: "host".into(),
//                 value: "node1".into(),
//             },
//             Label {
//                 name: "app".into(),
//                 value: "web".into(),
//             },
//         ];
//         assert_eq!(
//             labels_to_string(&labels),
//             "cpu_usage{app=\"web\",host=\"node1\",zone=\"us-east\"}"
//         );
//     }

//     #[test]
//     fn test_labels_to_string_no_name() {
//         let labels = vec![Label {
//             name: "host".into(),
//             value: "node1".into(),
//         }];
//         assert_eq!(labels_to_string(&labels), "{host=\"node1\"}");
//     }

//     #[test]
//     fn test_labels_to_string_name_only() {
//         let labels = vec![Label {
//             name: "__name__".into(),
//             value: "up".into(),
//         }];
//         assert_eq!(labels_to_string(&labels), "up");
//     }

//     #[test]
//     fn test_labels_to_string_empty() {
//         let labels: Vec<Label> = vec![];
//         assert_eq!(labels_to_string(&labels), "");
//     }

//     #[test]
//     fn test_protobuf_roundtrip() {
//         let write_req = WriteRequest {
//             timeseries: vec![TimeSeries {
//                 labels: vec![
//                     Label {
//                         name: "__name__".into(),
//                         value: "test_metric".into(),
//                     },
//                     Label {
//                         name: "env".into(),
//                         value: "prod".into(),
//                     },
//                 ],
//                 samples: vec![
//                     Sample {
//                         value: 1.5,
//                         timestamp: 1000,
//                     },
//                     Sample {
//                         value: 2.5,
//                         timestamp: 2000,
//                     },
//                 ],
//             }],
//         };

//         let encoded = write_req.encode_to_vec();
//         let decoded = WriteRequest::decode(encoded.as_slice()).unwrap();
//         assert_eq!(write_req, decoded);
//     }

//     #[test]
//     fn test_snappy_protobuf_decode() {
//         let write_req = WriteRequest {
//             timeseries: vec![TimeSeries {
//                 labels: vec![
//                     Label {
//                         name: "__name__".into(),
//                         value: "metric_a".into(),
//                     },
//                     Label {
//                         name: "job".into(),
//                         value: "test".into(),
//                     },
//                 ],
//                 samples: vec![Sample {
//                     value: 42.0,
//                     timestamp: 1700000000000,
//                 }],
//             }],
//         };

//         let proto_bytes = write_req.encode_to_vec();
//         let compressed = snap::raw::Encoder::new()
//             .compress_vec(&proto_bytes)
//             .unwrap();

//         let samples = decode_prometheus_remote_write(&compressed).unwrap();
//         assert_eq!(samples.len(), 1);
//         assert_eq!(samples[0].labels, "metric_a{job=\"test\"}");
//         assert_eq!(samples[0].timestamp_ms, 1700000000000);
//         assert!((samples[0].value - 42.0).abs() < f64::EPSILON);
//     }

//     #[test]
//     fn test_decode_multiple_timeseries() {
//         let write_req = WriteRequest {
//             timeseries: vec![
//                 TimeSeries {
//                     labels: vec![
//                         Label {
//                             name: "__name__".into(),
//                             value: "cpu".into(),
//                         },
//                         Label {
//                             name: "host".into(),
//                             value: "a".into(),
//                         },
//                     ],
//                     samples: vec![
//                         Sample {
//                             value: 0.5,
//                             timestamp: 100,
//                         },
//                         Sample {
//                             value: 0.6,
//                             timestamp: 200,
//                         },
//                     ],
//                 },
//                 TimeSeries {
//                     labels: vec![
//                         Label {
//                             name: "__name__".into(),
//                             value: "mem".into(),
//                         },
//                         Label {
//                             name: "host".into(),
//                             value: "b".into(),
//                         },
//                     ],
//                     samples: vec![Sample {
//                         value: 1024.0,
//                         timestamp: 100,
//                     }],
//                 },
//             ],
//         };

//         let proto_bytes = write_req.encode_to_vec();
//         let compressed = snap::raw::Encoder::new()
//             .compress_vec(&proto_bytes)
//             .unwrap();

//         let samples = decode_prometheus_remote_write(&compressed).unwrap();
//         assert_eq!(samples.len(), 3);
//         assert_eq!(samples[0].labels, "cpu{host=\"a\"}");
//         assert_eq!(samples[1].labels, "cpu{host=\"a\"}");
//         assert_eq!(samples[2].labels, "mem{host=\"b\"}");
//     }

//     #[test]
//     fn test_decode_invalid_snappy() {
//         let result = decode_prometheus_remote_write(b"not-snappy-data");
//         assert!(result.is_err());
//         assert!(matches!(
//             result.unwrap_err(),
//             PrometheusRemoteWriteError::SnappyDecompress(_)
//         ));
//     }

//     #[test]
//     fn test_decode_invalid_protobuf() {
//         // Valid snappy wrapping invalid protobuf
//         let garbage = b"this is not protobuf";
//         let compressed = snap::raw::Encoder::new().compress_vec(garbage).unwrap();

//         let result = decode_prometheus_remote_write(&compressed);
//         assert!(result.is_err());
//         assert!(matches!(
//             result.unwrap_err(),
//             PrometheusRemoteWriteError::ProtobufDecode(_)
//         ));
//     }
// }

// use axum::{body::Bytes, extract::State, http::StatusCode, routing::post, Router};
// use prost::Message;
// use std::sync::Arc;
// use tokio::net::TcpListener;
// use tracing::{debug, info, warn};

// use super::prometheus_remote_write::{labels_to_string, DecodedSample, WriteRequest};

// // ---------------------------------------------------------------------------
// // Config
// // ---------------------------------------------------------------------------

// /// Configuration for the VictoriaMetrics remote write ingest endpoint.
// ///
// /// This is a thin wrapper around the shared remote write decoder that always
// /// uses zstd compression (VictoriaMetrics remote write protocol).
// #[derive(Debug, Clone)]
// pub struct VictoriaMetricsRemoteWriteConfig {
//     pub port: u16,
// }

use super::prometheus_remote_write::{labels_to_string, DecodedSample, WriteRequest};
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub enum VictoriaMetricsRemoteWriteError {
    #[error("zstd decompression failed: {0}")]
    ZstdDecompress(String),
    #[error("protobuf decode failed: {0}")]
    ProtobufDecode(String),
}

/// Decode zstd-compressed VictoriaMetrics remote-write body into flat samples.
pub fn decode_victoriametrics_remote_write(
    body: &[u8],
) -> Result<Vec<DecodedSample>, VictoriaMetricsRemoteWriteError> {
    let decompressed = zstd::decode_all(body)
        .map_err(|e| VictoriaMetricsRemoteWriteError::ZstdDecompress(e.to_string()))?;

    let write_req = WriteRequest::decode(decompressed.as_slice())
        .map_err(|e| VictoriaMetricsRemoteWriteError::ProtobufDecode(e.to_string()))?;

    let mut out = Vec::new();
    for ts in &write_req.timeseries {
        let labels_str = labels_to_string(&ts.labels);
        for sample in &ts.samples {
            out.push(DecodedSample {
                labels: labels_str.clone(),
                timestamp_ms: sample.timestamp,
                value: sample.value,
            });
        }
    }

    Ok(out)
}

// impl Default for VictoriaMetricsRemoteWriteConfig {
//     fn default() -> Self {
//         // VictoriaMetrics commonly uses 8428, but the caller can override this.
//         Self { port: 8428 }
//     }
// }

// // ---------------------------------------------------------------------------
// // Server
// // ---------------------------------------------------------------------------

// /// Shared state accessible by axum handlers.
// struct ServerState {
//     /// Running counter of ingested samples (for logging).
//     samples_ingested: std::sync::atomic::AtomicU64,
// }

// /// A standalone HTTP server that accepts VictoriaMetrics remote write requests.
// ///
// /// This server listens on `VictoriaMetricsRemoteWriteConfig::port` and exposes
// /// a `POST /api/v1/write` endpoint that expects zstd-compressed protobuf
// /// `WriteRequest` bodies, matching the VictoriaMetrics remote write protocol.
// ///
// /// The decoded samples are logged at debug level. To integrate with a
// /// downstream store or precompute engine, extend the handler or wrap this
// /// server with a callback.
// pub struct VictoriaMetricsRemoteWriteServer {
//     config: VictoriaMetricsRemoteWriteConfig,
// }

// impl VictoriaMetricsRemoteWriteServer {
//     pub fn new(config: VictoriaMetricsRemoteWriteConfig) -> Self {
//         Self { config }
//     }

//     /// Start the server. Blocks until the listener is dropped or an error occurs.
//     pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//         let state = Arc::new(ServerState {
//             samples_ingested: std::sync::atomic::AtomicU64::new(0),
//         });

//         let app = Router::new()
//             .route("/api/v1/write", post(handle_victoriametrics_remote_write))
//             .with_state(state);

//         let addr = format!("0.0.0.0:{}", self.config.port);
//         info!(
//             "VictoriaMetrics remote write server listening on {} (zstd compression only)",
//             addr
//         );

//         let listener = TcpListener::bind(&addr).await?;
//         axum::serve(listener, app).await?;
//         Ok(())
//     }
// }

// async fn handle_victoriametrics_remote_write(
//     State(state): State<Arc<ServerState>>,
//     body: Bytes,
// ) -> StatusCode {
//     let samples: Vec<DecodedSample> = match decode_victoriametrics_remote_write(&body) {
//         Ok(s) => s,
//         Err(VictoriaMetricsRemoteWriteError::ZstdDecompress(e)) => {
//             warn!("Failed to zstd-decompress VictoriaMetrics remote write request: {e}");
//             return StatusCode::BAD_REQUEST;
//         }
//         Err(VictoriaMetricsRemoteWriteError::ProtobufDecode(e)) => {
//             warn!("Failed to decode VictoriaMetrics remote write protobuf: {e}");
//             return StatusCode::BAD_REQUEST;
//         }
//     };

//     let count = samples.len() as u64;
//     let total = state
//         .samples_ingested
//         .fetch_add(count, std::sync::atomic::Ordering::Relaxed)
//         + count;

//     debug!(
//         "Received {} VictoriaMetrics samples ({} total ingested)",
//         count, total
//     );
//     for s in &samples {
//         debug!("  {} t={} v={}", s.labels, s.timestamp_ms, s.value);
//     }

//     StatusCode::NO_CONTENT
// }

// // ---------------------------------------------------------------------------
// // Decode helpers
// // ---------------------------------------------------------------------------

// #[derive(Debug, thiserror::Error)]
// pub enum VictoriaMetricsRemoteWriteError {
//     #[error("zstd decompression failed: {0}")]
//     ZstdDecompress(String),
//     #[error("protobuf decode failed: {0}")]
//     ProtobufDecode(String),
// }

// /// Zstd-decompress and protobuf-decode a raw VictoriaMetrics remote write body
// /// into a flat list of [`DecodedSample`]s.
// pub fn decode_victoriametrics_remote_write(
//     body: &[u8],
// ) -> Result<Vec<DecodedSample>, VictoriaMetricsRemoteWriteError> {
//     let decompressed = zstd::decode_all(body)
//         .map_err(|e| VictoriaMetricsRemoteWriteError::ZstdDecompress(e.to_string()))?;

//     let write_req = WriteRequest::decode(decompressed.as_slice())
//         .map_err(|e| VictoriaMetricsRemoteWriteError::ProtobufDecode(e.to_string()))?;

//     let mut samples = Vec::new();
//     for ts in &write_req.timeseries {
//         let labels_str = labels_to_string(&ts.labels);
//         for s in &ts.samples {
//             samples.push(DecodedSample {
//                 labels: labels_str.clone(),
//                 timestamp_ms: s.timestamp,
//                 value: s.value,
//             });
//         }
//     }
//     Ok(samples)
// }

// // ---------------------------------------------------------------------------
// // Tests
// // ---------------------------------------------------------------------------

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::drivers::ingest::prometheus_remote_write::{
//         Label, Sample, TimeSeries, WriteRequest,
//     };

//     #[test]
//     fn test_zstd_decode_single_sample() {
//         let write_req = WriteRequest {
//             timeseries: vec![TimeSeries {
//                 labels: vec![
//                     Label {
//                         name: "__name__".into(),
//                         value: "vm_metric".into(),
//                     },
//                     Label {
//                         name: "region".into(),
//                         value: "us-east-1".into(),
//                     },
//                 ],
//                 samples: vec![Sample {
//                     value: 99.9,
//                     timestamp: 1700000000000,
//                 }],
//             }],
//         };

//         let proto_bytes = write_req.encode_to_vec();
//         let compressed = zstd::encode_all(proto_bytes.as_slice(), 0).unwrap();

//         let samples = decode_victoriametrics_remote_write(&compressed).unwrap();
//         assert_eq!(samples.len(), 1);
//         assert_eq!(samples[0].labels, "vm_metric{region=\"us-east-1\"}");
//         assert_eq!(samples[0].timestamp_ms, 1700000000000);
//         assert!((samples[0].value - 99.9).abs() < f64::EPSILON);
//     }

//     #[test]
//     fn test_decode_invalid_zstd() {
//         let result = decode_victoriametrics_remote_write(b"not-zstd-data");
//         assert!(result.is_err());
//         assert!(matches!(
//             result.unwrap_err(),
//             VictoriaMetricsRemoteWriteError::ZstdDecompress(_)
//         ));
//     }
// }

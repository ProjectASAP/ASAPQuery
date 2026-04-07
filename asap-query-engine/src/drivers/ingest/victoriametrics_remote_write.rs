use prost::Message;

use super::prometheus_remote_write::{labels_to_string, DecodedSample, WriteRequest};

// ---------------------------------------------------------------------------
// Decode helpers
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum VictoriaMetricsRemoteWriteError {
    #[error("zstd decompression failed: {0}")]
    ZstdDecompress(String),
    #[error("protobuf decode failed: {0}")]
    ProtobufDecode(String),
}

/// Zstd-decompress and protobuf-decode a raw VictoriaMetrics remote write body
/// into a flat list of [`DecodedSample`]s.
pub fn decode_victoriametrics_remote_write(
    body: &[u8],
) -> Result<Vec<DecodedSample>, VictoriaMetricsRemoteWriteError> {
    let decompressed = zstd::decode_all(body)
        .map_err(|e| VictoriaMetricsRemoteWriteError::ZstdDecompress(e.to_string()))?;

    let write_req = WriteRequest::decode(decompressed.as_slice())
        .map_err(|e| VictoriaMetricsRemoteWriteError::ProtobufDecode(e.to_string()))?;

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drivers::ingest::prometheus_remote_write::{
        Label, Sample, TimeSeries, WriteRequest,
    };

    #[test]
    fn test_zstd_decode_single_sample() {
        let write_req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".into(),
                        value: "vm_metric".into(),
                    },
                    Label {
                        name: "region".into(),
                        value: "us-east-1".into(),
                    },
                ],
                samples: vec![Sample {
                    value: 99.9,
                    timestamp: 1700000000000,
                }],
            }],
        };

        let proto_bytes = write_req.encode_to_vec();
        let compressed = zstd::encode_all(proto_bytes.as_slice(), 0).unwrap();

        let samples = decode_victoriametrics_remote_write(&compressed).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels, "vm_metric{region=\"us-east-1\"}");
        assert_eq!(samples[0].timestamp_ms, 1700000000000);
        assert!((samples[0].value - 99.9).abs() < f64::EPSILON);
    }

    #[test]
    fn test_decode_invalid_zstd() {
        let result = decode_victoriametrics_remote_write(b"not-zstd-data");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VictoriaMetricsRemoteWriteError::ZstdDecompress(_)
        ));
    }
}

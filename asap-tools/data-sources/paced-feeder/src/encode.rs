//! Wire encoders for the two sinks. Each series' labels are encoded once up
//! front, so the per-sample cost at high rates is just the value and timestamp.

use crate::workload::{Row, Workload};
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, WireType};

pub trait Encoder: Send + Sync {
    fn encode(&self, rows: &[Row]) -> Vec<u8>;
}

/// Prometheus remote write: a snappy-compressed `WriteRequest` with one
/// `TimeSeries` (labels + one sample) per row.
pub struct RemoteWriteEncoder {
    /// Encoded `Label` fields (`TimeSeries` field 1) for each series, sorted by
    /// label name as Prometheus requires: `__name__`, `instance`, `label_0`.
    series_labels: Vec<Vec<u8>>,
}

impl RemoteWriteEncoder {
    pub fn new(workload: &Workload, metric: &str) -> Self {
        let series_labels = (0..workload.num_series())
            .map(|series| {
                let (label_0, instance) = workload.series_labels(series);
                let mut buf = Vec::new();
                for (name, value) in [
                    ("__name__", metric),
                    ("instance", instance.as_str()),
                    ("label_0", label_0.as_str()),
                ] {
                    let mut label = Vec::new();
                    encode_bytes_field(1, name.as_bytes(), &mut label);
                    encode_bytes_field(2, value.as_bytes(), &mut label);
                    encode_bytes_field(1, &label, &mut buf);
                }
                buf
            })
            .collect();
        Self { series_labels }
    }
}

impl Encoder for RemoteWriteEncoder {
    fn encode(&self, rows: &[Row]) -> Vec<u8> {
        let mut request = Vec::new();
        let mut series = Vec::new();
        let mut sample = Vec::new();
        for row in rows {
            sample.clear();
            encode_key(1, WireType::SixtyFourBit, &mut sample);
            sample.extend_from_slice(&row.value.to_le_bytes());
            encode_key(2, WireType::Varint, &mut sample);
            encode_varint(row.timestamp_ms as u64, &mut sample);

            series.clear();
            series.extend_from_slice(&self.series_labels[row.series]);
            encode_bytes_field(2, &sample, &mut series);

            encode_bytes_field(1, &series, &mut request);
        }
        snap::raw::Encoder::new()
            .compress_vec(&request)
            .expect("snappy compression of an in-memory buffer cannot fail")
    }
}

/// ClickHouse `RowBinary` for columns `(ts DateTime64(3), label_0 String,
/// instance String, value Float64)`.
pub struct RowBinaryEncoder {
    /// `label_0` then `instance`, each as a varint length plus bytes.
    series_labels: Vec<Vec<u8>>,
}

/// The column list `RowBinaryEncoder` writes, in order.
pub const ROW_BINARY_COLUMNS: &str = "ts, label_0, instance, value";

impl RowBinaryEncoder {
    pub fn new(workload: &Workload) -> Self {
        let series_labels = (0..workload.num_series())
            .map(|series| {
                let (label_0, instance) = workload.series_labels(series);
                let mut buf = Vec::new();
                for s in [label_0, instance] {
                    encode_varint(s.len() as u64, &mut buf);
                    buf.extend_from_slice(s.as_bytes());
                }
                buf
            })
            .collect();
        Self { series_labels }
    }
}

impl Encoder for RowBinaryEncoder {
    fn encode(&self, rows: &[Row]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(rows.len() * 32);
        for row in rows {
            buf.extend_from_slice(&row.timestamp_ms.to_le_bytes());
            buf.extend_from_slice(&self.series_labels[row.series]);
            buf.extend_from_slice(&row.value.to_le_bytes());
        }
        buf
    }
}

fn encode_bytes_field(tag: u32, bytes: &[u8], buf: &mut Vec<u8>) {
    encode_key(tag, WireType::LengthDelimited, buf);
    buf.reserve(encoded_len_varint(bytes.len() as u64) + bytes.len());
    encode_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    // Mirrors the remote write types in
    // asap-query-engine/src/drivers/ingest/prometheus_remote_write.rs.
    #[derive(Clone, PartialEq, Message)]
    struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        timeseries: Vec<TimeSeries>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct TimeSeries {
        #[prost(message, repeated, tag = "1")]
        labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        samples: Vec<Sample>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Label {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(string, tag = "2")]
        value: String,
    }
    #[derive(Clone, PartialEq, Message)]
    struct Sample {
        #[prost(double, tag = "1")]
        value: f64,
        #[prost(int64, tag = "2")]
        timestamp: i64,
    }

    fn workload() -> Workload {
        Workload {
            seed: 1,
            groups: 2,
            series_per_group: 3,
            samples_per_sec: 1,
            pareto_shape: 1.5,
            pareto_scale: 1.0,
        }
    }

    #[test]
    fn remote_write_decodes_to_one_series_per_row() {
        let w = workload();
        let rows = w.tick_rows(2, 1_700_000_000_000);
        let body = RemoteWriteEncoder::new(&w, "data").encode(&rows);

        let raw = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
        let request = WriteRequest::decode(raw.as_slice()).unwrap();
        assert_eq!(request.timeseries.len(), rows.len());
        for (ts, row) in request.timeseries.iter().zip(&rows) {
            let (label_0, instance) = w.series_labels(row.series);
            let labels: Vec<(&str, &str)> = ts
                .labels
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect();
            assert_eq!(
                labels,
                vec![
                    ("__name__", "data"),
                    ("instance", instance.as_str()),
                    ("label_0", label_0.as_str()),
                ]
            );
            assert_eq!(
                ts.samples,
                vec![Sample {
                    value: row.value,
                    timestamp: row.timestamp_ms
                }]
            );
        }
    }

    #[test]
    fn row_binary_round_trips_every_column() {
        let w = workload();
        let rows = w.tick_rows(0, 42_000);
        let buf = RowBinaryEncoder::new(&w).encode(&rows);

        let mut cursor = buf.as_slice();
        let mut take = |n: usize| -> Vec<u8> {
            let (head, rest) = cursor.split_at(n);
            cursor = rest;
            head.to_vec()
        };
        for row in &rows {
            let ts = i64::from_le_bytes(take(8).try_into().unwrap());
            // Labels are short, so their varint length is one byte.
            let label_0_len = take(1)[0] as usize;
            let label_0 = String::from_utf8(take(label_0_len)).unwrap();
            let instance_len = take(1)[0] as usize;
            let instance = String::from_utf8(take(instance_len)).unwrap();
            let value = f64::from_le_bytes(take(8).try_into().unwrap());
            assert_eq!(ts, row.timestamp_ms);
            assert_eq!((label_0, instance), w.series_labels(row.series));
            assert_eq!(value, row.value);
        }
        assert!(cursor.is_empty());
    }
}

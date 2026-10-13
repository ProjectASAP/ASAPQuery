//! Wire encoders for the two sinks. Each series' labels are encoded once up
//! front, so the per-sample cost at high rates is just the value and timestamp.

use crate::workload::{Metric, Row};
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, WireType};

pub trait Encoder: Send + Sync {
    fn encode(&self, rows: &[Row]) -> Vec<u8>;
}

/// Prometheus remote write: a snappy-compressed `WriteRequest` with one
/// `TimeSeries` (labels + one sample) per row.
pub struct RemoteWriteEncoder {
    /// Encoded `Label` fields (`TimeSeries` field 1) for each series, sorted by
    /// label name as Prometheus requires, `__name__` included.
    series_labels: Vec<Vec<u8>>,
}

impl RemoteWriteEncoder {
    /// Series of `metric`, sent with `__name__` = `name`.
    pub fn new(metric: &Metric, name: &str) -> Self {
        let series_labels = metric
            .series
            .iter()
            .map(|values| {
                let mut labels: Vec<(&str, &str)> = metric
                    .labels
                    .iter()
                    .map(|l| l.name.as_str())
                    .zip(values.iter().map(String::as_str))
                    .collect();
                labels.push(("__name__", name));
                labels.sort_unstable();
                let mut buf = Vec::new();
                for (name, value) in labels {
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

/// ClickHouse `RowBinary` for columns `ts DateTime64(3)`, then each label of
/// the metric as `String` in its order, then `value Float64`.
pub struct RowBinaryEncoder {
    /// Each series' label values, each as a varint length plus bytes.
    series_labels: Vec<Vec<u8>>,
}

/// The column list `RowBinaryEncoder` writes for `metric`, in order.
pub fn row_binary_columns(metric: &Metric) -> String {
    let labels = metric.labels.iter().map(|l| l.name.as_str());
    std::iter::once("ts")
        .chain(labels)
        .chain(std::iter::once("value"))
        .collect::<Vec<_>>()
        .join(", ")
}

impl RowBinaryEncoder {
    pub fn new(metric: &Metric) -> Self {
        let series_labels = metric
            .series
            .iter()
            .map(|values| {
                let mut buf = Vec::new();
                for s in values {
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
    use crate::table::series_labels;
    use crate::workload::{Kind, LabelSpec, Metric, Workload};
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

    fn grouped() -> Workload {
        Workload::grouped("data", 1, 2, 3, 1, 1.5, 1.0)
    }

    /// Labels out of name order, an uppercase one sorting before `__name__`.
    fn multi_label() -> Workload {
        let label = |name: &str, cardinality, child_of: Option<&str>| LabelSpec {
            name: name.to_string(),
            cardinality,
            child_of: child_of.map(str::to_string),
        };
        let labels = vec![
            label("zone", 2, None),
            label("job", 2, None),
            label("label_0", 3, Some("job")),
            label("Az", 1, None),
        ];
        let series = series_labels(&labels).unwrap();
        let metric = Metric {
            name: "data_total".to_string(),
            scales: vec![1.0; series.len()],
            labels,
            series,
            kind: Kind::Counter,
            seed: 0,
        };
        Workload {
            samples_per_sec: 1,
            metrics: vec![metric],
        }
    }

    fn decode_remote_write(body: &[u8]) -> WriteRequest {
        let raw = snap::raw::Decoder::new().decompress_vec(body).unwrap();
        WriteRequest::decode(raw.as_slice()).unwrap()
    }

    #[test]
    fn remote_write_decodes_to_one_series_per_row() {
        let w = grouped();
        let rows = w.tick_rows(0, 2, 1_700_000_000_000);
        let request =
            decode_remote_write(&RemoteWriteEncoder::new(&w.metrics[0], "data").encode(&rows));
        assert_eq!(request.timeseries.len(), rows.len());
        for (ts, row) in request.timeseries.iter().zip(&rows) {
            let values = &w.metrics[0].series[row.series];
            let labels: Vec<(&str, &str)> = ts
                .labels
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect();
            assert_eq!(
                labels,
                vec![
                    ("__name__", "data"),
                    ("instance", values[1].as_str()),
                    ("label_0", values[0].as_str()),
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
    fn remote_write_sorts_labels_by_name() {
        let w = multi_label();
        let metric = &w.metrics[0];
        let rows = w.tick_rows(0, 0, 1_000);
        let request =
            decode_remote_write(&RemoteWriteEncoder::new(metric, &metric.name).encode(&rows));
        assert_eq!(request.timeseries.len(), 12);
        for (ts, row) in request.timeseries.iter().zip(&rows) {
            let [zone, job, label_0, az] = metric.series[row.series].as_slice() else {
                panic!("four labels");
            };
            let labels: Vec<(&str, &str)> = ts
                .labels
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect();
            assert_eq!(
                labels,
                vec![
                    ("Az", az.as_str()),
                    ("__name__", "data_total"),
                    ("job", job.as_str()),
                    ("label_0", label_0.as_str()),
                    ("zone", zone.as_str()),
                ]
            );
        }
    }

    /// Decodes RowBinary rows of `ts`, `num_labels` strings and `value`.
    fn decode_row_binary(buf: &[u8], num_labels: usize) -> Vec<(i64, Vec<String>, f64)> {
        let mut cursor = buf;
        let mut take = |n: usize| -> Vec<u8> {
            let (head, rest) = cursor.split_at(n);
            cursor = rest;
            head.to_vec()
        };
        let mut rows = Vec::new();
        let mut remaining = buf.len();
        while remaining > 0 {
            let ts = i64::from_le_bytes(take(8).try_into().unwrap());
            // Labels are short, so their varint length is one byte.
            let labels: Vec<String> = (0..num_labels)
                .map(|_| {
                    let len = take(1)[0] as usize;
                    String::from_utf8(take(len)).unwrap()
                })
                .collect();
            let value = f64::from_le_bytes(take(8).try_into().unwrap());
            remaining -= 16 + labels.iter().map(|l| 1 + l.len()).sum::<usize>();
            rows.push((ts, labels, value));
        }
        rows
    }

    #[test]
    fn row_binary_round_trips_every_column() {
        let w = grouped();
        let metric = &w.metrics[0];
        assert_eq!(row_binary_columns(metric), "ts, label_0, instance, value");
        let rows = w.tick_rows(0, 0, 42_000);
        let decoded = decode_row_binary(&RowBinaryEncoder::new(metric).encode(&rows), 2);
        let expected: Vec<_> = rows
            .iter()
            .map(|r| (r.timestamp_ms, metric.series[r.series].clone(), r.value))
            .collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn row_binary_writes_labels_in_description_order() {
        let w = multi_label();
        let metric = &w.metrics[0];
        assert_eq!(
            row_binary_columns(metric),
            "ts, zone, job, label_0, Az, value"
        );
        let rows = w.tick_rows(0, 3, 0);
        let decoded = decode_row_binary(&RowBinaryEncoder::new(metric).encode(&rows), 4);
        assert_eq!(decoded.len(), 12);
        assert_eq!(
            decoded[4],
            (
                4_000,
                vec!["0".into(), "1".into(), "1.1".into(), "0".into()],
                4.0
            )
        );
    }
}

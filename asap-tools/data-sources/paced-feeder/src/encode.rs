//! Wire encoders for the two sinks. Each series' labels (or each label value
//! of a records metric) are encoded once up front, so the per-sample cost at
//! high rates is just the values and timestamp.

use crate::records::{RecordMetric, Records};
use crate::workload::{Metric, Row, Rows};
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, WireType};
use std::ops::Range;
use std::sync::Arc;

pub trait Encoder: Send + Sync {
    /// Encodes `rows[range]`, which are of the kind the encoder was built for.
    fn encode(&self, rows: &Rows, range: Range<usize>) -> Vec<u8>;
}

fn samples(rows: &Rows) -> &[Row] {
    match rows {
        Rows::Samples(rows) => rows,
        Rows::Records(_) => panic!("a series encoder was given records"),
    }
}

fn records(rows: &Rows) -> &Records {
    match rows {
        Rows::Records(records) => records,
        Rows::Samples(_) => panic!("a records encoder was given samples"),
    }
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
                    encode_label(name, value, &mut buf);
                }
                buf
            })
            .collect();
        Self { series_labels }
    }
}

impl Encoder for RemoteWriteEncoder {
    fn encode(&self, rows: &Rows, range: Range<usize>) -> Vec<u8> {
        let mut request = Vec::new();
        let mut series = Vec::new();
        for row in &samples(rows)[range] {
            series.clear();
            series.extend_from_slice(&self.series_labels[row.series]);
            encode_sample(row.value, row.timestamp_ms, &mut series);
            encode_bytes_field(1, &series, &mut request);
        }
        compress(&request)
    }
}

/// Generates records `range` of `rows` (indices relative to their first) and
/// calls `f` with each one's timestamp, label value indices and values.
fn for_each_record(
    metric: &RecordMetric,
    rows: &Rows,
    range: Range<usize>,
    mut f: impl FnMut(i64, &[u32], &[f64]),
) {
    let records = records(rows);
    let mut labels = vec![0u32; metric.labels.len()];
    let mut values = vec![0.0; metric.fields.len()];
    let first = records.indices.start;
    for index in first + range.start as u64..first + range.end as u64 {
        metric.record(index, &mut labels, &mut values);
        f(
            metric.timestamp_ms(index, records.start_ms),
            &labels,
            &values,
        );
    }
}

/// Remote write of a records metric: each record is one sample per value
/// field, named `<metric>.<field>`, with the record's labels and timestamp.
/// ASAPQuery's SQL mode routes a sample to the aggregations of
/// `table.value_column`, so each field feeds only its own aggregations.
pub struct RecordsRemoteWriteEncoder {
    metric: Arc<RecordMetric>,
    /// The labels in name order, `None` for `__name__`.
    order: Vec<Option<usize>>,
    /// The encoded `Label` field of each value of each label.
    label_values: Vec<Vec<Vec<u8>>>,
    /// The encoded `__name__` `Label` field of each value field.
    names: Vec<Vec<u8>>,
}

/// The `__name__` of value field `field` of records metric `metric`.
pub fn record_sample_name(metric: &str, field: &str) -> String {
    format!("{metric}.{field}")
}

impl RecordsRemoteWriteEncoder {
    pub fn new(metric: Arc<RecordMetric>) -> Self {
        let mut sorted: Vec<(&str, Option<usize>)> = metric
            .labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l.name.as_str(), Some(i)))
            .collect();
        sorted.push(("__name__", None));
        sorted.sort_unstable();
        let label_values = metric
            .labels
            .iter()
            .zip(&metric.label_values)
            .map(|(label, values)| {
                values
                    .iter()
                    .map(|value| {
                        let mut buf = Vec::new();
                        encode_label(&label.name, value, &mut buf);
                        buf
                    })
                    .collect()
            })
            .collect();
        let names = metric
            .fields
            .iter()
            .map(|field| {
                let mut buf = Vec::new();
                encode_label(
                    "__name__",
                    &record_sample_name(&metric.name, field),
                    &mut buf,
                );
                buf
            })
            .collect();
        Self {
            order: sorted.into_iter().map(|(_, i)| i).collect(),
            label_values,
            names,
            metric,
        }
    }
}

impl Encoder for RecordsRemoteWriteEncoder {
    fn encode(&self, rows: &Rows, range: Range<usize>) -> Vec<u8> {
        let mut request = Vec::new();
        let mut series = Vec::new();
        for_each_record(&self.metric, rows, range, |timestamp_ms, labels, values| {
            for (field, &value) in values.iter().enumerate() {
                series.clear();
                for label in &self.order {
                    series.extend_from_slice(match label {
                        None => &self.names[field],
                        Some(l) => &self.label_values[*l][labels[*l] as usize],
                    });
                }
                encode_sample(value, timestamp_ms, &mut series);
                encode_bytes_field(1, &series, &mut request);
            }
        });
        compress(&request)
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
                    encode_string(s, &mut buf);
                }
                buf
            })
            .collect();
        Self { series_labels }
    }
}

impl Encoder for RowBinaryEncoder {
    fn encode(&self, rows: &Rows, range: Range<usize>) -> Vec<u8> {
        let rows = &samples(rows)[range];
        let mut buf = Vec::with_capacity(rows.len() * 32);
        for row in rows {
            buf.extend_from_slice(&row.timestamp_ms.to_le_bytes());
            buf.extend_from_slice(&self.series_labels[row.series]);
            buf.extend_from_slice(&row.value.to_le_bytes());
        }
        buf
    }
}

/// ClickHouse `RowBinary` of a records metric for columns `ts DateTime64(3)`,
/// each label as `String` in description order, then each value field as
/// `Float64` in name order.
pub struct RecordsRowBinaryEncoder {
    metric: Arc<RecordMetric>,
    /// Each value of each label as a varint length plus bytes.
    label_values: Vec<Vec<Vec<u8>>>,
}

/// The column list `RecordsRowBinaryEncoder` writes for `metric`, in order.
pub fn record_columns(metric: &RecordMetric) -> String {
    let labels = metric.labels.iter().map(|l| l.name.as_str());
    std::iter::once("ts")
        .chain(labels)
        .chain(metric.fields.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(", ")
}

impl RecordsRowBinaryEncoder {
    pub fn new(metric: Arc<RecordMetric>) -> Self {
        let label_values = metric
            .label_values
            .iter()
            .map(|values| {
                values
                    .iter()
                    .map(|value| {
                        let mut buf = Vec::new();
                        encode_string(value, &mut buf);
                        buf
                    })
                    .collect()
            })
            .collect();
        Self {
            metric,
            label_values,
        }
    }
}

impl Encoder for RecordsRowBinaryEncoder {
    fn encode(&self, rows: &Rows, range: Range<usize>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(range.len() * 64);
        for_each_record(&self.metric, rows, range, |timestamp_ms, labels, values| {
            buf.extend_from_slice(&timestamp_ms.to_le_bytes());
            for (label_values, &v) in self.label_values.iter().zip(labels) {
                buf.extend_from_slice(&label_values[v as usize]);
            }
            for value in values {
                buf.extend_from_slice(&value.to_le_bytes());
            }
        });
        buf
    }
}

/// A `Label` message as `TimeSeries` field 1.
fn encode_label(name: &str, value: &str, buf: &mut Vec<u8>) {
    let mut label = Vec::new();
    encode_bytes_field(1, name.as_bytes(), &mut label);
    encode_bytes_field(2, value.as_bytes(), &mut label);
    encode_bytes_field(1, &label, buf);
}

/// A `Sample` message as `TimeSeries` field 2.
fn encode_sample(value: f64, timestamp_ms: i64, series: &mut Vec<u8>) {
    // One-byte keys of fields 1 (fixed 64-bit) and 2 (varint).
    let len = 1 + 8 + 1 + encoded_len_varint(timestamp_ms as u64);
    encode_key(2, WireType::LengthDelimited, series);
    encode_varint(len as u64, series);
    encode_key(1, WireType::SixtyFourBit, series);
    series.extend_from_slice(&value.to_le_bytes());
    encode_key(2, WireType::Varint, series);
    encode_varint(timestamp_ms as u64, series);
}

/// A ClickHouse `String`: varint length plus bytes.
fn encode_string(s: &str, buf: &mut Vec<u8>) {
    encode_varint(s.len() as u64, buf);
    buf.extend_from_slice(s.as_bytes());
}

fn compress(request: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(request)
        .expect("snappy compression of an in-memory buffer cannot fail")
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
            skew: None,
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
            records: Vec::new(),
        }
    }

    fn encode_all(encoder: &dyn Encoder, rows: &[Row]) -> Vec<u8> {
        encoder.encode(&Rows::Samples(rows.to_vec()), 0..rows.len())
    }

    fn decode_remote_write(body: &[u8]) -> WriteRequest {
        let raw = snap::raw::Decoder::new().decompress_vec(body).unwrap();
        WriteRequest::decode(raw.as_slice()).unwrap()
    }

    #[test]
    fn remote_write_decodes_to_one_series_per_row() {
        let w = grouped();
        let rows = w.series_rows(0, 2, 0, 1_700_000_000_000);
        let request = decode_remote_write(&encode_all(
            &RemoteWriteEncoder::new(&w.metrics[0], "data"),
            &rows,
        ));
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
        let rows = w.series_rows(0, 0, 0, 1_000);
        let request = decode_remote_write(&encode_all(
            &RemoteWriteEncoder::new(metric, &metric.name),
            &rows,
        ));
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
        let rows = w.series_rows(0, 0, 0, 42_000);
        let decoded = decode_row_binary(&encode_all(&RowBinaryEncoder::new(metric), &rows), 2);
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
        let rows = w.series_rows(0, 3, 0, 0);
        let decoded = decode_row_binary(&encode_all(&RowBinaryEncoder::new(metric), &rows), 4);
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

    fn http() -> Arc<RecordMetric> {
        let spec = crate::records::tests::http_spec();
        Arc::new(RecordMetric::from_table("http", &spec, Some(1_000), 42).unwrap())
    }

    /// Record `index` of `metric` as (timestamp, label values, values).
    fn record(metric: &RecordMetric, index: u64, start_ms: i64) -> (i64, Vec<String>, Vec<f64>) {
        let mut labels = vec![0; metric.labels.len()];
        let mut values = vec![0.0; metric.fields.len()];
        metric.record(index, &mut labels, &mut values);
        let names = labels
            .iter()
            .zip(&metric.label_values)
            .map(|(&v, values)| values[v as usize].clone())
            .collect();
        (metric.timestamp_ms(index, start_ms), names, values)
    }

    #[test]
    fn remote_write_sends_a_sample_per_record_field() {
        let metric = http();
        // Slot 3 of tick 1: records 1030..1040 at 1_000 + 1_000 + 40 ms.
        let rows = Rows::Records(Records {
            start_ms: 1_000,
            indices: 1_030..1_040,
        });
        let request = decode_remote_write(
            &RecordsRemoteWriteEncoder::new(Arc::clone(&metric)).encode(&rows, 2..5),
        );
        assert_eq!(request.timeseries.len(), 6);
        for (i, pair) in request.timeseries.chunks(2).enumerate() {
            let (timestamp, labels, values) = record(&metric, 1_032 + i as u64, 1_000);
            assert_eq!(timestamp, 2_040);
            // Value fields in name order: latency, then user_id.
            for (ts, (field, value)) in pair
                .iter()
                .zip([("latency", values[0]), ("user_id", values[1])])
            {
                let got: Vec<(&str, &str)> = ts
                    .labels
                    .iter()
                    .map(|l| (l.name.as_str(), l.value.as_str()))
                    .collect();
                let name = format!("http.{field}");
                assert_eq!(
                    got,
                    vec![
                        ("__name__", name.as_str()),
                        ("endpoint", labels[2].as_str()),
                        ("region", labels[0].as_str()),
                        ("service", labels[1].as_str()),
                        ("status", labels[3].as_str()),
                    ]
                );
                assert_eq!(ts.samples, vec![Sample { value, timestamp }]);
            }
        }
    }

    /// Decodes RowBinary rows of `ts`, `num_labels` strings and `num_values` floats.
    fn decode_record_rows(
        buf: &[u8],
        num_labels: usize,
        num_values: usize,
    ) -> Vec<(i64, Vec<String>, Vec<f64>)> {
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
            let labels: Vec<String> = (0..num_labels)
                .map(|_| {
                    let len = take(1)[0] as usize;
                    String::from_utf8(take(len)).unwrap()
                })
                .collect();
            let values: Vec<f64> = (0..num_values)
                .map(|_| f64::from_le_bytes(take(8).try_into().unwrap()))
                .collect();
            remaining -= 8 + 8 * num_values + labels.iter().map(|l| 1 + l.len()).sum::<usize>();
            rows.push((ts, labels, values));
        }
        rows
    }

    #[test]
    fn row_binary_writes_a_row_per_record() {
        let metric = http();
        assert_eq!(
            record_columns(&metric),
            "ts, region, service, endpoint, status, latency, user_id"
        );
        let rows = Rows::Records(metric.tick(0, 5_000));
        assert_eq!(rows.len(), 1_000);
        let body = RecordsRowBinaryEncoder::new(Arc::clone(&metric)).encode(&rows, 0..1_000);
        let decoded = decode_record_rows(&body, 4, 2);
        let expected: Vec<_> = (0..1_000).map(|i| record(&metric, i, 5_000)).collect();
        assert_eq!(decoded, expected);
        assert_eq!((decoded[0].0, decoded[999].0), (5_010, 6_000));
    }

    #[test]
    fn both_sinks_send_the_same_records() {
        let metric = http();
        let rows = Rows::Records(metric.tick(2, 0));
        let range = 0..rows.len();
        let request = decode_remote_write(
            &RecordsRemoteWriteEncoder::new(Arc::clone(&metric)).encode(&rows, range.clone()),
        );
        let decoded = decode_record_rows(
            &RecordsRowBinaryEncoder::new(Arc::clone(&metric)).encode(&rows, range),
            4,
            2,
        );
        assert_eq!(request.timeseries.len(), 2 * decoded.len());
        for (pair, (ts, labels, values)) in request.timeseries.chunks(2).zip(&decoded) {
            for (series, value) in pair.iter().zip(values) {
                assert_eq!(
                    series.samples,
                    vec![Sample {
                        value: *value,
                        timestamp: *ts
                    }]
                );
                let mut sent: Vec<&str> = series.labels[1..]
                    .iter()
                    .map(|l| l.value.as_str())
                    .collect();
                let mut want: Vec<&str> = labels.iter().map(String::as_str).collect();
                sent.sort_unstable();
                want.sort_unstable();
                assert_eq!(sent, want);
            }
        }
    }

    #[test]
    #[should_panic(expected = "a series encoder was given records")]
    fn series_encoders_reject_records() {
        let w = grouped();
        let rows = Rows::Records(http().tick(0, 0));
        RowBinaryEncoder::new(&w.metrics[0]).encode(&rows, 0..1);
    }
}

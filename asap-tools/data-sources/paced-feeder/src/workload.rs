//! Seeded synthetic workload: either one or more metrics, each a set of
//! labelled series emitting `samples_per_sec` evenly spaced samples per
//! second, or one or more `records` metrics (see [`crate::records`]).
//!
//! A sample's value depends only on `(metric seed, series, sample index)`,
//! never on wall-clock time or on which sink sends it, so two feeders started
//! with the same parameters produce identical data.

use crate::records::{RecordMetric, Records};
use serde::Deserialize;
use std::sync::Arc;

/// One generated sample of one metric. `series` indexes into [`Metric::series`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Row {
    pub series: usize,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// One tick's rows of one metric: samples of series, or records.
#[derive(Debug, Clone, PartialEq)]
pub enum Rows {
    Samples(Vec<Row>),
    Records(Records),
}

impl Rows {
    pub fn len(&self) -> usize {
        match self {
            Rows::Samples(rows) => rows.len(),
            Rows::Records(records) => records.len(),
        }
    }
}

/// A label with `cardinality` values, or, with `child_of`, that many values
/// under each value of the parent label. `skew` (records metrics only) is the
/// Zipf exponent its values are drawn with.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelSpec {
    pub name: String,
    pub cardinality: u64,
    #[serde(default)]
    pub child_of: Option<String>,
    #[serde(default)]
    pub skew: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    /// `scale · pareto_scale · U^(-1/pareto_shape)`: Pareto(shape, scale) times
    /// the series' scale.
    Gauge {
        pareto_shape: f64,
        pareto_scale: f64,
    },
    /// `(n + 1) · scale` at sample index `n`: starts at the first sample, grows
    /// by the series' scale per sample and never resets.
    Counter,
}

#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub labels: Vec<LabelSpec>,
    /// Each series' label values, in `labels` order.
    pub series: Vec<Vec<String>>,
    /// Each series' value multiplier.
    pub scales: Vec<f64>,
    pub kind: Kind,
    pub seed: u64,
}

/// Gauge and counter `metrics`, or `records` metrics, never both: they have
/// different sub-second slots.
#[derive(Debug, Clone)]
pub struct Workload {
    /// Samples per second per series of `metrics`; unused for `records`.
    pub samples_per_sec: u32,
    pub metrics: Vec<Metric>,
    pub records: Vec<Arc<RecordMetric>>,
}

impl Workload {
    /// The fixed `--groups C × --series-per-group s` workload: one gauge with
    /// series `g * s + i` labelled `label_0="g{g}", instance="i{i}"`.
    pub fn grouped(
        name: &str,
        seed: u64,
        groups: usize,
        series_per_group: usize,
        samples_per_sec: u32,
        pareto_shape: f64,
        pareto_scale: f64,
    ) -> Self {
        let series: Vec<Vec<String>> = (0..groups * series_per_group)
            .map(|s| {
                vec![
                    format!("g{}", s / series_per_group),
                    format!("i{}", s % series_per_group),
                ]
            })
            .collect();
        let scales = vec![1.0; series.len()];
        let label = |name: &str, cardinality: usize| LabelSpec {
            name: name.to_string(),
            cardinality: cardinality as u64,
            child_of: None,
            skew: None,
        };
        let metric = Metric {
            name: name.to_string(),
            labels: vec![
                label("label_0", groups),
                label("instance", series_per_group),
            ],
            series,
            scales,
            kind: Kind::Gauge {
                pareto_shape,
                pareto_scale,
            },
            seed,
        };
        Self {
            samples_per_sec,
            metrics: vec![metric],
            records: Vec::new(),
        }
    }

    pub fn num_series(&self) -> usize {
        self.metrics.iter().map(|m| m.series.len()).sum()
    }

    /// Series samples or records per second.
    pub fn rows_per_tick(&self) -> usize {
        self.num_series() * self.samples_per_sec as usize
            + self
                .records
                .iter()
                .map(|r| r.records_per_sec as usize)
                .sum::<usize>()
    }

    /// The metric names, gauges and counters first, then records.
    pub fn names(&self) -> Vec<String> {
        let series = self.metrics.iter().map(|m| m.name.clone());
        series
            .chain(self.records.iter().map(|r| r.name.clone()))
            .collect()
    }

    /// Rows of the `target`-th metric of [`Self::names`] for one-second tick
    /// `tick`, with event time in `(t, t + 1 s]`, where `t` is
    /// `start_ms + tick · 1 s`: the same left-open interval as ASAPQuery's
    /// one-second windows, so a tick never spans two of them.
    pub fn tick_rows(&self, target: usize, tick: u64, start_ms: i64) -> Rows {
        match target.checked_sub(self.metrics.len()) {
            None => Rows::Samples(
                (0..self.samples_per_sec)
                    .flat_map(|slot| self.series_rows(target, tick, slot, start_ms))
                    .collect(),
            ),
            Some(r) => Rows::Records(self.records[r].tick(tick, start_ms)),
        }
    }

    /// Samples of `metric` for sub-second slot `slot` of tick `tick`, one per
    /// series. Slot `j` of `n` is at `t + (j + 1) · 1 s / n`.
    pub fn series_rows(&self, metric: usize, tick: u64, slot: u32, start_ms: i64) -> Vec<Row> {
        let per_sec = u64::from(self.samples_per_sec);
        let slot = u64::from(slot);
        let timestamp_ms = start_ms + tick as i64 * 1_000 + ((slot + 1) * 1_000 / per_sec) as i64;
        let sample_index = tick * per_sec + slot;
        let metric = &self.metrics[metric];
        (0..metric.series.len())
            .map(|series| Row {
                series,
                timestamp_ms,
                value: metric.value(series, sample_index),
            })
            .collect()
    }
}

impl Metric {
    pub fn value(&self, series: usize, sample_index: u64) -> f64 {
        let scale = self.scales[series];
        match self.kind {
            // Pareto by inverse transform with `U` uniform in (0, 1], so every
            // value is ≥ `scale · pareto_scale` and positive.
            Kind::Gauge {
                pareto_shape,
                pareto_scale,
            } => {
                let u = unit_uniform(self.seed, series as u64, sample_index);
                scale * pareto_scale * u.powf(-1.0 / pareto_shape)
            }
            Kind::Counter => (sample_index + 1) as f64 * scale,
        }
    }
}

/// Counter-based uniform draw in (0, 1]: a stateless hash of the inputs, so
/// any sample can be regenerated independently of the ones before it.
pub fn unit_uniform(seed: u64, series: u64, sample_index: u64) -> f64 {
    let x = splitmix64(seed ^ splitmix64(series ^ splitmix64(sample_index)));
    // Top 53 bits, shifted into (0, 1] so the Pareto transform never divides by zero.
    ((x >> 11) + 1) as f64 / (1u64 << 53) as f64
}

pub fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workload() -> Workload {
        Workload::grouped("data", 7, 3, 2, 2, 1.5, 1.0)
    }

    #[test]
    fn each_slot_has_every_series_at_its_timestamp() {
        let w = workload();
        for (slot, timestamp_ms) in [(0, 15_500), (1, 16_000)] {
            let rows = w.series_rows(0, 5, slot, 10_000);
            assert_eq!(rows.len(), 6);
            assert!(rows.iter().all(|r| r.timestamp_ms == timestamp_ms));
            assert!(rows.iter().map(|r| r.series).eq(0..6));
        }
    }

    #[test]
    fn values_depend_only_on_seed_series_and_sample_index() {
        let w = workload();
        let values = |w: &Workload, start_ms| -> Vec<f64> {
            w.series_rows(0, 4, 1, start_ms)
                .iter()
                .map(|r| r.value)
                .collect()
        };
        // Same slot, different start time: same values.
        assert_eq!(values(&w, 0), values(&w, 1_000_000));
        // A different seed changes them.
        let other = Workload::grouped("data", 8, 3, 2, 2, 1.5, 1.0);
        assert_ne!(values(&w, 0), values(&other, 0));
    }

    #[test]
    fn grouped_values_keep_the_seeded_pareto_draw() {
        // The grouped workload's values are the plain Pareto draw of
        // `(seed, series, sample index)`, as before metrics had scales.
        let w = workload();
        let u = unit_uniform(7, 3, 9);
        assert_eq!(w.metrics[0].value(3, 9), u.powf(-1.0 / 1.5));
    }

    #[test]
    fn labels_split_series_into_groups_and_instances() {
        let w = workload();
        let series = &w.metrics[0].series;
        assert_eq!(series[0], ["g0", "i0"]);
        assert_eq!(series[3], ["g1", "i1"]);
        assert_eq!(series[5], ["g2", "i1"]);
    }

    #[test]
    fn values_follow_pareto_distribution() {
        let w = Workload::grouped("data", 7, 1, 1, 2, 1.5, 1.0);
        let n = 200_000u64;
        let mut values: Vec<f64> = (0..n).map(|i| w.metrics[0].value(0, i)).collect();
        assert!(values.iter().all(|&v| v >= 1.0));
        values.sort_by(f64::total_cmp);
        // Pareto(a, xm) quantile: xm · (1 - q)^(-1/a).
        for q in [0.5, 0.9, 0.99] {
            let empirical = values[(q * n as f64) as usize];
            let theoretical = (1.0 - q).powf(-1.0 / 1.5);
            assert!(
                (empirical - theoretical).abs() / theoretical < 0.05,
                "q={q}: empirical {empirical} vs theoretical {theoretical}"
            );
        }
    }

    #[test]
    fn counters_increase_strictly_per_series() {
        let metric = Metric {
            kind: Kind::Counter,
            scales: vec![0.5, 2.0],
            ..workload().metrics[0].clone()
        };
        for series in 0..2 {
            let values: Vec<f64> = (0..100).map(|n| metric.value(series, n)).collect();
            assert_eq!(values[0], metric.scales[series]);
            assert!(values.windows(2).all(|w| w[1] > w[0]));
        }
    }
}

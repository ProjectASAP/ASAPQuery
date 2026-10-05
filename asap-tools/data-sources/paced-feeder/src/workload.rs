//! Seeded synthetic workload: `groups × series_per_group` series, each emitting
//! `samples_per_sec` Pareto-distributed values per second.
//!
//! A sample's value depends only on `(seed, series, sample index)`, never on
//! wall-clock time or on which sink sends it, so two feeders started with the
//! same parameters produce identical data.

/// One generated sample. `series` indexes into [`Workload::series_labels`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Row {
    pub series: usize,
    pub timestamp_ms: i64,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct Workload {
    pub seed: u64,
    pub groups: usize,
    pub series_per_group: usize,
    pub samples_per_sec: u32,
    pub pareto_shape: f64,
    pub pareto_scale: f64,
}

impl Workload {
    pub fn num_series(&self) -> usize {
        self.groups * self.series_per_group
    }

    pub fn rows_per_tick(&self) -> usize {
        self.num_series() * self.samples_per_sec as usize
    }

    /// `(label_0, instance)` for a series: series `g * series_per_group + i`
    /// is `("g{g}", "i{i}")`.
    pub fn series_labels(&self, series: usize) -> (String, String) {
        let group = series / self.series_per_group;
        let instance = series % self.series_per_group;
        (format!("g{group}"), format!("i{instance}"))
    }

    /// All samples for one-second tick `tick`, with event time starting at
    /// `start_ms`. Samples within the second are spaced evenly.
    pub fn tick_rows(&self, tick: u64, start_ms: i64) -> Vec<Row> {
        let per_sec = u64::from(self.samples_per_sec);
        let tick_start_ms = start_ms + tick as i64 * 1_000;
        let mut rows = Vec::with_capacity(self.rows_per_tick());
        for k in 0..per_sec {
            let timestamp_ms = tick_start_ms + (k * 1_000 / per_sec) as i64;
            let sample_index = tick * per_sec + k;
            for series in 0..self.num_series() {
                rows.push(Row {
                    series,
                    timestamp_ms,
                    value: self.value(series, sample_index),
                });
            }
        }
        rows
    }

    /// Pareto(shape, scale) by inverse transform: `scale · U^(-1/shape)` with
    /// `U` uniform in (0, 1], so every value is ≥ `scale` and positive.
    pub fn value(&self, series: usize, sample_index: u64) -> f64 {
        let u = unit_uniform(self.seed, series as u64, sample_index);
        self.pareto_scale * u.powf(-1.0 / self.pareto_shape)
    }
}

/// Counter-based uniform draw in (0, 1]: a stateless hash of the inputs, so
/// any sample can be regenerated independently of the ones before it.
fn unit_uniform(seed: u64, series: u64, sample_index: u64) -> f64 {
    let x = splitmix64(seed ^ splitmix64(series ^ splitmix64(sample_index)));
    // Top 53 bits, shifted into (0, 1] so the Pareto transform never divides by zero.
    ((x >> 11) + 1) as f64 / (1u64 << 53) as f64
}

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workload() -> Workload {
        Workload {
            seed: 7,
            groups: 3,
            series_per_group: 2,
            samples_per_sec: 2,
            pareto_shape: 1.5,
            pareto_scale: 1.0,
        }
    }

    #[test]
    fn tick_has_every_series_at_every_sub_second_slot() {
        let w = workload();
        let rows = w.tick_rows(5, 10_000);
        assert_eq!(rows.len(), 3 * 2 * 2);
        let first: Vec<_> = rows.iter().filter(|r| r.timestamp_ms == 15_000).collect();
        let second: Vec<_> = rows.iter().filter(|r| r.timestamp_ms == 15_500).collect();
        assert_eq!(first.len(), 6);
        assert_eq!(second.len(), 6);
    }

    #[test]
    fn values_depend_only_on_seed_series_and_sample_index() {
        let w = workload();
        // Same tick, different start time: same values.
        let a: Vec<f64> = w.tick_rows(4, 0).iter().map(|r| r.value).collect();
        let b: Vec<f64> = w.tick_rows(4, 1_000_000).iter().map(|r| r.value).collect();
        assert_eq!(a, b);
        // A different seed changes them.
        let other = Workload { seed: 8, ..w };
        assert_ne!(
            a,
            other
                .tick_rows(4, 0)
                .iter()
                .map(|r| r.value)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn labels_split_series_into_groups_and_instances() {
        let w = workload();
        assert_eq!(w.series_labels(0), ("g0".to_string(), "i0".to_string()));
        assert_eq!(w.series_labels(3), ("g1".to_string(), "i1".to_string()));
        assert_eq!(w.series_labels(5), ("g2".to_string(), "i1".to_string()));
    }

    #[test]
    fn values_follow_pareto_distribution() {
        let w = Workload {
            groups: 1,
            series_per_group: 1,
            ..workload()
        };
        let n = 200_000u64;
        let mut values: Vec<f64> = (0..n).map(|i| w.value(0, i)).collect();
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
}

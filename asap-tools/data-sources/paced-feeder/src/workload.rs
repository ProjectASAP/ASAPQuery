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

    /// The samples of one-second tick `tick`, series-major: each series'
    /// `samples_per_sec` samples in timestamp order, then the next series'.
    /// Slot `j` of `n` is at `t + (j + 1) · 1 s / n`, where `t` is the tick's
    /// start, so the tick lies in `(t, t + 1 s]`: the same left-open interval as
    /// ASAPQuery's one-second windows, so a tick never spans two of them.
    pub fn tick_rows(&self, tick: u64, start_ms: i64) -> Vec<Row> {
        let per_sec = u64::from(self.samples_per_sec);
        let tick_ms = start_ms + tick as i64 * 1_000;
        (0..self.num_series())
            .flat_map(|series| {
                (0..per_sec).map(move |slot| Row {
                    series,
                    timestamp_ms: tick_ms + ((slot + 1) * 1_000 / per_sec) as i64,
                    value: self.value(series, tick * per_sec + slot),
                })
            })
            .collect()
    }

    /// Pareto(shape, scale) by inverse transform: `scale · U^(-1/shape)` with
    /// `U` uniform in (0, 1], so every value is ≥ `scale` and positive.
    pub fn value(&self, series: usize, sample_index: u64) -> f64 {
        let u = unit_uniform(self.seed, series as u64, sample_index);
        self.pareto_scale * u.powf(-1.0 / self.pareto_shape)
    }
}

/// Row ranges of the requests for `rows` series-major rows with
/// `per_series` rows per series: about `batch_rows` each, cut only between
/// series (at least one series per request), so every series' samples of a
/// tick reach the sink in one request, in timestamp order. Concurrent requests
/// then reorder samples only across series, never within one, which keeps
/// counters monotonic for consumers that read them in arrival order.
pub fn series_batches(
    rows: usize,
    per_series: usize,
    batch_rows: usize,
) -> impl Iterator<Item = std::ops::Range<usize>> {
    let batch = (batch_rows / per_series).max(1) * per_series;
    (0..rows)
        .step_by(batch)
        .map(move |begin| begin..(begin + batch).min(rows))
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
    fn a_tick_is_series_major_in_timestamp_order() {
        let w = workload();
        let rows = w.tick_rows(5, 10_000);
        assert_eq!(rows.len(), 12);
        for (series, samples) in rows.chunks(2).enumerate() {
            assert!(samples.iter().all(|r| r.series == series));
            let timestamps: Vec<i64> = samples.iter().map(|r| r.timestamp_ms).collect();
            assert_eq!(timestamps, [15_500, 16_000]);
        }
    }

    #[test]
    fn batches_keep_each_series_in_one_request_in_order() {
        let w = Workload {
            samples_per_sec: 3,
            ..workload()
        };
        let rows = w.tick_rows(2, 0);
        // 7 rows round down to 2 series (6 rows); 1 row still takes a series.
        for (batch_rows, sizes) in [(7, vec![6, 6, 6]), (1, vec![3; 6]), (100, vec![18])] {
            let batches: Vec<_> = series_batches(rows.len(), 3, batch_rows).collect();
            assert_eq!(batches.iter().map(|b| b.len()).collect::<Vec<_>>(), sizes);
            for series in 0..w.num_series() {
                let holding: Vec<_> = batches
                    .iter()
                    .filter(|b| rows[(*b).clone()].iter().any(|r| r.series == series))
                    .collect();
                assert_eq!(holding.len(), 1, "series {series}");
                let timestamps: Vec<i64> = rows[holding[0].clone()]
                    .iter()
                    .filter(|r| r.series == series)
                    .map(|r| r.timestamp_ms)
                    .collect();
                assert_eq!(timestamps.len(), 3);
                assert!(timestamps.windows(2).all(|t| t[0] < t[1]));
            }
        }
    }

    #[test]
    fn values_depend_only_on_seed_series_and_sample_index() {
        let w = workload();
        let values = |w: &Workload, start_ms| -> Vec<f64> {
            w.tick_rows(4, start_ms).iter().map(|r| r.value).collect()
        };
        // Same tick, different start time: same values.
        assert_eq!(values(&w, 0), values(&w, 1_000_000));
        // A different seed changes them.
        let other = Workload {
            seed: 8,
            ..w.clone()
        };
        assert_ne!(values(&w, 0), values(&other, 0));
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

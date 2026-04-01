use std::collections::BTreeMap;

/// Per-series sample buffer backed by a `BTreeMap<i64, f64>` for automatic
/// ordering by timestamp. Tracks a per-series watermark.
pub struct SeriesBuffer {
    /// Samples keyed by timestamp_ms. BTreeMap keeps them sorted.
    samples: BTreeMap<i64, f64>,
    /// High-watermark: the maximum timestamp seen so far for this series.
    watermark_ms: i64,
    /// Maximum number of samples to retain. When exceeded, oldest are evicted.
    max_buffer_size: usize,
}

impl SeriesBuffer {
    pub fn new(max_buffer_size: usize) -> Self {
        Self {
            samples: BTreeMap::new(),
            watermark_ms: i64::MIN,
            max_buffer_size,
        }
    }

    /// Insert a sample. Updates the watermark if `timestamp_ms` is the new max.
    /// Returns `true` if the sample was actually inserted (not a duplicate timestamp
    /// with the same value).
    pub fn insert(&mut self, timestamp_ms: i64, value: f64) -> bool {
        if timestamp_ms > self.watermark_ms {
            self.watermark_ms = timestamp_ms;
        }
        self.samples.insert(timestamp_ms, value);

        // Enforce max buffer size by evicting oldest entries
        while self.samples.len() > self.max_buffer_size {
            self.samples.pop_first();
        }

        true
    }

    /// Return current watermark.
    pub fn watermark_ms(&self) -> i64 {
        self.watermark_ms
    }

    /// Read all samples in `[start_ms, end_ms)` — inclusive start, exclusive end.
    /// Returns them in timestamp order.
    pub fn read_range(&self, start_ms: i64, end_ms: i64) -> Vec<(i64, f64)> {
        self.samples
            .range(start_ms..end_ms)
            .map(|(&ts, &val)| (ts, val))
            .collect()
    }

    /// Drain (remove and return) all samples with `timestamp_ms < up_to_ms`.
    pub fn drain_up_to(&mut self, up_to_ms: i64) -> Vec<(i64, f64)> {
        let mut drained = Vec::new();
        // split_off returns everything >= up_to_ms; we keep that part
        let remaining = self.samples.split_off(&up_to_ms);
        // self.samples now contains everything < up_to_ms
        drained.extend(self.samples.iter().map(|(&ts, &val)| (ts, val)));
        self.samples = remaining;
        drained
    }

    /// Number of buffered samples.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_watermark() {
        let mut buf = SeriesBuffer::new(100);
        assert_eq!(buf.watermark_ms(), i64::MIN);

        buf.insert(1000, 1.0);
        assert_eq!(buf.watermark_ms(), 1000);

        buf.insert(500, 0.5); // out-of-order
        assert_eq!(buf.watermark_ms(), 1000); // watermark should not go back

        buf.insert(2000, 2.0);
        assert_eq!(buf.watermark_ms(), 2000);
    }

    #[test]
    fn test_sorted_order() {
        let mut buf = SeriesBuffer::new(100);
        buf.insert(3000, 3.0);
        buf.insert(1000, 1.0);
        buf.insert(2000, 2.0);

        let all = buf.read_range(0, 4000);
        assert_eq!(all, vec![(1000, 1.0), (2000, 2.0), (3000, 3.0)]);
    }

    #[test]
    fn test_read_range() {
        let mut buf = SeriesBuffer::new(100);
        for t in [1000, 2000, 3000, 4000, 5000] {
            buf.insert(t, t as f64);
        }

        // [2000, 4000) should return 2000, 3000
        let range = buf.read_range(2000, 4000);
        assert_eq!(range, vec![(2000, 2000.0), (3000, 3000.0)]);
    }

    #[test]
    fn test_drain_up_to() {
        let mut buf = SeriesBuffer::new(100);
        for t in [1000, 2000, 3000, 4000, 5000] {
            buf.insert(t, t as f64);
        }

        let drained = buf.drain_up_to(3000);
        assert_eq!(drained, vec![(1000, 1000.0), (2000, 2000.0)]);
        assert_eq!(buf.len(), 3); // 3000, 4000, 5000 remain
    }

    #[test]
    fn test_max_buffer_enforcement() {
        let mut buf = SeriesBuffer::new(3);
        buf.insert(1000, 1.0);
        buf.insert(2000, 2.0);
        buf.insert(3000, 3.0);
        buf.insert(4000, 4.0); // should evict 1000
        assert_eq!(buf.len(), 3);

        let all = buf.read_range(0, 5000);
        assert_eq!(all, vec![(2000, 2.0), (3000, 3.0), (4000, 4.0)]);
    }

    #[test]
    fn test_dedup_by_timestamp() {
        let mut buf = SeriesBuffer::new(100);
        buf.insert(1000, 1.0);
        buf.insert(1000, 2.0); // same timestamp, overwrites
        assert_eq!(buf.len(), 1);

        let all = buf.read_range(0, 2000);
        assert_eq!(all, vec![(1000, 2.0)]);
    }

    #[test]
    fn test_empty_operations() {
        let buf = SeriesBuffer::new(100);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.read_range(0, 1000), vec![]);
    }
}

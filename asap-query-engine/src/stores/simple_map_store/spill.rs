//! Write-only archive of windows evicted from the in-memory store.
//!
//! One append-only file per aggregation, `agg_<id>.bin`. Each record is, in
//! little-endian u64s: window start ms, window end ms, key length, key bytes
//! (bincode label values; length `NO_KEY` and no bytes for an unkeyed
//! window), accumulator length, accumulator bytes (`serialize_to_bytes`).

use super::common::{EvictedEntry, InternTable};
use dashmap::DashMap;
use lazy_static::lazy_static;
use prometheus::{register_counter, Counter};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use tracing::error;

/// Key length recorded for a window without a label key.
pub const NO_KEY: u64 = u64::MAX;

lazy_static! {
    pub static ref SPILL_WRITE_ERRORS_TOTAL: Counter = register_counter!(
        "asap_store_spill_write_errors_total",
        "Evicted-window batches that could not be written to the spill directory"
    )
    .unwrap();
}

pub struct SpillWriter {
    dir: PathBuf,
    files: DashMap<u64, File>,
}

impl SpillWriter {
    pub fn new(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        // Register now so /metrics reports 0 rather than omitting the counter.
        lazy_static::initialize(&SPILL_WRITE_ERRORS_TOTAL);
        Ok(Self {
            dir,
            files: DashMap::new(),
        })
    }

    /// Appends `evicted` to the aggregation's file. A failure is logged and
    /// counted, not returned: the windows leave memory either way.
    pub fn spill(&self, aggregation_id: u64, intern: &InternTable, evicted: &[EvictedEntry]) {
        if evicted.is_empty() {
            return;
        }
        if let Err(e) = self.append(aggregation_id, intern, evicted) {
            SPILL_WRITE_ERRORS_TOTAL.inc();
            error!(
                "spilling {} evicted entries of aggregation {aggregation_id} failed: {e}",
                evicted.len()
            );
        }
    }

    fn append(
        &self,
        aggregation_id: u64,
        intern: &InternTable,
        evicted: &[EvictedEntry],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = Vec::new();
        for (window, metric_id, aggregate) in evicted {
            buf.extend_from_slice(&window.0.to_le_bytes());
            buf.extend_from_slice(&window.1.to_le_bytes());
            match intern.resolve(*metric_id) {
                Some(key) => put_len_prefixed(&mut buf, &key.serialize_to_bytes()?),
                None => buf.extend_from_slice(&NO_KEY.to_le_bytes()),
            }
            put_len_prefixed(&mut buf, &aggregate.serialize_to_bytes()?);
        }
        let mut file = self.files.entry(aggregation_id).or_try_insert_with(|| {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.dir.join(format!("agg_{aggregation_id}.bin")))
        })?;
        // One write per batch, so a killed process loses at most the batch in flight.
        file.write_all(&buf)?;
        Ok(())
    }
}

fn put_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{
        AggregationType, CleanupPolicy, KeyByLabelValues, LockStrategy, PrecomputedOutput,
        SerializableToSink, StreamingConfig, WindowType,
    };
    use crate::precompute_operators::SumAccumulator;
    use crate::stores::Store;
    use crate::{AggregationConfig, SimpleMapStore};
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// (start, end, key, accumulator bytes) per record.
    type Record = (u64, u64, Option<KeyByLabelValues>, Vec<u8>);

    fn read_records(path: &std::path::Path) -> Vec<Record> {
        let bytes = fs::read(path).unwrap();
        let mut pos = 0;
        let next_u64 = |pos: &mut usize| {
            let value = u64::from_le_bytes(bytes[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            value
        };
        let mut records = Vec::new();
        while pos < bytes.len() {
            let start = next_u64(&mut pos);
            let end = next_u64(&mut pos);
            let key = match next_u64(&mut pos) {
                NO_KEY => None,
                len => {
                    let key = &bytes[pos..pos + len as usize];
                    pos += len as usize;
                    Some(KeyByLabelValues::deserialize_from_bytes(key).unwrap())
                }
            };
            let len = next_u64(&mut pos) as usize;
            records.push((start, end, key, bytes[pos..pos + len].to_vec()));
            pos += len;
        }
        records
    }

    fn store(
        strategy: LockStrategy,
        policy: CleanupPolicy,
        retain: Option<u64>,
        read_count_threshold: Option<u64>,
        spill_dir: &std::path::Path,
    ) -> SimpleMapStore {
        let config = AggregationConfig::new(
            1,
            AggregationType::Sum,
            "".to_string(),
            HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            "".to_string(),
            60_000,
            60_000,
            WindowType::Tumbling,
            "".to_string(),
            "cpu_usage".to_string(),
            retain,
            read_count_threshold,
            None,
            None,
        );
        let streaming = Arc::new(StreamingConfig::new(HashMap::from([(1, config)])));
        SimpleMapStore::new_with_strategy(streaming, policy, strategy)
            .with_spill(Arc::new(SpillWriter::new(spill_dir).unwrap()))
    }

    fn insert(store: &SimpleMapStore, window: u64, key: Option<&str>) {
        let output = PrecomputedOutput::new(
            window * 60_000,
            (window + 1) * 60_000,
            key.map(|k| KeyByLabelValues::new_with_labels(vec![k.to_string()])),
            1,
        );
        store
            .insert_precomputed_output(output, Box::new(SumAccumulator::with_sum(window as f64)))
            .unwrap();
    }

    fn sum_bytes(value: f64) -> Vec<u8> {
        SumAccumulator::with_sum(value)
            .serialize_to_bytes()
            .unwrap()
    }

    #[test]
    fn circular_buffer_spills_each_evicted_window_once() {
        for strategy in [LockStrategy::PerKey, LockStrategy::Global] {
            let dir = tempfile::tempdir().unwrap();
            let store = store(
                strategy,
                CleanupPolicy::CircularBuffer,
                Some(2),
                None,
                dir.path(),
            );
            for window in 0..5 {
                insert(&store, window, Some("host-a"));
            }

            let key = Some(KeyByLabelValues::new_with_labels(vec!["host-a".into()]));
            let expected: Vec<Record> = (0..3)
                .map(|w| {
                    (
                        w * 60_000,
                        (w + 1) * 60_000,
                        key.clone(),
                        sum_bytes(w as f64),
                    )
                })
                .collect();
            assert_eq!(
                read_records(&dir.path().join("agg_1.bin")),
                expected,
                "{strategy:?}"
            );
        }
    }

    #[test]
    fn read_based_spills_windows_read_past_the_threshold() {
        for strategy in [LockStrategy::PerKey, LockStrategy::Global] {
            let dir = tempfile::tempdir().unwrap();
            let store = store(
                strategy,
                CleanupPolicy::ReadBased,
                None,
                Some(1),
                dir.path(),
            );
            insert(&store, 0, None);
            store
                .query_precomputed_output("cpu_usage", 1, 0, 60_000)
                .unwrap();
            // Cleanup runs on insert.
            insert(&store, 1, None);

            assert_eq!(
                read_records(&dir.path().join("agg_1.bin")),
                vec![(0, 60_000, None, sum_bytes(0.0))],
                "{strategy:?}"
            );
        }
    }

    /// The harness fails a run when the counter is missing, so it must be
    /// exported before any write fails.
    #[test]
    fn error_counter_is_exported_once_spill_is_on() {
        let dir = tempfile::tempdir().unwrap();
        SpillWriter::new(dir.path()).unwrap();
        assert!(prometheus::gather()
            .iter()
            .any(|family| family.get_name() == "asap_store_spill_write_errors_total"));
    }

    #[test]
    fn failed_spill_is_counted_and_still_evicts() {
        for strategy in [LockStrategy::PerKey, LockStrategy::Global] {
            let dir = tempfile::tempdir().unwrap();
            let spill_dir = dir.path().join("spill");
            let store = store(
                strategy,
                CleanupPolicy::CircularBuffer,
                Some(1),
                None,
                &spill_dir,
            );
            fs::remove_dir(&spill_dir).unwrap();
            let errors_before = SPILL_WRITE_ERRORS_TOTAL.get();

            insert(&store, 0, None);
            insert(&store, 1, None);

            assert!(
                SPILL_WRITE_ERRORS_TOTAL.get() > errors_before,
                "{strategy:?}"
            );
            let evicted = store
                .query_precomputed_output_exact("cpu_usage", 1, 0, 60_000)
                .unwrap();
            assert!(
                evicted.is_empty(),
                "{strategy:?}: window 0 must leave memory"
            );
        }
    }
}

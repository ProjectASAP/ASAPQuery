use crate::data_model::{
    AggregateCore, CleanupPolicy, KeyByLabelValues, PrecomputedOutput, StreamingConfig,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{debug, error, info};

type TimestampRange = (u64, u64);
type StoreKey = u64;
type StoreValue = Vec<(Option<KeyByLabelValues>, Box<dyn AggregateCore>)>;

/// Legacy single-mutex store kept for benchmarking comparison only.
#[deprecated(note = "Replaced by the epoch-partitioned inverted-index store in global.rs")]
pub struct LegacySimpleMapStoreGlobal {
    lock: Mutex<StoreData>,
    streaming_config: Arc<StreamingConfig>,
    cleanup_policy: CleanupPolicy,
}

struct StoreData {
    store: HashMap<StoreKey, HashMap<TimestampRange, StoreValue>>,
    metrics: HashSet<String>,
    items_inserted: HashMap<String, u64>,
    earliest_timestamp_per_aggregation_id: HashMap<u64, u64>,
    read_counts: HashMap<StoreKey, HashMap<TimestampRange, u64>>,
}

#[allow(deprecated)]
impl LegacySimpleMapStoreGlobal {
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self {
            lock: Mutex::new(StoreData {
                store: HashMap::new(),
                metrics: HashSet::new(),
                items_inserted: HashMap::new(),
                earliest_timestamp_per_aggregation_id: HashMap::new(),
                read_counts: HashMap::new(),
            }),
            streaming_config,
            cleanup_policy,
        }
    }

    fn create_table(&self, data: &mut StoreData, metric: &str) {
        data.metrics.insert(metric.to_string());
    }

    fn cleanup_old_aggregates_fixed_count(
        &self,
        data: &mut StoreData,
        metric: &str,
        aggregation_id: u64,
        num_aggregates_to_retain: Option<u64>,
    ) {
        let configured_limit = match num_aggregates_to_retain {
            Some(limit) => limit as usize,
            None => return,
        };

        let retention_limit = configured_limit * 4;
        let store_key = aggregation_id;

        if let Some(time_map) = data.store.get_mut(&store_key) {
            if time_map.len() <= retention_limit {
                return;
            }

            let mut timestamp_windows: Vec<TimestampRange> = time_map.keys().copied().collect();
            timestamp_windows.sort_by_key(|&(start, _end)| start);

            let num_to_remove = timestamp_windows.len() - retention_limit;
            let windows_to_remove: Vec<TimestampRange> =
                timestamp_windows.into_iter().take(num_to_remove).collect();

            for window in windows_to_remove {
                if time_map.remove(&window).is_some() {
                    debug!(
                        "Removed old aggregate for {} aggregation_id {} window {}-{} (retention limit: {}, configured: {})",
                        metric,
                        aggregation_id,
                        window.0,
                        window.1,
                        retention_limit,
                        configured_limit
                    );
                }
            }
        }
    }

    fn cleanup_old_aggregates_read_based(
        &self,
        data: &mut StoreData,
        metric: &str,
        aggregation_id: u64,
        read_count_threshold: Option<u64>,
    ) {
        let threshold = match read_count_threshold {
            Some(t) => t,
            None => return,
        };

        let store_key = aggregation_id;
        let time_map = match data.store.get_mut(&store_key) {
            Some(map) => map,
            None => return,
        };

        let read_count_map = data.read_counts.entry(store_key).or_default();
        let mut windows_to_remove: Vec<TimestampRange> = Vec::new();

        for timestamp_range in time_map.keys() {
            let read_count = read_count_map.get(timestamp_range).copied().unwrap_or(0);
            if read_count >= threshold {
                windows_to_remove.push(*timestamp_range);
            }
        }

        for window in &windows_to_remove {
            if time_map.remove(window).is_some() {
                let read_count = read_count_map.get(window).copied().unwrap_or(0);
                read_count_map.remove(window);

                debug!(
                    "Removed aggregate for {} aggregation_id {} window {}-{} (read_count: {} >= threshold: {})",
                    metric,
                    aggregation_id,
                    window.0,
                    window.1,
                    read_count,
                    threshold
                );
            }
        }
    }

    fn cleanup_old_aggregates(
        &self,
        data: &mut StoreData,
        metric: &str,
        aggregation_id: u64,
        num_aggregates_to_retain: Option<u64>,
        read_count_threshold: Option<u64>,
    ) {
        match self.cleanup_policy {
            CleanupPolicy::CircularBuffer => self.cleanup_old_aggregates_fixed_count(
                data,
                metric,
                aggregation_id,
                num_aggregates_to_retain,
            ),
            CleanupPolicy::ReadBased => self.cleanup_old_aggregates_read_based(
                data,
                metric,
                aggregation_id,
                read_count_threshold,
            ),
            CleanupPolicy::NoCleanup => {}
        }
    }
}

#[async_trait::async_trait]
#[allow(deprecated)]
impl Store for LegacySimpleMapStoreGlobal {
    fn insert_precomputed_output(
        &self,
        output: PrecomputedOutput,
        precompute: Box<dyn AggregateCore>,
    ) -> StoreResult<()> {
        self.insert_precomputed_output_batch(vec![(output, precompute)])
    }

    fn insert_precomputed_output_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        let batch_insert_start_time = Instant::now();
        let batch_size = outputs.len();

        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        let mut data = self.lock.lock().unwrap();

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Insert lock wait time: {:.2}ms (batch_size: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                batch_size
            );
        }

        #[cfg(feature = "lock_profiling")]
        let lock_hold_start = Instant::now();

        for (output, precompute) in outputs {
            let aggregation_config = self
                .streaming_config
                .get_aggregation_config(output.aggregation_id);

            if aggregation_config.is_none() {
                error!(
                    "Aggregation config not found for aggregation_id {}. Skipping insert.",
                    output.aggregation_id
                );
                continue;
            }
            let aggregation_config = aggregation_config.unwrap();

            let metric = aggregation_config.metric.clone();
            let aggregation_id = output.aggregation_id;

            if !data.metrics.contains(&metric) {
                self.create_table(&mut data, &metric);
            }

            if let Some(current_earliest) = data
                .earliest_timestamp_per_aggregation_id
                .get_mut(&aggregation_id)
            {
                if output.start_timestamp < *current_earliest {
                    *current_earliest = output.start_timestamp;
                }
            } else {
                data.earliest_timestamp_per_aggregation_id
                    .insert(aggregation_id, output.start_timestamp);
            }

            let store_key = aggregation_id;
            let timestamp_range = (output.start_timestamp, output.end_timestamp);
            let time_map = data.store.entry(store_key).or_default();
            let store_value = time_map.entry(timestamp_range).or_default();
            store_value.push((output.key, precompute));

            if aggregation_config.aggregation_type != "DeltaSetAggregator" {
                self.cleanup_old_aggregates(
                    &mut data,
                    &metric,
                    aggregation_id,
                    aggregation_config.num_aggregates_to_retain,
                    aggregation_config.read_count_threshold,
                );
            }

            let current_count = data.items_inserted.entry(metric.clone()).or_insert(0);
            *current_count += 1;

            if (*current_count).is_multiple_of(1000) {
                debug!("Inserted {} items into {}", current_count, metric);
            }
        }

        #[cfg(feature = "lock_profiling")]
        {
            let lock_hold_duration = lock_hold_start.elapsed();
            info!(
                "🔓 Insert lock hold time: {:.2}ms (batch_size: {})",
                lock_hold_duration.as_secs_f64() * 1000.0,
                batch_size
            );
        }

        let batch_insert_duration = batch_insert_start_time.elapsed();
        debug!(
            "Batch insert of {} items took: {:.2}ms",
            batch_size,
            batch_insert_duration.as_secs_f64() * 1000.0
        );
        Ok(())
    }

    fn query_precomputed_output(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        let mut data = self.lock.lock().unwrap();

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Query lock wait time: {:.2}ms (metric: {}, agg_id: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id
            );
        }

        #[cfg(feature = "lock_profiling")]
        let lock_hold_start = Instant::now();

        let time_map = match data.store.get(&store_key) {
            Some(map) => map,
            None => {
                info!("Metric {} not found in store", metric);
                return Ok(HashMap::new());
            }
        };

        let mut results: TimestampedBucketsMap = HashMap::new();
        let mut total_entries = 0;
        let range_scan_start_time = Instant::now();

        let mut matching_ranges: Vec<TimestampRange> = time_map
            .keys()
            .filter(|(range_start, range_end)| start <= *range_start && end >= *range_end)
            .copied()
            .collect();
        matching_ranges.sort_by_key(|(range_start, _)| *range_start);

        for timestamp_range in &matching_ranges {
            if let Some(store_values) = time_map.get(timestamp_range) {
                for (key_opt, precompute) in store_values {
                    results
                        .entry(key_opt.clone())
                        .or_default()
                        .push((*timestamp_range, precompute.clone_boxed_core().into()));
                    total_entries += 1;
                }
            }
        }

        let read_count_map = data.read_counts.entry(store_key).or_default();
        for timestamp_range in &matching_ranges {
            *read_count_map.entry(*timestamp_range).or_insert(0) += 1;
        }

        let range_scan_duration = range_scan_start_time.elapsed();
        debug!(
            "Range scanning took: {:.2}ms",
            range_scan_duration.as_secs_f64() * 1000.0
        );

        let query_duration = query_start_time.elapsed();
        debug!(
            "Total query took: {:.2}ms",
            query_duration.as_secs_f64() * 1000.0
        );

        debug!(
            "Found {} entries for query on {} (aggregation_id: {}, start: {}, end: {})",
            total_entries, metric, aggregation_id, start, end
        );
        debug!("Found {} unique keys", results.len());

        #[cfg(feature = "lock_profiling")]
        {
            let lock_hold_duration = lock_hold_start.elapsed();
            info!(
                "🔓 Query lock hold time: {:.2}ms (metric: {}, agg_id: {}, entries: {})",
                lock_hold_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id,
                total_entries
            );
        }

        Ok(results)
    }

    fn query_precomputed_output_exact(
        &self,
        metric: &str,
        aggregation_id: u64,
        exact_start: u64,
        exact_end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        let mut data = self.lock.lock().unwrap();

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Exact query lock wait time: {:.2}ms (metric: {}, agg_id: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id
            );
        }

        #[cfg(feature = "lock_profiling")]
        let lock_hold_start = Instant::now();

        let time_map = match data.store.get(&store_key) {
            Some(map) => map,
            None => {
                debug!("Metric {} not found in store for exact query", metric);
                return Ok(HashMap::new());
            }
        };

        let mut results: TimestampedBucketsMap = HashMap::new();
        let timestamp_range = (exact_start, exact_end);
        let mut found_match = false;

        if let Some(store_values) = time_map.get(&timestamp_range) {
            found_match = true;
            let mut total_entries = 0;
            for (key_opt, precompute) in store_values {
                results
                    .entry(key_opt.clone())
                    .or_default()
                    .push((timestamp_range, precompute.clone_boxed_core().into()));
                total_entries += 1;
            }

            debug!(
                "Exact match FOUND for [{}, {}]: {} entries across {} keys",
                exact_start,
                exact_end,
                total_entries,
                results.len()
            );
        } else {
            debug!(
                "Exact match NOT FOUND for metric: {}, agg_id: {}, range: [{}, {}]",
                metric, aggregation_id, exact_start, exact_end
            );
        }

        if found_match {
            let read_count_map = data.read_counts.entry(store_key).or_default();
            *read_count_map.entry(timestamp_range).or_insert(0) += 1;
        }

        #[cfg(feature = "lock_profiling")]
        {
            let lock_hold_duration = lock_hold_start.elapsed();
            info!(
                "🔓 Exact query lock hold time: {:.2}ms (metric: {}, agg_id: {}, found: {})",
                lock_hold_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id,
                !results.is_empty()
            );
        }

        let query_duration = query_start_time.elapsed();
        debug!(
            "Exact timestamp query took: {:.2}ms (found: {})",
            query_duration.as_secs_f64() * 1000.0,
            !results.is_empty()
        );

        Ok(results)
    }

    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
        let data = self.lock.lock().unwrap();
        Ok(data.earliest_timestamp_per_aggregation_id.clone())
    }

    fn close(&self) -> StoreResult<()> {
        info!("LegacySimpleMapStoreGlobal closed");
        Ok(())
    }
}

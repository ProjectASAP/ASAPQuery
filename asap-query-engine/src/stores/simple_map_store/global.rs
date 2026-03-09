use crate::data_model::{
    AggregateCore, CleanupPolicy, KeyByLabelValues, PrecomputedOutput, StreamingConfig,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tracing::{debug, error, info};

type TimestampRange = (u64, u64); // (start_timestamp, end_timestamp)
type StoreKey = u64; // aggregation_id
type LabelMap =
    HashMap<Option<KeyByLabelValues>, BTreeMap<TimestampRange, Vec<Arc<dyn AggregateCore>>>>;
type WindowToLabels = HashMap<TimestampRange, HashSet<Option<KeyByLabelValues>>>;

/// In-memory storage implementation using single mutex (like Python version)
pub struct SimpleMapStoreGlobal {
    // Single global mutex protecting all data structures
    lock: Mutex<StoreData>,

    // Store the streaming configuration
    streaming_config: Arc<StreamingConfig>,

    // Policy for cleaning up old aggregates
    cleanup_policy: CleanupPolicy,
}

struct StoreData {
    // Main storage: aggregation_id -> label -> time-sorted aggregates (inverted index)
    store: HashMap<StoreKey, LabelMap>,

    // Reverse index: aggregation_id -> (time range -> labels that contain data for this window)
    window_to_labels: HashMap<StoreKey, WindowToLabels>,

    // Secondary index: all known time ranges per aggregation_id (for cleanup counting/iteration)
    time_ranges: HashMap<StoreKey, BTreeSet<TimestampRange>>,

    // Track metrics that have been created
    metrics: std::collections::HashSet<String>,

    // Count items inserted per metric for logging
    items_inserted: HashMap<String, u64>,

    // Track earliest timestamp per aggregation ID
    earliest_timestamp_per_aggregation_id: HashMap<u64, u64>,

    // Track how many times each aggregate window has been read
    read_counts: HashMap<StoreKey, HashMap<TimestampRange, u64>>,
}

impl SimpleMapStoreGlobal {
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self {
            lock: Mutex::new(StoreData {
                store: HashMap::new(),
                window_to_labels: HashMap::new(),
                time_ranges: HashMap::new(),
                metrics: std::collections::HashSet::new(),
                items_inserted: HashMap::new(),
                earliest_timestamp_per_aggregation_id: HashMap::new(),
                read_counts: HashMap::new(),
            }),
            streaming_config,
            cleanup_policy,
        }
    }

    fn create_table(&self, data: &mut StoreData, metric: &str) {
        // In the in-memory implementation, "creating a table" just means
        // marking the metric as known
        data.metrics.insert(metric.to_string());
    }

    fn remove_windows_from_store_key(
        &self,
        data: &mut StoreData,
        store_key: StoreKey,
        windows_to_remove: &[TimestampRange],
    ) {
        let Some(label_map) = data.store.get_mut(&store_key) else {
            return;
        };

        let Some(window_to_labels) = data.window_to_labels.get_mut(&store_key) else {
            return;
        };

        for window in windows_to_remove {
            let Some(labels) = window_to_labels.remove(window) else {
                continue;
            };

            for label in labels {
                let remove_label = if let Some(btree) = label_map.get_mut(&label) {
                    btree.remove(window);
                    btree.is_empty()
                } else {
                    false
                };

                if remove_label {
                    label_map.remove(&label);
                }
            }
        }
    }

    fn cleanup_old_aggregates_fixed_count(
        &self,
        data: &mut StoreData,
        metric: &str,
        aggregation_id: u64,
        num_aggregates_to_retain: Option<u64>,
    ) {
        // Return early if no retention limit configured
        let configured_limit = match num_aggregates_to_retain {
            Some(limit) => limit as usize,
            None => return,
        };

        let retention_limit = configured_limit.saturating_mul(4);
        let store_key = aggregation_id;

        // Check time_ranges count
        let time_ranges = match data.time_ranges.get(&store_key) {
            Some(tr) => tr,
            None => return,
        };

        if time_ranges.len() <= retention_limit {
            return; // Nothing to clean up
        }

        // Iterate time_ranges from start (already sorted in BTreeSet), take oldest
        let num_to_remove = time_ranges.len() - retention_limit;
        let windows_to_remove: Vec<TimestampRange> =
            time_ranges.iter().copied().take(num_to_remove).collect();

        // Remove from time_ranges
        if let Some(time_ranges) = data.time_ranges.get_mut(&store_key) {
            for window in &windows_to_remove {
                time_ranges.remove(window);
            }
        }

        // Remove from read_counts
        if let Some(read_count_map) = data.read_counts.get_mut(&store_key) {
            for window in &windows_to_remove {
                read_count_map.remove(window);
            }
        }

        // Remove only from labels known to have each removed window.
        self.remove_windows_from_store_key(data, store_key, &windows_to_remove);

        for window in &windows_to_remove {
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

    fn cleanup_old_aggregates_read_based(
        &self,
        data: &mut StoreData,
        metric: &str,
        aggregation_id: u64,
        read_count_threshold: Option<u64>,
    ) {
        // Return early if no threshold configured
        let threshold = match read_count_threshold {
            Some(t) => t,
            None => return,
        };

        let store_key = aggregation_id;

        // Collect windows where read_count >= threshold
        let windows_to_remove: Vec<(TimestampRange, u64)> = data
            .read_counts
            .get(&store_key)
            .map(|read_count_map| {
                read_count_map
                    .iter()
                    .filter(|(_, &count)| count >= threshold)
                    .map(|(range, &count)| (*range, count))
                    .collect()
            })
            .unwrap_or_default();
        let windows_only: Vec<TimestampRange> = windows_to_remove
            .iter()
            .map(|(window, _)| *window)
            .collect();

        if windows_to_remove.is_empty() {
            return;
        }

        // Remove from read_counts
        if let Some(read_count_map) = data.read_counts.get_mut(&store_key) {
            for (window, _) in &windows_to_remove {
                read_count_map.remove(window);
            }
        }

        // Remove from time_ranges
        if let Some(time_ranges) = data.time_ranges.get_mut(&store_key) {
            for (window, _) in &windows_to_remove {
                time_ranges.remove(window);
            }
        }

        // Remove only from labels known to have each removed window.
        self.remove_windows_from_store_key(data, store_key, &windows_only);

        for (window, read_count) in &windows_to_remove {
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

    fn cleanup_old_aggregates(
        &self,
        data: &mut StoreData,
        metric: &str,
        aggregation_id: u64,
        num_aggregates_to_retain: Option<u64>,
        read_count_threshold: Option<u64>,
    ) {
        match self.cleanup_policy {
            CleanupPolicy::CircularBuffer => {
                self.cleanup_old_aggregates_fixed_count(
                    data,
                    metric,
                    aggregation_id,
                    num_aggregates_to_retain,
                );
            }
            CleanupPolicy::ReadBased => {
                self.cleanup_old_aggregates_read_based(
                    data,
                    metric,
                    aggregation_id,
                    read_count_threshold,
                );
            }
            CleanupPolicy::NoCleanup => {
                // Do nothing - no cleanup
            }
        }
    }
}

#[async_trait::async_trait]
impl Store for SimpleMapStoreGlobal {
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

        // Measure lock acquisition time
        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        // Single lock for entire batch (like Python version)
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

            // Create table if it doesn't exist
            if !data.metrics.contains(&metric) {
                self.create_table(&mut data, &metric);
            }

            // Update earliest timestamp tracking
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
            let label_key = output.key;

            // Insert into inverted index: label → BTreeMap<TimestampRange, Vec<Aggregate>>
            let label_map = data.store.entry(store_key).or_default();
            label_map
                .entry(label_key.clone())
                .or_default()
                .entry(timestamp_range)
                .or_default()
                .push(Arc::from(precompute));
            data.window_to_labels
                .entry(store_key)
                .or_default()
                .entry(timestamp_range)
                .or_default()
                .insert(label_key);
            data.time_ranges
                .entry(store_key)
                .or_default()
                .insert(timestamp_range);

            // Apply retention policy if configured (but exclude DeltaSetAggregator)
            if aggregation_config.aggregation_type != "DeltaSetAggregator" {
                self.cleanup_old_aggregates(
                    &mut data,
                    &metric,
                    aggregation_id,
                    aggregation_config.num_aggregates_to_retain,
                    aggregation_config.read_count_threshold,
                );
            }

            // Update insertion count
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

        // Lock will be dropped here when `data` goes out of scope

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
        if start > end {
            debug!(
                "Invalid query range for metric {} agg_id {}: start {} > end {}",
                metric, aggregation_id, start, end
            );
            return Ok(HashMap::new());
        }

        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        // Measure lock acquisition time
        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        // Single lock for entire query
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

        let mut results: TimestampedBucketsMap = HashMap::new();
        let mut total_entries = 0;

        // Find all matching entries using the inverted index (label → BTreeMap)
        let range_scan_start_time = Instant::now();

        {
            let label_map = match data.store.get(&store_key) {
                Some(map) => map,
                None => {
                    info!("Metric {} not found in store", metric);
                    return Ok(HashMap::new());
                }
            };

            for (label, btree) in label_map.iter() {
                for (&timestamp_range, aggregates) in btree.range((start, 0)..=(end, u64::MAX)) {
                    if timestamp_range.1 > end {
                        continue; // Filter: range_end must be <= end
                    }
                    let entry = results.entry(label.clone()).or_default();
                    for agg in aggregates {
                        entry.push((timestamp_range, Arc::clone(agg)));
                        total_entries += 1;
                    }
                }
            }
        }

        // Update read counts using secondary index (after label_map borrow is dropped)
        // Collect matching ranges first to avoid simultaneous borrows on data fields
        let matching_ranges: Vec<TimestampRange> = data
            .time_ranges
            .get(&store_key)
            .map(|time_ranges| {
                time_ranges
                    .range((start, 0)..=(end, u64::MAX))
                    .filter(|&&(_, range_end)| range_end <= end)
                    .copied()
                    .collect()
            })
            .unwrap_or_default();

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

        // Lock will be dropped here when `data` goes out of scope

        Ok(results)
    }

    fn query_precomputed_output_exact(
        &self,
        metric: &str,
        aggregation_id: u64,
        exact_start: u64,
        exact_end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        if exact_start > exact_end {
            debug!(
                "Invalid exact query range for metric {} agg_id {}: start {} > end {}",
                metric, aggregation_id, exact_start, exact_end
            );
            return Ok(HashMap::new());
        }

        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        // Measure lock acquisition time
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

        let mut results: TimestampedBucketsMap = HashMap::new();
        let timestamp_range = (exact_start, exact_end);
        let mut found_match = false;
        let mut total_entries = 0;

        // Fast miss path: avoid scanning all labels if this window does not exist.
        if !data
            .window_to_labels
            .get(&store_key)
            .is_some_and(|index| index.contains_key(&timestamp_range))
        {
            debug!(
                "Exact match NOT FOUND for metric: {}, agg_id: {}, range: [{}, {}]",
                metric, aggregation_id, exact_start, exact_end
            );
            return Ok(HashMap::new());
        }

        // Use reverse index to scan only labels that actually have this window.
        {
            let label_map = match data.store.get(&store_key) {
                Some(map) => map,
                None => {
                    debug!("Metric {} not found in store for exact query", metric);
                    return Ok(HashMap::new());
                }
            };

            if let Some(labels) = data
                .window_to_labels
                .get(&store_key)
                .and_then(|index| index.get(&timestamp_range))
            {
                for label in labels {
                    if let Some(aggregates) = label_map
                        .get(label)
                        .and_then(|btree| btree.get(&timestamp_range))
                    {
                        found_match = true;
                        let entry = results.entry(label.clone()).or_default();
                        for agg in aggregates {
                            entry.push((timestamp_range, Arc::clone(agg)));
                            total_entries += 1;
                        }
                    }
                }
            }

            if found_match {
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
        }

        // Now update read count (after label_map borrow is dropped)
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

        // Lock will be dropped here when `data` goes out of scope

        Ok(results)
    }

    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
        let data = self.lock.lock().unwrap();
        Ok(data.earliest_timestamp_per_aggregation_id.clone())
    }

    fn close(&self) -> StoreResult<()> {
        // For in-memory store, no cleanup needed
        info!("SimpleMapStoreGlobal closed");
        Ok(())
    }
}

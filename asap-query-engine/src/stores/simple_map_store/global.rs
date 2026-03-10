use crate::data_model::{AggregateCore, CleanupPolicy, PrecomputedOutput, StreamingConfig};
use crate::stores::simple_map_store::common::{
    EpochData, EpochID, InternTable, MetricID, TimestampRange,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tracing::{debug, error, info};

type StoreKey = u64; // aggregation_id

/// Per-aggregation_id state within the global store
struct PerKeyState {
    /// Label interning table (Optimization 1)
    intern: InternTable,

    /// Epoch-partitioned storage (Optimization 2)
    epochs: BTreeMap<EpochID, EpochData>,

    /// Current epoch ID (monotonically increasing)
    current_epoch_id: EpochID,

    /// Max distinct time-windows per epoch before opening a new one.
    /// None = unlimited (set on first insert from num_aggregates_to_retain).
    epoch_capacity: Option<usize>,

    /// Max number of epochs to retain (O(1) drop of oldest when exceeded).
    max_epochs: usize,
}

impl PerKeyState {
    fn new() -> Self {
        let mut epochs = BTreeMap::new();
        epochs.insert(0u64, EpochData::new());
        Self {
            intern: InternTable::new(),
            epochs,
            current_epoch_id: 0,
            epoch_capacity: None,
            max_epochs: 4,
        }
    }

    /// Set epoch_capacity on first insert (no-op after first call).
    fn configure_epochs(&mut self, num_aggregates_to_retain: Option<u64>) {
        if self.epoch_capacity.is_none() {
            if let Some(cap) = num_aggregates_to_retain {
                self.epoch_capacity = Some(cap as usize);
            }
        }
    }

    /// O(1) epoch rotation: if current epoch is full, open new epoch and drop oldest if needed.
    /// Returns windows of the dropped epoch (for cleaning up read_counts).
    fn maybe_rotate_epoch(&mut self) -> Vec<TimestampRange> {
        let capacity = match self.epoch_capacity {
            Some(c) if c > 0 => c,
            _ => return Vec::new(), // unlimited
        };

        let current_count = self
            .epochs
            .get(&self.current_epoch_id)
            .map(|e| e.window_count())
            .unwrap_or(0);

        if current_count < capacity {
            return Vec::new();
        }

        // Open new epoch
        let new_epoch_id = self.current_epoch_id + 1;
        self.epochs.insert(new_epoch_id, EpochData::new());
        self.current_epoch_id = new_epoch_id;

        // Drop oldest epoch if we now exceed max_epochs (O(1))
        if self.epochs.len() > self.max_epochs {
            if let Some((&oldest_id, _)) = self.epochs.iter().next() {
                if oldest_id != self.current_epoch_id {
                    if let Some(oldest_epoch) = self.epochs.remove(&oldest_id) {
                        return oldest_epoch.time_ranges.into_iter().collect();
                    }
                }
            }
        }

        Vec::new()
    }

    /// Apply ReadBased cleanup across all epochs.
    #[allow(dead_code)]
    fn cleanup_read_based(
        &mut self,
        read_counts: &mut HashMap<TimestampRange, u64>,
        metric: &str,
        aggregation_id: u64,
        threshold: u64,
    ) {
        let windows_to_remove: Vec<TimestampRange> = read_counts
            .iter()
            .filter(|(_, &count)| count >= threshold)
            .map(|(range, _)| *range)
            .collect();

        if windows_to_remove.is_empty() {
            return;
        }

        for window in &windows_to_remove {
            debug!(
                "Removed aggregate for {} aggregation_id {} window {}-{} (read_count >= threshold: {})",
                metric, aggregation_id, window.0, window.1, threshold
            );
            read_counts.remove(window);
        }

        // Remove from all epochs; drop empty epochs
        for epoch in self.epochs.values_mut() {
            epoch.remove_windows(&windows_to_remove);
        }
        self.epochs.retain(|_, epoch| !epoch.is_empty());

        // Ensure current epoch still exists
        if !self.epochs.contains_key(&self.current_epoch_id) {
            self.epochs.insert(self.current_epoch_id, EpochData::new());
        }
    }
}

struct StoreData {
    /// Per-aggregation_id state (replaces old nested HashMap)
    stores: HashMap<StoreKey, PerKeyState>,

    /// Track metrics that have been created
    metrics: HashSet<String>,

    /// Count items inserted per metric for logging
    items_inserted: HashMap<String, u64>,

    /// Track earliest timestamp per aggregation ID
    earliest_timestamp_per_aggregation_id: HashMap<u64, u64>,

    /// Track how many times each aggregate window has been read (per store key)
    /// No inner Mutex needed — outer Mutex serializes everything.
    read_counts: HashMap<StoreKey, HashMap<TimestampRange, u64>>,
}

/// In-memory storage implementation using single mutex (like Python version)
pub struct SimpleMapStoreGlobal {
    // Single global mutex protecting all data structures
    lock: Mutex<StoreData>,

    // Store the streaming configuration
    streaming_config: Arc<StreamingConfig>,

    // Policy for cleaning up old aggregates
    cleanup_policy: CleanupPolicy,
}

impl SimpleMapStoreGlobal {
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self {
            lock: Mutex::new(StoreData {
                stores: HashMap::new(),
                metrics: HashSet::new(),
                items_inserted: HashMap::new(),
                earliest_timestamp_per_aggregation_id: HashMap::new(),
                read_counts: HashMap::new(),
            }),
            streaming_config,
            cleanup_policy,
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
            let store_key = aggregation_id;

            // Create table if it doesn't exist
            data.metrics.insert(metric.clone());

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

            let timestamp_range = (output.start_timestamp, output.end_timestamp);

            // Get or create PerKeyState
            let per_key = data
                .stores
                .entry(store_key)
                .or_insert_with(PerKeyState::new);

            // Configure epoch capacity on first insert (Optimization 2)
            if aggregation_config.aggregation_type != "DeltaSetAggregator" {
                per_key.configure_epochs(aggregation_config.num_aggregates_to_retain);
            }

            // Intern the label key (Optimization 1)
            let metric_id = per_key.intern.intern(output.key);

            // Insert into current epoch
            let current_epoch_id = per_key.current_epoch_id;
            let epoch = per_key
                .epochs
                .get_mut(&current_epoch_id)
                .expect("current epoch always exists");
            epoch.insert(metric_id, timestamp_range, Arc::from(precompute));

            // Apply retention policy if configured (but exclude DeltaSetAggregator)
            if aggregation_config.aggregation_type != "DeltaSetAggregator" {
                match self.cleanup_policy {
                    CleanupPolicy::CircularBuffer => {
                        // Optimization 2: O(1) epoch rotation
                        let dropped_windows = per_key.maybe_rotate_epoch();
                        if !dropped_windows.is_empty() {
                            if let Some(rc_map) = data.read_counts.get_mut(&store_key) {
                                for window in &dropped_windows {
                                    rc_map.remove(window);
                                }
                            }
                            for window in &dropped_windows {
                                debug!(
                                    "Removed old aggregate for {} aggregation_id {} window {}-{} (epoch rotation)",
                                    metric, aggregation_id, window.0, window.1
                                );
                            }
                        }
                    }
                    CleanupPolicy::ReadBased => {
                        if let Some(threshold) = aggregation_config.read_count_threshold {
                            let rc_map = data.read_counts.entry(store_key).or_default();
                            // We need to temporarily detach to satisfy borrow checker
                            let windows_to_remove: Vec<TimestampRange> = rc_map
                                .iter()
                                .filter(|(_, &count)| count >= threshold)
                                .map(|(range, _)| *range)
                                .collect();

                            if !windows_to_remove.is_empty() {
                                for window in &windows_to_remove {
                                    debug!(
                                        "Removed aggregate for {} aggregation_id {} window {}-{} (read_count >= threshold: {})",
                                        metric, aggregation_id, window.0, window.1, threshold
                                    );
                                    rc_map.remove(window);
                                }

                                let per_key = data.stores.get_mut(&store_key).unwrap();
                                for epoch in per_key.epochs.values_mut() {
                                    epoch.remove_windows(&windows_to_remove);
                                }
                                per_key.epochs.retain(|_, epoch| !epoch.is_empty());
                                if !per_key.epochs.contains_key(&per_key.current_epoch_id) {
                                    let cur_id = per_key.current_epoch_id;
                                    per_key.epochs.insert(cur_id, EpochData::new());
                                }
                            }
                        }
                    }
                    CleanupPolicy::NoCleanup => {
                        // Do nothing
                    }
                }
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

        let mut total_entries = 0;
        let mut matched_windows: Vec<TimestampRange> = Vec::new();

        let range_scan_start_time = Instant::now();

        // Accumulate by MetricID first (no intermediate flat Vec allocation).
        let mut mid: HashMap<MetricID, Vec<(TimestampRange, Arc<dyn AggregateCore>)>> = {
            let per_key = match data.stores.get(&store_key) {
                Some(pk) => pk,
                None => {
                    info!("Metric {} not found in store", metric);
                    return Ok(HashMap::new());
                }
            };

            let mut mid: HashMap<MetricID, Vec<(TimestampRange, Arc<dyn AggregateCore>)>> =
                HashMap::with_capacity(per_key.intern.len());

            for epoch in per_key.epochs.values() {
                // Skip epoch if it has no windows overlapping [start, end]
                if let (Some(&min_tr), Some(&max_tr)) = (
                    epoch.time_ranges.iter().next(),
                    epoch.time_ranges.iter().next_back(),
                ) {
                    if min_tr.0 > end || max_tr.1 < start {
                        continue;
                    }
                } else {
                    continue; // empty epoch
                }

                epoch.range_query_into(start, end, &mut mid, &mut matched_windows);
            }
            mid
        };

        // Resolve MetricIDs → labels in a single pass (scope ends before read_counts borrow)
        let mut results: TimestampedBucketsMap = {
            let per_key = data.stores.get(&store_key).unwrap();
            let mut r = HashMap::with_capacity(mid.len());
            for (metric_id, buckets) in mid.drain() {
                total_entries += buckets.len();
                let label = per_key.intern.resolve(metric_id).clone();
                r.insert(label, buckets);
            }
            r
        };

        // Update read counts (outer Mutex already held — no inner Mutex needed)
        let rc_map = data.read_counts.entry(store_key).or_default();
        for window in &matched_windows {
            *rc_map.entry(*window).or_insert(0) += 1;
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

        {
            let per_key = match data.stores.get(&store_key) {
                Some(pk) => pk,
                None => {
                    debug!("Metric {} not found in store for exact query", metric);
                    return Ok(HashMap::new());
                }
            };

            // Search epochs newest-first for exact window match
            for epoch in per_key.epochs.values().rev() {
                if let Some(entries) = epoch.exact_query(timestamp_range) {
                    found_match = true;
                    for (metric_id, agg) in entries {
                        let label = per_key.intern.resolve(metric_id).clone();
                        results
                            .entry(label)
                            .or_default()
                            .push((timestamp_range, agg));
                        total_entries += 1;
                    }
                    break; // exact match found in newest containing epoch
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

        // Now update read count (outer Mutex held — no inner Mutex needed)
        if found_match {
            let rc_map = data.read_counts.entry(store_key).or_default();
            *rc_map.entry(timestamp_range).or_insert(0) += 1;
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

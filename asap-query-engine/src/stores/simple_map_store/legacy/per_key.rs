use crate::data_model::{
    AggregateCore, CleanupPolicy, KeyByLabelValues, PrecomputedOutput, StreamingConfig,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use dashmap::DashMap;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tracing::{debug, error, info};

type TimestampRange = (u64, u64); // (start_timestamp, end_timestamp)
type StoreKey = u64; // aggregation_id
type LabelMap =
    HashMap<Option<KeyByLabelValues>, BTreeMap<TimestampRange, Vec<Arc<dyn AggregateCore>>>>;
type WindowToLabels = HashMap<TimestampRange, HashSet<Option<KeyByLabelValues>>>;

/// Per-aggregation_id data protected by RwLock
struct StoreKeyData {
    // Primary index: label → time-sorted aggregates (inverted index)
    label_map: LabelMap,

    // Reverse index: time range -> labels that contain data for this window
    window_to_labels: WindowToLabels,

    // Secondary index: all known time ranges (for cleanup counting/iteration)
    time_ranges: BTreeSet<TimestampRange>,

    // Track how many times each timestamp range has been read
    // Behind Mutex so queries can use a read lock on the outer RwLock
    read_counts: Mutex<HashMap<TimestampRange, u64>>,
}

impl StoreKeyData {
    fn new() -> Self {
        Self {
            label_map: HashMap::new(),
            window_to_labels: HashMap::new(),
            time_ranges: BTreeSet::new(),
            read_counts: Mutex::new(HashMap::new()),
        }
    }
}

/// In-memory storage implementation using per-key locks for concurrency
pub struct LegacySimpleMapStorePerKey {
    // Lock-free concurrent outer map - per aggregation_id
    store: DashMap<StoreKey, Arc<RwLock<StoreKeyData>>>,

    // Separate concurrent maps for global state
    earliest_timestamps: DashMap<u64, AtomicU64>,
    metrics: DashMap<String, ()>, // HashSet equivalent
    items_inserted: DashMap<String, AtomicU64>,

    // Store the streaming configuration
    streaming_config: Arc<StreamingConfig>,

    // Policy for cleaning up old aggregates
    cleanup_policy: CleanupPolicy,
}

impl LegacySimpleMapStorePerKey {
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self {
            store: DashMap::new(),
            earliest_timestamps: DashMap::new(),
            metrics: DashMap::new(),
            items_inserted: DashMap::new(),
            streaming_config,
            cleanup_policy,
        }
    }

    fn remove_windows_from_label_index(
        &self,
        data: &mut StoreKeyData,
        windows_to_remove: &[TimestampRange],
    ) {
        for window in windows_to_remove {
            let Some(labels) = data.window_to_labels.remove(window) else {
                continue;
            };

            for label in labels {
                let remove_label = if let Some(btree) = data.label_map.get_mut(&label) {
                    btree.remove(window);
                    btree.is_empty()
                } else {
                    false
                };

                if remove_label {
                    data.label_map.remove(&label);
                }
            }
        }
    }

    fn cleanup_old_aggregates_fixed_count(
        &self,
        data: &mut StoreKeyData,
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

        if data.time_ranges.len() <= retention_limit {
            return; // Nothing to clean up
        }

        // Iterate time_ranges from start (already sorted in BTreeSet), take oldest
        let num_to_remove = data.time_ranges.len() - retention_limit;
        let windows_to_remove: Vec<TimestampRange> = data
            .time_ranges
            .iter()
            .copied()
            .take(num_to_remove)
            .collect();

        // Remove from read_counts (bypass Mutex via get_mut since we have &mut self)
        let read_counts = data.read_counts.get_mut().unwrap();
        for window in &windows_to_remove {
            data.time_ranges.remove(window);
            read_counts.remove(window);
        }

        // Remove only from labels known to have each removed window.
        self.remove_windows_from_label_index(data, &windows_to_remove);

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
        data: &mut StoreKeyData,
        metric: &str,
        aggregation_id: u64,
        read_count_threshold: Option<u64>,
    ) {
        // Return early if no threshold configured
        let threshold = match read_count_threshold {
            Some(t) => t,
            None => return,
        };

        // Access read_counts directly (bypass Mutex via get_mut since we have &mut self)
        let read_counts = data.read_counts.get_mut().unwrap();

        // Collect windows where read_count >= threshold
        let windows_to_remove: Vec<(TimestampRange, u64)> = read_counts
            .iter()
            .filter(|(_, &count)| count >= threshold)
            .map(|(range, &count)| (*range, count))
            .collect();
        let windows_only: Vec<TimestampRange> = windows_to_remove
            .iter()
            .map(|(window, _)| *window)
            .collect();

        // Remove from read_counts and time_ranges
        for (window, read_count) in &windows_to_remove {
            read_counts.remove(window);
            data.time_ranges.remove(window);

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

        // Remove only from labels known to have each removed window.
        self.remove_windows_from_label_index(data, &windows_only);
    }

    fn cleanup_old_aggregates(
        &self,
        data: &mut StoreKeyData,
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

    fn insert_for_store_key(
        &self,
        store_key: &StoreKey,
        metric: &str,
        items: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        let aggregation_id = *store_key;
        let metric_key = metric.to_string();
        let inserted_delta = items.len() as u64;

        // Measure lock acquisition time
        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        // Get or create the store data for this key
        let store_data_lock = self
            .store
            .entry(*store_key)
            .or_insert_with(|| Arc::new(RwLock::new(StoreKeyData::new())));

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Insert DashMap get time: {:.2}ms (metric: {}, agg_id: {}, items: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key,
                items.len()
            );
        }

        #[cfg(feature = "lock_profiling")]
        let rwlock_wait_start = Instant::now();

        // Acquire write lock for this aggregation_id only
        let mut data = store_data_lock.write().map_err(|e| {
            format!(
                "Failed to acquire write lock for aggregation_id {}: {}",
                store_key, e
            )
        })?;

        #[cfg(feature = "lock_profiling")]
        {
            let rwlock_wait_duration = rwlock_wait_start.elapsed();
            info!(
                "🔒 Insert RwLock wait time: {:.2}ms (metric: {}, agg_id: {}, items: {})",
                rwlock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key,
                items.len()
            );
        }

        #[cfg(feature = "lock_profiling")]
        let lock_hold_start = Instant::now();

        // Create metric if needed (lock-free DashMap insert)
        self.metrics.entry(metric_key.clone()).or_insert(());

        // Update insertion counter once per grouped batch (instead of once per item).
        let items_inserted_counter = self
            .items_inserted
            .entry(metric_key)
            .or_insert_with(|| AtomicU64::new(0));
        let previous_total = items_inserted_counter.fetch_add(inserted_delta, Ordering::Relaxed);
        let new_total = previous_total + inserted_delta;
        if new_total / 1000 > previous_total / 1000 {
            debug!("Inserted {} items into {}", new_total, metric);
        }

        for (output, precompute) in items {
            // Update earliest timestamp (lock-free atomic operation)
            self.earliest_timestamps
                .entry(aggregation_id)
                .and_modify(|earliest| {
                    earliest.fetch_min(output.start_timestamp, Ordering::Relaxed);
                })
                .or_insert_with(|| AtomicU64::new(output.start_timestamp));

            // Insert into inverted index: label → BTreeMap<TimestampRange, Vec<Aggregate>>
            let timestamp_range = (output.start_timestamp, output.end_timestamp);
            let label_key = output.key;
            data.label_map
                .entry(label_key.clone())
                .or_default()
                .entry(timestamp_range)
                .or_default()
                .push(Arc::from(precompute));
            data.window_to_labels
                .entry(timestamp_range)
                .or_default()
                .insert(label_key);
            data.time_ranges.insert(timestamp_range);
        }

        // Apply retention policy if configured (but exclude DeltaSetAggregator)
        let aggregation_config = self
            .streaming_config
            .get_aggregation_config(aggregation_id)
            .ok_or_else(|| format!("Aggregation config not found for {}", aggregation_id))?;

        if aggregation_config.aggregation_type != "DeltaSetAggregator" {
            self.cleanup_old_aggregates(
                &mut data,
                metric,
                aggregation_id,
                aggregation_config.num_aggregates_to_retain,
                aggregation_config.read_count_threshold,
            );
        }

        #[cfg(feature = "lock_profiling")]
        {
            let lock_hold_duration = lock_hold_start.elapsed();
            info!(
                "🔓 Insert lock hold time: {:.2}ms (metric: {}, agg_id: {})",
                lock_hold_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key
            );
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Store for LegacySimpleMapStorePerKey {
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

        // Group by aggregation_id
        #[allow(clippy::type_complexity)]
        let mut grouped: HashMap<
            StoreKey,
            (String, Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>),
        > = HashMap::new();

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
            let store_key = output.aggregation_id;

            grouped
                .entry(store_key)
                .or_insert_with(|| (metric.clone(), Vec::new()))
                .1
                .push((output, precompute));
        }

        // Process each aggregation_id group; each iteration locks at most one key.
        for (store_key, (metric, items)) in grouped {
            self.insert_for_store_key(&store_key, &metric, items)?;
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

        // Get the store data for this aggregation_id
        let store_data_lock = match self.store.get(&store_key) {
            Some(lock) => lock,
            None => {
                info!("Metric {} not found in store", metric);
                return Ok(HashMap::new());
            }
        };

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Query DashMap get time: {:.2}ms (metric: {}, agg_id: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id
            );
        }

        #[cfg(feature = "lock_profiling")]
        let rwlock_wait_start = Instant::now();

        // Acquire read lock (read_counts behind inner Mutex)
        let data = store_data_lock.read().map_err(|e| {
            format!(
                "Failed to acquire read lock for query aggregation_id {}: {}",
                store_key, e
            )
        })?;

        #[cfg(feature = "lock_profiling")]
        {
            let rwlock_wait_duration = rwlock_wait_start.elapsed();
            info!(
                "🔒 Query RwLock wait time: {:.2}ms (metric: {}, agg_id: {})",
                rwlock_wait_duration.as_secs_f64() * 1000.0,
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

        for (label, btree) in data.label_map.iter() {
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

        // Update read counts using secondary index (lock inner Mutex briefly)
        {
            let mut read_counts = data.read_counts.lock().unwrap();
            for &timestamp_range in data.time_ranges.range((start, 0)..=(end, u64::MAX)) {
                if timestamp_range.1 > end {
                    continue;
                }
                *read_counts.entry(timestamp_range).or_insert(0) += 1;
            }
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

        // Get the store data for this aggregation_id
        let store_data_lock = match self.store.get(&store_key) {
            Some(lock) => lock,
            None => {
                debug!("Metric {} not found in store for exact query", metric);
                return Ok(HashMap::new());
            }
        };

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Exact query DashMap get time: {:.2}ms (metric: {}, agg_id: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id
            );
        }

        #[cfg(feature = "lock_profiling")]
        let rwlock_wait_start = Instant::now();

        // Acquire read lock (read_counts behind inner Mutex)
        let data = store_data_lock.read().map_err(|e| {
            format!(
                "Failed to acquire read lock for exact query aggregation_id {}: {}",
                store_key, e
            )
        })?;

        #[cfg(feature = "lock_profiling")]
        {
            let rwlock_wait_duration = rwlock_wait_start.elapsed();
            info!(
                "🔒 Exact query RwLock wait time: {:.2}ms (metric: {}, agg_id: {})",
                rwlock_wait_duration.as_secs_f64() * 1000.0,
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
        if !data.window_to_labels.contains_key(&timestamp_range) {
            debug!(
                "Exact match NOT FOUND for metric: {}, agg_id: {}, range: [{}, {}]",
                metric, aggregation_id, exact_start, exact_end
            );
            return Ok(HashMap::new());
        }

        // Use reverse index to scan only labels that actually have this window.
        if let Some(labels) = data.window_to_labels.get(&timestamp_range) {
            for label in labels {
                if let Some(aggregates) = data
                    .label_map
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

        // Update read count (lock inner Mutex briefly)
        if found_match {
            let mut read_counts = data.read_counts.lock().unwrap();
            *read_counts.entry(timestamp_range).or_insert(0) += 1;
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
        // No lock needed - DashMap with AtomicU64
        let result = self
            .earliest_timestamps
            .iter()
            .map(|entry| (*entry.key(), entry.value().load(Ordering::Relaxed)))
            .collect();

        Ok(result)
    }

    fn close(&self) -> StoreResult<()> {
        // For in-memory store, no cleanup needed
        info!("LegacySimpleMapStorePerKey closed");
        Ok(())
    }
}

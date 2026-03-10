use crate::data_model::{
    AggregateCore, CleanupPolicy, KeyByLabelValues, PrecomputedOutput, StreamingConfig,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::{debug, error, info};

type TimestampRange = (u64, u64); // (start_timestamp, end_timestamp)
type StoreKey = u64; // aggregation_id
type StoreValue = Vec<(Option<KeyByLabelValues>, Box<dyn AggregateCore>)>;

/// Per-aggregation_id data protected by RwLock
struct StoreKeyData {
    // Main storage: (start_time, end_time) -> [(key, precompute)]
    time_map: HashMap<TimestampRange, StoreValue>,

    // Track how many times each timestamp range has been read
    read_counts: HashMap<TimestampRange, u64>,
}

impl StoreKeyData {
    fn new() -> Self {
        Self {
            time_map: HashMap::new(),
            read_counts: HashMap::new(),
        }
    }
}

/// In-memory storage implementation using per-key locks for concurrency
/// Legacy time-primary store — kept for benchmarking comparison only.
#[deprecated(note = "Replaced by the epoch-partitioned inverted-index store in per_key.rs")]
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

        let retention_limit = configured_limit * 4;

        if data.time_map.len() <= retention_limit {
            return; // Nothing to clean up
        }

        // Collect all timestamp ranges and sort by start timestamp (oldest first)
        let mut timestamp_windows: Vec<TimestampRange> = data.time_map.keys().copied().collect();
        timestamp_windows.sort_by_key(|&(start, _end)| start);

        // Calculate which ones to remove (oldest first)
        let num_to_remove = timestamp_windows.len() - retention_limit;
        let windows_to_remove: Vec<TimestampRange> =
            timestamp_windows.into_iter().take(num_to_remove).collect();

        // Remove old windows from both time_map and read_counts
        for window in windows_to_remove {
            if data.time_map.remove(&window).is_some() {
                data.read_counts.remove(&window); // Also remove from read_counts
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

        // Collect windows where read_count >= threshold
        let mut windows_to_remove: Vec<TimestampRange> = Vec::new();

        for (timestamp_range, _) in data.time_map.iter() {
            let read_count = data.read_counts.get(timestamp_range).copied().unwrap_or(0);

            if read_count >= threshold {
                windows_to_remove.push(*timestamp_range);
            }
        }

        // Remove windows that exceeded threshold
        for window in &windows_to_remove {
            //if let Some(_) = data.time_map.remove(window) {
            if data.time_map.remove(window).is_some() {
                let read_count = data.read_counts.get(window).copied().unwrap_or(0);
                data.read_counts.remove(window);

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

        for (output, precompute) in items {
            // Create metric if needed (lock-free DashMap insert)
            self.metrics.entry(metric.to_string()).or_insert(());

            // Update earliest timestamp (lock-free atomic operation)
            self.earliest_timestamps
                .entry(aggregation_id)
                .and_modify(|earliest| {
                    let current = earliest.load(Ordering::Relaxed);
                    if output.start_timestamp < current {
                        earliest.store(output.start_timestamp, Ordering::Relaxed);
                    }
                })
                .or_insert_with(|| AtomicU64::new(output.start_timestamp));

            // Insert into time map
            let timestamp_range = (output.start_timestamp, output.end_timestamp);
            data.time_map
                .entry(timestamp_range)
                .or_default()
                .push((output.key, precompute));

            // Update insertion count (lock-free atomic increment)
            self.items_inserted
                .entry(metric.to_string())
                .and_modify(|count| {
                    let new_count = count.fetch_add(1, Ordering::Relaxed) + 1;
                    if new_count.is_multiple_of(1000) {
                        debug!("Inserted {} items into {}", new_count, metric);
                    }
                })
                .or_insert_with(|| AtomicU64::new(1));
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

        // Sort keys to avoid deadlock when acquiring multiple locks
        let mut keys: Vec<_> = grouped.keys().cloned().collect();
        keys.sort();

        // Process each group
        for store_key in keys {
            let (metric, items) = grouped.remove(&store_key).unwrap();
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

        // Acquire write lock (needed to update read_counts)
        let mut data = store_data_lock.write().map_err(|e| {
            format!(
                "Failed to acquire write lock for query aggregation_id {}: {}",
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

        // Find all timestamp ranges that overlap with our query range
        let range_scan_start_time = Instant::now();

        // First, collect all matching timestamp ranges
        let mut matching_ranges: Vec<TimestampRange> = data
            .time_map
            .keys()
            .filter(|(range_start, range_end)| start <= *range_start && end >= *range_end)
            .copied()
            .collect();

        // Sort by start timestamp to ensure chronological order
        // This is important for range queries that use sliding windows
        matching_ranges.sort_by_key(|(range_start, _)| *range_start);

        // Now iterate in sorted order, including timestamp with each bucket
        for timestamp_range in &matching_ranges {
            if let Some(store_values) = data.time_map.get(timestamp_range) {
                for (key_opt, precompute) in store_values.iter() {
                    results
                        .entry(key_opt.clone())
                        .or_default()
                        .push((*timestamp_range, precompute.clone_boxed_core().into()));

                    total_entries += 1;
                }
            }
        }

        // Update read counts for accessed ranges
        for timestamp_range in &matching_ranges {
            *data.read_counts.entry(*timestamp_range).or_insert(0) += 1;
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

        // Acquire write lock (needed to update read_counts)
        let mut data = store_data_lock.write().map_err(|e| {
            format!(
                "Failed to acquire write lock for exact query aggregation_id {}: {}",
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

        // Look for exact timestamp match (strict - no tolerance)
        let timestamp_range = (exact_start, exact_end);
        let mut found_match = false;

        // First, collect the results (immutable borrow of time_map)
        if let Some(store_values) = data.time_map.get(&timestamp_range) {
            found_match = true;

            // Collect results with timestamp
            let mut total_entries = 0;
            for (key_opt, precompute) in store_values.iter() {
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

        // Now update read count (mutable borrow of data.read_counts)
        if found_match {
            *data.read_counts.entry(timestamp_range).or_insert(0) += 1;
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
        info!("SimpleMapStorePerKey closed");
        Ok(())
    }
}

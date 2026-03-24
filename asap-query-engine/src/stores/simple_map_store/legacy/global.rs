use crate::data_model::{
    AggregateCore, CleanupPolicy, KeyByLabelValues, PrecomputedOutput, StreamingConfig,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tracing::{debug, error, info};

type TimestampRange = (u64, u64); // (start_timestamp, end_timestamp)
type StoreKey = u64; // aggregation_id
type StoreValue = Vec<(Option<KeyByLabelValues>, Box<dyn AggregateCore>)>;

/// In-memory storage implementation using single mutex (like Python version)
pub struct LegacySimpleMapStoreGlobal {
    // Single global mutex protecting all data structures
    lock: Mutex<StoreData>,

    // Store the streaming configuration
    streaming_config: Arc<StreamingConfig>,

    // Policy for cleaning up old aggregates
    cleanup_policy: CleanupPolicy,
}

struct StoreData {
    // Main storage: aggregation_id -> (start_time, end_time) -> [(key, precompute)]
    store: HashMap<StoreKey, HashMap<TimestampRange, StoreValue>>,

    // Track metrics that have been created
    metrics: std::collections::HashSet<String>,

    // Count items inserted per metric for logging
    items_inserted: HashMap<String, u64>,

    // Track earliest timestamp per aggregation ID
    earliest_timestamp_per_aggregation_id: HashMap<u64, u64>,

    // Track how many times each aggregate window has been read
    read_counts: HashMap<StoreKey, HashMap<TimestampRange, u64>>,
}

impl LegacySimpleMapStoreGlobal {
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self {
            lock: Mutex::new(StoreData {
                store: HashMap::new(),
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

        let retention_limit = configured_limit * 4;
        let store_key = aggregation_id;

        // Get the time map for this store key
        if let Some(time_map) = data.store.get_mut(&store_key) {
            if time_map.len() <= retention_limit {
                return; // Nothing to clean up
            }

            // Collect all timestamp ranges and sort by start timestamp (oldest first)
            let mut timestamp_windows: Vec<TimestampRange> = time_map.keys().copied().collect();
            timestamp_windows.sort_by_key(|&(start, _end)| start);

            // Calculate which ones to remove (oldest first)
            let num_to_remove = timestamp_windows.len() - retention_limit;
            let windows_to_remove: Vec<TimestampRange> =
                timestamp_windows.into_iter().take(num_to_remove).collect();

            // Remove old windows
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
        // Return early if no threshold configured
        let threshold = match read_count_threshold {
            Some(t) => t,
            None => return,
        };

        let store_key = aggregation_id;

        // Get both the time map and read count map
        let time_map = match data.store.get_mut(&store_key) {
            Some(map) => map,
            None => return,
        };

        let read_count_map = data.read_counts.entry(store_key).or_default();

        // Collect windows where read_count >= threshold
        let mut windows_to_remove: Vec<TimestampRange> = Vec::new();

        for (timestamp_range, _) in time_map.iter() {
            let read_count = read_count_map.get(timestamp_range).copied().unwrap_or(0);

            if read_count >= threshold {
                windows_to_remove.push(*timestamp_range);
            }
        }

        // Remove windows that exceeded threshold
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

            // Get or create the time-based map for this aggregation
            let time_map = data.store.entry(store_key).or_default();

            // Get or create the value vector for this timestamp range
            let store_value = time_map.entry(timestamp_range).or_default();

            // Add the new entry with the real precompute data
            store_value.push((output.key, precompute));

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
        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        // Measure lock acquisition time
        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        // Single lock for entire query - now mutable to track read counts
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

        // Find all timestamp ranges that overlap with our query range
        let range_scan_start_time = Instant::now();

        // First, collect all matching timestamp ranges
        let mut matching_ranges: Vec<TimestampRange> = time_map
            .keys()
            .filter(|(range_start, range_end)| start <= *range_start && end >= *range_end)
            .copied()
            .collect();

        // Sort by start timestamp to ensure chronological order
        // This is important for range queries that use sliding windows
        matching_ranges.sort_by_key(|(range_start, _)| *range_start);

        // Now iterate in sorted order, including timestamp with each bucket
        for timestamp_range in &matching_ranges {
            if let Some(store_values) = time_map.get(timestamp_range) {
                for (key_opt, precompute) in store_values.iter() {
                    results
                        .entry(key_opt.clone())
                        .or_default()
                        .push((*timestamp_range, precompute.clone_boxed_core().into()));

                    total_entries += 1;
                }
            }
        }

        // Update read counts for accessed ranges (after we're done with time_map to avoid borrow conflicts)
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

        let time_map = match data.store.get(&store_key) {
            Some(map) => map,
            None => {
                debug!("Metric {} not found in store for exact query", metric);
                return Ok(HashMap::new());
            }
        };

        let mut results: TimestampedBucketsMap = HashMap::new();

        // Look for exact timestamp match (strict - no tolerance)
        let timestamp_range = (exact_start, exact_end);
        let mut found_match = false;

        // First, collect the results (immutable borrow of time_map)
        if let Some(store_values) = time_map.get(&timestamp_range) {
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
        // This happens after we're done with time_map
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
        info!("LegacySimpleMapStoreGlobal closed");
        Ok(())
    }
}

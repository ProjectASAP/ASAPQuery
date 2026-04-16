use crate::data_model::{
    AggregateCore, AggregationType, CleanupPolicy, PrecomputedOutput, StreamingConfig,
};
use crate::stores::simple_map_store::common::{
    EpochID, InternTable, MetricBucketMap, MutableEpoch, SealedEpoch, TimestampRange,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tracing::{debug, error, info};

type StoreKey = u64; // aggregation_id

/// Per-aggregation_id state within the global store
struct PerKeyState {
    /// Label interning table (Optimization 1)
    intern: InternTable,

    /// Active epoch — always present, accepts inserts.
    current_epoch: MutableEpoch,

    /// Sealed (immutable) epochs stored as flat sorted Vecs (Optimization 2).
    sealed_epochs: BTreeMap<EpochID, SealedEpoch>,

    /// Monotonically increasing ID of the current epoch.
    current_epoch_id: EpochID,

    /// Max distinct time-windows per epoch before sealing.
    /// None = unlimited (set on first insert from num_aggregates_to_retain).
    epoch_capacity: Option<usize>,

    /// Max total epochs (1 current + sealed) to retain.
    max_epochs: usize,
}

impl PerKeyState {
    fn new() -> Self {
        Self {
            intern: InternTable::new(),
            current_epoch: MutableEpoch::new(),
            sealed_epochs: BTreeMap::new(),
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

    /// Seal the current epoch when full, then evict the minimum number of oldest windows
    /// to keep total distinct windows ≤ `epoch_capacity * max_epochs`.
    /// Returns the evicted windows so the caller can clean up `read_counts`.
    fn maybe_rotate_epoch(&mut self) -> Vec<TimestampRange> {
        let capacity = match self.epoch_capacity {
            Some(c) if c > 0 => c,
            _ => return Vec::new(), // unlimited
        };
        let retention_limit = capacity * self.max_epochs;

        // Step 1: seal current epoch if it has hit the window capacity threshold.
        if self.current_epoch.window_count() >= capacity {
            let hint = self.current_epoch.len();
            let old = std::mem::replace(&mut self.current_epoch, MutableEpoch::with_capacity(hint));
            self.sealed_epochs.insert(self.current_epoch_id, old.seal());
            self.current_epoch_id += 1;
        }

        // Step 2: evict oldest windows until total distinct windows ≤ retention_limit.
        let total: usize = self.current_epoch.window_count()
            + self
                .sealed_epochs
                .values()
                .map(|e| e.distinct_window_count())
                .sum::<usize>();

        if total <= retention_limit {
            return Vec::new();
        }
        let mut over = total - retention_limit;
        let mut evicted = Vec::new();

        while over > 0 {
            let oldest_id = match self.sealed_epochs.keys().next().copied() {
                Some(id) => id,
                None => break,
            };
            let oldest_windows = self.sealed_epochs[&oldest_id].unique_windows();
            let n_evict = over.min(oldest_windows.len());
            let to_remove = oldest_windows[..n_evict].to_vec();
            over -= n_evict;
            evicted.extend_from_slice(&to_remove);

            if n_evict == oldest_windows.len() {
                self.sealed_epochs.remove(&oldest_id);
            } else {
                self.sealed_epochs
                    .get_mut(&oldest_id)
                    .unwrap()
                    .remove_windows(&to_remove);
            }
        }
        evicted
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

    // Store the streaming configuration.
    // RwLock<Arc<StreamingConfig>>: readers briefly lock to clone the Arc pointer,
    // then use the Arc without holding the lock.
    streaming_config: RwLock<Arc<StreamingConfig>>,

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
            streaming_config: RwLock::new(streaming_config),
            cleanup_policy,
        }
    }

    /// Replace the streaming config at runtime.
    pub fn update_streaming_config(&self, new_config: StreamingConfig) {
        *self.streaming_config.write().unwrap() = Arc::new(new_config);
    }

    /// Collect diagnostic info about store contents.
    pub fn diagnostic_info(&self) -> super::StoreDiagnostics {
        use super::{AggregationDiagnostic, StoreDiagnostics};

        let data = self.lock.lock().unwrap();
        let mut per_aggregation = Vec::new();
        let mut total_time_map_entries: usize = 0;
        let total_sketch_bytes: usize = 0;

        for (&agg_id, per_key) in &data.stores {
            let time_map_len = per_key.current_epoch.window_count()
                + per_key
                    .sealed_epochs
                    .values()
                    .map(|e| e.distinct_window_count())
                    .sum::<usize>();
            let read_counts_len = data
                .read_counts
                .get(&agg_id)
                .map(|rc| rc.len())
                .unwrap_or(0);
            total_time_map_entries += time_map_len;

            let num_aggregate_objects = per_key.current_epoch.len()
                + per_key
                    .sealed_epochs
                    .values()
                    .map(|e| e.entries.len())
                    .sum::<usize>();

            per_aggregation.push(AggregationDiagnostic {
                aggregation_id: agg_id,
                time_map_len,
                read_counts_len,
                num_aggregate_objects,
                sketch_bytes: 0, // skip serialization for diagnostics
            });
        }

        StoreDiagnostics {
            num_aggregations: data.stores.len(),
            total_time_map_entries,
            total_sketch_bytes,
            per_aggregation,
        }
    }
}

/// Extracted config fields needed inside the locked batch loop.
type GroupedBatch = HashMap<
    StoreKey,
    (
        BatchConfig,
        u64,
        Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ),
>;

/// Pre-computed outside the lock to avoid per-item config lookups (Opt 4).
struct BatchConfig {
    metric: String,
    is_delta: bool,
    num_aggregates_to_retain: Option<u64>,
    read_count_threshold: Option<u64>,
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

        // Opt 4: Pre-group by aggregation_id and resolve config BEFORE acquiring the lock.
        // Config lookups (streaming_config HashMap access) are moved out of the hot locked
        // loop: each unique aggregation_id pays one lookup regardless of batch size.
        // Also pre-compute batch_min_ts per group to collapse N earliest-ts updates into 1.
        let mut grouped: GroupedBatch = HashMap::new();

        let sc = self.streaming_config.read().unwrap().clone();
        for (output, precompute) in outputs {
            let aggregation_config = sc.get_aggregation_config(output.aggregation_id);

            if aggregation_config.is_none() {
                error!(
                    "Aggregation config not found for aggregation_id {}. Skipping insert.",
                    output.aggregation_id
                );
                continue;
            }
            let aggregation_config = aggregation_config.unwrap();

            let store_key = output.aggregation_id;
            let ts = output.start_timestamp;

            let entry = grouped.entry(store_key).or_insert_with(|| {
                (
                    BatchConfig {
                        metric: aggregation_config.metric.clone(),
                        is_delta: aggregation_config.aggregation_type
                            == AggregationType::DeltaSetAggregator,
                        num_aggregates_to_retain: aggregation_config.num_aggregates_to_retain,
                        read_count_threshold: aggregation_config.read_count_threshold,
                    },
                    u64::MAX,
                    Vec::new(),
                )
            });
            // Track batch minimum timestamp for earliest-ts update (Opt 4)
            entry.1 = entry.1.min(ts);
            entry.2.push((output, precompute));
        }

        // Measure lock acquisition time
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

        for (store_key, (cfg, batch_min_ts, items)) in grouped {
            // Opt 4: one metrics insert per group (was one per item)
            data.metrics.insert(cfg.metric.clone());

            // Opt 4: one earliest-ts update per group using the pre-computed batch minimum
            let entry = data
                .earliest_timestamp_per_aggregation_id
                .entry(store_key)
                .or_insert(batch_min_ts);
            *entry = (*entry).min(batch_min_ts);

            let batch_len = items.len() as u64;

            // Ensure PerKeyState exists and configure epoch capacity once per group (Opt 4).
            // configure_epochs is a no-op after the first call, so calling it once here
            // avoids the is_none() check on every inner iteration.
            {
                let per_key = data
                    .stores
                    .entry(store_key)
                    .or_insert_with(PerKeyState::new);
                if !cfg.is_delta {
                    per_key.configure_epochs(cfg.num_aggregates_to_retain);
                }
            } // per_key borrow ends here

            for (output, precompute) in items {
                // Get per_key fresh each iteration so the borrow of data.stores ends before
                // the cleanup branches borrow data.read_counts (different field — NLL splits
                // them, but only if the per_key borrow scope is confined to each iteration).
                let per_key = data.stores.get_mut(&store_key).unwrap();

                // Intern the label key (Optimization 1)
                let timestamp_range = (output.start_timestamp, output.end_timestamp);
                let metric_id = per_key.intern.intern(output.key);

                // Insert into current (mutable) epoch.
                per_key
                    .current_epoch
                    .insert(metric_id, timestamp_range, Arc::from(precompute));

                // Apply retention policy if configured (but exclude DeltaSetAggregator).
                // per_key is last used above; NLL ends its borrow so data.read_counts can
                // be accessed in the cleanup branches below.
                if !cfg.is_delta {
                    match self.cleanup_policy {
                        CleanupPolicy::CircularBuffer => {
                            let dropped_windows = data
                                .stores
                                .get_mut(&store_key)
                                .unwrap()
                                .maybe_rotate_epoch();
                            if !dropped_windows.is_empty() {
                                if let Some(rc_map) = data.read_counts.get_mut(&store_key) {
                                    for window in &dropped_windows {
                                        rc_map.remove(window);
                                    }
                                }
                                for window in &dropped_windows {
                                    debug!(
                                        "Removed old aggregate for {} aggregation_id {} window {}-{} (epoch rotation)",
                                        cfg.metric, store_key, window.0, window.1
                                    );
                                }
                            }
                        }
                        CleanupPolicy::ReadBased => {
                            if let Some(threshold) = cfg.read_count_threshold {
                                let rc_map = data.read_counts.entry(store_key).or_default();
                                let windows_to_remove: Vec<TimestampRange> = rc_map
                                    .iter()
                                    .filter(|(_, &count)| count >= threshold)
                                    .map(|(range, _)| *range)
                                    .collect();

                                if !windows_to_remove.is_empty() {
                                    for window in &windows_to_remove {
                                        debug!(
                                            "Removed aggregate for {} aggregation_id {} window {}-{} (read_count >= threshold: {})",
                                            cfg.metric, store_key, window.0, window.1, threshold
                                        );
                                        rc_map.remove(window);
                                    }

                                    let per_key = data.stores.get_mut(&store_key).unwrap();
                                    per_key.current_epoch.remove_windows(&windows_to_remove);
                                    per_key.sealed_epochs.retain(|_, epoch| {
                                        epoch.remove_windows(&windows_to_remove);
                                        !epoch.is_empty()
                                    });
                                }
                            }
                        }
                        CleanupPolicy::NoCleanup => {}
                    }
                }
            }

            // Opt 4: one count update per group (was one per item)
            let current_count = data.items_inserted.entry(cfg.metric.clone()).or_insert(0);
            let old_count = *current_count;
            *current_count += batch_len;
            if *current_count / 1000 > old_count / 1000 {
                debug!("Inserted {} items into {}", current_count, cfg.metric);
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

        let mut mid: MetricBucketMap = {
            let per_key = match data.stores.get(&store_key) {
                Some(pk) => pk,
                None => {
                    info!("Metric {} not found in store", metric);
                    return Ok(HashMap::new());
                }
            };

            let mut mid: MetricBucketMap = HashMap::with_capacity(per_key.intern.len());

            // Query current (mutable) epoch.
            if let Some((min_start, max_end)) = per_key.current_epoch.time_bounds() {
                if !(min_start > end || max_end < start) {
                    per_key.current_epoch.range_query_into(
                        start,
                        end,
                        &mut mid,
                        &mut matched_windows,
                    );
                }
            }

            // Query sealed epochs; skip those with no overlap.
            for epoch in per_key.sealed_epochs.values() {
                let Some((min_start, max_end)) = epoch.time_bounds() else {
                    continue;
                };
                if min_start > end || max_end < start {
                    continue;
                }
                epoch.range_query_into(start, end, &mut mid, &mut matched_windows);
            }

            mid
        };

        // Resolve MetricIDs → labels in a single pass (scope ends before read_counts borrow)
        let results: TimestampedBucketsMap = {
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

        let timestamp_range = (exact_start, exact_end);

        // Opt 1: exact_query now takes &mut self (lazy index build).
        // Call it inside a scoped block so the &mut borrow on data.stores ends before we
        // re-borrow data.stores immutably to resolve MetricIDs → labels.
        let entries_opt: Option<Vec<_>> = {
            let per_key = match data.stores.get_mut(&store_key) {
                Some(pk) => pk,
                None => {
                    debug!("Metric {} not found in store for exact query", metric);
                    return Ok(HashMap::new());
                }
            };
            // Check current epoch first (newest). exact_query returns an owned Vec so the
            // &mut borrow of per_key ends immediately — no lifetime overlap with the
            // sealed_epochs scan below.
            per_key
                .current_epoch
                .exact_query(timestamp_range)
                .or_else(|| {
                    per_key
                        .sealed_epochs
                        .values()
                        .rev()
                        .find_map(|epoch| epoch.exact_query(timestamp_range))
                })
        }; // &mut borrow of data.stores ends here

        let mut results: TimestampedBucketsMap = HashMap::new();
        let mut total_entries = 0;
        let found_match = entries_opt.is_some();

        if let Some(entries) = entries_opt {
            let per_key = data.stores.get(&store_key).unwrap();
            for (metric_id, agg) in entries {
                let label = per_key.intern.resolve(metric_id).clone();
                results
                    .entry(label)
                    .or_default()
                    .push((timestamp_range, agg));
                total_entries += 1;
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

        // Update read count (outer Mutex held — no inner Mutex needed)
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

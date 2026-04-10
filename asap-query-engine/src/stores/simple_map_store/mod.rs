mod common;
pub mod global;
pub mod legacy;
pub mod per_key;

use crate::data_model::{
    AggregateCore, CleanupPolicy, LockStrategy, PrecomputedOutput, StreamingConfig,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use global::SimpleMapStoreGlobal;
use per_key::SimpleMapStorePerKey;
use std::collections::HashMap;
use std::sync::Arc;

/// Diagnostic snapshot from a single aggregation ID in the store.
pub struct AggregationDiagnostic {
    pub aggregation_id: u64,
    pub time_map_len: usize,
    pub read_counts_len: usize,
    pub num_aggregate_objects: usize,
    pub sketch_bytes: usize,
}

/// Diagnostic snapshot of the entire store.
pub struct StoreDiagnostics {
    pub num_aggregations: usize,
    pub total_time_map_entries: usize,
    pub total_sketch_bytes: usize,
    pub per_aggregation: Vec<AggregationDiagnostic>,
}

/// Enum wrapper that dispatches to either global or per-key lock implementation
pub enum SimpleMapStore {
    Global(SimpleMapStoreGlobal),
    PerKey(SimpleMapStorePerKey),
}

impl SimpleMapStore {
    /// Constructor with default strategy (backward compatibility for tests)
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self::new_with_strategy(streaming_config, cleanup_policy, LockStrategy::PerKey)
    }

    /// Collect diagnostic info for memory investigation.
    pub fn diagnostic_info(&self) -> StoreDiagnostics {
        match self {
            SimpleMapStore::Global(store) => store.diagnostic_info(),
            SimpleMapStore::PerKey(store) => store.diagnostic_info(),
        }
    }

    /// Constructor with explicit lock strategy (used by main.rs)
    pub fn new_with_strategy(
        streaming_config: Arc<StreamingConfig>,
        cleanup_policy: CleanupPolicy,
        lock_strategy: LockStrategy,
    ) -> Self {
        match lock_strategy {
            LockStrategy::Global => {
                SimpleMapStore::Global(SimpleMapStoreGlobal::new(streaming_config, cleanup_policy))
            }
            LockStrategy::PerKey => {
                SimpleMapStore::PerKey(SimpleMapStorePerKey::new(streaming_config, cleanup_policy))
            }
        }
    }
}

#[async_trait::async_trait]
impl Store for SimpleMapStore {
    fn insert_precomputed_output(
        &self,
        output: PrecomputedOutput,
        precompute: Box<dyn AggregateCore>,
    ) -> StoreResult<()> {
        match self {
            SimpleMapStore::Global(store) => store.insert_precomputed_output(output, precompute),
            SimpleMapStore::PerKey(store) => store.insert_precomputed_output(output, precompute),
        }
    }

    fn insert_precomputed_output_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        match self {
            SimpleMapStore::Global(store) => store.insert_precomputed_output_batch(outputs),
            SimpleMapStore::PerKey(store) => store.insert_precomputed_output_batch(outputs),
        }
    }

    fn query_precomputed_output(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            SimpleMapStore::Global(store) => {
                store.query_precomputed_output(metric, aggregation_id, start, end)
            }
            SimpleMapStore::PerKey(store) => {
                store.query_precomputed_output(metric, aggregation_id, start, end)
            }
        }
    }

    fn query_precomputed_output_exact(
        &self,
        metric: &str,
        aggregation_id: u64,
        exact_start: u64,
        exact_end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            SimpleMapStore::Global(store) => {
                store.query_precomputed_output_exact(metric, aggregation_id, exact_start, exact_end)
            }
            SimpleMapStore::PerKey(store) => {
                store.query_precomputed_output_exact(metric, aggregation_id, exact_start, exact_end)
            }
        }
    }

    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            SimpleMapStore::Global(store) => store.get_earliest_timestamp_per_aggregation_id(),
            SimpleMapStore::PerKey(store) => store.get_earliest_timestamp_per_aggregation_id(),
        }
    }

    fn close(&self) -> StoreResult<()> {
        match self {
            SimpleMapStore::Global(store) => store.close(),
            SimpleMapStore::PerKey(store) => store.close(),
        }
    }
}

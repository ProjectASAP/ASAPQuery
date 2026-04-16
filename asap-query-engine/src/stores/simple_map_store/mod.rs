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

    /// Replace the streaming config at runtime. Delegates to the active variant.
    pub fn update_streaming_config(&self, new_config: StreamingConfig) {
        match self {
            SimpleMapStore::Global(store) => store.update_streaming_config(new_config),
            SimpleMapStore::PerKey(store) => store.update_streaming_config(new_config),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{AggregationType, PrecomputedOutput};
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::stores::Store;
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowType;
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use std::collections::HashMap;

    fn make_agg_config(id: u64, metric: &str) -> AggregationConfig {
        AggregationConfig::new(
            id,
            AggregationType::SingleSubpopulation,
            "Sum".to_string(),
            HashMap::new(),
            KeyByLabelNames::new(vec![]),
            KeyByLabelNames::new(vec![]),
            KeyByLabelNames::new(vec![]),
            String::new(),
            10,
            0,
            WindowType::Tumbling,
            metric.to_string(),
            metric.to_string(),
            None,
            None,
            None,
            None,
        )
    }

    /// Verify that `update_streaming_config` makes newly added aggregation IDs visible
    /// to the store. Inserts for an unknown agg_id are silently dropped; after a config
    /// update that adds the agg_id, inserts are accepted and queryable.
    ///
    /// Tested for both the Global and PerKey variants.
    #[test]
    fn test_update_streaming_config_accepts_new_agg_insert() {
        for strategy in [LockStrategy::Global, LockStrategy::PerKey] {
            let store = SimpleMapStore::new_with_strategy(
                Arc::new(StreamingConfig::new(HashMap::new())),
                CleanupPolicy::NoCleanup,
                strategy,
            );

            // Insert for unknown agg_id=1 — should be silently dropped.
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(0, 10_000, None, 1),
                    Box::new(SumAccumulator::with_sum(99.0)),
                )
                .unwrap();
            let result = store.query_precomputed_output("cpu", 1, 0, 20_000).unwrap();
            assert!(
                result.is_empty(),
                "insert for unknown agg_id should be silently dropped ({strategy:?})"
            );

            // Update streaming config to include agg_id=1.
            let mut agg_configs = HashMap::new();
            agg_configs.insert(1, make_agg_config(1, "cpu"));
            store.update_streaming_config(StreamingConfig::new(agg_configs));

            // Insert again — now accepted.
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(0, 10_000, None, 1),
                    Box::new(SumAccumulator::with_sum(42.0)),
                )
                .unwrap();
            let result = store.query_precomputed_output("cpu", 1, 0, 20_000).unwrap();
            assert!(
                !result.is_empty(),
                "insert after config update should be accepted ({strategy:?})"
            );
        }
    }
}

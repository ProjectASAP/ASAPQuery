use crate::data_model::{AggregateCore, PrecomputedOutput};
use crate::stores::Store;
use std::sync::Arc;

/// Trait for emitting completed window outputs.
pub trait OutputSink: Send + Sync {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Output sink that writes directly to a `Store`.
pub struct StoreOutputSink {
    store: Arc<dyn Store>,
}

impl StoreOutputSink {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

impl OutputSink for StoreOutputSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if outputs.is_empty() {
            return Ok(());
        }
        self.store.insert_precomputed_output_batch(outputs)
    }
}

/// Output sink for raw passthrough mode — forwards raw samples to the store
/// without sketch computation. In this mode the samples are stored as
/// SumAccumulators (one per sample).
pub struct RawPassthroughSink {
    store: Arc<dyn Store>,
}

impl RawPassthroughSink {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

impl OutputSink for RawPassthroughSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if outputs.is_empty() {
            return Ok(());
        }
        self.store.insert_precomputed_output_batch(outputs)
    }
}

/// A no-op sink for testing that just counts emitted batches.
pub struct NoopOutputSink {
    pub emit_count: std::sync::atomic::AtomicU64,
}

impl NoopOutputSink {
    pub fn new() -> Self {
        Self {
            emit_count: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl OutputSink for NoopOutputSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.emit_count
            .fetch_add(outputs.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

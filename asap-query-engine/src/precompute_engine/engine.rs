use crate::data_model::{AggregationType, StreamingConfig};
use crate::precompute_engine::accumulator_factory::create_accumulator_updater;
use crate::precompute_engine::aggregation_spec::RoutingConfigSet;
use crate::precompute_engine::build_spec::BuildSpec;
use crate::precompute_engine::config::PrecomputeEngineConfig;
use crate::precompute_engine::ingest_source::IngestSource;
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::pipeline::{
    PipelineHandle, PrecomputePipeline, PrecomputeWorkerDiagnostics,
};
use asap_types::aggregation_config::AggregationConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// A cloneable handle for applying runtime config updates to a running engine.
///
/// Obtained via `PrecomputeEngine::handle()` before calling `run()`.
/// Calling `update_streaming_config` swaps the ingest handler's routing configs
/// atomically (lock-free via ArcSwap) and broadcasts the new map to all workers.
pub struct PrecomputeEngineHandle {
    pipeline: PipelineHandle<RoutingConfigSet>,
}

impl PrecomputeEngineHandle {
    /// Apply a new streaming config to the running engine.
    ///
    /// Updates the ingest handler's routing configs via a lock-free ArcSwap store,
    /// then broadcasts the new config map to all workers via their message channels.
    pub async fn update_streaming_config(
        &self,
        config: &StreamingConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let routing_configs = RoutingConfigSet::from_streaming_config(config)
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
        let agg_configs_map: HashMap<u64, Arc<AggregationConfig>> = routing_configs
            .configs()
            .iter()
            .map(|routing_config| {
                (
                    routing_config.config.aggregation_id,
                    routing_config.config.clone(),
                )
            })
            .collect();

        self.pipeline
            .update_spec(BuildSpec {
                routing: routing_configs,
                builds: agg_configs_map,
            })
            .await?;

        info!(
            "PrecomputeEngineHandle: applied new streaming config ({} aggregations)",
            config.get_all_aggregation_configs().len()
        );
        Ok(())
    }
}

/// The precompute engine: a `PrecomputePipeline` given an
/// `AggregationConfig`-based build specification from a `StreamingConfig`.
///
/// Call `handle()` before `run()` to obtain a `PrecomputeEngineHandle` for
/// applying runtime config updates while the engine is running.
pub struct PrecomputeEngine {
    pipeline: PrecomputePipeline<RoutingConfigSet>,
    streaming_config: Arc<StreamingConfig>,
}

impl PrecomputeEngine {
    pub fn new(
        config: PrecomputeEngineConfig,
        streaming_config: Arc<StreamingConfig>,
        output_sink: Arc<dyn OutputSink>,
        sources: Vec<Box<dyn IngestSource>>,
    ) -> Self {
        Self {
            pipeline: PrecomputePipeline::new(config, output_sink, sources),
            streaming_config,
        }
    }

    /// Get a handle to worker diagnostics, readable even after `run()` starts.
    pub fn diagnostics(&self) -> Arc<PrecomputeWorkerDiagnostics> {
        self.pipeline.diagnostics()
    }

    /// Test-support builder: override the wall-clock source used by every
    /// worker's wall-clock fallback (`flush_all`). Lets integration tests
    /// drive the fallback deterministically through the real ingest pipeline
    /// instead of racing a real clock with `tokio::time::sleep`. Must be
    /// called before `run()`. Production code never calls this.
    pub fn with_now_ms_fn(self, f: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        Self {
            pipeline: self.pipeline.with_now_ms_fn(f),
            streaming_config: self.streaming_config,
        }
    }

    /// Return a handle for applying runtime config updates to this engine.
    /// Must be called before `run()`.
    pub fn handle(&self) -> PrecomputeEngineHandle {
        PrecomputeEngineHandle {
            pipeline: self.pipeline.handle(),
        }
    }

    /// Start the precompute engine. This spawns worker tasks and all registered
    /// ingest sources, then blocks until shutdown.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        validate_startup_aggregation_configs(&self.streaming_config)?;
        let routing_configs = RoutingConfigSet::from_streaming_config(&self.streaming_config)
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;

        // Build aggregation config map from streaming config for workers.
        let agg_configs: HashMap<u64, Arc<AggregationConfig>> = self
            .streaming_config
            .get_all_aggregation_configs()
            .iter()
            .map(|(&id, cfg)| (id, Arc::new(cfg.clone())))
            .collect();

        self.pipeline
            .run(BuildSpec {
                routing: routing_configs,
                builds: agg_configs,
            })
            .await
    }
}

/// Validate aggregation configs before starting any worker or ingest task.
///
/// Count-Min Sketch and MultipleSum configs carry their semantic contract in
/// subtype fields or parameters; allowing an invalid value to reach the lazy
/// worker path would leave the engine running while silently losing that
/// contract.
fn validate_startup_aggregation_configs(
    streaming_config: &StreamingConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut errors = Vec::new();

    for (&aggregation_id, config) in streaming_config.get_all_aggregation_configs() {
        if !matches!(
            config.aggregation_type,
            AggregationType::CountMinSketch
                | AggregationType::CountMinSketchWithHeap
                | AggregationType::MultipleSum
        ) {
            continue;
        }

        if let Err(err) = create_accumulator_updater(config) {
            errors.push((aggregation_id, err));
        }
    }

    if !errors.is_empty() {
        errors.sort_by_key(|(aggregation_id, _)| *aggregation_id);
        let details = errors
            .into_iter()
            .map(|(aggregation_id, err)| {
                format!("invalid aggregation config for aggregation_id {aggregation_id}: {err}")
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Err(details.into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{AggregationType, StreamingConfig, WindowType};
    use crate::precompute_engine::config::LateDataPolicy;
    use crate::precompute_engine::ingest_source::{IngestContext, IngestSource};
    use crate::precompute_engine::output_sink::NoopOutputSink;
    use async_trait::async_trait;
    use serde_json::json;

    struct ShutdownSource;

    #[async_trait]
    impl IngestSource for ShutdownSource {
        async fn run(
            self: Box<Self>,
            ctx: IngestContext,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            ctx.router.broadcast_shutdown().await
        }
    }

    #[tokio::test]
    async fn run_rejects_invalid_spatial_filter_before_starting_workers() {
        let config = AggregationConfig::new(
            1,
            AggregationType::Sum,
            "Sum".to_string(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            1_000,
            1_000,
            WindowType::Tumbling,
            r#"{job=}"#.to_string(),
            "requests_total".to_string(),
            None,
            None,
            None,
            None,
        );
        let engine = PrecomputeEngine::new(
            PrecomputeEngineConfig::default(),
            Arc::new(StreamingConfig::new(HashMap::from([(1, config)]))),
            Arc::new(NoopOutputSink::new()),
            vec![Box::new(ShutdownSource)],
        );

        let error = engine
            .run()
            .await
            .expect_err("invalid spatial filter must reject startup");
        assert!(error.to_string().contains("invalid spatialFilter"));
    }

    #[tokio::test]
    async fn run_rejects_invalid_cms_subtype_before_starting_workers() {
        let mut parameters = HashMap::new();
        parameters.insert("depth".to_string(), json!(3_u64));
        parameters.insert("width".to_string(), json!(128_u64));
        let cms = AggregationConfig::new(
            1,
            AggregationType::CountMinSketch,
            String::new(),
            parameters,
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![
                "host".to_string()
            ]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            1_000,
            1_000,
            WindowType::Tumbling,
            String::new(),
            "requests_total".to_string(),
            None,
            None,
            None,
            None,
        );
        let engine = PrecomputeEngine::new(
            PrecomputeEngineConfig {
                num_workers: 1,
                late_data_policy: LateDataPolicy::Drop,
                ..PrecomputeEngineConfig::default()
            },
            Arc::new(StreamingConfig::new(HashMap::from([(1, cms)]))),
            Arc::new(NoopOutputSink::new()),
            vec![Box::new(ShutdownSource)],
        );

        let result = engine.run().await;
        let err = match result {
            Ok(()) => panic!("invalid CMS subtype must fail before startup"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("aggregation_id 1"));
        assert!(err.to_string().contains("sum") && err.to_string().contains("count"));
    }

    #[tokio::test]
    async fn run_rejects_invalid_multiple_sum_subtype_before_starting_workers() {
        let multiple_sum = AggregationConfig::new(
            1,
            AggregationType::MultipleSum,
            String::new(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![
                "host".to_string()
            ]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            1_000,
            1_000,
            WindowType::Tumbling,
            String::new(),
            "requests_total".to_string(),
            None,
            None,
            None,
            None,
        );
        let engine = PrecomputeEngine::new(
            PrecomputeEngineConfig {
                num_workers: 1,
                late_data_policy: LateDataPolicy::Drop,
                ..PrecomputeEngineConfig::default()
            },
            Arc::new(StreamingConfig::new(HashMap::from([(1, multiple_sum)]))),
            Arc::new(NoopOutputSink::new()),
            vec![Box::new(ShutdownSource)],
        );

        let result = engine.run().await;
        let err = match result {
            Ok(()) => panic!("invalid MultipleSum subtype must fail before startup"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("aggregation_id 1"));
        assert!(err.to_string().contains("sum") && err.to_string().contains("count"));
    }

    #[tokio::test]
    async fn run_rejects_invalid_cms_with_heap_subtype_before_starting_workers() {
        let mut parameters = HashMap::new();
        parameters.insert("depth".to_string(), json!(3_u64));
        parameters.insert("width".to_string(), json!(128_u64));
        parameters.insert("heapsize".to_string(), json!(32_u64));
        let cms = AggregationConfig::new(
            1,
            AggregationType::CountMinSketchWithHeap,
            String::new(),
            parameters,
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![
                "host".to_string()
            ]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            1_000,
            1_000,
            WindowType::Tumbling,
            String::new(),
            "requests_total".to_string(),
            None,
            None,
            None,
            None,
        );
        let engine = PrecomputeEngine::new(
            PrecomputeEngineConfig {
                num_workers: 1,
                late_data_policy: LateDataPolicy::Drop,
                ..PrecomputeEngineConfig::default()
            },
            Arc::new(StreamingConfig::new(HashMap::from([(1, cms)]))),
            Arc::new(NoopOutputSink::new()),
            vec![Box::new(ShutdownSource)],
        );

        let result = engine.run().await;
        let err = match result {
            Ok(()) => panic!("invalid heap CMS subtype must fail before startup"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("aggregation_id 1"));
        assert!(err.to_string().contains("sum") && err.to_string().contains("count"));
    }

    #[tokio::test]
    async fn run_reports_all_invalid_cms_configs_in_aggregation_id_order() {
        let cms_config = |id, aggregation_type, aggregation_sub_type, parameters| {
            AggregationConfig::new(
                id,
                aggregation_type,
                aggregation_sub_type,
                parameters,
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![
                    "host".to_string(),
                ]),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                String::new(),
                1_000,
                1_000,
                WindowType::Tumbling,
                String::new(),
                "requests_total".to_string(),
                None,
                None,
                None,
                None,
            )
        };

        let mut heap_parameters = HashMap::new();
        heap_parameters.insert("depth".to_string(), json!(3_u64));
        heap_parameters.insert("width".to_string(), json!(128_u64));
        heap_parameters.insert("heapsize".to_string(), json!(32_u64));

        let configs = HashMap::from([
            (
                20,
                cms_config(
                    20,
                    AggregationType::CountMinSketchWithHeap,
                    String::new(),
                    heap_parameters,
                ),
            ),
            (
                3,
                cms_config(
                    3,
                    AggregationType::CountMinSketch,
                    String::new(),
                    HashMap::from([
                        ("depth".to_string(), json!(3_u64)),
                        ("width".to_string(), json!(128_u64)),
                    ]),
                ),
            ),
        ]);
        let engine = PrecomputeEngine::new(
            PrecomputeEngineConfig {
                num_workers: 1,
                late_data_policy: LateDataPolicy::Drop,
                ..PrecomputeEngineConfig::default()
            },
            Arc::new(StreamingConfig::new(configs)),
            Arc::new(NoopOutputSink::new()),
            vec![Box::new(ShutdownSource)],
        );

        let result = engine.run().await;
        let err = match result {
            Ok(()) => panic!("invalid CMS configs must fail before startup"),
            Err(err) => err.to_string(),
        };
        let id_3 = err
            .find("aggregation_id 3")
            .expect("CMS error should be reported");
        let id_20 = err
            .find("aggregation_id 20")
            .expect("heap CMS error should be reported");
        assert!(
            id_3 < id_20,
            "errors should be ordered by aggregation ID: {err}"
        );
    }
}

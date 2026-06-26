use asap_types::enums::CleanupPolicy;
use asap_types::inference_config::InferenceConfig;
use asap_types::streaming_config::StreamingConfig;
use asap_types::PromQLSchema;
use promql_utilities::data_model::KeyByLabelNames;
use serde::Deserialize;
use tracing::warn;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerConfig {
    pub query_groups: Vec<QueryGroup>,
    pub sketch_parameters: Option<SketchParameterOverrides>,
    pub aggregate_cleanup: Option<AggregateCleanupConfig>,
    /// Optional hint: per-metric label sets used as a fallback when Prometheus
    /// returns no series for a metric. Prometheus-inferred labels take priority.
    #[serde(default)]
    pub metrics: Option<Vec<MetricDefinition>>,
    /// Current streaming config, passed as context for repeated reconfiguration.
    /// NOTE: reserved for future use — the planner does not yet act on these fields.
    /// They are wired through now so that repeated-reconfig support can be added
    /// without a second round of type-signature changes.
    #[serde(default)]
    pub existing_streaming_config: Option<StreamingConfig>,
    /// Current inference config, passed as context for repeated reconfiguration.
    /// NOTE: see existing_streaming_config — same future-use caveat applies.
    /// Not serializable via serde (InferenceConfig does not impl Deserialize);
    /// set programmatically only.
    #[serde(skip)]
    pub existing_inference_config: Option<InferenceConfig>,
}

impl ControllerConfig {
    /// Warn if any query group has both SLAs at 0.0 (the serde Default),
    /// which indicates `controller_options` was omitted from the config.
    pub fn warn_default_slas(&self) {
        for qg in &self.query_groups {
            let opts = &qg.controller_options;
            if opts.accuracy_sla == 0.0 && opts.latency_sla == 0.0 {
                warn!(
                    query_group_id = ?qg.id,
                    "controller_options not set in query group; \
                     accuracy_sla=0.0 and latency_sla=0.0 will be used — \
                     add controller_options to your config"
                );
            }
        }
    }

    /// Build a `PromQLSchema` from the `metrics` hints in this config.
    /// Returns an empty schema if no hints are present.
    pub fn schema_from_hints(&self) -> PromQLSchema {
        let mut schema = PromQLSchema::new();
        if let Some(metrics) = &self.metrics {
            for m in metrics {
                schema =
                    schema.add_metric(m.metric.clone(), KeyByLabelNames::new(m.labels.clone()));
            }
        }
        schema
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryGroup {
    pub id: Option<u32>,
    pub queries: Vec<String>,
    pub repetition_delay_ms: u64,
    #[serde(default)]
    pub controller_options: ControllerOptions,
    /// Per-group step override (ms). Falls back to `RuntimeOptions::step_ms` when None.
    #[serde(default)]
    pub step_ms: Option<u64>,
    /// Per-group range_duration override (ms). Falls back to `RuntimeOptions::range_duration_ms` when None.
    #[serde(default)]
    pub range_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ControllerOptions {
    pub accuracy_sla: f64,
    pub latency_sla: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricDefinition {
    pub metric: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateCleanupConfig {
    pub policy: Option<CleanupPolicy>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SketchParameterOverrides {
    #[serde(rename = "CountMinSketch")]
    pub count_min_sketch: Option<CmsParams>,
    #[serde(rename = "CountMinSketchWithHeap")]
    pub count_min_sketch_with_heap: Option<CmsHeapParams>,
    #[serde(rename = "DatasketchesKLL")]
    pub datasketches_kll: Option<KllParams>,
    #[serde(rename = "HydraKLL")]
    pub hydra_kll: Option<HydraParams>,
    #[serde(rename = "HLL")]
    pub hll: Option<HllParams>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CmsParams {
    pub depth: u64,
    pub width: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CmsHeapParams {
    pub depth: u64,
    pub width: u64,
    pub heap_multiplier: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KllParams {
    #[serde(rename = "K")]
    pub k: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HydraParams {
    pub row_num: u64,
    pub col_num: u64,
    pub k: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HllParams {
    pub precision: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SQLControllerConfig {
    pub query_groups: Vec<SQLQueryGroup>,
    pub tables: Vec<TableDefinition>,
    pub sketch_parameters: Option<SketchParameterOverrides>,
    pub aggregate_cleanup: Option<AggregateCleanupConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SQLQueryGroup {
    pub id: Option<u32>,
    pub queries: Vec<String>,
    pub repetition_delay_ms: u64,
    pub controller_options: ControllerOptions,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDefinition {
    pub name: String,
    pub time_column: String,
    pub value_columns: Vec<String>,
    #[serde(default)]
    pub metadata_columns: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElasticDSLControllerConfig {
    pub query_groups: Vec<ElasticDSLQueryGroup>,
    pub sketch_parameters: Option<SketchParameterOverrides>,
    pub aggregate_cleanup: Option<AggregateCleanupConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElasticDSLQueryGroup {
    pub id: Option<u32>,
    pub queries: Vec<String>,
    pub repetition_delay_ms: u64,
    pub index: String,
    pub time_field: String,
    pub controller_options: ControllerOptions,
}

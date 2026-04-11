use asap_types::enums::CleanupPolicy;
use asap_types::PromQLSchema;
use promql_utilities::data_model::KeyByLabelNames;
use serde::Deserialize;

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
}

impl ControllerConfig {
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
pub struct QueryGroup {
    pub id: Option<u32>,
    pub queries: Vec<String>,
    pub repetition_delay: u64,
    #[serde(default)]
    pub controller_options: ControllerOptions,
    /// Per-group step override (seconds). Falls back to `RuntimeOptions::step` when None.
    #[serde(default)]
    pub step: Option<u64>,
    /// Per-group range_duration override (seconds). Falls back to `RuntimeOptions::range_duration` when None.
    #[serde(default)]
    pub range_duration: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ControllerOptions {
    pub accuracy_sla: f64,
    pub latency_sla: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricDefinition {
    pub metric: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AggregateCleanupConfig {
    pub policy: Option<CleanupPolicy>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SketchParameterOverrides {
    #[serde(rename = "CountMinSketch")]
    pub count_min_sketch: Option<CmsParams>,
    #[serde(rename = "CountMinSketchWithHeap")]
    pub count_min_sketch_with_heap: Option<CmsHeapParams>,
    #[serde(rename = "DatasketchesKLL")]
    pub datasketches_kll: Option<KllParams>,
    #[serde(rename = "HydraKLL")]
    pub hydra_kll: Option<HydraParams>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CmsParams {
    pub depth: u64,
    pub width: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CmsHeapParams {
    pub depth: u64,
    pub width: u64,
    pub heap_multiplier: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KllParams {
    #[serde(rename = "K")]
    pub k: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HydraParams {
    pub row_num: u64,
    pub col_num: u64,
    pub k: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SQLControllerConfig {
    pub query_groups: Vec<SQLQueryGroup>,
    pub tables: Vec<TableDefinition>,
    pub sketch_parameters: Option<SketchParameterOverrides>,
    pub aggregate_cleanup: Option<AggregateCleanupConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SQLQueryGroup {
    pub id: Option<u32>,
    pub queries: Vec<String>,
    pub repetition_delay: u64,
    pub controller_options: ControllerOptions,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableDefinition {
    pub name: String,
    pub time_column: String,
    pub value_columns: Vec<String>,
    pub metadata_columns: Vec<String>,
}

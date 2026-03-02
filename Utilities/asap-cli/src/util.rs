use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

type BoxedErr = Box<dyn std::error::Error>;

/// Complete Hydra configuration combining all config sources
/// This represents the fully resolved Hydra config with all overrides applied
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct HydraConfig {
    #[serde(default)]
    pub experiment: Option<ExperimentMetadata>,
    #[serde(default)]
    pub cloudlab: CloudLabConfig,
    #[serde(default)]
    pub prometheus: PrometheusConfig,
    #[serde(default)]
    pub streaming: StreamingConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub profiling: ProfilingConfig,
    #[serde(default)]
    pub manual: ManualConfig,
    #[serde(default)]
    pub flow: FlowConfig,
    #[serde(default)]
    pub experiment_variants: ExperimentVariants,
    #[serde(default)]
    pub fake_exporter_language: Option<String>,
    #[serde(default)]
    pub query_engine: QueryEngineConfig,
    #[serde(default)]
    pub aggregate_cleanup: AggregateCleanupConfig,

    /// The experiment parameters from the experiment_type config group
    #[serde(default)]
    pub experiment_params: Option<ExperimentConfig>,
}

/// Experiment configuration from experiment_type configs
/// This represents the YAML files in config/experiment_type/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExperimentConfig {
    pub experiment: Vec<ExperimentMode>,
    pub servers: Vec<Server>,
    pub workloads: Option<HashMap<String, Workload>>,
    pub exporters: Exporters,
    pub query_groups: Vec<QueryGroup>,
    pub metrics: Vec<Metric>,
}

/// CloudLab infrastructure configuration
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CloudLabConfig {
    pub num_nodes: Option<u32>,
    pub username: Option<String>,
    pub hostname_suffix: Option<String>,
}

/// Prometheus configuration overrides
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct PrometheusConfig {
    pub local_config_dir: Option<String>,
    pub scrape_interval: Option<String>,
    pub evaluation_interval: Option<String>,
    pub query_log_file: Option<String>,
    pub recording_rules: Option<RecordingRulesConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RecordingRulesConfig {
    pub interval: String,
}

/// Remote write configuration for Prometheus
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RemoteWriteConfig {
    pub ip: Option<String>,
    pub base_port: Option<u16>,
    pub path: Option<String>,
}

/// Streaming engine configuration
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct StreamingConfig {
    pub engine: Option<String>,              // "flink" | "arroyo"
    pub flink_input_format: Option<String>,  // "json" | "avro-json" | "avro-binary"
    pub flink_output_format: Option<String>, // "json" | "byte"
    pub enable_object_reuse: Option<bool>,
    pub do_local_flink: Option<bool>,
    pub forward_unsupported_queries: Option<bool>,
    pub parallelism: Option<u32>,            // Pipeline parallelism
    pub remote_write: Option<RemoteWriteConfig>, // Prometheus remote write config
}

/// Logging configuration
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct LoggingConfig {
    pub level: Option<String>, // "DEBUG" | "INFO" | "WARNING" | "ERROR"
}

/// Profiling configuration
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ProfilingConfig {
    pub query_engine: Option<bool>,
    pub prometheus_time: Option<u32>,
    pub flink: Option<bool>,
    pub arroyo: Option<bool>,
}

/// Manual mode configuration
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ManualConfig {
    pub query_engine: Option<bool>,
    pub remote_monitor: Option<bool>,
}

/// Flow control configuration
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct FlowConfig {
    pub no_teardown: Option<bool>,
    pub steady_state_wait: Option<u32>,
}

/// Query engine configuration
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct QueryEngineConfig {
    pub dump_precomputes: Option<bool>,
    pub lock_strategy: Option<String>,  // "global" or "per-key"
}

/// Aggregate cleanup configuration
/// Policy options: "circular_buffer", "read_based", "no_cleanup"
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AggregateCleanupConfig {
    pub policy: Option<String>,
}

/// Experiment metadata
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ExperimentMetadata {
    pub name: Option<String>,
    pub config_file: Option<String>,
}

/// Experiment variants for specific scripts
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ExperimentVariants {
    pub sketchdboffline: Option<SketchDbOfflineConfig>,
    pub flink_aggregations: Option<FlinkAggregationsConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SketchDbOfflineConfig {
    pub experiment_dir: String,
    pub labels: Option<Vec<String>>,
    pub groupby: Vec<String>,
    pub aggregation: String, // "sum" | "avg" | "count" | "min" | "max"
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FlinkAggregationsConfig {
    pub config: String,
    pub aggregation_id: u32,
    pub min_aggregations: u32,
    pub max_aggregations: u32,
    pub profile_duration: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExperimentMode {
    pub mode: String,
    pub query_prometheus_too: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Server {
    pub name: String,
    pub url: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Workload {
    #[serde(rename = "use")]
    pub use_workload: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Exporters {
    pub only_start_if_queries_exist: bool,
    pub exporter_list: HashMap<String, ExporterConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ExporterConfig {
    NodeExporter {
        port: u16,
        extra_flags: Option<String>,
    },
    FakeExporter {
        num_ports_per_server: u16,
        start_port: u16,
        dataset: String,
        synthetic_data_value_scale: u32,
        num_labels: u8,
        num_values_per_label: u16,
        metric_type: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryGroup {
    pub id: u32,
    pub queries: Vec<String>,
    pub repetition_delay: u32,
    pub client_options: ClientOptions,
    pub controller_options: ControllerOptions,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientOptions {
    pub repetitions: u32,
    pub query_time_offset: Option<u32>,
    pub starting_delay: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ControllerOptions {
    pub accuracy_sla: f64,
    pub latency_sla: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metric {
    pub metric: String,
    pub labels: Vec<String>,
    pub exporter: String,
}

/**
 * @brief Auto-detects and parses the config file.
 * If it's an experiment_type config, wraps it in HydraConfig.
 * If it's a full Hydra config, parses directly.
 *
 * @returns A HydraConfig struct with all fields populated.
 */
pub async fn parse_config_auto(config: &Path) -> Result<HydraConfig, BoxedErr> {
    let content = tokio::fs::read_to_string(config).await?;

    // Try parsing as HydraConfig first
    if let Ok(hydra_config) = serde_yaml::from_str::<HydraConfig>(&content) {
        // Check if it has experiment_params or looks like a full Hydra config
        if hydra_config.experiment_params.is_some() ||
           hydra_config.cloudlab.num_nodes.is_some() ||
           hydra_config.streaming.engine.is_some() {
            return Ok(hydra_config);
        }
    }

    // Otherwise, try parsing as ExperimentConfig and wrap it
    let exp_config: ExperimentConfig = serde_yaml::from_str(&content)?;
    Ok(HydraConfig {
        experiment_params: Some(exp_config),
        ..Default::default()
    })
}

/// Controller client configuration - same as ExperimentConfig but without experiment and workloads
#[derive(Serialize, Deserialize, Debug)]
struct ControllerClientConfig {
    pub servers: Vec<Server>,
    pub exporters: Exporters,
    pub query_groups: Vec<QueryGroup>,
    pub metrics: Vec<Metric>,
}

pub async fn generate_controller_client_config(
    experiment_params: ExperimentConfig,
    experiment_output_dir: &Path
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let output_dir = experiment_output_dir.join("controller_client_configs");
    tokio::fs::create_dir_all(&output_dir).await?;

    // Build a map from server name to server config
    let mut server_map: HashMap<String, Server> = HashMap::new();
    for server in &experiment_params.servers {
        server_map.insert(server.name.clone(), server.clone());
    }

    // Generate a config file for each experiment mode
    for mode in &experiment_params.experiment {
        // Determine which servers to include
        let servers = if mode.mode == "sketchdb"
            && mode.query_prometheus_too.unwrap_or(false) {
            // Special case: include all servers
            experiment_params.servers.clone()
        } else {
            // Normal case: only include the server matching the mode
            vec![server_map.get(&mode.mode)
                .ok_or_else(|| format!("Server '{}' not found in servers config", mode.mode))?
                .clone()]
        };

        // Create the controller client config
        let controller_config = ControllerClientConfig {
            servers,
            exporters: experiment_params.exporters.clone(),
            query_groups: experiment_params.query_groups.clone(),
            metrics: experiment_params.metrics.clone(),
        };

        // Write to file
        let output_file = output_dir.join(format!("{}.yaml", mode.mode));
        let file = std::fs::File::create(&output_file)?;
        serde_yaml::to_writer(file, &controller_config)?;
    }

    // NOTE: An experiment configuration may have multiple modes resulting
    // in multiple runs, but the CLI should only run a single mode for now.
    // The CLI is hardcoded to use "sketchdb" mode only (matching experiment_run_grafana_demo.py).
    // In the future we should probably have yaml configurations strictly for
    // configuring a single run of ProjectASAP.
    Ok(output_dir.join("sketchdb.yaml"))
}

/// Get list of metrics that should be written to remote write based on experiment configuration.
/// This matches the Python implementation in experiment_utils/core.py
pub fn get_metrics_to_remote_write(experiment_params: &ExperimentConfig) -> Vec<String> {
    // Check if only_start_if_queries_exist flag is set
    let only_if_queries_exist = experiment_params.exporters.only_start_if_queries_exist;

    if !only_if_queries_exist {
        // Return all metrics
        return experiment_params
            .metrics
            .iter()
            .map(|m| m.metric.clone())
            .collect();
    }

    // Get all queries from all query groups
    let mut all_queries = Vec::new();
    for group in &experiment_params.query_groups {
        all_queries.extend(group.queries.clone());
    }

    // Filter metrics that appear in queries
    let mut metrics_to_remote_write = Vec::new();
    for metric_config in &experiment_params.metrics {
        for query in &all_queries {
            if query.contains(&metric_config.metric) {
                metrics_to_remote_write.push(metric_config.metric.clone());
                break;
            }
        }
    }

    metrics_to_remote_write
}

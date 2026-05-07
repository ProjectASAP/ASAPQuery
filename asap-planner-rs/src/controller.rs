use std::path::Path;
use tracing::debug;

use asap_types::PromQLSchema;
use promql_utilities::data_model::KeyByLabelNames;

use crate::config::input::ControllerConfig;
use crate::error::ControllerError;
use crate::planner_output::PlannerOutput;
use crate::RuntimeOptions;
use crate::{output, prometheus_client, query_log};

pub struct Controller {
    config: ControllerConfig,
    schema: PromQLSchema,
    options: RuntimeOptions,
}

impl Controller {
    pub fn new(config: ControllerConfig, schema: PromQLSchema, options: RuntimeOptions) -> Self {
        Self {
            config,
            schema,
            options,
        }
    }

    /// Build a `Controller` from a config file, fetching metric labels from Prometheus.
    ///
    /// `prometheus_url` is queried via `GET /api/v1/series?match[]=<metric>` for each metric
    /// name found in the config's PromQL queries.
    pub fn from_file(
        path: &Path,
        opts: RuntimeOptions,
        prometheus_url: &str,
    ) -> Result<Self, ControllerError> {
        let yaml_str = std::fs::read_to_string(path)?;
        let config: ControllerConfig = serde_yaml::from_str(&yaml_str)?;
        let all_queries: Vec<String> = config
            .query_groups
            .iter()
            .flat_map(|qg| qg.queries.clone())
            .collect();
        let mut schema =
            prometheus_client::build_schema_from_prometheus(prometheus_url, &all_queries)?;
        // For any metric that Prometheus had no series for, fall back to the
        // `metrics` hint in the config file (if present).
        if let Some(metric_hints) = &config.metrics {
            for hint in metric_hints {
                if !schema.config.contains_key(&hint.metric) {
                    debug!(
                        "Prometheus had no series for '{}'; falling back to config-file hint with labels {:?}",
                        hint.metric, hint.labels
                    );
                    schema = schema.add_metric(
                        hint.metric.clone(),
                        KeyByLabelNames::new(hint.labels.clone()),
                    );
                }
            }
        }
        Ok(Self {
            config,
            schema,
            options: opts,
        })
    }

    /// Build a `Controller` from a config file with a caller-supplied `PromQLSchema`.
    ///
    /// Use this when the schema is available without querying Prometheus (e.g. in tests
    /// or when the schema is constructed in-process by the caller).
    pub fn from_file_with_schema(
        path: &Path,
        schema: PromQLSchema,
        opts: RuntimeOptions,
    ) -> Result<Self, ControllerError> {
        let yaml_str = std::fs::read_to_string(path)?;
        let config: ControllerConfig = serde_yaml::from_str(&yaml_str)?;
        Ok(Self {
            config,
            schema,
            options: opts,
        })
    }

    /// Build a `Controller` from a YAML string with a caller-supplied `PromQLSchema`.
    pub fn from_yaml_with_schema(
        yaml: &str,
        schema: PromQLSchema,
        opts: RuntimeOptions,
    ) -> Result<Self, ControllerError> {
        let config: ControllerConfig = serde_yaml::from_str(yaml)?;
        Ok(Self {
            config,
            schema,
            options: opts,
        })
    }

    /// Build a `Controller` from a Prometheus query log file, fetching metric labels from
    /// Prometheus.
    ///
    /// - `log_path`: newline-delimited JSON query log (Prometheus `--query.log-file` output)
    /// - `prometheus_url`: base URL queried for label discovery
    pub fn from_query_log(
        log_path: &Path,
        opts: RuntimeOptions,
        prometheus_url: &str,
    ) -> Result<Self, ControllerError> {
        let entries = query_log::parse_log_file(log_path)?;
        let (instants, ranges) =
            query_log::infer_queries(&entries, opts.prometheus_scrape_interval);
        let config = query_log::to_controller_config(instants, ranges);
        let all_queries: Vec<String> = config
            .query_groups
            .iter()
            .flat_map(|qg| qg.queries.clone())
            .collect();
        let schema = prometheus_client::build_schema_from_prometheus(prometheus_url, &all_queries)?;
        Ok(Self {
            config,
            schema,
            options: opts,
        })
    }

    /// Build a `Controller` from a Prometheus query log file with a caller-supplied `PromQLSchema`.
    ///
    /// Use this when the schema is available without querying Prometheus (e.g. in tests).
    pub fn from_query_log_with_schema(
        log_path: &Path,
        schema: PromQLSchema,
        opts: RuntimeOptions,
    ) -> Result<Self, ControllerError> {
        let entries = query_log::parse_log_file(log_path)?;
        let (instants, ranges) =
            query_log::infer_queries(&entries, opts.prometheus_scrape_interval);
        let config = query_log::to_controller_config(instants, ranges);
        Ok(Self {
            config,
            schema,
            options: opts,
        })
    }

    pub fn generate(&self) -> Result<PlannerOutput, ControllerError> {
        let output = output::generator::generate_plan(&self.config, &self.schema, &self.options)?;
        Ok(PlannerOutput::from_output(output))
    }

    pub fn generate_to_dir(&self, dir: &Path) -> Result<PlannerOutput, ControllerError> {
        let output = self.generate()?;
        std::fs::create_dir_all(dir)?;
        let streaming_str = serde_yaml::to_string(output.streaming_yaml())?;
        let inference_str = serde_yaml::to_string(output.inference_yaml())?;
        std::fs::write(dir.join("streaming_config.yaml"), streaming_str)?;
        std::fs::write(dir.join("inference_config.yaml"), inference_str)?;
        Ok(output)
    }
}

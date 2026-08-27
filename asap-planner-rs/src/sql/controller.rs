use std::path::Path;

use tracing::debug;

use super::generator;
use crate::clickhouse_client;
use crate::config::input::SQLControllerConfig;
use crate::error::ControllerError;
use crate::planner_output::PlannerOutput;
use crate::sql::generator::SQLRuntimeOptions;

pub struct SQLController {
    config: SQLControllerConfig,
    options: SQLRuntimeOptions,
}

impl SQLController {
    pub fn new(config: SQLControllerConfig, options: SQLRuntimeOptions) -> Self {
        Self { config, options }
    }

    pub fn from_file(path: &Path, opts: SQLRuntimeOptions) -> Result<Self, ControllerError> {
        let yaml_str = std::fs::read_to_string(path)?;
        Self::from_yaml(&yaml_str, opts)
    }

    /// Build a `SQLController` from a config file, filling in any empty
    /// `metadata_columns` via auto-discovery from the ClickHouse HTTP API.
    ///
    /// Mirrors `promql::Controller::from_file`, which fetches label sets from
    /// Prometheus. Tables whose `metadata_columns` is already populated in the
    /// config are left untouched; only empty ones are discovered.
    pub fn from_file_with_discovery(
        path: &Path,
        clickhouse_url: &str,
        clickhouse_database: &str,
        opts: SQLRuntimeOptions,
    ) -> Result<Self, ControllerError> {
        let yaml_str = std::fs::read_to_string(path)?;
        let mut config: SQLControllerConfig = serde_yaml::from_str(&yaml_str)?;
        if let Some(windowing) = &config.windowing {
            windowing
                .validate()
                .map_err(ControllerError::PlannerError)?;
        }
        for table in &mut config.tables {
            if table.metadata_columns.is_empty() {
                debug!(
                    "Table '{}' has no metadata_columns; discovering via ClickHouse system.columns at {}",
                    table.name, clickhouse_url
                );
                table.metadata_columns = clickhouse_client::infer_metadata_columns(
                    clickhouse_url,
                    clickhouse_database,
                    &table.name,
                    &table.time_column,
                    &table.value_columns,
                )?;
            } else {
                debug!(
                    "Table '{}' has {} metadata_columns in config; skipping discovery",
                    table.name,
                    table.metadata_columns.len()
                );
            }
        }
        Ok(Self {
            config,
            options: opts,
        })
    }

    pub fn from_yaml(yaml: &str, opts: SQLRuntimeOptions) -> Result<Self, ControllerError> {
        let config: SQLControllerConfig = serde_yaml::from_str(yaml)?;
        if let Some(windowing) = &config.windowing {
            windowing
                .validate()
                .map_err(ControllerError::PlannerError)?;
        }
        Ok(Self {
            config,
            options: opts,
        })
    }

    pub fn generate(&self) -> Result<PlannerOutput, ControllerError> {
        let output = generator::generate_sql_plan(&self.config, &self.options)?;
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

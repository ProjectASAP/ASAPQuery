
use std::path::Path;

use crate::config::input::ElasticDSLControllerConfig;
use crate::error::ControllerError;
use crate::planner_output::PlannerOutput;
use crate::elastic_dsl::generator::{ElasticRuntimeOptions, generate_elastic_plan};

pub struct ElasticController {
    config: ElasticDSLControllerConfig,
    options: ElasticRuntimeOptions,
}

impl ElasticController {
    pub fn new(config: ElasticDSLControllerConfig, options: ElasticRuntimeOptions) -> Self {
        Self { config, options }
    }

    pub fn from_file(path: &Path, opts: ElasticRuntimeOptions) -> Result<Self, ControllerError> {
        let yaml_str = std::fs::read_to_string(path)?;
        Self::from_yaml(&yaml_str, opts)
    }

    pub fn from_yaml(yaml: &str, opts: ElasticRuntimeOptions) -> Result<Self, ControllerError> {
        let config: ElasticDSLControllerConfig = serde_yaml::from_str(yaml)?;
        Ok(Self {
            config,
            options: opts,
        })
    }

    pub fn generate(&self) -> Result<PlannerOutput, ControllerError> {
        let output = generate_elastic_plan(&self.config, &self.options)?;
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
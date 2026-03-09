pub mod config;
pub mod error;
pub mod output;
pub mod planner;

use serde_yaml::Value as YamlValue;
use std::path::Path;

pub use config::input::ControllerConfig;
pub use error::ControllerError;
pub use output::generator::{GeneratorOutput, PuntedQuery};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingEngine {
    Arroyo,
    Flink,
}

#[derive(Debug, Clone)]
pub struct RuntimeOptions {
    pub prometheus_scrape_interval: u64,
    pub streaming_engine: StreamingEngine,
    pub enable_punting: bool,
    pub range_duration: u64,
    pub step: u64,
}

pub struct Controller {
    config: ControllerConfig,
    options: RuntimeOptions,
}

/// Output of the planning process — contains the two YAML configs
pub struct PlannerOutput {
    pub punted_queries: Vec<PuntedQuery>,
    streaming_yaml: YamlValue,
    inference_yaml: YamlValue,
    aggregation_count: usize,
    query_count: usize,
}

impl PlannerOutput {
    pub fn streaming_aggregation_count(&self) -> usize {
        self.aggregation_count
    }

    pub fn inference_query_count(&self) -> usize {
        self.query_count
    }

    pub fn has_aggregation_type(&self, t: &str) -> bool {
        if let YamlValue::Mapping(root) = &self.streaming_yaml {
            if let Some(YamlValue::Sequence(aggs)) = root.get("aggregations") {
                return aggs.iter().any(|agg| {
                    if let YamlValue::Mapping(m) = agg {
                        if let Some(YamlValue::String(agg_type)) = m.get("aggregationType") {
                            return agg_type == t;
                        }
                    }
                    false
                });
            }
        }
        false
    }

    pub fn all_tumbling_window_sizes_eq(&self, s: u64) -> bool {
        self.check_tumbling_window_sizes(|size| size == s)
    }

    pub fn all_tumbling_window_sizes_leq(&self, s: u64) -> bool {
        self.check_tumbling_window_sizes(|size| size <= s)
    }

    fn check_tumbling_window_sizes(&self, predicate: impl Fn(u64) -> bool) -> bool {
        if let YamlValue::Mapping(root) = &self.streaming_yaml {
            if let Some(YamlValue::Sequence(aggs)) = root.get("aggregations") {
                return aggs.iter().all(|agg| {
                    if let YamlValue::Mapping(m) = agg {
                        if let Some(val) = m.get("tumblingWindowSize") {
                            let size = match val {
                                YamlValue::Number(n) => n.as_u64().unwrap_or(0),
                                _ => 0,
                            };
                            return predicate(size);
                        }
                    }
                    false
                });
            }
        }
        false
    }

    /// Returns the sorted labels for the first aggregation matching `agg_type`,
    /// for the given `label_kind` ("rollup", "grouping", or "aggregated").
    pub fn aggregation_labels(&self, agg_type: &str, label_kind: &str) -> Vec<String> {
        if let YamlValue::Mapping(root) = &self.streaming_yaml {
            if let Some(YamlValue::Sequence(aggs)) = root.get("aggregations") {
                for agg in aggs {
                    if let YamlValue::Mapping(m) = agg {
                        if let Some(YamlValue::String(t)) = m.get("aggregationType") {
                            if t == agg_type {
                                if let Some(YamlValue::Mapping(labels)) = m.get("labels") {
                                    if let Some(YamlValue::Sequence(seq)) = labels.get(label_kind) {
                                        let mut result: Vec<String> = seq
                                            .iter()
                                            .filter_map(|v| {
                                                if let YamlValue::String(s) = v {
                                                    Some(s.clone())
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect();
                                        result.sort();
                                        return result;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        vec![]
    }

    /// Returns the cleanup param (read_count_threshold or num_aggregates_to_retain)
    /// for the first aggregation entry of the given query string.
    pub fn inference_cleanup_param(&self, query: &str) -> Option<u64> {
        if let YamlValue::Mapping(root) = &self.inference_yaml {
            if let Some(YamlValue::Sequence(queries)) = root.get("queries") {
                for q in queries {
                    if let YamlValue::Mapping(qm) = q {
                        if let Some(YamlValue::String(qs)) = qm.get("query") {
                            if qs == query {
                                if let Some(YamlValue::Sequence(aggs)) = qm.get("aggregations") {
                                    if let Some(YamlValue::Mapping(agg)) = aggs.first() {
                                        for key in
                                            ["read_count_threshold", "num_aggregates_to_retain"]
                                        {
                                            if let Some(YamlValue::Number(n)) = agg.get(key) {
                                                return n.as_u64();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        None
    }

    pub fn to_streaming_yaml_string(&self) -> Result<String, anyhow::Error> {
        Ok(serde_yaml::to_string(&self.streaming_yaml)?)
    }

    pub fn to_inference_yaml_string(&self) -> Result<String, anyhow::Error> {
        Ok(serde_yaml::to_string(&self.inference_yaml)?)
    }
}

impl Controller {
    pub fn from_file(path: &Path, opts: RuntimeOptions) -> Result<Self, ControllerError> {
        let yaml_str = std::fs::read_to_string(path)?;
        Self::from_yaml(&yaml_str, opts)
    }

    pub fn from_yaml(yaml: &str, opts: RuntimeOptions) -> Result<Self, ControllerError> {
        let config: ControllerConfig = serde_yaml::from_str(yaml)?;
        Ok(Self {
            config,
            options: opts,
        })
    }

    pub fn generate(&self) -> Result<PlannerOutput, ControllerError> {
        let output = output::generator::generate_plan(&self.config, &self.options)?;
        Ok(PlannerOutput {
            punted_queries: output.punted_queries,
            streaming_yaml: output.streaming_yaml,
            inference_yaml: output.inference_yaml,
            aggregation_count: output.aggregation_count,
            query_count: output.query_count,
        })
    }

    pub fn generate_to_dir(&self, dir: &Path) -> Result<PlannerOutput, ControllerError> {
        let output = self.generate()?;
        std::fs::create_dir_all(dir)?;
        let streaming_str = serde_yaml::to_string(&output.streaming_yaml)?;
        let inference_str = serde_yaml::to_string(&output.inference_yaml)?;
        std::fs::write(dir.join("streaming_config.yaml"), streaming_str)?;
        std::fs::write(dir.join("inference_config.yaml"), inference_str)?;
        Ok(output)
    }
}

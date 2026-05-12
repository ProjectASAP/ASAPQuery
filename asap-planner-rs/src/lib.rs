pub mod config;
pub mod error;
pub mod generator;
pub mod planner;
pub mod planner_output;
pub mod prometheus_client;
pub mod promql;
pub mod query_log;
pub mod sql;
pub mod elastic_dsl;

pub use asap_types::PromQLSchema;
pub use config::input::ControllerConfig;
pub use config::input::SQLControllerConfig;
pub use config::input::ElasticDSLControllerConfig;
pub use error::ControllerError;
pub use generator::{GeneratorOutput, PuntedQuery};
pub use planner_output::PlannerOutput;
pub use prometheus_client::build_schema_from_prometheus;
pub use promql::Controller;
pub use sql::SQLController;
pub use sql::SQLRuntimeOptions;
pub use elastic_dsl::ElasticController;
pub use elastic_dsl::ElasticRuntimeOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingEngine {
    Arroyo,
    Flink,
    Precompute,
}

#[derive(Debug, Clone)]
pub struct RuntimeOptions {
    pub prometheus_scrape_interval: u64,
    pub streaming_engine: StreamingEngine,
    pub enable_punting: bool,
    pub range_duration: u64,
    pub step: u64,
}

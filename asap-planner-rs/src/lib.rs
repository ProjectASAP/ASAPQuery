pub mod config;
pub mod controller;
pub mod error;
pub mod output;
pub mod planner;
pub mod planner_output;
pub mod prometheus_client;
pub mod query_log;
pub mod sql_controller;

pub use asap_types::PromQLSchema;
pub use config::input::ControllerConfig;
pub use config::input::SQLControllerConfig;
pub use controller::Controller;
pub use error::ControllerError;
pub use output::generator::{GeneratorOutput, PuntedQuery};
pub use output::sql_generator::SQLRuntimeOptions;
pub use planner_output::PlannerOutput;
pub use prometheus_client::build_schema_from_prometheus;
pub use sql_controller::SQLController;

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

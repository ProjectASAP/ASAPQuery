use thiserror::Error;

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),
    #[error("PromQL parse error: {0}")]
    PromQLParse(String),
    #[error("Duplicate query: {0}")]
    DuplicateQuery(String),
    #[error("Planner error: {0}")]
    PlannerError(String),
    #[error("Unknown metric: {0}")]
    UnknownMetric(String),
    #[error("SQL parse error: {0}")]
    SqlParse(String),
    #[error("Unknown table: {0}")]
    UnknownTable(String),
}

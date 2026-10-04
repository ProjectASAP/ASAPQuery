use promql_utilities::query_logics::enums::Statistic;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OptimizerError {
    #[error("query '{query}' has repetition_delay_ms=0")]
    InvalidRepeatInterval { query: String },

    #[error("cannot optimize leaf '{leaf}' in query '{query}': {reason}")]
    UnsupportedLeaf {
        query: String,
        leaf: String,
        reason: String,
    },

    #[error("{items:?}")]
    UnservableItems { items: Vec<UnservableItem> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnservableItem {
    pub metric: String,
    pub statistics: Vec<Statistic>,
    pub data_range_ms: u64,
    pub t_repeat_ms: u64,
    pub accuracy_sla: f64,
    pub latency_sla: f64,
    pub reason: String,
}

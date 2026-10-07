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
}

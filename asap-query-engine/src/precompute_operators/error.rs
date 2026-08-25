use std::fmt;

#[derive(Debug, PartialEq)]
pub enum AccumulatorError {
    /// Returned by constructors when `sub_type` is not "min" or "max".
    InvalidSubType(String),
    /// Returned by `SimpleEngine::merge_accumulators` when called with an empty
    /// slice. This is a programming error (violated precondition), not a domain
    /// error from the `MergeableAccumulator` trait impls — those erase their
    /// errors into `Box<dyn Error>` to accommodate heterogeneous accumulator
    /// types (KLL, CMS, etc.) that produce library-specific errors.
    EmptySlice,
    /// Returned when merging accumulators whose `sub_type` fields disagree.
    MergeTypeMismatch { expected: String, got: String },
    /// Returned by `SimpleEngine::merge_accumulators` when a `merge_with`
    /// call in the sequential fallback fold fails. Aborts the merge rather
    /// than skipping the failed accumulator, matching `NaiveMerger::merge_all`.
    MergeFailed(String),
}

impl fmt::Display for AccumulatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSubType(s) => write!(f, "sub_type must be 'min' or 'max', got '{s}'"),
            Self::EmptySlice => write!(f, "merge_accumulators called with empty slice"),
            Self::MergeTypeMismatch { expected, got } => write!(
                f,
                "cannot merge accumulators: expected sub_type '{expected}', got '{got}'"
            ),
            Self::MergeFailed(e) => write!(f, "failed to merge accumulators: {e}"),
        }
    }
}

impl std::error::Error for AccumulatorError {}

pub(crate) mod merge_utils;
pub(crate) mod query_plan;
pub mod query_result;
pub mod simple_engine;
pub(crate) mod sliding_window_composition;
pub mod window_merger;

pub use query_result::{InstantVector, QueryResult, RangeVector, RangeVectorElement, Sample};
#[cfg(feature = "native_query_legacy_test_support")]
pub use simple_engine::NativeRangeExecutionMode;
pub use simple_engine::{QueryExecutionError, SimpleEngine};
pub use window_merger::{create_window_merger, NaiveMerger, WindowMerger};

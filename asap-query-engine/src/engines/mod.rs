pub(crate) mod merge_utils;
pub mod query_result;
pub mod simple_engine;
pub(crate) mod sliding_window_composition;
pub mod window_merger;

pub use query_result::{InstantVector, QueryResult, RangeVector, RangeVectorElement, Sample};
pub use simple_engine::SimpleEngine;
pub use window_merger::{create_window_merger, NaiveMerger, WindowMerger};

mod global;
mod per_key;

pub use global::LegacySimpleMapStoreGlobal;
pub use per_key::{AggregationDiagnostic, LegacySimpleMapStorePerKey, StoreDiagnostics};

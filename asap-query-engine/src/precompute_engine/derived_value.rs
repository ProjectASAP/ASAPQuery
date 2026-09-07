// DerivedValueConfig lives in asap_types so asap-planner-rs can construct it
// (from detecting a min/max/sum/avg-over-a-non-value-column SQL shape) and
// the engine can consume it (to re-emit that column's value as its own
// ingest stream) without either crate depending on the other - see
// asap_types::derived_value for the shared definition and design note.
pub use asap_types::derived_value::DerivedValueConfig;

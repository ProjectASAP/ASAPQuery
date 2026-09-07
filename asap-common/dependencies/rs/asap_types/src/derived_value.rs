use serde::{Deserialize, Serialize};

/// What a derived-value stream's samples actually carry.
///
/// `Numeric` (the original, and default, shape) re-emits `source_column`'s
/// own value as `value: f64` - see `DerivedValueConfig`'s doc comment.
///
/// `ArgMax`/`ArgMin` back `argMax(source_column, <time_column>)` /
/// `argMin(...)`: each sample's `value` is the row's own timestamp (the
/// comparison key `MAX`/`MIN` already knows how to accumulate) and its
/// `arg_value` is `source_column`'s raw string for that row - the value to
/// remember from whichever row has the extremal timestamp. Scoped
/// deliberately to "compare against the table's own time column" rather
/// than an arbitrary second column: every real argMax/argMin query in
/// practice asks "the latest/earliest X", and restricting to it means the
/// comparison key is always already available as `timestamp_ms` per
/// sample - no second numeric payload needs to ride along.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum DerivedValueKind {
    #[default]
    Numeric,
    ArgMax,
    ArgMin,
}

/// Config for a derived-value ingest stream: re-emits `source_column`'s own
/// value, per row, as an independent sample under `metric_name` - same
/// labels and timestamp as the row it came from, but its own metric name so
/// it never mixes with the main metric's samples.
///
/// This exists because ingest carries exactly one `value` per row, shared by
/// every aggregation registered against that metric (a `count()` and a
/// `sum(x)` on the same table both see the same weight). That's fine when
/// every registered aggregate wants the same thing (an event count), but
/// breaks the moment a query needs `min`/`max`/`sum`/`avg` over a real
/// column that isn't the table's designated value column - e.g. `med`,
/// `local_pref`, or the table's own time column (`min(timestamp)`). Routing
/// that one aggregate through its own derived metric, carrying the raw
/// column's value instead of the shared per-row weight, answers it without
/// disturbing every other aggregate already registered on the table -
/// the same "give the odd one out its own virtual table" pattern
/// `StatefulTransitionConfig` and lag-gap's derived table already use.
///
/// Shared between asap-planner-rs (which detects the SQL pattern and emits
/// this config into streaming_config.yaml) and asap-query-engine (which
/// reads it back out to drive the ingest-time re-emission).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct DerivedValueConfig {
    /// Generated metric name for the derived value stream.
    /// Example: derived_value_timestamp_bgp
    pub metric_name: String,

    /// The raw column whose value gets re-emitted. May be a metadata column
    /// (e.g. `med`) or the table's own time column (e.g. `timestamp`) - both
    /// are excluded from `value_columns` by construction, which is exactly
    /// why the aggregate needs this derived stream in the first place.
    pub source_column: String,

    /// What kind of stream this is - see `DerivedValueKind`. Defaults to
    /// `Numeric` so every config predating this field keeps its original
    /// meaning.
    pub kind: DerivedValueKind,
}

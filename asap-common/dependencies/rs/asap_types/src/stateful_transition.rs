use serde::{Deserialize, Serialize};

/// Config for a stateful transition operator: remembers the last value of
/// `state_column` per `partition_by` key and emits a derived event into
/// `metric_name` when `predicate` (comparing the remembered previous value
/// to the current row) holds. This is how patterns like ClickHouse's
/// `lagInFrame(col) OVER (PARTITION BY ... ORDER BY timestamp)` get lowered
/// into something the precompute engine's ordinary aggregators can count,
/// without the planner or query engine ever needing to understand window
/// functions.
///
/// Shared between asap-planner-rs (which detects the SQL pattern and emits
/// this config into streaming_config.yaml) and asap-query-engine (which
/// reads it back out to drive the ingest-time operator). Previously these
/// were two disconnected things: the engine had this struct privately and
/// nothing ever auto-populated it, so it had to be hand-written per query.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct StatefulTransitionConfig {
    /// Generated metric name for the derived event stream.
    /// Example: derived_lag_transition_path_changes
    pub metric_name: String,

    /// Columns that define independent state machines.
    /// Example: [prefix, collector, peer_ip]
    pub partition_by: Vec<String>,

    /// Column whose previous value is remembered.
    /// Example: as_path
    pub state_column: String,

    /// Alias used by the SQL query for the previous value.
    /// Example: previous_path
    pub previous_alias: String,

    /// Raw predicate from countIf(...).
    /// V0 supports AND of simple comparisons:
    ///   previous_alias != ''
    ///   previous_alias != state_column
    ///   previous_alias = 'literal'
    ///   previous_alias != 'literal'
    pub predicate: String,

    /// Labels to put on emitted derived samples.
    /// Usually this is the outer GROUP BY list.
    /// Empty means global aggregate.
    pub emit_labels: Vec<String>,

    /// When set, this operator emits a NUMERIC derived value instead of a
    /// boolean-triggered event: `dateDiff(<unit>, previous state_column,
    /// current state_column)`, in this ClickHouse dateDiff unit name
    /// ("second", "minute", ...). Lowers `dateDiff('second',
    /// lagInFrame(timestamp) OVER (PARTITION BY ... ORDER BY timestamp),
    /// timestamp) AS gap` - the "gap between consecutive rows" shape - the
    /// same way the boolean mode above lowers `countIf(previous_x != x)`.
    /// `predicate`/`previous_alias` are unused in this mode; every row with
    /// a remembered previous value emits (subject to `min_gap`), rather
    /// than only rows where a predicate holds.
    pub gap_unit: Option<String>,

    /// Only emit when `gap_unit` is set AND the computed gap is strictly
    /// greater than this value (e.g. `Some(0.0)` folds a `WHERE gap > 0`
    /// filter into ingest-time emission). `None` means always emit once a
    /// previous value exists.
    pub min_gap: Option<f64>,
}

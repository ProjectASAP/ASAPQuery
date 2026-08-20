use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
struct Columns {
    time: String,
    value_columns: HashSet<String>,
    metadata_columns: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub time_column: String,
    pub value_columns: HashSet<String>,
    pub metadata_columns: HashSet<String>,
}

impl Table {
    pub fn new(
        table_name: String,
        time_column: String,
        value_columns: HashSet<String>,
        metadata_columns: HashSet<String>,
    ) -> Self {
        Self {
            name: table_name,
            time_column,
            value_columns,
            metadata_columns,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SQLSchema {
    info: HashMap<String, Columns>,
}

impl SQLSchema {
    pub fn new(table_schemas: Vec<Table>) -> Self {
        let mut info = HashMap::new();

        for table in table_schemas {
            let columns = Columns {
                time: table.time_column,
                value_columns: table.value_columns,
                metadata_columns: table.metadata_columns,
            };
            info.insert(table.name, columns);
        }

        Self { info }
    }

    pub fn get_time_column(&self, table_name: &str) -> Option<&String> {
        self.info.get(table_name).map(|cols| &cols.time)
    }

    pub fn get_value_columns(&self, table_name: &str) -> Option<&HashSet<String>> {
        self.info.get(table_name).map(|cols| &cols.value_columns)
    }

    pub fn get_metadata_columns(&self, table_name: &str) -> Option<&HashSet<String>> {
        self.info.get(table_name).map(|cols| &cols.metadata_columns)
    }

    pub fn is_valid_value_column(&self, table: &str, value_column: &str) -> bool {
        if let Some(value_columns) = self.get_value_columns(table) {
            value_columns.contains(value_column)
        } else {
            false
        }
    }

    pub fn are_valid_metadata_columns(&self, table: &str, columns: &HashSet<String>) -> bool {
        if let Some(table_metadata_columns) = self.get_metadata_columns(table) {
            for col in columns {
                if !table_metadata_columns.contains(col) {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub struct SQLQueryData {
    pub aggregation_info: AggregationInfo,
    /// Metadata predicates from WHERE after removing the time predicate.
    /// Example: collector = 'rrc00' or collector IN ('rrc00').
    pub spatial_filter: Option<String>,
    /// Alias of the aggregate function in SELECT, e.g. `agg(v) AS p99` → `Some("p99")`.
    /// Captured separately from `aggregation_info` because it's presentational only:
    /// two queries that differ solely in alias must still match the same template.
    pub aggregation_alias: Option<String>,
    pub metric: String,
    pub labels: HashSet<String>,
    pub time_info: TimeInfo,
    pub subquery: Option<Box<SQLQueryData>>,
    /// `ORDER BY` items in source order. Empty when no ORDER BY is present.
    /// Excluded from `matches_sql_pattern` since ordering is post-aggregation.
    pub order_by: Vec<OrderByItem>,
    /// `LIMIT N`. None when no LIMIT is present. Excluded from `matches_sql_pattern`.
    pub limit: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SQLBucketedCountIfOutput {
    pub alias: String,
    /// Extra per-output filter extracted from countIf(...).
    /// Example: operation = 'A'
    pub filter: String,
}

#[derive(Debug, Clone)]
pub struct SQLBucketedCountIfQueryData {
    pub metric: String,
    pub time_info: TimeInfo,
    pub bucket_alias: String,
    pub bucket_ms: u64,
    /// WHERE predicates after removing the time predicate.
    /// Example: collector = 'rrc00'
    pub base_spatial_filter: Option<String>,
    pub outputs: Vec<SQLBucketedCountIfOutput>,
    pub order_by: Vec<OrderByItem>,
}

impl SQLBucketedCountIfQueryData {
    /// Match reusable bucketed templates by structure, not by absolute timestamps.
    pub fn matches_bucketed_pattern(&self, template: &SQLBucketedCountIfQueryData) -> bool {
        self.metric == template.metric
            && self.time_info.get_time_col_name() == template.time_info.get_time_col_name()
            && self.bucket_ms == template.bucket_ms
            && self.base_spatial_filter == template.base_spatial_filter
            && self.outputs.len() == template.outputs.len()
            && self
                .outputs
                .iter()
                .zip(template.outputs.iter())
                .all(|(a, b)| a.alias == b.alias && a.filter == b.filter)
    }
}

/// Single `ORDER BY` clause item: a column reference plus sort direction.
/// `column` is either a GROUP BY identifier or the aggregate alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByItem {
    pub column: String,
    /// `true` for ASC (the default when neither ASC nor DESC is specified), `false` for DESC.
    pub ascending: bool,
}

#[derive(Debug, Clone)]
pub struct TimeInfo {
    time_col_name: String,
    // Can be changed to use timezone (normal datetime incorporates TimeZone) in the future
    start: f64,
    // is_now: bool,
    duration: f64,
}

impl TimeInfo {
    pub fn new(time_col_name: String, start: f64, duration: f64) -> Self {
        Self {
            time_col_name,
            start,
            // is_now,
            duration,
        }
    }

    pub fn get_time_col_name(&self) -> &str {
        &self.time_col_name
    }

    pub fn get_start(&self) -> f64 {
        self.start
    }

    pub fn get_duration(&self) -> f64 {
        self.duration
    }
}

#[derive(Debug, Clone)]
pub struct AggregationInfo {
    name: String,
    value_column_name: String,
    args: Vec<String>,
}

impl AggregationInfo {
    pub fn new(name: String, value_column_name: String, args: Vec<String>) -> Self {
        Self {
            name,
            value_column_name,
            args,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_value_column_name(&self) -> &str {
        &self.value_column_name
    }

    pub fn get_args(&self) -> &Vec<String> {
        &self.args
    }

    /// Returns true if this aggregation matches the given template
    /// (same function name, value column, and arguments).
    pub fn matches_pattern(&self, other: &AggregationInfo) -> bool {
        self.name == other.name
            && self.value_column_name == other.value_column_name
            && self.args == other.args
    }
}

impl TimeInfo {
    /// Returns true if this time info matches the given template.
    ///
    /// For "UNUSED" time columns (the outer level of a subquery which has no WHERE
    /// time clause), only the column name is compared.
    /// For real time columns, the column name and duration are compared but the
    /// absolute start time is ignored — this allows NOW()-based templates to match
    /// incoming queries that use absolute timestamps.
    pub fn matches_pattern(&self, other: &TimeInfo) -> bool {
        if self.time_col_name != other.time_col_name {
            return false;
        }
        if self.time_col_name == "UNUSED" {
            return true;
        }
        (self.duration - other.duration).abs() < f64::EPSILON
    }
}

impl SQLQueryData {
    /// Returns true if this query data structurally matches the given template.
    ///
    /// Templates in inference_config use NOW()-relative timestamps; actual incoming
    /// queries use absolute timestamps. Only the duration is compared, not the
    /// absolute start time. All other fields (metric, aggregation, labels, time
    /// column name) must match exactly.
    pub fn matches_sql_pattern(&self, template: &SQLQueryData) -> bool {
        self.metric == template.metric
            && self
                .aggregation_info
                .matches_pattern(&template.aggregation_info)
            && self.labels == template.labels
            && self.time_info.matches_pattern(&template.time_info)
            && match (&self.subquery, &template.subquery) {
                (None, None) => true,
                (Some(sq), Some(tq)) => sq.matches_sql_pattern(tq),
                _ => false,
            }
    }
}

/// How a top-k query weights each observation fed into the heavy-hitter sketch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopkWeighting {
    /// `COUNT(col)`: every matching row contributes weight 1, so the heap ranks
    /// keys by event frequency (`count_events: true`).
    Count,
    /// `SUM(col)`: every matching row contributes weight = `col`, so the heap
    /// ranks keys by summed value (`count_events: false`).
    ///
    /// Assumes **non-negative** summands: `CountMinSketch` is a frequency sketch
    /// and cannot represent negative weights, so a `SUM` over a column that can
    /// go negative would produce meaningless estimates.
    Sum,
}

/// A detected SQL top-k query: the `LIMIT k` plus how the sketch is weighted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlTopk {
    pub k: u64,
    pub weighting: TopkWeighting,
}

impl SqlTopk {
    /// `count_events` flag the backing `CountMinSketchWithHeap` must use:
    /// `true` for COUNT (unit weight), `false` for SUM (value weight).
    pub fn count_events(&self) -> bool {
        matches!(self.weighting, TopkWeighting::Count)
    }
}

/// Detect a SQL top-k query and return its `k` plus sketch weighting.
///
/// Recognises the heavy-hitter shape that `CountMinSketchWithHeap` serves:
///
/// ```sql
/// SELECT <key>, COUNT(<col>) AS <alias>   -- or SUM(<col>)
/// FROM <table> WHERE <1s window>
/// GROUP BY <key>
/// ORDER BY <alias> DESC
/// LIMIT k
/// ```
///
/// The grouping key (`<key>`) becomes the *aggregated* dimension inside the
/// sketch's heap — not a precompute partition key — so a single sketch per
/// window tracks the top keys by event count (COUNT) or summed value (SUM).
///
/// The SQL parser only accepts identifier ORDER BY targets, so the descending
/// order must reference the aggregate's alias (e.g. `transfer_events`), not the
/// `COUNT(col)` / `SUM(col)` expression itself.
///
/// This detection inspects a single SELECT layer only. For nested queries the
/// ORDER BY / LIMIT sit on the outer SELECT, which on its own matches this
/// shape; callers that must exclude nested patterns (e.g. spatial-over-temporal)
/// are responsible for gating before calling this (the query engine gates on
/// query pattern type; the planner rejects nested queries up front).
pub fn detect_sql_topk(query_data: &SQLQueryData) -> Option<SqlTopk> {
    let k = query_data.limit?;
    // LIMIT 0 is an empty-result query, not a top-k heavy-hitter request.
    if k == 0 {
        return None;
    }
    // Need a GROUP BY key to rank and an ORDER BY to define "top".
    if query_data.labels.is_empty() || query_data.order_by.is_empty() {
        return None;
    }
    // CountMinSketchWithHeap tracks heavy hitters by COUNT (unit weight) or
    // SUM (value weight). Any other aggregate (MIN/MAX/quantile/...) cannot be
    // served by the additive frequency sketch.
    let name = query_data.aggregation_info.get_name();
    let weighting = if name.eq_ignore_ascii_case("count") {
        TopkWeighting::Count
    } else if name.eq_ignore_ascii_case("sum") {
        TopkWeighting::Sum
    } else {
        return None;
    };
    // Primary ordering must be the aggregate alias, descending (largest first).
    let primary = &query_data.order_by[0];
    if primary.ascending {
        return None;
    }
    // ORDER BY may differ only by identifier case for unquoted aliases.
    let alias = query_data.aggregation_alias.as_deref()?;
    if !alias.eq_ignore_ascii_case(primary.column.as_str()) {
        return None;
    }
    Some(SqlTopk { k, weighting })
}

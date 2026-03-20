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
    pub metric: String,
    pub labels: HashSet<String>,
    pub time_info: TimeInfo,
    pub subquery: Option<Box<SQLQueryData>>,
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

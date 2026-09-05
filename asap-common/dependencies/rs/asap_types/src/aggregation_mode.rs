use promql_utilities::query_logics::enums::AggregationType;

/// Whether a sample contributes its value (SUM) or a unit weight (COUNT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountMode {
    Sum,
    Count,
}

/// Which end of the value range a MinMax-family aggregation tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinMaxMode {
    Min,
    Max,
}

/// The typed meaning of `aggregation_sub_type`, resolved for a given
/// `AggregationType`. Aggregation kinds that share a sub_type axis (e.g. plain
/// `CountMinSketch` and `CountMinSketchWithHeap` both carry SUM/COUNT
/// weighting) resolve to the same variant here, so callers dispatch on one
/// typed value instead of re-parsing the raw string per kind (#670).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationMode {
    /// This aggregation type has no sub_type axis.
    None,
    SumOrCount(CountMode),
    MinOrMax(MinMaxMode),
}

impl AggregationMode {
    /// Parse `sub_type` according to the axis `agg_type` uses. Case-insensitive,
    /// matching the existing wire convention.
    pub fn parse(agg_type: AggregationType, sub_type: &str) -> Result<Self, String> {
        match agg_type {
            AggregationType::CountMinSketch
            | AggregationType::MultipleSum
            | AggregationType::CountMinSketchWithHeap => {
                if sub_type.eq_ignore_ascii_case("sum") {
                    Ok(AggregationMode::SumOrCount(CountMode::Sum))
                } else if sub_type.eq_ignore_ascii_case("count") {
                    Ok(AggregationMode::SumOrCount(CountMode::Count))
                } else {
                    Err(format!(
                        "{agg_type} requires aggregation_sub_type 'sum' or 'count', got '{sub_type}'"
                    ))
                }
            }
            AggregationType::MinMax | AggregationType::MultipleMinMax => {
                if sub_type.eq_ignore_ascii_case("min") {
                    Ok(AggregationMode::MinOrMax(MinMaxMode::Min))
                } else if sub_type.eq_ignore_ascii_case("max") {
                    Ok(AggregationMode::MinOrMax(MinMaxMode::Max))
                } else {
                    Err(format!(
                        "aggregation_sub_type must be 'min' or 'max', got '{sub_type}'"
                    ))
                }
            }
            _ => Ok(AggregationMode::None),
        }
    }

    /// The canonical wire string for this mode, e.g. for planner emission.
    pub fn as_sub_type_str(self) -> &'static str {
        match self {
            AggregationMode::None => "",
            AggregationMode::SumOrCount(CountMode::Sum) => "sum",
            AggregationMode::SumOrCount(CountMode::Count) => "count",
            AggregationMode::MinOrMax(MinMaxMode::Min) => "min",
            AggregationMode::MinOrMax(MinMaxMode::Max) => "max",
        }
    }
}

impl CountMode {
    pub fn from_count_events(count_events: bool) -> Self {
        if count_events {
            CountMode::Count
        } else {
            CountMode::Sum
        }
    }
}

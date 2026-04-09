use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryPatternType {
    OnlyTemporal,
    OnlySpatial,
    OneTemporalOneSpatial,
}

impl std::fmt::Display for QueryPatternType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug!("Formatting QueryPatternType: {:?}", self);
        match self {
            QueryPatternType::OnlyTemporal => write!(f, "only_temporal"),
            QueryPatternType::OnlySpatial => write!(f, "only_spatial"),
            QueryPatternType::OneTemporalOneSpatial => write!(f, "one_temporal_one_spatial"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryTreatmentType {
    Exact,
    Approximate,
}

impl std::fmt::Display for QueryTreatmentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug!("Formatting QueryTreatmentType: {:?}", self);
        match self {
            QueryTreatmentType::Exact => write!(f, "exact"),
            QueryTreatmentType::Approximate => write!(f, "approximate"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Statistic {
    Count,
    Sum,
    Cardinality,
    Increase,
    Rate,
    Min,
    Max,
    Quantile,
    Topk,
}

impl std::fmt::Display for Statistic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug!("Formatting Statistic: {:?}", self);
        match self {
            Statistic::Count => write!(f, "count"),
            Statistic::Sum => write!(f, "sum"),
            Statistic::Cardinality => write!(f, "cardinality"),
            Statistic::Increase => write!(f, "increase"),
            Statistic::Rate => write!(f, "rate"),
            Statistic::Min => write!(f, "min"),
            Statistic::Max => write!(f, "max"),
            Statistic::Quantile => write!(f, "quantile"),
            Statistic::Topk => write!(f, "topk"),
        }
    }
}

#[allow(clippy::should_implement_trait)]
impl Statistic {
    pub fn from_str(s: &str) -> Option<Self> {
        debug!("Parsing Statistic from string: {}", s);
        match s.to_lowercase().as_str() {
            "count" => Some(Statistic::Count),
            "sum" => Some(Statistic::Sum),
            "cardinality" => Some(Statistic::Cardinality),
            "increase" => Some(Statistic::Increase),
            "rate" => Some(Statistic::Rate),
            "min" => Some(Statistic::Min),
            "max" => Some(Statistic::Max),
            "quantile" => Some(Statistic::Quantile),
            "topk" => Some(Statistic::Topk),
            _ => None,
        }
    }
}

impl std::str::FromStr for Statistic {
    type Err = ();

    /// Parse a statistic from a string (case-insensitive).
    /// Use `s.parse::<Statistic>()` or `Statistic::from_str(s)`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        debug!("FromStr trait parsing Statistic: {}", s);
        Statistic::from_str(s).ok_or(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryResultType {
    InstantVector,
    RangeVector,
}

impl std::fmt::Display for QueryResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug!("Formatting QueryResultType: {:?}", self);
        match self {
            QueryResultType::InstantVector => write!(f, "instant_vector"),
            QueryResultType::RangeVector => write!(f, "range_vector"),
        }
    }
}

/// A PromQL function that produces a vector-valued result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PromQLFunction {
    Rate,
    Increase,
    SumOverTime,
    CountOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    QuantileOverTime,
}

impl PromQLFunction {
    pub fn as_str(self) -> &'static str {
        match self {
            PromQLFunction::Rate => "rate",
            PromQLFunction::Increase => "increase",
            PromQLFunction::SumOverTime => "sum_over_time",
            PromQLFunction::CountOverTime => "count_over_time",
            PromQLFunction::AvgOverTime => "avg_over_time",
            PromQLFunction::MinOverTime => "min_over_time",
            PromQLFunction::MaxOverTime => "max_over_time",
            PromQLFunction::QuantileOverTime => "quantile_over_time",
        }
    }

    /// Returns `true` for functions whose result requires approximate pre-aggregation.
    pub fn is_approximate(self) -> bool {
        matches!(
            self,
            PromQLFunction::QuantileOverTime
                | PromQLFunction::SumOverTime
                | PromQLFunction::CountOverTime
                | PromQLFunction::AvgOverTime
        )
    }
}

impl std::fmt::Display for PromQLFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for PromQLFunction {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "rate" => Ok(PromQLFunction::Rate),
            "increase" => Ok(PromQLFunction::Increase),
            "sum_over_time" => Ok(PromQLFunction::SumOverTime),
            "count_over_time" => Ok(PromQLFunction::CountOverTime),
            "avg_over_time" => Ok(PromQLFunction::AvgOverTime),
            "min_over_time" => Ok(PromQLFunction::MinOverTime),
            "max_over_time" => Ok(PromQLFunction::MaxOverTime),
            "quantile_over_time" => Ok(PromQLFunction::QuantileOverTime),
            other => Err(format!("Unknown PromQL function: '{other}'")),
        }
    }
}

/// A PromQL aggregation operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregationOperator {
    Sum,
    Count,
    Avg,
    Quantile,
    Min,
    Max,
    Topk,
}

impl AggregationOperator {
    pub fn as_str(self) -> &'static str {
        match self {
            AggregationOperator::Sum => "sum",
            AggregationOperator::Count => "count",
            AggregationOperator::Avg => "avg",
            AggregationOperator::Quantile => "quantile",
            AggregationOperator::Min => "min",
            AggregationOperator::Max => "max",
            AggregationOperator::Topk => "topk",
        }
    }

    /// Returns the `Statistic` values required to answer this operator.
    /// `Avg` needs both `Sum` and `Count`.
    pub fn to_statistics(self) -> Vec<Statistic> {
        match self {
            AggregationOperator::Avg => vec![Statistic::Sum, Statistic::Count],
            AggregationOperator::Sum => vec![Statistic::Sum],
            AggregationOperator::Count => vec![Statistic::Count],
            AggregationOperator::Quantile => vec![Statistic::Quantile],
            AggregationOperator::Min => vec![Statistic::Min],
            AggregationOperator::Max => vec![Statistic::Max],
            AggregationOperator::Topk => vec![Statistic::Topk],
        }
    }

    /// Returns `true` for operators whose result requires approximate pre-aggregation.
    pub fn is_approximate(self) -> bool {
        matches!(
            self,
            AggregationOperator::Quantile
                | AggregationOperator::Sum
                | AggregationOperator::Count
                | AggregationOperator::Avg
                | AggregationOperator::Topk
        )
    }
}

impl std::fmt::Display for AggregationOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AggregationOperator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sum" => Ok(AggregationOperator::Sum),
            "count" => Ok(AggregationOperator::Count),
            "avg" => Ok(AggregationOperator::Avg),
            "quantile" => Ok(AggregationOperator::Quantile),
            "min" => Ok(AggregationOperator::Min),
            "max" => Ok(AggregationOperator::Max),
            "topk" => Ok(AggregationOperator::Topk),
            other => Err(format!("Unknown aggregation operator: '{other}'")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_treatment_type_display() {
        assert_eq!(QueryTreatmentType::Exact.to_string(), "exact");
        assert_eq!(QueryTreatmentType::Approximate.to_string(), "approximate");
    }

    #[test]
    fn test_query_treatment_type_serialization() {
        let exact = QueryTreatmentType::Exact;
        let approximate = QueryTreatmentType::Approximate;

        // Test that they can be serialized/deserialized
        let exact_str = serde_json::to_string(&exact).unwrap();
        let approximate_str = serde_json::to_string(&approximate).unwrap();

        assert_eq!(exact_str, "\"Exact\"");
        assert_eq!(approximate_str, "\"Approximate\"");

        let exact_back: QueryTreatmentType = serde_json::from_str(&exact_str).unwrap();
        let approximate_back: QueryTreatmentType = serde_json::from_str(&approximate_str).unwrap();

        assert_eq!(exact_back, QueryTreatmentType::Exact);
        assert_eq!(approximate_back, QueryTreatmentType::Approximate);
    }
}

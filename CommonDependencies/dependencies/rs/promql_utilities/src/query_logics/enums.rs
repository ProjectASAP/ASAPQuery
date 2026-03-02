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

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
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

    pub fn as_str_slice() -> &'static [&'static str] {
        &["sum", "count", "avg", "quantile", "min", "max", "topk"]
    }
}

impl fmt::Display for AggregationOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AggregationOperator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
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

impl AggregationOperator {
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

/// Concrete aggregation/sketch type used in precompute configs and accumulator dispatch.
///
/// `Display` outputs the canonical PascalCase name used in YAML/JSON configs.
/// `FromStr` accepts the canonical name plus legacy aliases (e.g. "KLL" → `DatasketchesKLL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationType {
    // ---------- single-population (non-keyed) ----------
    Sum,
    Increase,
    MinMax,
    DatasketchesKLL,
    // ---------- multi-population (keyed) ----------
    MultipleSum,
    MultipleIncrease,
    MultipleMinMax,
    HydraKLL,
    CountMinSketch,
    CountMinSketchWithHeap,
    // ---------- cardinality / set tracking ----------
    SetAggregator,
    DeltaSetAggregator,
    HLL,
    // ---------- legacy config wrapper names ----------
    SingleSubpopulation,
    MultipleSubpopulation,
}

impl AggregationType {
    pub fn as_str(self) -> &'static str {
        match self {
            AggregationType::Sum => "Sum",
            AggregationType::Increase => "Increase",
            AggregationType::MinMax => "MinMax",
            AggregationType::DatasketchesKLL => "DatasketchesKLL",
            AggregationType::MultipleSum => "MultipleSum",
            AggregationType::MultipleIncrease => "MultipleIncrease",
            AggregationType::MultipleMinMax => "MultipleMinMax",
            AggregationType::HydraKLL => "HydraKLL",
            AggregationType::CountMinSketch => "CountMinSketch",
            AggregationType::CountMinSketchWithHeap => "CountMinSketchWithHeap",
            AggregationType::SetAggregator => "SetAggregator",
            AggregationType::DeltaSetAggregator => "DeltaSetAggregator",
            AggregationType::HLL => "HLL",
            AggregationType::SingleSubpopulation => "SingleSubpopulation",
            AggregationType::MultipleSubpopulation => "MultipleSubpopulation",
        }
    }

    /// Returns `true` if this type produces keyed (multi-population) accumulators.
    pub fn is_keyed(self) -> bool {
        matches!(
            self,
            AggregationType::MultipleSubpopulation
                | AggregationType::MultipleSum
                | AggregationType::MultipleIncrease
                | AggregationType::MultipleMinMax
                | AggregationType::CountMinSketch
                | AggregationType::CountMinSketchWithHeap
                | AggregationType::HydraKLL
        )
    }

    /// Returns `true` if this type needs a paired key aggregation (SetAggregator / DeltaSetAggregator).
    pub fn is_multi_population_value_type(self) -> bool {
        matches!(
            self,
            AggregationType::MultipleSum
                | AggregationType::MultipleMinMax
                | AggregationType::MultipleIncrease
                | AggregationType::CountMinSketch
                | AggregationType::CountMinSketchWithHeap
        )
    }

    /// Returns `true` if this is a key-tracking aggregation type.
    pub fn is_key_agg_type(self) -> bool {
        matches!(
            self,
            AggregationType::SetAggregator | AggregationType::DeltaSetAggregator
        )
    }
}

impl fmt::Display for AggregationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AggregationType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // Canonical names
            "Sum" => Ok(AggregationType::Sum),
            "Increase" => Ok(AggregationType::Increase),
            "MinMax" => Ok(AggregationType::MinMax),
            "DatasketchesKLL" => Ok(AggregationType::DatasketchesKLL),
            "MultipleSum" => Ok(AggregationType::MultipleSum),
            "MultipleIncrease" => Ok(AggregationType::MultipleIncrease),
            "MultipleMinMax" => Ok(AggregationType::MultipleMinMax),
            "HydraKLL" => Ok(AggregationType::HydraKLL),
            "CountMinSketch" => Ok(AggregationType::CountMinSketch),
            "CountMinSketchWithHeap" => Ok(AggregationType::CountMinSketchWithHeap),
            "SetAggregator" => Ok(AggregationType::SetAggregator),
            "DeltaSetAggregator" => Ok(AggregationType::DeltaSetAggregator),
            "HLL" | "HyperLogLog" => Ok(AggregationType::HLL),
            "SingleSubpopulation" => Ok(AggregationType::SingleSubpopulation),
            "MultipleSubpopulation" => Ok(AggregationType::MultipleSubpopulation),
            // Legacy accumulator-suffixed aliases
            "SumAccumulator" | "SumAggregator" | "sum" => Ok(AggregationType::Sum),
            "IncreaseAccumulator" | "IncreaseAggregator" | "increase" => {
                Ok(AggregationType::Increase)
            }
            "MinMaxAccumulator" | "MinMaxAggregator" | "min_max" => Ok(AggregationType::MinMax),
            "DatasketchesKLLAccumulator" | "KLL" | "kll" | "datasketches_kll" => {
                Ok(AggregationType::DatasketchesKLL)
            }
            "MultipleSumAccumulator" | "multiple_sum" => Ok(AggregationType::MultipleSum),
            "MultipleIncreaseAccumulator" | "multiple_increase" => {
                Ok(AggregationType::MultipleIncrease)
            }
            "MultipleMinMaxAccumulator" | "multiple_min_max" => Ok(AggregationType::MultipleMinMax),
            "HydraKllSketchAccumulator" | "hydra_kll" => Ok(AggregationType::HydraKLL),
            "CountMinSketchAccumulator" | "CMS" | "cms" | "count_min_sketch" => {
                Ok(AggregationType::CountMinSketch)
            }
            "CountMinSketchWithHeapAccumulator" => Ok(AggregationType::CountMinSketchWithHeap),
            "SetAggregatorAccumulator" => Ok(AggregationType::SetAggregator),
            "DeltaSetAggregatorAccumulator" => Ok(AggregationType::DeltaSetAggregator),
            _ => Err(format!("Unknown aggregation type: '{s}'")),
        }
    }
}

impl Serialize for AggregationType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AggregationType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
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

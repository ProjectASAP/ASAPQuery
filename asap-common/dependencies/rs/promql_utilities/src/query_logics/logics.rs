use crate::query_logics::enums::{
    AggregationOperator, AggregationType, PromQLFunction, QueryTreatmentType, Statistic,
};
use tracing::debug;

/// Map statistic to precompute operator based on treatment type
/// This mirrors the Python implementation's logic
pub fn map_statistic_to_precompute_operator(
    statistic: Statistic,
    treatment_type: QueryTreatmentType,
) -> Result<(AggregationType, String), String> {
    debug!(
        "Mapping statistic {:?} with treatment type {:?} to precompute operator",
        statistic, treatment_type
    );
    match statistic {
        Statistic::Quantile => {
            if treatment_type == QueryTreatmentType::Exact {
                Err("Statistic Quantile cannot be computed exactly".to_string())
            } else {
                Ok((AggregationType::DatasketchesKLL, "".to_string()))
                //Ok((AggregationType::HydraKLL, "".to_string()))
            }
        }
        Statistic::Min | Statistic::Max => {
            if treatment_type == QueryTreatmentType::Approximate {
                Ok((AggregationType::DatasketchesKLL, "".to_string()))
                //Ok((AggregationType::HydraKLL, "".to_string()))
            } else {
                Ok((
                    AggregationType::MultipleMinMax,
                    statistic.to_string().to_lowercase(),
                ))
            }
        }
        Statistic::Sum | Statistic::Count => {
            if treatment_type == QueryTreatmentType::Approximate {
                Ok((
                    AggregationType::CountMinSketch,
                    statistic.to_string().to_lowercase(),
                ))
            } else {
                Ok((
                    AggregationType::MultipleSum,
                    statistic.to_string().to_lowercase(),
                ))
            }
        }
        Statistic::Rate | Statistic::Increase => {
            Ok((AggregationType::MultipleIncrease, "".to_string()))
        }
        Statistic::Topk => Ok((AggregationType::CountMinSketchWithHeap, "topk".to_string())),
        Statistic::Cardinality => Ok((AggregationType::SetAggregator, "".to_string())),
    }
}

/// Check if a precompute operator supports subpopulations (multiple keys)
pub fn does_precompute_operator_support_subpopulations(
    statistic: Statistic,
    precompute_operator: AggregationType,
) -> bool {
    debug!(
        "Checking if precompute operator '{}' supports subpopulations for statistic {:?}",
        precompute_operator, statistic
    );
    match precompute_operator {
        // Single-key operators
        AggregationType::Increase
        | AggregationType::MinMax
        | AggregationType::Sum
        | AggregationType::DatasketchesKLL => false,

        // Multi-key operators
        AggregationType::MultipleIncrease
        | AggregationType::MultipleMinMax
        | AggregationType::MultipleSum
        | AggregationType::HydraKLL => true,

        // CountMinSketch supports subpopulations only for certain statistics
        AggregationType::CountMinSketch => matches!(statistic, Statistic::Sum | Statistic::Count),

        // CountMinSketchWithHeap is only supported for Topk — does not support subpopulations
        AggregationType::CountMinSketchWithHeap if matches!(statistic, Statistic::Topk) => false,
        AggregationType::SetAggregator => false,
        AggregationType::DeltaSetAggregator => false,
        AggregationType::HLL => false,

        // Default: not supported
        _ => panic!("Unexpected precompute operator: {}", precompute_operator),
    }
}

/// Check if temporal and spatial aggregations are collapsible.
/// Based on Python implementation in promql_utilities/query_logics/logics.py
pub fn get_is_collapsable(
    temporal_aggregation: PromQLFunction,
    spatial_aggregation: AggregationOperator,
) -> bool {
    debug!(
        "Checking if temporal aggregation '{}' and spatial aggregation '{}' are collapsable",
        temporal_aggregation, spatial_aggregation
    );
    match spatial_aggregation {
        AggregationOperator::Sum => matches!(
            temporal_aggregation,
            // Note: Increase and Rate are commented out in the Python reference
            PromQLFunction::SumOverTime | PromQLFunction::CountOverTime
        ),
        AggregationOperator::Min => temporal_aggregation == PromQLFunction::MinOverTime,
        AggregationOperator::Max => temporal_aggregation == PromQLFunction::MaxOverTime,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_statistic_to_precompute_operator() {
        // Test exact sum
        let result =
            map_statistic_to_precompute_operator(Statistic::Sum, QueryTreatmentType::Exact)
                .unwrap();
        assert_eq!(result, (AggregationType::MultipleSum, "sum".to_string()));

        // Test approximate sum
        let result =
            map_statistic_to_precompute_operator(Statistic::Sum, QueryTreatmentType::Approximate)
                .unwrap();
        assert_eq!(result, (AggregationType::CountMinSketch, "sum".to_string()));

        // Test exact quantile (should fail)
        let result =
            map_statistic_to_precompute_operator(Statistic::Quantile, QueryTreatmentType::Exact);
        assert!(result.is_err());

        // Test approximate quantile
        let result = map_statistic_to_precompute_operator(
            Statistic::Quantile,
            QueryTreatmentType::Approximate,
        )
        .unwrap();
        assert_eq!(result, (AggregationType::DatasketchesKLL, "".to_string()));
        //assert_eq!(result, (AggregationType::HydraKLL, "".to_string()));
    }

    #[test]
    fn test_does_precompute_operator_support_subpopulations() {
        // Test MultipleSum supports subpopulations
        assert!(does_precompute_operator_support_subpopulations(
            Statistic::Sum,
            AggregationType::MultipleSum,
        ));

        // Test DatasketchesKLL does not support subpopulations
        assert!(!does_precompute_operator_support_subpopulations(
            Statistic::Quantile,
            AggregationType::DatasketchesKLL,
        ));

        // Test HydraKLL supports subpopulations
        assert!(does_precompute_operator_support_subpopulations(
            Statistic::Quantile,
            AggregationType::HydraKLL,
        ));

        // Test CountMinSketch with valid statistic
        assert!(does_precompute_operator_support_subpopulations(
            Statistic::Sum,
            AggregationType::CountMinSketch,
        ));
    }

    #[test]
    fn test_topk_maps_to_count_min_sketch_with_heap() {
        let result =
            map_statistic_to_precompute_operator(Statistic::Topk, QueryTreatmentType::Approximate)
                .unwrap();
        assert_eq!(
            result,
            (AggregationType::CountMinSketchWithHeap, "topk".to_string())
        );
    }

    #[test]
    fn test_get_is_collapsable() {
        assert!(get_is_collapsable(
            PromQLFunction::SumOverTime,
            AggregationOperator::Sum
        ));
        assert!(get_is_collapsable(
            PromQLFunction::CountOverTime,
            AggregationOperator::Sum
        ));
        assert!(get_is_collapsable(
            PromQLFunction::MinOverTime,
            AggregationOperator::Min
        ));
        assert!(get_is_collapsable(
            PromQLFunction::MaxOverTime,
            AggregationOperator::Max
        ));
        assert!(!get_is_collapsable(
            PromQLFunction::MinOverTime,
            AggregationOperator::Sum
        ));
        assert!(!get_is_collapsable(
            PromQLFunction::Rate,
            AggregationOperator::Sum
        ));
    }
}

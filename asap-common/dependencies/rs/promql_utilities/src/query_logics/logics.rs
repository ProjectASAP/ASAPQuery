use crate::query_logics::enums::{QueryTreatmentType, Statistic};
use tracing::debug;

/// Map statistic to precompute operator based on treatment type
/// This mirrors the Python implementation's logic
pub fn map_statistic_to_precompute_operator(
    statistic: Statistic,
    treatment_type: QueryTreatmentType,
) -> Result<(String, String), String> {
    debug!(
        "Mapping statistic {:?} with treatment type {:?} to precompute operator",
        statistic, treatment_type
    );
    match statistic {
        Statistic::Quantile => {
            if treatment_type == QueryTreatmentType::Exact {
                Err("Statistic Quantile cannot be computed exactly".to_string())
            } else {
                Ok(("DatasketchesKLL".to_string(), "".to_string()))
                //Ok(("HydraKLL".to_string(), "".to_string()))
            }
        }
        Statistic::Min | Statistic::Max => {
            if treatment_type == QueryTreatmentType::Approximate {
                Ok(("DatasketchesKLL".to_string(), "".to_string()))
                //Ok(("HydraKLL".to_string(), "".to_string()))
            } else {
                Ok((
                    "MultipleMinMax".to_string(),
                    statistic.to_string().to_lowercase(),
                ))
            }
        }
        Statistic::Sum | Statistic::Count => {
            if treatment_type == QueryTreatmentType::Approximate {
                Ok((
                    "CountMinSketch".to_string(),
                    statistic.to_string().to_lowercase(),
                ))
            } else {
                Ok((
                    "MultipleSum".to_string(),
                    statistic.to_string().to_lowercase(),
                ))
            }
        }
        Statistic::Rate | Statistic::Increase => {
            Ok(("MultipleIncrease".to_string(), "".to_string()))
        }
        Statistic::Topk => Ok(("CountMinSketchWithHeap".to_string(), "topk".to_string())),
        _ => Err(format!("Statistic {statistic:?} not supported")),
    }
}

/// Check if a precompute operator supports subpopulations (multiple keys)
pub fn does_precompute_operator_support_subpopulations(
    statistic: Statistic,
    precompute_operator: &str,
) -> bool {
    debug!(
        "Checking if precompute operator '{}' supports subpopulations for statistic {:?}",
        precompute_operator, statistic
    );
    match precompute_operator {
        // Single-key operators
        "Increase" | "MinMax" | "Sum" | "DatasketchesKLL" => false,

        // Multi-key operators
        "MultipleIncrease" | "MultipleMinMax" | "MultipleSum" | "HydraKLL" => true,

        // CountMinSketch supports subpopulations only for certain statistics
        "CountMinSketch" => matches!(statistic, Statistic::Sum | Statistic::Count),

        // "CountMinSketchWithHeap" is only supported for Topk
        // Other usages of CountMinSketchWithHeap will fall through.
        "CountMinSketchWithHeap" if matches!(statistic, Statistic::Topk) => false,

        // Default: not supported
        _ => panic!("Unexpected precompute operator: {}", precompute_operator),
    }
}

/// Check if temporal and spatial aggregations are collapsible
/// Based on Python implementation in promql_utilities/query_logics/logics.py
pub fn get_is_collapsable(temporal_aggregation: &str, spatial_aggregation: &str) -> bool {
    debug!(
        "Checking if temporal aggregation '{}' and spatial aggregation '{}' are collapsable",
        temporal_aggregation, spatial_aggregation
    );
    match spatial_aggregation {
        "sum" => matches!(
            temporal_aggregation,
            "sum_over_time" | "count_over_time" // Note: "increase" and "rate" are commented out in Python
        ),
        "min" => temporal_aggregation == "min_over_time",
        "max" => temporal_aggregation == "max_over_time",
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
        assert_eq!(result, ("MultipleSum".to_string(), "sum".to_string()));

        // Test approximate sum
        let result =
            map_statistic_to_precompute_operator(Statistic::Sum, QueryTreatmentType::Approximate)
                .unwrap();
        assert_eq!(result, ("CountMinSketch".to_string(), "sum".to_string()));

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
        assert_eq!(result, ("DatasketchesKLL".to_string(), "".to_string()));
        //assert_eq!(result, ("HydraKLL".to_string(), "".to_string()));
    }

    #[test]
    fn test_does_precompute_operator_support_subpopulations() {
        // Test MultipleSum supports subpopulations
        assert!(does_precompute_operator_support_subpopulations(
            Statistic::Sum,
            "MultipleSum"
        ));

        // Test DatasketchesKLL does not support subpopulations
        assert!(!does_precompute_operator_support_subpopulations(
            Statistic::Quantile,
            "DatasketchesKLL"
        ));

        // Test HydraKLL supports subpopulations
        assert!(does_precompute_operator_support_subpopulations(
            Statistic::Quantile,
            "HydraKLL"
        ));

        // Test CountMinSketch with valid statistic
        assert!(does_precompute_operator_support_subpopulations(
            Statistic::Sum,
            "CountMinSketch"
        ));
    }

    #[test]
    fn test_topk_maps_to_count_min_sketch_with_heap() {
        let result =
            map_statistic_to_precompute_operator(Statistic::Topk, QueryTreatmentType::Approximate)
                .unwrap();
        assert_eq!(
            result,
            ("CountMinSketchWithHeap".to_string(), "topk".to_string())
        );
    }

    #[test]
    fn test_get_is_collapsable() {
        assert!(get_is_collapsable("sum_over_time", "sum"));
        assert!(get_is_collapsable("count_over_time", "sum"));
        assert!(get_is_collapsable("min_over_time", "min"));
        assert!(get_is_collapsable("max_over_time", "max"));
        assert!(!get_is_collapsable("min_over_time", "sum"));
        assert!(!get_is_collapsable("unknown", "sum"));
    }
}

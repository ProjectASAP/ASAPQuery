/// The sketch types supported by PromSketch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PromSketchType {
    /// ExponentialHistogram wrapping UnivMon (entropy, cardinality, L1, L2, distinct).
    EHUniv,
    /// ExponentialHistogram wrapping KLL (quantile, min, max).
    EHKLL,
    /// ExponentialHistogram wrapping UniformSampling (avg, count, sum, stddev, stdvar).
    USampling,
}

/// Maps a PromQL function name to the sketch types it requires.
pub fn promsketch_func_map(func_name: &str) -> Option<&'static [PromSketchType]> {
    match func_name {
        "entropy_over_time" => Some(&[PromSketchType::EHUniv]),
        "distinct_over_time" => Some(&[PromSketchType::EHUniv]),
        "l1_over_time" => Some(&[PromSketchType::EHUniv]),
        "l2_over_time" => Some(&[PromSketchType::EHUniv]),
        "quantile_over_time" => Some(&[PromSketchType::EHKLL]),
        "min_over_time" => Some(&[PromSketchType::EHKLL]),
        "max_over_time" => Some(&[PromSketchType::EHKLL]),
        "avg_over_time" => Some(&[PromSketchType::USampling]),
        "count_over_time" => Some(&[PromSketchType::USampling]),
        "sum_over_time" => Some(&[PromSketchType::USampling]),
        "sum2_over_time" => Some(&[PromSketchType::USampling]),
        "stddev_over_time" => Some(&[PromSketchType::USampling]),
        "stdvar_over_time" => Some(&[PromSketchType::USampling]),
        _ => None,
    }
}

/// Returns `true` when `func_name` maps to a USampling-backed sketch function.
pub fn is_usampling_function(func_name: &str) -> bool {
    matches!(promsketch_func_map(func_name),
        Some(types) if types.contains(&PromSketchType::USampling))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_promsketch_func_map_coverage() {
        assert_eq!(
            promsketch_func_map("entropy_over_time"),
            Some([PromSketchType::EHUniv].as_slice())
        );
        assert_eq!(
            promsketch_func_map("quantile_over_time"),
            Some([PromSketchType::EHKLL].as_slice())
        );
        assert_eq!(
            promsketch_func_map("avg_over_time"),
            Some([PromSketchType::USampling].as_slice())
        );
        assert!(promsketch_func_map("nonexistent").is_none());
    }

    #[test]
    fn test_is_usampling_function() {
        // USampling functions
        assert!(is_usampling_function("avg_over_time"));
        assert!(is_usampling_function("count_over_time"));
        assert!(is_usampling_function("sum_over_time"));
        assert!(is_usampling_function("sum2_over_time"));
        assert!(is_usampling_function("stddev_over_time"));
        assert!(is_usampling_function("stdvar_over_time"));

        // Non-USampling functions
        assert!(!is_usampling_function("entropy_over_time"));
        assert!(!is_usampling_function("quantile_over_time"));
        assert!(!is_usampling_function("min_over_time"));
        assert!(!is_usampling_function("nonexistent"));
    }
}

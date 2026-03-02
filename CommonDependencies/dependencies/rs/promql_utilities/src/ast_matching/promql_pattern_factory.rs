//use crate::ast_matching::{PromQLPattern, PromQLPatternBuilder};
//use tracing::debug;
//
///// Pattern factory for creating common PromQL patterns
//pub struct PromQLPatternFactory;
//
//impl PromQLPatternFactory {
//    /// Create pattern for OnlyTemporal queries (e.g., rate(metric[5m]))
//    pub fn only_temporal_pattern() -> PromQLPattern {
//        debug!("Creating only temporal pattern");
//        let ms = PromQLPatternBuilder::matrix_selector(
//            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
//            None,
//            Some("range_vector"),
//        );
//
//        let func_args: Vec<Option<std::collections::HashMap<String, serde_json::Value>>> = vec![ms];
//
//        let pattern = PromQLPatternBuilder::function(
//            vec![
//                "rate",
//                "increase",
//                "sum_over_time",
//                "avg_over_time",
//                "min_over_time",
//                "max_over_time",
//                "count_over_time",
//            ],
//            func_args,
//            Some("function"),
//            None,
//        );
//
//        PromQLPattern::new(
//            pattern,
//            //vec![
//            //    "metric".to_string(),
//            //    "function".to_string(),
//            //    "range_vector".to_string(),
//            //],
//            // QueryPatternType::OnlyTemporal,
//        )
//    }
//
//    /// Create pattern for OnlySpatial queries (e.g., sum(metric) by (label))
//    pub fn only_spatial_pattern() -> PromQLPattern {
//        debug!("Creating only spatial pattern");
//        let metric = PromQLPatternBuilder::metric(None, None, None, Some("metric"));
//
//        let pattern = PromQLPatternBuilder::aggregation(
//            vec!["sum", "count", "avg", "min", "max", "quantile"],
//            metric,
//            None,
//            None,
//            None,
//            Some("aggregation"),
//        );
//
//        PromQLPattern::new(
//            pattern,
//            //vec!["metric".to_string(), "aggregation".to_string()],
//            // QueryPatternType::OnlySpatial,
//        )
//    }
//
//    /// Create pattern for OneTemporalOneSpatial queries (e.g., sum(rate(metric[5m])) by (label))
//    pub fn one_temporal_one_spatial_pattern() -> PromQLPattern {
//        debug!("Creating one temporal one spatial pattern");
//        let ms2 = PromQLPatternBuilder::matrix_selector(
//            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
//            None,
//            Some("range_vector"),
//        );
//
//        let func_args2: Vec<Option<std::collections::HashMap<String, serde_json::Value>>> =
//            vec![ms2];
//
//        let temporal_part = PromQLPatternBuilder::function(
//            vec![
//                "rate",
//                "increase",
//                "sum_over_time",
//                "avg_over_time",
//                "min_over_time",
//                "max_over_time",
//                "count_over_time",
//            ],
//            func_args2,
//            Some("function"),
//            None,
//        );
//
//        let pattern = PromQLPatternBuilder::aggregation(
//            vec!["sum", "count", "avg", "min", "max", "quantile"],
//            temporal_part,
//            None,
//            None,
//            None,
//            Some("aggregation"),
//        );
//
//        PromQLPattern::new(
//            pattern,
//            //vec![
//            //    "metric".to_string(),
//            //    "function".to_string(),
//            //    "range_vector".to_string(),
//            //    "aggregation".to_string(),
//            //],
//            // QueryPatternType::OneTemporalOneSpatial,
//        )
//    }
//
//    /// Get all standard patterns
//    pub fn get_all_patterns() -> Vec<PromQLPattern> {
//        debug!("Getting all standard patterns");
//        vec![
//            Self::one_temporal_one_spatial_pattern(),
//            Self::only_temporal_pattern(),
//            Self::only_spatial_pattern(),
//        ]
//    }
//}

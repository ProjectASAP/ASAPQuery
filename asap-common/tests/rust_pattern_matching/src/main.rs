use promql_utilities::ast_matching::{PromQLPattern, PromQLPatternBuilder};
use promql_utilities::query_logics::enums::QueryPatternType;
use serde_json::Value;
use std::collections::HashMap;

// Helper functions (these would be closures or separate methods)
fn temporal_pattern(
    pattern_type: &str,
    blocks: &HashMap<String, Option<HashMap<String, Value>>>,
) -> PromQLPattern {
    PromQLPattern::new(blocks[pattern_type].clone())
}

fn spatial_pattern(
    pattern_type: &str,
    blocks: &HashMap<String, Option<HashMap<String, Value>>>,
) -> PromQLPattern {
    PromQLPattern::new(blocks[pattern_type].clone())
}

fn spatial_of_temporal_pattern(temporal_block: &Option<HashMap<String, Value>>) -> PromQLPattern {
    let pattern = PromQLPatternBuilder::aggregation(
        vec!["sum", "count", "avg", "quantile", "min", "max"],
        temporal_block.clone(),
        None,
        None,
        None,
        Some("aggregation"),
    );
    PromQLPattern::new(pattern)
}

fn main() {
    let mut temporal_pattern_blocks = HashMap::new();
    temporal_pattern_blocks.insert(
        "quantile".to_string(),
        PromQLPatternBuilder::function(
            vec!["quantile_over_time"],
            vec![
                PromQLPatternBuilder::number(None, Some("quantile_param")),
                PromQLPatternBuilder::matrix_selector(
                    PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                    None,
                    Some("range_vector"),
                ),
            ],
            Some("function"),
            Some("function_args"),
        ),
    );

    temporal_pattern_blocks.insert(
        "generic".to_string(),
        PromQLPatternBuilder::function(
            vec![
                "sum_over_time",
                "count_over_time",
                "avg_over_time",
                "min_over_time",
                "max_over_time",
                "increase",
                "rate",
            ],
            vec![PromQLPatternBuilder::matrix_selector(
                PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                None,
                Some("range_vector"),
            )],
            Some("function"),
            Some("function_args"),
        ),
    );

    // Create spatial pattern blocks
    let mut spatial_pattern_blocks = HashMap::new();
    spatial_pattern_blocks.insert(
        "generic".to_string(),
        PromQLPatternBuilder::aggregation(
            vec!["sum", "count", "avg", "quantile", "min", "max"],
            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
            None,
            None,
            None,
            Some("aggregation"),
        ),
    );

    // Create controller patterns
    let mut controller_patterns = HashMap::new();
    controller_patterns.insert(
        QueryPatternType::OnlyTemporal,
        vec![
            temporal_pattern("quantile", &temporal_pattern_blocks),
            temporal_pattern("generic", &temporal_pattern_blocks),
        ],
    );
    controller_patterns.insert(
        QueryPatternType::OnlySpatial,
        vec![spatial_pattern("generic", &spatial_pattern_blocks)],
    );
    controller_patterns.insert(
        QueryPatternType::OneTemporalOneSpatial,
        vec![
            spatial_of_temporal_pattern(&temporal_pattern_blocks["quantile"]),
            spatial_of_temporal_pattern(&temporal_pattern_blocks["generic"]),
        ],
    );

    let queries = vec![
        // "sum_over_time(fake_metric_total[1m])",
        // "count_over_time(fake_metric_total[1m])",
        // "quantile_over_time(0.95, fake_metric_total[1m])",
        // "sum by (instance, job) (fake_metric_total)",
        // "count without (instance) (fake_metric_total)",
        // "quantile by (instance) (0.95, fake_metric_total)",
        // "sum by (instance, job) (rate(fake_metric_total[1m]))",
        "sum by (instance, job) (sum_over_time(fake_metric_total[1m]))",
        "sum by (instance, job) (count_over_time(fake_metric_total[1m]))",
    ];

    for query in queries {
        let ast = match promql_parser::parser::parse(query) {
            Ok(parsed) => parsed,
            Err(e) => {
                eprintln!("Failed to parse query '{}': {}", query, e);
                continue;
            }
        };

        let mut found_match = None;
        for (pattern_type, patterns) in &controller_patterns {
            for pattern in patterns {
                // println!(
                //     "Trying pattern type: {:?} for query: {}",
                //     pattern_type, query
                // );
                let match_result = pattern.matches(&ast);
                if match_result.matches {
                    println!("Query: {}; Pattern: {:?}", query, pattern_type);
                    println!("Match result: {:?}", match_result);
                    found_match = Some((*pattern_type, match_result));
                    break;
                }
            }
            if found_match.is_some() {
                break;
            }
        }
    }
}

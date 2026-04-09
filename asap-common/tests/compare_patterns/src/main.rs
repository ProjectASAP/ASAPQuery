use promql_utilities::ast_matching::{PromQLPattern, PromQLPatternBuilder};
use std::collections::HashMap;
use std::fs;

fn main() {
    let mut out: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

    // ONLY_TEMPORAL patterns
    let mut only_temporal_patterns = Vec::new();

    // Pattern 1: rate/increase functions
    let ms1 = PromQLPatternBuilder::matrix_selector(
        PromQLPatternBuilder::metric(None, None, None, Some("metric")),
        None,
        Some("range_vector"),
    );
    let func_args1: Vec<Option<std::collections::HashMap<String, serde_json::Value>>> = vec![ms1];
    let pattern_1 = PromQLPatternBuilder::function(
        vec!["rate", "increase"],
        func_args1,
        Some("function"),
        None,
    );
    let pattern1 = PromQLPattern::new(pattern_1);
    if let Some(ast) = pattern1.ast_pattern {
        only_temporal_patterns.push(serde_json::Value::Object(ast.into_iter().collect()));
    }

    // Pattern 2: quantile_over_time function
    let ms2 = PromQLPatternBuilder::matrix_selector(
        PromQLPatternBuilder::metric(None, None, None, Some("metric")),
        None,
        Some("range_vector"),
    );
    let func_args2: Vec<Option<std::collections::HashMap<String, serde_json::Value>>> =
        vec![PromQLPatternBuilder::number(None, None), ms2];
    let pattern_2 = PromQLPatternBuilder::function(
        vec!["quantile_over_time"],
        func_args2,
        Some("function"),
        Some("function_args"),
    );
    let pattern2 = PromQLPattern::new(pattern_2);
    if let Some(ast) = pattern2.ast_pattern {
        only_temporal_patterns.push(serde_json::Value::Object(ast.into_iter().collect()));
    }

    out.insert("ONLY_TEMPORAL".to_string(), only_temporal_patterns);

    // ONLY_SPATIAL patterns
    let mut only_spatial_patterns = Vec::new();

    // Pattern 1: aggregation functions
    let pattern_3 = PromQLPatternBuilder::aggregation(
        vec!["sum", "count", "avg", "quantile", "min", "max"],
        PromQLPatternBuilder::metric(None, None, None, Some("metric")),
        None,
        None,
        None,
        Some("aggregation"),
    );
    let pattern3 = PromQLPattern::new(pattern_3);
    if let Some(ast) = pattern3.ast_pattern {
        only_spatial_patterns.push(serde_json::Value::Object(ast.into_iter().collect()));
    }

    // Pattern 2: basic metric
    let pattern_4 = PromQLPatternBuilder::metric(None, None, None, Some("metric"));
    let pattern4 = PromQLPattern::new(pattern_4);
    if let Some(ast) = pattern4.ast_pattern {
        only_spatial_patterns.push(serde_json::Value::Object(ast.into_iter().collect()));
    }

    out.insert("ONLY_SPATIAL".to_string(), only_spatial_patterns);

    // ONE_TEMPORAL_ONE_SPATIAL patterns
    let mut one_temporal_one_spatial_patterns = Vec::new();

    // Pattern 1: aggregation of quantile_over_time
    let ms3 = PromQLPatternBuilder::matrix_selector(
        PromQLPatternBuilder::metric(None, None, None, Some("metric")),
        None,
        Some("range_vector"),
    );
    let quantile_func_args: Vec<Option<std::collections::HashMap<String, serde_json::Value>>> =
        vec![PromQLPatternBuilder::number(None, None), ms3];
    let quantile_func = PromQLPatternBuilder::function(
        vec!["quantile_over_time"],
        quantile_func_args,
        Some("function"),
        Some("function_args"),
    );
    let pattern_5 = PromQLPatternBuilder::aggregation(
        vec!["sum", "count", "avg", "quantile", "min", "max"],
        quantile_func,
        None,
        None,
        None,
        Some("aggregation"),
    );
    let pattern5 = PromQLPattern::new(pattern_5);
    if let Some(ast) = pattern5.ast_pattern {
        one_temporal_one_spatial_patterns
            .push(serde_json::Value::Object(ast.into_iter().collect()));
    }

    // Pattern 2: aggregation of various temporal functions
    let ms4 = PromQLPatternBuilder::matrix_selector(
        PromQLPatternBuilder::metric(None, None, None, Some("metric")),
        None,
        Some("range_vector"),
    );
    let temporal_func_args: Vec<Option<std::collections::HashMap<String, serde_json::Value>>> =
        vec![ms4];
    let temporal_func = PromQLPatternBuilder::function(
        vec![
            "sum_over_time",
            "count_over_time",
            "avg_over_time",
            "min_over_time",
            "max_over_time",
            "rate",
            "increase",
        ],
        temporal_func_args,
        Some("function"),
        None,
    );
    let pattern_6 = PromQLPatternBuilder::aggregation(
        vec!["sum", "count", "avg", "quantile", "min", "max"],
        temporal_func,
        None,
        None,
        None,
        Some("aggregation"),
    );
    let pattern6 = PromQLPattern::new(pattern_6);
    if let Some(ast) = pattern6.ast_pattern {
        one_temporal_one_spatial_patterns
            .push(serde_json::Value::Object(ast.into_iter().collect()));
    }

    out.insert(
        "ONE_TEMPORAL_ONE_SPATIAL".to_string(),
        one_temporal_one_spatial_patterns,
    );

    let out_dir = std::path::Path::new("./out");
    std::fs::create_dir_all(out_dir).unwrap();
    let out_path = out_dir.join("rust_patterns.json");
    // sort by keys
    let sorted: HashMap<_, _> = out.into_iter().collect();
    let s = serde_json::to_string_pretty(&sorted).unwrap();
    fs::write(&out_path, s).unwrap();
    println!("Wrote {}", out_path.display());
}

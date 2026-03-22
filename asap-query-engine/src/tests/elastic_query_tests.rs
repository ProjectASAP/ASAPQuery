#[cfg(test)]
use crate::engines::SimpleEngine;
use promql_parser::label;
use serde_json::{json, Value};
use crate::QueryResult;

use crate::data_model::{AggregateCore, KeyByLabelValues};
use crate::precompute_operators::{
    CountMinSketchAccumulator, DatasketchesKLLAccumulator, DeltaSetAggregatorAccumulator,
    SumAccumulator,
};

use crate::tests::test_utilities::{self, create_engine_multi_timestamp, create_engine_single_pop};

fn create_kll_accumulator_with_values(values: &[f64]) -> DatasketchesKLLAccumulator {
    let mut kll = DatasketchesKLLAccumulator::new(200);
    for &v in values {
        kll._update(v);
    }
    kll
}

fn create_kll_data_with_timestamps(
    timestamps: &[u64],
    label_values: Vec<Option<Vec<String>>>,
) -> Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)> {
    let mut result = Vec::new();
    for label_value in label_values {
        println!("Creating KLL histogram for label value: {label_value:?}");
        result.extend(timestamps.iter().enumerate().map(|(i, &timestamp)| {
            (
                timestamp,
                label_value.clone(),
                Box::new(create_kll_accumulator_with_values(
                    (i * 100 + 1..=i * 100 + 100)
                        .map(|v| v as f64)
                        .collect::<Vec<f64>>()
                        .as_slice(),
                )) as Box<dyn AggregateCore>,
            )
        }))
    }
    result
}

#[test]
fn test_esdsl_simple_aggregation_quantile() {
    // let _ = tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::DEBUG)
    //     .with_test_writer() // Routes output through the test runner's capture mechanism
    //     .try_init();

    // Elastic DSL query (batch filtered).
    let elastic_query = json!({
        "size": 0,
        "aggs": {
            "out": {
                "percentiles": {
                    "field": "http_requests",
                    "percents": [90]
                }
            }
        }
    });

    // Create data. Engine expects 1 second (1000 ms) intervals.
    let timestamps = vec![999_000, 1_000_000];
    let label_values = vec![
        Some(Vec::new()) // No labels for this test
    ];
    let kll_data = create_kll_data_with_timestamps(&timestamps, label_values);

    let engine = create_engine_multi_timestamp(
        "http_requests",
        "DatasketchesKLLAccumulator",
        Vec::new(), // No labels for this test
        kll_data,
        &elastic_query.to_string(),
    );

    let time = 1_000.0; // Arbitrary timestamp for testing
    let output = engine.handle_query_elastic(elastic_query.to_string(), time);
    if let Some((_, result)) = output {
        match &result {
            QueryResult::Vector(instant) => {
                assert_eq!(instant.values.len(), 1);
                let sample = &instant.values[0];
                assert_eq!(sample.labels, KeyByLabelValues::new()); // No labels expected
            }
            _ => {
                panic!("Expected Vector result");
            }
        }
        let result_json = serde_json::to_string(&result).unwrap();
        println!("Query Result: {result_json}");
    } else {
        panic!("Expected query result, got None");
    }
}

#[test]
fn test_esdsl_single_label_groupby_aggregation_quantile() {
    // let _ = tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::DEBUG)
    //     .with_test_writer() // Routes output through the test runner's capture mechanism
    //     .try_init();

    // Elastic DSL query (batch filtered).
    let elastic_query = json!({
        "size": 0,
        "aggs": {
            "out": {
                "terms": {
                    "field": "host.keyword"
                },
                "aggs": {
                    "out": {
                        "percentiles": {
                            "field": "http_requests",
                            "percents": [90]
                        }
                    }
                }
            }
        }
    });

    // Create data. Engine expects 1 second (1000 ms) intervals.
    let timestamps = vec![999_000, 1_000_000];
    let label_values = vec![
        Some(vec!["host-a".to_string()]),
        Some(vec!["host-b".to_string()]),
    ];
    let kll_data = create_kll_data_with_timestamps(&timestamps, label_values);

    let engine = create_engine_multi_timestamp(
        "http_requests",
        "DatasketchesKLLAccumulator",
        vec!["host"],
        kll_data,
        &elastic_query.to_string(),
    );

    let time = 1_000.0; // Arbitrary timestamp for testing
    let output = engine.handle_query_elastic(elastic_query.to_string(), time);
    if let Some((_, result)) = output {
        match &result {
            QueryResult::Vector(instant) => {
                assert_eq!(instant.values.len(), 2);
                let label_combinations = vec![
                    "host-a",
                    "host-b",
                ];
                let mut found_combinations = Vec::new();
                for sample in instant.values.iter() {
                    let label_string = sample.labels.to_semicolon_str();
                    found_combinations.push(label_string);
                }
                for expected in label_combinations {
                    assert!(
                        found_combinations.contains(&expected.to_string()),
                        "Expected label combination not found: {expected}"
                    );
                }
                    
            }
            _ => {
                panic!("Expected Vector result");
            }
        }
        let result_json = serde_json::to_string(&result).unwrap();
        println!("Query Result: {result_json}");
    } else {
        panic!("Expected query result, got None");
    }
}

#[test]
fn test_esdsl_multi_label_groupby_aggregation_quantile() {
    // let _ = tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::DEBUG)
    //     .with_test_writer() // Routes output through the test runner's capture mechanism
    //     .try_init();

    // Elastic DSL query (batch filtered).
    let elastic_query = json!({
        "size": 0,
        "aggs": {
            "out": {
                "multi_terms": {
                    "terms": [
                        {
                            "field": "host.keyword"
                        },
                        {
                            "field": "region.keyword"
                        }
                    ]
                },
                "aggs": {
                    "out": {
                        "percentiles": {
                            "field": "http_requests",
                            "percents": [90]
                        }
                    }
                }
            }
        }
    });

    // Create data. Engine expects 1 second (1000 ms) intervals.
    let timestamps = vec![998_000, 999_000, 1_000_000];
    let label_values = vec![
        Some(vec!["host-a".to_string(), "region-a".to_string()]),
        Some(vec!["host-b".to_string(), "region-b".to_string()]),
        Some(vec!["host-c".to_string(), "region-c".to_string()]),
        Some(vec!["host-b".to_string(), "region-c".to_string()]),
    ];
    let kll_data = create_kll_data_with_timestamps(&timestamps, label_values);

    let engine = create_engine_multi_timestamp(
        "http_requests",
        "DatasketchesKLLAccumulator",
        vec!["host", "region"],
        kll_data,
        &elastic_query.to_string(),
    );

    let time = 1_000.0; // Arbitrary timestamp for testing
    let output = engine.handle_query_elastic(elastic_query.to_string(), time);
    if let Some((_, result)) = output {
        match &result {
            QueryResult::Vector(instant) => {
                assert_eq!(instant.values.len(), 4);
                let label_combinations = vec![
                    "host-a;region-a",
                    "host-b;region-b",
                    "host-c;region-c",
                    "host-b;region-c",
                ];
                let mut found_combinations = Vec::new();
                for sample in instant.values.iter() {
                    let label_string = sample.labels.to_semicolon_str();
                    found_combinations.push(label_string);
                }
                for expected in label_combinations {
                    assert!(
                        found_combinations.contains(&expected.to_string()),
                        "Expected label combination not found: {expected}"
                    );
                }
                    
            }
            _ => {
                panic!("Expected Vector result");
            }
        }
        let result_json = serde_json::to_string(&result).unwrap();
        println!("Query Result: {result_json}");
    } else {
        panic!("Expected query result, got None");
    }
}

#[test]
fn test_esdsl_unsupported_query() {
    // let _ = tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::DEBUG)
    //     .with_test_writer() // Routes output through the test runner's capture mechanism
    //     .try_init();

    // Elastic DSL query (batch filtered).
    let elastic_query = json!({
        "size": 0,
        "aggs": {
            "out": {
                "multi_terms": {
                    "terms": [
                        {
                            "field": "host.keyword"
                        },
                        {
                            "field": "region.keyword"
                        }
                    ]
                },
                "aggs": {
                    "out": {
                        "fake_aggregation": {
                            "field": "http_requests"
                        }
                    }
                }
            }
        }
    });

    // Create data. Engine expects 1 second (1000 ms) intervals.
    let timestamps = vec![998_000, 999_000, 1_000_000];
    let label_values = vec![
        Some(vec!["host-a".to_string(), "region-a".to_string()]),
        Some(vec!["host-b".to_string(), "region-b".to_string()]),
        Some(vec!["host-c".to_string(), "region-c".to_string()]),
        Some(vec!["host-b".to_string(), "region-c".to_string()]),
    ];
    let kll_data = create_kll_data_with_timestamps(&timestamps, label_values);

    let engine = create_engine_multi_timestamp(
        "http_requests",
        "DatasketchesKLLAccumulator",
        vec!["host", "region"],
        kll_data,
        &elastic_query.to_string(),
    );

    let time = 1_000.0; // Arbitrary timestamp for testing
    let output = engine.handle_query_elastic(elastic_query.to_string(), time);
    assert!(output.is_none(), "Expected None for unsupported query, got Some({:?})", output);
}
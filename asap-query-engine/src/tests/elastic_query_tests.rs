#[cfg(test)]
use crate::data_model::{CleanupPolicy, InferenceConfig, QueryLanguage, StreamingConfig};
use crate::drivers::query::adapters::AdapterConfig;
use crate::drivers::query::servers::http::{HttpServer, HttpServerConfig};
use crate::engines::SimpleEngine;
use crate::stores::simple_map_store::SimpleMapStore;
use reqwest::Client;
use serde_json::{json, Value};
use sketchlib_rust::elastic;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

use crate::data_model::{AggregateCore, KeyByLabelValues, PrecomputedOutput};
use crate::precompute_operators::{
    DatasketchesKLLAccumulator, DeltaSetAggregatorAccumulator, CountMinSketchAccumulator, SumAccumulator,
};

use crate::tests::test_utilities::{self, create_engine_multi_timestamp, create_engine_single_pop};

#[test]
fn test_esdsl_groupby_aggregation_query_sum() {

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer() // Routes output through the test runner's capture mechanism
        .try_init();

    // Elastic DSL query (batch filtered).
    let elastic_query = json!({
        "size": 0,                                         
        "aggs": {
            "out": {
                "filters": {
                    "filters": {
                        "bucket1": {
                            "term": { "host.keyword": "host-a" },
                        },
                        "bucket2": {
                            "term": { "host.keyword": "host-b" },
                        }
                    }
                },
                "aggs": {
                    "out": {
                        "sum": {
                            "field": "http_requests",
                        }
                    }
                }
            }
        }
    });

    let engine = create_engine_single_pop(
        "http_requests",
        "SumAccumulator",
        vec!["host"],
        vec![
            (
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)),
            ),
            (
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(200.0)),
            ),
        ],
        &elastic_query.to_string(),
    );

    let time = 1_000.0; // Arbitrary timestamp for testing
    let output = engine.handle_query_elastic(elastic_query.to_string(), time);
    if let Some((_, result)) = output {
        let result_json = serde_json::to_string(&result).unwrap();
        println!("Query Result: {result_json}");
    } else {
        panic!("Expected query result, got None");
    }

}

#[test]
fn test_esdsl_groupby_aggregation_quantile() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer() // Routes output through the test runner's capture mechanism
        .try_init();

    let mut kll_a_1 = DatasketchesKLLAccumulator::new(200);
    for v in 1..=100 {
        kll_a_1._update(v as f64);
    }
    let mut kll_a_2 = DatasketchesKLLAccumulator::new(200);
    for v in 101..=200 {
        kll_a_2._update(v as f64);
    }
    let mut kll_b_1 = DatasketchesKLLAccumulator::new(200);
    for v in 1..=200 {
        kll_b_1._update(v as f64);
    }
    let mut kll_b_2 = DatasketchesKLLAccumulator::new(200);
    for v in 201..=400 {
        kll_b_2._update(v as f64);
    }

    // Elastic DSL query (batch filtered).
    let elastic_query = json!({
        "size": 0,                                         
        "aggs": {
            "out": {
                "filters": {
                    "filters": {
                        "bucket1": {
                            "term": { "host.keyword": "host-a" },
                        },
                        "bucket2": {
                            "term": { "host.keyword": "host-b" },
                        }
                    }
                },
                "aggs": {
                    "out": {
                        "percentiles": {
                            "field": "http_requests",
                            "percents": [0.90]
                        }
                    }
                }
            }
        }
    });

    let engine = create_engine_multi_timestamp(
        "http_requests",
        "DatasketchesKLLAccumulator",
        vec!["host"],
        vec![
            (
                999_000,
                Some(vec!["host-a".to_string()]),
                Box::new(kll_a_1),
            ),
            (
                999_000,
                Some(vec!["host-b".to_string()]),
                Box::new(kll_b_1),
            ),
            (
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(kll_a_2),
            ),
            (
                1_000_000,
                Some(vec!["host-b".to_string()]),
                Box::new(kll_b_2),
            ),
        ],
        &elastic_query.to_string(),
    );

    let time = 1_000.0; // Arbitrary timestamp for testing
    let output = engine.handle_query_elastic(elastic_query.to_string(), time);
    if let Some((_, result)) = output {
        let result_json = serde_json::to_string(&result).unwrap();
        println!("Query Result: {result_json}");
    } else {
        panic!("Expected query result, got None");
    }

}


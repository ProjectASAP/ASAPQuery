use asap_planner::{ElasticController, ElasticRuntimeOptions, PlannerOutput, StreamingEngine};
use asap_types::{QueryLanguage, SchemaConfig};
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;

fn indent_block(text: &str, indent: usize) -> String {
    let padding = " ".repeat(indent);
    text.trim()
        .lines()
        .map(|line| format!("{}{}", padding, line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn elastic_yaml(index: &str, time_field: &str, query: &str, t_repeat_ms: u64) -> String {
    format!(
        r#"
query_groups:
  - id: 1
    index: {index}
    time_field: {time_field}
    queries:
      - |
{query}
    repetition_delay_ms: {t_repeat_ms}
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 1.0
aggregate_cleanup:
  policy: read_based
"#,
        index = index,
        time_field = time_field,
        query = indent_block(query, 8),
        t_repeat_ms = t_repeat_ms,
    )
}

fn elastic_output(index: &str, time_field: &str, query: &str, t_repeat_ms: u64) -> PlannerOutput {
    elastic_output_with_interval(index, time_field, query, t_repeat_ms, 15_000)
}

fn elastic_output_with_interval(
    index: &str,
    time_field: &str,
    query: &str,
    t_repeat_ms: u64,
    data_ingestion_interval_ms: u64,
) -> PlannerOutput {
    let yaml = elastic_yaml(index, time_field, query, t_repeat_ms);
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();

    let opts = ElasticRuntimeOptions {
        streaming_engine: StreamingEngine::Arroyo,
        data_ingestion_interval_ms,
    };

    ElasticController::from_file(Path::new(file.path()), opts)
        .unwrap()
        .generate()
        .unwrap()
}

fn assert_index_schema(
    out: &PlannerOutput,
    index: &str,
    metric_column: &str,
    metadata_columns: &[&str],
) {
    let inference_config = out
        .to_inference_config(QueryLanguage::elastic_querydsl)
        .unwrap();

    match inference_config.schema {
        SchemaConfig::ElasticQueryDSL(schema) => {
            assert_eq!(
                schema.get_time_field(index),
                Some(&"@timestamp".to_string())
            );

            let metric_columns = schema.get_metric_columns(index).unwrap();
            assert!(metric_columns.contains(metric_column));

            let actual_metadata = schema.get_metadata_columns(index).unwrap();
            for column in metadata_columns {
                assert!(actual_metadata.contains(*column));
            }
        }
        other => panic!("expected elastic querydsl schema, got {:?}", other),
    }
}

#[test]
fn elastic_querydsl_emits_index_schema() {
    let opts = ElasticRuntimeOptions {
        streaming_engine: StreamingEngine::Arroyo,
        data_ingestion_interval_ms: 15_000,
    };
    let c = ElasticController::from_file(Path::new("tests/elastic_example.yaml"), opts).unwrap();
    let out = c.generate().unwrap();
    let inference_config = out
        .to_inference_config(QueryLanguage::elastic_querydsl)
        .unwrap();

    match inference_config.schema {
        SchemaConfig::ElasticQueryDSL(schema) => {
            assert_eq!(
                schema.get_time_field("metrics"),
                Some(&"@timestamp".to_string())
            );
            let metric_columns = schema.get_metric_columns("metrics").unwrap();
            assert!(metric_columns.contains("cpu_usage"));
            let metadata_columns = schema.get_metadata_columns("metrics").unwrap();
            assert!(metadata_columns.is_empty());
        }
        other => panic!("expected elastic querydsl schema, got {:?}", other),
    }
}

#[test]
fn elastic_sum_produces_basic_plan_and_schema() {
    let query = r#"
{
    "aggs": {
        "by_datacenter": {
            "terms": {
                "field": "datacenter.keyword"
            },
            "aggs": {
                "sum_cpu": {
                    "sum": {
                        "field": "cpu_usage"
                    }
                }
            }
        }
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": "now-5m",
                            "lte": "now"
                        }
                    }
                }
            ]
        }
    }
}
"#;
    let out = elastic_output("metrics", "\"@timestamp\"", query, 300_000);

    assert_eq!(out.streaming_aggregation_count(), 2);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("CountMinSketch"));
    assert!(out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300_000));
    assert_eq!(
        out.aggregation_table_name("CountMinSketch"),
        Some("metrics".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("CountMinSketch"),
        Some("cpu_usage".to_string())
    );
    assert_index_schema(&out, "metrics", "cpu_usage", &["datacenter"]);
}

#[test]
fn elastic_avg_produces_three_configs() {
    let query = r#"
{
    "aggs": {
        "by_datacenter": {
            "terms": {
                "field": "datacenter.keyword"
            },
            "aggs": {
                "avg_cpu": {
                    "avg": {
                        "field": "cpu_usage"
                    }
                }
            }
        }
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": "now-5m",
                            "lte": "now"
                        }
                    }
                }
            ]
        }
    }
}
"#;
    let out = elastic_output("metrics", "\"@timestamp\"", query, 300_000);

    assert_eq!(out.streaming_aggregation_count(), 2);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("MultipleSum"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300_000));
    assert_eq!(
        out.aggregation_table_name("MultipleSum"),
        Some("metrics".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("MultipleSum"),
        Some("cpu_usage".to_string())
    );

    assert_index_schema(&out, "metrics", "cpu_usage", &["datacenter"]);
}

#[test]
fn elastic_min_produces_exact_plan() {
    let query = r#"
{
    "aggs": {
        "by_service": {
            "terms": {
                "field": "service.keyword"
            },
            "aggs": {
                "min_cpu": {
                    "min": {
                        "field": "cpu_usage"
                    }
                }
            }
        }
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": "now-5m",
                            "lte": "now"
                        }
                    }
                }
            ]
        }
    }
}
"#;
    let out = elastic_output("metrics", "\"@timestamp\"", query, 300_000);

    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("MultipleMinMax"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300_000));
    assert_eq!(
        out.aggregation_table_name("MultipleMinMax"),
        Some("metrics".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("MultipleMinMax"),
        Some("cpu_usage".to_string())
    );
    assert_index_schema(&out, "metrics", "cpu_usage", &["service"]);
}

#[test]
fn elastic_percentiles_produce_kll_plan() {
    let query = r#"
{
    "aggs": {
        "by_service": {
            "terms": {
                "field": "service.keyword"
            },
            "aggs": {
                "latency_percentiles": {
                    "percentiles": {
                        "field": "latency_ms",
                        "percents": [50.0, 95.0]
                    }
                }
            }
        }
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": "now-5m",
                            "lte": "now"
                        }
                    }
                }
            ]
        }
    }
}
"#;
    let out = elastic_output("metrics", "\"@timestamp\"", query, 300_000);

    assert_eq!(out.streaming_aggregation_count(), 1);
    assert_eq!(out.inference_query_count(), 1);
    assert!(out.has_aggregation_type("DatasketchesKLL"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300_000));
    assert_eq!(
        out.aggregation_table_name("DatasketchesKLL"),
        Some("metrics".to_string())
    );
    assert_eq!(
        out.aggregation_value_column("DatasketchesKLL"),
        Some("latency_ms".to_string())
    );
    assert_index_schema(&out, "metrics", "latency_ms", &["service"]);
}

#[test]
fn elastic_multi_index_schema_inference() {
    let yaml = r#"
query_groups:
  - id: 1
    index: metrics
    time_field: "@timestamp"
    queries:
            - |
                {
                    "aggs": {
                        "by_datacenter": {
                            "terms": {
                                "field": "datacenter.keyword"
                            },
                            "aggs": {
                                "avg_cpu": {
                                    "avg": {
                                        "field": "cpu_usage"
                                    }
                                }
                            }
                        }
                    },
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": "now-5m",
                                            "lte": "now"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
    repetition_delay_ms: 300000
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 1.0
  - id: 2
    index: other_metrics
    time_field: "timestamp"
    queries:
            - |
                {
                    "aggs": {
                        "by_service": {
                            "terms": {
                                "field": "service.keyword"
                            },
                            "aggs": {
                                "avg_mem": {
                                    "avg": {
                                        "field": "memory_usage"
                                    }
                                }
                            }
                        }
                    },
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "range": {
                                        "timestamp": {
                                            "gte": "now-5m",
                                            "lte": "now"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
    repetition_delay_ms: 300000
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 1.0
aggregate_cleanup:
  policy: read_based
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();

    let opts = ElasticRuntimeOptions {
        streaming_engine: StreamingEngine::Arroyo,
        data_ingestion_interval_ms: 15_000,
    };

    let c = ElasticController::from_file(Path::new(file.path()), opts).unwrap();
    let out = c.generate().unwrap();

    assert_eq!(out.streaming_aggregation_count(), 4);
    assert_eq!(out.inference_query_count(), 2);
    assert!(out.has_aggregation_type("MultipleSum"));
    assert!(!out.has_aggregation_type("DeltaSetAggregator"));
    assert!(out.all_tumbling_window_sizes_eq(300_000));

    let inference_config = out
        .to_inference_config(QueryLanguage::elastic_querydsl)
        .unwrap();

    match inference_config.schema {
        SchemaConfig::ElasticQueryDSL(schema) => {
            assert_eq!(
                schema.get_time_field("metrics"),
                Some(&"@timestamp".to_string())
            );
            let metric_columns = schema.get_metric_columns("metrics").unwrap();
            assert!(metric_columns.contains("cpu_usage"));
            let metadata_columns = schema.get_metadata_columns("metrics").unwrap();
            assert!(metadata_columns.contains("datacenter"));

            assert_eq!(
                schema.get_time_field("other_metrics"),
                Some(&"timestamp".to_string())
            );
            let other_metric_columns = schema.get_metric_columns("other_metrics").unwrap();
            assert!(other_metric_columns.contains("memory_usage"));
            let other_metadata_columns = schema.get_metadata_columns("other_metrics").unwrap();
            assert!(other_metadata_columns.contains("service"));
        }
        other => panic!("expected elastic querydsl schema, got {:?}", other),
    }
}

// ── sub-second precision ──────────────────────────────────────────────────────

/// repetition_delay_ms = 500 (sub-second): validates ms-precision plumbing end-to-end.
#[test]
fn sub_second_repetition_delay_ms() {
    let query = r#"
{
    "aggs": {
        "avg_cpu": {
            "avg": {
                "field": "cpu_usage"
            }
        }
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "gte": "now-5m",
                            "lte": "now"
                        }
                    }
                }
            ]
        }
    }
}"#;
    let out = elastic_output_with_interval("metrics", "\"@timestamp\"", query, 500, 500);
    assert_eq!(out.streaming_aggregation_count(), 2);
    assert!(out.all_tumbling_window_sizes_eq(500));
}

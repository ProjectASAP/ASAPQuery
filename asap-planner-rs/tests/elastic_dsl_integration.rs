use asap_planner::{
    ElasticController, ElasticRuntimeOptions, StreamingEngine,
};
use asap_types::{QueryLanguage, SchemaConfig};
use std::path::Path;


#[test]
fn elastic_querydsl_emits_index_schema() {
    let opts = ElasticRuntimeOptions {
        streaming_engine: StreamingEngine::Arroyo,
        data_ingestion_interval: 15,
    };
    let c = ElasticController::from_file(Path::new("tests/elastic_example.yaml"), opts).unwrap();
    let out = c.generate().unwrap();
    let inference_config = out
        .to_inference_config(QueryLanguage::elastic_querydsl)
        .unwrap();

    match inference_config.schema {
        SchemaConfig::ElasticQueryDSL(schema) => {
            assert_eq!(schema.get_time_field("metrics"), Some(&"@timestamp".to_string()));
            let metric_columns = schema.get_metric_columns("metrics").unwrap();
            assert!(metric_columns.contains("cpu_usage"));
            let metadata_columns = schema.get_metadata_columns("metrics").unwrap();
            assert!(metadata_columns.is_empty());
        }
        other => panic!("expected elastic querydsl schema, got {:?}", other),
    }
}
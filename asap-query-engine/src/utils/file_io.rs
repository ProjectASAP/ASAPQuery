use crate::data_model::{InferenceConfig, QueryLanguage, StreamingConfig};
// use crate::stores::promsketch_store::config::PromSketchConfig;
use anyhow::{Context, Result};

/// Read inference configuration from a YAML file
pub fn read_inference_config(
    yaml_file: &str,
    query_language: QueryLanguage,
) -> Result<InferenceConfig> {
    let config = InferenceConfig::from_yaml_file(yaml_file, query_language)
        .with_context(|| format!("Failed to parse YAML config from: {yaml_file}"))?;

    Ok(config)
}

// /// Read sketch configuration from a YAML file
// pub fn read_promsketch_config(yaml_file: &str) -> Result<PromSketchConfig> {
//     PromSketchConfig::from_yaml_file(yaml_file)
//         .with_context(|| format!("Failed to load sketch config from: {yaml_file}"))
// }

pub fn read_streaming_config(
    yaml_file: &str,
    inference_config: &InferenceConfig,
) -> Result<StreamingConfig> {
    let yaml_data = std::fs::read_to_string(yaml_file)
        .with_context(|| format!("Failed to read YAML file: {yaml_file}"))?;
    let yaml_data: serde_yaml::Value = serde_yaml::from_str(&yaml_data)
        .with_context(|| format!("Failed to parse YAML file: {yaml_file}"))?;

    let config = StreamingConfig::from_yaml_data(&yaml_data, Some(inference_config))
        .with_context(|| format!("Failed to parse YAML config from: {yaml_file}"))?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::QueryLanguage;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// The PromQL compliance roll-up suite ships hand-written planner output;
    /// parse it the way the engine does so it can't drift from the format.
    #[test]
    fn compliance_rollup_planner_output_loads_as_engine_config() {
        let directory = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../promql-compliance/suites/rollup-planner-output"
        );
        let inference = read_inference_config(
            &format!("{directory}/inference_config.yaml"),
            QueryLanguage::promql,
        )
        .unwrap();
        let streaming =
            read_streaming_config(&format!("{directory}/streaming_config.yaml"), &inference)
                .unwrap();

        assert!(!inference.query_configs.is_empty());
        for query_config in &inference.query_configs {
            assert!(
                !query_config.aggregations.is_empty(),
                "{}",
                query_config.query
            );
            for reference in &query_config.aggregations {
                assert!(
                    streaming
                        .get_aggregation_config(reference.aggregation_id)
                        .is_some(),
                    "{} names missing aggregation {}",
                    query_config.query,
                    reference.aggregation_id
                );
            }
        }
    }

    #[test]
    fn test_read_streaming_config() {
        let streaming_yaml_content = r#"
aggregations:
- aggregationId: 1
  aggregationSubType: ''
  aggregationType: DatasketchesKLL
  labels:
    aggregated: []
    grouping:
    - instance
    - job
    - label_0
    - label_1
    - label_2
    rollup: []

  metric: fake_metric_total
  parameters:
    K: 200
  spatialFilter: ''
  windowSizeMs: 10000
metrics:
  fake_metric_total:
  - instance
  - job
  - label_0
  - label_1
  - label_2
"#;

        let inference_yaml_content = r#"
cleanup_policy:
  name: "circular_buffer"
metrics:
  fake_metric_total:
  - instance
  - job
  - label_0
  - label_1
  - label_2
queries:
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.5, fake_metric_total[1m])
  planned_subquery: quantile_over_time(0.5, fake_metric_total[1m])
  query_time_aggregations: []
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.95, fake_metric_total[1m])
  planned_subquery: quantile_over_time(0.95, fake_metric_total[1m])
  query_time_aggregations: []
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.99, fake_metric_total[1m])
  planned_subquery: quantile_over_time(0.99, fake_metric_total[1m])
  query_time_aggregations: []
        "#;

        let mut inference_temp_file = NamedTempFile::new().unwrap();
        write!(inference_temp_file, "{inference_yaml_content}").unwrap();
        let inference_config = read_inference_config(
            inference_temp_file.path().to_str().unwrap(),
            QueryLanguage::promql,
        )
        .unwrap();
        assert!(!inference_config.query_configs.is_empty());

        let mut streaming_temp_file = NamedTempFile::new().unwrap();
        write!(streaming_temp_file, "{streaming_yaml_content}").unwrap();

        let config = read_streaming_config(
            streaming_temp_file.path().to_str().unwrap(),
            &inference_config,
        )
        .unwrap();
        assert!(!config.aggregation_configs.is_empty());
    }

    #[test]
    //     fn test_read_promsketch_config() {
    //         let yaml_content = r#"
    // eh_univ:
    //   k: 75
    //   time_window: 500000
    // eh_kll:
    //   k: 60
    //   kll_k: 128
    //   time_window: 750000
    // sampling:
    //   sample_rate: 0.3
    //   time_window: 900000
    // "#;
    //         let mut temp_file = NamedTempFile::new().unwrap();
    //         write!(temp_file, "{yaml_content}").unwrap();

    //         let config = read_promsketch_config(temp_file.path().to_str().unwrap()).unwrap();
    //         assert_eq!(config.eh_univ.k, 75);
    //         assert_eq!(config.eh_univ.time_window, 500_000);
    //         assert_eq!(config.eh_kll.k, 60);
    //         assert_eq!(config.eh_kll.kll_k, 128);
    //         assert!((config.sampling.sample_rate - 0.3).abs() < f64::EPSILON);
    //     }
    // #[test]

    fn test_read_inference_config() {
        let yaml_content = r#"
cleanup_policy:
  name: "circular_buffer"
metrics:
  fake_metric_total:
  - instance
  - job
  - label_0
  - label_1
  - label_2
queries:
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.5, fake_metric_total[1m])
  planned_subquery: quantile_over_time(0.5, fake_metric_total[1m])
  query_time_aggregations: []
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.95, fake_metric_total[1m])
  planned_subquery: quantile_over_time(0.95, fake_metric_total[1m])
  query_time_aggregations: []
- aggregations:
  - aggregation_id: 1
    num_aggregates_to_retain: 6
  query: quantile_over_time(0.99, fake_metric_total[1m])
  planned_subquery: quantile_over_time(0.99, fake_metric_total[1m])
  query_time_aggregations: []
        "#;

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, "{yaml_content}").unwrap();

        let config =
            read_inference_config(temp_file.path().to_str().unwrap(), QueryLanguage::promql)
                .unwrap();
        assert!(!config.query_configs.is_empty());
    }
}

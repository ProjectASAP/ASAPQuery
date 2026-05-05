use asap_types::enums::QueryLanguage;
use query_engine_rust::data_model::enums::{InputFormat, LockStrategy, StreamingEngine};

pub fn check_config(config: &EngineConfig) -> Result<(), String> {
    match (&config.ingest, &config.streaming_engine) {
        (IngestConfig::Kafka { .. }, StreamingEngine::Arroyo) => {}
        (
            IngestConfig::HttpRemoteWrite { .. } | IngestConfig::Csv { .. },
            StreamingEngine::Precompute,
        ) => {}
        (IngestConfig::Otlp { .. }, StreamingEngine::Arroyo) => {}
        (IngestConfig::Otlp { .. }, StreamingEngine::Precompute) => {
            return Err("ingest.type=otlp requires streaming_engine=arroyo (precompute engine does not apply to OTLP)".into());
        }
        (IngestConfig::Kafka { .. }, StreamingEngine::Precompute) => {
            return Err("ingest.type=kafka requires streaming_engine=arroyo".into());
        }
        (_, StreamingEngine::Arroyo) => {
            return Err("streaming_engine=arroyo requires ingest.type=kafka".into());
        }
    }

    if let IngestConfig::Csv {
        timestamp_col: None,
        ts_step_ms: None,
        ..
    } = &config.ingest
    {
        return Err("ingest.ts_step_ms is required when ingest.timestamp_col is not set".into());
    }

    if config.prometheus_scrape_interval == 0 {
        return Err("prometheus_scrape_interval must be greater than 0".into());
    }

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    pub output_dir: String,
    pub log_level: String,
    pub query_language: QueryLanguage,
    pub prometheus_scrape_interval: u64,
    pub streaming_engine: StreamingEngine,
    pub do_profiling: bool,
    pub http_server: HttpServerSettings,
    pub store: StoreSettings,
    pub ingest: IngestConfig,
    pub precompute_engine: PrecomputeSettings,
    pub query_tracker: QueryTrackerSettings,
    pub inference_config: Option<String>,
    pub streaming_config: Option<String>,
    pub promsketch_config: Option<String>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            output_dir: "./output".to_string(),
            log_level: "INFO".to_string(),
            query_language: QueryLanguage::promql,
            prometheus_scrape_interval: 15,
            streaming_engine: StreamingEngine::Precompute,
            do_profiling: false,
            http_server: HttpServerSettings::default(),
            store: StoreSettings::default(),
            ingest: IngestConfig::default(),
            precompute_engine: PrecomputeSettings::default(),
            query_tracker: QueryTrackerSettings::default(),
            inference_config: None,
            streaming_config: None,
            promsketch_config: None,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
pub struct HttpServerSettings {
    pub port: u16,
    pub prometheus_server: String,
    pub forward_unsupported_queries: bool,
}

impl Default for HttpServerSettings {
    fn default() -> Self {
        Self {
            port: 8088,
            prometheus_server: "http://localhost:9090".to_string(),
            forward_unsupported_queries: false,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
pub struct StoreSettings {
    pub lock_strategy: LockStrategy,
}

impl Default for StoreSettings {
    fn default() -> Self {
        Self {
            lock_strategy: LockStrategy::PerKey,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IngestConfig {
    HttpRemoteWrite {
        #[serde(default = "default_http_remote_write_port")]
        port: u16,
    },
    Kafka {
        #[serde(default = "default_kafka_broker")]
        broker: String,
        topic: String,
        input_format: InputFormat,
        #[serde(default)]
        decompress_json: bool,
    },
    Csv {
        path: String,
        metric_name: String,
        value_col: String,
        #[serde(default)]
        label_cols: Vec<String>,
        timestamp_col: Option<String>,
        #[serde(default)]
        start_ts_ms: i64,
        ts_step_ms: Option<i64>,
        #[serde(default = "default_csv_batch_size")]
        batch_size: usize,
    },
    Otlp {
        #[serde(default = "default_otlp_grpc_port")]
        grpc_port: u16,
        #[serde(default = "default_otlp_http_port")]
        http_port: u16,
    },
}

impl Default for IngestConfig {
    fn default() -> Self {
        IngestConfig::HttpRemoteWrite {
            port: default_http_remote_write_port(),
        }
    }
}

fn default_http_remote_write_port() -> u16 {
    9090
}

fn default_kafka_broker() -> String {
    "localhost:9092".to_string()
}

fn default_csv_batch_size() -> usize {
    1000
}

fn default_otlp_grpc_port() -> u16 {
    4317
}

fn default_otlp_http_port() -> u16 {
    4318
}

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
pub struct PrecomputeSettings {
    pub num_workers: usize,
    pub allowed_lateness_ms: i64,
    pub max_buffer_per_series: usize,
    pub flush_interval_ms: u64,
    pub channel_buffer_size: usize,
    pub dump_precomputes: bool,
}

impl Default for PrecomputeSettings {
    fn default() -> Self {
        Self {
            num_workers: 4,
            allowed_lateness_ms: 5000,
            max_buffer_per_series: 10000,
            flush_interval_ms: 1000,
            channel_buffer_size: 10000,
            dump_precomputes: false,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
pub struct QueryTrackerSettings {
    pub enabled: bool,
    pub observation_window_secs: u64,
}

impl Default for QueryTrackerSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            observation_window_secs: 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::{
        providers::{Format, Yaml},
        Figment,
    };

    const MINIMAL_YAML: &str = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
"#;

    // Verifies that figment's tuple provider ("a.b", val) resolves dotted paths
    // into nested structs rather than treating the key as a flat string.
    #[test]
    fn dotted_key_override_sets_nested_field() {
        let config: EngineConfig = Figment::new()
            .merge(Yaml::string(MINIMAL_YAML))
            .merge(("precompute_engine.num_workers", 8usize))
            .merge(("http_server.port", 9000u16))
            .extract()
            .expect("config should deserialize");

        assert_eq!(config.precompute_engine.num_workers, 8);
        assert_eq!(config.http_server.port, 9000);
    }

    #[test]
    fn check_config_valid_precompute_http() {
        let config: EngineConfig = Figment::new()
            .merge(Yaml::string(MINIMAL_YAML))
            .extract()
            .unwrap();
        assert!(check_config(&config).is_ok());
    }

    #[test]
    fn check_config_valid_kafka_arroyo() {
        let yaml = r#"
streaming_engine: "arroyo"
ingest:
  type: "kafka"
  topic: "my-topic"
  input_format: "json"
output_dir: "./output"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_ok());
    }

    #[test]
    fn check_config_rejects_kafka_with_precompute() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "kafka"
  topic: "t"
  input_format: "json"
output_dir: "./output"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_err());
    }

    #[test]
    fn check_config_rejects_http_with_arroyo() {
        let yaml = r#"
streaming_engine: "arroyo"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_err());
    }

    #[test]
    fn check_config_rejects_otlp_with_precompute() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "otlp"
output_dir: "./output"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_err());
    }

    #[test]
    fn check_config_rejects_csv_without_ts_step_ms() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "csv"
  path: "data.csv"
  metric_name: "m"
  value_col: "v"
output_dir: "./output"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_err());
    }

    #[test]
    fn check_config_csv_with_timestamp_col_does_not_require_ts_step_ms() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "csv"
  path: "data.csv"
  metric_name: "m"
  value_col: "v"
  timestamp_col: "ts"
output_dir: "./output"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_ok());
    }

    #[test]
    fn check_config_valid_otlp_arroyo() {
        let yaml = r#"
streaming_engine: "arroyo"
ingest:
  type: "otlp"
output_dir: "./output"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_ok());
    }

    #[test]
    fn enum_override_sets_lock_strategy() {
        let config: EngineConfig = Figment::new()
            .merge(Yaml::string(MINIMAL_YAML))
            .merge(("store.lock_strategy", "global"))
            .extract()
            .expect("config should deserialize");
        assert_eq!(
            config.store.lock_strategy,
            query_engine_rust::data_model::enums::LockStrategy::Global
        );
    }

    #[test]
    fn check_config_rejects_zero_scrape_interval() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
prometheus_scrape_interval: 0
output_dir: "./output"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_err());
    }
}

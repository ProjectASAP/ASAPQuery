use asap_types::enums::QueryLanguage;
use query_engine_rust::data_model::enums::{InputFormat, LockStrategy, StreamingEngine};

pub fn check_config(config: &EngineConfig) -> Result<(), String> {
    match (&config.ingest, &config.streaming_engine) {
        (IngestConfig::Kafka { .. }, StreamingEngine::Arroyo) => {}
        (
            IngestConfig::HttpRemoteWrite { .. }
            | IngestConfig::Csv { .. }
            | IngestConfig::Json { .. },
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

    if config.query_tracker.enabled && !matches!(config.backend, BackendConfig::Prometheus { .. }) {
        return Err("query_tracker.enabled=true requires backend.type=prometheus".into());
    }

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    pub output_dir: String,
    pub log_level: String,
    pub prometheus_scrape_interval: u64,
    pub streaming_engine: StreamingEngine,
    pub http_server: HttpServerSettings,
    pub backend: BackendConfig,
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
            prometheus_scrape_interval: 15,
            streaming_engine: StreamingEngine::Precompute,
            http_server: HttpServerSettings::default(),
            backend: BackendConfig::default(),
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
}

impl Default for HttpServerSettings {
    fn default() -> Self {
        Self { port: 8088 }
    }
}

/// Which DB backend the query server exposes and optionally forwards to.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendConfig {
    Prometheus {
        /// Prometheus server URL used for query forwarding and planner context.
        #[serde(default = "default_prometheus_server")]
        server: String,
        /// When true, queries not answerable from sketches are forwarded to `server`.
        /// The server must be reachable at startup.
        #[serde(default)]
        forward_unsupported_queries: bool,
        /// HTTP timeout in seconds for forwarded queries. Increase for long-range Thanos queries.
        #[serde(default = "default_fallback_timeout_secs")]
        fallback_timeout_secs: u64,
    },
    Clickhouse {
        /// ClickHouse HTTP interface base URL.
        #[serde(default = "default_clickhouse_url")]
        url: String,
        /// ClickHouse database name.
        #[serde(default = "default_clickhouse_database")]
        database: String,
        /// When true, queries not answerable from sketches are forwarded to `url`.
        #[serde(default)]
        forward_unsupported_queries: bool,
    },
    ElasticQuerydsl {
        /// Elasticsearch base URL.
        #[serde(default = "default_elastic_url")]
        url: String,
        /// Elasticsearch index pattern to query.
        index: String,
        /// When true, queries not answerable from sketches are forwarded to `url`.
        #[serde(default)]
        forward_unsupported_queries: bool,
    },
    ElasticSql {
        /// Elasticsearch base URL.
        #[serde(default = "default_elastic_url")]
        url: String,
        /// Elasticsearch index pattern to query.
        index: String,
        /// When true, queries not answerable from sketches are forwarded to `url`.
        #[serde(default)]
        forward_unsupported_queries: bool,
    },
}

impl Default for BackendConfig {
    fn default() -> Self {
        BackendConfig::Prometheus {
            server: default_prometheus_server(),
            forward_unsupported_queries: false,
            fallback_timeout_secs: default_fallback_timeout_secs(),
        }
    }
}

impl BackendConfig {
    pub fn query_language(&self) -> QueryLanguage {
        match self {
            BackendConfig::Prometheus { .. } => QueryLanguage::promql,
            BackendConfig::Clickhouse { .. } => QueryLanguage::sql,
            BackendConfig::ElasticQuerydsl { .. } => QueryLanguage::elastic_querydsl,
            BackendConfig::ElasticSql { .. } => QueryLanguage::elastic_sql,
        }
    }

    pub fn forward_unsupported_queries(&self) -> bool {
        match self {
            BackendConfig::Prometheus {
                forward_unsupported_queries,
                ..
            }
            | BackendConfig::Clickhouse {
                forward_unsupported_queries,
                ..
            }
            | BackendConfig::ElasticQuerydsl {
                forward_unsupported_queries,
                ..
            }
            | BackendConfig::ElasticSql {
                forward_unsupported_queries,
                ..
            } => *forward_unsupported_queries,
        }
    }
}

fn default_prometheus_server() -> String {
    "http://localhost:9090".to_string()
}

fn default_fallback_timeout_secs() -> u64 {
    30
}

fn default_clickhouse_url() -> String {
    "http://localhost:8123".to_string()
}

fn default_clickhouse_database() -> String {
    "default".to_string()
}

fn default_elastic_url() -> String {
    "http://localhost:9200".to_string()
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
    Json {
        path: String,
        metric_name: String,
        value_col: String,
        #[serde(default)]
        label_cols: Vec<String>,
        timestamp_col: String,
        #[serde(default = "default_timestamp_unit")]
        timestamp_unit: String,
        #[serde(default = "default_json_batch_size")]
        batch_size: usize,
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

fn default_timestamp_unit() -> String {
    "seconds".to_string()
}

fn default_json_batch_size() -> usize {
    1000
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
    /// Wall-clock grace period (ms) for the flush fallback that force-closes
    /// idle windows when event-time stagnates (e.g. one-shot batch ingest
    /// where every record shares a timestamp). Set to <= 0 to disable and
    /// keep strict event-time-only semantics. See
    /// `PrecomputeEngineConfig::wall_clock_grace_period_ms`.
    pub wall_clock_grace_period_ms: i64,
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
            wall_clock_grace_period_ms: 5000,
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
    fn check_config_valid_json_precompute() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "json"
  path: "hits.json"
  metric_name: "hits"
  value_col: "ResolutionWidth"
  label_cols: ["OS", "RegionID"]
  timestamp_col: "EventTime"
  timestamp_unit: "seconds"
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

    #[test]
    fn backend_defaults_to_prometheus() {
        let config: EngineConfig = Figment::new()
            .merge(Yaml::string(MINIMAL_YAML))
            .extract()
            .unwrap();
        assert!(matches!(config.backend, BackendConfig::Prometheus { .. }));
        assert_eq!(config.backend.query_language(), QueryLanguage::promql);
        assert!(!config.backend.forward_unsupported_queries());
    }

    #[test]
    fn backend_clickhouse_parses() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
backend:
  type: "clickhouse"
  url: "http://clickhouse:8123"
  database: "metrics"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(matches!(config.backend, BackendConfig::Clickhouse { .. }));
        assert_eq!(config.backend.query_language(), QueryLanguage::sql);
        assert!(!config.backend.forward_unsupported_queries());
    }

    #[test]
    fn backend_clickhouse_defaults_url_and_database() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
backend:
  type: "clickhouse"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        if let BackendConfig::Clickhouse { url, database, .. } = &config.backend {
            assert_eq!(url, "http://localhost:8123");
            assert_eq!(database, "default");
        } else {
            panic!("expected Clickhouse backend");
        }
    }

    #[test]
    fn backend_elastic_querydsl_parses() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
backend:
  type: "elastic_querydsl"
  url: "http://elastic:9200"
  index: "metrics-*"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(matches!(
            config.backend,
            BackendConfig::ElasticQuerydsl { .. }
        ));
        assert_eq!(
            config.backend.query_language(),
            QueryLanguage::elastic_querydsl
        );
    }

    #[test]
    fn backend_elastic_sql_parses() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
backend:
  type: "elastic_sql"
  url: "http://elastic:9200"
  index: "metrics-*"
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(matches!(config.backend, BackendConfig::ElasticSql { .. }));
        assert_eq!(config.backend.query_language(), QueryLanguage::elastic_sql);
    }

    #[test]
    fn backend_prometheus_explicit_fields() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
backend:
  type: "prometheus"
  server: "http://prom:9090"
  forward_unsupported_queries: true
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        if let BackendConfig::Prometheus {
            server,
            forward_unsupported_queries,
            ..
        } = &config.backend
        {
            assert_eq!(server, "http://prom:9090");
            assert!(forward_unsupported_queries);
        } else {
            panic!("expected Prometheus backend");
        }
        assert!(config.backend.forward_unsupported_queries());
    }

    #[test]
    fn check_config_rejects_query_tracker_with_non_prometheus_backend() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
backend:
  type: "clickhouse"
query_tracker:
  enabled: true
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_err());
    }

    #[test]
    fn check_config_allows_query_tracker_with_prometheus_backend() {
        let yaml = r#"
streaming_engine: "precompute"
ingest:
  type: "http_remote_write"
  port: 9090
output_dir: "./output"
backend:
  type: "prometheus"
query_tracker:
  enabled: true
"#;
        let config: EngineConfig = Figment::new().merge(Yaml::string(yaml)).extract().unwrap();
        assert!(check_config(&config).is_ok());
    }
}

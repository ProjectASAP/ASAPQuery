use asap_planner::{Controller, RuntimeOptions, SQLController, SQLRuntimeOptions, StreamingEngine};
use clap::Parser;
use sketch_db_common::enums::QueryLanguage;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "asap-planner", about = "ASAP Query Planner")]
struct Args {
    /// Path to a hand-authored YAML workload config. Mutually exclusive with --query-log.
    #[arg(long = "input_config", conflicts_with = "query_log")]
    input_config: Option<PathBuf>,

    /// Path to a Prometheus query log file (newline-delimited JSON). Mutually exclusive with --input_config.
    #[arg(long = "query-log", conflicts_with = "input_config")]
    query_log: Option<PathBuf>,

    #[arg(long = "output_dir")]
    output_dir: PathBuf,

    #[arg(long = "prometheus_scrape_interval", required = false)]
    prometheus_scrape_interval: Option<u64>,

    /// Base URL of the Prometheus instance used to auto-infer metric label sets.
    /// Optional: when provided, the planner queries Prometheus for label discovery.
    /// When absent, labels are taken from the `metrics` hint in the config file.
    /// Example: http://localhost:9090
    #[arg(long = "prometheus-url", required = false)]
    prometheus_url: Option<String>,

    #[arg(long = "streaming_engine", value_enum)]
    streaming_engine: EngineArg,

    #[arg(long = "enable-punting", default_value = "false")]
    enable_punting: bool,

    #[arg(long = "range-duration", default_value = "0")]
    range_duration: u64,

    #[arg(long = "step", default_value = "0")]
    step: u64,

    #[arg(long = "query-language", value_enum, default_value = "promql")]
    query_language: QueryLanguage,

    #[arg(long = "data-ingestion-interval", required = false)]
    data_ingestion_interval: Option<u64>,

    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum EngineArg {
    Arroyo,
    Flink,
    Precompute,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(if args.verbose > 0 {
            tracing::Level::DEBUG
        } else {
            tracing::Level::WARN
        })
        .init();

    let engine = match args.streaming_engine {
        EngineArg::Arroyo => StreamingEngine::Arroyo,
        EngineArg::Flink => StreamingEngine::Flink,
        EngineArg::Precompute => StreamingEngine::Precompute,
    };

    match args.query_language {
        QueryLanguage::promql => {
            let scrape_interval = args.prometheus_scrape_interval.ok_or_else(|| {
                anyhow::anyhow!("--prometheus_scrape_interval is required for PromQL mode")
            })?;
            let opts = RuntimeOptions {
                prometheus_scrape_interval: scrape_interval,
                streaming_engine: engine,
                enable_punting: args.enable_punting,
                range_duration: args.range_duration,
                step: args.step,
            };
            let controller = match (args.input_config, args.query_log, args.prometheus_url) {
                (Some(config_path), None, Some(url)) => {
                    Controller::from_file(&config_path, opts, &url)?
                }
                (Some(config_path), None, None) => {
                    let yaml_str = std::fs::read_to_string(&config_path)?;
                    let config: asap_planner::ControllerConfig = serde_yaml::from_str(&yaml_str)?;
                    let schema = config.schema_from_hints();
                    Controller::from_file_with_schema(&config_path, schema, opts)?
                }
                (None, Some(log_path), Some(url)) => {
                    Controller::from_query_log(&log_path, opts, &url)?
                }
                (None, None, Some(url)) => Controller::from_prometheus(&url, opts)?,
                (None, Some(_log_path), None) => {
                    anyhow::bail!(
                        "--prometheus-url is required when using --query-log \
                         (query logs have no metrics hint to fall back on)"
                    )
                }
                (None, None, None) => {
                    anyhow::bail!("provide one of --input_config, --query-log, or --prometheus-url")
                }
                _ => unreachable!("clap conflicts_with prevents this combination"),
            };
            controller.generate_to_dir(&args.output_dir)?;
        }
        QueryLanguage::sql | QueryLanguage::elastic_sql => {
            let interval = args.data_ingestion_interval.ok_or_else(|| {
                anyhow::anyhow!("--data-ingestion-interval is required for SQL mode")
            })?;
            let config_path = args
                .input_config
                .ok_or_else(|| anyhow::anyhow!("--input_config is required for SQL mode"))?;
            let opts = SQLRuntimeOptions {
                streaming_engine: engine,
                query_evaluation_time: None,
                data_ingestion_interval: interval,
            };
            SQLController::from_file(&config_path, opts)?.generate_to_dir(&args.output_dir)?;
        }
        QueryLanguage::elastic_querydsl => {
            anyhow::bail!("ElasticQueryDSL is not yet supported");
        }
    }

    println!("Generated configs in {}", args.output_dir.display());
    Ok(())
}

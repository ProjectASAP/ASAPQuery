/// @NOTE: As new label-value combinations are added to each metric,
/// they will persist unless another metric with the same label-value combo
/// overwipes it. Therefore, user should be wary about the possibility
/// of program memory usage steadily increasing over the course of the runtime
use crate::alibaba_metrics::*;
use crate::google_metrics::*;
use crate::utilities::*;
use clap::Parser;
use hyper::body::Incoming;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper::Response;
use hyper_util::rt::TokioIo;
use prometheus::{Encoder, TextEncoder};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::OnceLock;
use std::{panic, process, thread};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod alibaba_metrics;
mod google_metrics;
mod utilities;

type BoxedErr = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Google or Alibaba. Must be initialized before starting export routine
static DATA_PROVIDER: OnceLock<Provider> = OnceLock::new();

/// @brief Async call-back function for servicing http requests, like
///        prometheus scrapes
///
/// @param[in] _req The incoming http request
///
/// @return Prometheus metrics on success
///         BoxedErr on failure
async fn serve_req(_req: Request<Incoming>) -> Result<Response<String>, BoxedErr> {
    let encoder = TextEncoder::new();
    let provider = DATA_PROVIDER.get().unwrap();

    match provider {
        Provider::Google => google_metrics::export_from_queue(),
        Provider::Alibaba => alibaba_metrics::export_from_queue(),
    }

    let metric_families = prometheus::gather();
    let body = encoder.encode_to_string(&metric_families)?;
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(body)?;

    Ok(response)
}

/// @brief Starts a thread to read and queue Google cluster data
///
/// @param[in] input_dir  The input directory to Google task resource usage
///                       cluster data
/// @param[in] all_parts  Whether to run the exporter across all csv parts or
///                       not. This should be false if part index is not None
/// @param[in] part_index The part number, out of 500, of the csv file to use
///                       when exporting task resource usage data. This should
///                       be None if all_parts is true.
/// @param[in] metrics    The list of metrics from the task resource usage data
///                       to export
///
/// @post All globals required by the main exporter thread are initialized.
fn start_google_thread(
    input_dir: String,
    all_parts: bool,
    part_index: Option<u16>,
    metrics: Vec<TruMetrics>,
) {
    debug!("Starting Google reader thread");
    thread::spawn(move || {
        // start reader thread
        // Drops thread handle => thread is implicitly detached
        if let Err(err) =
            google_metrics::reader_thread_routine(input_dir, all_parts, part_index, metrics)
        {
            error!("Error in Google reader thread: {:?}", err);
            process::exit(1);
        }
    });
    // Must be initialized before main thread starts exporting
    google_metrics::GOOGLE_METRICS.wait();
    debug!("Google reader thread initialized");
}

/// @brief Starts a thread to read and queue Alibaba cluster data
///
/// @param[in] input_dir  The input directory containing the csv files for
///                       reading
/// @param[in] all_parts  Whether to run the exporter from part 0 until no more
///                       csv files are found, or not. This should be false if
///                       part index is not None.
/// @param[in] part_index Which csv file part to use as the data source.
///                       This should be None if all_parts is true.
/// @param[in] data_type  Which type of microservice data the reading thread
///                       should be configured to read and queue
/// @param[in] data_year  The year from which the source data comes from. Valid
///                       options are 2021 and 2022
/// @param[in] speedup    Speedup factor for faster-than-realtime export
///
/// @post All globals required by the main exporter thread are initialized.
fn start_alibaba_thread(
    input_dir: String,
    all_parts: bool,
    part_index: Option<u16>,
    data_type: MsDataType,
    data_year: u32,
    speedup: u64,
) {
    debug!("Starting Alibaba reader thread");
    thread::spawn(move || {
        if let Err(err) = alibaba_metrics::reader_thread_routine(
            input_dir, all_parts, part_index, data_type, data_year, speedup,
        ) {
            error!("Error in Alibaba reader thread: {:?}", err);
            process::exit(1);
        }
    });
    // Must be initialized before main thread starts exporting
    alibaba_metrics::EXPORTER_DATA_TYPE.wait();
    debug!("Alibaba reader thread initialized");
}

/// @brief Sets up logging with optional file output
///
/// @param[in] log_dir   Optional directory for log file output
/// @param[in] log_level Log level string (DEBUG, INFO, WARN, ERROR)
///
/// @return WorkerGuard if file logging is enabled, None otherwise.
///         The guard must be kept alive for the duration of the program.
fn setup_logging(
    log_dir: Option<&str>,
    log_level: &str,
) -> Result<Option<tracing_appender::non_blocking::WorkerGuard>, BoxedErr> {
    // Create env filter that respects RUST_LOG, with fallback to command line arg
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    if let Some(dir) = log_dir {
        // Log to file AND stdout
        std::fs::create_dir_all(dir)?;
        let file_appender =
            tracing_appender::rolling::never(dir, "cluster_data_exporter.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stdout)
                    .with_ansi(true),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false),
            )
            .init();

        info!(
            "Logging initialized with file output: {}/cluster_data_exporter.log",
            dir
        );
        Ok(Some(guard))
    } else {
        // Log to stdout only
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();

        info!("Logging initialized (stdout only)");
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxedErr> {
    let cli = Cli::parse();

    // Initialize logging (keep guard alive for lifetime of program)
    let _log_guard = setup_logging(cli.log_dir.as_deref(), &cli.log_level)?;

    info!("Starting cluster_data_exporter");
    info!("Input directory: {}", cli.input_directory);
    info!("Port: {}", cli.port);

    // This code forces the program to exit if a reader thread panics.
    // Comment it out if it's preferable for the main thread to remain
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and then exit the process
        orig_hook(panic_info);
        process::exit(1);
    }));

    let input_directory: String = cli.input_directory.clone();
    let port: u16 = cli.port;
    let addr: SocketAddr = (Ipv4Addr::UNSPECIFIED, port).into();

    let _ = utilities::T_START; // init t_start

    // Spin up reader thread to start queueing csv data
    match cli.provider {
        ProviderCmd::Google {
            metrics,
            all_parts,
            part_index,
        } => {
            info!("Provider: Google");
            info!("Metrics: {:?}", metrics);
            info!("Parts mode: {}", if all_parts { "all-parts" } else { "part-index" });
            if let Some(idx) = part_index {
                info!("Part index: {}", idx);
            }
            let _ = DATA_PROVIDER.set(Provider::Google);
            start_google_thread(input_directory, all_parts, part_index, metrics);
        }
        ProviderCmd::Alibaba {
            data_type,
            data_year,
            all_parts,
            part_index,
            speedup,
        } => {
            info!("Provider: Alibaba");
            info!("Data type: {:?}", data_type);
            info!("Data year: {}", data_year);
            info!("Parts mode: {}", if all_parts { "all-parts" } else { "part-index" });
            if let Some(idx) = part_index {
                info!("Part index: {}", idx);
            }
            info!("Speedup factor: {}x", speedup);
            let _ = DATA_PROVIDER.set(Provider::Alibaba);
            start_alibaba_thread(input_directory, all_parts, part_index, data_type, data_year, speedup);
        }
    }

    let listener = TcpListener::bind(addr).await?;
    info!("Server listening on http://{}", addr);

    loop {
        // Main exporter routine
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let service = service_fn(serve_req);
        if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
            error!("Server error: {:?}", err);
        };
    }
}

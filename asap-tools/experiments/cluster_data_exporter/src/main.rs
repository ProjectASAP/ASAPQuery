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
    thread::spawn(move || {
        // start reader thread
        // Drops thread handle => thread is implicitly detached
        if let Err(err) =
            google_metrics::reader_thread_routine(input_dir, all_parts, part_index, metrics)
        {
            eprintln!("error in google reader thread: {:?}", err);
        }
    });
    // Must be initialized before main thread starts exporting
    google_metrics::GOOGLE_METRICS.wait();
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
///
/// @post All globals required by the main exporter thread are initialized.
fn start_alibaba_thread(
    input_dir: String,
    all_parts: bool,
    part_index: Option<u16>,
    data_type: MsDataType,
    data_year: u32,
) {
    thread::spawn(move || {
        if let Err(err) = alibaba_metrics::reader_thread_routine(
            input_dir, all_parts, part_index, data_type, data_year,
        ) {
            eprintln!("error in alibaba reader thread: {:?}", err);
        }
    });
    // Must be initialized before main thread starts exporting
    alibaba_metrics::EXPORTER_DATA_TYPE.wait();
}

#[tokio::main]
async fn main() -> Result<(), BoxedErr> {
    let cli = Cli::parse();

    // @TODO Test this more thoroughly
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
    println!("Listening on http://{}", addr);

    let _ = utilities::T_START; // init t_start

    // Spin up reader thread to start queueing csv data
    match cli.provider {
        ProviderCmd::Google {
            metrics,
            all_parts,
            part_index,
        } => {
            let _ = DATA_PROVIDER.set(Provider::Google);
            start_google_thread(input_directory, all_parts, part_index, metrics);
        }
        ProviderCmd::Alibaba {
            data_type,
            data_year,
            all_parts,
            part_index,
        } => {
            let _ = DATA_PROVIDER.set(Provider::Alibaba);
            start_alibaba_thread(input_directory, all_parts, part_index, data_type, data_year);
        }
    }

    let listener = TcpListener::bind(addr).await?;

    loop {
        // Main exporter routine
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let service = service_fn(serve_req);
        if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
            eprintln!("server error: {:?}", err);
        };
    }
}

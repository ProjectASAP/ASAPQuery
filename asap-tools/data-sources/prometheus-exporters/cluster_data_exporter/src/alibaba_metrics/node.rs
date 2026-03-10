use crate::utilities;
use concurrent_queue::ConcurrentQueue;
use csv::{Reader, ReaderBuilder};
use flate2::read::GzDecoder;
use lazy_static::lazy_static;
use prometheus::{register_gauge_vec, GaugeVec};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::thread;
use std::time::Duration;
use tracing::{debug, info};

type BoxedErr = Box<dyn std::error::Error + Send + Sync + 'static>;
type CsvGzReader<File> = Reader<GzDecoder<BufReader<File>>>;

const FILENAME_PARTS_2021: [&str; 2] = ["Node_", ".csv.gz"];
const FILENAME_PARTS_2022: [&str; 2] = ["NodeMetrics_", ".csv.gz"];

const DATA_QUEUE_CAP: usize = 400_000;
const QUEUE_POLL_INTERVAL_MS: u64 = 250;
const CSV_DELIMITER: u8 = b',';

const LABELS: [&str; 1] = ["node_id"];

/// Struct for holding fields after deserialization
#[derive(Debug, serde::Deserialize)]
pub struct NodeCsvFields {
    #[serde(rename = "", skip)]
    _trace: u64,

    #[serde(rename = "timestamp")]
    timestamp: u64,

    #[serde(rename = "nodeid")]
    node_id: String,

    #[serde(alias = "node_cpu_usage", alias = "cpu_utilization")]
    cpu_usage: Option<f64>,

    #[serde(alias = "node_memory_usage", alias = "memory_utilization")]
    memory_usage: Option<f64>,
}

lazy_static! {
    pub static ref NODE_DATA_QUEUE: ConcurrentQueue<NodeCsvFields> =
        ConcurrentQueue::bounded(DATA_QUEUE_CAP);
    pub static ref CPU_USAGE: GaugeVec = register_gauge_vec!(
        "alibaba_node_cpu_usage",
        "Cpu usages by alibaba nodes",
        &LABELS,
    )
    .unwrap();
    pub static ref MEMORY_USAGE: GaugeVec = register_gauge_vec!(
        "alibaba_node_memory_usage",
        "Memory usages by alibaba nodes",
        &LABELS,
    )
    .unwrap();
}

/// @brief Gets the filename for the Node_<idx>.csv.gz data based on the year
/// and the index number
///
/// @param[in] year     The year of the trace data. Supported values are 2021
///                     and 2022
/// @param[in] index_no The index of the csv file
///
/// @return A String of the filename based on the data year and index num
fn get_filename(year: u32, index_no: u16) -> String {
    let mut filename: String = String::new();
    let prefix: &str;
    let suffix: &str;
    let index: &str = &format!("{}", index_no);

    match year {
        2021 => {
            prefix = FILENAME_PARTS_2021[0];
            suffix = FILENAME_PARTS_2021[1];
        }
        2022 => {
            prefix = FILENAME_PARTS_2022[0];
            suffix = FILENAME_PARTS_2022[1];
        }
        _ => {
            panic!("Invalid year: {}", year);
        }
    }
    filename.push_str(prefix);
    filename.push_str(index);
    filename.push_str(suffix);

    filename
}

/// @brief Gets a csv reader for Node data
///
/// @param[in] input_dir The directory containing the csv file
/// @param[in] year      Which trace data year to create the reader for.
///                      supported years are 2021 and 2022
///
/// @return A reader for the .csv.gz files
///
/// @pre All files should have been converted to a .csv.gz format from the
///      .tar.gz format that they come as initially.
pub fn get_reader(
    input_dir: &str,
    year: u32,
    index_no: u16,
) -> Result<CsvGzReader<File>, BoxedErr> {
    let filename = get_filename(year, index_no);
    let file_path = Path::new(input_dir).join(&filename);
    let fd: File = File::open(file_path)?;
    let buf_rdr: BufReader<File> = BufReader::new(fd);
    let gz_decoder: GzDecoder<BufReader<File>> = GzDecoder::new(buf_rdr);

    let csv_rdr: CsvGzReader<File> = ReaderBuilder::new()
        .delimiter(CSV_DELIMITER)
        .flexible(true)
        .has_headers(true)
        .from_reader(gz_decoder);

    Ok(csv_rdr)
}

/// @brief Takes the timestamp of a trace in milliseconds and
///        returns the normalized time as a Duration
///
/// @param[in] time_millis The trace timestamp in milliseconds
///
/// @return The normalized timestamp as a Duration
///
/// @NOTE: Brief check of data suggests no dilation is necessary
///
/// @NOTE: Node data from 2022 is not sorted by timestamp whatsoever,
/// sometimes the data is listed in order of decreasing timestamp and other
/// times it's listed in order of increasing timestamp, so the timestamps
/// are modified to work with the exporter before being queued
///
/// @NOTE: SPEEDUP_FACTOR can be set via --speedup CLI argument for faster-than-realtime export
pub fn get_normalized_start_time(time_millis: u64) -> Duration {
    let speedup = crate::alibaba_metrics::SPEEDUP_FACTOR.get().unwrap_or(&1);
    Duration::from_millis(time_millis / speedup)
}

/// @brief Reads the csv data from .csv.gz files and adds them to the queue.
///
/// @param[in] input_dir The input directory
/// @param[in] data_year The year of the trace data
///
/// @pre All csv data should have been sorted by timestamp and compressed with
///      gzip
pub fn read_and_queue(
    input_dir: &str,
    all_parts: bool,
    part_index: Option<u16>,
    data_year: u32,
) -> Result<(), BoxedErr> {
    let mut part: u16 = 0;
    if !all_parts {
        part = part_index.unwrap();
    }

    while let Ok(mut rdr) = get_reader(input_dir, data_year, part) {
        let csv_iter = rdr.deserialize();
        for csv_line in csv_iter {
            while NODE_DATA_QUEUE.is_full() {
                thread::sleep(Duration::from_millis(QUEUE_POLL_INTERVAL_MS));
            }
            let parsed_line: NodeCsvFields = csv_line?;
            let _ = NODE_DATA_QUEUE.push(parsed_line);
        } // EOF
        part += 1;

        if !all_parts {
            break;
        }
    } // No more files to read, or couldn't find initial file

    if part == 0 {
        // Reading always starts at part 0
        panic!(
            "Failed to read initial .csv.gz file. Check that all data files
             are named in the correct format (2021: '{}<idx>{}', 2022: '{}<idx>{}),
             and that the csv files contain the field headers at the top.
            ",
            FILENAME_PARTS_2021[0],
            FILENAME_PARTS_2021[1],
            FILENAME_PARTS_2022[0],
            FILENAME_PARTS_2022[1]
        );
    } else {
        NODE_DATA_QUEUE.close();
        Ok(())
    }
}

/// @brief Exports a single line from the NODE_DATA_QUEUE
///
/// @param[in] csv_line A parsed line from a Node csv file
pub fn export_line(csv_line: NodeCsvFields) {
    let label_vals: [&str; 1] = [csv_line.node_id.as_str()];

    if let Some(cpu_usage) = csv_line.cpu_usage {
        CPU_USAGE.with_label_values(&label_vals).set(cpu_usage);
    }

    if let Some(memory_usage) = csv_line.memory_usage {
        MEMORY_USAGE
            .with_label_values(&label_vals)
            .set(memory_usage);
    }
}

/// @brief Exports lines from the queue until a line is found with a timestamp
///        later than the current runtime. This function will terminate the
///        the program once the queue has both been closed by the reader thread
///        and the queue is empty
pub fn export_from_queue() {
    let elapsed_t: Duration = utilities::get_time_elapsed();
    let check_time = |line: &NodeCsvFields| get_normalized_start_time(line.timestamp) <= elapsed_t;
    NODE_DATA_QUEUE
        .try_iter()
        .take_while(check_time)
        .for_each(export_line);

    // No more files to read and empty queue
    if NODE_DATA_QUEUE.is_closed() && NODE_DATA_QUEUE.is_empty() {
        info!("No more Node data to export, shutting down");
        std::process::exit(0);
    }
}

use clap::Parser;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rand_distr::{Distribution, Uniform};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{json, Value as JsonValue};
use std::time::Duration;
use tokio::time::sleep;

const RNG_SEED: u64 = 0;

/// Converts comma-separated string to vector of usize
fn get_num_vals_per_column(num_values_str: &str, num_columns: usize) -> Vec<usize> {
    let parse: Result<Vec<usize>, _> = num_values_str
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::parse::<usize>)
        .collect();

    let num_values: Vec<usize> = match parse {
        Ok(list) => list,
        Err(error) => panic!("Couldn't parse num_values_per_metadata_column: {error:?}"),
    };

    if num_values.len() == 1 {
        vec![num_values[0]; num_columns]
    } else {
        if num_values.len() != num_columns {
            panic!(
                "Number of num_values_per_metadata_column must be equal to metadata_columns count (got {} vs {})",
                num_values.len(),
                num_columns
            );
        }
        num_values
    }
}

/// Computes all combinations of metadata column values
fn compute_metadata_combinations(
    column_names: &[String],
    num_values_per_column: &[usize],
) -> Vec<Vec<(String, String)>> {
    // Build values for each column
    let mut values_per_column: Vec<Vec<String>> = Vec::with_capacity(column_names.len());
    for (col_idx, col_name) in column_names.iter().enumerate() {
        let count = num_values_per_column[col_idx];
        let mut bucket = Vec::with_capacity(count);
        for value_idx in 0..count {
            bucket.push(format!("{}_{}", col_name, value_idx));
        }
        values_per_column.push(bucket);
    }

    // Cartesian product
    fn cartesian_product(pools: &[Vec<String>]) -> Vec<Vec<String>> {
        let mut result: Vec<Vec<String>> = vec![Vec::new()];
        for pool in pools {
            let mut next = Vec::new();
            for prefix in &result {
                for item in pool {
                    let mut new_prefix = prefix.clone();
                    new_prefix.push(item.clone());
                    next.push(new_prefix);
                }
            }
            result = next;
        }
        result
    }

    let combos = cartesian_product(&values_per_column);

    // Pair column names with values
    combos
        .into_iter()
        .map(|combo| {
            column_names
                .iter()
                .zip(combo.into_iter())
                .map(|(name, val)| (name.clone(), val))
                .collect()
        })
        .collect()
}

/// Builds a JSON record for a single data point
fn build_json_record(
    timestamp_ms: i64,
    time_column: &str,
    metadata: &[(String, String)],
    value_columns: &[String],
    rng: &mut SmallRng,
    uniform_dist: &Uniform<f64>,
) -> JsonValue {
    let mut record = json!({});
    let obj = record.as_object_mut().unwrap();

    // Add timestamp
    obj.insert(time_column.to_string(), json!(timestamp_ms));

    // Add metadata columns
    for (col_name, col_value) in metadata {
        obj.insert(col_name.clone(), json!(col_value));
    }

    // Add value columns with random values
    for col_name in value_columns {
        let value = uniform_dist.sample(rng);
        obj.insert(col_name.clone(), json!(value));
    }

    record
}

#[derive(Parser)]
#[command(name = "fake_kafka_exporter")]
#[command(about = "A fake data exporter that outputs SQL/tabular-style JSON records to Kafka")]
struct Args {
    #[arg(long, default_value = "localhost:9092", help = "Kafka broker address")]
    kafka_broker: String,

    #[arg(long, help = "Kafka topic name")]
    kafka_topic: String,

    #[arg(long, default_value = "time", help = "Name of the timestamp column")]
    time_column: String,

    #[arg(long, help = "Comma-separated metadata column names")]
    metadata_columns: String,

    #[arg(long, help = "Comma-separated counts per metadata column")]
    num_values_per_metadata_column: String,

    #[arg(long, help = "Comma-separated value column names")]
    value_columns: String,

    #[arg(
        long,
        default_value = "100.0",
        help = "Max value for uniform distribution [0, value_scale]"
    )]
    value_scale: f64,

    #[arg(long, default_value = "1", help = "Seconds between data batches")]
    frequency: u64,

    #[arg(long, default_value = "false", help = "Print records to console")]
    debug_print: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Parse column names
    let metadata_columns: Vec<String> = args
        .metadata_columns
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let value_columns: Vec<String> = args
        .value_columns
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if metadata_columns.is_empty() {
        panic!("At least one metadata column is required");
    }
    if value_columns.is_empty() {
        panic!("At least one value column is required");
    }

    // Parse num_values_per_metadata_column
    let num_values_per_column =
        get_num_vals_per_column(&args.num_values_per_metadata_column, metadata_columns.len());

    // Compute all metadata combinations
    let all_metadata_combinations =
        compute_metadata_combinations(&metadata_columns, &num_values_per_column);

    let num_combinations: usize = num_values_per_column.iter().product();
    println!(
        "Generated {} metadata combinations from {} columns",
        num_combinations,
        metadata_columns.len()
    );

    // Create Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create Kafka producer");

    println!(
        "Connected to Kafka broker: {}, topic: {}",
        args.kafka_broker, args.kafka_topic
    );

    // Initialize RNG and distribution
    let mut rng = SmallRng::seed_from_u64(RNG_SEED);
    let uniform_dist = Uniform::new_inclusive(0.0, args.value_scale)
        .expect("Failed to create Uniform distribution");

    // Main data generation loop
    loop {
        let timestamp_ms = chrono::Utc::now().timestamp_millis();

        for metadata_combo in &all_metadata_combinations {
            let record = build_json_record(
                timestamp_ms,
                &args.time_column,
                metadata_combo,
                &value_columns,
                &mut rng,
                &uniform_dist,
            );

            let record_str = serde_json::to_string(&record)?;

            if args.debug_print {
                println!("{}", record_str);
            }

            // Send to Kafka
            let delivery_status = producer
                .send(
                    FutureRecord::to(&args.kafka_topic)
                        .payload(&record_str)
                        .key(""),
                    Duration::from_secs(0),
                )
                .await;

            if let Err((err, _)) = delivery_status {
                eprintln!("Failed to send message to Kafka: {}", err);
            }
        }

        sleep(Duration::from_secs(args.frequency)).await;
    }
}

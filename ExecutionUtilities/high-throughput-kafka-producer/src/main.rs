use anyhow::Result;
use clap::Parser;
use futures::future::join_all;
use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
struct PrometheusTemplate {
    template: String,
    metric_name: String,
}

impl PrometheusTemplate {
    fn new(metric_name: String, labels: &[String]) -> Self {
        // Create JSON template with placeholders
        let mut label_parts = Vec::new();
        let label_keys = ["hostname", "location", "application_name", "instance", "job"];

        for (i, key) in label_keys.iter().enumerate() {
            if i < labels.len() {
                label_parts.push(format!("\"{}\": \"{}\"", key, labels[i]));
            }
        }
        let labels_json = label_parts.join(", ");

        let template = format!(
            "{{\"metric_name\": \"{}\", \"timestamp\": {{TIMESTAMP}}, \"value\": {{VALUE}}, \"labels\": {{{}}}}}",
            metric_name, labels_json
        );

        Self {
            template,
            metric_name,
        }
    }

    fn generate_message(&self, timestamp: u64, value: f64) -> Vec<u8> {
        // Fast string replacement instead of JSON serialization
        let mut result = self.template.clone();
        result = result.replace("{TIMESTAMP}", &timestamp.to_string());
        result = result.replace("{VALUE}", &format!("{:.2}", value));
        result.into_bytes()
    }
}

#[derive(Debug, Clone)]
struct LabelChoices {
    hostname: Vec<String>,
    location: Vec<String>,
    application_name: Vec<String>,
    instance: Vec<String>,
    job: Vec<String>,
}

impl Default for LabelChoices {
    fn default() -> Self {
        Self {
            hostname: vec![
                "host1".to_string(),
                "host2".to_string(),
                "host3".to_string(),
                "host4".to_string(),
                "host5".to_string(),
            ],
            location: vec![
                "us-east".to_string(),
                "us-west".to_string(),
                "eu-central".to_string(),
                "ap-southeast".to_string(),
            ],
            application_name: vec![
                "app1".to_string(),
                "app2".to_string(),
                "app3".to_string(),
                "app4".to_string(),
            ],
            instance: vec![
                "worker1".to_string(),
                "worker2".to_string(),
                "worker3".to_string(),
                "worker4".to_string(),
            ],
            job: vec![
                "throughput-test".to_string(),
                "latency-test".to_string(),
                "stress-test".to_string(),
            ],
        }
    }
}

static METRIC_NAMES: &[&str] = &[
    "cpu_usage",
    "memory_usage",
    "network_throughput",
    "disk_iops",
    "response_time",
    "error_rate",
];

#[derive(Debug, Clone)]
struct ProducerStats {
    messages_sent: Arc<AtomicU64>,
    bytes_sent: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
}

impl ProducerStats {
    fn new() -> Self {
        Self {
            messages_sent: Arc::new(AtomicU64::new(0)),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
        }
    }

    fn add_message(&self, bytes: u64) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    fn add_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (u64, u64, u64) {
        (
            self.messages_sent.load(Ordering::Relaxed),
            self.bytes_sent.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

#[derive(Parser, Debug)]
#[command(name = "kafka-throughput-producer")]
#[command(about = "High-performance Kafka producer for Arroyo benchmarking")]
struct Args {
    #[arg(long, default_value = "localhost:9092")]
    kafka_broker: String,

    #[arg(long)]
    kafka_topic: String,

    #[arg(long, default_value = "1000000")]
    total_messages: u64,

    #[arg(long, default_value = "10000")]
    messages_per_second: u64,

    #[arg(long)]
    duration: Option<u64>,

    #[arg(long, default_value = "1")]
    num_threads: usize,

    #[arg(long, default_value = "1")]
    num_partitions: i32,

    #[arg(long, default_value = "1")]
    replication_factor: i32,

    #[arg(long)]
    vary_labels: bool,

    #[arg(long, default_value = "false")]
    enable_flush: bool,

    #[arg(long, default_value = "none")]
    compression: String,

    #[arg(long, default_value = "65536")]
    batch_size: usize,
}

#[derive(Clone)]
struct HighThroughputProducer {
    producer: FutureProducer,
    topic_name: String,
    stats: ProducerStats,
    templates: Arc<Vec<PrometheusTemplate>>,
    label_choices: LabelChoices,
}


impl HighThroughputProducer {
    async fn new_with_compression(
        kafka_broker: &str,
        topic_name: String,
        num_partitions: i32,
        replication_factor: i32,
        compression: &str,
    ) -> Result<Self> {
        // High-performance producer configuration optimized for throughput
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", kafka_broker)
            .set("linger.ms", "5")
            .set("batch.size", "1048576") // 1MB batches
            .set("compression.type", compression)
            .set("queue.buffering.max.messages", "1000000")
            .set("queue.buffering.max.kbytes", "2097152") // 2GB
            .set("batch.num.messages", "10000")
            .set("acks", "0") // No acknowledgments for max throughput
            .set("retries", "0") // No retries for max throughput
            .set("message.max.bytes", "1048576") // 1MB
            .set("queue.buffering.max.ms", "10")
            .set("delivery.timeout.ms", "30000")
            .create()?;

        // Pre-generate templates for all label combinations
        let label_choices = LabelChoices::default();
        let all_labels = Self::generate_all_label_combinations_static(&label_choices);
        let mut templates = Vec::new();

        for metric_name in METRIC_NAMES {
            for labels in &all_labels {
                templates.push(PrometheusTemplate::new(metric_name.to_string(), labels));
            }
        }

        let kafka_producer = Self {
            producer,
            topic_name: topic_name.clone(),
            stats: ProducerStats::new(),
            templates: Arc::new(templates),
            label_choices,
        };

        kafka_producer
            .create_topic_if_not_exists(kafka_broker, &topic_name, num_partitions, replication_factor)
            .await?;

        Ok(kafka_producer)
    }

    async fn create_topic_if_not_exists(
        &self,
        kafka_broker: &str,
        topic_name: &str,
        num_partitions: i32,
        replication_factor: i32,
    ) -> Result<()> {
        let admin: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", kafka_broker)
            .create()?;

        let metadata = admin.inner().fetch_metadata(None, Duration::from_secs(10))?;

        let topic_exists = metadata.topics().iter().any(|t| t.name() == topic_name);

        if !topic_exists {
            let new_topic = NewTopic::new(
                topic_name,
                num_partitions,
                TopicReplication::Fixed(replication_factor),
            );

            let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
            let results = admin.create_topics(&[new_topic], &opts).await?;

            for result in results {
                match result {
                    Ok(topic) => info!("Created topic: {}", topic),
                    Err((topic, error)) => {
                        error!("Failed to create topic {}: {}", topic, error);
                        return Err(anyhow::anyhow!("Topic creation failed"));
                    }
                }
            }

            // Wait for topic creation to propagate
            sleep(Duration::from_secs(2)).await;
            info!("Topic '{}' created with {} partitions", topic_name, num_partitions);
        }

        Ok(())
    }

    fn generate_fast_prometheus_message(&self) -> Vec<u8> {
        let mut rng = thread_rng();

        // Select random template (pre-built with metric name and labels)
        let template = self.templates.choose(&mut rng).unwrap();

        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let value = match template.metric_name.as_str() {
            "cpu_usage" | "memory_usage" => rng.gen_range(0.0..100.0),
            "network_throughput" => rng.gen_range(1000.0..10000.0),
            "disk_iops" => rng.gen_range(100.0..5000.0),
            "response_time" => rng.gen_range(0.1..1000.0),
            "error_rate" => rng.gen_range(0.0..5.0),
            _ => rng.gen_range(0.0..1000.0),
        };

        template.generate_message(timestamp, value)
    }

    fn generate_prometheus_metric(&self, labels: &[String]) -> Result<HashMap<String, serde_json::Value>> {
        let mut rng = thread_rng();

        let metric_name = METRIC_NAMES.choose(&mut rng).unwrap().to_string();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

        let value = match metric_name.as_str() {
            "cpu_usage" | "memory_usage" => rng.gen_range(0.0..100.0),
            "network_throughput" => rng.gen_range(1000.0..10000.0),
            "disk_iops" => rng.gen_range(100.0..5000.0),
            "response_time" => rng.gen_range(0.1..1000.0),
            "error_rate" => rng.gen_range(0.0..5.0),
            _ => rng.gen_range(0.0..1000.0),
        };

        let label_keys = ["hostname", "location", "application_name", "instance", "job"];
        let mut label_map = HashMap::new();

        for (i, key) in label_keys.iter().enumerate() {
            if i < labels.len() {
                label_map.insert(key.to_string(), serde_json::Value::String(labels[i].clone()));
            }
        }

        let mut metric = HashMap::new();
        metric.insert("metric_name".to_string(), serde_json::Value::String(metric_name));
        metric.insert("timestamp".to_string(), serde_json::Value::Number(serde_json::Number::from(timestamp)));
        metric.insert("value".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(value).unwrap()));
        metric.insert("labels".to_string(), serde_json::Value::Object(label_map.into_iter().collect()));

        Ok(metric)
    }

    async fn produce_message_batch(
        &self,
        batch: Vec<(String, Vec<String>)>,
    ) -> Result<()> {
        let mut futures = Vec::new();

        for (partition_key, labels) in batch {
            let metric = self.generate_prometheus_metric(&labels)?;
            let message_data = serde_json::to_vec(&metric)?;
            let message_size = message_data.len();

            let stats = self.stats.clone();
            let producer = self.producer.clone();
            let topic_name = self.topic_name.clone();

            let future = async move {
                let record = FutureRecord::to(&topic_name)
                    .key(&partition_key)
                    .payload(&message_data);

                match producer.send(record, Duration::from_secs(10)).await {
                    Ok((partition, offset)) => {
                        stats.add_message(message_size as u64);
                        Ok((partition, offset))
                    }
                    Err((kafka_error, message)) => {
                        stats.add_error();
                        warn!("Failed to send message: {}", kafka_error);
                        Err((kafka_error, message))
                    }
                }
            };
            futures.push(future);
        }

        let _results = join_all(futures).await;
        Ok(())
    }


    fn generate_all_label_combinations_static(label_choices: &LabelChoices) -> Vec<Vec<String>> {
        let label_values = vec![
            &label_choices.hostname,
            &label_choices.location,
            &label_choices.application_name,
            &label_choices.instance,
            &label_choices.job,
        ];

        label_values
            .into_iter()
            .multi_cartesian_product()
            .map(|combo| combo.into_iter().cloned().collect())
            .collect()
    }

    fn generate_all_label_combinations(&self) -> Vec<Vec<String>> {
        let label_values = vec![
            &self.label_choices.hostname,
            &self.label_choices.location,
            &self.label_choices.application_name,
            &self.label_choices.instance,
            &self.label_choices.job,
        ];

        label_values
            .into_iter()
            .multi_cartesian_product()
            .map(|combo| combo.into_iter().cloned().collect())
            .collect()
    }

    async fn run_benchmark(
        &self,
        args: &Args,
    ) -> Result<()> {
        info!(
            "Starting benchmark: {} messages at {} msg/s using {} threads",
            args.total_messages, args.messages_per_second, args.num_threads
        );
        info!("Producer initialized with {} pre-generated templates", self.templates.len());
        info!("🚀 Data generation started!");

        let all_labels = self.generate_all_label_combinations();
        let start_time = Instant::now();
        let mut messages_sent = 0u64;

        let messages_per_interval = args.messages_per_second;
        let batch_size = std::cmp::max(1, args.batch_size);
        let interval = Duration::from_secs(1);

        while messages_sent < args.total_messages {
            if let Some(duration) = args.duration {
                if start_time.elapsed().as_secs() > duration {
                    break;
                }
            }

            let interval_start = Instant::now();

            // Select labels for this interval
            let labels_subset = if args.vary_labels {
                let mut rng = thread_rng();
                let num_labels = rng.gen_range(1..=std::cmp::min(all_labels.len(), messages_per_interval as usize));
                all_labels.choose_multiple(&mut rng, num_labels).cloned().collect::<Vec<_>>()
            } else {
                all_labels[..std::cmp::min(all_labels.len(), messages_per_interval as usize)].to_vec()
            };

            // Create batches for parallel processing
            let mut tasks = Vec::new();
            let mut remaining_messages = std::cmp::min(
                messages_per_interval,
                args.total_messages - messages_sent
            );

            while remaining_messages > 0 && tasks.len() < args.num_threads {
                let current_batch_size = std::cmp::min(batch_size as u64, remaining_messages) as usize;
                let batch: Vec<(String, Vec<String>)> = (0..current_batch_size)
                    .map(|i| {
                        let labels = &labels_subset[i % labels_subset.len()];
                        let partition_key = format!("{}_{}", labels[0], labels[1]);
                        (partition_key, labels.clone())
                    })
                    .collect();

                // Clone necessary data for the async task
                let producer = self.producer.clone();
                let topic_name = self.topic_name.clone();
                let stats = self.stats.clone();
                let label_choices = self.label_choices.clone();

                tasks.push(tokio::spawn(async move {
                    let temp_producer = HighThroughputProducer {
                        producer,
                        topic_name,
                        stats,
                        templates: Arc::new(Vec::new()), // Empty templates for batch producer
                        label_choices,
                    };
                    temp_producer.produce_message_batch(batch).await
                }));

                remaining_messages -= current_batch_size as u64;
                messages_sent += current_batch_size as u64;
            }

            // Wait for all tasks to complete
            for task in tasks {
                if let Err(e) = task.await? {
                    error!("Batch processing failed: {}", e);
                }
            }

            // Rate limiting
            let elapsed = interval_start.elapsed();
            if elapsed < interval {
                sleep(interval - elapsed).await;
            }

            // Print progress
            if messages_sent % (args.messages_per_second) == 0 {
                self.print_stats(start_time);
            }
        }

        // Final flush - wait for all messages to be delivered
        info!("Flushing remaining messages...");
        if let Err(e) = self.producer.flush(Duration::from_secs(30)) {
            warn!("Error during flush: {}", e);
        }

        info!("Benchmark completed!");
        self.print_stats(start_time);

        Ok(())
    }

    fn print_stats(&self, start_time: Instant) {
        let (messages, bytes, errors) = self.stats.get_stats();
        let elapsed = start_time.elapsed().as_secs_f64();

        let rate = if elapsed > 0.0 { messages as f64 / elapsed } else { 0.0 };
        let throughput_mb = if elapsed > 0.0 {
            (bytes as f64 / (1024.0 * 1024.0)) / elapsed
        } else {
            0.0
        };

        info!(
            "Messages: {}, Rate: {:.2} msg/s, Throughput: {:.2} MB/s, Errors: {}",
            messages, rate, throughput_mb, errors
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let producer = HighThroughputProducer::new_with_compression(
        &args.kafka_broker,
        args.kafka_topic.clone(),
        args.num_partitions,
        args.replication_factor,
        &args.compression,
    ).await?;

    producer.run_benchmark(&args).await?;

    Ok(())
}

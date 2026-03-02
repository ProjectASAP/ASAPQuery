use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::time::interval;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "throughput-test-group")
        .set("bootstrap.servers", "localhost:9092")
        .set("auto.offset.reset", "earliest")
        // Performance optimizations
        .set("fetch.min.bytes", "50000")
        .set("fetch.wait.max.ms", "500")
        .set("queued.min.messages", "100000")
        .set("receive.message.max.bytes", "100000000")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["test_input"])?;

    let message_count = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    // Stats reporting task
    let stats_count = Arc::clone(&message_count);
    let stats_bytes = Arc::clone(&total_bytes);
    let stats_start = start_time;

    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            print_stats(&stats_count, &stats_bytes, stats_start);
        }
    });

    // Main consumer loop
    loop {
        tokio::select! {
            message = consumer.recv() => {
                match message {
                    Ok(msg) => {
                        message_count.fetch_add(1, Ordering::Relaxed);
                        if let Some(payload) = msg.payload() {
                            total_bytes.fetch_add(payload.len() as u64, Ordering::Relaxed);
                        }
                    }
                    Err(e) => eprintln!("Error receiving message: {}", e),
                }
            }
            _ = signal::ctrl_c() => {
                println!("\n=== Final Statistics ===");
                print_stats(&message_count, &total_bytes, start_time);
                break;
            }
        }
    }

    Ok(())
}

fn print_stats(
    message_count: &Arc<AtomicU64>,
    total_bytes: &Arc<AtomicU64>,
    start_time: Instant,
) {
    let elapsed = start_time.elapsed().as_millis() as f64;
    let count = message_count.load(Ordering::Relaxed) as f64;
    let bytes = total_bytes.load(Ordering::Relaxed) as f64;

    let messages_per_sec = (count * 1000.0) / elapsed;
    let mb_per_sec = (bytes * 1000.0) / (elapsed * 1024.0 * 1024.0);

    println!(
        "Messages: {}, Rate: {:.2} msg/s, Throughput: {:.2} MB/s",
        count as u64, messages_per_sec, mb_per_sec
    );
}

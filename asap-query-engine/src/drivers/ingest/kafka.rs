use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::data_model::enums::{InputFormat, StreamingEngine};
use crate::data_model::traits::SerializableToSink;
use crate::data_model::PrecomputedOutput;
use crate::data_model::StreamingConfig;
use crate::stores::Store;
use crate::utils::PrecomputeDumper;

#[derive(Debug, Clone)]
pub struct KafkaConsumerConfig {
    pub broker: String,
    pub topic: String,
    pub group_id: String,
    pub auto_offset_reset: String,
    pub input_format: InputFormat,
    pub decompress_json: bool,
    pub batch_size: usize,
    pub poll_timeout_ms: u64,
    pub streaming_engine: StreamingEngine,
    pub dump_precomputes: bool,
    pub dump_output_dir: Option<String>,
}

pub struct KafkaConsumer<T: Store> {
    config: KafkaConsumerConfig,
    store: Arc<T>,
    consumer: StreamConsumer,
    streaming_config: Arc<StreamingConfig>,
    previous_consume_time: Option<Instant>,
    precompute_dumper: Option<PrecomputeDumper>,
}

impl<T: Store + Send + Sync + 'static> KafkaConsumer<T> {
    pub fn new(
        config: KafkaConsumerConfig,
        store: Arc<T>,
        streaming_config: Arc<StreamingConfig>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.broker)
            .set("group.id", &config.group_id)
            .set("auto.offset.reset", &config.auto_offset_reset)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .create()?;

        // Subscribe to the topic
        consumer.subscribe(&[&config.topic])?;

        // Initialize precompute dumper if enabled
        let precompute_dumper = if config.dump_precomputes {
            match &config.dump_output_dir {
                Some(output_dir) => match PrecomputeDumper::new(output_dir) {
                    Ok(dumper) => {
                        info!("Precompute dumping enabled to: {}", dumper.get_file_path());
                        Some(dumper)
                    }
                    Err(e) => {
                        error!("Failed to create precompute dumper: {}", e);
                        info!("Continuing without precompute dumping");
                        None
                    }
                },
                None => {
                    warn!("Precompute dumping requested but no output directory provided");
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            config,
            store,
            consumer,
            streaming_config,
            previous_consume_time: None,
            precompute_dumper,
        })
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Starting Kafka consumer for topic: {} on broker: {}",
            self.config.topic, self.config.broker
        );

        let mut batch = Vec::new();

        loop {
            // Collect messages into batches like Python implementation
            let timeout_duration = Duration::from_millis(self.config.poll_timeout_ms);

            // StreamConsumer uses recv() for async message reception
            match tokio::time::timeout(timeout_duration, self.consumer.recv()).await {
                Ok(Ok(message)) => {
                    // Add timing debug similar to Python
                    let current_consume_time = Instant::now();
                    if let Some(previous_time) = self.previous_consume_time {
                        let elapsed = current_consume_time.duration_since(previous_time);
                        debug!(
                            "Time since last consume: {:.2} seconds",
                            elapsed.as_secs_f64()
                        );
                    }
                    self.previous_consume_time = Some(current_consume_time);
                    // Process single message and add to batch
                    match self.process_message(&message) {
                        Ok(Some((precomputed_output, precompute_accumulator))) => {
                            // Check if this is an empty DeltaSetAggregator and skip it
                            if let Some(delta_acc) = precompute_accumulator
                                .as_any()
                                .downcast_ref::<crate::precompute_operators::delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator>()
                            {
                                if delta_acc.is_empty() {
                                    debug!("Skipping empty DeltaSetAggregatorAccumulator");
                                    continue;
                                }
                            }

                            // Dump precompute if enabled
                            if let Some(ref mut dumper) = self.precompute_dumper {
                                if let Err(e) = dumper.dump_precompute(
                                    &precomputed_output,
                                    precompute_accumulator.as_ref(),
                                ) {
                                    error!("Failed to dump precompute: {}", e);
                                }
                            }

                            // Store both the metadata and the real accumulator data
                            batch.push((precomputed_output, precompute_accumulator));
                        }
                        Ok(None) => {
                            debug!("Message processed but no precomputed output produced");
                        }
                        Err(e) => {
                            error!("Error processing message: {e}");
                            continue; // Skip this message and continue
                        }
                    }

                    // Process batch when we reach batch_size or periodically
                    if batch.len() >= self.config.batch_size {
                        self.process_batch(&mut batch).await?;
                    }
                }
                Ok(Err(kafka_err)) => {
                    if kafka_err.rdkafka_error_code()
                        == Some(rdkafka::types::RDKafkaErrorCode::PartitionEOF)
                    {
                        debug!("Reached end of partition");
                        continue;
                    } else {
                        error!("Kafka error: {kafka_err}");
                        return Err(Box::new(kafka_err));
                    }
                }
                Err(_) => {
                    // Timeout occurred - process any accumulated batch
                    if !batch.is_empty() {
                        debug!(
                            "Poll timeout, processing accumulated batch of {} items",
                            batch.len()
                        );
                        self.process_batch(&mut batch).await?;
                    } else {
                        debug!("Poll timeout, no messages to process");
                    }
                }
            }
        }
    }

    async fn process_batch(
        &self,
        batch: &mut Vec<(PrecomputedOutput, Box<dyn crate::data_model::AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_start_time = Instant::now();
        debug!("Processing batch of {} messages", batch.len());

        // Batch insert with real precompute data like Python implementation
        let store_insert_start_time = Instant::now();
        match self.store.insert_precomputed_output_batch(batch.to_vec()) {
            Ok(_) => {
                let store_insert_duration = store_insert_start_time.elapsed();
                debug!(
                    "Store batch insert took: {:.2}ms",
                    store_insert_duration.as_secs_f64() * 1000.0
                );
                debug!("{}", batch[0].0.get_freshness_debug_string());
                for (item, _) in batch.iter() {
                    debug!(
                        "Received message: {} with aggregation_id: {}",
                        serde_json::to_string(&item.serialize_to_json())
                            .unwrap_or_else(|_| "failed to serialize".to_string()),
                        item.aggregation_id
                    );
                }
            }
            Err(e) => {
                error!("Error inserting precomputed output batch: {}", e);
                return Err(e);
            }
        }

        batch.clear();
        let total_batch_duration = batch_start_time.elapsed();
        debug!(
            "Total batch processing took: {:.2}ms",
            total_batch_duration.as_secs_f64() * 1000.0
        );
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn process_message(
        &self,
        message: &rdkafka::message::BorrowedMessage<'_>,
    ) -> Result<
        Option<(PrecomputedOutput, Box<dyn crate::data_model::AggregateCore>)>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let message_start_time = Instant::now();
        let payload = match message.payload() {
            Some(payload) => payload,
            None => {
                warn!("Received message with no payload");
                return Ok(None);
            }
        };

        match self.config.input_format {
            InputFormat::Byte => {
                // For binary format, we need to first extract metadata to get aggregation_type
                // Then use it to create the proper accumulator
                // let (metadata, _precompute_bytes) =
                //     match PrecomputedOutput::deserialize_from_bytes_with_precompute(payload) {
                //         Ok(result) => result,
                //         Err(e) => {
                //             error!("Error deserializing binary message metadata: {}", e);
                //             return Err(format!("Binary deserialization error: {e}").into());
                //         }
                //     };

                // // Now deserialize with the correct accumulator type
                // match PrecomputedOutput::deserialize_from_bytes_with_precompute_and_type(
                //     payload,
                //     &metadata.config.aggregation_type,
                // ) {
                //     Ok((output, precompute)) => {
                //         debug!("Successfully deserialized binary message with precompute data");
                //         Ok(Some((output, precompute)))
                //     }
                //     Err(e) => {
                //         error!("Error deserializing binary message with precompute: {}", e);
                //         Err(e)
                //     }
                // }
                error!("Binary input format with precompute not implemented");
                Err("Binary input format with precompute not implemented".into())
            }
            InputFormat::Json => {
                // Arroyo messages - gzip decompression is applied at precompute level, not message level
                let json_str = match String::from_utf8(payload.to_vec()) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Error converting bytes to UTF-8: {}", e);
                        return Err(format!("UTF-8 conversion error: {e}").into());
                    }
                };

                let json_dict: serde_json::Value = match serde_json::from_str(&json_str) {
                    Ok(dict) => dict,
                    Err(e) => {
                        error!("Error parsing Arroyo JSON: {}", e);
                        debug!("JSON content: {}", json_str);
                        return Err(format!("JSON parsing error: {e}").into());
                    }
                };

                let deserialize_start_time = Instant::now();
                match PrecomputedOutput::deserialize_from_json_arroyo(
                    &json_dict,
                    &self.streaming_config,
                ) {
                    Ok((output, precompute)) => {
                        let deserialize_duration = deserialize_start_time.elapsed();
                        debug!(
                            "Arroyo deserialization took: {:.2}ms",
                            deserialize_duration.as_secs_f64() * 1000.0
                        );
                        debug!(
                            "Successfully deserialized Arroyo JSON message with precompute data"
                        );
                        let total_message_duration = message_start_time.elapsed();
                        debug!(
                            "Total Arroyo message processing took: {:.2}ms",
                            total_message_duration.as_secs_f64() * 1000.0
                        );
                        Ok(Some((output, precompute)))
                    }
                    Err(e) => {
                        error!(
                            "Error deserializing Arroyo PrecomputedOutput from JSON with precompute: {e}"
                        );
                        debug!("JSON content: {}", json_str);
                        Err(e)
                    }
                }
            }
        }
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Stopping Kafka consumer");

        // Flush precompute dumper if it exists
        if let Some(ref mut dumper) = self.precompute_dumper {
            if let Err(e) = dumper.flush() {
                error!("Failed to flush precompute dumper on stop: {}", e);
            }
        }

        // The consumer will be dropped automatically
        Ok(())
    }
}

use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::ingest_source::{route_decoded_samples, IngestContext, IngestSource};
use chrono::NaiveDateTime;
use std::time::Instant;
use tracing::info;

pub enum TimestampUnit {
    Seconds,
    Millis,
}

impl std::str::FromStr for TimestampUnit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "seconds" => Ok(TimestampUnit::Seconds),
            "millis" => Ok(TimestampUnit::Millis),
            other => Err(format!(
                "unknown timestamp_unit '{}': expected 'seconds' or 'millis'",
                other
            )),
        }
    }
}

impl TimestampUnit {
    pub fn to_ms(&self, raw: i64) -> i64 {
        match self {
            TimestampUnit::Seconds => raw * 1000,
            TimestampUnit::Millis => raw,
        }
    }
}

pub struct JsonFileIngestConfig {
    pub path: String,
    pub metric_name: String,
    pub value_col: String,
    /// Label columns. Will be sorted alphabetically in the labels string.
    pub label_cols: Vec<String>,
    pub timestamp_col: String,
    pub timestamp_unit: TimestampUnit,
    pub batch_size: usize,
    /// Test-support only: real delay after sending each batch, in
    /// milliseconds. `0` (the only value any production config uses) is a
    /// no-op — the reader sends batches as fast as it can, unchanged from
    /// before this field existed. A nonzero value exists so integration
    /// tests can force ingest to span real wall-clock time deterministically
    /// (e.g. to give a periodic flush timer real chances to fire mid-ingest),
    /// instead of hoping incidental scheduling overhead is enough.
    pub batch_delay_ms: u64,
}

pub struct JsonFileIngestSource {
    config: JsonFileIngestConfig,
}

impl JsonFileIngestSource {
    pub fn new(config: JsonFileIngestConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl IngestSource for JsonFileIngestSource {
    async fn run(
        self: Box<Self>,
        ctx: IngestContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<DecodedSample>>(8);

        let reader_handle = tokio::task::spawn_blocking(
            move || -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
                let file = std::fs::File::open(&config.path)?;
                let reader = std::io::BufReader::new(file);

                let mut sorted_label_cols = config.label_cols.clone();
                sorted_label_cols.sort();

                let mut batch: Vec<DecodedSample> = Vec::with_capacity(config.batch_size);
                let mut row_count: u64 = 0;

                use std::io::BufRead;
                for line_result in reader.lines() {
                    let line = line_result?;
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    let obj: serde_json::Value = serde_json::from_str(trimmed).map_err(
                        |e| -> Box<dyn std::error::Error + Send + Sync> {
                            std::io::Error::other(format!(
                                "failed to parse JSON line {}: {}",
                                row_count + 1,
                                e
                            ))
                            .into()
                        },
                    )?;

                    let value: f64 = obj
                        .get(&config.value_col)
                        .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                            std::io::Error::other(format!(
                                "value column '{}' not found in JSON object",
                                config.value_col
                            ))
                            .into()
                        })?
                        .as_f64()
                        .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                            std::io::Error::other(format!(
                                "value column '{}' is not a number",
                                config.value_col
                            ))
                            .into()
                        })?;

                    let ts_val = obj.get(&config.timestamp_col).ok_or_else(
                        || -> Box<dyn std::error::Error + Send + Sync> {
                            std::io::Error::other(format!(
                                "timestamp column '{}' not found in JSON object",
                                config.timestamp_col
                            ))
                            .into()
                        },
                    )?;

                    // Accept integer (Unix epoch) or string datetime "YYYY-MM-DD HH:MM:SS".
                    let raw_ts: i64 = if let Some(i) = ts_val.as_i64() {
                        i
                    } else if let Some(s) = ts_val.as_str() {
                        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                                std::io::Error::other(format!(
                                    "timestamp column '{}' value {:?} is not an integer or \
                                     a parseable datetime string: {}",
                                    config.timestamp_col, s, e
                                ))
                                .into()
                            })?
                            .and_utc()
                            .timestamp()
                    } else {
                        return Err(std::io::Error::other(format!(
                            "timestamp column '{}' is not an integer or string",
                            config.timestamp_col
                        ))
                        .into());
                    };

                    let timestamp_ms = config.timestamp_unit.to_ms(raw_ts);

                    let labels = if sorted_label_cols.is_empty() {
                        config.metric_name.clone()
                    } else {
                        let mut s = String::with_capacity(64);
                        s.push_str(&config.metric_name);
                        s.push('{');
                        for (i, col) in sorted_label_cols.iter().enumerate() {
                            if i > 0 {
                                s.push(',');
                            }
                            let val_owned;
                            let val = if let Some(s) = obj.get(col).and_then(|v| v.as_str()) {
                                s
                            } else if let Some(v) = obj.get(col) {
                                val_owned = v.to_string();
                                val_owned.as_str()
                            } else {
                                return Err(std::io::Error::other(format!(
                                    "label column '{}' not found in JSON object (row {})",
                                    col,
                                    row_count + 1
                                ))
                                .into());
                            };
                            s.push_str(col);
                            s.push_str("=\"");
                            s.push_str(val);
                            s.push('"');
                        }
                        s.push('}');
                        s
                    };

                    batch.push(DecodedSample {
                        labels,
                        timestamp_ms,
                        value,
                    });
                    row_count += 1;

                    if batch.len() >= config.batch_size {
                        let send_batch =
                            std::mem::replace(&mut batch, Vec::with_capacity(config.batch_size));
                        if tx.blocking_send(send_batch).is_err() {
                            break;
                        }
                        if config.batch_delay_ms > 0 {
                            std::thread::sleep(std::time::Duration::from_millis(
                                config.batch_delay_ms,
                            ));
                        }
                    }
                }

                if !batch.is_empty() {
                    let _ = tx.blocking_send(batch);
                    if config.batch_delay_ms > 0 {
                        std::thread::sleep(std::time::Duration::from_millis(config.batch_delay_ms));
                    }
                }

                Ok(row_count)
            },
        );

        let mut total_samples: u64 = 0;
        while let Some(batch) = rx.recv().await {
            total_samples += batch.len() as u64;
            route_decoded_samples(&ctx, batch, Instant::now()).await?;
        }

        let rows = reader_handle.await??;
        info!(
            "JSON ingest complete: {} rows ingested, {} samples routed",
            rows, total_samples
        );

        ctx.router.broadcast_shutdown().await?;
        Ok(())
    }
}

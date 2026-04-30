use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::ingest_source::{route_decoded_samples, IngestContext, IngestSource};
use std::time::Instant;
use tracing::info;

pub struct CsvFileIngestConfig {
    pub path: String,
    pub metric_name: String,
    pub value_col: String,
    /// Label columns. Will be sorted alphabetically in the labels string.
    pub label_cols: Vec<String>,
    /// If Some, parse this column as the timestamp in milliseconds.
    /// If None, synthesize timestamps using start_ts_ms + row_index * ts_step_ms.
    pub timestamp_col: Option<String>,
    pub start_ts_ms: i64,
    /// Required when timestamp_col is None.
    pub ts_step_ms: i64,
    pub batch_size: usize,
}

pub struct CsvFileIngestSource {
    config: CsvFileIngestConfig,
}

impl CsvFileIngestSource {
    pub fn new(config: CsvFileIngestConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl IngestSource for CsvFileIngestSource {
    async fn run(
        self: Box<Self>,
        ctx: IngestContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<DecodedSample>>(8);

        let reader_handle = tokio::task::spawn_blocking(
            move || -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
                let mut rdr = csv::Reader::from_path(&config.path)?;
                let headers = rdr.headers()?.clone();

                let value_idx = headers
                    .iter()
                    .position(|h| h == config.value_col)
                    .ok_or_else(|| format!("value column '{}' not found in CSV", config.value_col))
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        std::io::Error::other(e).into()
                    })?;

                let ts_idx = config
                    .timestamp_col
                    .as_ref()
                    .map(|col| {
                        headers
                            .iter()
                            .position(|h| h == col.as_str())
                            .ok_or_else(|| format!("timestamp column '{}' not found in CSV", col))
                            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                                std::io::Error::other(e).into()
                            })
                    })
                    .transpose()?;

                let mut sorted_label_cols = config.label_cols.clone();
                sorted_label_cols.sort();

                let mut label_idxs: Vec<(String, usize)> = Vec::new();
                for col in &sorted_label_cols {
                    let idx = headers
                        .iter()
                        .position(|h| h == col.as_str())
                        .ok_or_else(|| format!("label column '{}' not found in CSV", col))
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                            std::io::Error::other(e).into()
                        })?;
                    label_idxs.push((col.clone(), idx));
                }

                let mut batch: Vec<DecodedSample> = Vec::with_capacity(config.batch_size);
                let mut row_count: u64 = 0;

                for result in rdr.records() {
                    let record = result?;

                    let labels = if label_idxs.is_empty() {
                        config.metric_name.clone()
                    } else {
                        let mut s = String::with_capacity(64);
                        s.push_str(&config.metric_name);
                        s.push('{');
                        for (i, (col, idx)) in label_idxs.iter().enumerate() {
                            if i > 0 {
                                s.push(',');
                            }
                            s.push_str(col);
                            s.push_str("=\"");
                            s.push_str(record.get(*idx).unwrap_or(""));
                            s.push('"');
                        }
                        s.push('}');
                        s
                    };

                    let value: f64 = record
                        .get(value_idx)
                        .ok_or("missing value field")
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                            std::io::Error::other(e).into()
                        })?
                        .parse()
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                            std::io::Error::other(format!("failed to parse value: {}", e)).into()
                        })?;

                    let timestamp_ms = match ts_idx {
                        Some(idx) => record
                            .get(idx)
                            .ok_or("missing timestamp field")
                            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                                std::io::Error::other(e).into()
                            })?
                            .parse::<i64>()
                            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                                std::io::Error::other(format!("failed to parse timestamp: {}", e))
                                    .into()
                            })?,
                        None => config.start_ts_ms + (row_count as i64) * config.ts_step_ms,
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
                    }
                }

                if !batch.is_empty() {
                    let _ = tx.blocking_send(batch);
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
            "CSV ingest complete: {} rows ingested, {} samples routed",
            rows, total_samples
        );

        ctx.router.broadcast_shutdown().await?;
        Ok(())
    }
}

use super::derived_value::DerivedValueConfig;
use super::stateful_transition::{StatefulTransitionConfig, StatefulTransitionOperator};
use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::computed_labels::ComputedLabelConfig;
use crate::precompute_engine::ingest_source::{route_decoded_samples, IngestContext, IngestSource};
use crate::precompute_engine::row_expansion::{
    expand_row_to_samples, DerivedValueSource, LabelSource, RowExpansionConfig,
};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{info, warn};

pub struct CsvFileIngestConfig {
    pub path: String,
    pub metric_name: String,
    pub value_col: Option<String>,
    /// Label columns. Will be sorted alphabetically in the labels string.
    pub label_cols: Vec<String>,
    /// Computed labels. Each key is the logical label name; the rule says how to compute it.
    pub computed_label_cols: HashMap<String, ComputedLabelConfig>,
    pub stateful_transitions: Vec<StatefulTransitionConfig>,
    pub derived_value_cols: Vec<DerivedValueConfig>,
    /// If Some, parse this column as the timestamp.
    /// Accepts Unix milliseconds or ClickHouse DateTime strings like YYYY-MM-DD HH:MM:SS.
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

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = year - if month <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn parse_timestamp_ms(raw: &str) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let trimmed = raw.trim();

    // Existing behavior: Unix milliseconds.
    if let Ok(v) = trimmed.parse::<i64>() {
        return Ok(v);
    }

    // ClickHouse DateTime commonly exports as "YYYY-MM-DD HH:MM:SS".
    // Also tolerate "YYYY-MM-DDTHH:MM:SS" and trailing fractional seconds.
    let normalized = trimmed.replace('T', " ");
    let base = normalized
        .split('.')
        .next()
        .unwrap_or(normalized.as_str())
        .trim_end_matches('Z');

    if base.len() < 19 {
        return Err(std::io::Error::other(format!("unsupported timestamp format: {}", raw)).into());
    }

    let dt = &base[..19];

    let year: i64 = dt[0..4].parse()?;
    let month: i64 = dt[5..7].parse()?;
    let day: i64 = dt[8..10].parse()?;
    let hour: i64 = dt[11..13].parse()?;
    let minute: i64 = dt[14..16].parse()?;
    let second: i64 = dt[17..19].parse()?;

    let days = days_from_civil(year, month, day);
    Ok((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1000)
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

                let value_idx = match config.value_col.as_deref() {
                    Some(value_col) => Some(
                        headers
                            .iter()
                            .position(|h| h == value_col)
                            .ok_or_else(|| format!("value column '{}' not found in CSV", value_col))
                            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                                std::io::Error::other(e).into()
                            })?,
                    ),
                    None => {
                        warn!(
                            "CSV ingest config has no value_col; every row will be treated as \
                             an event count of 1.0. If a real metric column was intended, set \
                             value_col explicitly — this is otherwise silent."
                        );
                        None
                    }
                };

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

                let mut label_sources: Vec<LabelSource> = Vec::new();
                for col in &sorted_label_cols {
                    if headers.iter().any(|h| h == col.as_str()) {
                        label_sources.push(LabelSource::Physical { name: col.clone() });
                        continue;
                    }

                    if let Some(rule) = config.computed_label_cols.get(col) {
                        if !headers.iter().any(|h| h == rule.source_col.as_str()) {
                            return Err(std::io::Error::other(format!(
                                "source column '{}' for computed label '{}' not found in CSV",
                                rule.source_col, col
                            ))
                            .into());
                        }

                        label_sources.push(LabelSource::Computed {
                            name: col.clone(),
                            source_col: rule.source_col.clone(),
                            rule: rule.clone(),
                        });
                        continue;
                    }

                    return Err(std::io::Error::other(format!(
                        "label column '{}' not found in CSV and no computed_label_cols rule was provided",
                        col
                    ))
                    .into());
                }

                let mut batch: Vec<DecodedSample> = Vec::with_capacity(config.batch_size);
                let mut row_count: u64 = 0;
                let mut stateful_ops: Vec<StatefulTransitionOperator> = config
                    .stateful_transitions
                    .iter()
                    .cloned()
                    .map(StatefulTransitionOperator::new)
                    .collect();

                // Resolve each derived-value stream's source once, up front -
                // see `DerivedValueSource`'s doc comment (row_expansion.rs)
                // for what each variant means.
                let derived_value_sources: Vec<(String, DerivedValueSource)> = config
                    .derived_value_cols
                    .iter()
                    .map(|dv| -> Result<(String, DerivedValueSource), Box<dyn std::error::Error + Send + Sync>> {
                        let is_timestamp = config
                            .timestamp_col
                            .as_deref()
                            .is_some_and(|c| c == dv.source_column);
                        let source = if matches!(
                            dv.kind,
                            asap_types::derived_value::DerivedValueKind::ArgMax
                                | asap_types::derived_value::DerivedValueKind::ArgMin
                        ) {
                            if !headers.iter().any(|h| h == dv.source_column.as_str()) {
                                return Err(std::io::Error::other(format!(
                                    "source column '{}' for arg derived value '{}' not found in CSV",
                                    dv.source_column, dv.metric_name
                                ))
                                .into());
                            }
                            DerivedValueSource::ArgColumn {
                                source_col: dv.source_column.clone(),
                            }
                        } else if is_timestamp {
                            DerivedValueSource::UseTimestamp
                        } else if let Some(rule) = config.computed_label_cols.get(&dv.source_column) {
                            if !headers.iter().any(|h| h == rule.source_col.as_str()) {
                                return Err(std::io::Error::other(format!(
                                    "source column '{}' for computed derived value '{}' not found in CSV",
                                    rule.source_col, dv.metric_name
                                ))
                                .into());
                            }
                            DerivedValueSource::ComputedLabel {
                                source_col: rule.source_col.clone(),
                                rule: rule.clone(),
                            }
                        } else {
                            if !headers.iter().any(|h| h == dv.source_column.as_str()) {
                                return Err(std::io::Error::other(format!(
                                    "source column '{}' for derived value stream '{}' not found in CSV",
                                    dv.source_column, dv.metric_name
                                ))
                                .into());
                            }
                            DerivedValueSource::Column(dv.source_column.clone())
                        };
                        Ok((dv.metric_name.clone(), source))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let row_expansion_config = RowExpansionConfig {
                    metric_name: config.metric_name.clone(),
                    label_sources: label_sources.clone(),
                    derived_value_sources: derived_value_sources.clone(),
                };

                for result in rdr.records() {
                    let record = result?;

                    let row_map: HashMap<String, String> = headers
                        .iter()
                        .enumerate()
                        .map(|(idx, name)| {
                            (name.to_string(), record.get(idx).unwrap_or("").to_string())
                        })
                        .collect();

                    let value: f64 = match value_idx {
                        Some(idx) => record
                            .get(idx)
                            .ok_or("missing value field")
                            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                                std::io::Error::other(e).into()
                            })?
                            .parse()
                            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                                std::io::Error::other(format!("failed to parse value: {}", e))
                                    .into()
                            })?,
                        None => 1.0,
                    };

                    let timestamp_ms = match ts_idx {
                        Some(idx) => {
                            let raw_ts = record.get(idx).ok_or("missing timestamp field").map_err(
                                |e| -> Box<dyn std::error::Error + Send + Sync> {
                                    std::io::Error::other(e).into()
                                },
                            )?;
                            parse_timestamp_ms(raw_ts)?
                        }
                        None => config.start_ts_ms + (row_count as i64) * config.ts_step_ms,
                    };

                    let row_samples = expand_row_to_samples(
                        &row_expansion_config,
                        &row_map,
                        timestamp_ms,
                        value,
                    )
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        std::io::Error::other(e).into()
                    })?;

                    // Empty only when a required computed label was missing
                    // from this row (`on_missing: skip_sample`) - matches the
                    // original behavior of skipping the whole row, including
                    // stateful-transition processing below.
                    if row_samples.is_empty() && !label_sources.is_empty() {
                        row_count += 1;
                        continue;
                    }

                    for sample in row_samples {
                        batch.push(sample);

                        if batch.len() >= config.batch_size {
                            let send_batch = std::mem::replace(
                                &mut batch,
                                Vec::with_capacity(config.batch_size),
                            );
                            if tx.blocking_send(send_batch).is_err() {
                                return Ok(row_count);
                            }
                        }
                    }

                    for op in &mut stateful_ops {
                        if let Some((labels, value)) = op.process_row(&row_map) {
                            batch.push(DecodedSample {
                                labels,
                                timestamp_ms,
                                value,
                                arg_value: None,
                            });

                            if batch.len() >= config.batch_size {
                                let send_batch = std::mem::replace(
                                    &mut batch,
                                    Vec::with_capacity(config.batch_size),
                                );

                                if tx.blocking_send(send_batch).is_err() {
                                    return Ok(row_count);
                                }
                            }
                        }
                    }

                    row_count += 1;
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

        // CSV precompute must explicitly flush after all batches are routed.
        // Otherwise the final active windows may not be materialized before
        // worker shutdown, causing "No precomputed outputs found" at query time.
        ctx.router.broadcast_flush().await?;
        ctx.router.broadcast_shutdown().await?;
        Ok(())
    }
}

use super::derived_value::DerivedValueConfig;
use super::stateful_transition::{StatefulTransitionConfig, StatefulTransitionOperator};
use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::computed_labels::ComputedLabelConfig;
use crate::precompute_engine::ingest_source::{route_decoded_samples, IngestContext, IngestSource};
use crate::precompute_engine::row_expansion::{
    expand_row_to_samples, DerivedValueSource, LabelSource, RowExpansionConfig,
};
use bgpkit_parser::models::ElemType;
use bgpkit_parser::BgpkitParser;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{info, warn};

/// The fixed physical schema every MRT-sourced row exposes - see the field
/// mapping table in the MRT ingest plan for where each column comes from.
/// Unlike CSV ingest, this set isn't user-configurable: it's determined by
/// what `BgpElem` actually carries.
const PHYSICAL_LABEL_COLUMNS: &[&str] = &[
    "operation",
    "prefix",
    "peer_ip",
    "peer_asn",
    "as_path",
    "origin",
    "next_hop",
    "local_pref",
    "med",
    "communities",
    "atomic",
    "aggr_asn",
    "aggr_ip",
    "collector",
    "source_file",
];

/// The synthetic column name a `derived_value_cols` entry uses to mean
/// "the row's own event timestamp" - there's no user-configurable
/// `timestamp_col` for MRT (unlike CSV) since the time axis always comes
/// straight from `BgpElem::timestamp`.
const TIMESTAMP_COLUMN: &str = "timestamp";

pub struct MrtFileIngestConfig {
    pub path: String,
    pub metric_name: String,
    /// RIS/RouteViews collector name (e.g. "rrc00") - not derivable from the
    /// MRT bytes themselves, since that's a filename/URL convention rather
    /// than part of the wire format.
    pub collector: String,
    pub computed_label_cols: HashMap<String, ComputedLabelConfig>,
    pub stateful_transitions: Vec<StatefulTransitionConfig>,
    pub derived_value_cols: Vec<DerivedValueConfig>,
    pub batch_size: usize,
}

pub struct MrtFileIngestSource {
    config: MrtFileIngestConfig,
}

impl MrtFileIngestSource {
    pub fn new(config: MrtFileIngestConfig) -> Self {
        Self { config }
    }
}

/// Builds the shared row-expansion config from the schema-defining fields
/// every MRT ingest source (single-file, directory) accepts identically.
/// Free-standing (not tied to `MrtFileIngestConfig`) so it can be shared
/// with `MrtDirectoryIngestSource` without either source depending on the
/// other's config type.
pub(crate) fn build_row_expansion_config(
    metric_name: &str,
    computed_label_cols: &HashMap<String, ComputedLabelConfig>,
    derived_value_cols: &[DerivedValueConfig],
) -> Result<RowExpansionConfig, Box<dyn std::error::Error + Send + Sync>> {
    let mut named_label_sources: Vec<(String, LabelSource)> = PHYSICAL_LABEL_COLUMNS
        .iter()
        .map(|name| {
            (
                name.to_string(),
                LabelSource::Physical {
                    name: name.to_string(),
                },
            )
        })
        .collect();
    for (name, rule) in computed_label_cols {
        named_label_sources.push((
            name.clone(),
            LabelSource::Computed {
                name: name.clone(),
                source_col: rule.source_col.clone(),
                rule: rule.clone(),
            },
        ));
    }
    // Alphabetical, matching CsvFileIngestSource's label ordering convention
    // (label_cols.sort()) so the rendered labels string has a consistent
    // column order regardless of ingest source.
    named_label_sources.sort_by(|a, b| a.0.cmp(&b.0));
    let label_sources = named_label_sources.into_iter().map(|(_, s)| s).collect();

    let is_known_column = |col: &str| col == TIMESTAMP_COLUMN || PHYSICAL_LABEL_COLUMNS.contains(&col);

    let derived_value_sources: Vec<(String, DerivedValueSource)> = derived_value_cols
        .iter()
        .map(|dv| -> Result<(String, DerivedValueSource), Box<dyn std::error::Error + Send + Sync>> {
            let source = if matches!(
                dv.kind,
                asap_types::derived_value::DerivedValueKind::ArgMax
                    | asap_types::derived_value::DerivedValueKind::ArgMin
            ) {
                if !is_known_column(&dv.source_column) {
                    return Err(std::io::Error::other(format!(
                        "source column '{}' for arg derived value '{}' is not a known MRT column",
                        dv.source_column, dv.metric_name
                    ))
                    .into());
                }
                DerivedValueSource::ArgColumn {
                    source_col: dv.source_column.clone(),
                }
            } else if dv.source_column == TIMESTAMP_COLUMN {
                DerivedValueSource::UseTimestamp
            } else if let Some(rule) = computed_label_cols.get(&dv.source_column) {
                DerivedValueSource::ComputedLabel {
                    source_col: rule.source_col.clone(),
                    rule: rule.clone(),
                }
            } else {
                if !is_known_column(&dv.source_column) {
                    return Err(std::io::Error::other(format!(
                        "source column '{}' for derived value stream '{}' is not a known MRT column",
                        dv.source_column, dv.metric_name
                    ))
                    .into());
                }
                DerivedValueSource::Column(dv.source_column.clone())
            };
            Ok((dv.metric_name.clone(), source))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RowExpansionConfig {
        metric_name: metric_name.to_string(),
        label_sources,
        derived_value_sources,
    })
}

/// Maps one decoded `BgpElem` to the fixed physical row schema in
/// `PHYSICAL_LABEL_COLUMNS`, per the MRT ingest plan's field-mapping table.
fn build_row_from_elem(
    elem: &bgpkit_parser::models::BgpElem,
    collector: &str,
    source_file: &str,
) -> HashMap<String, String> {
    let operation = match elem.elem_type {
        ElemType::ANNOUNCE => "A",
        ElemType::WITHDRAW => "W",
    };
    let as_path = elem
        .as_path
        .as_ref()
        .map(|p| p.to_string())
        .unwrap_or_default();
    // MRT has no independently-verified "origin ASN" field separate from
    // AS_PATH - origin_asns is itself derived from the AS_PATH's last hop,
    // so this will always agree with splitByChar(' ', as_path)[-1] on
    // MRT-sourced data.
    let origin = elem
        .origin_asns
        .as_ref()
        .and_then(|asns| asns.first())
        .map(|a| a.to_string())
        .unwrap_or_default();
    let communities = elem
        .communities
        .as_ref()
        .map(|cs| {
            cs.iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();

    let mut row: HashMap<String, String> = HashMap::with_capacity(PHYSICAL_LABEL_COLUMNS.len());
    row.insert("operation".to_string(), operation.to_string());
    row.insert("prefix".to_string(), elem.prefix.prefix.to_string());
    row.insert("peer_ip".to_string(), elem.peer_ip.to_string());
    row.insert("peer_asn".to_string(), elem.peer_asn.to_string());
    row.insert("as_path".to_string(), as_path);
    row.insert("origin".to_string(), origin);
    row.insert(
        "next_hop".to_string(),
        elem.next_hop.map(|h| h.to_string()).unwrap_or_default(),
    );
    row.insert(
        "local_pref".to_string(),
        elem.local_pref.map(|v| v.to_string()).unwrap_or_default(),
    );
    row.insert(
        "med".to_string(),
        elem.med.map(|v| v.to_string()).unwrap_or_default(),
    );
    row.insert("communities".to_string(), communities);
    row.insert(
        "atomic".to_string(),
        if elem.atomic { "1" } else { "0" }.to_string(),
    );
    row.insert(
        "aggr_asn".to_string(),
        elem.aggr_asn.map(|a| a.to_string()).unwrap_or_default(),
    );
    row.insert(
        "aggr_ip".to_string(),
        elem.aggr_ip.map(|ip| ip.to_string()).unwrap_or_default(),
    );
    row.insert("collector".to_string(), collector.to_string());
    row.insert("source_file".to_string(), source_file.to_string());
    // Not in PHYSICAL_LABEL_COLUMNS (not a selectable label) - only present so
    // computed labels sourced from "timestamp" (date_bucket, hour_of_day,
    // day_of_week, week_start_bucket, five_minute_bucket) can resolve it the
    // same way CSV ingest's row map does, since those read a formatted string
    // via parse_ingest_timestamp rather than the numeric timestamp_ms passed
    // separately to expand_row_to_samples.
    let formatted_timestamp = DateTime::<Utc>::from_timestamp(
        elem.timestamp.trunc() as i64,
        ((elem.timestamp.fract()) * 1_000_000_000.0) as u32,
    )
    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
    .unwrap_or_default();
    row.insert("timestamp".to_string(), formatted_timestamp);
    row
}

/// Parses one MRT file end to end and sends its samples over `tx`, batched.
/// Blocking - must be called from within `spawn_blocking`. Shared by
/// `MrtFileIngestSource` (one known file) and `MrtDirectoryIngestSource`
/// (one call per file discovered in the watched directory), so a single
/// file's ingest behavior - including the log-and-skip malformed-record
/// policy - can't drift between the two.
pub(crate) fn ingest_one_mrt_file_blocking(
    path: &str,
    collector: &str,
    row_expansion_config: &RowExpansionConfig,
    stateful_transitions: &[StatefulTransitionConfig],
    batch_size: usize,
    tx: &tokio::sync::mpsc::Sender<Vec<DecodedSample>>,
) -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
    let parser = BgpkitParser::new(path)?;

    let mut batch: Vec<DecodedSample> = Vec::with_capacity(batch_size);
    let mut elem_count: u64 = 0;
    let mut error_count: u64 = 0;
    // Fresh per file, not shared across a directory ingest's files: files are
    // processed with bounded concurrency (see MrtBatchDirectoryIngestSource),
    // so there's no single well-ordered "previous row" stream to track state
    // across file boundaries without synchronization. This means a gap that
    // spans exactly across two files' boundary is missed, but every gap
    // within a file (the common case - collectors emit far more often than
    // once per file) is computed correctly.
    let mut stateful_ops: Vec<StatefulTransitionOperator> = stateful_transitions
        .iter()
        .cloned()
        .map(StatefulTransitionOperator::new)
        .collect();

    for result in parser.into_fallible_elem_iter() {
        let elem = match result {
            Ok(elem) => elem,
            Err(e) => {
                // MRT files are long-running collector dumps that
                // realistically contain occasional corrupt records;
                // abort-on-error (as CSV ingest does) would lose an entire
                // multi-GB file to one bad record. Log and keep going
                // instead.
                warn!("Skipping malformed MRT record in {}: {}", path, e);
                error_count += 1;
                continue;
            }
        };

        let row = build_row_from_elem(&elem, collector, path);
        let timestamp_ms = (elem.timestamp * 1000.0).round() as i64;

        let row_samples = expand_row_to_samples(row_expansion_config, &row, timestamp_ms, 1.0)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                std::io::Error::other(e).into()
            })?;

        for sample in row_samples {
            batch.push(sample);

            if batch.len() >= batch_size {
                let send_batch = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                if tx.blocking_send(send_batch).is_err() {
                    return Ok((elem_count, error_count));
                }
            }
        }

        for op in &mut stateful_ops {
            if let Some((labels, value)) = op.process_row(&row) {
                batch.push(DecodedSample {
                    labels,
                    timestamp_ms,
                    value,
                    arg_value: None,
                });

                if batch.len() >= batch_size {
                    let send_batch =
                        std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                    if tx.blocking_send(send_batch).is_err() {
                        return Ok((elem_count, error_count));
                    }
                }
            }
        }

        elem_count += 1;
    }

    if !batch.is_empty() {
        let _ = tx.blocking_send(batch);
    }

    Ok((elem_count, error_count))
}

#[async_trait::async_trait]
impl IngestSource for MrtFileIngestSource {
    async fn run(
        self: Box<Self>,
        ctx: IngestContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config;
        let row_expansion_config = build_row_expansion_config(
            &config.metric_name,
            &config.computed_label_cols,
            &config.derived_value_cols,
        )?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<DecodedSample>>(8);

        let reader_handle = tokio::task::spawn_blocking(move || {
            ingest_one_mrt_file_blocking(
                &config.path,
                &config.collector,
                &row_expansion_config,
                &config.stateful_transitions,
                config.batch_size,
                &tx,
            )
        });

        let mut total_samples: u64 = 0;
        while let Some(batch) = rx.recv().await {
            total_samples += batch.len() as u64;
            route_decoded_samples(&ctx, batch, Instant::now()).await?;
        }

        let (elems, errors) = reader_handle.await??;
        info!(
            "MRT ingest complete: {} elements ingested, {} malformed records skipped, {} samples routed",
            elems, errors, total_samples
        );

        // MRT precompute must explicitly flush after all batches are routed,
        // same as CsvFileIngestSource - otherwise the final active windows
        // may not be materialized before worker shutdown.
        ctx.router.broadcast_flush().await?;
        ctx.router.broadcast_shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bgpkit_parser::models::{AsPath, BgpElem, NetworkPrefix};
    use std::net::IpAddr;
    use std::str::FromStr;

    #[test]
    fn announce_element_maps_to_expected_row() {
        let elem = BgpElem {
            timestamp: 1_700_000_000.5,
            elem_type: ElemType::ANNOUNCE,
            peer_ip: IpAddr::from_str("192.0.2.1").unwrap(),
            peer_asn: 65000.into(),
            prefix: NetworkPrefix::from_str("10.0.0.0/8").unwrap(),
            next_hop: Some(IpAddr::from_str("192.0.2.254").unwrap()),
            as_path: Some(AsPath::from_sequence([65000u32, 65001, 65002])),
            origin_asns: Some(vec![65002.into()]),
            local_pref: Some(100),
            med: Some(50),
            atomic: false,
            ..Default::default()
        };

        let row = build_row_from_elem(&elem, "rrc00", "updates.20240101.0000.bz2");

        assert_eq!(row.get("operation").unwrap(), "A");
        assert_eq!(row.get("prefix").unwrap(), "10.0.0.0/8");
        assert_eq!(row.get("peer_ip").unwrap(), "192.0.2.1");
        assert_eq!(row.get("peer_asn").unwrap(), "65000");
        assert_eq!(row.get("as_path").unwrap(), "65000 65001 65002");
        // origin_asns' last hop matches as_path's last hop by construction -
        // see build_row_from_elem's doc comment on why this is expected to
        // always hold for MRT-sourced data.
        assert_eq!(row.get("origin").unwrap(), "65002");
        assert_eq!(row.get("next_hop").unwrap(), "192.0.2.254");
        assert_eq!(row.get("local_pref").unwrap(), "100");
        assert_eq!(row.get("med").unwrap(), "50");
        assert_eq!(row.get("atomic").unwrap(), "0");
        assert_eq!(row.get("aggr_asn").unwrap(), "");
        assert_eq!(row.get("aggr_ip").unwrap(), "");
        assert_eq!(row.get("collector").unwrap(), "rrc00");
        assert_eq!(row.get("source_file").unwrap(), "updates.20240101.0000.bz2");
    }

    #[test]
    fn withdraw_element_has_empty_path_attributes() {
        let elem = BgpElem {
            elem_type: ElemType::WITHDRAW,
            prefix: NetworkPrefix::from_str("198.51.100.0/24").unwrap(),
            next_hop: None,
            as_path: None,
            origin_asns: None,
            ..Default::default()
        };

        let row = build_row_from_elem(&elem, "rrc00", "w.bz2");

        assert_eq!(row.get("operation").unwrap(), "W");
        assert_eq!(row.get("prefix").unwrap(), "198.51.100.0/24");
        assert_eq!(row.get("as_path").unwrap(), "");
        assert_eq!(row.get("origin").unwrap(), "");
        assert_eq!(row.get("next_hop").unwrap(), "");
    }
}

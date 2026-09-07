use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::computed_labels::{
    compute_label_values, should_skip_on_missing, ComputedLabelConfig,
};
use crate::precompute_engine::worker::stable_string_hash_as_exact_f64;
use std::collections::HashMap;

/// One label to attach to every sample built from a row. `Physical` reads the
/// value straight from the row under `name` (a label always named after its
/// own source column); `Computed` derives one-or-more values from
/// `source_col` via `rule`, fanning a single row into multiple samples when
/// the rule yields more than one value (e.g. `token_explode`).
#[derive(Clone)]
pub enum LabelSource {
    Physical {
        name: String,
    },
    Computed {
        name: String,
        source_col: String,
        rule: ComputedLabelConfig,
    },
}

/// Where a derived-value stream's numeric payload comes from for a given row.
/// Mirrors `LabelSource` but for `DerivedValueConfig` entries - see that
/// type's doc comment for why derived values are re-emitted as independent
/// samples under their own metric name rather than reusing the row's main
/// value.
#[derive(Clone)]
pub enum DerivedValueSource {
    /// The source column is the table's own time column - the value is
    /// already parsed into `timestamp_ms`, so it's reused directly rather
    /// than re-parsed from the row as a plain f64 (which would fail outright
    /// for a DateTime string like "2024-01-03 00:00:00").
    UseTimestamp,
    /// Parse this raw row column as f64.
    Column(String),
    /// `source_col` names a `computed_label_cols` rule rather than a raw
    /// column - the value fed to the accumulator is the computed label's own
    /// (string) value, parsed as f64.
    ComputedLabel {
        source_col: String,
        rule: ComputedLabelConfig,
    },
    /// argMax/argMin: the sample's numeric value is always the row's own
    /// timestamp (the comparison key), and `source_col` names the raw column
    /// whose *string* value rides along as `arg_value` - see
    /// `DecodedSample`'s doc comment.
    ArgColumn { source_col: String },
}

#[derive(Clone)]
pub struct RowExpansionConfig {
    pub metric_name: String,
    pub label_sources: Vec<LabelSource>,
    pub derived_value_sources: Vec<(String, DerivedValueSource)>,
}

/// Expand one row into the samples it produces: any derived-value streams
/// registered on this config, followed by the row's own main-value sample(s)
/// (one per label combination, when a computed label fans a single row into
/// several). Returns an empty vec (not an error) when a computed label
/// required for the label set is missing and its rule says to skip.
pub fn expand_row_to_samples(
    config: &RowExpansionConfig,
    row: &HashMap<String, String>,
    timestamp_ms: i64,
    event_value: f64,
) -> Result<Vec<DecodedSample>, String> {
    let label_strings = match expand_label_strings(config, row)? {
        Some(labels) => labels,
        None => return Ok(Vec::new()),
    };

    let mut samples = Vec::with_capacity(label_strings.len() * (config.derived_value_sources.len() + 1));

    for (derived_metric_name, source) in &config.derived_value_sources {
        let (derived_value, derived_arg_value): (Option<f64>, Option<String>) = match source {
            DerivedValueSource::UseTimestamp => (Some(timestamp_ms as f64), None),
            DerivedValueSource::Column(col) => (
                // A derived-value column isn't always numeric (peer_ip,
                // prefix - CIDR/IP strings that can never parse as f64):
                // parsing genuinely numeric columns (origin ASNs, med, ...)
                // stays exact, but a non-numeric column previously failed
                // to parse silently, dropping every sample for it (a
                // uniqExact(peer_ip)/uniqExact(prefix) style query would
                // then just have no data ever, indistinguishable from a
                // capability-matching gap). Falls back to the same stable
                // hash surrogate `resolve_sample_value` in worker.rs
                // already uses for this exact "non-numeric value column
                // feeding an HLL/cardinality accumulator" case - only
                // distinctness matters there, so a deterministic hash is a
                // correct substitute, not merely a permissive one.
                row.get(col)
                    .and_then(|s| s.parse::<f64>().ok().or(Some(stable_string_hash_as_exact_f64(s)))),
                None,
            ),
            DerivedValueSource::ComputedLabel { source_col, rule } => {
                let raw_value = row.get(source_col).map(|s| s.as_str()).unwrap_or("");
                let v = compute_label_values(rule, raw_value)
                    .ok()
                    .and_then(|values| values.into_iter().next())
                    .and_then(|v| {
                        // Most computed labels are already numeric strings
                        // (token_select ASNs, split_length/string_length
                        // counts, hour_of_day, day_of_week). date_bucket is
                        // the one exception - a "YYYY-MM-DD" string, since
                        // that's also its correct GROUP BY display value - so
                        // a failed f64 parse falls back to that specific
                        // format rather than being treated as missing.
                        v.parse::<f64>().ok().or_else(|| {
                            chrono::NaiveDate::parse_from_str(&v, "%Y-%m-%d")
                                .ok()
                                .and_then(|d| d.and_hms_opt(0, 0, 0))
                                .map(|dt| dt.and_utc().timestamp_millis() as f64)
                        })
                    });
                (v, None)
            }
            DerivedValueSource::ArgColumn { source_col } => {
                let raw_value = row.get(source_col).map(|s| s.as_str()).unwrap_or("").to_string();
                (Some(timestamp_ms as f64), Some(raw_value))
            }
        };
        let Some(derived_value) = derived_value else {
            continue;
        };
        for labels in &label_strings {
            let derived_labels = format!(
                "{}{}",
                derived_metric_name,
                &labels[config.metric_name.len()..]
            );
            samples.push(DecodedSample {
                labels: derived_labels,
                timestamp_ms,
                value: derived_value,
                arg_value: derived_arg_value.clone(),
            });
        }
    }

    for labels in label_strings {
        samples.push(DecodedSample {
            labels,
            timestamp_ms,
            value: event_value,
            arg_value: None,
        });
    }

    Ok(samples)
}

fn expand_label_strings(
    config: &RowExpansionConfig,
    row: &HashMap<String, String>,
) -> Result<Option<Vec<String>>, String> {
    if config.label_sources.is_empty() {
        return Ok(Some(vec![config.metric_name.clone()]));
    }

    let mut expanded: Vec<Vec<(String, String)>> = vec![Vec::new()];

    for source in &config.label_sources {
        match source {
            LabelSource::Physical { name } => {
                let value = row.get(name).map(|s| s.as_str()).unwrap_or("").to_string();
                for labels in &mut expanded {
                    labels.push((name.clone(), value.clone()));
                }
            }

            LabelSource::Computed {
                name,
                source_col,
                rule,
            } => {
                let raw_value = row.get(source_col).map(|s| s.as_str()).unwrap_or("");
                let mut values = compute_label_values(rule, raw_value)?;

                if values.is_empty() {
                    if should_skip_on_missing(rule) {
                        return Ok(None);
                    }
                    values.push(String::new());
                }

                let mut next = Vec::with_capacity(expanded.len() * values.len());
                for labels in expanded.into_iter() {
                    for value in &values {
                        let mut labels2 = labels.clone();
                        labels2.push((name.clone(), value.clone()));
                        next.push(labels2);
                    }
                }
                expanded = next;
            }
        }
    }

    Ok(Some(
        expanded
            .into_iter()
            .map(|pairs| {
                let mut s = String::with_capacity(64);
                s.push_str(&config.metric_name);
                s.push('{');

                for (i, (name, value)) in pairs.into_iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    s.push_str(&name);
                    s.push_str("=\"");
                    s.push_str(&value);
                    s.push('"');
                }

                s.push('}');
                s
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn computed_rule(r#type: &str, source_col: &str) -> ComputedLabelConfig {
        ComputedLabelConfig {
            r#type: r#type.to_string(),
            source_col: source_col.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn plain_label_passthrough() {
        let config = RowExpansionConfig {
            metric_name: "requests".to_string(),
            label_sources: vec![LabelSource::Physical {
                name: "method".to_string(),
            }],
            derived_value_sources: vec![],
        };
        let row = row(&[("method", "GET")]);
        let samples = expand_row_to_samples(&config, &row, 1000, 1.0).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels, "requests{method=\"GET\"}");
        assert_eq!(samples[0].value, 1.0);
        assert_eq!(samples[0].timestamp_ms, 1000);
    }

    #[test]
    fn computed_label_fans_out_multiple_samples() {
        let config = RowExpansionConfig {
            metric_name: "hops".to_string(),
            label_sources: vec![LabelSource::Computed {
                name: "hop_asn".to_string(),
                source_col: "as_path".to_string(),
                rule: computed_rule("token_explode", "as_path"),
            }],
            derived_value_sources: vec![],
        };
        let row = row(&[("as_path", "100 200 300")]);
        let samples = expand_row_to_samples(&config, &row, 5000, 1.0).unwrap();
        assert_eq!(samples.len(), 3);
        let labels: Vec<&str> = samples.iter().map(|s| s.labels.as_str()).collect();
        assert!(labels.contains(&"hops{hop_asn=\"100\"}"));
        assert!(labels.contains(&"hops{hop_asn=\"200\"}"));
        assert!(labels.contains(&"hops{hop_asn=\"300\"}"));
    }

    #[test]
    fn derived_value_reemitted_under_its_own_metric() {
        let config = RowExpansionConfig {
            metric_name: "updates".to_string(),
            label_sources: vec![LabelSource::Physical {
                name: "peer".to_string(),
            }],
            derived_value_sources: vec![(
                "updates_med_avg".to_string(),
                DerivedValueSource::Column("med".to_string()),
            )],
        };
        let row = row(&[("peer", "1.1.1.1"), ("med", "42")]);
        let samples = expand_row_to_samples(&config, &row, 2000, 1.0).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels, "updates_med_avg{peer=\"1.1.1.1\"}");
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[1].labels, "updates{peer=\"1.1.1.1\"}");
        assert_eq!(samples[1].value, 1.0);
    }

    #[test]
    fn missing_required_computed_label_skips_row() {
        let config = RowExpansionConfig {
            metric_name: "x".to_string(),
            label_sources: vec![LabelSource::Computed {
                name: "last_hop".to_string(),
                source_col: "as_path".to_string(),
                rule: ComputedLabelConfig {
                    on_missing: Some("skip_sample".to_string()),
                    ..computed_rule("token_select", "as_path")
                },
            }],
            derived_value_sources: vec![],
        };
        let row = row(&[("as_path", "")]);
        let samples = expand_row_to_samples(&config, &row, 0, 1.0).unwrap();
        assert!(samples.is_empty());
    }

    #[test]
    fn arg_column_carries_timestamp_as_value_and_raw_string_as_arg() {
        let config = RowExpansionConfig {
            metric_name: "path".to_string(),
            label_sources: vec![LabelSource::Physical {
                name: "prefix".to_string(),
            }],
            derived_value_sources: vec![(
                "path_arg".to_string(),
                DerivedValueSource::ArgColumn {
                    source_col: "as_path".to_string(),
                },
            )],
        };
        let row = row(&[("prefix", "10.0.0.0/8"), ("as_path", "100 200")]);
        let samples = expand_row_to_samples(&config, &row, 9000, 1.0).unwrap();
        assert_eq!(samples[0].value, 9000.0);
        assert_eq!(samples[0].arg_value.as_deref(), Some("100 200"));
    }
}

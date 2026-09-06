use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use asap_types::query_requirements::QueryRequirements;
use asap_types::PromQLSchema;
use csv::StringRecord;
use promql_parser::label::MatchOp;
use promql_parser::parser::{self, Expr};
use promql_utilities::data_model::KeyByLabelNames;
use thiserror::Error;

use crate::config::input::MetricDefinition;

use super::solution::AQE;

const METRIC_COLUMN: &str = "metric";

#[derive(Debug, Error)]
pub enum DatasetError {
    #[error("failed to open dataset '{path}': {source}")]
    Open {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse dataset CSV: {0}")]
    Csv(#[from] csv::Error),
    #[error("dataset must contain a column named 'metric'")]
    MissingMetricColumn,
    #[error("dataset header contains duplicate column '{0}'")]
    DuplicateColumn(String),
    #[error("dataset header contains an empty column name")]
    EmptyColumn,
    #[error("dataset contains no series rows")]
    Empty,
    #[error("dataset row {row} has an empty metric name")]
    EmptyMetric { row: usize },
    #[error(
        "metric '{metric}' has inconsistent label columns at row {row}: expected {expected:?}, found {found:?}"
    )]
    InconsistentMetricSchema {
        metric: String,
        row: usize,
        expected: Vec<String>,
        found: Vec<String>,
    },
    #[error(
        "metric '{metric}' has duplicate series at row {row}; first occurrence was row {first_row}"
    )]
    DuplicateSeries {
        metric: String,
        row: usize,
        first_row: usize,
    },
    #[error("dataset label column '{0}' is not used by any metric")]
    UnusedColumn(String),
    #[error("dataset is missing workload metric '{0}'")]
    MissingMetric(String),
    #[error("dataset contains metric '{0}', which is not referenced by the workload")]
    ExtraMetric(String),
    #[error("metric '{metric}' has no label column '{label}'")]
    MissingLabel { metric: String, label: String },
    #[error("metric hint '{metric}' is not present in the dataset")]
    ExtraMetricHint { metric: String },
    #[error(
        "metric '{metric}' label hint does not match the dataset: hint={hint:?}, dataset={dataset:?}"
    )]
    MetricHintMismatch {
        metric: String,
        hint: Vec<String>,
        dataset: Vec<String>,
    },
    #[error("invalid spatial filter '{filter}': {reason}")]
    InvalidFilter { filter: String, reason: String },
    #[error("spatial filter '{filter}' uses unsupported matcher '{matcher}'")]
    UnsupportedFilter { filter: String, matcher: String },
    #[error("spatial filter '{filter}' repeats label '{label}'")]
    DuplicateFilterLabel { filter: String, label: String },
    #[error("metric '{metric}' has no series matching filter '{filter}'")]
    NoMatchingSeries { metric: String, filter: String },
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ProfileKey {
    pub metric: String,
    pub spatial_filter_normalized: String,
    pub grouping_labels: KeyByLabelNames,
}

impl ProfileKey {
    pub fn from_requirements(requirements: &QueryRequirements) -> Self {
        Self {
            metric: requirements.metric.clone(),
            spatial_filter_normalized: requirements.spatial_filter_normalized.clone(),
            grouping_labels: requirements.grouping_labels.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct MetricData {
    label_names: KeyByLabelNames,
    series: Vec<HashMap<String, String>>,
}

#[derive(Debug, Clone)]
pub struct SeriesDataset {
    metrics: HashMap<String, MetricData>,
}

impl SeriesDataset {
    pub fn from_path(path: &Path) -> Result<Self, DatasetError> {
        let file = File::open(path).map_err(|source| DatasetError::Open {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_reader(file)
    }

    pub fn from_reader<R: Read>(reader: R) -> Result<Self, DatasetError> {
        let mut csv_reader = csv::Reader::from_reader(reader);
        let headers = csv_reader.headers()?.clone();
        let header_names = validate_headers(&headers)?;
        let metric_index = header_names
            .iter()
            .position(|name| name == METRIC_COLUMN)
            .ok_or(DatasetError::MissingMetricColumn)?;

        let mut raw_rows = Vec::new();
        for (index, record) in csv_reader.records().enumerate() {
            let row_number = index + 2;
            let record = record?;
            let metric = record.get(metric_index).unwrap_or_default().to_string();
            if metric.is_empty() {
                return Err(DatasetError::EmptyMetric { row: row_number });
            }

            let values = header_names
                .iter()
                .enumerate()
                .filter_map(|(column_index, column_name)| {
                    if column_index == metric_index {
                        return None;
                    }
                    let value = record.get(column_index).unwrap_or_default();
                    (!value.is_empty()).then(|| (column_name.clone(), value.to_string()))
                })
                .collect();

            raw_rows.push(RawRow {
                row_number,
                metric,
                values,
            });
        }

        if raw_rows.is_empty() {
            return Err(DatasetError::Empty);
        }

        let mut rows_by_metric: HashMap<String, Vec<RawRow>> = HashMap::new();
        for row in raw_rows {
            rows_by_metric
                .entry(row.metric.clone())
                .or_default()
                .push(row);
        }

        let mut metrics = HashMap::new();
        let mut used_columns = HashSet::new();
        for (metric, rows) in rows_by_metric {
            let expected = sorted_keys(&rows[0].values);
            let mut seen_series: HashMap<Vec<String>, usize> = HashMap::new();
            let mut series = Vec::with_capacity(rows.len());

            for row in rows {
                let found = sorted_keys(&row.values);
                if found != expected {
                    return Err(DatasetError::InconsistentMetricSchema {
                        metric: metric.clone(),
                        row: row.row_number,
                        expected: expected.clone(),
                        found,
                    });
                }

                let key: Vec<String> = expected
                    .iter()
                    .map(|label| row.values[label].clone())
                    .collect();
                if let Some(first_row) = seen_series.insert(key, row.row_number) {
                    return Err(DatasetError::DuplicateSeries {
                        metric: metric.clone(),
                        row: row.row_number,
                        first_row,
                    });
                }

                used_columns.extend(expected.iter().cloned());
                series.push(row.values);
            }

            metrics.insert(
                metric,
                MetricData {
                    label_names: KeyByLabelNames::new(expected),
                    series,
                },
            );
        }

        for column in header_names {
            if column != METRIC_COLUMN && !used_columns.contains(&column) {
                return Err(DatasetError::UnusedColumn(column));
            }
        }

        Ok(Self { metrics })
    }

    pub fn schema(&self) -> PromQLSchema {
        let mut schema = PromQLSchema::new();
        for (metric, data) in &self.metrics {
            schema = schema.add_metric(metric.clone(), data.label_names.clone());
        }
        schema
    }

    pub fn validate_metric_hints(
        &self,
        hints: Option<&[MetricDefinition]>,
    ) -> Result<(), DatasetError> {
        let Some(hints) = hints else {
            return Ok(());
        };

        for hint in hints {
            let Some(data) = self.metrics.get(&hint.metric) else {
                return Err(DatasetError::ExtraMetricHint {
                    metric: hint.metric.clone(),
                });
            };
            let hint_labels = KeyByLabelNames::new(hint.labels.clone()).labels;
            if hint_labels != data.label_names.labels {
                return Err(DatasetError::MetricHintMismatch {
                    metric: hint.metric.clone(),
                    hint: hint_labels,
                    dataset: data.label_names.labels.clone(),
                });
            }
        }
        Ok(())
    }

    pub fn profile_aqes(&self, aqes: &[AQE]) -> Result<HashMap<ProfileKey, u64>, DatasetError> {
        let workload_metrics: HashSet<&str> = aqes
            .iter()
            .map(|aqe| aqe.requirements.metric.as_str())
            .collect();

        for metric in &workload_metrics {
            if !self.metrics.contains_key(*metric) {
                return Err(DatasetError::MissingMetric((*metric).to_string()));
            }
        }
        for metric in self.metrics.keys() {
            if !workload_metrics.contains(metric.as_str()) {
                return Err(DatasetError::ExtraMetric(metric.clone()));
            }
        }

        let mut profiles = HashMap::new();
        for aqe in aqes {
            let key = ProfileKey::from_requirements(&aqe.requirements);
            if let Entry::Vacant(entry) = profiles.entry(key) {
                entry.insert(self.profile(&aqe.requirements)?);
            }
        }
        Ok(profiles)
    }

    pub fn profile(&self, requirements: &QueryRequirements) -> Result<u64, DatasetError> {
        let data = self
            .metrics
            .get(&requirements.metric)
            .ok_or_else(|| DatasetError::MissingMetric(requirements.metric.clone()))?;

        for label in &requirements.grouping_labels.labels {
            if !data.label_names.labels.contains(label) {
                return Err(DatasetError::MissingLabel {
                    metric: requirements.metric.clone(),
                    label: label.clone(),
                });
            }
        }

        let matchers = parse_exact_filter(
            &requirements.metric,
            &requirements.spatial_filter_normalized,
        )?;
        for (label, _) in &matchers {
            if !data.label_names.labels.contains(label) {
                return Err(DatasetError::MissingLabel {
                    metric: requirements.metric.clone(),
                    label: label.clone(),
                });
            }
        }

        let matching_series: Vec<&HashMap<String, String>> = data
            .series
            .iter()
            .filter(|series| {
                matchers
                    .iter()
                    .all(|(label, value)| series.get(label).is_some_and(|actual| actual == value))
            })
            .collect();

        if matching_series.is_empty() {
            return Err(DatasetError::NoMatchingSeries {
                metric: requirements.metric.clone(),
                filter: requirements.spatial_filter_normalized.clone(),
            });
        }

        if requirements.grouping_labels.is_empty() {
            return Ok(1);
        }

        let groups: HashSet<Vec<&str>> = matching_series
            .iter()
            .map(|series| {
                requirements
                    .grouping_labels
                    .labels
                    .iter()
                    .map(|label| series[label].as_str())
                    .collect()
            })
            .collect();
        Ok(groups.len() as u64)
    }
}

#[derive(Debug)]
struct RawRow {
    row_number: usize,
    metric: String,
    values: HashMap<String, String>,
}

fn validate_headers(headers: &StringRecord) -> Result<Vec<String>, DatasetError> {
    let mut seen = HashSet::new();
    let mut names = Vec::with_capacity(headers.len());
    for name in headers {
        if name.is_empty() {
            return Err(DatasetError::EmptyColumn);
        }
        if !seen.insert(name.to_string()) {
            return Err(DatasetError::DuplicateColumn(name.to_string()));
        }
        names.push(name.to_string());
    }
    Ok(names)
}

fn sorted_keys(values: &HashMap<String, String>) -> Vec<String> {
    let mut keys: Vec<String> = values.keys().cloned().collect();
    keys.sort();
    keys
}

fn parse_exact_filter(metric: &str, filter: &str) -> Result<Vec<(String, String)>, DatasetError> {
    if filter.is_empty() {
        return Ok(Vec::new());
    }

    let selector = format!("{metric}{filter}");
    let expression = parser::parse(&selector).map_err(|error| DatasetError::InvalidFilter {
        filter: filter.to_string(),
        reason: error.to_string(),
    })?;
    let Expr::VectorSelector(vector) = expression else {
        return Err(DatasetError::InvalidFilter {
            filter: filter.to_string(),
            reason: "expected a vector selector".to_string(),
        });
    };

    if !vector.matchers.or_matchers.is_empty() {
        return Err(DatasetError::UnsupportedFilter {
            filter: filter.to_string(),
            matcher: "or".to_string(),
        });
    }

    let mut seen_labels = HashSet::new();
    let mut matchers = Vec::with_capacity(vector.matchers.matchers.len());
    for matcher in vector.matchers.matchers {
        if !matches!(&matcher.op, MatchOp::Equal) {
            return Err(DatasetError::UnsupportedFilter {
                filter: filter.to_string(),
                matcher: matcher.op.to_string(),
            });
        }
        if !seen_labels.insert(matcher.name.clone()) {
            return Err(DatasetError::DuplicateFilterLabel {
                filter: filter.to_string(),
                label: matcher.name,
            });
        }
        matchers.push((matcher.name, matcher.value));
    }
    Ok(matchers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::query_requirements::QueryRequirements;
    use promql_utilities::data_model::KeyByLabelNames;

    fn requirements(metric: &str, labels: &[&str], filter: &str) -> QueryRequirements {
        QueryRequirements {
            metric: metric.to_string(),
            statistics: vec![],
            data_range_ms: 1,
            grouping_labels: KeyByLabelNames::new(
                labels.iter().map(|label| (*label).to_string()).collect(),
            ),
            spatial_filter_normalized: filter.to_string(),
            topk_count_events: None,
            topk_by_labels: None,
        }
    }

    #[test]
    fn derives_metric_schema_and_counts_distinct_group_values() {
        let dataset = SeriesDataset::from_reader(
            "metric,job,instance,region\nrequests,api,a,us\nrequests,api,b,us\nrequests,worker,c,us\n"
                .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            dataset.schema().get_labels("requests").unwrap().labels,
            vec!["instance", "job", "region"]
        );
        assert_eq!(
            dataset
                .profile(&requirements("requests", &["job"], ""))
                .unwrap(),
            2
        );
        assert_eq!(
            dataset
                .profile(&requirements("requests", &["instance"], ""))
                .unwrap(),
            3
        );
    }

    #[test]
    fn applies_multiple_exact_filters() {
        let dataset = SeriesDataset::from_reader(
            "metric,job,instance\nrequests,api,a\nrequests,api,b\nrequests,worker,c\n".as_bytes(),
        )
        .unwrap();

        assert_eq!(
            dataset
                .profile(&requirements(
                    "requests",
                    &["instance"],
                    "{job=\"api\",instance=\"b\"}"
                ))
                .unwrap(),
            1
        );
    }

    #[test]
    fn empty_grouping_is_one_but_empty_filter_result_fails() {
        let dataset =
            SeriesDataset::from_reader("metric,job\nrequests,api\nrequests,worker\n".as_bytes())
                .unwrap();

        assert_eq!(
            dataset.profile(&requirements("requests", &[], "")).unwrap(),
            1
        );
        assert!(matches!(
            dataset.profile(&requirements("requests", &[], "{job=\"missing\"}")),
            Err(DatasetError::NoMatchingSeries { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_series() {
        let error = SeriesDataset::from_reader(
            "metric,job,instance\nrequests,api,a\nrequests,api,a\n".as_bytes(),
        )
        .unwrap_err();

        assert!(matches!(error, DatasetError::DuplicateSeries { .. }));
    }

    #[test]
    fn rejects_schema_changes_within_a_metric() {
        let error = SeriesDataset::from_reader(
            "metric,job,instance\nrequests,api,a\nrequests,api,\n".as_bytes(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DatasetError::InconsistentMetricSchema { .. }
        ));
    }

    #[test]
    fn rejects_non_exact_filters() {
        let dataset = SeriesDataset::from_reader("metric,job\nrequests,api\n".as_bytes()).unwrap();

        let error = dataset
            .profile(&requirements("requests", &[], "{job=~\"api\"}"))
            .unwrap_err();
        assert!(matches!(error, DatasetError::UnsupportedFilter { .. }));
    }

    #[test]
    fn allows_different_stable_schemas_for_different_metrics() {
        let dataset = SeriesDataset::from_reader(
            "metric,job,instance,region\nrequests,api,a,\nrequests,api,b,\nlatency,,,us-east\nlatency,,,us-west\n"
                .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            dataset.schema().get_labels("requests").unwrap().labels,
            vec!["instance", "job"]
        );
        assert_eq!(
            dataset.schema().get_labels("latency").unwrap().labels,
            vec!["region"]
        );
    }

    #[test]
    fn rejects_missing_and_extra_workload_metrics() {
        let dataset =
            SeriesDataset::from_reader("metric,job\nrequests,api\nother,worker\n".as_bytes())
                .unwrap();
        let requests = AQE {
            requirements: requirements("requests", &["job"], ""),
            query_strings: vec![],
            query_frequency_hz: 1.0,
            min_t_repeat_ms: 1,
            t_repeat_gcd_ms: 1,
        };

        assert!(matches!(
            dataset.profile_aqes(&[requests]),
            Err(DatasetError::ExtraMetric(metric)) if metric == "other"
        ));
    }

    #[test]
    fn validates_existing_metric_hints_against_dataset_schema() {
        let dataset =
            SeriesDataset::from_reader("metric,job,instance\nrequests,api,a\n".as_bytes()).unwrap();
        let matching = MetricDefinition {
            metric: "requests".into(),
            labels: vec!["instance".into(), "job".into()],
        };
        let mismatching = MetricDefinition {
            metric: "requests".into(),
            labels: vec!["job".into()],
        };

        assert!(dataset.validate_metric_hints(Some(&[matching])).is_ok());
        assert!(matches!(
            dataset.validate_metric_hints(Some(&[mismatching])),
            Err(DatasetError::MetricHintMismatch { .. })
        ));
    }

    #[test]
    fn rejects_unknown_grouping_labels() {
        let dataset = SeriesDataset::from_reader("metric,job\nrequests,api\n".as_bytes()).unwrap();

        assert!(matches!(
            dataset.profile(&requirements("requests", &["instance"], "")),
            Err(DatasetError::MissingLabel { .. })
        ));
    }
}

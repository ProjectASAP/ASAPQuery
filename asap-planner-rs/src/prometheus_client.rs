use std::collections::HashSet;

use promql_parser::parser::Expr;
use promql_utilities::data_model::KeyByLabelNames;
use sketch_db_common::PromQLSchema;
use tracing::warn;

use crate::error::ControllerError;

/// Walk a PromQL AST and collect all metric names referenced by VectorSelectors.
fn collect_metric_names(expr: &Expr, names: &mut HashSet<String>) {
    match expr {
        Expr::VectorSelector(vs) => {
            if let Some(name) = &vs.name {
                names.insert(name.clone());
            }
        }
        Expr::MatrixSelector(ms) => {
            if let Some(name) = &ms.vs.name {
                names.insert(name.clone());
            }
        }
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_metric_names(arg, names);
            }
        }
        Expr::Aggregate(agg) => {
            collect_metric_names(&agg.expr, names);
        }
        Expr::Binary(bin) => {
            collect_metric_names(&bin.lhs, names);
            collect_metric_names(&bin.rhs, names);
        }
        Expr::Subquery(sq) => {
            collect_metric_names(&sq.expr, names);
        }
        _ => {}
    }
}

/// Extract all unique metric names referenced in a slice of PromQL query strings.
/// Queries that fail to parse are skipped with a warning.
pub fn extract_metric_names(queries: &[String]) -> HashSet<String> {
    let mut names = HashSet::new();
    for query in queries {
        match promql_parser::parser::parse(query) {
            Ok(expr) => collect_metric_names(&expr, &mut names),
            Err(e) => warn!(
                "Could not parse query {:?} for metric name extraction: {}",
                query, e
            ),
        }
    }
    names
}

/// Query Prometheus `GET /api/v1/series?match[]=<metric>` and return the set of label key names
/// for that metric, or `None` if no series were found.
///
/// Internal `__*__` labels (e.g. `__name__`) are excluded from the result.
///
/// TODO: This queries only the last 5 minutes of series data (Prometheus default when no
/// `start`/`end` parameters are provided). Expand to a configurable lookback window to capture
/// metrics that have not been seen recently.
fn fetch_labels_for_metric(
    prometheus_url: &str,
    metric_name: &str,
) -> Result<Option<Vec<String>>, ControllerError> {
    let url = format!("{}/api/v1/series", prometheus_url.trim_end_matches('/'));
    let client = reqwest::blocking::Client::new();
    let response = client
        .get(&url)
        .query(&[("match[]", metric_name)])
        .send()
        .map_err(|e| {
            ControllerError::PrometheusClient(format!(
                "HTTP request failed for metric '{}': {}",
                metric_name, e
            ))
        })?;

    if !response.status().is_success() {
        return Err(ControllerError::PrometheusClient(format!(
            "Prometheus returned HTTP {} for metric '{}'",
            response.status(),
            metric_name
        )));
    }

    let body: serde_json::Value = response.json().map_err(|e| {
        ControllerError::PrometheusClient(format!(
            "Failed to parse Prometheus response for metric '{}': {}",
            metric_name, e
        ))
    })?;

    let data = match body.get("data").and_then(|d| d.as_array()) {
        Some(arr) => arr,
        None => {
            warn!(
                "Prometheus returned no 'data' array for metric '{}'; skipping",
                metric_name
            );
            return Ok(None);
        }
    };

    if data.is_empty() {
        warn!(
            "Prometheus returned no series for metric '{}' in the last 5 minutes; skipping",
            metric_name
        );
        return Ok(None);
    }

    // Collect all unique label key names across all returned series,
    // filtering out internal __*__ labels.
    let mut label_keys: HashSet<String> = HashSet::new();
    for series in data {
        if let Some(labels) = series.as_object() {
            for key in labels.keys() {
                if !key.starts_with("__") {
                    label_keys.insert(key.clone());
                }
            }
        }
    }

    Ok(Some(label_keys.into_iter().collect()))
}

/// Build a `PromQLSchema` by querying Prometheus for each metric name found in the given
/// PromQL queries. Metrics with no series in Prometheus are skipped with a warning.
pub fn build_schema_from_prometheus(
    prometheus_url: &str,
    queries: &[String],
) -> Result<PromQLSchema, ControllerError> {
    let metric_names = extract_metric_names(queries);
    let mut schema = PromQLSchema::new();

    for metric_name in metric_names {
        match fetch_labels_for_metric(prometheus_url, &metric_name)? {
            Some(labels) => {
                schema = schema.add_metric(metric_name, KeyByLabelNames::new(labels));
            }
            None => {
                // Warning already emitted inside fetch_labels_for_metric.
            }
        }
    }

    Ok(schema)
}

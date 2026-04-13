use std::collections::HashSet;
use std::thread;
use std::time::Duration;

use asap_types::PromQLSchema;
use promql_parser::parser::Expr;
use promql_utilities::data_model::KeyByLabelNames;
use tracing::{debug, warn};

use crate::error::ControllerError;

/// Number of times to retry a Prometheus request on 503 (service not yet ready).
const MAX_RETRIES: u32 = 15;
/// Delay between retries.
const RETRY_DELAY: Duration = Duration::from_secs(2);

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

    for attempt in 1..=MAX_RETRIES {
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

        let status = response.status();

        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            warn!(
                "Prometheus returned 503 for metric '{}' (attempt {}/{}); retrying in {}s",
                metric_name,
                attempt,
                MAX_RETRIES,
                RETRY_DELAY.as_secs(),
            );
            thread::sleep(RETRY_DELAY);
            continue;
        }

        if !status.is_success() {
            return Err(ControllerError::PrometheusClient(format!(
                "Prometheus returned HTTP {} for metric '{}'",
                status, metric_name
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

        return Ok(Some(label_keys.into_iter().collect()));
    }

    Err(ControllerError::PrometheusClient(format!(
        "Prometheus returned 503 for metric '{}' after {} attempts; giving up",
        metric_name, MAX_RETRIES
    )))
}

/// Query Prometheus `GET /api/v1/label/__name__/values` and return all metric names.
pub fn fetch_all_metric_names(prometheus_url: &str) -> Result<Vec<String>, ControllerError> {
    let url = format!(
        "{}/api/v1/label/__name__/values",
        prometheus_url.trim_end_matches('/')
    );
    let client = reqwest::blocking::Client::new();

    for attempt in 1..=MAX_RETRIES {
        let response = client.get(&url).send().map_err(|e| {
            ControllerError::PrometheusClient(format!(
                "HTTP request failed for metric names: {}",
                e
            ))
        })?;

        let status = response.status();

        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            warn!(
                "Prometheus returned 503 for metric names (attempt {}/{}); retrying in {}s",
                attempt,
                MAX_RETRIES,
                RETRY_DELAY.as_secs(),
            );
            thread::sleep(RETRY_DELAY);
            continue;
        }

        if !status.is_success() {
            return Err(ControllerError::PrometheusClient(format!(
                "Prometheus returned HTTP {} for metric names",
                status
            )));
        }

        let body: serde_json::Value = response.json().map_err(|e| {
            ControllerError::PrometheusClient(format!(
                "Failed to parse Prometheus response for metric names: {}",
                e
            ))
        })?;

        let data = match body.get("data").and_then(|d| d.as_array()) {
            Some(arr) => arr,
            None => {
                warn!("Prometheus returned no 'data' array for metric names");
                return Ok(Vec::new());
            }
        };

        return Ok(data
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect());
    }

    Err(ControllerError::PrometheusClient(format!(
        "Prometheus returned 503 for metric names after {} attempts; giving up",
        MAX_RETRIES
    )))
}

/// Build a `PromQLSchema` by querying Prometheus for each metric name found in the given
/// PromQL queries. Metrics with no series in Prometheus are skipped with a warning.
pub fn build_schema_from_prometheus(
    prometheus_url: &str,
    queries: &[String],
) -> Result<PromQLSchema, ControllerError> {
    let metric_names = extract_metric_names(queries);
    debug!("Inferred metric names from queries: {:?}", metric_names);
    let mut schema = PromQLSchema::new();

    for metric_name in &metric_names {
        match fetch_labels_for_metric(prometheus_url, metric_name)? {
            Some(labels) => {
                debug!("Inferred labels for metric '{}': {:?}", metric_name, labels);
                schema = schema.add_metric(metric_name.clone(), KeyByLabelNames::new(labels));
            }
            None => {
                // Warning already emitted inside fetch_labels_for_metric.
            }
        }
    }

    Ok(schema)
}
